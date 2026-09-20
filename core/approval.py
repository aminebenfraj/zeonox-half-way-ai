#!/usr/bin/env python3
"""
Client for the local approval dashboard (approval_server.py).

Every bot (core/bot.py's ChatBot, XkussBot, JustloBot) calls request_approval()
after generating a reply and BEFORE pasting/sending it. This blocks the bot's
cycle until a human clicks Approve, Reject, Cancel, or Skip on the dashboard:
  - Approve -> returns the (possibly hand-edited) reply text; the bot pastes
    and sends it.
  - Reject  -> returns None; the bot clicks 'Antwort generieren' again for a
    fresh reply and submits that one for approval instead.
  - Cancel  -> raises ApprovalCancelled; the bot abandons this reply attempt
    and restarts/redetects the chat instead of retrying it.
  - Skip    -> raises ApprovalSkipped; supported bots actively leave the current
    conversation instead of generating or sending anything.

Bots also pass a `chat_still_active` check into request_approval() so a reply
whose chat closes out from under it (customer ended the conversation, tab
reloaded, etc.) while still pending gets cancelled automatically — the
dashboard card never becomes an orphan nobody will ever act on.

Fail-safe by design: if the approval server is unreachable, request_approval()
raises instead of silently letting a reply through. The bot's existing
error-handling loop (retry in 15s, eventual re-login) already does the right
thing with that — it never falls back to auto-sending.
"""

import asyncio
import base64
import json
import os
from datetime import datetime
from typing import Awaitable, Callable

import httpx

try:
    from simple_websocket import Client as WebSocketClient
except ImportError:  # flask-sock normally installs this; HTTP remains the fallback
    WebSocketClient = None

APPROVAL_SERVER_URL = os.environ.get("APPROVAL_SERVER_URL", "http://127.0.0.1:8799")
POLL_INTERVAL = 2  # seconds between "is it decided yet?" checks
WS_RECONNECT_INTERVAL = 2.0

# Matches APPROVAL_USER/APPROVAL_PASS on the server (see approval_server.py).
# None when unset, which disables auth on both sides for local dev.
_AUTH_USER = os.environ.get("APPROVAL_USER")
_AUTH_PASS = os.environ.get("APPROVAL_PASS")
_AUTH = (_AUTH_USER, _AUTH_PASS) if _AUTH_USER and _AUTH_PASS else None


def _approval_ws_url() -> str:
    base = APPROVAL_SERVER_URL.rstrip("/")
    if base.startswith("https://"):
        base = "wss://" + base[len("https://"):]
    elif base.startswith("http://"):
        base = "ws://" + base[len("http://"):]
    return f"{base}/ws/live"


def _approval_ws_headers() -> dict[str, str]:
    if not _AUTH:
        return {}
    raw = f"{_AUTH[0]}:{_AUTH[1]}".encode("utf-8")
    return {"Authorization": f"Basic {base64.b64encode(raw).decode('ascii')}"}


def _request_from_snapshot(payload: str | bytes, request_id: str) -> dict | None:
    """Find one approval request in a /ws/live full snapshot."""
    data = json.loads(payload)
    if data.get("type") != "snapshot":
        return None
    for bucket in (data.get("pending") or [], data.get("history") or []):
        for item in bucket:
            if item.get("id") == request_id:
                return item
    return None


def _decision_result(data: dict, reply: str, request_id: str):
    """Convert a request snapshot into a completed decision, if any."""
    status = data.get("status")
    if status == "approved":
        return True, (data.get("final_reply") or reply), request_id
    if status == "rejected":
        return False, None, request_id
    if status == "cancelled":
        raise ApprovalCancelled(request_id)
    if status == "skip_requested":
        raise ApprovalSkipped(request_id)
    return None


class ApprovalRejected(Exception):
    """Raised... actually not used — reject is a normal return, not an error.
    Kept out of the public API; see request_approval()'s return value instead."""


class ApprovalCancelled(Exception):
    """Raised by request_approval() when the pending request is cancelled —
    either a human clicked 'Cancel' on the dashboard, or (when a
    `chat_still_active` check was passed in) the chat this reply was meant for
    closed before anyone decided. Callers should treat this like
    ManualReviewLimitExceeded: abandon this reply attempt and restart/redetect
    rather than retrying the same request."""

    def __init__(self, request_id: str | None = None):
        super().__init__(f"Approval request {request_id} was cancelled")
        self.request_id = request_id


class ApprovalSkipped(Exception):
    """Raised when the dashboard asks the bot to transfer/skip the live conversation.

    The request remains ``skip_requested`` until the platform bot confirms the
    real UI action with mark_skipped(), so dashboard history never claims the
    conversation was skipped when the browser click actually failed.
    """

    def __init__(self, request_id: str | None = None):
        super().__init__(f"Approval request {request_id} requested a conversation skip")
        self.request_id = request_id


async def request_approval(
    platform: str,
    reply: str,
    *,
    last_message: str = "",
    customer_message: str = "",
    context: str = "",
    reply_type: str = "",
    client_profile: dict | None = None,
    fake_profile: dict | None = None,
    messages: list[dict] | None = None,
    timeout: float = 20.0,
    chat_still_active: Callable[[], Awaitable[bool]] | None = None,
    chat_check_interval: float = 6.0,
) -> tuple[bool, str | None, str | None]:
    """Submit a candidate reply for human review and block until decided.

    Returns (approved, final_text, request_id):
      - approved=True,  final_text = the text to actually send (edits applied)
      - approved=False, final_text = None  (human clicked Reject — regenerate)

    Raises ApprovalCancelled if a human clicks 'Cancel' on the dashboard, or if
    `chat_still_active` is given and reports the chat gone (checked every
    `chat_check_interval` seconds) before a decision is made. In the latter
    case the now-orphaned dashboard card is cancelled server-side too, so it
    doesn't sit in the queue forever after the conversation it belonged to has
    disappeared. Raises ApprovalSkipped when a supported approval card's
    "Skip conversation" action is clicked.
    """
    async with httpx.AsyncClient(timeout=timeout, auth=_AUTH) as client:
        resp = await client.post(
            f"{APPROVAL_SERVER_URL}/api/requests",
            json={
                "platform": platform,
                "reply": reply,
                "last_message": last_message,
                "customer_message": customer_message,
                "context": context,
                "reply_type": reply_type,
                "client_profile": client_profile or {},
                "fake_profile": fake_profile or {},
                "messages": messages or [],
                "created_at": datetime.now().isoformat(timespec="seconds"),
            },
        )
        resp.raise_for_status()
        req_id = resp.json()["id"]

        loop = asyncio.get_running_loop()
        next_chat_check = loop.time() + chat_check_interval
        next_status_ping = loop.time() + 20.0
        next_ws_retry = loop.time()
        ws = None

        try:
            while True:
                now = loop.time()

                # WebSocket is the primary decision channel. /ws/live sends an
                # immediate full snapshot on connect and another one for every
                # Approve/Reject/Cancel/Skip mutation, so dashboard actions are
                # consumed without waiting for the old two-second HTTP poll.
                if ws is None and WebSocketClient is not None and now >= next_ws_retry:
                    try:
                        ws = await asyncio.to_thread(
                            WebSocketClient,
                            _approval_ws_url(),
                            headers=_approval_ws_headers(),
                            ping_interval=20,
                        )
                    except Exception:
                        ws = None
                        next_ws_retry = now + WS_RECONNECT_INTERVAL

                data = None
                if ws is not None:
                    try:
                        payload = await asyncio.to_thread(ws.receive, 1.0)
                        if payload is not None:
                            data = _request_from_snapshot(payload, req_id)
                    except Exception:
                        try:
                            await asyncio.to_thread(ws.close)
                        except Exception:
                            pass
                        ws = None
                        next_ws_retry = loop.time() + WS_RECONNECT_INTERVAL
                else:
                    # Socket unavailable/reconnecting: retain the original HTTP
                    # behavior as a fail-safe so a decision can never be lost.
                    await asyncio.sleep(POLL_INTERVAL)
                    r = await client.get(f"{APPROVAL_SERVER_URL}/api/requests/{req_id}")
                    r.raise_for_status()
                    data = r.json()

                if data is not None:
                    decision = _decision_result(data, reply, req_id)
                    if decision is not None:
                        return decision

                now = loop.time()
                if now >= next_status_ping:
                    next_status_ping = now + 20.0
                    await report_status(
                        platform,
                        "approval",
                        "Waiting for approval",
                        checkpoint="approval",
                    )

                if chat_still_active is not None and now >= next_chat_check:
                    next_chat_check = now + chat_check_interval
                    try:
                        still_there = await chat_still_active()
                    except Exception:
                        still_there = True  # flaky check — never cancel on a fluke
                    if not still_there:
                        try:
                            await client.post(f"{APPROVAL_SERVER_URL}/api/requests/{req_id}/cancel")
                        except Exception:
                            pass  # dashboard bookkeeping only — still abandon locally below
                        raise ApprovalCancelled(req_id)
        finally:
            if ws is not None:
                try:
                    await asyncio.to_thread(ws.close)
                except Exception:
                    pass


async def mark_sent(request_id: str | None):
    """Best-effort: tell the dashboard the approved reply actually went out."""
    if not request_id:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0, auth=_AUTH) as client:
            await client.post(f"{APPROVAL_SERVER_URL}/api/requests/{request_id}/sent")
    except Exception:
        pass  # dashboard bookkeeping only — never let this break the bot cycle


async def mark_failed(request_id: str | None, error: str = ""):
    """Best-effort: tell the dashboard the approved reply failed to send."""
    if not request_id:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0, auth=_AUTH) as client:
            await client.post(
                f"{APPROVAL_SERVER_URL}/api/requests/{request_id}/failed",
                json={"error": error},
            )
    except Exception:
        pass


async def mark_skipped(request_id: str | None, result: str = "skipped"):
    """Confirm that a transfer-or-skip platform action succeeded."""
    if not request_id:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0, auth=_AUTH) as client:
            await client.post(
                f"{APPROVAL_SERVER_URL}/api/requests/{request_id}/skipped",
                json={"result": result},
            )
    except Exception:
        pass  # dashboard bookkeeping only; the browser action already succeeded


async def report_status(
    platform: str,
    state: str,
    detail: str = "",
    *,
    retry_count: int = 0,
    warning: str = "",
    checkpoint: str = "",
):
    """Best-effort workflow telemetry for the live dashboard.

    `retry_count`, `warning`, and `checkpoint` make recovery observable without
    affecting the bot cycle. A stale/missing status only makes the dashboard
    show the platform as offline; reporting can never stop message handling.
    """
    try:
        async with httpx.AsyncClient(timeout=5.0, auth=_AUTH) as client:
            await client.post(
                f"{APPROVAL_SERVER_URL}/api/status",
                json={
                    "platform": platform,
                    "pid": os.getpid(),
                    "state": state,
                    "detail": detail,
                    "retry_count": max(0, int(retry_count or 0)),
                    "warning": warning,
                    "checkpoint": checkpoint,
                },
            )
    except Exception:
        pass
