#!/usr/bin/env python3
"""
Client for the local approval dashboard (approval_server.py).

Every bot (core/bot.py's ChatBot, XkussBot, JustloBot) calls request_approval()
after generating a reply and BEFORE pasting/sending it. This blocks the bot's
cycle until a human clicks Approve, Reject or Cancel on the dashboard:
  - Approve -> returns the (possibly hand-edited) reply text; the bot pastes
    and sends it.
  - Reject  -> returns None; the bot clicks 'Antwort generieren' again for a
    fresh reply and submits that one for approval instead.
  - Cancel  -> raises ApprovalCancelled; the bot abandons this reply attempt
    and restarts/redetects the chat instead of retrying it.

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
import os
from datetime import datetime
from typing import Awaitable, Callable

import httpx

APPROVAL_SERVER_URL = os.environ.get("APPROVAL_SERVER_URL", "http://127.0.0.1:8799")
POLL_INTERVAL = 2  # seconds between "is it decided yet?" checks

# Matches APPROVAL_USER/APPROVAL_PASS on the server (see approval_server.py).
# None when unset, which disables auth on both sides for local dev.
_AUTH_USER = os.environ.get("APPROVAL_USER")
_AUTH_PASS = os.environ.get("APPROVAL_PASS")
_AUTH = (_AUTH_USER, _AUTH_PASS) if _AUTH_USER and _AUTH_PASS else None


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


async def request_approval(
    platform: str,
    reply: str,
    *,
    customer_message: str = "",
    context: str = "",
    reply_type: str = "",
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
    disappeared.
    """
    async with httpx.AsyncClient(timeout=timeout, auth=_AUTH) as client:
        resp = await client.post(
            f"{APPROVAL_SERVER_URL}/api/requests",
            json={
                "platform": platform,
                "reply": reply,
                "customer_message": customer_message,
                "context": context,
                "reply_type": reply_type,
                "created_at": datetime.now().isoformat(timespec="seconds"),
            },
        )
        resp.raise_for_status()
        req_id = resp.json()["id"]

        since_chat_check = 0.0
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            r = await client.get(f"{APPROVAL_SERVER_URL}/api/requests/{req_id}")
            r.raise_for_status()
            data = r.json()
            status = data.get("status")
            if status == "approved":
                return True, (data.get("final_reply") or reply), req_id
            if status == "rejected":
                return False, None, req_id
            if status == "cancelled":
                raise ApprovalCancelled(req_id)
            # still "pending" -> keep polling

            if chat_still_active is not None:
                since_chat_check += POLL_INTERVAL
                if since_chat_check >= chat_check_interval:
                    since_chat_check = 0.0
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


async def report_status(platform: str, state: str, detail: str = ""):
    """Best-effort: tell the dashboard what this bot is doing right now, e.g.
    'waiting_for_chat', 'chat_detected', 'generating', 'awaiting_approval',
    'sending', 'idle', 'error'. Powers the live per-platform detector shown on
    the dashboard. Never allowed to break the bot cycle — a stale/missing
    status just means the dashboard shows the platform as offline."""
    try:
        async with httpx.AsyncClient(timeout=5.0, auth=_AUTH) as client:
            await client.post(
                f"{APPROVAL_SERVER_URL}/api/status",
                json={"platform": platform, "state": state, "detail": detail},
            )
    except Exception:
        pass
