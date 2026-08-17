#!/usr/bin/env python3
"""
Client for the local approval dashboard (approval_server.py).

Every bot (core/bot.py's ChatBot, XkussBot, JustloBot) calls request_approval()
after generating a reply and BEFORE pasting/sending it. This blocks the bot's
cycle until a human clicks Approve or Reject on the dashboard:
  - Approve -> returns the (possibly hand-edited) reply text; the bot pastes
    and sends it.
  - Reject  -> returns None; the bot clicks 'Antwort generieren' again for a
    fresh reply and submits that one for approval instead.

Fail-safe by design: if the approval server is unreachable, request_approval()
raises instead of silently letting a reply through. The bot's existing
error-handling loop (retry in 15s, eventual re-login) already does the right
thing with that — it never falls back to auto-sending.
"""

import asyncio
import os
from datetime import datetime

import httpx

APPROVAL_SERVER_URL = os.environ.get("APPROVAL_SERVER_URL", "http://127.0.0.1:8799")
POLL_INTERVAL = 2  # seconds between "is it decided yet?" checks


class ApprovalRejected(Exception):
    """Raised... actually not used — reject is a normal return, not an error.
    Kept out of the public API; see request_approval()'s return value instead."""


async def request_approval(
    platform: str,
    reply: str,
    *,
    customer_message: str = "",
    context: str = "",
    timeout: float = 20.0,
) -> tuple[bool, str | None, str | None]:
    """Submit a candidate reply for human review and block until decided.

    Returns (approved, final_text, request_id):
      - approved=True,  final_text = the text to actually send (edits applied)
      - approved=False, final_text = None  (human clicked Reject — regenerate)
    """
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            f"{APPROVAL_SERVER_URL}/api/requests",
            json={
                "platform": platform,
                "reply": reply,
                "customer_message": customer_message,
                "context": context,
                "created_at": datetime.now().isoformat(timespec="seconds"),
            },
        )
        resp.raise_for_status()
        req_id = resp.json()["id"]

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
            # still "pending" -> keep polling


async def mark_sent(request_id: str | None):
    """Best-effort: tell the dashboard the approved reply actually went out."""
    if not request_id:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(f"{APPROVAL_SERVER_URL}/api/requests/{request_id}/sent")
    except Exception:
        pass  # dashboard bookkeeping only — never let this break the bot cycle


async def mark_failed(request_id: str | None, error: str = ""):
    """Best-effort: tell the dashboard the approved reply failed to send."""
    if not request_id:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                f"{APPROVAL_SERVER_URL}/api/requests/{request_id}/failed",
                json={"error": error},
            )
    except Exception:
        pass
