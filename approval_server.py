#!/usr/bin/env python3
"""
Local approval dashboard.

Every bot process (one per platform, see run_bot.py) POSTs a candidate reply
here right after generating it, then blocks polling until a human decides.
This process is the single source of truth for those pending decisions, so it
must be started BEFORE the bots (start_all.py / launch_all.py do this
automatically) and kept running for as long as any bot is running.

Endpoints:
  POST /api/requests              -> create a request, returns {id, auto}. In
                                      auto mode it's created pre-approved (see
                                      /api/mode below); in manual mode it's pending.
  GET  /api/requests?status=...   -> list requests (dashboard polls this)
  GET  /api/requests/<id>         -> single request (bots poll this)
  POST /api/requests/<id>/approve -> body {edited_reply?}
  POST /api/requests/<id>/reject
  POST /api/requests/<id>/cancel  -> abandon it; bot restarts/redetects instead of regenerating
  POST /api/requests/<id>/skip    -> request transfer, falling back to skip when unavailable
  POST /api/requests/<id>/skipped -> bot confirms the platform UI action succeeded
  POST /api/requests/<id>/sent    -> bot confirms the approved reply went out
  POST /api/requests/<id>/failed  -> body {error?}
  GET  /api/mode                  -> {mode: "manual"|"auto"} — the dashboard's review mode
  POST /api/mode                  -> body {mode: "manual"|"auto"}, flips it (dashboard's toggle button)
  POST /api/status                -> bot reports its current state, body {platform, state, detail?}
  GET  /api/status                -> live per-platform state (dashboard polls this)
  GET  /                          -> the dashboard page (dark, auto-refreshing)

State lives in memory only (a handful of concurrent platforms, short-lived
requests) — restarting this process drops any requests that were mid-review,
any live status, and resets the review mode back to "manual"; the affected
bot's next generate/status ping simply creates a fresh one.
"""

import concurrent.futures
import hmac
import itertools
import json
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

import httpx
from flask import Flask, jsonify, request, Response

from core.launcher import is_cdp_ready
from core import push_notifications


try:
    from flask_sock import Sock
    from simple_websocket import ConnectionClosed
except ImportError:  # live-push is a nice-to-have — dashboard falls back to HTTP polling without it
    Sock = None
    ConnectionClosed = Exception

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from groq import Groq
except ImportError:  # the "Test AI Key" panel is a nice-to-have — dashboard still works without it
    Groq = None

# $PORT is what most cloud hosts (Render, Railway, etc.) inject; fall back to
# the original local-dev var/default when running on a laptop.
PORT = int(os.environ.get("PORT") or os.environ.get("APPROVAL_SERVER_PORT", "8799"))
HOST = os.environ.get("HOST", "127.0.0.1")

# If both are set, every route requires HTTP Basic Auth with these creds --
# this dashboard shows real customer messages and can send real replies, so
# it must never sit on the public internet without a login. Left unset for
# local dev on 127.0.0.1, where only processes on the same machine can reach
# it anyway.
AUTH_USER = os.environ.get("APPROVAL_USER")
AUTH_PASS = os.environ.get("APPROVAL_PASS")

MAX_HISTORY = 300  # decided/sent/failed requests kept for the dashboard's history list

# Every platform this project knows about, in the order they should appear in
# the dashboard. Kept here (not imported from configs/) so this server can run
# standalone without pulling in Playwright configs. A platform name that shows
# up in a request but isn't in this list still gets its own section — it's
# just appended after the known ones instead of being dropped.
KNOWN_PLATFORMS = [
    "Gold", "Gold2", "Gold3", "Diamond", "Platin", "S69", "ML",
    "Xkuss", "Justlo", "Linduu", "Gnoxx",
]

# One accent color per platform (mirrors the ANSI colors launch_all.py/start_all.py
# already use for terminal output) so a platform is visually identifiable at a
# glance across the sidebar, badges and section headers.
PLATFORM_COLORS = {
    "Gold":    "#f59e0b",  # amber
    "Gold2":   "#eab308",  # yellow
    "Gold3":   "#d946ef",  # fuchsia
    "Diamond": "#22d3ee",  # cyan
    "Platin":  "#94a3b8",  # slate
    "S69":     "#ec4899",  # pink
    "ML":      "#22c55e",  # green
    "Xkuss":   "#ef4444",  # red
    "Justlo":  "#3b82f6",  # blue
    "Linduu":  "#10b981",  # emerald
    "Gnoxx":   "#0ea5e9",  # sky
}
_FALLBACK_PALETTE = ["#8b5cf6", "#06b6d4", "#f97316", "#14b8a6", "#a855f7"]

# launch_all.py's control API (restart/stop/fix/checkinall/... as HTTP instead
# of typed terminal commands — see run_bots() there). Only reachable when this
# dashboard runs on the same machine as launch_all.py, which is the local-dev
# setup; a cloud-hosted dashboard simply won't be able to reach it, and the
# control panel degrades to "unavailable" rather than erroring (see
# _control_get()/_control_post() below).
LAUNCHER_CONTROL_URL = os.environ.get("LAUNCHER_CONTROL_URL", "http://127.0.0.1:8800")

app = Flask(__name__)
sock = Sock(app) if Sock else None


@app.before_request
def _require_auth():
    if not AUTH_USER or not AUTH_PASS:
        return None  # auth disabled (local dev default)
    auth = request.authorization
    valid = (
        auth is not None
        and hmac.compare_digest(auth.username or "", AUTH_USER)
        and hmac.compare_digest(auth.password or "", AUTH_PASS)
    )
    if not valid:
        return Response(
            "Authentication required", 401,
            {"WWW-Authenticate": 'Basic realm="Chat Approval Dashboard"'},
        )
    return None


_lock = threading.Lock()
_requests: dict[str, dict] = {}
_order = itertools.count()  # monotonic insertion counter, used to sort newest-first

# Live-push plumbing for /ws/live (see near the bottom of the file). A plain
# version counter + condition variable, bumped by _bump_state() right after
# every mutation below — cheap to check, and lets connected sockets block on
# _state_cond.wait() instead of re-polling in-memory state on a timer. Kept
# deliberately separate from _lock (a different lock entirely) so bumping
# never has to happen while _lock is held.
_state_version = 0
_state_cond = threading.Condition()


def _bump_state():
    global _state_version
    with _state_cond:
        _state_version += 1
        _state_cond.notify_all()

# Live per-platform workflow state (see core/approval.py's report_status()),
# keyed by platform name. Process liveness comes independently from
# launch_all.py's control socket, so stale workflow telemetry does not make a
# still-running bot appear offline.
_status: dict[str, dict] = {}
STATUS_STALE_AFTER = 45  # workflow telemetry freshness; process liveness is tracked separately

# Review mode, toggled from the dashboard (see /api/mode below). "manual" is
# the original behaviour — every request waits in the queue for a human.
# "auto" auto-approves each NEW request the instant it's created, so the bot's
# request_approval() poll (unchanged — it just sees status "approved" on its
# very first check) sends it straight through with no one watching. Requests
# already pending when the mode flips are left alone either way — never yank
# a card out from under someone who's mid-edit.
#
# This is the DEFAULT for every platform. _mode_overrides (below) lets the
# self-managed platforms opt out of it independently without touching
# this global — so flipping Xkuss to auto never affects Gold/Diamond/etc, and
# never affects Justlo/Linduu either.
_mode = "manual"

# Per-platform override of the review mode above, keyed by lowercase platform
# slug ("xkuss"/"justlo"/"linduu"/"gnoxx" — see /bots). Populated only when a platform
# has explicitly chosen something other than "use the global default"; a
# platform with no entry here just falls back to _mode. Read live on every
# request (see create_request()'s `_mode_overrides.get(...)` lookup) — no
# restart needed, same as the global toggle always worked.
_mode_overrides: dict[str, str] = {}

SELF_MANAGED_PLATFORMS = ("xkuss", "justlo", "linduu", "gnoxx")
SKIPPABLE_APPROVAL_PLATFORMS = frozenset(SELF_MANAGED_PLATFORMS)

# ── Bot process management (see /bots) ──────────────────────────────────────
# Only the self-managed platforms: each does its own Chrome launch +
# login inside run_bot.py, so starting one is just spawning that one process
# -- no Playwright-based Chrome/login bring-up needed here (that stays in
# launch_all.py, kept deliberately out of this process; see KNOWN_PLATFORMS
# above for the same reasoning). React platforms (Gold/Diamond/...) still
# only start via launch_all.py / Bot Controls, unchanged.
_BASE_DIR = Path(__file__).resolve().parent
_SELF_MANAGED_CDP_PORTS = {"xkuss": 9227, "justlo": 9229, "linduu": 9230, "gnoxx": 9231}
_bot_procs: dict[str, subprocess.Popen] = {}


def _bot_status(platform: str) -> dict:
    """Best-effort status for one self-managed platform. Distinguishes a bot
    this dashboard started (has a tracked Popen) from one that's merely
    occupying that platform's CDP port some other way (started via
    launch_all.py, a leftover process, ...) so Start can refuse to double-
    launch either way, with an honest reason."""
    proc = _bot_procs.get(platform)
    if proc is not None and proc.poll() is None:
        return {"running": True, "pid": proc.pid, "managed_by": "dashboard"}
    if proc is not None:
        _bot_procs.pop(platform, None)  # exited -- stop tracking it as running
    port = _SELF_MANAGED_CDP_PORTS[platform]
    if is_cdp_ready(port):
        return {"running": True, "pid": None, "managed_by": "external"}
    return {"running": False, "pid": None, "managed_by": None}


@app.get("/api/bots/status")
def bots_status():
    return jsonify({p: _bot_status(p) for p in SELF_MANAGED_PLATFORMS})


@app.post("/api/bots/<platform>/start")
def bots_start(platform):
    platform = platform.strip().lower()
    if platform not in SELF_MANAGED_PLATFORMS:
        return jsonify({"ok": False, "error": f"platform must be one of {SELF_MANAGED_PLATFORMS}"}), 400
    status = _bot_status(platform)
    if status["running"]:
        return jsonify({"ok": False, "error": f"{platform} is already running ({status['managed_by']})"}), 409
    # Give it its own visible console instead of swallowing stdout/stderr --
    # every bot already logs its every step verbosely (self.log(...) calls
    # throughout core/xkuss_bot.py / core/justlo_bot.py), so this is how you
    # actually see what it's doing / why it's stuck, same as running
    # `python run_bot.py <platform>` in a terminal yourself.
    popen_kwargs = {}
    if sys.platform == "win32":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_CONSOLE
    proc = subprocess.Popen(
        [sys.executable, "-u", str(_BASE_DIR / "run_bot.py"), platform],
        cwd=str(_BASE_DIR),
        **popen_kwargs,
    )
    _bot_procs[platform] = proc
    return jsonify({"ok": True, "pid": proc.pid})


@app.post("/api/bots/<platform>/stop")
def bots_stop(platform):
    platform = platform.strip().lower()
    if platform not in SELF_MANAGED_PLATFORMS:
        return jsonify({"ok": False, "error": f"platform must be one of {SELF_MANAGED_PLATFORMS}"}), 400
    proc = _bot_procs.get(platform)
    if proc is None or proc.poll() is not None:
        return jsonify({
            "ok": False,
            "error": f"{platform} isn't running under this dashboard's control "
                     "(either stopped already, or started some other way -- stop it from wherever it was started).",
        }), 409
    proc.terminate()
    _bot_procs.pop(platform, None)
    return jsonify({"ok": True})


# Translation calls use the configured Groq account first and OpenRouter as a
# provider-level fallback. Keep them on a bounded worker thread so an upstream
# outage never stalls a bot cycle indefinitely.
_translate_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="translate")

GROQ_TRANSLATION_MODEL = os.environ.get("GROQ_TRANSLATION_MODEL", "openai/gpt-oss-120b")
OPENROUTER_TRANSLATION_MODEL = os.environ.get("OPENROUTER_TRANSLATION_MODEL", "openai/gpt-4o")
_TRANSLATION_SYSTEM_PROMPT = (
    "Translate the user's German text into natural English. Preserve meaning, "
    "tone, names, emojis, paragraph breaks, and punctuation. Return only the "
    "English translation, with no notes, labels, or quotation marks."
)


def _translation_messages(text: str) -> list[dict[str, str]]:
    return [
        {"role": "system", "content": _TRANSLATION_SYSTEM_PROMPT},
        {"role": "user", "content": text},
    ]


def _translate_with_groq(text: str) -> str | None:
    client = _get_groq_test_client()
    if client is None:
        return None
    response = client.chat.completions.create(
        model=GROQ_TRANSLATION_MODEL,
        messages=_translation_messages(text),
        temperature=0,
        max_completion_tokens=1200,
        top_p=1,
        reasoning_effort="low",
        timeout=8.0,
    )
    return (response.choices[0].message.content or "").strip() or None


def _translate_with_openrouter(text: str) -> str | None:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        return None

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": os.environ.get("OPENROUTER_HTTP_REFERER", "http://127.0.0.1:8799"),
        "X-Title": os.environ.get("OPENROUTER_APP_TITLE", "Zenox"),
    }
    response = httpx.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers=headers,
        json={
            "model": OPENROUTER_TRANSLATION_MODEL,
            "messages": _translation_messages(text),
            "temperature": 0,
            "max_tokens": 1200,
        },
        timeout=12.0,
    )
    response.raise_for_status()
    data = response.json()
    return (data["choices"][0]["message"]["content"] or "").strip() or None


def _translate_de_en_sync(text: str) -> str | None:
    try:
        translated = _translate_with_groq(text)
        if translated:
            return translated
    except Exception:
        pass

    try:
        return _translate_with_openrouter(text)
    except Exception:
        return None


def _translate_de_en(text: str, timeout: float = 22.0) -> str | None:
    text = (text or "").strip()
    if not text:
        return None
    try:
        future = _translate_pool.submit(_translate_de_en_sync, text)
        return future.result(timeout=timeout)
    except Exception:
        return None


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


# ── Meeting guard (Groq) ─────────────────────────────────────────────────────
# Two consumers share this: the dashboard's "Test AI Key" panel (a sandbox —
# nothing it does gets sent anywhere), and the live Auto-pilot path in
# create_request() below, which actually uses the verdict to decide what gets
# auto-sent. Lazily constructed (not at import time) so a missing/invalid key
# only disables these features instead of the whole dashboard, and so a key
# added to .env after the server started still works without a restart.
GROQ_TEST_MODEL = "openai/gpt-oss-120b"
_GROQ_TEST_SYSTEM_PROMPT = """You are reviewing a chat message on behalf of a user.

Step 1: Decide if the message is about scheduling, proposing, or confirming a meeting or making plans.
Step 2:
- If it is NOT about a meeting/plans: approve it as-is — the reply is the original message, unchanged.
- If it IS about a meeting/plans: write a reply that does ONE of: stall/procrastinate, propose a reschedule, or politely decline while keeping the conversation open.

Hard rules for the "reply" field, whenever you write a new one (Step 2's meeting case):
- Always write it in German. Never any other language, even if the input message is in a different language.
- Never use an en dash or em dash followed by a space (no "– " and no "— ") as a stylistic pause. Use a comma or a period instead.

Respond ONLY with valid JSON, no markdown fences, in this exact shape:
{"contains_meeting": true or false, "action": "approve" or "procrastinate" or "reschedule" or "decline", "reply": "the reply text"}"""

_BANNED_DASH_RE = re.compile("[–—]\\s+")  # en dash / em dash used as a pause: "– " / "— "


def _sanitize_meeting_reply(text: str) -> str:
    text = _BANNED_DASH_RE.sub("", text or "")
    return re.sub(r" {2,}", " ", text).strip()


def _get_groq_test_client():
    if Groq is None:
        return None
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        return None
    return Groq(api_key=api_key)


def _classify_message_with_groq(message: str) -> dict:
    """Runs the meeting-guard prompt on `message`. Returns either
    {"ok": True, "contains_meeting", "action", "reply"} (reply already
    sanitized — German-only instruction is the model's job, the dash strip is
    enforced here regardless of whether the model actually complied) or
    {"ok": False, "error"}."""
    client = _get_groq_test_client()
    if client is None:
        reason = "groq package not installed" if Groq is None else "GROQ_API_KEY is not set"
        return {"ok": False, "error": reason, "status": 400}

    try:
        resp = client.chat.completions.create(
            model=GROQ_TEST_MODEL,
            messages=[
                {"role": "system", "content": _GROQ_TEST_SYSTEM_PROMPT},
                {"role": "user", "content": message},
            ],
            temperature=0,
            max_completion_tokens=512,
            top_p=1,
            reasoning_effort="low",
            response_format={"type": "json_object"},
        )
    except Exception as e:
        return {"ok": False, "error": str(e), "status": 502}

    raw = resp.choices[0].message.content or "{}"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {"ok": False, "error": "model returned invalid JSON", "raw": raw, "status": 502}

    return {
        "ok": True,
        "contains_meeting": bool(data.get("contains_meeting", False)),
        "action": data.get("action") or "approve",
        "reply": _sanitize_meeting_reply(data.get("reply") or ""),
    }


@app.post("/api/test-groq")
def test_groq():
    body = request.get_json(force=True, silent=True) or {}
    message = (body.get("message") or "").strip()
    if not message:
        return jsonify({"ok": False, "error": "message is required"}), 400

    result = _classify_message_with_groq(message)
    status = result.pop("status", 200)
    return jsonify(result), status




def _prune_history_locked():
    """Cap total stored requests so a long-running dashboard doesn't leak memory.
    Only ever drops fully decided requests, oldest first. A skip request stays
    protected until its bot acknowledges the browser action."""
    if len(_requests) <= MAX_HISTORY:
        return
    decided = sorted(
        (r for r in _requests.values() if r["status"] not in ("pending", "skip_requested")),
        key=lambda r: r["_seq"],
    )
    overflow = len(_requests) - MAX_HISTORY
    for r in decided[:overflow]:
        _requests.pop(r["id"], None)


# ── API ──────────────────────────────────────────────────────────────────────

@app.post("/api/requests")
def create_request():
    body = request.get_json(force=True, silent=True) or {}
    platform = (body.get("platform") or "unknown").strip()
    reply = body.get("reply") or ""
    customer_message = body.get("customer_message") or ""
    # Literal latest conversation message, regardless of sender. Older bot
    # versions only send customer_message, so retain that as a display fallback.
    last_message = body.get("last_message") or customer_message
    reply_type = (body.get("reply_type") or "").strip() or None  # e.g. "DIA" / "ASA Follow-up" (Justlo only) — None means the bot doesn't report one
    # Optional profile data supplied by a bot and shown on the dashboard so a
    # reviewer can sanity-check the context before approving.
    client_profile = body.get("client_profile") or {}
    fake_profile = body.get("fake_profile") or {}
    if not reply.strip():
        return jsonify({"error": "reply is required"}), 400

    # Best-effort DE->EN translation so a reviewer who doesn't read German can
    # still judge the reply. The German text is always what's authoritative /
    # editable / actually sent — translations are read-only context.
    reply_en = _translate_de_en(reply)
    customer_message_en = _translate_de_en(customer_message) if customer_message else None
    last_message_en = _translate_de_en(last_message) if last_message else None

    with _lock:
        # A platform's own override (set on /bots) wins over the dashboard's
        # global default — this is what makes "auto for Xkuss, manual for
        # everything else" possible without a separate code path per platform.
        auto = _mode_overrides.get(platform.strip().lower(), _mode) == "auto"

    # Auto-pilot's meeting guard: every reply is checked before it can ever
    # auto-send. Classifies the customer's message when the bot forwarded one
    # (most platforms do); falls back to classifying the reply text itself for
    # the platforms that currently don't (xkuss, justlo). If a meeting is
    # detected, the German stall/reschedule/decline text replaces the reply.
    # If the check itself fails (key missing/invalid, network, bad JSON), this
    # fails SAFE: falls back to pending exactly like manual mode, rather than
    # auto-sending something that was never actually checked.
    final_reply = reply
    meeting_guard = None
    contains_meeting = None  # None = guard never ran (manual mode, or fell back before checking)
    if auto:
        classify_input = customer_message.strip() or reply
        result = _classify_message_with_groq(classify_input)
        if not result["ok"]:
            print(f"[MeetingGuard] Groq check failed for {platform} — holding for manual review: {result.get('error')}")
            auto = False
        else:
            contains_meeting = result["contains_meeting"]
            meeting_guard = result["action"]  # recorded even for "approve" so the dashboard can show every verdict, not just the ones that changed something
            if result["contains_meeting"] and result["reply"]:
                final_reply = result["reply"]

    req_id = str(uuid.uuid4())
    with _lock:
        now = _now()
        _requests[req_id] = {
            "id": req_id,
            "platform": platform,
            "last_message": last_message,
            "last_message_en": last_message_en,
            "customer_message": customer_message,
            "customer_message_en": customer_message_en,
            "client_profile": client_profile,
            "fake_profile": fake_profile,
            "context": body.get("context") or "",
            "reply_type": reply_type,
            "reply": reply,
            "reply_en": reply_en,
            "final_reply": final_reply if auto else None,
            "status": "approved" if auto else "pending",
            "created_at": body.get("created_at") or now,
            "decided_at": now if auto else None,
            "sent_at": None,
            "error": None,
            "auto": auto,
            "meeting_guard": meeting_guard,
            "contains_meeting": contains_meeting,
            "_seq": next(_order),
        }
        _prune_history_locked()
        created_request = dict(_requests[req_id])
        pending_count = sum(1 for item in _requests.values() if item["status"] == "pending")
    _bump_state()
    if not auto:
        push_notifications.queue_approval(created_request, pending_count)
    return jsonify({"id": req_id, "auto": auto, "meeting_guard": meeting_guard}), 201


@app.get("/api/push/config")
def push_config():
    return jsonify({
        "enabled": push_notifications.is_available(),
        "public_key": push_notifications.PUBLIC_KEY,
        "subscriptions": push_notifications.subscription_count(),
    })


@app.post("/api/push/subscribe")
def push_subscribe():
    if not push_notifications.is_available():
        return jsonify({
            "ok": False,
            "error": "Web Push is not configured. Check pywebpush and VAPID settings.",
        }), 503
    body = request.get_json(force=True, silent=True) or {}
    subscription = body.get("subscription") or body
    try:
        count = push_notifications.add_subscription(subscription)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    if body.get("send_test", True):
        push_notifications.queue_test(subscription.get("endpoint", ""))
    return jsonify({"ok": True, "subscriptions": count})


@app.delete("/api/push/subscribe")
def push_unsubscribe():
    body = request.get_json(force=True, silent=True) or {}
    endpoint = body.get("endpoint") or ""
    count = push_notifications.remove_subscription(endpoint)
    return jsonify({"ok": True, "subscriptions": count})


@app.get("/api/mode")
def get_mode():
    with _lock:
        return jsonify({"mode": _mode})


@app.post("/api/mode")
def set_mode():
    global _mode
    body = request.get_json(force=True, silent=True) or {}
    mode = body.get("mode")
    if mode not in ("manual", "auto"):
        return jsonify({"error": "mode must be 'manual' or 'auto'"}), 400
    with _lock:
        _mode = mode
    _bump_state()
    return jsonify({"ok": True, "mode": mode})


@app.get("/api/mode/override")
def get_mode_override():
    platform = (request.args.get("platform") or "").strip().lower()
    if platform not in SELF_MANAGED_PLATFORMS:
        return jsonify({"error": f"platform must be one of {SELF_MANAGED_PLATFORMS}"}), 400
    with _lock:
        override = _mode_overrides.get(platform)
        effective = override or _mode
    return jsonify({"platform": platform, "override": override, "mode": _mode, "effective": effective})


@app.post("/api/mode/override")
def set_mode_override():
    body = request.get_json(force=True, silent=True) or {}
    platform = (body.get("platform") or "").strip().lower()
    if platform not in SELF_MANAGED_PLATFORMS:
        return jsonify({"error": f"platform must be one of {SELF_MANAGED_PLATFORMS}"}), 400
    mode = body.get("mode")
    if mode not in ("manual", "auto", "default"):
        return jsonify({"error": "mode must be 'manual', 'auto', or 'default'"}), 400
    with _lock:
        if mode == "default":
            _mode_overrides.pop(platform, None)
        else:
            _mode_overrides[platform] = mode
        effective = _mode_overrides.get(platform, _mode)
    _bump_state()
    # Approval mode is read live per-request (see create_request()) — no
    # restart needed for this one to take effect, unlike Source above.
    return jsonify({"ok": True, "platform": platform, "effective": effective})


@app.get("/api/platforms")
def list_platforms():
    return jsonify([
        {"name": name, "color": PLATFORM_COLORS.get(name, _FALLBACK_PALETTE[i % len(_FALLBACK_PALETTE)])}
        for i, name in enumerate(KNOWN_PLATFORMS)
    ])


@app.get("/api/requests")
def list_requests():
    status = request.args.get("status")
    limit = request.args.get("limit", type=int)
    with _lock:
        items = list(_requests.values())
    if status:
        wanted = set(status.split(","))
        items = [r for r in items if r["status"] in wanted]
    items.sort(key=lambda r: r["_seq"], reverse=True)
    if limit:
        items = items[:limit]
    return jsonify([{k: v for k, v in r.items() if k != "_seq"} for r in items])


@app.get("/api/requests/<req_id>")
def get_request(req_id):
    with _lock:
        r = _requests.get(req_id)
    if not r:
        return jsonify({"error": "not found"}), 404
    return jsonify({k: v for k, v in r.items() if k != "_seq"})


@app.post("/api/requests/<req_id>/approve")
def approve_request(req_id):
    body = request.get_json(force=True, silent=True) or {}
    with _lock:
        r = _requests.get(req_id)
        if not r:
            return jsonify({"error": "not found"}), 404
        if r["status"] != "pending":
            return jsonify({"error": f"already {r['status']}"}), 409
        edited = body.get("edited_reply")
        r["final_reply"] = edited.strip() if isinstance(edited, str) and edited.strip() else r["reply"]
        r["status"] = "approved"
        r["decided_at"] = _now()
    _bump_state()
    return jsonify({"ok": True})


@app.post("/api/requests/<req_id>/reject")
def reject_request(req_id):
    with _lock:
        r = _requests.get(req_id)
        if not r:
            return jsonify({"error": "not found"}), 404
        if r["status"] != "pending":
            return jsonify({"error": f"already {r['status']}"}), 409
        r["status"] = "rejected"
        r["decided_at"] = _now()
    _bump_state()
    return jsonify({"ok": True})


@app.post("/api/requests/<req_id>/cancel")
def cancel_request(req_id):
    """Abandon a pending request entirely — unlike reject, the bot does not
    regenerate and resubmit; it restarts/redetects the chat instead. Used both
    by the dashboard's Cancel button and by a bot that notices its own chat
    closed out from under a still-pending request (see request_approval()'s
    chat_still_active check in core/approval.py)."""
    with _lock:
        r = _requests.get(req_id)
        if not r:
            return jsonify({"error": "not found"}), 404
        if r["status"] != "pending":
            return jsonify({"error": f"already {r['status']}"}), 409
        r["status"] = "cancelled"
        r["decided_at"] = _now()
    _bump_state()
    return jsonify({"ok": True})


@app.post("/api/requests/<req_id>/skip")
def skip_request(req_id):
    """Ask a supported bot to transfer or leave its loaded conversation.

    The request stays ``skip_requested`` until the bot acknowledges that the
    real platform UI action succeeded. Xkuss leaves via Home. The shared
    Justlo/Linduu/Gnoxx engine tries ``Übergeben`` first and presses
    ``Überspringen`` only when no other moderator is available.
    """
    with _lock:
        r = _requests.get(req_id)
        if not r:
            return jsonify({"error": "not found"}), 404
        if r["status"] != "pending":
            return jsonify({"error": f"already {r['status']}"}), 409
        platform = (r.get("platform") or "").strip().lower()
        if platform not in SKIPPABLE_APPROVAL_PLATFORMS:
            return jsonify({
                "error": "Skip conversation is only available for Xkuss, Justlo, Linduu, and Gnoxx"
            }), 400
        r["status"] = "skip_requested"
        r["decided_at"] = _now()
    _bump_state()
    return jsonify({"ok": True})


@app.post("/api/requests/<req_id>/skipped")
def mark_request_skipped(req_id):
    """Bot acknowledgement that the transfer-or-skip action succeeded."""
    body = request.get_json(force=True, silent=True) or {}
    result = (body.get("result") or "skipped").strip().lower()
    if result not in ("skipped", "transferred"):
        return jsonify({"error": "result must be 'skipped' or 'transferred'"}), 400
    with _lock:
        r = _requests.get(req_id)
        if not r:
            return jsonify({"error": "not found"}), 404
        if r["status"] != "skip_requested":
            return jsonify({"error": f"cannot mark skipped from {r['status']}"}), 409
        r["status"] = result
        r["sent_at"] = _now()
    _bump_state()
    return jsonify({"ok": True})


@app.post("/api/requests/<req_id>/sent")
def mark_sent(req_id):
    with _lock:
        r = _requests.get(req_id)
        if r:
            r["status"] = "sent"
            r["sent_at"] = _now()
    _bump_state()
    return jsonify({"ok": True})


@app.post("/api/requests/<req_id>/failed")
def mark_failed(req_id):
    body = request.get_json(force=True, silent=True) or {}
    with _lock:
        r = _requests.get(req_id)
        if r:
            r["status"] = "failed"
            r["error"] = body.get("error") or ""
            r["sent_at"] = _now()
    _bump_state()
    return jsonify({"ok": True})


@app.post("/api/status")
def update_status():
    """Receive bot stage, retry, warning, and recovery-checkpoint telemetry."""
    body = request.get_json(force=True, silent=True) or {}
    platform = (body.get("platform") or "").strip()
    if not platform:
        return jsonify({"error": "platform is required"}), 400
    try:
        retry_count = max(0, int(body.get("retry_count") or 0))
    except (TypeError, ValueError):
        retry_count = 0
    with _lock:
        _status[platform] = {
            "state": body.get("state") or "unknown",
            "detail": body.get("detail") or "",
            "retry_count": retry_count,
            "warning": body.get("warning") or "",
            "checkpoint": body.get("checkpoint") or "",
            "updated_at": _now(),
        }
    _bump_state()
    return jsonify({"ok": True})


@app.get("/api/status")
def list_status():
    with _lock:
        return jsonify(dict(_status))


@app.get("/api/health")
def health():
    return jsonify({"ok": True})


# ── Control panel (proxies launch_all.py's control API) ────────────────────
# Browser JS never talks to LAUNCHER_CONTROL_URL directly — it goes through
# these two routes so the control port stays loopback-only and this page's
# existing Basic Auth (if configured) still gates it.
#
# The actual httpx call is polled in a background thread rather than made
# inline on every request: when the control server isn't running at all (the
# common case for a standalone dashboard), connecting to a closed local port
# can itself take 1-3s to fail depending on the OS/network stack — fine for
# an occasional page-load fetch, but fatal to /ws/live's whole point if it
# happened inline in that loop (every connected socket would stall its
# state-changed push behind a multi-second dead TCP attempt). Polling once in
# the background and having every reader hit an in-memory cache decouples the
# two completely, and also means N open dashboards cost the control server
# one poll every couple seconds, not N.
_control_cache_lock = threading.Lock()
_control_cache: dict = {"ok": False, "available": False, "error": "not checked yet"}


_CONTROL_TIMEOUT = httpx.Timeout(connect=0.5, read=1.5, write=1.5, pool=1.5)


def _fetch_control_status_live() -> dict:
    try:
        r = httpx.get(f"{LAUNCHER_CONTROL_URL}/control/status", timeout=_CONTROL_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        data["available"] = True
        return data
    except Exception as e:
        return {"ok": False, "available": False, "error": str(e)}


def _control_status_cached() -> dict:
    with _control_cache_lock:
        return dict(_control_cache)


def _control_status_poller():
    """Runs for the lifetime of the process. Backs off while the control
    server is unreachable (the common case for a standalone dashboard with no
    bots running) instead of retrying a doomed connection every 2s forever —
    each attempt still costs a real TCP round trip's worth of GIL time even
    though it fails fast, and there's no point paying that continuously for a
    service that isn't running."""
    global _control_cache
    interval = 2.0
    while True:
        result = _fetch_control_status_live()
        previous = _control_status_cached()
        # Uptime changes while the launcher CMD and child bot processes are
        # alive, so this also acts as a process heartbeat. Bumping the shared
        # version pushes the new liveness snapshot over /ws/live immediately.
        changed = result != previous
        with _control_cache_lock:
            _control_cache = result
        if changed:
            _bump_state()
        interval = 2.0 if result.get("available") else min(interval * 1.5, 20.0)
        time.sleep(interval)


threading.Thread(target=_control_status_poller, daemon=True).start()


@app.get("/api/control/status")
def control_status():
    return jsonify(_control_status_cached())


# ── Live push (/ws/live) ────────────────────────────────────────────────────
# One WebSocket per open dashboard tab, each running ws_live() below. Every
# mutation above calls _bump_state() right after releasing _lock, which wakes
# every connected socket's _state_cond.wait() immediately — so a card that
# changes on one screen (approved from a phone, say) shows up on every other
# open dashboard within a beat, and the same push is what lets a "Restarting…"
# detector flip to "Idle" live once the bot reconnects, no reload needed.
# Purely additive: the dashboard's HTTP polling (GET /api/requests etc.) still
# works unmodified, so a browser that can't do WebSockets, or a deploy that
# doesn't have flask-sock installed (see the optional import up top), falls
# back to that with zero behavior change — see refresh() in the page's JS.

def _requests_snapshot(pending: bool, limit: int | None = None) -> list[dict]:
    with _lock:
        items = [r for r in _requests.values() if (r["status"] == "pending") == pending]
        items.sort(key=lambda r: r["_seq"], reverse=True)
        if limit:
            items = items[:limit]
        return [{k: v for k, v in r.items() if k != "_seq"} for r in items]


def _full_snapshot() -> dict:
    with _lock:
        mode = _mode
        status = dict(_status)
    return {
        "type": "snapshot",
        "pending": _requests_snapshot(pending=True),
        "history": _requests_snapshot(pending=False, limit=300),
        "status": status,
        "mode": mode,
        "control": _control_status_cached(),
        "bots": {p: _bot_status(p) for p in SELF_MANAGED_PLATFORMS},
    }


if sock:
    @sock.route("/ws/live")
    def ws_live(ws):
        last_version = None
        try:
            while True:
                with _state_cond:
                    changed = last_version is None or _state_version != last_version
                    if not changed:
                        # A periodic snapshot is the process-liveness heartbeat
                        # for bots launched from /bots. Their Popen state can
                        # change without an HTTP status mutation to wake us.
                        _state_cond.wait(timeout=5.0)
                        changed = _state_version != last_version
                    last_version = _state_version
                ws.send(json.dumps(_full_snapshot()))
        except ConnectionClosed:
            pass
        except Exception:
            pass


@app.post("/api/control/command")
def control_command():
    body = request.get_json(force=True, silent=True) or {}
    cmd = (body.get("cmd") or "").strip()
    if not cmd:
        return jsonify({"ok": False, "error": "cmd is required"}), 400
    try:
        r = httpx.post(
            f"{LAUNCHER_CONTROL_URL}/control/command",
            json={"cmd": cmd, "target": body.get("target") or "all"},
            # fix/checkins/checkinall drive real Chrome tabs and can take a while
            timeout=120,
        )
        return Response(r.content, status=r.status_code, mimetype="application/json")
    except Exception as e:
        return jsonify({"ok": False, "error": f"Launcher control server unreachable at {LAUNCHER_CONTROL_URL}: {e}"}), 502


# ── Dashboard page ──────────────────────────────────────────────────────────

_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<meta name="theme-color" content="#09090b" />
<meta name="apple-mobile-web-app-capable" content="yes" />
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent" />
<meta name="apple-mobile-web-app-title" content="Zenox" />
<link rel="manifest" href="/static/manifest.webmanifest" />
<link rel="apple-touch-icon" href="/static/icons/zenox-180.png" />
<title>Chat Approval Dashboard</title>
<style>
  :root {
    --background: #09090b; --foreground: #fafafa;
    --card: #18181b; --card-foreground: #fafafa;
    --border: #27272a; --input: #27272a;
    --muted: #18181b; --muted-foreground: #a1a1aa;
    --accent: #27272a; --accent-foreground: #fafafa;
    --primary: #6366f1; --primary-foreground: #fafafa;
    --success: #22c55e; --success-foreground: #052e16;
    --destructive: #ef4444; --destructive-foreground: #450a0a;
    --warning: #eab308; --info: #38bdf8; --violet: #a78bfa;
    --ring: #6366f1;
    --radius: 10px;
    --sidebar-w: 248px;
  }
  * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
  html, body { height: 100%; overflow-x: hidden; }
  body {
    margin: 0; background: var(--background); color: var(--foreground);
    font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, Roboto, Arial, sans-serif;
    display: flex; width: 100%;
  }
  body.drawer-open { overflow: hidden; }
  ::selection { background: var(--primary); color: #fff; }

  /* ── Mobile top bar (hidden on desktop) ─────────────────────────────── */
  #mobileBar { display: none; }

  /* ── Sidebar ─────────────────────────────────────────────────────── */
  #sidebar {
    width: var(--sidebar-w); flex: none; height: 100vh; position: sticky; top: 0;
    background: var(--card); border-right: 1px solid var(--border);
    display: flex; flex-direction: column; overflow-y: auto;
  }
  #backdrop { display: none; }
  .brand {
    display: flex; align-items: center; gap: 9px; padding: 18px 18px 14px;
    border-bottom: 1px solid var(--border);
  }
  .brand-dot {
    width: 9px; height: 9px; border-radius: 999px; background: var(--success);
    box-shadow: 0 0 0 3px rgba(34,197,94,.18);
    animation: pulse 2s ease-in-out infinite;
  }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: .4; } }
  .brand-text { font-weight: 600; font-size: 14px; letter-spacing: -.01em; }
  .brand-sub { color: var(--muted-foreground); font-size: 11.5px; margin-left: auto; }

  .mode-box { padding: 14px 18px; border-bottom: 1px solid var(--border); }
  .mode-toggle {
    display: flex; align-items: center; gap: 9px; width: 100%;
    background: var(--muted); border: 1px solid var(--border); border-radius: 9px;
    padding: 9px 11px; cursor: pointer; font: inherit; text-align: left;
    transition: background .15s, border-color .15s;
  }
  .mode-toggle:hover { filter: brightness(1.1); }
  .mode-switch {
    position: relative; flex: none; width: 34px; height: 19px; border-radius: 999px;
    background: var(--border); transition: background .15s;
  }
  .mode-switch::after {
    content: ""; position: absolute; top: 2px; left: 2px; width: 15px; height: 15px;
    border-radius: 999px; background: #fff; transition: transform .15s;
  }
  .mode-toggle.auto .mode-switch { background: var(--destructive); }
  .mode-toggle.auto .mode-switch::after { transform: translateX(15px); }
  .mode-toggle-label { font-size: 13px; font-weight: 650; color: var(--foreground); }
  .mode-toggle.auto .mode-toggle-label { color: var(--destructive); }
  .mode-toggle-hint { margin: 7px 1px 0; font-size: 11px; color: var(--muted-foreground); line-height: 1.45; }
  .sound-toggle {
    margin-top: 10px; width: 100%; display: flex; align-items: center; justify-content: center; gap: 6px;
    background: var(--muted); border: 1px solid var(--border); border-radius: 9px;
    padding: 7px 11px; cursor: pointer; font: inherit; font-size: 12px; font-weight: 600; color: var(--foreground);
    transition: background .15s, border-color .15s;
  }
  .sound-toggle:hover { filter: brightness(1.1); }
  .sound-toggle.muted { color: var(--muted-foreground); }
  .sound-toggle.push-active { color: var(--success); border-color: color-mix(in srgb, var(--success) 50%, var(--border)); }
  .sound-toggle.push-warn { color: var(--warning); }
  nav { padding: 10px; flex: 1; }
  .nav-label {
    font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: .06em;
    color: var(--muted-foreground); padding: 10px 10px 6px;
  }
  .nav-item {
    display: flex; align-items: center; gap: 9px; width: 100%; text-align: left;
    background: transparent; border: none; color: var(--muted-foreground);
    padding: 7px 10px; border-radius: 7px; font: inherit; font-size: 13px;
    cursor: pointer; margin-bottom: 1px; transition: background .12s, color .12s;
  }
  .nav-item:hover { background: var(--accent); color: var(--foreground); }
  .nav-item.has-pending { color: var(--foreground); background: color-mix(in srgb, var(--pc, var(--accent)) 12%, transparent); }
  .nav-dot {
    width: 8px; height: 8px; border-radius: 999px; flex: none;
    background: var(--pc, var(--border));
  }
  .nav-item.has-pending .nav-dot {
    box-shadow: 0 0 0 3px color-mix(in srgb, var(--pc, var(--warning)) 30%, transparent);
  }
  .nav-count {
    margin-left: auto; font-size: 11px; font-weight: 600; color: var(--muted-foreground);
    background: var(--accent); border-radius: 999px; min-width: 20px; text-align: center;
    padding: 1px 6px;
  }
  .nav-item.has-pending .nav-count { background: var(--pc, var(--primary)); color: #fff; }
  .nav-detector {
    font-size: 10.5px; color: var(--muted-foreground); padding: 0 10px 6px 27px;
    margin-top: -3px; margin-bottom: 2px; display: flex; align-items: center; gap: 5px;
  }
  .nav-detector .det-dot { width: 6px; height: 6px; border-radius: 999px; flex: none; background: var(--border); }
  .nav-detector.live .det-dot { background: var(--det, var(--success)); box-shadow: 0 0 0 2px color-mix(in srgb, var(--det, var(--success)) 30%, transparent); animation: pulse 2s ease-in-out infinite; }
  .nav-detector.offline { opacity: .55; }
  .detector-pill {
    display: inline-flex; align-items: center; gap: 6px; font-size: 11px; font-weight: 600;
    padding: 3px 10px; border-radius: 999px; background: var(--accent); color: var(--muted-foreground);
    border: 1px solid var(--border);
  }
  .detector-pill .det-dot { width: 7px; height: 7px; border-radius: 999px; background: var(--border); flex: none; }
  .detector-pill.live { color: var(--foreground); background: color-mix(in srgb, var(--det, var(--accent)) 16%, var(--card)); border-color: color-mix(in srgb, var(--det, var(--border)) 45%, transparent); }
  .detector-pill.live .det-dot { background: var(--det, var(--success)); box-shadow: 0 0 0 2px color-mix(in srgb, var(--det, var(--success)) 30%, transparent); animation: pulse 2s ease-in-out infinite; }
  .detector-pill.offline { opacity: .6; }
  .workflow-status {
    margin: 8px 0 14px; padding: 10px 12px; border: 1px solid var(--border);
    border-radius: 10px; background: color-mix(in srgb, var(--card) 88%, transparent);
  }
  .workflow-steps { display: flex; align-items: center; gap: 5px; flex-wrap: wrap; }
  .workflow-step {
    font-size: 10.5px; font-weight: 650; color: var(--muted-foreground);
    padding: 3px 7px; border-radius: 999px; border: 1px solid var(--border);
  }
  .workflow-step.done { color: var(--success); border-color: color-mix(in srgb, var(--success) 38%, var(--border)); }
  .workflow-step.active { color: var(--foreground); background: color-mix(in srgb, var(--pc) 20%, var(--accent)); border-color: var(--pc); }
  .workflow-arrow { color: var(--muted-foreground); font-size: 10px; }
  .workflow-detail { margin-top: 7px; font-size: 11.5px; color: var(--muted-foreground); display: flex; gap: 10px; flex-wrap: wrap; }
  .workflow-warning { color: var(--warning); }
  .workflow-checkpoint { color: var(--info); }
  .sidebar-foot {
    padding: 12px 18px; border-top: 1px solid var(--border);
    color: var(--muted-foreground); font-size: 11.5px;
  }

  /* ── Main ────────────────────────────────────────────────────────── */
  #main { flex: 1; min-width: 0; padding: 26px 32px 60px; max-width: 1280px; }
  .page-head { margin-bottom: 22px; }
  .page-head h1 { font-size: 20px; margin: 0 0 4px; letter-spacing: -.01em; }
  .page-head p { margin: 0; color: var(--muted-foreground); font-size: 13.5px; }

  .test-box {
    background: var(--card); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 16px 18px; margin-bottom: 26px;
  }
  .test-box-head { display: flex; align-items: baseline; gap: 8px; margin-bottom: 3px; flex-wrap: wrap; }
  .test-box-head h2 { font-size: 14.5px; margin: 0; font-weight: 650; }
  .test-box-sub { color: var(--muted-foreground); font-size: 12px; }
  textarea.test-input {
    width: 100%; min-height: 64px; resize: vertical; margin-top: 10px;
    background: var(--muted); color: var(--foreground); border: 1px solid var(--input);
    border-radius: 8px; padding: 9px 11px; font: inherit; font-size: 13.5px;
  }
  textarea.test-input:focus { outline: none; border-color: var(--ring); box-shadow: 0 0 0 3px rgba(99,102,241,.22); }
  .test-actions { display: flex; align-items: center; gap: 10px; margin-top: 10px; }
  .btn-test { background: var(--primary); color: #fff; }
  .btn-test:disabled { opacity: .6; cursor: default; }
  .test-status { color: var(--muted-foreground); font-size: 12.5px; }
  .test-result { margin-top: 14px; padding-top: 14px; border-top: 1px solid var(--border); }
  .test-error {
    color: var(--destructive); background: rgba(239,68,68,.1); border: 1px solid rgba(239,68,68,.3);
    border-radius: 8px; padding: 9px 12px; font-size: 13px;
  }
  .test-ok { color: var(--success); font-size: 12.5px; font-weight: 650; margin-bottom: 10px; }
  .test-row { display: flex; align-items: center; gap: 9px; margin-bottom: 8px; font-size: 13px; }
  .test-label { color: var(--muted-foreground); min-width: 128px; }
  .pill { font-size: 11px; font-weight: 700; padding: 2px 9px; border-radius: 999px; text-transform: uppercase; letter-spacing: .03em; }
  .pill.yes { background: rgba(234,179,8,.15); color: var(--warning); }
  .pill.no { background: rgba(34,197,94,.15); color: var(--success); }
  .pill.action-approve { background: rgba(34,197,94,.15); color: var(--success); }
  .pill.action-procrastinate, .pill.action-reschedule { background: rgba(234,179,8,.15); color: var(--warning); }
  .pill.action-decline { background: rgba(239,68,68,.15); color: var(--destructive); }
  .pill.type-dia { background: var(--accent); color: var(--muted-foreground); }
  .pill.type-asa { background: rgba(167,139,250,.15); color: var(--violet); }
  .test-reply-box {
    background: var(--muted); border: 1px solid var(--border); border-radius: 8px;
    padding: 9px 11px; white-space: pre-wrap; font-size: 13.5px; margin-top: 2px;
  }

  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 30px; }
  .stat-card {
    background: var(--card); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 14px 16px;
  }
  .stat-card .label { font-size: 12px; color: var(--muted-foreground); margin-bottom: 6px; }
  .stat-card .value { font-size: 24px; font-weight: 650; letter-spacing: -.02em; }
  .stat-card.warn .value { color: var(--warning); }
  .stat-card.ok .value { color: var(--success); }
  .stat-card.bad .value { color: var(--destructive); }

  .platform-section { margin-bottom: 34px; scroll-margin-top: 18px; }
  .platform-head { display: flex; align-items: center; gap: 10px; margin-bottom: 14px; }
  .pc-dot {
    width: 10px; height: 10px; border-radius: 999px; flex: none;
    background: var(--pc, var(--muted-foreground));
    box-shadow: 0 0 0 3px color-mix(in srgb, var(--pc, transparent) 25%, transparent);
  }
  .platform-head h2 { font-size: 15.5px; margin: 0; font-weight: 650; color: var(--pc, var(--foreground)); }
  .count-pill {
    font-size: 11px; font-weight: 600; padding: 2px 9px; border-radius: 999px;
    background: var(--accent); color: var(--muted-foreground); border: 1px solid var(--border);
  }
  .count-pill.active { background: rgba(234,179,8,.15); color: var(--warning); border-color: rgba(234,179,8,.3); }
  .platform-head hr {
    flex: 1; border: none; height: 1px; background: var(--border);
    background: linear-gradient(90deg, color-mix(in srgb, var(--pc, var(--border)) 55%, transparent), transparent);
  }

  .empty-state {
    color: var(--muted-foreground); padding: 22px; text-align: center;
    border: 1px dashed var(--border); border-radius: var(--radius); font-size: 13px;
  }

  .cards-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(380px, 1fr)); gap: 14px; }

  .card {
    background: var(--card); border: 1px solid var(--border);
    border-left: 3px solid var(--border);
    border-left-color: color-mix(in srgb, var(--pc, var(--border)) 70%, var(--border));
    border-radius: var(--radius);
    padding: 16px 18px; box-shadow: 0 1px 2px rgba(0,0,0,.25);
  }
  .card-head { display: flex; align-items: center; gap: 8px; margin-bottom: 12px; }
  .badge {
    font-size: 10.5px; font-weight: 700; text-transform: uppercase; letter-spacing: .04em;
    padding: 3px 8px; border-radius: 6px;
    background: var(--accent); color: var(--foreground); border: 1px solid var(--border);
    background: color-mix(in srgb, var(--pc, var(--accent)) 20%, var(--card));
    color: color-mix(in srgb, var(--pc, var(--foreground)) 80%, white);
    border: 1px solid color-mix(in srgb, var(--pc, var(--border)) 45%, transparent);
  }
  .time { color: var(--muted-foreground); font-size: 12px; margin-left: auto; }
  .status-badge { font-size: 10.5px; font-weight: 700; text-transform: uppercase; padding: 3px 8px; border-radius: 6px; letter-spacing: .03em; }
  .status-badge.approved, .status-badge.sent { background: rgba(34,197,94,.15); color: var(--success); }
  .status-badge.rejected, .status-badge.failed { background: rgba(239,68,68,.15); color: var(--destructive); }
  .status-badge.cancelled { background: rgba(161,161,170,.18); color: var(--muted-foreground); }
  .status-badge.skip_requested, .status-badge.skipped, .status-badge.transferred { background: rgba(249,115,22,.16); color: #fb923c; }
  .status-badge.pending { background: rgba(234,179,8,.15); color: var(--warning); }

  .card-divider { height: 1px; background: var(--border); margin: 14px 0; }

  .auto-section-label {
    font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .06em;
    color: var(--muted-foreground); margin: 18px 0 10px;
  }
  .auto-section-label:first-child { margin-top: 0; }
  .guard-row { display: flex; align-items: center; gap: 8px; margin: 12px 0 2px; flex-wrap: wrap; }
  .guard-row .test-label { color: var(--muted-foreground); font-size: 12px; }
  .pill.unchecked { background: var(--accent); color: var(--muted-foreground); }
  .changed-label { color: var(--warning) !important; }

  .field-label {
    display: flex; align-items: center; gap: 0;
    font-size: 10.5px; font-weight: 700; text-transform: uppercase; letter-spacing: .05em;
    margin: 12px 0 6px;
  }
  .field-label:first-of-type { margin-top: 0; }
  .field-label.customer-label { color: var(--info); }
  .field-label.reply-label { color: var(--violet); }
  .lang-tag {
    display: inline-block; font-size: 9.5px; font-weight: 700;
    border-radius: 4px; padding: 0 5px; margin-left: 6px;
    letter-spacing: .03em; vertical-align: 1px;
  }
  .lang-tag.tag-de { color: var(--muted-foreground); border: 1px solid var(--border); }
  .lang-tag.tag-en { color: var(--info); border: 1px solid color-mix(in srgb, var(--info) 45%, transparent); background: rgba(56,189,248,.1); }
  .de-box {
    background: var(--muted); border: 1px solid var(--border); border-radius: 8px;
    padding: 9px 11px; white-space: pre-wrap; font-size: 13.5px;
  }
  .translation-row {
    display: flex; align-items: flex-start; gap: 7px; margin-top: 7px; padding: 2px 2px 0;
  }
  .translation-row .lang-tag { margin-left: 0; margin-top: 1px; flex: none; }
  .en-box {
    color: var(--info); opacity: .85; font-style: italic; font-size: 12.5px;
    white-space: pre-wrap; flex: 1;
  }

  .extracted-data { margin-top: 12px; }
  .extracted-data summary {
    cursor: pointer; font-size: 10.5px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .05em; color: var(--warning); list-style: none;
  }
  .extracted-data summary::-webkit-details-marker { display: none; }
  .extracted-data summary::before { content: "▸ "; }
  .extracted-data[open] summary::before { content: "▾ "; }
  .extracted-data summary .hint-inline { color: var(--muted-foreground); text-transform: none; font-weight: 400; letter-spacing: 0; }
  .profile-cols { display: flex; gap: 14px; flex-wrap: wrap; margin-top: 10px; }
  .profile-col { flex: 1 1 220px; min-width: 220px; }
  .profile-col .field-label { margin: 0 0 4px; color: var(--muted-foreground); }
  .profile-row { display: flex; gap: 8px; padding: 5px 0; font-size: 12.5px; border-bottom: 1px solid var(--border); }
  .profile-row .k { color: var(--muted-foreground); min-width: 110px; flex: none; }
  .profile-empty { font-size: 12px; color: var(--muted-foreground); font-style: italic; padding: 4px 0; }

  textarea.reply-input {
    width: 100%; min-height: 92px; resize: vertical;
    background: var(--muted); color: var(--foreground); border: 1px solid var(--input);
    border-radius: 8px; padding: 10px 11px; font: inherit; font-size: 13.5px;
  }
  textarea.reply-input:focus {
    outline: none; border-color: var(--pc, var(--ring));
    box-shadow: 0 0 0 3px color-mix(in srgb, var(--pc, var(--ring)) 22%, transparent);
  }

  .actions { display: flex; align-items: center; gap: 8px; margin-top: 13px; flex-wrap: wrap; }
  button {
    font: inherit; font-weight: 600; font-size: 13px; border: 1px solid transparent;
    border-radius: 7px; padding: 8px 14px; cursor: pointer; transition: filter .12s, background .12s;
  }
  button:hover { filter: brightness(1.08); }
  button:active { transform: translateY(1px); }
  .btn-approve { background: var(--success); color: var(--success-foreground); }
  .btn-reject { background: transparent; color: var(--destructive); border-color: rgba(239,68,68,.4); }
  .btn-reject:hover { background: rgba(239,68,68,.1); }
  .btn-cancel { background: transparent; color: var(--muted-foreground); border-color: var(--border); }
  .btn-cancel:hover { background: var(--accent); color: var(--foreground); }
  .btn-skip { background: transparent; color: #fb923c; border-color: rgba(249,115,22,.45); }
  .btn-skip:hover { background: rgba(249,115,22,.12); }
  .hint { color: var(--muted-foreground); font-size: 11.5px; }

  .history-list { display: flex; flex-direction: column; gap: 8px; }
  .history-card {
    background: var(--card); border: 1px solid var(--border);
    border-left: 3px solid color-mix(in srgb, var(--pc, var(--border)) 60%, var(--border));
    border-radius: 9px; padding: 10px 14px;
  }
  .history-card .card-head { margin-bottom: 6px; }
  .history-reply { font-size: 13px; white-space: pre-wrap; }
  .history-en { color: var(--info); opacity: .8; font-style: italic; font-size: 12px; margin-top: 3px; white-space: pre-wrap; }
  .history-error { color: var(--destructive); font-size: 12px; margin-top: 5px; }

  /* ── Bot controls (restart/fix/etc — proxied to launch_all.py) ──────── */
  .system-ctrl-box {
    background: var(--card); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 14px 18px; margin-bottom: 26px;
  }
  .system-ctrl-head { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-bottom: 4px; }
  .system-ctrl-head h2 { font-size: 14.5px; margin: 0; font-weight: 650; }
  .system-ctrl-sub { color: var(--muted-foreground); font-size: 12px; }
  .system-ctrl-actions { display: flex; align-items: center; gap: 10px; margin-top: 10px; flex-wrap: wrap; }
  .btn-money { background: var(--warning); color: #1c1500; }
  .system-ctrl-status { color: var(--muted-foreground); font-size: 12.5px; }
  .ctrl-unavailable {
    color: var(--muted-foreground); font-size: 12px; background: var(--muted);
    border: 1px dashed var(--border); border-radius: 8px; padding: 8px 11px; margin-top: 10px;
  }

  .ctrl-bar { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin: -4px 0 14px; }
  .ctrl-btn {
    font-size: 11.5px; font-weight: 650; padding: 5px 10px; border-radius: 6px;
    background: var(--accent); color: var(--foreground); border: 1px solid var(--border);
    cursor: pointer; transition: filter .12s;
  }
  .ctrl-btn:hover { filter: brightness(1.15); }
  .ctrl-btn:disabled { opacity: .55; cursor: default; }
  .ctrl-btn.danger { color: var(--destructive); border-color: rgba(239,68,68,.35); }
  .ctrl-btn.primary { background: var(--primary); color: #fff; border-color: transparent; }
  .ctrl-status-pill {
    font-size: 10.5px; font-weight: 700; padding: 2px 9px; border-radius: 999px;
    border: 1px solid var(--border); color: var(--muted-foreground); letter-spacing: .02em;
  }
  .ctrl-status-pill.running { color: var(--success); border-color: rgba(34,197,94,.3); background: rgba(34,197,94,.1); }
  .ctrl-status-pill.dead    { color: var(--destructive); border-color: rgba(239,68,68,.3); background: rgba(239,68,68,.1); }
  .ctrl-output {
    display: none; background: var(--muted); border: 1px solid var(--border); border-radius: 8px;
    padding: 10px 12px; margin: -6px 0 14px; max-height: 280px; overflow-y: auto;
  }
  .ctrl-output.show { display: block; }
  .ctrl-output-head { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; font-size: 11px; font-weight: 700; color: var(--muted-foreground); text-transform: uppercase; letter-spacing: .04em; }
  .ctrl-output-head .close-x {
    margin-left: auto; cursor: pointer; color: var(--muted-foreground); background: none;
    border: none; font-size: 14px; padding: 0 4px; font-weight: 400;
  }
  .ctrl-output-body {
    font-size: 12px; white-space: pre-wrap; font-family: ui-monospace, "SF Mono", Consolas, monospace;
  }

  /* ── Mobile / small screens ─────────────────────────────────────────
     Below 860px the sidebar becomes a slide-in drawer (opened via the
     hamburger in the mobile top bar) instead of a permanent column, and
     layouts that assumed side-by-side space collapse to a single column. */
  @media (max-width: 860px) {
    body { display: block; }

    #mobileBar {
      display: flex; align-items: center; gap: 12px;
      position: sticky; top: 0; z-index: 30;
      padding: 12px 14px; background: var(--card); border-bottom: 1px solid var(--border);
    }
    #hamburger {
      display: flex; flex-direction: column; justify-content: center; gap: 4px;
      width: 38px; height: 38px; padding: 0; border-radius: 8px;
      background: var(--accent); border: 1px solid var(--border); cursor: pointer;
    }
    #hamburger span { display: block; width: 16px; height: 2px; background: var(--foreground); margin: 0 auto; border-radius: 2px; }
    #mobileBar .brand-text { font-weight: 600; font-size: 14.5px; }
    #mobileBar .brand-dot { width: 8px; height: 8px; border-radius: 999px; background: var(--success); box-shadow: 0 0 0 3px rgba(34,197,94,.18); }
    #mobileBar .mobile-pending {
      margin-left: auto; font-size: 12px; font-weight: 700; color: var(--warning);
      background: rgba(234,179,8,.15); border-radius: 999px; padding: 3px 10px;
    }

    #sidebar {
      position: fixed; top: 0; left: 0; z-index: 50; width: min(84vw, 300px);
      transform: translateX(-100%); transition: transform .22s ease;
      box-shadow: 8px 0 24px rgba(0,0,0,.4);
    }
    #sidebar.open { transform: translateX(0); }
    #backdrop.open {
      display: block; position: fixed; inset: 0; z-index: 40;
      background: rgba(0,0,0,.55); backdrop-filter: blur(1px);
    }

    #main { padding: 16px 14px 48px; }
    .page-head h1 { font-size: 18px; }
    .page-head p { font-size: 13px; }

    .stats { grid-template-columns: repeat(2, 1fr); gap: 8px; margin-bottom: 22px; }
    .stat-card { padding: 12px 13px; }
    .stat-card .value { font-size: 20px; }

    .cards-grid { grid-template-columns: 1fr; }
    .card { padding: 14px; }

    .platform-head h2 { font-size: 14.5px; }

    /* Full-width, stacked action buttons are far easier to hit accurately
       with a thumb than two small side-by-side buttons. */
    .actions { flex-direction: column; align-items: stretch; }
    .actions button { width: 100%; padding: 12px 14px; font-size: 14px; }
    .hint { order: 3; text-align: center; }

    /* iOS Safari auto-zooms the page when a focused input's font is under
       16px — keep the textarea at 16px so approving on a phone doesn't
       trigger an unwanted zoom-in. */
    textarea.reply-input { font-size: 16px; min-height: 100px; }

    .nav-item { padding: 10px 12px; font-size: 14px; }
  }

  /* ── Toasts ──────────────────────────────────────────────────────────── */
  #toastRoot {
    position: fixed; z-index: 200; right: 16px; bottom: 16px;
    display: flex; flex-direction: column; gap: 8px; width: min(360px, calc(100vw - 32px));
    pointer-events: none;
  }
  .toast {
    pointer-events: auto; display: flex; align-items: flex-start; gap: 10px;
    background: var(--card); border: 1px solid var(--border); border-left: 3px solid var(--info);
    border-radius: var(--radius); padding: 11px 12px; box-shadow: 0 8px 24px rgba(0,0,0,.35);
    animation: toast-in .18s ease-out;
  }
  .toast.leaving { animation: toast-out .16s ease-in forwards; }
  .toast.success { border-left-color: var(--success); }
  .toast.error   { border-left-color: var(--destructive); }
  .toast.warning { border-left-color: var(--warning); }
  .toast-icon { flex: none; width: 16px; height: 16px; margin-top: 1px; }
  .toast-body { flex: 1; min-width: 0; }
  .toast-title { font-size: 13px; font-weight: 600; color: var(--foreground); }
  .toast-detail { font-size: 12px; color: var(--muted-foreground); margin-top: 2px; }
  .toast-close {
    flex: none; background: none; border: none; color: var(--muted-foreground);
    cursor: pointer; font-size: 14px; padding: 2px; line-height: 1;
  }
  .toast-close:hover { color: var(--foreground); }
  @keyframes toast-in { from { opacity: 0; transform: translateY(6px) scale(.98); } to { opacity: 1; transform: none; } }
  @keyframes toast-out { from { opacity: 1; } to { opacity: 0; transform: translateY(-4px); } }

  /* ── Confirm modal (replaces window.confirm) ────────────────────────── */
  #modalRoot {
    display: none; position: fixed; inset: 0; z-index: 210;
    align-items: center; justify-content: center; padding: 20px;
    background: rgba(0,0,0,.55); backdrop-filter: blur(1px);
  }
  #modalRoot.open { display: flex; }
  .modal-box {
    width: min(400px, 100%); background: var(--card); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 18px; box-shadow: 0 20px 50px rgba(0,0,0,.5);
    animation: toast-in .16s ease-out;
  }
  .modal-title { font-size: 15px; font-weight: 650; margin: 0 0 6px; }
  .modal-body { font-size: 13.5px; color: var(--muted-foreground); line-height: 1.5; }
  .modal-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 18px; }
  .modal-actions button {
    font: inherit; font-size: 13px; font-weight: 600; padding: 8px 14px;
    border-radius: 8px; cursor: pointer; border: 1px solid var(--border);
    background: var(--accent); color: var(--foreground);
  }
  .modal-actions .modal-confirm { background: var(--primary); border-color: transparent; color: #fff; }
  .modal-actions .modal-confirm.danger { background: var(--destructive); }

  /* ── Small reusable spinner (buttons show this instead of static "…") ── */
  .spinner {
    display: inline-block; width: 12px; height: 12px; border-radius: 999px;
    border: 2px solid currentColor; border-right-color: transparent; opacity: .8;
    animation: spin .6s linear infinite; vertical-align: -2px;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
  button:disabled { opacity: .68; cursor: default; }

  /* ── Live-connection badge (sidebar) ────────────────────────────────── */
  .live-badge {
    display: inline-flex; align-items: center; gap: 6px; font-size: 11px; font-weight: 600;
    padding: 4px 9px; border-radius: 999px; border: 1px solid var(--border); background: var(--accent);
    color: var(--muted-foreground); margin-top: 8px;
  }
  .live-badge .dot { width: 6px; height: 6px; border-radius: 999px; background: var(--border); flex: none; }
  .live-badge.live { color: var(--success); border-color: color-mix(in srgb, var(--success) 40%, var(--border)); }
  .live-badge.live .dot { background: var(--success); box-shadow: 0 0 0 2px color-mix(in srgb, var(--success) 30%, transparent); animation: pulse 2s ease-in-out infinite; }
  .live-badge.polling { color: var(--warning); }
  .live-badge.polling .dot { background: var(--warning); }

  /* ── System status strip (top of main content) ──────────────────────── */
  .system-status {
    display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 18px;
  }
  .system-status .chip {
    display: inline-flex; align-items: center; gap: 7px; font-size: 12px; font-weight: 600;
    padding: 6px 11px; border-radius: 999px; border: 1px solid var(--border); background: var(--card);
    color: var(--muted-foreground);
  }
  .system-status .chip .dot { width: 7px; height: 7px; border-radius: 999px; background: var(--border); flex: none; }
  .system-status .chip.ok { color: var(--success); }
  .system-status .chip.ok .dot { background: var(--success); box-shadow: 0 0 0 2px color-mix(in srgb, var(--success) 30%, transparent); }
  .system-status .chip.warn { color: var(--warning); }
  .system-status .chip.warn .dot { background: var(--warning); animation: pulse 1.4s ease-in-out infinite; }
  .system-status .chip.bad { color: var(--destructive); }
  .system-status .chip.bad .dot { background: var(--destructive); }
</style>
</head>
<body>
<div id="toastRoot" aria-live="polite"></div>
<div id="modalRoot"></div>
<header id="mobileBar">
  <button id="hamburger" onclick="toggleDrawer()" aria-label="Toggle platform menu"><span></span><span></span><span></span></button>
  <span class="brand-dot"></span>
  <span class="brand-text">Chat Approval</span>
  <span class="mobile-pending" id="mobilePendingBadge">0 pending</span>
</header>
<div id="backdrop" onclick="closeDrawer()"></div>

<aside id="sidebar">
  <div class="brand">
    <span class="brand-dot"></span>
    <span class="brand-text">Chat Approval</span>
    <span class="brand-sub">live</span>
  </div>
  <div class="mode-box">
    <button id="modeToggle" class="mode-toggle" onclick="toggleMode()">
      <span class="mode-switch"></span>
      <span class="mode-toggle-label" id="modeToggleLabel">Manual Review</span>
    </button>
    <p class="mode-toggle-hint" id="modeToggleHint">Every reply waits here for Approve, Reject or Cancel.</p>
    <button id="soundToggle" class="sound-toggle" onclick="toggleSound()">🔔 Sound on</button>
    <button id="pushToggle" class="sound-toggle" onclick="togglePushNotifications()">📲 Enable phone notifications</button>
    <a href="/bots" class="sound-toggle" style="margin-top:8px;text-decoration:none;">🤖 Bots (start / stop / configure)</a>
    <span class="live-badge" id="liveBadge" title="How this dashboard is getting updates"><span class="dot"></span><span id="liveBadgeLabel">Connecting…</span></span>
  </div>
  <nav>
    <div class="nav-label">Platforms</div>
    <div id="navList"></div>
  </nav>
  <div class="sidebar-foot">Replies wait here until approved.</div>
</aside>

<main id="main">
  <div class="page-head">
    <h1>Approval Queue</h1>
    <p>Review every AI reply before it's pasted and sent — German is what actually goes out, English is a translation for review.</p>
  </div>

  <div class="system-status" id="systemStatus"></div>

  <div class="test-box">
    <div class="test-box-head">
      <h2>Test AI Key</h2>
      <span class="test-box-sub">Checks the Groq API key works, and shows how it classifies a sample message as meeting-related or not — separate from the bots, nothing here gets sent.</span>
    </div>
    <textarea id="testMessageInput" class="test-input" placeholder="Paste a sample incoming message, e.g. &quot;Are you free Thursday at 3pm?&quot;"></textarea>
    <div class="test-actions">
      <button class="btn-test" id="testRunBtn" onclick="runGroqTest()">Test</button>
      <span class="test-status" id="testStatus"></span>
    </div>
    <div class="test-result" id="testResult" style="display:none"></div>
  </div>

  <div class="system-ctrl-box">
    <div class="system-ctrl-head">
      <h2>Bot Controls</h2>
      <span class="test-box-sub">Same commands as launch_all.py's terminal — restart/fix per platform below, or check total earnings across every account here.</span>
    </div>
    <div class="system-ctrl-actions">
      <button class="btn-money" id="checkinAllBtn" onclick="runCheckinAll()">Check-in All (money)</button>
      <span class="system-ctrl-status" id="checkinAllStatus"></span>
    </div>
    <div class="ctrl-output" id="checkinAllOutput">
      <div class="ctrl-output-head"><b>checkinall</b><button class="close-x" onclick="document.getElementById('checkinAllOutput').classList.remove('show')">✕</button></div>
      <div class="ctrl-output-body" id="checkinAllOutputBody"></div>
    </div>
    <div class="ctrl-unavailable" id="ctrlUnavailableNote" style="display:none">
      Bot controls unavailable — launch_all.py's control API isn't reachable. Start the bots with <code>python launch_all.py</code> on this machine to enable Restart/Fix/Checkinall buttons.
    </div>
  </div>

  <div class="stats">
    <div class="stat-card warn"><div class="label">Pending review</div><div class="value" id="statPending">0</div></div>
    <div class="stat-card ok"><div class="label">Sent today</div><div class="value" id="statSent">0</div></div>
    <div class="stat-card bad"><div class="label">Rejected today</div><div class="value" id="statRejected">0</div></div>
    <div class="stat-card"><div class="label">Platforms active</div><div class="value" id="statPlatforms">0</div></div>
  </div>

  <div id="sections"></div>

  <div class="platform-section">
    <div class="platform-head"><h2>Recent activity</h2><hr /></div>
    <div id="history" class="history-list"><div class="empty-state">Nothing yet.</div></div>
  </div>
</main>

<script>
const escapeHtml = (s) => (s || "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));

// ── Toasts (replaces window.alert) ──────────────────────────────────────
function toast(title, opts) {
  opts = opts || {};
  const type = opts.type || "info";
  const detail = opts.detail || "";
  const ms = opts.duration != null ? opts.duration : 4200;
  const root = document.getElementById("toastRoot");
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.innerHTML = `
    <div class="toast-body">
      <div class="toast-title"></div>
      ${detail ? `<div class="toast-detail"></div>` : ""}
    </div>
    <button class="toast-close" aria-label="Dismiss">✕</button>
  `;
  el.querySelector(".toast-title").textContent = title;
  if (detail) el.querySelector(".toast-detail").textContent = detail;
  const remove = () => { el.classList.add("leaving"); setTimeout(() => el.remove(), 170); };
  el.querySelector(".toast-close").onclick = remove;
  root.appendChild(el);
  if (ms) setTimeout(remove, ms);
  return remove;
}

// ── Confirm modal (replaces window.confirm, matches the rest of the UI) ──
function showConfirm(title, body, opts) {
  opts = opts || {};
  return new Promise((resolve) => {
    const root = document.getElementById("modalRoot");
    root.innerHTML = `
      <div class="modal-box">
        <p class="modal-title"></p>
        <p class="modal-body"></p>
        <div class="modal-actions">
          <button class="modal-cancel">${escapeHtml(opts.cancelLabel || "Cancel")}</button>
          <button class="modal-confirm ${opts.danger ? "danger" : ""}">${escapeHtml(opts.confirmLabel || "Confirm")}</button>
        </div>
      </div>
    `;
    root.querySelector(".modal-title").textContent = title;
    root.querySelector(".modal-body").textContent = body;
    const close = (result) => {
      root.classList.remove("open");
      root.innerHTML = "";
      document.removeEventListener("keydown", onKey);
      resolve(result);
    };
    const onKey = (e) => { if (e.key === "Escape") close(false); };
    document.addEventListener("keydown", onKey);
    root.querySelector(".modal-cancel").onclick = () => close(false);
    root.querySelector(".modal-confirm").onclick = () => close(true);
    root.onclick = (e) => { if (e.target === root) close(false); };
    root.classList.add("open");
  });
}

// ── Mobile drawer (sidebar becomes a slide-in panel below 860px) ───────────
function openDrawer() {
  document.getElementById("sidebar").classList.add("open");
  document.getElementById("backdrop").classList.add("open");
  document.body.classList.add("drawer-open");
}
function closeDrawer() {
  document.getElementById("sidebar").classList.remove("open");
  document.getElementById("backdrop").classList.remove("open");
  document.body.classList.remove("drawer-open");
}
function toggleDrawer() {
  document.getElementById("sidebar").classList.contains("open") ? closeDrawer() : openDrawer();
}
document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeDrawer(); });

function goToSection(name) {
  document.getElementById(slug(name)).scrollIntoView({ behavior: "smooth", block: "start" });
  closeDrawer();
}
const slug = (s) => "plat-" + (s || "unknown").toLowerCase().replace(/[^a-z0-9]+/g, "-");

// Edits the reviewer has typed are kept here (keyed by request id) so a
// background refresh can never silently wipe out in-progress wording changes.
const editedReplies = new Map();
let knownPlatforms = [];
let currentMode = "manual";
let serviceWorkerRegistration = null;
let currentPushSubscription = null;
let pushConfigured = false;

// --- Notification sound: chimes whenever a new chat starts waiting for approval. ---
let soundEnabled = localStorage.getItem("approvalSoundEnabled") !== "0";
let audioCtx = null;
// null until the first poll completes, so we don't chime for requests that
// were already pending when the page loaded.
let knownPendingIds = null;

function unlockAudio() {
  try {
    if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    if (audioCtx.state === "suspended") audioCtx.resume();
  } catch (e) {
    // Web Audio unavailable — sound stays silently off.
  }
}
document.addEventListener("pointerdown", unlockAudio, { once: true });
document.addEventListener("keydown", unlockAudio, { once: true });

function playNotifySound() {
  if (!soundEnabled) return;
  try {
    unlockAudio();
    if (!audioCtx) return;
    const now = audioCtx.currentTime;
    const notes = [880, 1108.73]; // short two-tone chime (A5, C#6)
    notes.forEach((freq, i) => {
      const osc = audioCtx.createOscillator();
      const gain = audioCtx.createGain();
      osc.type = "sine";
      osc.frequency.value = freq;
      const start = now + i * 0.14;
      gain.gain.setValueAtTime(0, start);
      gain.gain.linearRampToValueAtTime(0.25, start + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.0001, start + 0.35);
      osc.connect(gain).connect(audioCtx.destination);
      osc.start(start);
      osc.stop(start + 0.4);
    });
  } catch (e) {
    // transient audio glitch — not worth surfacing to the user
  }
}

function renderSoundToggle() {
  const btn = document.getElementById("soundToggle");
  if (!btn) return;
  btn.classList.toggle("muted", !soundEnabled);
  btn.textContent = soundEnabled ? "🔔 Sound on" : "🔕 Sound off";
}

function toggleSound() {
  soundEnabled = !soundEnabled;
  localStorage.setItem("approvalSoundEnabled", soundEnabled ? "1" : "0");
  unlockAudio();
  if (soundEnabled) playNotifySound();
  renderSoundToggle();
}

// ── Installable app + background Web Push notifications ────────────────
function base64UrlToBytes(value) {
  const padding = "=".repeat((4 - value.length % 4) % 4);
  const raw = atob((value + padding).replace(/-/g, "+").replace(/_/g, "/"));
  return Uint8Array.from([...raw].map(char => char.charCodeAt(0)));
}

function isStandaloneApp() {
  return window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
}

function isIOS() {
  return /iphone|ipad|ipod/i.test(navigator.userAgent);
}

function renderPushToggle(state, detail) {
  const btn = document.getElementById("pushToggle");
  if (!btn) return;
  btn.disabled = state === "unsupported" || state === "unconfigured";
  btn.classList.toggle("push-active", state === "enabled");
  btn.classList.toggle("push-warn", state === "needs-install" || state === "denied");
  const labels = {
    enabled: "✅ Phone notifications on",
    disabled: "📲 Enable phone notifications",
    "needs-install": "➕ Add app to Home Screen first",
    denied: "🚫 Notifications blocked in Settings",
    unconfigured: "⚠️ Push server not configured",
    unsupported: "Notifications unavailable",
  };
  btn.textContent = labels[state] || "📲 Enable phone notifications";
  btn.title = detail || "";
}

async function initPushNotifications() {
  if (!("serviceWorker" in navigator) || !("PushManager" in window) || !("Notification" in window)) {
    renderPushToggle("unsupported", "This browser does not support Web Push.");
    return;
  }
  if (isIOS() && !isStandaloneApp()) {
    renderPushToggle("needs-install", "In Safari, use Share → Add to Home Screen, then open the Zenox icon.");
    return;
  }
  try {
    const configRes = await fetch("/api/push/config");
    const config = await configRes.json();
    pushConfigured = !!config.enabled && !!config.public_key;
    if (!pushConfigured) {
      renderPushToggle("unconfigured", "Restart the dashboard after installing requirements and configuring VAPID.");
      return;
    }
    serviceWorkerRegistration = await navigator.serviceWorker.register("/service-worker.js", { scope: "/" });
    serviceWorkerRegistration = await navigator.serviceWorker.ready;
    currentPushSubscription = await serviceWorkerRegistration.pushManager.getSubscription();
    if (Notification.permission === "denied") renderPushToggle("denied");
    else renderPushToggle(currentPushSubscription ? "enabled" : "disabled");
  } catch (error) {
    renderPushToggle("unsupported", String(error));
  }
}

async function togglePushNotifications() {
  if (isIOS() && !isStandaloneApp()) {
    toast("Add Zenox to your Home Screen", {
      type: "info",
      detail: "Open this page in Safari, tap Share, choose Add to Home Screen, then open the Zenox icon."
    });
    return;
  }
  if (!pushConfigured || !serviceWorkerRegistration) {
    await initPushNotifications();
    if (!pushConfigured || !serviceWorkerRegistration) return;
  }

  try {
    currentPushSubscription = await serviceWorkerRegistration.pushManager.getSubscription();
    if (currentPushSubscription) {
      const endpoint = currentPushSubscription.endpoint;
      await fetch("/api/push/subscribe", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ endpoint })
      });
      await currentPushSubscription.unsubscribe();
      currentPushSubscription = null;
      renderPushToggle("disabled");
      toast("Phone notifications disabled", { type: "info" });
      return;
    }

    const permission = await Notification.requestPermission();
    if (permission !== "granted") {
      renderPushToggle("denied");
      toast("Notifications were not allowed", {
        type: "error",
        detail: "Enable Zenox in iPhone Settings → Notifications, then try again."
      });
      return;
    }
    const config = await (await fetch("/api/push/config")).json();
    currentPushSubscription = await serviceWorkerRegistration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: base64UrlToBytes(config.public_key)
    });
    const response = await fetch("/api/push/subscribe", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ subscription: currentPushSubscription.toJSON(), send_test: true })
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
    renderPushToggle("enabled");
    toast("Phone notifications enabled", {
      type: "success",
      detail: "A test notification is being sent now."
    });
  } catch (error) {
    renderPushToggle("disabled", String(error));
    toast("Could not enable notifications", { type: "error", detail: String(error) });
  }
}

function renderModeToggle() {
  const btn = document.getElementById("modeToggle");
  const label = document.getElementById("modeToggleLabel");
  const hint = document.getElementById("modeToggleHint");
  const auto = currentMode === "auto";
  btn.classList.toggle("auto", auto);
  label.textContent = auto ? "Auto-pilot" : "Manual Review";
  hint.textContent = auto
    ? "AI replies are sent immediately — nothing waits for review."
    : "Every reply waits here for Approve, Reject or Cancel.";
}

async function toggleMode() {
  const next = currentMode === "auto" ? "manual" : "auto";
  if (next === "auto") {
    const ok = await showConfirm(
      "Switch to Auto-pilot?",
      "Every new AI reply will be sent immediately, with no human review. Replies already waiting in the queue are not affected.",
      { confirmLabel: "Switch to Auto-pilot", danger: true },
    );
    if (!ok) return;
  }
  try {
    const res = await fetch("/api/mode", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: next }),
    });
    if (!res.ok) throw new Error(String(res.status));
    currentMode = next;
    toast(next === "auto" ? "Auto-pilot enabled" : "Manual review enabled", {
      type: next === "auto" ? "warning" : "success",
      detail: next === "auto"
        ? "New replies now send immediately, with no review."
        : "New replies now wait here for Approve, Reject or Cancel.",
    });
  } catch (e) {
    toast("Could not change mode", { type: "error", detail: "Is the approval server reachable?" });
  }
  renderModeToggle();
  renderSystemStatus();
}

const ACTION_LABELS = {
  approve: "Approve as-is",
  procrastinate: "Procrastinate",
  reschedule: "Reschedule",
  decline: "Decline",
};

async function runGroqTest() {
  const input = document.getElementById("testMessageInput");
  const btn = document.getElementById("testRunBtn");
  const statusEl = document.getElementById("testStatus");
  const resultEl = document.getElementById("testResult");
  const message = input.value.trim();

  resultEl.style.display = "none";
  if (!message) {
    statusEl.textContent = "Type a message first.";
    return;
  }

  btn.disabled = true;
  statusEl.textContent = "Testing…";
  try {
    const res = await fetch("/api/test-groq", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message }),
    });
    const data = await res.json().catch(() => ({}));
    resultEl.style.display = "block";

    if (!res.ok || !data.ok) {
      resultEl.innerHTML = `<div class="test-error">Key not working: ${escapeHtml(data.error || `HTTP ${res.status}`)}</div>`;
      return;
    }

    const actionLabel = ACTION_LABELS[data.action] || data.action;
    resultEl.innerHTML = `
      <div class="test-ok">✓ Key works — got a live response from Groq.</div>
      <div class="test-row"><span class="test-label">Contains meeting?</span><span class="pill ${data.contains_meeting ? "yes" : "no"}">${data.contains_meeting ? "Yes" : "No"}</span></div>
      <div class="test-row"><span class="test-label">Action</span><span class="pill action-${escapeHtml(data.action)}">${escapeHtml(actionLabel)}</span></div>
      <div class="test-row"><span class="test-label">Reply</span></div>
      <div class="test-reply-box">${escapeHtml(data.reply)}</div>
    `;
  } catch (e) {
    resultEl.style.display = "block";
    resultEl.innerHTML = `<div class="test-error">Request failed: ${escapeHtml(String(e))}</div>`;
  } finally {
    btn.disabled = false;
    statusEl.textContent = "";
  }
}

function timeAgo(iso) {
  if (!iso) return "";
  const secs = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 1000));
  if (secs < 5) return "just now";
  if (secs < 60) return secs + "s ago";
  if (secs < 3600) return Math.floor(secs / 60) + "m ago";
  return Math.floor(secs / 3600) + "h ago";
}

function isToday(iso) {
  if (!iso) return false;
  const d = new Date(iso), now = new Date();
  return d.getFullYear() === now.getFullYear() && d.getMonth() === now.getMonth() && d.getDate() === now.getDate();
}

const ACTION_MESSAGES = {
  approve: { verb: "Approving", done: "Approved — queued for pasting & sending", type: "success" },
  reject:  { verb: "Regenerating", done: "Rejected — generating a new reply now", type: "info" },
  cancel:  { verb: "Cancelling", done: "Cancelled — the bot will restart this chat", type: "warning" },
  skip:    { verb: "Transferring", done: "Transfer requested — the bot will skip only if nobody is online", type: "warning" },
};

const SKIPPABLE_CARD_PLATFORMS = new Set(["xkuss", "justlo", "linduu", "gnoxx"]);

async function act(id, action, body, btn) {
  // Disable the whole card's action row (not just the clicked button) so a
  // double-click can't fire two decisions on the same request while the
  // first one is still in flight.
  const card = document.querySelector(`.card[data-id="${id}"]`);
  const buttons = card ? card.querySelectorAll(".actions button") : [];
  buttons.forEach(b => { b.disabled = true; });
  const meta = ACTION_MESSAGES[action];
  const original = btn ? btn.innerHTML : null;
  if (btn && meta) btn.innerHTML = `<span class="spinner"></span> ${escapeHtml(meta.verb)}…`;
  try {
    const res = await fetch(`/api/requests/${id}/${action}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      toast(`Could not ${action}`, { type: "error", detail: err.error || `HTTP ${res.status}` });
      buttons.forEach(b => { b.disabled = false; });
      if (btn && original != null) btn.innerHTML = original;
    } else if (meta) {
      toast(meta.done, { type: meta.type, duration: 2600 });
    }
  } catch (e) {
    toast(`Could not ${action}`, { type: "error", detail: "Request failed — check your connection." });
    buttons.forEach(b => { b.disabled = false; });
    if (btn && original != null) btn.innerHTML = original;
  }
  editedReplies.delete(id);
  refresh();
}

function approveCard(id, btn) {
  const ta = document.getElementById(`ta-${id}`);
  act(id, "approve", { edited_reply: ta ? ta.value : undefined }, btn);
}

async function cancelCard(id, btn) {
  const ok = await showConfirm(
    "Cancel this reply?",
    "The bot will abandon it and restart the chat instead of regenerating.",
    { confirmLabel: "Cancel reply", danger: true },
  );
  if (!ok) return;
  act(id, "cancel", null, btn);
}

async function skipConversationCard(id, platform, btn) {
  const ok = await showConfirm(
    platform.toLowerCase() === "xkuss" ? "Skip this conversation?" : "Transfer this conversation?",
    platform.toLowerCase() === "xkuss"
      ? "The bot will leave this Xkuss dialog via Home. No reply will be sent."
      : `The bot will press “Übergeben” in ${platform} and send the conversation to another online moderator. If nobody is online, it will press “Überspringen” instead.`,
    { confirmLabel: platform.toLowerCase() === "xkuss" ? "Skip conversation" : "Transfer / skip", danger: true },
  );
  if (!ok) return;
  act(id, "skip", null, btn);
}

// Human labels + accent per bot state, reported via POST /api/status
// (see report_status() in core/approval.py). Anything not listed here still
// renders (falls back to the raw state string) so a new state added on the
// bot side never breaks the dashboard.
const STATE_META = {
  starting:           { label: "Starting…",           color: "var(--muted-foreground)" },
  waiting:            { label: "Waiting",             color: "var(--muted-foreground)" },
  waiting_for_chat:   { label: "Waiting for a chat",   color: "var(--muted-foreground)" },
  chat_detected:      { label: "Chat detected",        color: "var(--info)" },
  extracting:         { label: "Extracting…",          color: "var(--info)" },
  generating:         { label: "Generating reply…",    color: "var(--violet)" },
  retrying:           { label: "Retrying…",            color: "var(--warning)" },
  approval:           { label: "Awaiting approval",    color: "var(--warning)" },
  awaiting_approval:  { label: "Awaiting approval",    color: "var(--warning)" },
  sending:            { label: "Sending…",              color: "var(--success)" },
  sent:               { label: "Sent",                 color: "var(--success)" },
  idle:               { label: "Idle",                  color: "var(--muted-foreground)" },
  recovering:         { label: "Recovering…",           color: "var(--warning)" },
  restarting:         { label: "Restarting…",            color: "var(--info)" },
  error:              { label: "Error",                 color: "var(--destructive)" },
};
const STATUS_STALE_MS = 45_000; // workflow detail freshness, not process liveness

let liveStatus = {}; // platform -> {state, detail, updated_at}

function detectorFor(name) {
  const s = liveStatus[name];
  const platformSlug = name.toLowerCase();
  const launcherProcess = controlsAvailable ? controlPlatformState[platformSlug] : null;
  const process = launcherProcess || botProcessState[platformSlug] || null;
  const processRunning = process && (process.state === "running" || process.running === true);
  const processStopped = process && (
    process.state === "stopped" || process.state === "dead" || process.running === false
  );
  const updatedMs = s ? new Date(s.updated_at).getTime() : NaN;
  const telemetryFresh = s && Number.isFinite(updatedMs) && Date.now() - updatedMs <= STATUS_STALE_MS;

  // launch_all.py owns the bot subprocesses, so its live process state is the
  // authority for online/offline. Workflow pings only provide the richer
  // Waiting/Generating/etc. label while they are fresh.
  if (processStopped) {
    const label = process.state === "dead" ? "Process stopped" : "Stopped";
    return { live: false, label, color: "var(--border)", detail: "", state: "offline", retryCount: 0, warning: "", checkpoint: "" };
  }
  if (processRunning && !telemetryFresh) {
    return {
      live: true, label: "Running", color: "var(--success)",
      detail: "Bot process is online; waiting for a workflow update", state: "waiting",
      retryCount: 0, warning: "", checkpoint: "",
    };
  }
  if (!telemetryFresh) {
    return { live: false, label: "Offline", color: "var(--border)", detail: "", state: "offline", retryCount: 0, warning: "", checkpoint: "" };
  }
  const meta = STATE_META[s.state] || { label: s.state, color: "var(--info)" };
  return {
    live: true, label: meta.label, color: meta.color, detail: s.detail || "", state: s.state,
    retryCount: Number(s.retry_count || 0), warning: s.warning || "", checkpoint: s.checkpoint || "",
  };
}

const WORKFLOW_STEPS = ["Waiting", "Extracting", "Generating", "Retrying", "Approval", "Sending", "Sent"];
const WORKFLOW_INDEX = {
  starting: 0, waiting: 0, waiting_for_chat: 0, idle: 0, chat_detected: 1,
  extracting: 1, generating: 2, retrying: 3, recovering: 3, restarting: 3, error: 3,
  approval: 4, awaiting_approval: 4, sending: 5, sent: 6,
};

function workflowStatusHtml(det) {
  const active = WORKFLOW_INDEX[det.state] ?? 0;
  const steps = WORKFLOW_STEPS.map((label, i) => {
    const cls = i === active ? "active" : (i < active ? "done" : "");
    const step = `<span class="workflow-step ${cls}">${escapeHtml(label)}</span>`;
    return i ? `<span class="workflow-arrow">→</span>${step}` : step;
  }).join("");
  const details = [];
  if (det.detail) details.push(`<span>${escapeHtml(det.detail)}</span>`);
  if (det.retryCount) details.push(`<span>Retry ${det.retryCount}</span>`);
  if (det.warning) details.push(`<span class="workflow-warning">${escapeHtml(det.warning)}</span>`);
  if (det.checkpoint) details.push(`<span class="workflow-checkpoint">Restart point: ${escapeHtml(det.checkpoint)}</span>`);
  return `<div class="workflow-status"><div class="workflow-steps">${steps}</div>${details.length ? `<div class="workflow-detail">${details.join("")}</div>` : ""}</div>`;
}

const FALLBACK_PALETTE = ["#8b5cf6", "#06b6d4", "#f97316", "#14b8a6", "#a855f7"];
let platformColorMap = new Map();

function colorFor(name) {
  if (platformColorMap.has(name)) return platformColorMap.get(name);
  // Unknown platform seen live — assign a stable color by hashing its name.
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = (hash * 31 + name.charCodeAt(i)) >>> 0;
  const color = FALLBACK_PALETTE[hash % FALLBACK_PALETTE.length];
  platformColorMap.set(name, color);
  return color;
}

// ── Bot controls (restart/fix/checkinall/... — proxied to launch_all.py) ───
// The dashboard's display names ("S69", "Justlo", ...) are just the lowercase
// launch_all.py platform slug ("s69", "justlo", ...) capitalized, so no
// separate name-mapping table is needed here.
let controlsAvailable = false;
let controlPlatformState = {}; // slug -> {state, uptime, crashes, exit_code}
let botProcessState = {}; // dashboard-managed bot Popen/CDP state from /api/bots/status
// Last command output per platform, kept here (not just in the DOM) because
// renderSections() fully rebuilds #sections on every poll — without this the
// panel would vanish again 1.5s after a command finished.
let controlOutputs = new Map(); // slug -> output text

const CTRL_BUTTONS = [
  { cmd: "restart",   label: "Restart" },
  { cmd: "fix",       label: "Fix" },
  { cmd: "chameleon", label: "Chameleon" },
  { cmd: "extractor", label: "Extractor" },
  { cmd: "checkins",  label: "Ins (money)" },
  { cmd: "stop",      label: "Stop", cls: "danger" },
];

async function refreshControlStatus() {
  try {
    const res = await fetch("/api/control/status");
    const data = await res.json();
    controlsAvailable = !!data.available;
    controlPlatformState = data.platforms || {};
  } catch (e) {
    controlsAvailable = false;
    controlPlatformState = {};
  }
}

function fmtUptime(seconds) {
  if (seconds == null) return "";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h${Math.floor((seconds % 3600) / 60)}m`;
}

const CTRL_VERBS = {
  restart: "Restarting", fix: "Running Fix on", chameleon: "Opening Chameleon tab for",
  extractor: "Opening Extractor tab for", checkins: "Checking in", stop: "Stopping",
};

async function runControl(btn, slug, cmd) {
  if (btn.disabled) return;
  btn.disabled = true;
  const original = btn.textContent;
  btn.innerHTML = `<span class="spinner"></span>`;
  // duration:0 -> stays up until we dismiss it ourselves once the command
  // actually finishes, so "what am I doing right now" is never a guess.
  const dismiss = toast(`${CTRL_VERBS[cmd] || "Running " + cmd + " on"} ${slug}…`, { type: "info", duration: 0 });
  try {
    const res = await fetch("/api/control/command", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cmd, target: slug }),
    });
    const data = await res.json().catch(() => ({}));
    dismiss();
    toast(data.ok ? `${slug}: ${cmd} done` : `${slug}: ${cmd} failed`, {
      type: data.ok ? "success" : "error",
      detail: data.ok ? "" : (data.error || `HTTP ${res.status}`),
      duration: data.ok ? 3000 : 6000,
    });
    showControlOutput(slug, data.ok ? (data.output || "(no output)") : `Error: ${data.error || res.status}`);
  } catch (e) {
    dismiss();
    toast(`${slug}: ${cmd} failed`, { type: "error", detail: "Request failed — check your connection." });
    showControlOutput(slug, `Request failed: ${e}`);
  } finally {
    btn.disabled = false;
    btn.textContent = original;
    refresh();
  }
}

function showControlOutput(slug, text) {
  controlOutputs.set(slug, text);
  const box = document.getElementById(`ctrl-out-${slug}`);
  if (!box) return;
  box.querySelector(".ctrl-output-body").textContent = text;
  box.classList.add("show");
}

function closeControlOutput(slug) {
  controlOutputs.delete(slug);
  const box = document.getElementById(`ctrl-out-${slug}`);
  if (box) box.classList.remove("show");
}

function ctrlBarHtml(name) {
  if (!controlsAvailable) return "";
  const slug = name.toLowerCase();
  const st = controlPlatformState[slug];
  let pillHtml = "";
  if (st) {
    const label = st.state === "running"
      ? `RUNNING ${fmtUptime(st.uptime)}${st.crashes ? ` · ${st.crashes} crash${st.crashes === 1 ? "" : "es"}` : ""}`
      : st.state === "dead" ? `DEAD (exit ${st.exit_code})` : "STOPPED";
    pillHtml = `<span class="ctrl-status-pill ${escapeHtml(st.state)}">${escapeHtml(label)}</span>`;
  }
  const buttons = CTRL_BUTTONS.map(b =>
    `<button class="ctrl-btn ${b.cls || ""}" onclick="runControl(this, '${slug}', '${b.cmd}')">${escapeHtml(b.label)}</button>`
  ).join("");
  const savedOutput = controlOutputs.get(slug);
  return `
    <div class="ctrl-bar">
      ${pillHtml}
      ${buttons}
    </div>
    <div class="ctrl-output ${savedOutput != null ? "show" : ""}" id="ctrl-out-${slug}">
      <div class="ctrl-output-head"><b>Output</b><button class="close-x" onclick="closeControlOutput('${slug}')">✕</button></div>
      <div class="ctrl-output-body">${escapeHtml(savedOutput || "")}</div>
    </div>
  `;
}

async function runCheckinAll() {
  const btn = document.getElementById("checkinAllBtn");
  const statusEl = document.getElementById("checkinAllStatus");
  const outEl = document.getElementById("checkinAllOutput");
  const outBody = document.getElementById("checkinAllOutputBody");
  btn.disabled = true;
  statusEl.textContent = "Running — opening 'Meine Statistiken' on every account, this can take a minute…";
  outEl.classList.remove("show");
  try {
    const res = await fetch("/api/control/command", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cmd: "checkinall" }),
    });
    const data = await res.json().catch(() => ({}));
    outBody.textContent = data.ok ? (data.output || "(no output)") : `Error: ${data.error || res.status}`;
    outEl.classList.add("show");
  } catch (e) {
    outBody.textContent = `Request failed: ${e}`;
    outEl.classList.add("show");
  } finally {
    statusEl.textContent = "";
    btn.disabled = false;
  }
}

function profileRowsHtml(profile) {
  const keys = Object.keys(profile || {});
  if (!keys.length) return '<div class="profile-empty">No data extracted.</div>';
  return keys.map(k => `
    <div class="profile-row"><span class="k">${escapeHtml(k)}</span><span>${escapeHtml(String(profile[k]))}</span></div>
  `).join("");
}

function extractedDataHtml(r) {
  const hasClient = r.client_profile && Object.keys(r.client_profile).length;
  const hasFake = r.fake_profile && Object.keys(r.fake_profile).length;
  if (!hasClient && !hasFake) return "";
  return `
    <details class="extracted-data">
      <summary>Extracted data <span class="hint-inline">— review before approving</span></summary>
      <div class="profile-cols">
        <div class="profile-col">
          <div class="field-label">Client data</div>
          ${profileRowsHtml(r.client_profile)}
        </div>
        <div class="profile-col">
          <div class="field-label">Fake account data</div>
          ${profileRowsHtml(r.fake_profile)}
        </div>
      </div>
    </details>
  `;
}

function pendingCardHtml(r) {
  const val = editedReplies.has(r.id) ? editedReplies.get(r.id) : r.reply;
  const pc = colorFor(r.platform);
  const lastMessage = r.last_message || r.customer_message || "";
  const lastMessageEn = r.last_message_en || r.customer_message_en || "";
  const canSkipConversation = SKIPPABLE_CARD_PLATFORMS.has(String(r.platform || "").toLowerCase());
  return `
    <div class="card" id="approval-${r.id}" data-id="${r.id}" style="--pc:${pc}">
      <div class="card-head">
        <span class="badge">${escapeHtml(r.platform)}</span>
        ${r.reply_type ? `<span class="pill ${r.reply_type === "ASA Follow-up" ? "type-asa" : "type-dia"}">${escapeHtml(r.reply_type)}</span>` : ""}
        <span class="time">${timeAgo(r.created_at)}</span>
      </div>
      ${lastMessage ? `
        <div class="field-label customer-label">Last Message <span class="lang-tag tag-de">DE</span></div>
        <div class="de-box">${escapeHtml(lastMessage)}</div>
        ${lastMessageEn ? `
          <div class="translation-row">
            <span class="lang-tag tag-en">EN</span>
            <span class="en-box">${escapeHtml(lastMessageEn)}</span>
          </div>` : ""}
        <div class="card-divider"></div>
      ` : ""}
      <div class="field-label reply-label">Proposed reply <span class="lang-tag tag-de">DE · editable</span></div>
      <textarea class="reply-input" id="ta-${r.id}"
        oninput="editedReplies.set('${r.id}', this.value)">${escapeHtml(val)}</textarea>
      <div class="translation-row">
        <span class="lang-tag tag-en">EN</span>
        <span class="en-box">${r.reply_en ? escapeHtml(r.reply_en) : "(translation unavailable)"}</span>
      </div>
      ${extractedDataHtml(r)}
      <div class="actions">
        <button class="btn-approve" onclick="approveCard('${r.id}', this)">Approve &amp; Send</button>
        <button class="btn-reject" onclick="act('${r.id}', 'reject', null, this)">Reject &amp; Regenerate</button>
        ${canSkipConversation ? `<button class="btn-skip" onclick="skipConversationCard('${r.id}', '${escapeHtml(r.platform)}', this)">${String(r.platform).toLowerCase() === "xkuss" ? "Skip conversation" : "Transfer / Skip"}</button>` : ""}
        <button class="btn-cancel" onclick="cancelCard('${r.id}', this)">Cancel</button>
        <span class="hint">Edit the German text above before approving. Transfer / Skip hands the conversation to another online moderator, or skips it when none are available. Cancel abandons this reply and restarts the same chat.</span>
      </div>
    </div>
  `;
}

// Auto-pilot activity card: shows everything the guard saw for one auto-sent
// (or auto-attempted) reply — the AI-generated reply, what the Groq meeting
// check detected, and what actually went out if the guard rewrote it.
function autoCardHtml(r) {
  const pc = colorFor(r.platform);
  const lastMessage = r.last_message || r.customer_message || "";
  const lastMessageEn = r.last_message_en || r.customer_message_en || "";
  const detected = r.contains_meeting === true ? "yes" : (r.contains_meeting === false ? "no" : null);
  const changed = !!(r.meeting_guard && r.meeting_guard !== "approve" && r.final_reply && r.final_reply !== r.reply);
  const actionLabel = ACTION_LABELS[r.meeting_guard] || r.meeting_guard;
  return `
    <div class="card" data-id="${r.id}" style="--pc:${pc}">
      <div class="card-head">
        <span class="badge">${escapeHtml(r.platform)}</span>
        ${r.reply_type ? `<span class="pill ${r.reply_type === "ASA Follow-up" ? "type-asa" : "type-dia"}">${escapeHtml(r.reply_type)}</span>` : ""}
        <span class="status-badge ${r.status}">${r.status}</span>
        <span class="time">${timeAgo(r.decided_at || r.created_at)}</span>
      </div>
      ${lastMessage ? `
        <div class="field-label customer-label">Last Message <span class="lang-tag tag-de">DE</span></div>
        <div class="de-box">${escapeHtml(lastMessage)}</div>
        ${lastMessageEn ? `
          <div class="translation-row">
            <span class="lang-tag tag-en">EN</span>
            <span class="en-box">${escapeHtml(lastMessageEn)}</span>
          </div>` : ""}
        <div class="card-divider"></div>
      ` : ""}
      <div class="field-label reply-label">AI generated reply</div>
      <div class="de-box">${escapeHtml(r.reply)}</div>
      ${r.reply_en ? `
        <div class="translation-row">
          <span class="lang-tag tag-en">EN</span>
          <span class="en-box">${escapeHtml(r.reply_en)}</span>
        </div>` : ""}
      <div class="guard-row">
        <span class="test-label">Meeting detected?</span>
        ${detected === null
          ? `<span class="pill unchecked">Not checked</span>`
          : `<span class="pill ${detected}">${detected === "yes" ? "Yes" : "No"}</span>`}
        ${r.meeting_guard ? `<span class="pill action-${escapeHtml(r.meeting_guard)}">${escapeHtml(actionLabel)}</span>` : ""}
      </div>
      ${changed ? `
        <div class="card-divider"></div>
        <div class="field-label changed-label">Guard changed it to <span class="lang-tag tag-de">DE · sent</span></div>
        <div class="de-box">${escapeHtml(r.final_reply)}</div>
      ` : ""}
      ${r.error ? `<div class="history-error">Error: ${escapeHtml(r.error)}</div>` : ""}
    </div>
  `;
}

function renderSections(pending, autoByPlatform) {
  const byPlatform = new Map();
  for (const r of pending) {
    if (!byPlatform.has(r.platform)) byPlatform.set(r.platform, []);
    byPlatform.get(r.platform).push(r);
  }

  const showAuto = currentMode === "auto";

  // Known platforms first (stable order), then any unexpected ones seen live.
  const order = [...knownPlatforms];
  for (const name of byPlatform.keys()) if (!order.includes(name)) order.push(name);
  if (showAuto) for (const name of autoByPlatform.keys()) if (!order.includes(name)) order.push(name);

  document.getElementById("sections").innerHTML = order.map(name => {
    const items = byPlatform.get(name) || [];
    const autoItems = showAuto ? (autoByPlatform.get(name) || []) : [];
    const id = slug(name);
    const pc = colorFor(name);
    const det = detectorFor(name);

    let body = "";
    if (items.length) {
      body += `<div class="cards-grid">${items.map(pendingCardHtml).join("")}</div>`;
    }
    if (showAuto) {
      if (items.length) body += `<div class="auto-section-label">Auto-pilot activity</div>`;
      body += autoItems.length
        ? `<div class="cards-grid">${autoItems.map(autoCardHtml).join("")}</div>`
        : (items.length ? "" : `<div class="empty-state">No activity yet for ${escapeHtml(name)}.</div>`);
    } else if (!items.length) {
      body += `<div class="empty-state">No pending replies for ${escapeHtml(name)}.</div>`;
    }

    const countLabel = showAuto
      ? `${items.length} pending · ${autoItems.length} recent`
      : `${items.length} pending`;

    return `
      <section class="platform-section" id="${id}" style="--pc:${pc}">
        <div class="platform-head">
          <span class="pc-dot"></span>
          <h2>${escapeHtml(name)}</h2>
          <span class="count-pill ${items.length ? "active" : ""}">${countLabel}</span>
          <span class="detector-pill ${det.live ? "live" : "offline"}" style="--det:${det.color}" title="${escapeHtml(det.detail)}">
            <span class="det-dot"></span>${escapeHtml(det.label)}
          </span>
          <hr />
        </div>
        ${workflowStatusHtml(det)}
        ${ctrlBarHtml(name)}
        ${body}
      </section>
    `;
  }).join("");

  // Sidebar nav, same order.
  document.getElementById("navList").innerHTML = order.map(name => {
    const count = (byPlatform.get(name) || []).length;
    const pc = colorFor(name);
    const det = detectorFor(name);
    return `
      <button class="nav-item ${count ? "has-pending" : ""}" style="--pc:${pc}" onclick="goToSection('${name}')">
        <span class="nav-dot"></span>
        <span>${escapeHtml(name)}</span>
        <span class="nav-count">${count}</span>
      </button>
      <div class="nav-detector ${det.live ? "live" : "offline"}" style="--det:${det.color}">
        <span class="det-dot"></span><span>${escapeHtml(det.label)}</span>
      </div>
    `;
  }).join("");

  document.getElementById("statPending").textContent = pending.length;
  document.getElementById("statPlatforms").textContent = byPlatform.size;
  document.getElementById("mobilePendingBadge").textContent = `${pending.length} pending`;

  // Drop edit-buffers for requests that are no longer pending (decided elsewhere).
  const stillPending = new Set(pending.map(r => r.id));
  for (const id of [...editedReplies.keys()]) if (!stillPending.has(id)) editedReplies.delete(id);
}

function historyCardHtml(r) {
  const skipState = r.status === "skip_requested" || r.status === "skipped" || r.status === "transferred";
  return `
    <div class="history-card" style="--pc:${colorFor(r.platform)}">
      <div class="card-head">
        <span class="badge">${escapeHtml(r.platform)}</span>
        <span class="status-badge ${r.status}">${r.status}</span>
        ${r.auto ? '<span class="status-badge" style="background:rgba(99,102,241,.15);color:var(--primary)">auto</span>' : ""}
        ${r.meeting_guard ? `<span class="status-badge" style="background:rgba(234,179,8,.15);color:var(--warning)" title="Meeting detected — reply rewritten in German">guard: ${escapeHtml(r.meeting_guard)}</span>` : ""}
        <span class="time">${timeAgo(r.decided_at || r.sent_at || r.created_at)}</span>
      </div>
      <div class="history-reply">${skipState
        ? escapeHtml(
            r.status === "transferred" ? "Conversation transferred to another moderator — no reply sent."
            : r.status === "skipped" ? "Nobody else was online; conversation skipped — no reply sent."
            : "Transfer requested — waiting for the bot to transfer or skip the conversation."
          )
        : escapeHtml(r.final_reply || r.reply)}</div>
      ${!skipState && r.reply_en ? `<div class="history-en">${escapeHtml(r.reply_en)}</div>` : ""}
      ${r.error ? `<div class="history-error">Error: ${escapeHtml(r.error)}</div>` : ""}
    </div>
  `;
}

function renderHistory(items) {
  const el = document.getElementById("history");
  el.innerHTML = items.length ? items.map(historyCardHtml).join("") : '<div class="empty-state">Nothing yet.</div>';

  const sentToday = items.filter(r => (r.status === "sent" || r.status === "approved") && isToday(r.decided_at || r.sent_at)).length;
  const rejectedToday = items.filter(r => r.status === "rejected" && isToday(r.decided_at)).length;
  document.getElementById("statSent").textContent = sentToday;
  document.getElementById("statRejected").textContent = rejectedToday;
}

// Shared by both the WebSocket push (see connectLive() below) and the HTTP
// fallback (refresh()) — one code path renders a snapshot no matter where it
// came from, so the two can never quietly drift out of sync with each other.
function applySnapshot(data) {
  if (data.status) liveStatus = data.status;
  if (typeof data.mode === "string") currentMode = data.mode;
  renderModeToggle();

  if (data.control) {
    controlsAvailable = !!data.control.available;
    controlPlatformState = data.control.platforms || {};
  }
  if (data.bots) botProcessState = data.bots;
  const checkinBtn = document.getElementById("checkinAllBtn");
  if (checkinBtn) checkinBtn.disabled = !controlsAvailable;
  const unavailNote = document.getElementById("ctrlUnavailableNote");
  if (unavailNote) unavailNote.style.display = controlsAvailable ? "none" : "block";

  const history = data.history || []; // newest first
  const autoByPlatform = new Map();
  const AUTO_PER_PLATFORM = 12; // history is already newest-first, so this keeps the most recent N per platform
  for (const r of history) {
    if (!r.auto) continue;
    if (!autoByPlatform.has(r.platform)) autoByPlatform.set(r.platform, []);
    const arr = autoByPlatform.get(r.platform);
    if (arr.length < AUTO_PER_PLATFORM) arr.push(r);
  }
  renderHistory(history.slice(0, 30));

  const pendingList = data.pending || [];
  const currentPendingIds = new Set(pendingList.map(r => r.id));
  if (knownPendingIds !== null) {
    for (const id of currentPendingIds) {
      if (!knownPendingIds.has(id)) { playNotifySound(); break; }
    }
  }
  knownPendingIds = currentPendingIds;
  renderSections(pendingList, autoByPlatform);
  if (navigator.setAppBadge) {
    if (pendingList.length) navigator.setAppBadge(pendingList.length).catch(() => {});
    else if (navigator.clearAppBadge) navigator.clearAppBadge().catch(() => {});
  }
  const requestedApproval = new URLSearchParams(location.search).get("approval");
  if (requestedApproval) {
    requestAnimationFrame(() => {
      const card = document.getElementById(`approval-${requestedApproval}`);
      if (card) card.scrollIntoView({ behavior: "smooth", block: "center" });
    });
  }

  renderSystemStatus();
}

async function refresh() {
  // Never yank the textarea out from under someone mid-keystroke.
  if (document.activeElement && document.activeElement.classList.contains("reply-input")) return;
  try {
    const [pendingRes, historyRes, statusRes, modeRes, botsRes] = await Promise.all([
      fetch("/api/requests?status=pending"),
      fetch("/api/requests?status=approved,rejected,cancelled,skip_requested,skipped,transferred,sent,failed&limit=300"),
      fetch("/api/status"),
      fetch("/api/mode"),
      fetch("/api/bots/status"),
    ]);
    await refreshControlStatus();
    applySnapshot({
      status: await statusRes.json(),
      mode: ((await modeRes.json()).mode) || "manual",
      control: { available: controlsAvailable, platforms: controlPlatformState },
      bots: await botsRes.json(),
      history: await historyRes.json(),
      pending: await pendingRes.json(),
    });
  } catch (e) {
    // transient network hiccup — next poll will retry
  }
}

// ── Live push over /ws/live, HTTP polling as the fallback ─────────────────
// The dashboard always works via refresh()'s polling; this just makes it
// near-instant when it's available, so a card approved on someone else's
// screen (or a bot's status ping) shows up here within a beat instead of up
// to 1.5s later. Reconnects with backoff on any drop — liveConnected gates
// the polling interval below, so the two never both hammer the server.
let liveSocket = null;
let liveConnected = false;
let wsReconnectDelay = 1000;

function setLiveIndicator(connected) {
  const badge = document.getElementById("liveBadge");
  const label = document.getElementById("liveBadgeLabel");
  if (!badge || !label) return;
  badge.classList.toggle("live", connected);
  badge.classList.toggle("polling", !connected);
  label.textContent = connected ? "Live" : "Polling…";
}

function connectLive() {
  if (!("WebSocket" in window)) { setLiveIndicator(false); return; }
  let socket;
  try {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    socket = new WebSocket(`${proto}//${location.host}/ws/live`);
  } catch (e) {
    scheduleReconnect();
    return;
  }
  liveSocket = socket;
  socket.onopen = () => { liveConnected = true; wsReconnectDelay = 1000; setLiveIndicator(true); };
  socket.onmessage = (ev) => {
    if (document.activeElement && document.activeElement.classList.contains("reply-input")) return;
    try { applySnapshot(JSON.parse(ev.data)); } catch (e) {}
  };
  socket.onclose = () => { liveConnected = false; setLiveIndicator(false); scheduleReconnect(); };
  socket.onerror = () => { try { socket.close(); } catch (e) {} };
}

function scheduleReconnect() {
  setTimeout(connectLive, wsReconnectDelay);
  wsReconnectDelay = Math.min(wsReconnectDelay * 1.6, 15000);
}

// ── System status strip ("detectors") at the top of the page ──────────────
function renderSystemStatus() {
  const el = document.getElementById("systemStatus");
  if (!el || !knownPlatforms.length) return;
  const onlineCount = knownPlatforms.filter(p => detectorFor(p).live).length;
  const chips = [
    {
      label: `${onlineCount}/${knownPlatforms.length} bots online`,
      cls: onlineCount === 0 ? "bad" : (onlineCount < knownPlatforms.length ? "warn" : "ok"),
    },
    {
      label: controlsAvailable ? "Bot controls connected" : "Bot controls offline",
      cls: controlsAvailable ? "ok" : "warn",
    },
    {
      label: currentMode === "auto" ? "Auto-pilot is ON" : "Manual review",
      cls: currentMode === "auto" ? "warn" : "ok",
    },
  ];
  el.innerHTML = chips.map(c =>
    `<span class="chip ${c.cls}"><span class="dot"></span>${escapeHtml(c.label)}</span>`
  ).join("");
}

async function init() {
  try {
    const platforms = await (await fetch("/api/platforms")).json();
    knownPlatforms = platforms.map(p => p.name);
    for (const p of platforms) platformColorMap.set(p.name, p.color);
  } catch (e) {
    knownPlatforms = [];
  }
  renderSoundToggle();
  initPushNotifications();
  refresh();
  connectLive();
  // Only polls while the live socket is down — see setLiveIndicator() above.
  setInterval(() => { if (!liveConnected) refresh(); }, 1500);
}

init();
</script>
</body>
</html>
"""




# ── Bots launcher page ───────────────────────────────────────────────────────
# The single place to start/stop Xkuss, Justlo, Linduu and Gnoxx and configure
# Approval (human review vs fully automatic) independently — no terminal
# commands needed. Each card is self-contained: changing one never touches
# another platform's process or Approval setting.
_BOTS_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Bots — Launcher</title>
<style>
  :root {
    --background: #09090b; --foreground: #fafafa;
    --card: #18181b; --card-foreground: #fafafa;
    --border: #27272a; --input: #27272a;
    --muted: #18181b; --muted-foreground: #a1a1aa;
    --accent: #27272a; --accent-foreground: #fafafa;
    --primary: #6366f1; --primary-foreground: #fafafa;
    --success: #22c55e; --destructive: #ef4444;
    --warning: #eab308; --info: #38bdf8; --violet: #a78bfa;
    --ring: #6366f1; --radius: 10px;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--background); color: var(--foreground);
    font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, Roboto, Arial, sans-serif;
    padding: 22px 26px 60px;
  }
  a { color: inherit; }
  .topbar { display: flex; align-items: center; gap: 12px; margin-bottom: 22px; flex-wrap: wrap; }
  .back-link {
    font-size: 12.5px; color: var(--muted-foreground); text-decoration: none;
    border: 1px solid var(--border); border-radius: 7px; padding: 6px 11px;
  }
  .back-link:hover { color: var(--foreground); background: var(--accent); }
  .topbar h1 { font-size: 19px; margin: 0; letter-spacing: -.01em; }
  .topbar p { margin: 3px 0 0; color: var(--muted-foreground); font-size: 13px; }

  .bots-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 16px; }

  .bot-card {
    background: var(--card); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 16px 18px;
  }
  .bot-card-head { display: flex; align-items: center; gap: 10px; margin-bottom: 4px; }
  .bot-card-head h2 { font-size: 16px; margin: 0; font-weight: 700; }
  .status-pill {
    margin-left: auto; font-size: 10.5px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .03em; padding: 3px 9px; border-radius: 999px; border: 1px solid var(--border);
    color: var(--muted-foreground);
  }
  .status-pill.running { color: var(--success); border-color: rgba(34,197,94,.35); background: rgba(34,197,94,.1); }
  .status-pill.external { color: var(--warning); border-color: rgba(234,179,8,.35); background: rgba(234,179,8,.1); }
  .live-line { font-size: 11.5px; color: var(--muted-foreground); margin: 2px 0 12px; min-height: 15px; }

  .start-stop-row { display: flex; gap: 8px; margin-bottom: 14px; }
  button {
    font: inherit; font-weight: 600; font-size: 13px; border: 1px solid transparent;
    border-radius: 7px; padding: 8px 15px; cursor: pointer; transition: filter .12s;
  }
  button:hover { filter: brightness(1.08); }
  button:disabled { opacity: .5; cursor: default; }
  .btn-start { background: var(--success); color: #052e16; }
  .btn-stop { background: transparent; color: var(--destructive); border-color: rgba(239,68,68,.4); }

  .toggle-row {
    display: flex; align-items: center; justify-content: space-between; gap: 10px;
    padding: 9px 0; border-top: 1px solid var(--border);
  }
  .toggle-row:first-of-type { border-top: 1px solid var(--border); margin-top: 4px; }
  .toggle-name { font-size: 12.5px; font-weight: 650; color: var(--muted-foreground); }
  .toggle-switch-btn {
    display: flex; align-items: center; gap: 8px; background: var(--muted); border: 1px solid var(--border);
    border-radius: 999px; padding: 4px 12px 4px 4px; cursor: pointer; font: inherit;
  }
  .toggle-switch-btn:disabled { opacity: .5; cursor: default; }
  .toggle-dot {
    width: 15px; height: 15px; border-radius: 999px; background: var(--muted-foreground); flex: none;
    transition: background .15s;
  }
  .toggle-switch-btn.on .toggle-dot { background: var(--success); }
  .toggle-switch-btn.warn.on .toggle-dot { background: var(--warning); }
  .toggle-switch-label { font-size: 12.5px; font-weight: 650; }

  details.steps-block {
    margin-top: 12px; border-top: 1px solid var(--border); padding-top: 10px;
  }
  summary { cursor: pointer; font-size: 12px; font-weight: 650; color: var(--muted-foreground); }
  summary:hover { color: var(--foreground); }
  ol.steps-list { margin: 10px 0 0; padding-left: 20px; font-size: 12.5px; color: var(--foreground); }
  ol.steps-list li { margin-bottom: 5px; }
  .error-note { color: var(--destructive); font-size: 12px; margin-top: 8px; }

  /* ── Toasts + confirm modal (same component as the approval dashboard) ── */
  #toastRoot {
    position: fixed; z-index: 200; right: 16px; bottom: 16px;
    display: flex; flex-direction: column; gap: 8px; width: min(360px, calc(100vw - 32px));
    pointer-events: none;
  }
  .toast {
    pointer-events: auto; display: flex; align-items: flex-start; gap: 10px;
    background: var(--card); border: 1px solid var(--border); border-left: 3px solid var(--info);
    border-radius: var(--radius); padding: 11px 12px; box-shadow: 0 8px 24px rgba(0,0,0,.35);
    animation: toast-in .18s ease-out;
  }
  .toast.leaving { animation: toast-out .16s ease-in forwards; }
  .toast.success { border-left-color: var(--success); }
  .toast.error   { border-left-color: var(--destructive); }
  .toast.warning { border-left-color: var(--warning); }
  .toast-body { flex: 1; min-width: 0; }
  .toast-title { font-size: 13px; font-weight: 600; color: var(--foreground); }
  .toast-detail { font-size: 12px; color: var(--muted-foreground); margin-top: 2px; }
  .toast-close { flex: none; background: none; border: none; color: var(--muted-foreground); cursor: pointer; font-size: 14px; padding: 2px; line-height: 1; }
  .toast-close:hover { color: var(--foreground); }
  @keyframes toast-in { from { opacity: 0; transform: translateY(6px) scale(.98); } to { opacity: 1; transform: none; } }
  @keyframes toast-out { from { opacity: 1; } to { opacity: 0; transform: translateY(-4px); } }
  #modalRoot { display: none; position: fixed; inset: 0; z-index: 210; align-items: center; justify-content: center; padding: 20px; background: rgba(0,0,0,.55); backdrop-filter: blur(1px); }
  #modalRoot.open { display: flex; }
  .modal-box { width: min(400px, 100%); background: var(--card); border: 1px solid var(--border); border-radius: var(--radius); padding: 18px; box-shadow: 0 20px 50px rgba(0,0,0,.5); animation: toast-in .16s ease-out; }
  .modal-title { font-size: 15px; font-weight: 650; margin: 0 0 6px; }
  .modal-body { font-size: 13.5px; color: var(--muted-foreground); line-height: 1.5; }
  .modal-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 18px; }
  .modal-actions button { font: inherit; font-size: 13px; font-weight: 600; padding: 8px 14px; border-radius: 8px; cursor: pointer; border: 1px solid var(--border); background: var(--accent); color: var(--foreground); }
  .modal-actions .modal-confirm { background: var(--primary); border-color: transparent; color: #fff; }
  .modal-actions .modal-confirm.danger { background: var(--destructive); }
  .spinner { display: inline-block; width: 12px; height: 12px; border-radius: 999px; border: 2px solid currentColor; border-right-color: transparent; opacity: .8; animation: spin .6s linear infinite; vertical-align: -2px; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<div id="toastRoot" aria-live="polite"></div>
<div id="modalRoot"></div>
<div class="topbar">
  <a class="back-link" href="/">&larr; Approval Dashboard</a>
  <div>
    <h1>Bots</h1>
    <p>Start, stop, and configure Xkuss, Justlo, Linduu and Gnoxx. Every bot uses the real Chameleon-AI AgentWorkspace.</p>
  </div>
</div>

<div class="bots-grid" id="botsGrid"></div>

<script>
const escapeHtml = (s) => (s || "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));

function toast(title, opts) {
  opts = opts || {};
  const type = opts.type || "info";
  const detail = opts.detail || "";
  const ms = opts.duration != null ? opts.duration : 4200;
  const root = document.getElementById("toastRoot");
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.innerHTML = `
    <div class="toast-body">
      <div class="toast-title"></div>
      ${detail ? `<div class="toast-detail"></div>` : ""}
    </div>
    <button class="toast-close" aria-label="Dismiss">✕</button>
  `;
  el.querySelector(".toast-title").textContent = title;
  if (detail) el.querySelector(".toast-detail").textContent = detail;
  const remove = () => { el.classList.add("leaving"); setTimeout(() => el.remove(), 170); };
  el.querySelector(".toast-close").onclick = remove;
  root.appendChild(el);
  if (ms) setTimeout(remove, ms);
  return remove;
}

function showConfirm(title, body, opts) {
  opts = opts || {};
  return new Promise((resolve) => {
    const root = document.getElementById("modalRoot");
    root.innerHTML = `
      <div class="modal-box">
        <p class="modal-title"></p>
        <p class="modal-body"></p>
        <div class="modal-actions">
          <button class="modal-cancel">${escapeHtml(opts.cancelLabel || "Cancel")}</button>
          <button class="modal-confirm ${opts.danger ? "danger" : ""}">${escapeHtml(opts.confirmLabel || "Confirm")}</button>
        </div>
      </div>
    `;
    root.querySelector(".modal-title").textContent = title;
    root.querySelector(".modal-body").textContent = body;
    const close = (result) => {
      root.classList.remove("open");
      root.innerHTML = "";
      document.removeEventListener("keydown", onKey);
      resolve(result);
    };
    const onKey = (e) => { if (e.key === "Escape") close(false); };
    document.addEventListener("keydown", onKey);
    root.querySelector(".modal-cancel").onclick = () => close(false);
    root.querySelector(".modal-confirm").onclick = () => close(true);
    root.onclick = (e) => { if (e.target === root) close(false); };
    root.classList.add("open");
  });
}

const PLATFORMS = [
  { slug: "xkuss",  label: "Xkuss" },
  { slug: "justlo", label: "Justlo" },
  { slug: "linduu", label: "Linduu" },
  { slug: "gnoxx",  label: "Gnoxx" },
];

// Per-platform client-side state, kept separate per card by construction --
// every fetch/render/action below is always scoped to one `slug`, never "all".
const state = {};
for (const p of PLATFORMS) {
  state[p.slug] = { approvalEffective: "manual", running: false, managedBy: null, liveDetail: "" };
}

function stepsFor(slug, approval) {
  const approvalStep = approval === "auto"
    ? "Skip approval — the reply is used immediately (no one reviews it)"
    : "Wait for you to Approve or Reject it on the main Approval Dashboard";
  return [
    "Detect an incoming chat",
    "Capture that chat's HTML",
    "Paste it into the real Chameleon-AI AgentWorkspace",
    "Click \"Antwort generieren\" and wait for a reply",
    approvalStep,
    "Copy the reply text",
    "Paste it into the chat's reply box",
    "Wait about 15–20 seconds, then send",
  ];
}

function renderCard(slug, label) {
  const s = state[slug];
  const running = s.running;
  const statusClass = running ? (s.managedBy === "external" ? "external" : "running") : "";
  const statusText = running ? (s.managedBy === "external" ? "Running (external)" : "Running") : "Stopped";
  const steps = stepsFor(slug, s.approvalEffective);

  return `
    <div class="bot-card" data-slug="${slug}">
      <div class="bot-card-head">
        <h2>${escapeHtml(label)}</h2>
        <span class="status-pill ${statusClass}">${statusText}</span>
      </div>
      <div class="live-line">${escapeHtml(s.liveDetail || "")}</div>

      <div class="start-stop-row">
        <button class="btn-start" ${running ? "disabled" : ""} onclick="startBot('${slug}')">Start</button>
        <button class="btn-stop" ${(!running || s.managedBy !== "dashboard") ? "disabled" : ""} onclick="stopBot('${slug}')">Stop</button>
      </div>

      <div class="toggle-row">
        <span class="toggle-name">Approval</span>
        <button class="toggle-switch-btn warn ${s.approvalEffective === "auto" ? "on" : ""}" onclick="toggleApproval('${slug}')">
          <span class="toggle-dot"></span>
          <span class="toggle-switch-label">${s.approvalEffective === "auto" ? "Fully automatic" : "Manual review"}</span>
        </button>
      </div>

      <details class="steps-block">
        <summary>What happens, step by step</summary>
        <ol class="steps-list">${steps.map(st => `<li>${escapeHtml(st)}</li>`).join("")}</ol>
      </details>

      <div class="error-note" id="err-${slug}"></div>
    </div>
  `;
}

function render() {
  document.getElementById("botsGrid").innerHTML = PLATFORMS.map(p => renderCard(p.slug, p.label)).join("");
}

function showError(slug, msg) {
  const el = document.getElementById(`err-${slug}`);
  if (el) el.textContent = msg;
}

async function startBot(slug) {
  showError(slug, "");
  try {
    const res = await fetch(`/api/bots/${slug}/start`, { method: "POST" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) showError(slug, data.error || `HTTP ${res.status}`);
  } catch (e) {
    showError(slug, "Request failed: " + e);
  }
  await refreshStatus();
}

async function stopBot(slug) {
  showError(slug, "");
  try {
    const res = await fetch(`/api/bots/${slug}/stop`, { method: "POST" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) showError(slug, data.error || `HTTP ${res.status}`);
  } catch (e) {
    showError(slug, "Request failed: " + e);
  }
  await refreshStatus();
}

async function toggleApproval(slug) {
  showError(slug, "");
  const next = state[slug].approvalEffective === "auto" ? "manual" : "auto";
  try {
    const res = await fetch("/api/mode/override", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ platform: slug, mode: next }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) { showError(slug, data.error || `HTTP ${res.status}`); return; }
    state[slug].approvalEffective = data.effective;
    render();
  } catch (e) {
    showError(slug, "Request failed: " + e);
  }
}

async function refreshStatus() {
  try {
    const res = await fetch("/api/bots/status");
    const data = await res.json();
    for (const p of PLATFORMS) {
      const s = data[p.slug] || {};
      state[p.slug].running = !!s.running;
      state[p.slug].managedBy = s.managed_by || null;
    }
  } catch (e) { /* transient — next poll retries */ }

  try {
    const res = await fetch("/api/status");
    const data = await res.json();
    for (const p of PLATFORMS) {
      const entry = data[p.label] || data[p.label.toLowerCase()];
      state[p.slug].liveDetail = entry ? `${entry.state}${entry.detail ? " — " + entry.detail : ""}` : "";
    }
  } catch (e) { /* transient */ }

  render();
}

async function loadInitial() {
  for (const p of PLATFORMS) {
    try {
      const res = await fetch(`/api/mode/override?platform=${p.slug}`);
      const data = await res.json();
      if (data.effective) state[p.slug].approvalEffective = data.effective;
    } catch (e) { /* keep default */ }
  }
  await refreshStatus();
  setInterval(refreshStatus, 2500);
}

render();
loadInitial();
</script>
</body>
</html>
"""


@app.get("/")
def dashboard():
    return Response(_PAGE, mimetype="text/html")


@app.get("/service-worker.js")
def service_worker():
    response = app.send_static_file("service-worker.js")
    response.headers["Service-Worker-Allowed"] = "/"
    response.headers["Cache-Control"] = "no-cache"
    return response


@app.get("/bots")
def bots_page():
    return Response(_BOTS_PAGE, mimetype="text/html")


def main():
    print(f"[ApprovalServer] Dashboard running at http://{HOST}:{PORT}")
    if not (AUTH_USER and AUTH_PASS):
        print("[ApprovalServer] WARNING: APPROVAL_USER/APPROVAL_PASS not set -- "
              "no login required. Fine on 127.0.0.1, unsafe on the public internet.")
    app.run(host=HOST, port=PORT, threaded=True)


if __name__ == "__main__":
    main()
