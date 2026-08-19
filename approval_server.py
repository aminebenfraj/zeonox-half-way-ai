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
import threading
import uuid
from datetime import datetime

import httpx
from flask import Flask, jsonify, request, Response

try:
    from deep_translator import GoogleTranslator
except ImportError:  # translation is a nice-to-have — dashboard still works without it
    GoogleTranslator = None

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
    "Xkuss", "Justlo", "Linduu",
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

# Live per-platform bot state (see core/approval.py's report_status()), keyed
# by platform name. In-memory like everything else here — a restart just means
# every platform shows as offline until its next status ping.
_status: dict[str, dict] = {}
STATUS_STALE_AFTER = 30  # seconds without a ping before the dashboard treats a platform as offline

# Review mode, toggled from the dashboard (see /api/mode below). "manual" is
# the original behaviour — every request waits in the queue for a human.
# "auto" auto-approves each NEW request the instant it's created, so the bot's
# request_approval() poll (unchanged — it just sees status "approved" on its
# very first check) sends it straight through with no one watching. Requests
# already pending when the mode flips are left alone either way — never yank
# a card out from under someone who's mid-edit.
_mode = "manual"

# Translation calls hit Google's endpoint over the network; bound each one with
# a hard timeout on a worker thread so a slow/unreachable network never stalls
# the request-creation endpoint (and therefore never stalls a bot's cycle).
_translate_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="translate")


def _translate_de_en(text: str, timeout: float = 6.0) -> str | None:
    text = (text or "").strip()
    if not text or GoogleTranslator is None:
        return None
    try:
        future = _translate_pool.submit(lambda: GoogleTranslator(source="de", target="en").translate(text))
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
    Only ever drops decided (non-pending) requests, oldest first."""
    if len(_requests) <= MAX_HISTORY:
        return
    decided = sorted(
        (r for r in _requests.values() if r["status"] != "pending"),
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
    reply_type = (body.get("reply_type") or "").strip() or None  # e.g. "DIA" / "ASA Follow-up" (Justlo only) — None means the bot doesn't report one
    if not reply.strip():
        return jsonify({"error": "reply is required"}), 400

    # Best-effort DE->EN translation so a reviewer who doesn't read German can
    # still judge the reply. The German text is always what's authoritative /
    # editable / actually sent — translations are read-only context.
    reply_en = _translate_de_en(reply)
    customer_message_en = _translate_de_en(customer_message) if customer_message else None

    with _lock:
        auto = _mode == "auto"

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
            "customer_message": customer_message,
            "customer_message_en": customer_message_en,
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
    return jsonify({"id": req_id, "auto": auto, "meeting_guard": meeting_guard}), 201


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
    return jsonify({"ok": True, "mode": mode})


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
    return jsonify({"ok": True})


@app.post("/api/requests/<req_id>/sent")
def mark_sent(req_id):
    with _lock:
        r = _requests.get(req_id)
        if r:
            r["status"] = "sent"
            r["sent_at"] = _now()
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
    return jsonify({"ok": True})


@app.post("/api/status")
def update_status():
    """Bots ping this at each state transition (waiting for a chat, extracting,
    generating, awaiting approval, sending, idle, error) — see report_status()
    in core/approval.py. Powers the live 'detector' indicator per platform."""
    body = request.get_json(force=True, silent=True) or {}
    platform = (body.get("platform") or "").strip()
    if not platform:
        return jsonify({"error": "platform is required"}), 400
    with _lock:
        _status[platform] = {
            "state": body.get("state") or "unknown",
            "detail": body.get("detail") or "",
            "updated_at": _now(),
        }
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

@app.get("/api/control/status")
def control_status():
    try:
        r = httpx.get(f"{LAUNCHER_CONTROL_URL}/control/status", timeout=5)
        r.raise_for_status()
        data = r.json()
        data["available"] = True
        return jsonify(data)
    except Exception as e:
        return jsonify({"ok": False, "available": False, "error": str(e)})


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
</style>
</head>
<body>
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
  if (next === "auto" && !confirm(
    "Switch to Auto-pilot?\\n\\nEvery new AI reply will be sent immediately, with no human review. " +
    "Replies already waiting in the queue are not affected."
  )) return;
  try {
    const res = await fetch("/api/mode", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: next }),
    });
    if (!res.ok) throw new Error(String(res.status));
    currentMode = next;
  } catch (e) {
    alert("Could not change mode — is the server reachable?");
  }
  renderModeToggle();
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

async function act(id, action, body) {
  const res = await fetch(`/api/requests/${id}/${action}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    alert(`Could not ${action}: ${err.error || res.status}`);
  }
  editedReplies.delete(id);
  refresh();
}

function approveCard(id) {
  const ta = document.getElementById(`ta-${id}`);
  act(id, "approve", { edited_reply: ta ? ta.value : undefined });
}

function cancelCard(id) {
  if (!confirm("Cancel this reply? The bot will abandon it and restart the chat instead of regenerating.")) return;
  act(id, "cancel");
}

// Human labels + accent per bot state, reported via POST /api/status
// (see report_status() in core/approval.py). Anything not listed here still
// renders (falls back to the raw state string) so a new state added on the
// bot side never breaks the dashboard.
const STATE_META = {
  starting:           { label: "Starting…",           color: "var(--muted-foreground)" },
  waiting_for_chat:   { label: "Waiting for a chat",   color: "var(--muted-foreground)" },
  chat_detected:      { label: "Chat detected",        color: "var(--info)" },
  extracting:         { label: "Extracting…",          color: "var(--info)" },
  generating:         { label: "Generating reply…",    color: "var(--violet)" },
  awaiting_approval:  { label: "Awaiting approval",    color: "var(--warning)" },
  sending:            { label: "Sending…",              color: "var(--success)" },
  idle:               { label: "Idle",                  color: "var(--muted-foreground)" },
  recovering:         { label: "Recovering…",           color: "var(--warning)" },
  error:              { label: "Error",                 color: "var(--destructive)" },
};
const STATUS_STALE_MS = 30_000; // must match STATUS_STALE_AFTER on the server

let liveStatus = {}; // platform -> {state, detail, updated_at}

function detectorFor(name) {
  const s = liveStatus[name];
  if (!s) return { live: false, label: "Offline", color: "var(--border)", detail: "" };
  const ageMs = Date.now() - new Date(s.updated_at).getTime();
  if (ageMs > STATUS_STALE_MS) return { live: false, label: "Offline", color: "var(--border)", detail: "" };
  const meta = STATE_META[s.state] || { label: s.state, color: "var(--info)" };
  return { live: true, label: meta.label, color: meta.color, detail: s.detail || "" };
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

async function runControl(btn, slug, cmd) {
  if (btn.disabled) return;
  btn.disabled = true;
  const original = btn.textContent;
  btn.textContent = "…";
  try {
    const res = await fetch("/api/control/command", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cmd, target: slug }),
    });
    const data = await res.json().catch(() => ({}));
    showControlOutput(slug, data.ok ? (data.output || "(no output)") : `Error: ${data.error || res.status}`);
  } catch (e) {
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

function pendingCardHtml(r) {
  const val = editedReplies.has(r.id) ? editedReplies.get(r.id) : r.reply;
  const pc = colorFor(r.platform);
  return `
    <div class="card" data-id="${r.id}" style="--pc:${pc}">
      <div class="card-head">
        <span class="badge">${escapeHtml(r.platform)}</span>
        ${r.reply_type ? `<span class="pill ${r.reply_type === "ASA Follow-up" ? "type-asa" : "type-dia"}">${escapeHtml(r.reply_type)}</span>` : ""}
        <span class="time">${timeAgo(r.created_at)}</span>
      </div>
      ${r.customer_message ? `
        <div class="field-label customer-label">Last Message <span class="lang-tag tag-de">DE</span></div>
        <div class="de-box">${escapeHtml(r.customer_message)}</div>
        ${r.customer_message_en ? `
          <div class="translation-row">
            <span class="lang-tag tag-en">EN</span>
            <span class="en-box">${escapeHtml(r.customer_message_en)}</span>
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
      <div class="actions">
        <button class="btn-approve" onclick="approveCard('${r.id}')">Approve &amp; Send</button>
        <button class="btn-reject" onclick="act('${r.id}', 'reject')">Reject &amp; Regenerate</button>
        <button class="btn-cancel" onclick="cancelCard('${r.id}')">Cancel</button>
        <span class="hint">Edit the German text above before approving to send your own wording. Cancel abandons this reply and restarts the chat.</span>
      </div>
    </div>
  `;
}

// Auto-pilot activity card: shows everything the guard saw for one auto-sent
// (or auto-attempted) reply — the AI-generated reply, what the Groq meeting
// check detected, and what actually went out if the guard rewrote it.
function autoCardHtml(r) {
  const pc = colorFor(r.platform);
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
      ${r.customer_message ? `
        <div class="field-label customer-label">Last Message <span class="lang-tag tag-de">DE</span></div>
        <div class="de-box">${escapeHtml(r.customer_message)}</div>
        ${r.customer_message_en ? `
          <div class="translation-row">
            <span class="lang-tag tag-en">EN</span>
            <span class="en-box">${escapeHtml(r.customer_message_en)}</span>
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
  return `
    <div class="history-card" style="--pc:${colorFor(r.platform)}">
      <div class="card-head">
        <span class="badge">${escapeHtml(r.platform)}</span>
        <span class="status-badge ${r.status}">${r.status}</span>
        ${r.auto ? '<span class="status-badge" style="background:rgba(99,102,241,.15);color:var(--primary)">auto</span>' : ""}
        ${r.meeting_guard ? `<span class="status-badge" style="background:rgba(234,179,8,.15);color:var(--warning)" title="Meeting detected — reply rewritten in German">guard: ${escapeHtml(r.meeting_guard)}</span>` : ""}
        <span class="time">${timeAgo(r.decided_at || r.sent_at || r.created_at)}</span>
      </div>
      <div class="history-reply">${escapeHtml(r.final_reply || r.reply)}</div>
      ${r.reply_en ? `<div class="history-en">${escapeHtml(r.reply_en)}</div>` : ""}
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

async function refresh() {
  // Never yank the textarea out from under someone mid-keystroke.
  if (document.activeElement && document.activeElement.classList.contains("reply-input")) return;
  try {
    const [pendingRes, historyRes, statusRes, modeRes] = await Promise.all([
      fetch("/api/requests?status=pending"),
      fetch("/api/requests?status=approved,rejected,cancelled,sent,failed&limit=300"),
      fetch("/api/status"),
      fetch("/api/mode"),
    ]);
    liveStatus = await statusRes.json();
    currentMode = ((await modeRes.json()).mode) || "manual";
    renderModeToggle();

    await refreshControlStatus();
    document.getElementById("checkinAllBtn").disabled = !controlsAvailable;
    document.getElementById("ctrlUnavailableNote").style.display = controlsAvailable ? "none" : "block";

    const history = await historyRes.json(); // newest first
    const autoByPlatform = new Map();
    const AUTO_PER_PLATFORM = 12; // history is already newest-first, so this keeps the most recent N per platform
    for (const r of history) {
      if (!r.auto) continue;
      if (!autoByPlatform.has(r.platform)) autoByPlatform.set(r.platform, []);
      const arr = autoByPlatform.get(r.platform);
      if (arr.length < AUTO_PER_PLATFORM) arr.push(r);
    }

    const pendingList = await pendingRes.json();
    const currentPendingIds = new Set(pendingList.map(r => r.id));
    if (knownPendingIds !== null) {
      for (const id of currentPendingIds) {
        if (!knownPendingIds.has(id)) { playNotifySound(); break; }
      }
    }
    knownPendingIds = currentPendingIds;

    renderSections(pendingList, autoByPlatform);
    renderHistory(history.slice(0, 30));
  } catch (e) {
    // transient network hiccup — next poll will retry
  }
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
  refresh();
  setInterval(refresh, 1500);
}

init();
</script>
</body>
</html>
"""


@app.get("/")
def dashboard():
    return Response(_PAGE, mimetype="text/html")


def main():
    print(f"[ApprovalServer] Dashboard running at http://{HOST}:{PORT}")
    if not (AUTH_USER and AUTH_PASS):
        print("[ApprovalServer] WARNING: APPROVAL_USER/APPROVAL_PASS not set -- "
              "no login required. Fine on 127.0.0.1, unsafe on the public internet.")
    app.run(host=HOST, port=PORT, threaded=True)


if __name__ == "__main__":
    main()
