#!/usr/bin/env python3
"""
Local approval dashboard.

Every bot process (one per platform, see run_bot.py) POSTs a candidate reply
here right after generating it, then blocks polling until a human decides.
This process is the single source of truth for those pending decisions, so it
must be started BEFORE the bots (start_all.py / launch_all.py do this
automatically) and kept running for as long as any bot is running.

Endpoints:
  POST /api/requests              -> create a pending request, returns {id}
  GET  /api/requests?status=...   -> list requests (dashboard polls this)
  GET  /api/requests/<id>         -> single request (bots poll this)
  POST /api/requests/<id>/approve -> body {edited_reply?}
  POST /api/requests/<id>/reject
  POST /api/requests/<id>/cancel  -> abandon it; bot restarts/redetects instead of regenerating
  POST /api/requests/<id>/sent    -> bot confirms the approved reply went out
  POST /api/requests/<id>/failed  -> body {error?}
  POST /api/status                -> bot reports its current state, body {platform, state, detail?}
  GET  /api/status                -> live per-platform state (dashboard polls this)
  GET  /                          -> the dashboard page (dark, auto-refreshing)

State lives in memory only (a handful of concurrent platforms, short-lived
requests) — restarting this process drops any requests that were mid-review
and any live status; the affected bot's next generate/status ping simply
creates a fresh one.
"""

import concurrent.futures
import hmac
import itertools
import os
import threading
import uuid
from datetime import datetime

from flask import Flask, jsonify, request, Response

try:
    from deep_translator import GoogleTranslator
except ImportError:  # translation is a nice-to-have — dashboard still works without it
    GoogleTranslator = None

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
    if not reply.strip():
        return jsonify({"error": "reply is required"}), 400

    # Best-effort DE->EN translation so a reviewer who doesn't read German can
    # still judge the reply. The German text is always what's authoritative /
    # editable / actually sent — translations are read-only context.
    reply_en = _translate_de_en(reply)
    customer_message_en = _translate_de_en(customer_message) if customer_message else None

    req_id = str(uuid.uuid4())
    with _lock:
        _requests[req_id] = {
            "id": req_id,
            "platform": platform,
            "customer_message": customer_message,
            "customer_message_en": customer_message_en,
            "context": body.get("context") or "",
            "reply": reply,
            "reply_en": reply_en,
            "final_reply": None,
            "status": "pending",
            "created_at": body.get("created_at") or _now(),
            "decided_at": None,
            "sent_at": None,
            "error": None,
            "_seq": next(_order),
        }
        _prune_history_locked()
    return jsonify({"id": req_id}), 201


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

function pendingCardHtml(r) {
  const val = editedReplies.has(r.id) ? editedReplies.get(r.id) : r.reply;
  const pc = colorFor(r.platform);
  return `
    <div class="card" data-id="${r.id}" style="--pc:${pc}">
      <div class="card-head">
        <span class="badge">${escapeHtml(r.platform)}</span>
        <span class="time">${timeAgo(r.created_at)}</span>
      </div>
      ${r.customer_message ? `
        <div class="field-label customer-label">Customer said <span class="lang-tag tag-de">DE</span></div>
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

function renderSections(pending) {
  const byPlatform = new Map();
  for (const r of pending) {
    if (!byPlatform.has(r.platform)) byPlatform.set(r.platform, []);
    byPlatform.get(r.platform).push(r);
  }

  // Known platforms first (stable order), then any unexpected ones seen live.
  const order = [...knownPlatforms];
  for (const name of byPlatform.keys()) if (!order.includes(name)) order.push(name);

  document.getElementById("sections").innerHTML = order.map(name => {
    const items = byPlatform.get(name) || [];
    const id = slug(name);
    const pc = colorFor(name);
    const det = detectorFor(name);
    const body = items.length
      ? `<div class="cards-grid">${items.map(pendingCardHtml).join("")}</div>`
      : `<div class="empty-state">No pending replies for ${escapeHtml(name)}.</div>`;
    return `
      <section class="platform-section" id="${id}" style="--pc:${pc}">
        <div class="platform-head">
          <span class="pc-dot"></span>
          <h2>${escapeHtml(name)}</h2>
          <span class="count-pill ${items.length ? "active" : ""}">${items.length} pending</span>
          <span class="detector-pill ${det.live ? "live" : "offline"}" style="--det:${det.color}" title="${escapeHtml(det.detail)}">
            <span class="det-dot"></span>${escapeHtml(det.label)}
          </span>
          <hr />
        </div>
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
    const [pendingRes, historyRes, statusRes] = await Promise.all([
      fetch("/api/requests?status=pending"),
      fetch("/api/requests?status=approved,rejected,cancelled,sent,failed&limit=200"),
      fetch("/api/status"),
    ]);
    liveStatus = await statusRes.json();
    renderSections(await pendingRes.json());
    renderHistory((await historyRes.json()).slice(0, 30));
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
