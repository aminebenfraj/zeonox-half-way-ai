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

try:
    from deep_translator import GoogleTranslator
except ImportError:  # translation is a nice-to-have — dashboard still works without it
    GoogleTranslator = None

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
#
# This is the DEFAULT for every platform. _mode_overrides (below) lets the
# three self-managed platforms opt out of it independently without touching
# this global — so flipping Xkuss to auto never affects Gold/Diamond/etc, and
# never affects Justlo/Linduu either.
_mode = "manual"

# Per-platform override of the review mode above, keyed by lowercase platform
# slug ("xkuss"/"justlo"/"linduu" — see /bots). Populated only when a platform
# has explicitly chosen something other than "use the global default"; a
# platform with no entry here just falls back to _mode. Read live on every
# request (see create_request()'s `_mode_overrides.get(...)` lookup) — no
# restart needed, same as the global toggle always worked.
_mode_overrides: dict[str, str] = {}

# Chameleon reply source, toggled per-platform on the /chameleon and /bots
# pages. "real" (default) leaves that bot's tab2 pointed at the actual
# Chameleon-AI site, unchanged. "local" tells it to paste HTML into and read
# the reply from OUR OWN /chameleon page instead (see core/chameleon_local.py)
# — read once at bot startup, so flipping this takes effect on that platform's
# next restart, not mid-run. Only the three self-managed platforms ever read
# this (core/xkuss_bot.py, core/justlo_bot.py) — React platforms never call
# chameleon_local at all, so they're structurally unaffected regardless.
SELF_MANAGED_PLATFORMS = ("xkuss", "justlo", "linduu")
_chameleon_source: dict[str, str] = {p: "real" for p in SELF_MANAGED_PLATFORMS}

# ── Bot process management (see /bots) ──────────────────────────────────────
# Only the three self-managed platforms: each does its own Chrome launch +
# login inside run_bot.py, so starting one is just spawning that one process
# -- no Playwright-based Chrome/login bring-up needed here (that stays in
# launch_all.py, kept deliberately out of this process; see KNOWN_PLATFORMS
# above for the same reasoning). React platforms (Gold/Diamond/...) still
# only start via launch_all.py / Bot Controls, unchanged.
_BASE_DIR = Path(__file__).resolve().parent
_SELF_MANAGED_CDP_PORTS = {"xkuss": 9227, "justlo": 9229, "linduu": 9230}
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


@app.get("/api/chameleon/prompt")
def chameleon_prompt():
    """Read-only: the literal system prompt Groq gets for the local Chameleon
    reply generator (see _generate_chameleon_reply below) -- shown on /bots so
    "Local" source is never a black box."""
    return jsonify({"prompt": _CHAMELEON_SYSTEM_PROMPT})


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


# ── Chameleon standalone reply generator (Groq) ─────────────────────────────
# Powers the "/chameleon" page: paste raw chat HTML copied from the Xkuss or
# Justlo/Linduu mod site, extract it client-side (ported from those platforms'
# own Chameleon-AI extractor JS — see _CHAMELEON_PAGE below), and get back a
# single contextual reply from Groq. No browser automation and no real
# Chameleon-AI tab required, so this works even when no bot is running —
# it's a manual test/preview tool, independent of the approval queue above.
CHAMELEON_MODEL = "openai/gpt-oss-120b"
_CHAMELEON_SYSTEM_PROMPT = """You are ghostwriting the next chat message for an operator account ("Ich") in an ongoing conversation with a client ("Kunde") on a dating/chat platform.

You are given the recent conversation history and optional profile details for both sides. Write ONLY the single next message "Ich" should send.

Before writing, read the WHOLE transcript, not just the last line: track what has already been said (topics raised, questions asked, jokes made, things "Ich" already claimed) so the reply is consistent with it and never contradicts or repeats earlier ground.

Context modes:
- FC (Erstkontakt): no conversation yet — write a short, friendly opening message.
- DIA (Dialog): the client just sent the last message — this is what you are replying to. Actually respond to what they said: pick up on the specific thing they mentioned or asked (react to it, answer a question if they asked one, riff on a detail they gave) instead of a generic reply that could follow any message. Stay on topic.
- ASA (Nachfassen): "Ich" sent the last message and the client has gone quiet — write a short, natural follow-up, not a repeat of the last message.

Hard rules:
- Always write in German, regardless of the language used in the conversation.
- Match the client's energy: mirror their message length, enthusiasm, and tone — short and dry gets short and dry back, playful and eager gets playful and eager back, low-effort one-liners get a low-effort one-liner back. Never reply longer or more enthusiastic than the client's last message.
- Keep the conversation moving naturally: build on what was just said rather than changing the subject or restarting small talk, and vary your phrasing from "Ich"'s earlier messages in the transcript — don't reuse the same opener, question, or turn of phrase twice.
- Never use curse words, insults, or vulgar language of any kind, in German or any other language.
- Never use an exclamation mark.
- Never use an apostrophe.
- Never use the German umlauts ä or ü — write plain a / u instead (e.g. "naturlich" not "natürlich", "war" not "wär", "fur" not "für").
- Never propose, schedule, plan, or firmly agree to a real-life meeting — no dates, times, locations, activities, phone numbers, or addresses. Never say yes to a meeting the client proposes either. If the client brings it up, stall, deflect, or keep it vague and open instead of engaging with the specifics.
- Never use "haha" or similar filler laughter.
- Never sound like an AI assistant and never break character.
- Reply with ONLY the message text itself — no labels, no quotes, no explanation.

Respond ONLY with valid JSON, no markdown fences, in this exact shape:
{"reply": "the message text"}"""

_CHAMELEON_VALIDATOR_SYSTEM_PROMPT = """You are QA-checking a drafted chat reply before it gets sent, on behalf of the operator account ("Ich") in a conversation with a client ("Kunde").

You are given the conversation mode, the recent transcript, and the drafted reply. Check it against two things, in order:

1. fits_conversation: does the reply actually respond to what's going on? For DIA it must react to the specific thing the client just said (not a generic reply that could follow anything); for ASA it must be a natural follow-up, not a restatement of "Ich"'s last message; for FC it must be a short friendly opener. It must not contradict or ignore anything already established in the transcript, and must not repeat a phrase, question, or opener "Ich" already used earlier in the transcript.

2. follows_rules: does the reply comply with every one of these house rules?
- Written in German.
- Matches the client's energy — not longer or more enthusiastic than the client's last message.
- No curse words, insults, or vulgar language.
- No exclamation mark, no apostrophe, no a/u-umlaut (a/u only, never ä/ü).
- No "haha" or similar filler laughter.
- Never proposes, schedules, agrees to, or engages with the specifics of a real-life meeting (no dates, times, locations, activities, phone numbers, addresses) — if the client brought it up, the reply must stall/deflect/stay vague instead.
- Doesn't sound like an AI assistant and doesn't break character.

Respond ONLY with valid JSON, no markdown fences, in this exact shape:
{"fits_conversation": true or false, "follows_rules": true or false, "reason": "short explanation of what's wrong, in English, empty string if both are true"}"""


def _validate_chameleon_reply(mode_line: str, transcript: str, reply: str) -> dict:
    """Second Groq pass that QA-checks a drafted reply against (1) whether it
    actually fits the conversation and (2) whether it follows the hard style
    rules from _CHAMELEON_SYSTEM_PROMPT, so a reply that technically parsed as
    JSON but missed the mark gets caught and retried instead of shipped as-is.

    Best-effort like the rest of this module: any failure to reach Groq or
    parse its verdict returns {"ok": True} (i.e. skip the check) rather than
    blocking reply generation on the validator itself being unavailable — the
    validator is a quality gate on top of generation, not a new point of
    failure for it."""
    client = _get_groq_test_client()
    if client is None:
        return {"ok": True}

    user_content = f"{mode_line}\n\nBisherige Unterhaltung:\n{transcript}\n\nEntwurf von \"Ich\": {reply}"

    try:
        resp = client.chat.completions.create(
            model=CHAMELEON_MODEL,
            messages=[
                {"role": "system", "content": _CHAMELEON_VALIDATOR_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            temperature=0,
            max_completion_tokens=300,
            top_p=1,
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content or "{}")
    except Exception:
        return {"ok": True}

    fits = bool(data.get("fits_conversation", True))
    follows = bool(data.get("follows_rules", True))
    if fits and follows:
        return {"ok": True}
    return {"ok": False, "reason": (data.get("reason") or "").strip() or "failed quality check"}


_MODE_LABELS = {
    "FC": "Modus: FC (Erstkontakt) - es gibt noch keine Unterhaltung. Schreibe eine kurze, freundliche Eroeffnungsnachricht.",
    "DIA": "Modus: DIA (Dialog) - der Kunde hat zuletzt geschrieben. Schreibe die Antwort auf seine letzte Nachricht.",
    "ASA": "Modus: ASA (Nachfassen) - Ich habe zuletzt geschrieben und der Kunde hat noch nicht geantwortet. Schreibe eine kurze, natuerliche Anschlussnachricht, keine Wiederholung der letzten Nachricht.",
}

_BANG_APOSTROPHE_RE = re.compile(r"[!’']")
_UMLAUT_MAP = str.maketrans({"ä": "a", "Ä": "A", "ü": "u", "Ü": "U"})


def _sanitize_chameleon_reply(text: str) -> str:
    """Same dash-strip as the meeting guard, plus enforcing the "no !, no
    apostrophe, no a/u-umlaut" house style server-side regardless of whether
    the model actually complied (see _sanitize_meeting_reply for the same
    pattern)."""
    text = _BANNED_DASH_RE.sub("", text or "")
    text = _BANG_APOSTROPHE_RE.sub(lambda m: "." if m.group(0) == "!" else "", text)
    text = text.translate(_UMLAUT_MAP)
    return re.sub(r" {2,}", " ", text).strip()


def _format_profile(profile: dict) -> str:
    if not isinstance(profile, dict) or not profile:
        return "(keine Angaben)"
    parts = [f"{k}: {v}" for k, v in profile.items() if v]
    return "; ".join(parts) if parts else "(keine Angaben)"


def _format_transcript(conversation: list) -> str:
    lines = []
    for m in (conversation or [])[-10:]:
        text = (m.get("text") or "").strip() if isinstance(m, dict) else ""
        if not text:
            continue
        role = "Ich" if (isinstance(m, dict) and m.get("sender") == "fake_account") else "Kunde"
        lines.append(f"{role}: {text}")
    return "\n".join(lines) if lines else "(noch keine Nachrichten)"


def _generate_chameleon_reply(message_type: str, conversation: list, client_profile: dict,
                               fake_profile: dict, additional_instructions: str) -> dict:
    client = _get_groq_test_client()
    if client is None:
        reason = "groq package not installed" if Groq is None else "GROQ_API_KEY is not set"
        return {"ok": False, "error": reason, "status": 400}

    mode_line = _MODE_LABELS.get(message_type, _MODE_LABELS["DIA"])
    user_content = (
        f"{mode_line}\n\n"
        f"Kunde-Profil: {_format_profile(client_profile)}\n"
        f"Ich-Profil (mein Account): {_format_profile(fake_profile)}\n\n"
        f"Bisherige Unterhaltung:\n{_format_transcript(conversation)}"
    )
    if additional_instructions:
        user_content += f"\n\nZusatzanweisung: {additional_instructions}"

    # Groq's own structured-output validation ("json_validate_failed" / "Failed
    # to validate JSON") and an occasionally-malformed completion are transient
    # model hiccups, not a real problem with the request -- a same-prompt retry
    # usually succeeds (confirmed in practice: the very next click often works),
    # so retry a couple of times here instead of making every glitch someone's
    # problem to notice and retry by hand.
    max_attempts = 3
    last_error = "unknown error"
    for attempt in range(1, max_attempts + 1):
        try:
            resp = client.chat.completions.create(
                model=CHAMELEON_MODEL,
                messages=[
                    {"role": "system", "content": _CHAMELEON_SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                temperature=0.8,
                max_completion_tokens=600,
                top_p=1,
                response_format={"type": "json_object"},
            )
        except Exception as e:
            last_error = str(e)
            if "json_validate_failed" in last_error or "Failed to validate JSON" in last_error:
                continue
            return {"ok": False, "error": last_error, "status": 502}

        raw = resp.choices[0].message.content or "{}"
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            last_error = "model returned invalid JSON"
            continue

        reply = _sanitize_chameleon_reply(data.get("reply") or "")
        if not reply:
            last_error = "model returned an empty reply"
            continue

        # Double check: a second Groq pass QA's the draft against (1) whether
        # it actually fits the conversation and (2) whether it follows the
        # hard rules, before it's shown to anyone. A rejected draft is treated
        # like any other failed attempt — regenerated, not shipped — and the
        # rejection reason is fed back into the next attempt's prompt so the
        # retry actually addresses it instead of blindly resampling.
        verdict = _validate_chameleon_reply(mode_line, _format_transcript(conversation), reply)
        if not verdict["ok"]:
            last_error = f"quality check failed: {verdict['reason']}"
            user_content += (
                f"\n\nDer vorherige Entwurf wurde abgelehnt: {verdict['reason']}. "
                f"Schreibe einen neuen Vorschlag, der das behebt."
            )
            continue

        return {"ok": True, "reply": reply, "reply_en": _translate_de_en(reply)}

    return {"ok": False, "error": f"{last_error} (after {max_attempts} attempts)", "status": 502}


@app.post("/api/chameleon/translate")
def chameleon_translate():
    body = request.get_json(force=True, silent=True) or {}
    text = (body.get("text") or "").strip()
    if not text:
        return jsonify({"ok": False, "error": "text is required"}), 400
    translated = _translate_de_en(text)
    if translated is None:
        return jsonify({"ok": False, "error": "translation unavailable"}), 502
    return jsonify({"ok": True, "translated": translated})


def _restart_bot(platform: str):
    """Best-effort: ask launch_all.py's control server (if reachable) to
    restart exactly this one platform, so a flipped toggle wins immediately
    instead of sitting there until someone remembers to click Restart. Runs
    in a background thread — the caller never blocks on this. Silently does
    nothing if the control server isn't up (e.g. the bot was started
    standalone via run_bot.py, or via the /bots page's own process manager
    rather than launch_all.py) — restart is a convenience on top of the
    toggle, not a requirement for it to take effect eventually. Deliberately
    scoped to ONE platform: flipping Xkuss's settings must never restart, or
    otherwise touch, Justlo/Linduu or any React platform."""
    try:
        httpx.post(
            f"{LAUNCHER_CONTROL_URL}/control/command",
            json={"cmd": "restart", "target": platform},
            timeout=15,
        )
    except Exception:
        pass


@app.get("/api/chameleon/source")
def get_chameleon_source():
    platform = (request.args.get("platform") or "").strip().lower()
    if platform not in SELF_MANAGED_PLATFORMS:
        return jsonify({"error": f"platform must be one of {SELF_MANAGED_PLATFORMS}"}), 400
    with _lock:
        return jsonify({"platform": platform, "source": _chameleon_source[platform]})


@app.post("/api/chameleon/source")
def set_chameleon_source():
    body = request.get_json(force=True, silent=True) or {}
    platform = (body.get("platform") or "").strip().lower()
    if platform not in SELF_MANAGED_PLATFORMS:
        return jsonify({"error": f"platform must be one of {SELF_MANAGED_PLATFORMS}"}), 400
    source = body.get("source")
    if source not in ("real", "local"):
        return jsonify({"error": "source must be 'real' or 'local'"}), 400
    with _lock:
        changed = _chameleon_source[platform] != source
        _chameleon_source[platform] = source
        if changed:
            # Surface the restart-in-progress immediately (rather than leaving
            # the detector on its last real status until the bot reconnects
            # and pings again) so the dashboard visibly reflects that flipping
            # this toggle just kicked off a restart, live, with no page reload
            # needed to see it.
            _status[platform.capitalize()] = {
                "state": "restarting",
                "detail": f"Switching to {'Built-in Groq' if source == 'local' else 'Real Chameleon-AI'}…",
                "updated_at": _now(),
            }
    _bump_state()
    if changed:
        threading.Thread(target=_restart_bot, args=(platform,), daemon=True).start()
    return jsonify({"ok": True, "source": source})


@app.post("/api/chameleon/reply")
def chameleon_reply():
    body = request.get_json(force=True, silent=True) or {}
    platform = (body.get("platform") or "").strip()
    if platform not in ("xkuss", "justlo_lindu"):
        return jsonify({"ok": False, "error": "platform must be 'xkuss' or 'justlo_lindu'"}), 400
    message_type = (body.get("message_type") or "DIA").strip().upper()
    if message_type not in _MODE_LABELS:
        message_type = "DIA"
    conversation = body.get("conversation") or []
    if message_type != "FC" and not conversation:
        return jsonify({"ok": False, "error": "conversation is empty"}), 400

    result = _generate_chameleon_reply(
        message_type, conversation,
        body.get("client_profile") or {}, body.get("fake_profile") or {},
        (body.get("additional_instructions") or "").strip(),
    )
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
    _bump_state()
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
        changed = result.get("available") != _control_status_cached().get("available")
        with _control_cache_lock:
            _control_cache = result
        if changed:
            _bump_state()  # controls coming online/offline is worth an immediate push too
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
                        _state_cond.wait(timeout=5.0)  # periodic wake, purely as a safety net
                        changed = _state_version != last_version
                    last_version = _state_version
                if not changed:
                    continue
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
    <a href="/bots" class="sound-toggle" style="margin-top:8px;text-decoration:none;">🤖 Bots (start / stop / configure)</a>
    <a href="/chameleon" class="sound-toggle" style="margin-top:8px;text-decoration:none;">🦎 Chameleon (standalone)</a>
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
};

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
  restarting:         { label: "Restarting…",            color: "var(--info)" },
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
        <button class="btn-approve" onclick="approveCard('${r.id}', this)">Approve &amp; Send</button>
        <button class="btn-reject" onclick="act('${r.id}', 'reject', null, this)">Reject &amp; Regenerate</button>
        <button class="btn-cancel" onclick="cancelCard('${r.id}', this)">Cancel</button>
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

  renderSystemStatus();
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
    await refreshControlStatus();
    applySnapshot({
      status: await statusRes.json(),
      mode: ((await modeRes.json()).mode) || "manual",
      control: { available: controlsAvailable, platforms: controlPlatformState },
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
      label: `${onlineCount}/${knownPlatforms.length} bots reporting in`,
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


# ── Chameleon standalone page ───────────────────────────────────────────────
# Paste raw chat HTML from Xkuss or Justlo/Linduu, extract it in-browser (ported
# from those platforms' own Chameleon-AI extractor React components — pure DOM
# code, no framework needed), then get one contextual reply from Groq via
# POST /api/chameleon/reply. Entirely separate from the approval queue above:
# nothing here is sent anywhere or needs a bot/browser running.
_CHAMELEON_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Chameleon — Standalone</title>
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
  .topbar { display: flex; align-items: center; gap: 12px; margin-bottom: 18px; flex-wrap: wrap; }
  .back-link {
    font-size: 12.5px; color: var(--muted-foreground); text-decoration: none;
    border: 1px solid var(--border); border-radius: 7px; padding: 6px 11px;
  }
  .back-link:hover { color: var(--foreground); background: var(--accent); }
  .topbar h1 { font-size: 19px; margin: 0; letter-spacing: -.01em; }
  .topbar p { margin: 3px 0 0; color: var(--muted-foreground); font-size: 13px; }
  .auto-mode-box {
    background: var(--card); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 14px 18px; margin-top: 16px;
  }
  .auto-mode-hint { margin: 7px 1px 0; font-size: 11.5px; color: var(--muted-foreground); line-height: 1.45; }

  .platform-tabs { display: flex; gap: 8px; margin: 16px 0 20px; }
  .platform-tab {
    font: inherit; font-weight: 650; font-size: 13px; padding: 9px 16px;
    border-radius: 8px; border: 1px solid var(--border); background: var(--card);
    color: var(--muted-foreground); cursor: pointer; transition: filter .12s;
  }
  .platform-tab.active { background: var(--primary); color: #fff; border-color: transparent; }
  .platform-tab:hover { filter: brightness(1.1); }

  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; align-items: start; }
  @media (max-width: 920px) { .grid { grid-template-columns: 1fr; } }

  .card {
    background: var(--card); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 16px 18px; margin-bottom: 16px;
  }
  .card h2 { font-size: 13.5px; margin: 0 0 10px; font-weight: 650; text-transform: uppercase; letter-spacing: .04em; color: var(--muted-foreground); }

  textarea {
    width: 100%; resize: vertical; background: var(--muted); color: var(--foreground);
    border: 1px solid var(--input); border-radius: 8px; padding: 9px 11px;
    font: inherit; font-size: 13px;
  }
  textarea:focus { outline: none; border-color: var(--ring); box-shadow: 0 0 0 3px rgba(99,102,241,.22); }
  #htmlInput { min-height: 160px; font-family: ui-monospace, "SF Mono", Consolas, monospace; font-size: 12px; }
  #instructionsInput { min-height: 56px; }

  button {
    font: inherit; font-weight: 600; font-size: 13px; border: 1px solid transparent;
    border-radius: 7px; padding: 8px 15px; cursor: pointer; transition: filter .12s;
  }
  button:hover { filter: brightness(1.08); }
  button:disabled { opacity: .55; cursor: default; }
  .btn-primary { background: var(--success); color: #052e16; }
  .btn-groq { background: var(--primary); color: #fff; }
  .btn-ghost { background: transparent; color: var(--muted-foreground); border-color: var(--border); }

  .error-box {
    color: var(--destructive); background: rgba(239,68,68,.1); border: 1px solid rgba(239,68,68,.3);
    border-radius: 8px; padding: 9px 12px; font-size: 13px; margin-top: 10px;
  }
  .hint { color: var(--muted-foreground); font-size: 11.5px; margin-top: 8px; }

  .type-badge {
    display: inline-block; font-size: 10.5px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .04em; padding: 3px 9px; border-radius: 999px; margin-bottom: 12px;
  }
  .type-badge.fc { background: rgba(34,197,94,.15); color: var(--success); }
  .type-badge.dia { background: rgba(56,189,248,.15); color: var(--info); }
  .type-badge.asa { background: rgba(167,139,250,.15); color: var(--violet); }
  .last-msg-translation { margin-bottom: 12px; }

  .bubbles { display: flex; flex-direction: column; gap: 6px; max-height: 280px; overflow-y: auto; margin-bottom: 4px; }
  .bubble { border-radius: 8px; padding: 8px 10px; border-left: 3px solid var(--border); background: var(--muted); }
  .bubble.client { border-left-color: var(--info); }
  .bubble.fake_account { border-left-color: #ec4899; }
  .bubble-meta { display: flex; gap: 8px; align-items: center; margin-bottom: 3px; }
  .bubble-role { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: .03em; }
  .bubble.client .bubble-role { color: var(--info); }
  .bubble.fake_account .bubble-role { color: #f9a8d4; }
  .bubble-time { font-size: 10.5px; color: var(--muted-foreground); }
  .bubble-text { font-size: 13px; white-space: pre-wrap; }
  .empty-note { color: var(--muted-foreground); font-size: 12.5px; padding: 10px 2px; }

  details.profile-block { margin-top: 10px; border-top: 1px solid var(--border); padding-top: 8px; }
  details.profile-block summary { cursor: pointer; font-size: 12px; font-weight: 650; color: var(--muted-foreground); }
  details.profile-block summary:hover { color: var(--foreground); }
  .profile-row { display: flex; gap: 8px; padding: 5px 0; font-size: 12.5px; border-bottom: 1px solid var(--border); }
  .profile-row .k { color: var(--muted-foreground); min-width: 130px; flex: none; }

  .field-label { font-size: 10.5px; font-weight: 700; text-transform: uppercase; letter-spacing: .05em; margin: 4px 0 6px; color: var(--violet); }
  .de-box { background: var(--muted); border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; white-space: pre-wrap; font-size: 14px; }
  .en-box { color: var(--info); opacity: .85; font-style: italic; font-size: 12.5px; white-space: pre-wrap; margin-top: 7px; }
  .result-actions { display: flex; gap: 8px; margin-top: 10px; align-items: center; }
  .copied-note { font-size: 12px; color: var(--success); }
</style>
</head>
<body>
<div class="topbar">
  <a class="back-link" href="/">&larr; Approval Dashboard</a>
  <div>
    <h1>Chameleon — Standalone</h1>
    <p>Paste raw chat HTML, extract it locally, get one contextual reply from Groq. No bot or browser tab needed.</p>
  </div>
</div>

<div class="auto-mode-box">
  <p class="auto-mode-hint" style="margin:0;">
    This page is just a testing tool — pasting HTML and generating a reply here never affects any live bot.
    To turn a live bot's Automatic Mode on or off (per-platform: Xkuss, Justlo and Linduu are each independent),
    use <a href="/bots" style="color:var(--foreground);text-decoration:underline;">the Bots page</a>.
  </p>
</div>

<div class="platform-tabs">
  <button class="platform-tab active" id="tab-justlo" onclick="setPlatform('justlo_lindu')">Justlo / Linduu</button>
  <button class="platform-tab" id="tab-xkuss" onclick="setPlatform('xkuss')">Xkuss</button>
</div>

<div class="grid">
  <div class="card">
    <h2 id="pasteLabel">Justlo / Linduu — HTML einfügen</h2>
    <textarea id="htmlInput" placeholder="Ganzen HTML-Quellcode der Moderationsseite hier einfügen..."></textarea>
    <div style="margin-top:10px;display:flex;gap:8px;align-items:center;">
      <button class="btn-primary" onclick="doExtract()">Daten extrahieren</button>
      <span class="hint" id="extractStatus"></span>
    </div>
    <div id="extractError" class="error-box" style="display:none"></div>
  </div>

  <div class="card" id="extractedCard" style="display:none">
    <h2>Extrahierte Konversation</h2>
    <span class="type-badge" id="typeBadge"></span>
    <div class="last-msg-translation" id="lastMsgTranslation" style="display:none">
      <div class="field-label">Last message <span style="color:var(--muted-foreground);text-transform:none;letter-spacing:0;">EN</span></div>
      <div class="en-box" id="lastMsgTranslationText" style="margin-top:0;"></div>
    </div>
    <!-- Hidden, not a visible field: the raw DE text of the literal last
         message in the conversation (either side), read back by
         chameleon_local.py via Playwright — in local mode straight off this
         same tab, in real Chameleon-AI mode via a throwaway visit to this
         page just for the extraction (see extract_last_message()) — so the
         approval dashboard's "Last Message" card can show it next to the
         generated reply regardless of which mode produced that reply. -->
    <div id="lastMsgDe" style="display:none"></div>
    <div class="bubbles" id="bubbles"></div>
    <details class="profile-block">
      <summary>Kunde-Profil</summary>
      <div id="clientProfile"></div>
    </details>
    <details class="profile-block">
      <summary>Ich-Profil (mein Account)</summary>
      <div id="fakeProfile"></div>
    </details>
  </div>
</div>

<div class="card" id="generateCard" style="display:none">
  <h2>Antwort generieren (Groq)</h2>
  <textarea id="instructionsInput" placeholder="Zusatzanweisungen (optional), z. B. 'etwas flirtender'..."></textarea>
  <div style="margin-top:10px;display:flex;gap:8px;align-items:center;">
    <button class="btn-groq" id="genBtn" onclick="doGenerate()">Antwort generieren</button>
    <span class="hint" id="genStatus"></span>
  </div>
  <div id="genError" class="error-box" style="display:none"></div>
  <div id="genResult" style="display:none;margin-top:14px;">
    <div class="field-label">Antwort <span style="color:var(--muted-foreground);text-transform:none;letter-spacing:0;">DE</span></div>
    <div class="de-box" id="replyDe"></div>
    <div class="en-box" id="replyEn"></div>
    <div class="result-actions">
      <button class="btn-ghost" onclick="copyReply()">Kopieren</button>
      <span class="copied-note" id="copiedNote" style="display:none">Kopiert!</span>
    </div>
  </div>
</div>

<script>
const escapeHtml = (s) => (s || "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));

// ── Justlo / Linduu extractor (ported from the Chameleon-AI extractor React
// component pasted into this project — the DOM logic itself never depended on
// React, so it's lifted verbatim into a plain namespace). ─────────────────────
const JustloExtractor = (function () {
  const CLIENT_COLOR  = 'rgb(204, 204, 255)';
  const FAKE_COLOR    = 'rgb(255, 204, 204)';
  const DETAILS_COLOR = 'rgb(223, 233, 246)';
  const TIMESTAMP_RE  = /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})|(\d{1,2}\.\d{1,2}\.\d{2,4}\s+\d{2}:\d{2}(:\d{2})?)/;
  const BIRTHDATE_RE  = /(\d{1,2}\.\s*[A-Za-zÀ-ÿ]{3,}\s+\d{4})|(\d{4}-\d{2}-\d{2})|(\d{1,2}[.\/-]\d{1,2}[.\/-]\d{2,4})/;
  const RECORD_TR_RE  = /gridview-\d+-record-ext-record-\d+/;

  function cleanText(v) {
    if (!v) return '';
    return v.replace(/ /g, ' ').replace(/\s+/g, ' ').trim();
  }
  function normalizeKey(label) {
    let t = cleanText(label).replace(/:$/, '');
    if (!t) return '';
    return t.normalize('NFKD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
  }
  function hasProfileLink(el) {
    return !!el && !!el.querySelector('a[href*="/community/profile/show/username/"]');
  }
  function looksLikeTimestamp(text) { return TIMESTAMP_RE.test(cleanText(text)); }

  function parseTimestamp(text) {
    const t = cleanText(text);
    const formats = [
      [/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/, (m) => new Date(+m[1],+m[2]-1,+m[3],+m[4],+m[5],+m[6])],
      [/^(\d{1,2})\.(\d{1,2})\.(\d{2,4}) (\d{2}):(\d{2}):(\d{2})$/, (m) => new Date(+m[3]<100?2000+ +m[3]:+m[3],+m[2]-1,+m[1],+m[4],+m[5],+m[6])],
      [/^(\d{1,2})\.(\d{1,2})\.(\d{2,4}) (\d{2}):(\d{2})$/, (m) => new Date(+m[3]<100?2000+ +m[3]:+m[3],+m[2]-1,+m[1],+m[4],+m[5],0)],
    ];
    for (const [re, fn] of formats) {
      const m = t.match(re);
      if (m) { try { return fn(m); } catch (e) { /* try next format */ } }
    }
    return null;
  }

  function isProfileCard(el, color) {
    if (!el || el.tagName !== 'DIV') return false;
    const style = el.getAttribute('style') || '';
    if (color && !style.includes(color)) return false;
    return hasProfileLink(el) && !!el.querySelector('a[href*="/images/gallery/"]');
  }
  function findProfileCard(doc, color) {
    for (const div of Array.from(doc.querySelectorAll('div[style]'))) {
      const style = div.getAttribute('style') || '';
      if (style.includes(color) && isProfileCard(div, color)) return div;
    }
    return null;
  }
  function countProfileCards(container) {
    let count = 0;
    for (const div of Array.from(container.querySelectorAll('div[style]'))) {
      const style = div.getAttribute('style') || '';
      if ((style.includes(CLIENT_COLOR) || style.includes(FAKE_COLOR)) && hasProfileLink(div)) count++;
    }
    return count;
  }
  function countDetailsPanels(container) {
    let count = 0;
    for (const div of Array.from(container.querySelectorAll('div[style]'))) {
      if ((div.getAttribute('style')||'').includes(DETAILS_COLOR) && div.querySelector('label')) count++;
    }
    return count;
  }
  function findProfileContainer(card) {
    let parent = card.parentElement;
    while (parent) {
      if (parent.tagName === 'DIV' && countProfileCards(parent) === 1 && countDetailsPanels(parent) >= 1) return parent;
      parent = parent.parentElement;
    }
    return null;
  }
  function findDetailsPanel(container) {
    if (!container) return null;
    let best = null, bestScore = -1;
    for (const div of Array.from(container.querySelectorAll('div[style]'))) {
      if (!(div.getAttribute('style')||'').includes(DETAILS_COLOR)) continue;
      const score = div.querySelectorAll('label').length;
      if (score > bestScore) { bestScore = score; best = div; }
    }
    return best;
  }

  function extractCardHeader(card) {
    const header = {};
    const prev = card.previousElementSibling;
    if (!prev) return header;
    const text = cleanText(prev.textContent);
    if (!text) return header;
    const tsMatch = text.match(TIMESTAMP_RE);
    if (tsMatch) {
      header.panel_timestamp = tsMatch[0];
      const title = cleanText(text.replace(tsMatch[0], ''));
      if (title) header.panel_title = title;
    } else {
      header.panel_title = text;
    }
    return header;
  }

  function extractSummaryDetails(summaryP) {
    const details = {};
    const text = cleanText(summaryP.textContent);
    const ageM = text.match(/\((\d{1,3})\)/);
    if (ageM) details.age = ageM[1];
    const bdM = text.match(BIRTHDATE_RE);
    if (bdM) details.birthdate = cleanText(bdM[0]);

    const interests = [], rawBadges = [];
    for (const span of Array.from(summaryP.querySelectorAll(':scope > span'))) {
      const title = cleanText(span.getAttribute('title'));
      const spanText = cleanText(span.textContent);
      if (span.querySelector('a[href*="/community/profile/show/username/"]')) continue;
      const rel  = span.querySelector('span[title="Beziehungsstatus"]');
      const look = span.querySelector('span[title="Auf der Suche nach"]');
      if (rel)  { details.relationship_status = cleanText(rel.textContent);  continue; }
      if (look) { details.looking_for         = cleanText(look.textContent); continue; }
      if (title) { interests.push(title); continue; }
      if (spanText) rawBadges.push(spanText);
    }
    const extraBadges = [];
    for (const badge of rawBadges) {
      if (badge.includes('>') && !details.direction)  { details.direction = badge; continue; }
      if (/^\d+$/.test(badge) && !details.points)     { details.points    = badge; continue; }
      if (/\b\d{4,5}\b/.test(badge) && !details.location) {
        details.location = badge;
        const pm = badge.match(/\b(\d{4,5})\b/);
        if (pm) details.postal_code = pm[1];
        continue;
      }
      extraBadges.push(badge);
    }
    if (interests.length)   details.interests      = [...new Set(interests)];
    if (extraBadges.length) details.summary_badges = [...new Set(extraBadges)];
    return details;
  }

  function extractDetailsPanel(panel) {
    const details = {};
    if (!panel) return details;
    for (const table of Array.from(panel.querySelectorAll('table'))) {
      const labelTag = table.querySelector('label');
      if (!labelTag) continue;
      const key = normalizeKey(labelTag.textContent);
      if (!key) continue;
      let value = '';
      const field = table.querySelector('input, textarea, select');
      if (field) {
        if (field.tagName === 'SELECT') {
          const opt = field.querySelector('option[selected]');
          value = cleanText(opt ? opt.textContent : field.value);
        } else {
          value = cleanText(field.value || field.textContent);
        }
      }
      if (!value) {
        const td = table.querySelector('td:last-child');
        if (td && !td.querySelector('input, textarea, select')) value = cleanText(td.textContent);
      }
      if (key && value) details[key] = value;
    }
    return details;
  }

  function buildProfile(card) {
    const profile = { username: '', age: '', birthdate: '', profile_images: [], bio: '', additional_details: {} };
    if (!card) return profile;
    const container    = findProfileContainer(card);
    const detailsPanel = findDetailsPanel(container);
    const headerDetails = extractCardHeader(card);

    const usernameLink = card.querySelector('a[href*="/community/profile/show/username/"]');
    if (usernameLink) profile.username = cleanText(usernameLink.textContent);

    profile.profile_images = [...new Set(
      Array.from(card.querySelectorAll('a[href*="/images/gallery/"]')).map(a => a.getAttribute('href')).filter(Boolean)
    )];

    const paragraphs = Array.from(card.querySelectorAll('p')).filter(p => cleanText(p.textContent));
    const summaryP = paragraphs.find(p => hasProfileLink(p)) || null;
    const bioP     = paragraphs.find(p => p !== summaryP)    || null;

    const summaryDetails = summaryP ? extractSummaryDetails(summaryP) : {};
    if (summaryDetails.age)       profile.age       = String(summaryDetails.age);
    if (summaryDetails.birthdate) profile.birthdate = String(summaryDetails.birthdate);
    if (bioP) profile.bio = cleanText(bioP.textContent);

    const additional = {};
    for (const src of [headerDetails, summaryDetails, extractDetailsPanel(detailsPanel)]) {
      for (const [k, v] of Object.entries(src)) {
        if (k === 'age' || k === 'birthdate') continue;
        if (v !== '' && v !== null && !(Array.isArray(v) && v.length === 0)) additional[k] = v;
      }
    }
    if (!profile.birthdate && (additional.birthday || additional.geburtstag))
      profile.birthdate = additional.birthday || additional.geburtstag;
    if (!profile.age && (additional.age || additional.alter))
      profile.age = String(additional.age || additional.alter);

    profile.additional_details = additional;
    return profile;
  }

  function parseGridMessages(panelEl) {
    if (!panelEl) return [];
    const messages = [];
    const recordTrs = Array.from(panelEl.querySelectorAll('tr[id]'))
      .filter(tr => RECORD_TR_RE.test(tr.id));
    for (const tr of recordTrs) {
      const cells = Array.from(tr.querySelectorAll(':scope > td'));
      if (cells.length < 3) continue;
      const from      = cleanText(cells[0].textContent);
      const to        = cleanText(cells[1].textContent);
      const timestamp = cleanText(cells[2].textContent);
      const moderator = cells.length > 3 ? cleanText(cells[3].textContent) : '';
      if (!TIMESTAMP_RE.test(timestamp)) continue;
      const nextTr = tr.nextElementSibling;
      const message = nextTr ? cleanText(nextTr.textContent) : '';
      if (!message) continue;
      if (/^\*{2,}.*\*{2,}$/.test(message)) continue;
      const has_moderator = moderator.length > 0 && /[A-Z]{2,}[-_]\w+/.test(moderator);
      messages.push({ from, to, timestamp, moderator, has_moderator, message });
    }
    return messages;
  }

  function extractMessagesFromTable(table) {
    const body = table.querySelector('tbody') || table;
    const rows = Array.from(body.querySelectorAll(':scope > tr'));
    const messages = [];
    let i = 0;
    while (i < rows.length) {
      const row  = rows[i];
      const cls  = row.getAttribute('class') || '';
      const cells = Array.from(row.querySelectorAll(':scope > td'));
      if (cls.includes('x-grid-data-row') && cells.length >= 3) {
        const sender    = cleanText(cells[0].textContent);
        const recipient = cleanText(cells[1].textContent);
        const timestamp = cleanText(cells[2].textContent);
        const moderator = cells.length > 3 ? cleanText(cells[3].textContent) : '';
        if (looksLikeTimestamp(timestamp)) {
          let message = '', rowType = '';
          if (i + 1 < rows.length) {
            const nextRow = rows[i + 1];
            const nextCls = nextRow.getAttribute('class') || '';
            if (nextCls.includes('rowbody')) {
              message = cleanText(nextRow.textContent);
              const typeMatch = nextCls.match(/type-(\w+)/);
              rowType = typeMatch ? typeMatch[1] : '';
              i++;
            }
          }
          if (!message && cells.length >= 5) message = cleanText(cells[4].textContent);
          const has_moderator = moderator.length > 0 && /[A-Z]{2,}[-_]\w+/.test(moderator);
          messages.push({ from: sender, to: recipient, timestamp, message, moderator, has_moderator, row_type: rowType });
        }
      } else if (!cls.includes('rowbody') && cells.length >= 5) {
        const sender    = cleanText(cells[0].textContent);
        const recipient = cleanText(cells[1].textContent);
        const timestamp = cleanText(cells[2].textContent);
        const moderator = cleanText(cells[3].textContent);
        if (looksLikeTimestamp(timestamp)) {
          const message = cleanText(cells[4].textContent);
          const has_moderator = moderator.length > 0 && /[A-Z]{2,}[-_]\w+/.test(moderator);
          messages.push({ from: sender, to: recipient, timestamp, message, moderator, has_moderator, row_type: '' });
        }
      }
      i++;
    }
    const seen = new Set();
    return messages.filter(m => {
      const key = [m.from, m.to, m.timestamp, m.message].join('|');
      if (seen.has(key)) return false;
      seen.add(key); return true;
    });
  }

  function sortChronologically(messages) {
    return [...messages].sort((a, b) => {
      const ta = parseTimestamp(a.timestamp);
      const tb = parseTimestamp(b.timestamp);
      if (!ta && !tb) return 0;
      if (!ta) return 1;
      if (!tb) return -1;
      return ta.getTime() - tb.getTime();
    });
  }

  function getUnterhaltungBody(doc) {
    for (const id of ['gridview-1055', 'conversation-panel']) {
      const el = doc.getElementById(id);
      if (el) return el;
    }
    const walker = doc.createTreeWalker(doc.body, NodeFilter.SHOW_TEXT);
    let node;
    while ((node = walker.nextNode())) {
      if (/^Unterhaltung$/i.test(node.textContent.trim())) {
        let el = node.parentElement;
        for (let d = 0; d < 10; d++) {
          if (!el) break;
          if (el.querySelector('tr[id]') || el.querySelector('tbody')) return el;
          el = el.parentElement;
        }
      }
    }
    return null;
  }

  function getHistoryPanel(doc) {
    for (const id of ['gridview-1068', 'history-grid']) {
      const el = doc.getElementById(id);
      if (el) return el;
    }
    const walker = doc.createTreeWalker(doc.body, NodeFilter.SHOW_TEXT);
    let node;
    while ((node = walker.nextNode())) {
      if (/^Protokoll$/i.test(node.textContent.trim())) {
        let el = node.parentElement;
        for (let d = 0; d < 10; d++) {
          if (!el) break;
          if (el.querySelector('tr[id]')) return el;
          el = el.parentElement;
        }
      }
    }
    return null;
  }

  function isClientMessage(message) {
    if (!message) return false;
    return !message.has_moderator;
  }

  function extract(html) {
    const doc = new DOMParser().parseFromString(html, 'text/html');

    const clientCard = findProfileCard(doc, CLIENT_COLOR);
    const fakeCard   = findProfileCard(doc, FAKE_COLOR);
    const clientInfo = buildProfile(clientCard);
    const fakeInfo   = buildProfile(fakeCard);

    const untPanel  = getUnterhaltungBody(doc);
    const histPanel = getHistoryPanel(doc);

    let untMsgs  = parseGridMessages(untPanel);
    let histMsgs = parseGridMessages(histPanel);

    if (!untMsgs.length && !histMsgs.length) {
      const allTables = [];
      for (const table of Array.from(doc.querySelectorAll('table'))) {
        const msgs = extractMessagesFromTable(table).filter(m => m.message && !/^\*{2,}.*\*{2,}$/.test(m.message.trim()) && m.row_type !== 'poke');
        if (msgs.length) allTables.push(msgs);
      }
      if (allTables.length) {
        histMsgs = allTables.reduce((a, b) => a.length >= b.length ? a : b);
      }
    }

    const FASA_PATTERNS = [
      /Im Moment hat unser.*Mitglied noch nichts/i,
      /FASA/i,
      /Du solltest vielleicht mal eine Nachricht schicken/i,
      /Sicher freut sich Dein Empfänger/i,
      /Lasse mich überraschen/i,
      /^\*{2,}.*\*{2,}$/,
      /^Antupser$/i,
      /^Freundschaft$/i,
    ];
    function isFasaTemplate(text) {
      return FASA_PATTERNS.some(re => re.test((text || '').trim()));
    }
    untMsgs  = untMsgs.filter(m => !isFasaTemplate(m.message));
    histMsgs = histMsgs.filter(m => !isFasaTemplate(m.message));

    const histSorted = sortChronologically(histMsgs);

    const seen = new Set();
    const fullConv = [];
    for (const m of [...histSorted, ...sortChronologically(untMsgs)]) {
      const key = [m.from, m.to, m.timestamp, m.message].join('|');
      if (seen.has(key)) continue;
      seen.add(key);
      fullConv.push(m);
    }
    const fullSorted = sortChronologically(fullConv);

    let latestMessage = null;
    let clientSentLast = false;

    if (fullSorted.length > 0) {
      latestMessage = fullSorted[fullSorted.length - 1];
      if (untMsgs.length > 0) {
        clientSentLast = true;
      } else {
        clientSentLast = isClientMessage(latestMessage);
      }
    }

    const messageType = fullSorted.length === 0 ? 'FC' : (clientSentLast ? 'DIA' : 'ASA');
    const trimmedConversation = fullSorted.slice(-10);
    const lastClientMsg = clientSentLast ? (latestMessage ? latestMessage.message : '') : '';

    return {
      message_type: messageType,
      client_information: clientInfo,
      fake_account: fakeInfo,
      conversation: trimmedConversation,
      last_message: latestMessage,
      last_client_message: lastClientMsg,
    };
  }

  return { extract };
})();

// ── Xkuss (Global) extractor (ported the same way). ─────────────────────────
const XkussExtractor = (function () {
  const TS_SHORT_RE = /^\d{2}\.\d{2}\.\d{2} \d{2}:\d{2}$/;

  function parseTs(s) {
    if (!s) return null;
    let m = s.match(/^(\d{2})\.(\d{2})\.(\d{2}) (\d{2}):(\d{2})$/);
    if (m) {
      const dd = m[1], mm = m[2], yy = m[3], HH = m[4], MM = m[5];
      return new Date(2000 + parseInt(yy), parseInt(mm) - 1, parseInt(dd), parseInt(HH), parseInt(MM));
    }
    m = s.match(/^(\d{2})\.(\d{2})\.(\d{4}) (\d{2}):(\d{2})(:(\d{2}))?$/);
    if (m) {
      const dd = m[1], mm = m[2], yyyy = m[3], HH = m[4], MM = m[5], SS = m[7];
      return new Date(parseInt(yyyy), parseInt(mm) - 1, parseInt(dd), parseInt(HH), parseInt(MM), parseInt(SS || 0));
    }
    return null;
  }

  function sortByTs(msgs) {
    return [...msgs].sort((a, b) => {
      const ta = parseTs(a.timestamp), tb = parseTs(b.timestamp);
      if (!ta && !tb) return 0;
      if (!ta) return 1;
      if (!tb) return -1;
      return ta.getTime() - tb.getTime();
    });
  }

  function clean(el) {
    return (el ? (el.textContent || el.innerText || '') : '').replace(/ /g, ' ').replace(/\s+/g, ' ').trim();
  }

  function extractChatMessages(doc) {
    const messages = [];
    const seen = new Set();

    for (const tr of doc.querySelectorAll('tr')) {
      const cells = [...tr.querySelectorAll(':scope > td')];
      if (cells.length !== 3) continue;
      const ts = clean(cells[1]);
      const mod = clean(cells[2]);
      if (!TS_SHORT_RE.test(ts)) continue;

      // The header row (icon/timestamp/name) sits inside a small table nested
      // a few levels down inside the message's outer per-message <td> -- the
      // exact depth varies by xkuss page variant, so walk up to the nearest
      // ancestor <td> instead of counting a fixed number of hops.
      const container = tr.closest('td');
      if (!container) continue;

      let msg = clean(container).replace(ts, '').replace(mod, '').replace(/\s+/g, ' ').trim();
      if (!msg) continue;

      const has_mod = mod.length > 0 && /[a-zA-Z]{2,}/.test(mod);
      const sender = has_mod ? 'fake_account' : 'client';
      const key = ts + '|' + msg.substring(0, 30);
      if (seen.has(key)) continue;
      seen.add(key);

      messages.push({ sender, moderator: mod, has_moderator: has_mod, timestamp: ts, message: msg });
    }

    return sortByTs(messages);
  }

  function extractTopics(doc) {
    const topics = [];
    const seen = new Set();

    for (const tr of doc.querySelectorAll('tr')) {
      const cells = [...tr.querySelectorAll(':scope > td')];
      if (cells.length < 2 || cells.length > 3) continue;
      const ts = clean(cells[0]);
      if (!TS_SHORT_RE.test(ts)) continue;

      const text = clean(cells[cells.length - 1]);
      if (!text) continue;

      let el = tr.parentElement;
      let inDiaInfo = false;
      for (let i = 0; i < 8; i++) {
        if (!el) break;
        if (el.tagName === 'FIELDSET') {
          const leg = el.querySelector('legend');
          if (leg && /Dia Info|Info/i.test(leg.textContent)) { inDiaInfo = true; break; }
        }
        el = el.parentElement;
      }
      if (!inDiaInfo) continue;

      const key = ts + '|' + text.substring(0, 30);
      if (seen.has(key)) continue;
      seen.add(key);
      topics.push({ timestamp: ts, note: text });
    }

    return sortByTs(topics);
  }

  function selectedText(sel) {
    if (!sel || !sel.options || sel.selectedIndex < 0) return '';
    const opt = sel.options[sel.selectedIndex];
    const text = clean(opt);
    return /^(DD|MM|YYYY)$/.test(text) ? '' : text;
  }

  // Reads one side of a client<->fake comparison cell: a plain text input, a
  // day/month/year birthdate select group, or (xkuss renders the fake side's
  // Name/Geburtstag as read-only) bare cell text with no form control at all.
  function fieldValue(cell) {
    const selects = [...cell.querySelectorAll('select')];
    if (selects.length >= 2) {
      return selects.map(selectedText).filter(Boolean).join('.');
    }
    const input = cell.querySelector('input');
    if (input) return (input.value || '').trim();
    return clean(cell);
  }

  // The profile-comparison table (Name/Wohnort/Beruf/Arbeitszeiten/Status/
  // Fortschritt/Geburtstag) is laid out as <td>client value</td><td><b>label</b></td>
  // <td>fake value</td> per row. Background colors on these inputs are not a
  // reliable signal (they vary by field and are sometimes swapped-looking at a
  // glance), so the client side is identified precisely by its "k_"-prefixed
  // input/select ids -- xkuss's own naming for "Kunde" (customer) fields --
  // with the fake account's side simply being whatever sits in the third cell.
  function extractComparisonFields(doc, clientProfile, fakeProfile) {
    for (const tr of doc.querySelectorAll('tr')) {
      const cells = [...tr.querySelectorAll(':scope > td')];
      if (cells.length !== 3) continue;
      const labelB = cells[1].querySelector('b');
      if (!labelB) continue;
      if (!cells[0].querySelector('[id^="k_"]')) continue; // not a client<->fake comparison row

      const label = clean(labelB).replace(/:$/, '').trim();
      if (!label) continue;

      const clientVal = fieldValue(cells[0]);
      const fakeVal = fieldValue(cells[2]);
      if (clientVal) clientProfile.details[label] = clientVal;
      if (fakeVal) fakeProfile.details[label] = fakeVal;
    }
  }

  function extractProfiles(doc, messages) {
    const clientProfile = { role: 'client', username: '', details: {}, dialog_info: '', global_info: '' };
    const fakeProfile = { role: 'fake_account', username: '', details: {}, dialog_info: '', global_info: '' };

    extractComparisonFields(doc, clientProfile, fakeProfile);

    for (const fs of doc.querySelectorAll('fieldset')) {
      const leg = fs.querySelector('legend');
      if (!leg) continue;
      const lt = clean(leg);

      const diagMatch = lt.match(/Dialoginformation(?:en)? von (\w+)/i);
      if (diagMatch) {
        const uname = diagMatch[1];
        const hasGlobalInfo = fs.parentElement
          ? [...fs.parentElement.querySelectorAll('fieldset')].some(f => {
              const l = f.querySelector('legend');
              return l && /Globale Info/i.test(clean(l));
            })
          : false;
        const target = hasGlobalInfo ? fakeProfile : clientProfile;
        target.username = target.username || uname;
        target.dialog_info = clean(fs).replace(lt, '').trim();
      }

      if (/Globale Info von (\w+)/i.test(lt)) {
        const m = lt.match(/Globale Info von (\w+)/i);
        if (m) fakeProfile.username = fakeProfile.username || m[1];
        fakeProfile.global_info = clean(fs).replace(lt, '').trim();
      }

      if (/Dia Info (.+) <-> (.+)/i.test(lt)) {
        const m = lt.match(/Dia Info (.+) <-> (.+)/i);
        if (m) {
          clientProfile.username = clientProfile.username || m[1].trim();
          fakeProfile.username = fakeProfile.username || m[2].trim();
        }
      }
    }

    if (!fakeProfile.username) {
      const modNames = [...new Set(messages.filter(m => m.has_moderator && m.moderator).map(m => m.moderator))];
      if (modNames.length) fakeProfile.username = modNames[0];
    }

    const kvKeys = ['Geschlecht', 'Alter', 'Wohnort', 'PLZ', 'Beruf', 'Mitglied seit', 'Zuletzt online',
      'Sucht', 'Groesse', 'Haarfarbe', 'Augenfarbe', 'Raucher', 'Geburtstag', 'Name', 'Status'];

    for (const tr of doc.querySelectorAll('tr')) {
      const cells = [...tr.querySelectorAll(':scope > td')];
      if (cells.length < 2) continue;
      const keyEl = cells[0].querySelector('b');
      if (!keyEl) continue;
      const key = clean(keyEl).replace(':', '').trim();
      if (!kvKeys.includes(key)) continue;
      const val = clean(cells[1]);
      if (!val || val === key) continue;
      if (!clientProfile.details[key]) clientProfile.details[key] = val;
      else if (!fakeProfile.details[key]) fakeProfile.details[key] = val;
    }

    return { clientProfile, fakeProfile };
  }

  function extract(html) {
    const doc = new DOMParser().parseFromString(html, 'text/html');
    const messages = extractChatMessages(doc);
    const topics = extractTopics(doc);
    const profiles = extractProfiles(doc, messages);

    const trimmedMessages = messages.slice(-10);
    const rawLast = trimmedMessages.length ? trimmedMessages[trimmedMessages.length - 1] : null;

    const trimmedLast = rawLast ? {
      sender: rawLast.sender,
      text: rawLast.message,
      time: rawLast.timestamp,
      moderator: rawLast.moderator,
      has_moderator: rawLast.has_moderator,
    } : null;

    const normalisedMessages = trimmedMessages.map(m => ({
      sender: m.sender,
      text: m.message,
      time: m.timestamp,
      moderator: m.moderator,
      has_moderator: m.has_moderator,
    }));

    const lastClientArr = [...trimmedMessages].reverse().filter(m => m.sender === 'client');
    const lastClientMsg = lastClientArr.length ? lastClientArr[0] : null;

    const messageType = messages.length === 0 ? 'FC' : (trimmedLast && trimmedLast.sender === 'client' ? 'DIA' : 'ASA');

    return {
      message_type: messageType,
      client: profiles.clientProfile,
      fake_account: profiles.fakeProfile,
      messages: normalisedMessages,
      topics,
      last_message: trimmedLast,
      last_client_message: lastClientMsg ? lastClientMsg.message : '',
    };
  }

  return { extract };
})();

// ── Shared UI wiring ─────────────────────────────────────────────────────────
function flattenProfile(raw) {
  const out = {};
  if (!raw) return out;
  if (raw.username) out['Username'] = raw.username;
  if (raw.age) out['Alter'] = raw.age;
  if (raw.birthdate) out['Geburtsdatum'] = raw.birthdate;
  if (raw.bio) out['Bio'] = raw.bio;
  const extra = raw.additional_details || raw.details || {};
  for (const k of Object.keys(extra)) {
    const v = extra[k];
    if (v === '' || v == null) continue;
    out[k] = Array.isArray(v) ? v.join(', ') : String(v);
  }
  if (raw.dialog_info) out['Dialoginformationen'] = raw.dialog_info;
  if (raw.global_info) out['Globale Info'] = raw.global_info;
  return out;
}

function normalizeExtracted(platform, raw) {
  if (platform === 'xkuss') {
    return {
      messageType: raw.message_type,
      conversation: (raw.messages || []).map(m => ({ sender: m.sender, text: m.text, time: m.time })),
      clientProfile: flattenProfile(raw.client),
      fakeProfile: flattenProfile(raw.fake_account),
    };
  }
  return {
    messageType: raw.message_type,
    conversation: (raw.conversation || []).map(m => ({
      sender: m.has_moderator ? 'fake_account' : 'client', text: m.message, time: m.timestamp,
    })),
    clientProfile: flattenProfile(raw.client_information),
    fakeProfile: flattenProfile(raw.fake_account),
  };
}

let currentPlatform = 'justlo_lindu';
let extracted = null;

const PASTE_LABELS = { justlo_lindu: 'Justlo / Linduu — HTML einfügen', xkuss: 'Xkuss — HTML einfügen' };

function setPlatform(p) {
  currentPlatform = p;
  document.getElementById('tab-justlo').classList.toggle('active', p === 'justlo_lindu');
  document.getElementById('tab-xkuss').classList.toggle('active', p === 'xkuss');
  document.getElementById('pasteLabel').textContent = PASTE_LABELS[p];
  extracted = null;
  document.getElementById('extractedCard').style.display = 'none';
  document.getElementById('generateCard').style.display = 'none';
  document.getElementById('extractError').style.display = 'none';
  document.getElementById('genResult').style.display = 'none';
}

function renderProfile(elId, profile) {
  const el = document.getElementById(elId);
  const keys = Object.keys(profile || {});
  if (!keys.length) { el.innerHTML = '<div class="empty-note">Keine Daten gefunden.</div>'; return; }
  el.innerHTML = keys.map(k => `
    <div class="profile-row"><span class="k">${escapeHtml(k)}</span><span>${escapeHtml(String(profile[k]))}</span></div>
  `).join('');
}

function doExtract() {
  const html = document.getElementById('htmlInput').value.trim();
  const errEl = document.getElementById('extractStatus');
  const errBox = document.getElementById('extractError');
  errBox.style.display = 'none';
  errEl.textContent = '';
  if (!html) { errEl.textContent = 'Bitte HTML einfügen.'; return; }

  try {
    const raw = currentPlatform === 'xkuss' ? XkussExtractor.extract(html) : JustloExtractor.extract(html);
    extracted = normalizeExtracted(currentPlatform, raw);
  } catch (e) {
    errBox.textContent = 'Der eingefügte Chat konnte nicht gelesen werden. Bitte Format und Inhalt prüfen.';
    errBox.style.display = 'block';
    document.getElementById('extractedCard').style.display = 'none';
    document.getElementById('generateCard').style.display = 'none';
    return;
  }

  const badge = document.getElementById('typeBadge');
  badge.textContent = extracted.messageType;
  badge.className = 'type-badge ' + extracted.messageType.toLowerCase();

  const bubbles = document.getElementById('bubbles');
  if (!extracted.conversation.length) {
    bubbles.innerHTML = '<div class="empty-note">Keine Nachrichten gefunden (Erstkontakt).</div>';
  } else {
    bubbles.innerHTML = extracted.conversation.map(m => `
      <div class="bubble ${m.sender}">
        <div class="bubble-meta">
          <span class="bubble-role">${m.sender === 'fake_account' ? 'fake' : 'client'}</span>
          <span class="bubble-time">${escapeHtml(m.time || '')}</span>
        </div>
        <div class="bubble-text">${escapeHtml(m.text || '')}</div>
      </div>
    `).join('');
  }

  renderProfile('clientProfile', extracted.clientProfile);
  renderProfile('fakeProfile', extracted.fakeProfile);

  // The dashboard's "Last Message" card wants the literal last message in the
  // conversation regardless of who sent it (in ASA mode that's "Ich"'s own
  // follow-up, which is exactly what a reviewer needs to judge the reply
  // against) — not specifically the customer's last message.
  const lastMsg = extracted.conversation.length ? extracted.conversation[extracted.conversation.length - 1] : null;
  document.getElementById('lastMsgDe').textContent = lastMsg ? lastMsg.text : '';

  document.getElementById('extractedCard').style.display = 'block';
  document.getElementById('generateCard').style.display = 'block';
  document.getElementById('genResult').style.display = 'none';
  document.getElementById('genError').style.display = 'none';

  showLastMessageTranslation();
}

async function showLastMessageTranslation() {
  const box = document.getElementById('lastMsgTranslation');
  const textEl = document.getElementById('lastMsgTranslationText');
  if (!extracted || !extracted.conversation.length) {
    box.style.display = 'none';
    return;
  }
  const last = extracted.conversation[extracted.conversation.length - 1];
  box.style.display = 'block';
  textEl.textContent = 'Übersetze…';
  try {
    const res = await fetch('/api/chameleon/translate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: last.text }),
    });
    const data = await res.json().catch(() => ({}));
    textEl.textContent = (res.ok && data.ok) ? data.translated : '(Übersetzung nicht verfügbar)';
  } catch (e) {
    textEl.textContent = '(Übersetzung nicht verfügbar)';
  }
}

async function doGenerate() {
  if (!extracted) return;
  const btn = document.getElementById('genBtn');
  const statusEl = document.getElementById('genStatus');
  const errBox = document.getElementById('genError');
  const resultBox = document.getElementById('genResult');
  errBox.style.display = 'none';
  resultBox.style.display = 'none';
  btn.disabled = true;
  statusEl.textContent = 'Generiere…';

  try {
    const res = await fetch('/api/chameleon/reply', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        platform: currentPlatform,
        message_type: extracted.messageType,
        conversation: extracted.conversation,
        client_profile: extracted.clientProfile,
        fake_profile: extracted.fakeProfile,
        additional_instructions: document.getElementById('instructionsInput').value.trim(),
      }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) {
      errBox.textContent = data.error || `HTTP ${res.status}`;
      errBox.style.display = 'block';
      return;
    }
    document.getElementById('replyDe').textContent = data.reply;
    const enEl = document.getElementById('replyEn');
    enEl.textContent = data.reply_en ? data.reply_en : '';
    enEl.style.display = data.reply_en ? 'block' : 'none';
    resultBox.style.display = 'block';
  } catch (e) {
    errBox.textContent = 'Request failed: ' + e;
    errBox.style.display = 'block';
  } finally {
    btn.disabled = false;
    statusEl.textContent = '';
  }
}

async function copyReply() {
  const text = document.getElementById('replyDe').textContent;
  if (!text) return;
  try {
    await navigator.clipboard.writeText(text);
    const note = document.getElementById('copiedNote');
    note.style.display = 'inline';
    setTimeout(() => { note.style.display = 'none'; }, 1500);
  } catch (e) {
    alert('Konnte nicht kopieren: ' + e);
  }
}
</script>
</body>
</html>
"""


# ── Bots launcher page ───────────────────────────────────────────────────────
# The single place to start/stop Xkuss, Justlo and Linduu and configure each
# one's Source (real Chameleon-AI vs this project's own Groq extractor) and
# Approval (human review vs fully automatic) independently — no terminal
# commands needed. Each of the three cards is fully self-contained: changing
# one never touches another's process, Source, or Approval setting.
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

  details.steps-block, details.prompt-block {
    margin-top: 12px; border-top: 1px solid var(--border); padding-top: 10px;
  }
  summary { cursor: pointer; font-size: 12px; font-weight: 650; color: var(--muted-foreground); }
  summary:hover { color: var(--foreground); }
  ol.steps-list { margin: 10px 0 0; padding-left: 20px; font-size: 12.5px; color: var(--foreground); }
  ol.steps-list li { margin-bottom: 5px; }
  .prompt-box {
    margin-top: 8px; background: var(--muted); border: 1px solid var(--border); border-radius: 8px;
    padding: 10px 12px; font-size: 11.5px; white-space: pre-wrap; font-family: ui-monospace, "SF Mono", Consolas, monospace;
    color: var(--muted-foreground); max-height: 260px; overflow-y: auto;
  }
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
    <p>Start, stop, and configure Xkuss, Justlo and Linduu — each one fully independent, no shared switches.
    Want to test extraction/Groq by hand without touching a live bot? <a href="/chameleon" style="text-decoration:underline;">Chameleon — Standalone</a>.</p>
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
];

// Per-platform client-side state, kept separate per card by construction --
// every fetch/render/action below is always scoped to one `slug`, never "all".
const state = {};
for (const p of PLATFORMS) {
  state[p.slug] = { source: "real", approvalEffective: "manual", running: false, managedBy: null, liveDetail: "" };
}

function stepsFor(slug, source, approval) {
  const target = source === "local" ? "this project's own /chameleon page" : "the real Chameleon-AI site";
  const genLabel = source === "local" ? "\"Daten extrahieren\" then \"Antwort generieren\" (Groq)" : "\"Antwort generieren\"";
  const approvalStep = approval === "auto"
    ? "Skip approval — the reply is used immediately (no one reviews it)"
    : "Wait for you to Approve or Reject it on the main Approval Dashboard";
  const copyStep = source === "local" ? "Click \"Kopieren\" to copy the reply" : "Copy the reply text";
  return [
    "Detect an incoming chat",
    "Capture that chat's HTML",
    `Paste it into ${target}`,
    `Click ${genLabel} and wait for a reply`,
    approvalStep,
    copyStep,
    "Paste it into the chat's reply box",
    "Wait about 15–20 seconds, then send",
  ];
}

function renderCard(slug, label) {
  const s = state[slug];
  const running = s.running;
  const statusClass = running ? (s.managedBy === "external" ? "external" : "running") : "";
  const statusText = running ? (s.managedBy === "external" ? "Running (external)" : "Running") : "Stopped";
  const steps = stepsFor(slug, s.source, s.approvalEffective);

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
        <span class="toggle-name">Source</span>
        <button class="toggle-switch-btn ${s.source === "local" ? "on" : ""}" onclick="toggleSource('${slug}')">
          <span class="toggle-dot"></span>
          <span class="toggle-switch-label">${s.source === "local" ? "Built-in Groq" : "Real Chameleon-AI"}</span>
        </button>
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

      <details class="prompt-block" id="promptBlock-${slug}" ontoggle="onPromptToggle('${slug}', this.open)">
        <summary>View system prompt</summary>
        <div class="prompt-box" id="promptBox-${slug}">
          ${s.source === "local" ? "Loading…" : "Real Chameleon-AI's prompt is external — not something we control or can show."}
        </div>
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

async function onPromptToggle(slug, open) {
  if (!open || state[slug].source !== "local") return;
  const box = document.getElementById(`promptBox-${slug}`);
  if (!box) return;
  try {
    const res = await fetch("/api/chameleon/prompt");
    const data = await res.json();
    box.textContent = data.prompt || "(unavailable)";
  } catch (e) {
    box.textContent = "(could not load prompt)";
  }
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

async function toggleSource(slug) {
  showError(slug, "");
  const next = state[slug].source === "local" ? "real" : "local";
  if (next === "local") {
    const ok = await showConfirm(
      `Switch ${slug} to Built-in Groq?`,
      `This only affects ${slug} — no other platform is touched. If it's currently running, it restarts automatically ` +
      "(via launch_all.py, if that's how it was started) to pick this up.",
      { confirmLabel: "Switch & restart", danger: true },
    );
    if (!ok) return;
  }
  try {
    const res = await fetch("/api/chameleon/source", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ platform: slug, source: next }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) {
      const errMsg = data.error || `HTTP ${res.status}`;
      showError(slug, errMsg);
      toast(`Could not switch ${slug}'s source`, { type: "error", detail: errMsg });
      return;
    }
    state[slug].source = next;
    render();
    toast(
      `${slug}: switching to ${next === "local" ? "Built-in Groq" : "Real Chameleon-AI"}`,
      { type: "warning", detail: "Restarting now — watch the status pill above for it to come back." },
    );
  } catch (e) {
    showError(slug, "Request failed: " + e);
    toast(`Could not switch ${slug}'s source`, { type: "error", detail: "Request failed — check your connection." });
  }
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
      const res = await fetch(`/api/chameleon/source?platform=${p.slug}`);
      const data = await res.json();
      if (data.source) state[p.slug].source = data.source;
    } catch (e) { /* keep default */ }
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


@app.get("/chameleon")
def chameleon_page():
    return Response(_CHAMELEON_PAGE, mimetype="text/html")


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
