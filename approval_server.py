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

import asyncio
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
from prompts import JUDGE_SYSTEM_PROMPT, MEETING_ALERT_SYSTEM_PROMPT
from core import push_notifications


try:
    from flask_sock import Sock
    from simple_websocket import Client as WebSocketClient, ConnectionClosed
except ImportError:  # live-push is a nice-to-have — dashboard falls back to HTTP polling without it
    Sock = None
    WebSocketClient = None
    ConnectionClosed = Exception

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from groq import Groq
except ImportError:  # Groq-backed translation/meeting checks remain optional
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


def _pid_is_running(pid: int | None) -> bool:
    """Return whether a reported worker PID is still alive."""
    if not pid or pid <= 0:
        return False
    if sys.platform == "win32":
        try:
            import ctypes
            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
            if not handle:
                return False
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def _reported_bot_pid(platform: str) -> int | None:
    """Read the latest PID reported by this platform's worker telemetry."""
    with _lock:
        entry = next(
            (value for key, value in _status.items() if key.strip().lower() == platform),
            None,
        )
        raw_pid = entry.get("pid") if entry else None
    try:
        pid = int(raw_pid)
    except (TypeError, ValueError):
        return None
    return pid if _pid_is_running(pid) else None


def _terminate_process_tree(pid: int | None) -> bool:
    """Force-stop one bot worker and all children, including its CMD window."""
    if not pid or pid == os.getpid():
        return False
    if not _pid_is_running(pid):
        return True
    if sys.platform == "win32":
        completed = subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
            timeout=8,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return completed.returncode == 0 or not _pid_is_running(pid)

    try:
        os.kill(pid, 15)
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and _pid_is_running(pid):
            time.sleep(0.05)
        if _pid_is_running(pid):
            os.kill(pid, 9)
        return not _pid_is_running(pid)
    except OSError:
        return not _pid_is_running(pid)


def _stop_via_launcher(platform: str) -> bool:
    """Tell launch_all.py not to restart a worker that Stop is terminating."""
    try:
        response = httpx.post(
            f"{LAUNCHER_CONTROL_URL}/control/command",
            json={"cmd": "stop", "target": platform},
            timeout=2.5,
        )
        return response.is_success and bool(response.json().get("ok"))
    except Exception:
        return False


def _cdp_listener_pid(port: int) -> int | None:
    """Resolve only the process listening on one dedicated CDP port."""
    if sys.platform != "win32":
        return None
    try:
        completed = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True,
            text=True,
            timeout=5,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        pattern = re.compile(
            rf"^\s*TCP\s+\S*:{port}\s+\S+\s+LISTENING\s+(\d+)\s*$",
            re.IGNORECASE,
        )
        for line in completed.stdout.splitlines():
            match = pattern.match(line)
            if match:
                return int(match.group(1))
    except Exception:
        pass
    return None


def _close_platform_chrome(port: int) -> bool:
    """Close exactly the Chrome instance exposed on this platform's CDP port."""
    if not is_cdp_ready(port):
        return True

    # Ask Chrome to shut down cleanly first. Each platform has its own CDP port
    # and profile, so Browser.close cannot touch unrelated Chrome windows.
    if WebSocketClient is not None:
        try:
            version = httpx.get(f"http://127.0.0.1:{port}/json/version", timeout=2.0).json()
            debugger_url = version.get("webSocketDebuggerUrl")
            if debugger_url:
                ws = WebSocketClient(debugger_url)
                try:
                    ws.send(json.dumps({"id": 1, "method": "Browser.close"}))
                    ws.receive(timeout=1.0)
                finally:
                    try:
                        ws.close()
                    except Exception:
                        pass
        except Exception:
            pass

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and is_cdp_ready(port):
        time.sleep(0.05)
    if not is_cdp_ready(port):
        return True

    # If the browser ignored CDP shutdown, kill only the listener resolved from
    # this exact dedicated port. Never search by process name or close all Chrome.
    browser_pid = _cdp_listener_pid(port)
    return _terminate_process_tree(browser_pid) if browser_pid else False


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
    reported_pid = _reported_bot_pid(platform)
    if reported_pid:
        return {"running": True, "pid": reported_pid, "managed_by": "external"}
    return {
        "running": False,
        "pid": None,
        "managed_by": None,
        "browser_open": is_cdp_ready(_SELF_MANAGED_CDP_PORTS[platform]),
    }


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

    port = _SELF_MANAGED_CDP_PORTS[platform]
    proc = _bot_procs.get(platform)
    pids = set()
    if proc is not None and proc.poll() is None:
        pids.add(proc.pid)
    reported_pid = _reported_bot_pid(platform)
    if reported_pid:
        pids.add(reported_pid)

    # When launch_all.py owns this bot, this marks it intentionally stopped so
    # its crash monitor does not immediately relaunch the process we terminate.
    _stop_via_launcher(platform)

    worker_stopped = True
    for pid in pids:
        worker_stopped = _terminate_process_tree(pid) and worker_stopped
    _bot_procs.pop(platform, None)
    browser_stopped = _close_platform_chrome(port)

    label = next(p for p in SELF_MANAGED_PLATFORMS if p == platform).capitalize()
    now = _now()
    with _lock:
        # A stopped bot cannot complete an approval. Remove its cards from the
        # actionable queue instead of leaving permanent orphan approvals.
        for item in _requests.values():
            if (
                (item.get("platform") or "").strip().lower() == platform
                and item.get("status") in ("pending", "skip_requested")
            ):
                item["status"] = "cancelled"
                item["decided_at"] = now
        _status[label] = {
            "state": "stopped",
            "detail": "Bot and browser stopped",
            "retry_count": 0,
            "warning": "",
            "checkpoint": "stopped",
            "pid": None,
            "updated_at": now,
        }
    _bump_state()

    if not worker_stopped or not browser_stopped:
        failed = []
        if not worker_stopped:
            failed.append("bot process")
        if not browser_stopped:
            failed.append("Chrome")
        return jsonify({
            "ok": False,
            "error": f"Could not fully stop {platform}: {', '.join(failed)}",
            "worker_stopped": worker_stopped,
            "browser_stopped": browser_stopped,
        }), 500
    return jsonify({
        "ok": True,
        "worker_stopped": True,
        "browser_stopped": True,
    })


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

# Provider health is updated by real translation/meeting-guard calls and by the
# dashboard's explicit "Check now" action. It intentionally stores only a
# masked key suffix, never the credential itself.
_api_health_lock = threading.Lock()
_api_health: dict[str, dict] = {
    "groq": {"state": "unchecked", "checked_at": None, "cooldown_until": None, "detail": ""},
    "openrouter": {"state": "unchecked", "checked_at": None, "cooldown_until": None, "detail": ""},
}


def _masked_api_key(env_name: str) -> str:
    value = (os.environ.get(env_name) or "").strip()
    if not value:
        return ""
    return f"••••{value[-4:]}" if len(value) > 4 else "••••"


def _retry_after_seconds(error: Exception) -> int:
    response = getattr(error, "response", None)
    headers = getattr(response, "headers", {}) or {}
    raw = headers.get("retry-after") or headers.get("Retry-After")
    try:
        return max(1, int(float(raw)))
    except (TypeError, ValueError):
        pass

    message = str(error)
    match = re.search(r"(?:try again|retry)(?: in| after)?\s*(?:(\d+(?:\.\d+)?)m)?\s*(\d+(?:\.\d+)?)?s", message, re.I)
    if match:
        minutes = float(match.group(1) or 0)
        seconds = float(match.group(2) or 0)
        return max(1, int(minutes * 60 + seconds))
    return 60


def _record_api_success(provider: str, detail: str = "Available") -> None:
    with _api_health_lock:
        _api_health[provider] = {
            "state": "ready",
            "checked_at": _now(),
            "cooldown_until": None,
            "detail": detail,
        }
    _bump_state()


def _record_api_failure(provider: str, error: Exception) -> None:
    response = getattr(error, "response", None)
    status_code = getattr(error, "status_code", None) or getattr(response, "status_code", None)
    is_cooldown = status_code == 429 or "429" in str(error) or "rate limit" in str(error).lower()
    cooldown_until = None
    if is_cooldown:
        cooldown_until = time.time() + _retry_after_seconds(error)
        detail = "Rate limit reached"
    else:
        detail = f"HTTP {status_code}" if status_code else str(error).strip()[:160]
        detail = detail or "Provider check failed"
    with _api_health_lock:
        _api_health[provider] = {
            "state": "cooldown" if is_cooldown else "error",
            "checked_at": _now(),
            "cooldown_until": cooldown_until,
            "detail": detail,
        }
    _bump_state()


def _api_health_snapshot() -> list[dict]:
    providers = (
        ("groq", "Groq", "GROQ_API_KEY", GROQ_TRANSLATION_MODEL),
        ("openrouter", "OpenRouter", "OPENROUTER_API_KEY", OPENROUTER_TRANSLATION_MODEL),
    )
    now = time.time()
    with _api_health_lock:
        stored = {name: dict(value) for name, value in _api_health.items()}
    result = []
    for name, label, env_name, model in providers:
        configured = bool((os.environ.get(env_name) or "").strip())
        item = stored[name]
        state = item["state"] if configured else "not_configured"
        cooldown_until = item.get("cooldown_until")
        if state == "cooldown" and cooldown_until and cooldown_until <= now:
            state = "unchecked"
            cooldown_until = None
            item["detail"] = "Cooldown elapsed; check again"
        result.append({
            "provider": name,
            "label": label,
            "configured": configured,
            "key_hint": _masked_api_key(env_name),
            "model": model,
            "state": state,
            "detail": item.get("detail") or "",
            "checked_at": item.get("checked_at"),
            "cooldown_until": (
                datetime.fromtimestamp(cooldown_until).isoformat(timespec="seconds")
                if cooldown_until else None
            ),
            "cooldown_seconds": max(0, int(cooldown_until - now)) if cooldown_until else 0,
        })
    return result


def _translation_messages(text: str) -> list[dict[str, str]]:
    return [
        {"role": "system", "content": _TRANSLATION_SYSTEM_PROMPT},
        {"role": "user", "content": text},
    ]


def _translate_with_groq(text: str) -> str | None:
    client = _get_groq_test_client()
    if client is None:
        return None
    try:
        response = client.chat.completions.create(
            model=GROQ_TRANSLATION_MODEL,
            messages=_translation_messages(text),
            temperature=0,
            max_completion_tokens=1200,
            top_p=1,
            reasoning_effort="low",
            timeout=8.0,
        )
    except Exception as error:
        _record_api_failure("groq", error)
        raise
    _record_api_success("groq", "Translation request succeeded")
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
    try:
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
    except Exception as error:
        _record_api_failure("openrouter", error)
        raise
    _record_api_success("openrouter", "Translation request succeeded")
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
# The live Auto-pilot path in create_request() uses this verdict before it can
# auto-send. Lazily constructed so a missing/invalid key only disables the
# guard instead of the whole dashboard, and a newly configured key is picked up
# without restarting the process.
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
        _record_api_failure("groq", e)
        return {"ok": False, "error": str(e), "status": 502}
    _record_api_success("groq", "Meeting guard request succeeded")

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


# ── Meeting-alert analyzer (Groq) ────────────────────────────────────────────
# Advisory only, never blocking: unlike the meeting guard above (which
# rewrites the fake account's own reply), this looks at the whole conversation
# for (a) the CLIENT pushing toward a real meeting, phone number, or
# off-platform contact, and (b) a running read on tone/direction/what the
# client seems to expect -- both just surfaced on the dashboard for a human to
# see. Runs on every request regardless of auto/manual mode.
MEETING_ALERT_MODEL = os.environ.get("MEETING_ALERT_MODEL", "openai/gpt-oss-20b")

_EMPTY_MEETING_ALERT = {
    "ok": True,
    "requested": False,
    "reason": "",
    "tone": "",
    "direction": "",
    "client_expectation": "",
}


def _meeting_alert_transcript(messages: list[dict]) -> str:
    lines = []
    for m in messages[-10:]:
        sender = "FAKE" if (m.get("sender") or "").strip().lower() == "fake_account" else "CLIENT"
        text = (m.get("text") or "").strip()
        if text:
            lines.append(f"{sender}: {text}")
    return "\n".join(lines)


def _analyze_meeting_alert(messages: list[dict]) -> dict:
    """Runs the meeting-alert + conversation-analysis prompt on the last 10
    turns of `messages`. Returns
    {"ok": True, "requested", "reason", "tone", "direction", "client_expectation"}
    or {"ok": False, "error"}. Returns the all-blank version above without
    calling the model at all if there's no conversation yet."""
    transcript = _meeting_alert_transcript(messages)
    if not transcript:
        return dict(_EMPTY_MEETING_ALERT)

    client = _get_groq_test_client()
    if client is None:
        reason = "groq package not installed" if Groq is None else "GROQ_API_KEY is not set"
        return {"ok": False, "error": reason}

    try:
        resp = client.chat.completions.create(
            model=MEETING_ALERT_MODEL,
            messages=[
                {"role": "system", "content": MEETING_ALERT_SYSTEM_PROMPT},
                {"role": "user", "content": f"CONVERSATION (oldest first):\n{transcript}"},
            ],
            temperature=0,
            max_completion_tokens=300,
            top_p=1,
            reasoning_effort="low",
            response_format={"type": "json_object"},
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}

    raw = resp.choices[0].message.content or "{}"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {"ok": False, "error": "model returned invalid JSON", "raw": raw}

    return {
        "ok": True,
        "requested": data.get("requested") is True,
        "reason": str(data.get("reason") or "").strip(),
        "tone": str(data.get("tone") or "").strip(),
        "direction": str(data.get("direction") or "").strip(),
        "client_expectation": str(data.get("client_expectation") or "").strip(),
    }


def _probe_groq_key() -> None:
    client = _get_groq_test_client()
    if client is None:
        if (os.environ.get("GROQ_API_KEY") or "").strip():
            _record_api_failure("groq", RuntimeError("groq package is not installed"))
        return
    try:
        client.chat.completions.create(
            model=GROQ_TRANSLATION_MODEL,
            messages=[{"role": "user", "content": "Reply only with OK."}],
            temperature=0,
            max_completion_tokens=8,
            reasoning_effort="low",
            timeout=8.0,
        )
    except Exception as error:
        _record_api_failure("groq", error)
        return
    _record_api_success("groq", "Key check succeeded")


def _probe_openrouter_key() -> None:
    api_key = (os.environ.get("OPENROUTER_API_KEY") or "").strip()
    if not api_key:
        return
    try:
        response = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": os.environ.get("OPENROUTER_HTTP_REFERER", "http://127.0.0.1:8799"),
                "X-Title": os.environ.get("OPENROUTER_APP_TITLE", "Zenox"),
            },
            json={
                "model": OPENROUTER_TRANSLATION_MODEL,
                "messages": [{"role": "user", "content": "Reply only with OK."}],
                "temperature": 0,
                "max_tokens": 4,
            },
            timeout=12.0,
        )
        response.raise_for_status()
    except Exception as error:
        _record_api_failure("openrouter", error)
        return
    _record_api_success("openrouter", "Key check succeeded")


@app.get("/api/ai-keys/status")
def ai_key_status():
    return jsonify(_api_health_snapshot())


@app.post("/api/ai-keys/check")
def check_ai_keys():
    # Run both small probes concurrently so one slow provider cannot hold up the
    # other's result. This is only triggered by the dashboard button.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(_probe_groq_key), pool.submit(_probe_openrouter_key)]
        for future in futures:
            try:
                future.result(timeout=15)
            except Exception:
                pass
    return jsonify({"ok": True, "providers": _api_health_snapshot()})


# ── Judge (OpenRouter) ───────────────────────────────────────────────────────
# A second, independent AI opinion on every reply that reaches the approval
# queue. Lazily reads the API key per-call (same reasoning as the meeting
# guard above). Fails safe: any error here is treated by create_request() as
# "not a 10", i.e. falls back to manual review exactly like a failed meeting
# guard check does.
JUDGE_MODEL = os.environ.get("OPENROUTER_MODEL", "cognitivecomputations/dolphin-mistral-24b-venice-edition")


def _judge_reply(platform: str, last_message: str, customer_message: str,
                  client_profile: dict, fake_profile: dict, reply: str) -> dict:
    """Runs the judge prompt on a reply. Returns
    {"ok": True, "score", "verdict", "reasoning"} or {"ok": False, "error"}."""
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        return {"ok": False, "error": "OPENROUTER_API_KEY is not set"}

    payload = {
        "platform": platform,
        "conversation": {
            "last_message": last_message,
            "customer_message": customer_message,
        },
        "client_profile": client_profile,
        "fake_profile": fake_profile,
        "proposed_reply": reply,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": os.environ.get("OPENROUTER_HTTP_REFERER", "http://127.0.0.1:8799"),
        "X-Title": os.environ.get("OPENROUTER_APP_TITLE", "Zenox"),
    }

    try:
        response = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            json={
                "model": JUDGE_MODEL,
                "messages": [
                    {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                "temperature": 0,
                "max_tokens": 500,
            },
            timeout=20.0,
        )
        response.raise_for_status()
        raw = response.json()["choices"][0]["message"]["content"] or "{}"
    except Exception as e:
        return {"ok": False, "error": str(e)}

    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:]
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {"ok": False, "error": "model returned invalid JSON", "raw": raw}

    try:
        score = max(0, min(10, int(data.get("score"))))
    except (TypeError, ValueError):
        return {"ok": False, "error": "model returned invalid score"}

    return {
        "ok": True,
        "score": score,
        "verdict": str(data.get("verdict") or "").strip(),
        "reasoning": str(data.get("reasoning") or "").strip(),
    }


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
    messages = body.get("messages") or []
    if not reply.strip():
        return jsonify({"error": "reply is required"}), 400

    # Best-effort DE->EN translation so a reviewer who doesn't read German can
    # still judge the reply. The German text is always what's authoritative /
    # editable / actually sent — translations are read-only context.
    reply_en = _translate_de_en(reply)
    customer_message_en = _translate_de_en(customer_message) if customer_message else None
    last_message_en = _translate_de_en(last_message) if last_message else None

    # Meeting-alert analyzer: advisory only, never affects auto/manual routing
    # -- just a warning banner + a running conversation read for whoever ends
    # up looking at this request.
    alert_result = _analyze_meeting_alert(messages)
    meeting_alert_requested = alert_result.get("requested", False) if alert_result["ok"] else False
    meeting_alert_reason = alert_result.get("reason") if alert_result["ok"] else alert_result.get("error")
    conversation_tone = alert_result.get("tone", "") if alert_result["ok"] else ""
    conversation_direction = alert_result.get("direction", "") if alert_result["ok"] else ""
    conversation_client_expectation = alert_result.get("client_expectation", "") if alert_result["ok"] else ""

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

    # Judge AI: a second opinion, scored against the conversation and both
    # profiles, on every reply reaching the queue (auto or manual) so the
    # dashboard can always show a score pill. In auto mode, only a perfect
    # 10 is allowed through -- anything else (including a failed judge call)
    # falls back to manual review, same fail-safe pattern as the meeting
    # guard above.
    judge_result = _judge_reply(
        platform=platform,
        last_message=last_message,
        customer_message=customer_message,
        client_profile=client_profile,
        fake_profile=fake_profile,
        reply=final_reply,
    )
    judge_score = judge_result.get("score") if judge_result["ok"] else None
    judge_verdict = judge_result.get("verdict") if judge_result["ok"] else None
    judge_reasoning = judge_result.get("reasoning") if judge_result["ok"] else judge_result.get("error")
    if auto and judge_score != 10:
        if judge_result["ok"]:
            print(f"[Judge] {platform} reply scored {judge_score}/10 — holding for manual review")
        else:
            print(f"[Judge] OpenRouter check failed for {platform} — holding for manual review: {judge_result.get('error')}")
        auto = False

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
            "judge_score": judge_score,
            "judge_verdict": judge_verdict,
            "judge_reasoning": judge_reasoning,
            "meeting_alert_requested": meeting_alert_requested,
            "meeting_alert_reason": meeting_alert_reason,
            "conversation_tone": conversation_tone,
            "conversation_direction": conversation_direction,
            "conversation_client_expectation": conversation_client_expectation,
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
    try:
        pid = int(body.get("pid")) if body.get("pid") is not None else None
    except (TypeError, ValueError):
        pid = None
    if pid is not None and pid <= 0:
        pid = None
    with _lock:
        _status[platform] = {
            "pid": pid,
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
_checkinall_lock = threading.Lock()


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
        "ai_keys": _api_health_snapshot(),
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
    if cmd.lower() in ("checkinall", "checkinsall", "ca"):
        # Run this here instead of proxying it to launch_all.py. That keeps the
        # dashboard calculator on the current extraction code even when the
        # long-running launcher was started before an update.
        if not _checkinall_lock.acquire(blocking=False):
            return jsonify({"ok": False, "error": "Check-in All is already running"}), 409
        try:
            from core.checkinall import gather_and_write
            platforms = ["gold", "gold2", "diamond", "platin", "s69", "ml",
                         "xkuss", "justlo", "linduu", "gnoxx"]
            text, path = asyncio.run(gather_and_write(platforms))
            return jsonify({"ok": True, "output": f"{text}\n[checkinall] Note saved to {path}"})
        except Exception as error:
            return jsonify({"ok": False, "error": f"Check-in All failed: {error}"}), 500
        finally:
            _checkinall_lock.release()
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
  .api-health-box {
    background: var(--card); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 16px 18px; margin-bottom: 26px;
  }
  .api-health-head { display: flex; align-items: center; gap: 14px; justify-content: space-between; flex-wrap: wrap; }
  .api-health-head h2 { font-size: 14.5px; margin: 0 0 3px; font-weight: 650; }
  .api-health-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; margin-top: 14px; }
  .api-key-card {
    position: relative; overflow: hidden; padding: 13px 14px; border: 1px solid var(--border);
    border-radius: 10px; background: color-mix(in srgb, var(--muted) 72%, transparent);
  }
  .api-key-card::before { content: ""; position: absolute; inset: 0 auto 0 0; width: 2px; background: var(--muted-foreground); }
  .api-key-card.ready::before { background: var(--success); box-shadow: 0 0 16px var(--success); }
  .api-key-card.cooldown::before { background: var(--warning); box-shadow: 0 0 16px var(--warning); }
  .api-key-card.error::before, .api-key-card.not_configured::before { background: var(--destructive); }
  .api-key-top { display: flex; align-items: center; justify-content: space-between; gap: 10px; }
  .api-key-name { font-size: 13px; font-weight: 750; }
  .api-key-state { font-size: 10px; font-weight: 750; letter-spacing: .05em; text-transform: uppercase; padding: 3px 8px; border-radius: 999px; border: 1px solid var(--border); }
  .api-key-card.ready .api-key-state { color: var(--success); border-color: color-mix(in srgb, var(--success) 42%, var(--border)); }
  .api-key-card.cooldown .api-key-state { color: var(--warning); border-color: color-mix(in srgb, var(--warning) 42%, var(--border)); }
  .api-key-card.error .api-key-state, .api-key-card.not_configured .api-key-state { color: var(--destructive); }
  .api-key-meta { margin-top: 8px; color: var(--muted-foreground); font-size: 11px; line-height: 1.55; overflow-wrap: anywhere; }
  .api-key-meta code { color: var(--foreground); font-size: 11px; }
  .btn-api-check { background: var(--primary); color: #fff; white-space: nowrap; }
  .btn-api-check:disabled { opacity: .6; }
  @media (max-width: 720px) { .api-health-grid { grid-template-columns: 1fr; } }

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
  .judge-reasoning { font-size: 12px; color: var(--muted-foreground); margin: 0 0 10px; }
  .meeting-alert-banner { display: flex; align-items: flex-start; gap: 10px; border: 2px solid var(--destructive); background: rgba(239,68,68,.12); border-radius: 12px; padding: 10px 14px; margin: 0 0 12px; }
  .meeting-alert-banner .title { font-size: 11px; font-weight: 700; color: var(--destructive); text-transform: uppercase; letter-spacing: .03em; margin-bottom: 2px; }
  .meeting-alert-banner .reason { font-size: 12px; color: var(--destructive); }
  .conversation-analysis { display: flex; flex-direction: column; gap: 3px; background: var(--accent); border-radius: 10px; padding: 8px 12px; margin: 0 0 12px; font-size: 12px; color: var(--muted-foreground); }
  .conversation-analysis .ca-label { font-weight: 700; color: var(--foreground); margin-right: 6px; }
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

  /* ── Futuristic glass / depth pass ────────────────────────────────── */
  html { color-scheme: dark; }
  body {
    background:
      radial-gradient(circle at 18% -12%, rgba(56,189,248,.12), transparent 34%),
      radial-gradient(circle at 88% 8%, rgba(99,102,241,.14), transparent 30%),
      linear-gradient(145deg, #05070c 0%, #090b12 52%, #070910 100%);
    background-attachment: fixed;
  }
  body::before {
    content: ""; position: fixed; inset: 0; pointer-events: none; z-index: 0; opacity: .18;
    background-image:
      linear-gradient(rgba(148,163,184,.07) 1px, transparent 1px),
      linear-gradient(90deg, rgba(148,163,184,.07) 1px, transparent 1px);
    background-size: 42px 42px;
    mask-image: linear-gradient(to bottom, #000, transparent 72%);
  }
  #sidebar, #main, #mobileBar { position: relative; z-index: 1; }
  #sidebar {
    background: rgba(9,11,18,.88); backdrop-filter: blur(18px) saturate(125%);
    box-shadow: 18px 0 50px rgba(0,0,0,.18);
  }
  .card, .stat-card, .test-box, .api-health-box, .system-ctrl-box, .workflow-status, .history-card {
    background: linear-gradient(145deg, rgba(24,24,30,.92), rgba(12,15,24,.9));
    box-shadow: 0 14px 36px rgba(0,0,0,.2), inset 0 1px 0 rgba(255,255,255,.025);
    backdrop-filter: blur(12px);
  }
  .card {
    position: relative; overflow: hidden;
    transition: transform .18s ease, border-color .18s ease, box-shadow .18s ease;
  }
  .card::after {
    content: ""; position: absolute; inset: 0 0 auto; height: 1px; pointer-events: none;
    background: linear-gradient(90deg, transparent, color-mix(in srgb, var(--pc, var(--primary)) 55%, transparent), transparent);
  }
  .card:hover {
    transform: translateY(-2px);
    border-color: color-mix(in srgb, var(--pc, var(--border)) 45%, var(--border));
    box-shadow: 0 18px 46px rgba(0,0,0,.28), 0 0 26px color-mix(in srgb, var(--pc, transparent) 8%, transparent);
  }
  .extracted-data {
    margin-top: 14px; border: 1px solid color-mix(in srgb, var(--warning) 24%, var(--border));
    border-radius: 10px; background: rgba(234,179,8,.035); overflow: hidden;
  }
  .extracted-data summary {
    padding: 10px 12px; user-select: none; display: flex; align-items: center; gap: 4px;
    transition: color .15s ease, background .15s ease;
  }
  .extracted-data summary:hover { background: rgba(234,179,8,.07); color: #fde047; }
  .extracted-data summary::before { content: "＋"; width: 16px; display: inline-block; transition: transform .18s ease; }
  .extracted-data[open] summary::before { content: "−"; transform: rotate(180deg); }
  .extracted-data .profile-cols { margin: 0; padding: 12px; border-top: 1px solid rgba(234,179,8,.14); }
  .extracted-data[open] .profile-cols { animation: disclosure-in .18s ease-out; }
  summary:focus-visible, button:focus-visible, a:focus-visible {
    outline: 2px solid var(--info); outline-offset: 3px;
  }
  button { transition: transform .14s ease, filter .14s ease, box-shadow .14s ease, background .14s ease; }
  button:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 8px 22px rgba(0,0,0,.22); }
  button:active:not(:disabled) { transform: translateY(0) scale(.985); }
  @keyframes disclosure-in { from { opacity: 0; transform: translateY(-5px); } to { opacity: 1; transform: none; } }
  @media (prefers-reduced-motion: reduce) {
    *, *::before, *::after { animation-duration: .01ms !important; transition-duration: .01ms !important; }
  }
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
    <a href="/bots" class="sound-toggle" style="margin-top:8px;text-decoration:none;">🤖 Bots</a>
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

  <div class="api-health-box">
    <div class="api-health-head">
      <div>
        <h2>AI API Key Health</h2>
        <span class="test-box-sub">See which configured key is ready, rate-limited, or unavailable. Keys are always masked.</span>
      </div>
      <button class="btn-api-check" id="apiKeyCheckBtn" onclick="checkApiKeys()">Check now</button>
    </div>
    <div class="api-health-grid" id="apiKeyHealth">
      <div class="api-key-card unchecked"><div class="api-key-name">Loading provider status…</div></div>
    </div>
  </div>

  <div class="system-ctrl-box">
    <div class="system-ctrl-head">
      <h2>Bot Controls</h2>
      <span class="test-box-sub">Restart/fix individual bots below, or fill the earnings calculator automatically from every live account.</span>
    </div>
    <div class="system-ctrl-actions">
      <button class="btn-money" id="checkinAllBtn" onclick="runCheckinAll()">Check-in All Calculator</button>
      <span class="system-ctrl-status" id="checkinAllStatus"></span>
    </div>
    <div class="ctrl-output" id="checkinAllOutput">
      <div class="ctrl-output-head"><b>checkinall</b><button class="close-x" onclick="document.getElementById('checkinAllOutput').classList.remove('show')">✕</button></div>
      <div class="ctrl-output-body" id="checkinAllOutputBody"></div>
    </div>
    <div class="ctrl-unavailable" id="ctrlUnavailableNote" style="display:none">
      Launcher bot controls are offline, so Restart/Fix are unavailable. Check-in All still reads any browser tabs that are currently open.
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
// Live snapshots rebuild cards. Persist disclosure state separately so an
// open Extracted data panel never collapses just because telemetry refreshed.
const openDisclosurePanels = new Set();
document.addEventListener("toggle", (event) => {
  const details = event.target;
  if (!(details instanceof HTMLDetailsElement)) return;
  const key = details.dataset.uiKey;
  if (!key) return;
  if (details.open) openDisclosurePanels.add(key);
  else openDisclosurePanels.delete(key);
}, true);
let knownPlatforms = [];
let currentMode = "manual";
let apiKeyHealthState = [];
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

function renderApiKeyHealth(items) {
  if (Array.isArray(items)) apiKeyHealthState = items;
  const root = document.getElementById("apiKeyHealth");
  if (!root) return;
  const labels = {
    ready: "Ready",
    cooldown: "Cooldown",
    error: "Error",
    not_configured: "Not configured",
    unchecked: "Not checked",
  };
  root.innerHTML = apiKeyHealthState.map(item => {
    const cooldown = item.state === "cooldown"
      ? ` · retry in ${Math.max(0, Number(item.cooldown_seconds || 0))}s`
      : "";
    const checked = item.checked_at ? `Last checked ${timeAgo(item.checked_at)}` : "No live check yet";
    const key = item.key_hint ? `<code>${escapeHtml(item.key_hint)}</code>` : "No key in .env";
    return `
      <div class="api-key-card ${escapeHtml(item.state)}">
        <div class="api-key-top">
          <span class="api-key-name">${escapeHtml(item.label)}</span>
          <span class="api-key-state">${escapeHtml(labels[item.state] || item.state)}</span>
        </div>
        <div class="api-key-meta">
          ${key} · ${escapeHtml(item.model || "No model")}<br>
          ${escapeHtml(item.detail || checked)}${escapeHtml(cooldown)}${item.detail ? `<br>${escapeHtml(checked)}` : ""}
        </div>
      </div>`;
  }).join("") || '<div class="api-key-card unchecked"><div class="api-key-name">No providers found</div></div>';
}

async function checkApiKeys() {
  const btn = document.getElementById("apiKeyCheckBtn");
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> Checking…'; }
  try {
    const res = await fetch("/api/ai-keys/check", { method: "POST" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    renderApiKeyHealth(data.providers || []);
    toast("AI key status updated", { type: "success" });
  } catch (error) {
    toast("Could not check AI keys", { type: "error", detail: String(error) });
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "Check now"; }
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
let checkinAllRunning = false;
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
  checkinAllRunning = true;
  btn.disabled = true;
  statusEl.textContent = "Reading Gold stats, Xkuss INs, and Justlo/Linduu/Gnoxx monthly counters…";
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
    checkinAllRunning = false;
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
  const disclosureKey = `extracted:${r.id}`;
  return `
    <details class="extracted-data" data-ui-key="${disclosureKey}" ${openDisclosurePanels.has(disclosureKey) ? "open" : ""}>
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

// Judge AI score pill + reasoning, shared by pending and auto-pilot cards.
function judgeRowHtml(r) {
  if (r.judge_score === null || r.judge_score === undefined) return "";
  const cls = r.judge_score === 10 ? "yes" : (r.judge_score >= 7 ? "action-procrastinate" : "action-decline");
  return `
    <div class="guard-row">
      <span class="test-label">Judge score</span>
      <span class="pill ${cls}">${r.judge_score}/10${r.judge_verdict ? " · " + escapeHtml(r.judge_verdict) : ""}</span>
    </div>
    ${r.judge_reasoning ? `<div class="judge-reasoning">${escapeHtml(r.judge_reasoning)}</div>` : ""}
  `;
}

// Advisory-only: the CLIENT (not the fake account) pushed for a real
// meeting, phone number, or off-platform contact somewhere in the
// conversation. Never blocks anything -- just a heads-up for whoever reviews
// this card.
function meetingAlertBannerHtml(r) {
  if (!r.meeting_alert_requested) return "";
  return `
    <div class="meeting-alert-banner">
      <span>⚠️</span>
      <div>
        <div class="title">Meeting / contact request detected</div>
        <div class="reason">${escapeHtml(r.meeting_alert_reason || "")}</div>
      </div>
    </div>
  `;
}

// Advisory-only: a running read on the conversation as a whole -- tone,
// where it's heading, and what the client seems to expect from it. Shown
// even when no alert fired, since it's useful context on every reply.
function conversationAnalysisHtml(r) {
  if (!r.conversation_tone && !r.conversation_direction && !r.conversation_client_expectation) return "";
  return `
    <div class="conversation-analysis">
      ${r.conversation_tone ? `<div><span class="ca-label">Tone</span> ${escapeHtml(r.conversation_tone)}</div>` : ""}
      ${r.conversation_direction ? `<div><span class="ca-label">Direction</span> ${escapeHtml(r.conversation_direction)}</div>` : ""}
      ${r.conversation_client_expectation ? `<div><span class="ca-label">Client expects</span> ${escapeHtml(r.conversation_client_expectation)}</div>` : ""}
    </div>
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
      ${meetingAlertBannerHtml(r)}
      ${conversationAnalysisHtml(r)}
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
      ${judgeRowHtml(r)}
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
      ${meetingAlertBannerHtml(r)}
      ${conversationAnalysisHtml(r)}
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
      ${judgeRowHtml(r)}
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
  for (const key of [...openDisclosurePanels]) {
    if (key.startsWith("extracted:") && !stillPending.has(key.slice("extracted:".length))) {
      openDisclosurePanels.delete(key);
    }
  }
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
  if (data.ai_keys) renderApiKeyHealth(data.ai_keys);
  renderModeToggle();

  if (data.control) {
    controlsAvailable = !!data.control.available;
    controlPlatformState = data.control.platforms || {};
  }
  if (data.bots) botProcessState = data.bots;
  const checkinBtn = document.getElementById("checkinAllBtn");
  if (checkinBtn) checkinBtn.disabled = checkinAllRunning;
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
    const [pendingRes, historyRes, statusRes, modeRes, botsRes, apiKeysRes] = await Promise.all([
      fetch("/api/requests?status=pending"),
      fetch("/api/requests?status=approved,rejected,cancelled,skip_requested,skipped,transferred,sent,failed&limit=300"),
      fetch("/api/status"),
      fetch("/api/mode"),
      fetch("/api/bots/status"),
      fetch("/api/ai-keys/status"),
    ]);
    await refreshControlStatus();
    applySnapshot({
      status: await statusRes.json(),
      mode: ((await modeRes.json()).mode) || "manual",
      ai_keys: await apiKeysRes.json(),
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

  /* ── Futuristic glass / depth pass ────────────────────────────────── */
  html { color-scheme: dark; min-height: 100%; }
  body {
    min-height: 100vh;
    background:
      radial-gradient(circle at 14% -10%, rgba(56,189,248,.13), transparent 32%),
      radial-gradient(circle at 92% 4%, rgba(99,102,241,.15), transparent 29%),
      linear-gradient(145deg, #05070c, #090b12 54%, #070910);
    background-attachment: fixed; position: relative;
  }
  body::before {
    content: ""; position: fixed; inset: 0; pointer-events: none; opacity: .2;
    background-image:
      linear-gradient(rgba(148,163,184,.07) 1px, transparent 1px),
      linear-gradient(90deg, rgba(148,163,184,.07) 1px, transparent 1px);
    background-size: 42px 42px;
    mask-image: linear-gradient(to bottom, #000, transparent 78%);
  }
  .topbar, .bots-grid { position: relative; z-index: 1; }
  .topbar h1 {
    background: linear-gradient(90deg, #fff, #7dd3fc 48%, #a5b4fc);
    -webkit-background-clip: text; background-clip: text; color: transparent;
  }
  .back-link { background: rgba(15,18,28,.72); backdrop-filter: blur(10px); transition: all .16s ease; }
  .back-link:hover { border-color: rgba(56,189,248,.4); box-shadow: 0 0 24px rgba(56,189,248,.08); }
  .bot-card {
    position: relative; overflow: hidden;
    background: linear-gradient(145deg, rgba(24,24,30,.92), rgba(11,14,23,.9));
    box-shadow: 0 16px 42px rgba(0,0,0,.28), inset 0 1px 0 rgba(255,255,255,.03);
    backdrop-filter: blur(14px) saturate(120%);
    transition: transform .18s ease, border-color .18s ease, box-shadow .18s ease;
  }
  .bot-card::before {
    content: ""; position: absolute; inset: 0 0 auto; height: 1px;
    background: linear-gradient(90deg, transparent, rgba(56,189,248,.45), rgba(99,102,241,.4), transparent);
  }
  .bot-card:hover {
    transform: translateY(-3px); border-color: rgba(99,102,241,.35);
    box-shadow: 0 22px 56px rgba(0,0,0,.34), 0 0 34px rgba(99,102,241,.07);
  }
  details.steps-block {
    border: 1px solid var(--border); border-radius: 10px; padding: 0; overflow: hidden;
    background: rgba(9,11,18,.5);
  }
  details.steps-block summary {
    list-style: none; padding: 10px 12px; user-select: none;
    display: flex; align-items: center; gap: 7px; transition: background .15s ease, color .15s ease;
  }
  details.steps-block summary::-webkit-details-marker { display: none; }
  details.steps-block summary::before { content: "＋"; color: var(--info); width: 15px; transition: transform .18s ease; }
  details.steps-block[open] summary::before { content: "−"; transform: rotate(180deg); }
  details.steps-block summary:hover { background: rgba(56,189,248,.055); color: var(--foreground); }
  details.steps-block .steps-list { margin: 0; padding: 11px 18px 12px 34px; border-top: 1px solid var(--border); animation: disclosure-in .18s ease-out; }
  details.steps-block .steps-list li::marker { color: var(--info); font-weight: 700; }
  button { transition: transform .14s ease, filter .14s ease, box-shadow .14s ease, background .14s ease; }
  button:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 8px 22px rgba(0,0,0,.24); }
  button:active:not(:disabled) { transform: translateY(0) scale(.985); }
  summary:focus-visible, button:focus-visible, a:focus-visible { outline: 2px solid var(--info); outline-offset: 3px; }
  @keyframes disclosure-in { from { opacity: 0; transform: translateY(-5px); } to { opacity: 1; transform: none; } }
  @media (max-width: 760px) {
    body { padding: 16px 14px 42px; }
    .bots-grid { grid-template-columns: 1fr; }
  }
  @media (prefers-reduced-motion: reduce) {
    *, *::before, *::after { animation-duration: .01ms !important; transition-duration: .01ms !important; }
  }
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
  state[p.slug] = { approvalEffective: "manual", running: false, managedBy: null, liveDetail: "", stopping: false, stepsOpen: false };
}

function stepsFor(slug, approval) {
  const approvalStep = approval === "auto"
    ? "Run the meeting guard, then continue automatically without waiting for a reviewer"
    : "Pause on the Approval Dashboard: Approve sends it, Reject regenerates, Cancel abandons it, and Transfer/Skip leaves the chat";
  if (slug === "xkuss") {
    return [
      "Log in, open Home, and keep the Dialog Scanner running",
      "Detect when Home opens a new conversation and verify the chat is really active",
      "Capture the conversation HTML and inject it into Chameleon-AI AgentWorkspace",
      "Extract the data; if Chameleon marks First Contact, leave through Home and return to waiting",
      "Click \"Antwort generieren\" and wait for Chameleon's complete reply",
      approvalStep,
      "If Transfer/Skip was requested, click Home and confirm the dashboard action completed",
      "Otherwise paste the approved reply, wait about 15–20 seconds, and send it",
      "Click Home again, reset Chameleon, and wait for a different conversation",
    ];
  }
  return [
    "Log in, open Mod, start Play, and keep the moderation queue scanner active",
    "Detect a newly loaded conversation and verify the customer/message grid is ready",
    "Capture the conversation HTML and inject it into Chameleon-AI AgentWorkspace",
    "Extract the data and let Chameleon decide whether this is First Contact",
    "For First Contact: click \"Übergeben\"; if nobody is online, click \"Überspringen\" and confirm \"Ja\"",
    "For a normal chat: click \"Antwort generieren\" and wait for Chameleon's complete reply",
    approvalStep,
    "A dashboard Transfer/Skip request uses the same Übergeben → Überspringen → Ja fallback",
    "Otherwise paste the approved reply, wait about 15–20 seconds, and send it",
    "Reset Chameleon and return to Waiting until the scanner loads another conversation",
  ];
}

function renderCard(slug, label) {
  const s = state[slug];
  const running = s.running;
  const statusClass = s.stopping ? "external" : (running ? (s.managedBy === "external" ? "external" : "running") : "");
  const statusText = s.stopping ? "Stopping…" : (running ? (s.managedBy === "external" ? "Running (external)" : "Running") : "Stopped");
  const steps = stepsFor(slug, s.approvalEffective);

  return `
    <div class="bot-card" data-slug="${slug}">
      <div class="bot-card-head">
        <h2>${escapeHtml(label)}</h2>
        <span class="status-pill ${statusClass}">${statusText}</span>
      </div>
      <div class="live-line">${escapeHtml(s.liveDetail || "")}</div>

      <div class="start-stop-row">
        <button class="btn-start" ${(running || s.stopping) ? "disabled" : ""} onclick="startBot('${slug}')">Start</button>
        <button class="btn-stop" ${(!running || s.stopping) ? "disabled" : ""} onclick="stopBot('${slug}')">${s.stopping ? "Stopping…" : "Stop"}</button>
      </div>

      <div class="toggle-row">
        <span class="toggle-name">Approval</span>
        <button class="toggle-switch-btn warn ${s.approvalEffective === "auto" ? "on" : ""}" onclick="toggleApproval('${slug}')">
          <span class="toggle-dot"></span>
          <span class="toggle-switch-label">${s.approvalEffective === "auto" ? "Fully automatic" : "Manual review"}</span>
        </button>
      </div>

      <details class="steps-block" ${s.stepsOpen ? "open" : ""} ontoggle="state['${slug}'].stepsOpen = this.open">
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
  state[slug].stopping = true;
  render();
  try {
    const res = await fetch(`/api/bots/${slug}/stop`, { method: "POST" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) {
      showError(slug, data.error || `HTTP ${res.status}`);
    } else {
      state[slug].running = false;
      state[slug].managedBy = null;
      state[slug].liveDetail = "stopped — Bot and browser stopped";
    }
  } catch (e) {
    showError(slug, "Request failed: " + e);
  } finally {
    state[slug].stopping = false;
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
