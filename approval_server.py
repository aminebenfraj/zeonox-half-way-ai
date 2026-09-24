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

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import httpx
from flask import Flask, jsonify, render_template, request, Response
from dotenv import set_key

from core.ai_providers import gemini_generate, openai_chat_completion
from core.launcher import is_cdp_ready
from core.bot_lifecycle import LauncherClient
from core.bot import pause_flag_path
from core.platform_operations import run_browser_operation
from core.runtime_settings import get_runtime_settings, update_runtime_settings
from core.platforms import (
    KNOWN_PLATFORM_LABELS,
    PLATFORM_BY_SLUG,
    PLATFORM_COLORS,
    PLATFORMS,
    SELF_MANAGED_PLATFORM_SLUGS,
    platform_label,
)
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
KNOWN_PLATFORMS = list(KNOWN_PLATFORM_LABELS)

# One accent color per platform (mirrors the ANSI colors launch_all.py/start_all.py
# already use for terminal output) so a platform is visually identifiable at a
# glance across the sidebar, badges and section headers.
_FALLBACK_PALETTE = ["#8b5cf6", "#06b6d4", "#f97316", "#14b8a6", "#a855f7"]

# launch_all.py's control API (restart/stop/fix/checkinall/... as HTTP instead
# of typed terminal commands — see run_bots() there). Only reachable when this
# dashboard runs on the same machine as launch_all.py, which is the local-dev
# setup; a cloud-hosted dashboard simply won't be able to reach it, and the
# control panel degrades to "unavailable" rather than erroring (see
# _control_get()/_control_post() below).
LAUNCHER_CONTROL_URL = os.environ.get("LAUNCHER_CONTROL_URL", "http://127.0.0.1:8800")
_BASE_DIR = Path(__file__).resolve().parent
_launcher_client = LauncherClient(LAUNCHER_CONTROL_URL, _BASE_DIR)

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

SELF_MANAGED_PLATFORMS = SELF_MANAGED_PLATFORM_SLUGS
SKIPPABLE_APPROVAL_PLATFORMS = frozenset(SELF_MANAGED_PLATFORMS)

# ── Bot process management (see /bots) ──────────────────────────────────────
# Only the self-managed platforms: each does its own Chrome launch +
# login inside run_bot.py, so starting one is just spawning that one process
# -- no Playwright-based Chrome/login bring-up needed here (that stays in
# launch_all.py, kept deliberately out of this process; see KNOWN_PLATFORMS
# above for the same reasoning). React platforms (Gold/Diamond/...) still
# only start via launch_all.py / Bot Controls, unchanged.
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


def _standalone_bot_status(platform: str) -> dict:
    """Best-effort status for one self-managed platform. Distinguishes a bot
    this dashboard started (has a tracked Popen) from one that's merely
    occupying that platform's CDP port some other way (started via
    launch_all.py, a leftover process, ...) so Start can refuse to double-
    launch either way, with an honest reason."""
    proc = _bot_procs.get(platform)
    if proc is not None and proc.poll() is None:
        return {
            "running": True,
            "pid": proc.pid,
            "managed_by": "dashboard",
            "paused": pause_flag_path(platform).exists(),
        }
    if proc is not None:
        _bot_procs.pop(platform, None)  # exited -- stop tracking it as running
    reported_pid = _reported_bot_pid(platform)
    if reported_pid:
        return {
            "running": True,
            "pid": reported_pid,
            "managed_by": "external",
            "paused": pause_flag_path(platform).exists(),
        }
    return {
        "running": False,
        "pid": None,
        "managed_by": None,
        "paused": False,
        "browser_open": is_cdp_ready(_SELF_MANAGED_CDP_PORTS[platform]),
    }


@app.get("/api/bots/status")
def bots_status():
    return jsonify(_all_bot_statuses())


def _all_bot_statuses() -> dict[str, dict]:
    """Return one lifecycle shape for every platform, regardless of owner."""
    control = _control_status_cached()
    launcher_states = (control.get("platforms") or {}) if control.get("available") else {}
    result = {}
    for platform in PLATFORMS:
        launcher_state = launcher_states.get(platform.slug)
        standalone_state = _standalone_bot_status(platform.slug) if platform.self_managed else None
        if launcher_state is not None and launcher_state.get("state") == "running":
            result[platform.slug] = {
                **launcher_state,
                "running": True,
                "managed_by": "launcher",
            }
        elif standalone_state and standalone_state.get("running"):
            result[platform.slug] = standalone_state
        elif launcher_state is not None:
            result[platform.slug] = {
                **launcher_state,
                "running": False,
                "managed_by": "launcher",
            }
        elif platform.self_managed:
            result[platform.slug] = standalone_state
        else:
            result[platform.slug] = {
                "state": "stopped",
                "running": False,
                "pid": None,
                "managed_by": None,
            }
    return result


@app.post("/api/bots/<platform>/start")
def bots_start(platform):
    platform = platform.strip().lower()
    if platform not in PLATFORM_BY_SLUG:
        return jsonify({"ok": False, "error": f"unknown platform '{platform}'"}), 404
    status = _all_bot_statuses()[platform]
    if status["running"]:
        return jsonify({"ok": False, "error": f"{platform} is already running ({status['managed_by']})"}), 409
    control = _control_status_cached()
    if control.get("available"):
        data, status_code = _launcher_client.command("start", platform)
        return jsonify(data), status_code

    try:
        proc = _launcher_client.spawn(platform)
    except RuntimeError as error:
        return jsonify({"ok": False, "error": str(error)}), 409
    return jsonify({"ok": True, "starting": True, "pid": proc.pid}), 202


@app.post("/api/bots/<platform>/stop")
def bots_stop(platform):
    platform = platform.strip().lower()
    if platform not in PLATFORM_BY_SLUG:
        return jsonify({"ok": False, "error": f"unknown platform '{platform}'"}), 404

    status = _all_bot_statuses()[platform]
    control = _control_status_cached()
    if control.get("available") and status.get("managed_by") == "launcher":
        data, status_code = _launcher_client.command("stop", platform)
        if not data.get("ok"):
            return jsonify(data), status_code
        _cancel_pending_for_platform(platform, "Bot stopped")
        return jsonify(data), status_code

    if platform not in SELF_MANAGED_PLATFORMS:
        return jsonify({"ok": True, "already_stopped": True})

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

    _cancel_pending_for_platform(platform, "Bot and browser stopped")

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


def _cancel_pending_for_platform(platform: str, detail: str) -> None:
    """Remove approvals that a stopped worker can no longer complete."""
    label = platform_label(platform)
    now = _now()
    with _lock:
        for item in _requests.values():
            if (
                (item.get("platform") or "").strip().lower() == platform
                and item.get("status") in ("pending", "skip_requested")
            ):
                item["status"] = "cancelled"
                item["decided_at"] = now
        _status[label] = {
            "state": "stopped",
            "detail": detail,
            "retry_count": 0,
            "warning": "",
            "checkpoint": "stopped",
            "pid": None,
            "updated_at": now,
        }
    _bump_state()


# Translation calls use the configured Groq account first and OpenRouter as a
# provider-level fallback. Keep them on a bounded worker thread so an upstream
# outage never stalls a bot cycle indefinitely.
_translate_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="translate")
_judge_pool = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="judge")

GROQ_TRANSLATION_MODEL = os.environ.get("GROQ_TRANSLATION_MODEL", "openai/gpt-oss-120b")
OPENROUTER_TRANSLATION_MODEL = os.environ.get("OPENROUTER_TRANSLATION_MODEL", "openai/gpt-4o")
OPENROUTER_JUDGE_MODEL = os.environ.get(
    "OPENROUTER_MODEL", "cognitivecomputations/dolphin-mistral-24b-venice-edition"
)
NVIDIA_JUDGE_MODEL = os.environ.get("NVIDIA_MODEL", "deepseek-ai/deepseek-v4.1-flash")
GEMINI_JUDGE_MODEL = os.environ.get("GEMINI_MODEL", "gemini-flash-latest")
BAI_JUDGE_MODEL = os.environ.get("BAI_MODEL", "gpt-6-luna")

API_PROVIDER_SPECS = (
    ("groq", "Groq", "GROQ_API_KEY", GROQ_TRANSLATION_MODEL, "Translation and meeting checks"),
    ("openrouter", "OpenRouter", "OPENROUTER_API_KEY", OPENROUTER_JUDGE_MODEL, "Judge primary"),
    ("nvidia", "NVIDIA", "NVIDIA_API_KEY", NVIDIA_JUDGE_MODEL, "Judge fallback 1"),
    ("gemini", "Gemini", "GEMINI_API_KEY", GEMINI_JUDGE_MODEL, "Judge fallback 2"),
    ("bai", "B.AI", "BAI_API_KEY", BAI_JUDGE_MODEL, "Judge fallback 3"),
)
API_PROVIDER_BY_NAME = {item[0]: item for item in API_PROVIDER_SPECS}
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
    "nvidia": {"state": "unchecked", "checked_at": None, "cooldown_until": None, "detail": ""},
    "gemini": {"state": "unchecked", "checked_at": None, "cooldown_until": None, "detail": ""},
    "bai": {"state": "unchecked", "checked_at": None, "cooldown_until": None, "detail": ""},
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
        provider_message = ""
        if response is not None:
            try:
                payload = response.json()
                raw_error = payload.get("error") if isinstance(payload, dict) else None
                if isinstance(raw_error, dict):
                    provider_message = str(raw_error.get("message") or raw_error.get("detail") or "")
                elif raw_error:
                    provider_message = str(raw_error)
                if not provider_message and isinstance(payload, dict):
                    provider_message = str(payload.get("detail") or payload.get("message") or "")
            except (TypeError, ValueError):
                pass
        prefix = f"HTTP {status_code}: " if status_code else ""
        detail = (prefix + provider_message).strip(": ")[:220] if provider_message else (
            f"HTTP {status_code}" if status_code else str(error).strip()[:220]
        )
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
    now = time.time()
    with _api_health_lock:
        stored = {name: dict(value) for name, value in _api_health.items()}
    result = []
    for name, label, env_name, model, purpose in API_PROVIDER_SPECS:
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
            "purpose": purpose,
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
        openai_chat_completion(
            url="https://openrouter.ai/api/v1/chat/completions",
            api_key=api_key,
            model=OPENROUTER_JUDGE_MODEL,
            messages=[{"role": "user", "content": "Reply only with OK."}],
            max_tokens=8,
            timeout=12.0,
            extra_headers={
                "HTTP-Referer": os.environ.get("OPENROUTER_HTTP_REFERER", "http://127.0.0.1:8799"),
                "X-Title": os.environ.get("OPENROUTER_APP_TITLE", "Zenox"),
            },
        )
    except Exception as error:
        _record_api_failure("openrouter", error)
        return
    _record_api_success("openrouter", "Key check succeeded")


def _probe_nvidia_key() -> None:
    api_key = (os.environ.get("NVIDIA_API_KEY") or "").strip()
    if not api_key:
        return
    try:
        openai_chat_completion(
            url="https://integrate.api.nvidia.com/v1/chat/completions",
            api_key=api_key,
            model=NVIDIA_JUDGE_MODEL,
            messages=[{"role": "user", "content": "Reply only with OK."}],
            max_tokens=8,
            timeout=15.0,
        )
    except Exception as error:
        _record_api_failure("nvidia", error)
        return
    _record_api_success("nvidia", "Key check succeeded")


def _probe_gemini_key() -> None:
    api_key = (os.environ.get("GEMINI_API_KEY") or "").strip()
    if not api_key:
        return
    try:
        gemini_generate(
            api_key=api_key,
            model=GEMINI_JUDGE_MODEL,
            system_prompt="Return a small JSON object.",
            user_prompt='Return exactly {"status":"OK"}.',
            max_tokens=20,
            timeout=15.0,
        )
    except Exception as error:
        _record_api_failure("gemini", error)
        return
    _record_api_success("gemini", "Key check succeeded")


def _probe_bai_key() -> None:
    api_key = (os.environ.get("BAI_API_KEY") or "").strip()
    if not api_key:
        return
    try:
        openai_chat_completion(
            url="https://api.b.ai/v1/chat/completions",
            api_key=api_key,
            model=BAI_JUDGE_MODEL,
            messages=[{"role": "user", "content": "Reply only with OK."}],
            max_tokens=8,
            timeout=15.0,
        )
    except Exception as error:
        _record_api_failure("bai", error)
        return
    _record_api_success("bai", "Key check succeeded")


@app.get("/api/ai-keys/status")
def ai_key_status():
    return jsonify(_api_health_snapshot())


@app.post("/api/ai-keys/<provider>")
def update_ai_key(provider):
    provider = provider.strip().lower()
    spec = API_PROVIDER_BY_NAME.get(provider)
    if spec is None:
        return jsonify({"ok": False, "error": f"unknown provider '{provider}'"}), 404
    body = request.get_json(force=True, silent=True) or {}
    value = str(body.get("key") or "").strip()
    if "\n" in value or "\r" in value or len(value) > 2048:
        return jsonify({"ok": False, "error": "invalid API key format"}), 400

    env_name = spec[2]
    try:
        set_key(str(_BASE_DIR / ".env"), env_name, value, quote_mode="always")
    except Exception as error:
        return jsonify({"ok": False, "error": f"Could not update .env: {error}"}), 500
    if value:
        os.environ[env_name] = value
    else:
        os.environ.pop(env_name, None)
    with _api_health_lock:
        _api_health[provider] = {
            "state": "unchecked",
            "checked_at": None,
            "cooldown_until": None,
            "detail": "Key updated; run a check",
        }
    _bump_state()
    item = next(item for item in _api_health_snapshot() if item["provider"] == provider)
    return jsonify({"ok": True, "provider": item})


@app.post("/api/ai-keys/check")
def check_ai_keys():
    # Run both small probes concurrently so one slow provider cannot hold up the
    # other's result. This is only triggered by the dashboard button.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = [
            pool.submit(_probe_groq_key),
            pool.submit(_probe_openrouter_key),
            pool.submit(_probe_nvidia_key),
            pool.submit(_probe_gemini_key),
            pool.submit(_probe_bai_key),
        ]
        for future in futures:
            try:
                future.result(timeout=15)
            except Exception:
                pass
    return jsonify({"ok": True, "providers": _api_health_snapshot()})


# ── Judge provider chain ─────────────────────────────────────────────────────
# The Judge fails over in order: OpenRouter -> NVIDIA -> Gemini -> B.AI. A provider is
# considered failed on network/auth/rate errors or malformed Judge JSON. Only
# when every provider fails does Auto mode fall back to manual review.


def _judge_provider_response(provider: str, payload: dict) -> str:
    user_prompt = json.dumps(payload, ensure_ascii=False)
    if provider == "openrouter":
        api_key = (os.environ.get("OPENROUTER_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY is not set")
        return openai_chat_completion(
            url="https://openrouter.ai/api/v1/chat/completions",
            api_key=api_key,
            model=OPENROUTER_JUDGE_MODEL,
            messages=[
                {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=500,
            timeout=20.0,
            extra_headers={
                "HTTP-Referer": os.environ.get("OPENROUTER_HTTP_REFERER", "http://127.0.0.1:8799"),
                "X-Title": os.environ.get("OPENROUTER_APP_TITLE", "Zenox"),
            },
        )
    if provider == "nvidia":
        api_key = (os.environ.get("NVIDIA_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("NVIDIA_API_KEY is not set")
        return openai_chat_completion(
            url="https://integrate.api.nvidia.com/v1/chat/completions",
            api_key=api_key,
            model=NVIDIA_JUDGE_MODEL,
            messages=[
                {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=500,
            timeout=20.0,
        )
    if provider == "gemini":
        api_key = (os.environ.get("GEMINI_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY is not set")
        return gemini_generate(
            api_key=api_key,
            model=GEMINI_JUDGE_MODEL,
            system_prompt=JUDGE_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            max_tokens=500,
            timeout=20.0,
        )
    if provider == "bai":
        api_key = (os.environ.get("BAI_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("BAI_API_KEY is not set")
        return openai_chat_completion(
            url="https://api.b.ai/v1/chat/completions",
            api_key=api_key,
            model=BAI_JUDGE_MODEL,
            messages=[
                {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=500,
            timeout=20.0,
        )
    raise ValueError(f"Unknown Judge provider: {provider}")


def _parse_judge_response(raw: str, provider: str) -> dict:
    cleaned = (raw or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.I)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError as error:
        raise ValueError("model returned invalid JSON") from error
    try:
        score = max(0, min(10, int(data.get("score"))))
    except (TypeError, ValueError) as error:
        raise ValueError("model returned invalid score") from error
    return {
        "ok": True,
        "provider": provider,
        "score": score,
        "verdict": str(data.get("verdict") or "").strip(),
        "reasoning": str(data.get("reasoning") or "").strip(),
        "analysis": str(data.get("analysis") or data.get("reasoning") or "").strip(),
    }


def _judge_reply(platform: str, last_message: str, customer_message: str,
                  client_profile: dict, fake_profile: dict, reply: str) -> dict:
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
    errors = []
    for provider in ("openrouter", "nvidia", "gemini", "bai"):
        try:
            result = _parse_judge_response(
                _judge_provider_response(provider, payload), provider
            )
        except Exception as error:
            _record_api_failure(provider, error)
            errors.append(f"{API_PROVIDER_BY_NAME[provider][1]}: {error}")
            continue
        _record_api_success(provider, "Judge request succeeded")
        result["fallback_errors"] = errors
        return result
    return {
        "ok": False,
        "error": "All Judge providers failed: " + "; ".join(errors),
        "fallback_errors": errors,
    }


def _complete_judge_analysis(req_id: str, judge_payload: dict, auto_candidate: bool,
                             auto_final_reply: str, auto_policy: str) -> None:
    """Evaluate a queued reply without blocking the bot's approval POST.

    The request is inserted before this worker starts, so the bot always gets
    its request id quickly and waits on exactly one card. Auto-pilot requests
    remain pending until a perfect Judge result promotes that same request to
    approved. Depending on Settings, a non-perfect result either leaves that
    same card for manual review or rejects it so the bot regenerates. Provider
    failure always falls back to manual review.
    """
    judge_result = _judge_reply(**judge_payload)
    judge_score = judge_result.get("score") if judge_result["ok"] else None
    should_notify = False
    created_request = None
    pending_count = 0
    with _lock:
        item = _requests.get(req_id)
        if item is None:
            return
        item.update({
            "judge_score": judge_score,
            "judge_verdict": judge_result.get("verdict") if judge_result["ok"] else None,
            "judge_reasoning": judge_result.get("reasoning") if judge_result["ok"] else None,
            "judge_analysis": judge_result.get("analysis") if judge_result["ok"] else None,
            "judge_provider": judge_result.get("provider") if judge_result["ok"] else None,
            "judge_error": None if judge_result["ok"] else judge_result.get("error"),
            "judge_pending": False,
        })
        if auto_candidate and item["status"] == "pending":
            if judge_result["ok"] and judge_score == 10:
                item["status"] = "approved"
                item["auto"] = True
                item["final_reply"] = auto_final_reply
                item["decided_at"] = _now()
            elif judge_result["ok"] and auto_policy == "judge_regenerate":
                # All bot implementations already interpret a rejection as
                # "discard this candidate and generate a fresh reply".
                item["status"] = "rejected"
                item["auto"] = True
                item["decided_at"] = _now()
            else:
                should_notify = True
                created_request = dict(item)
                pending_count = sum(1 for queued in _requests.values() if queued["status"] == "pending")
    if auto_candidate:
        if judge_result["ok"] and judge_score == 10:
            print(f"[Judge] {judge_payload['platform']} reply scored 10/10 — auto-approved")
        elif judge_result["ok"] and auto_policy == "judge_regenerate":
            print(f"[Judge] {judge_payload['platform']} reply scored {judge_score}/10 — regenerating")
        elif judge_result["ok"]:
            print(f"[Judge] {judge_payload['platform']} reply scored {judge_score}/10 — holding for manual review")
        else:
            print(f"[Judge] All providers failed for {judge_payload['platform']} — holding for manual review: {judge_result.get('error')}")
    _bump_state()
    if should_notify and created_request is not None:
        push_notifications.queue_approval(created_request, pending_count)


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

    runtime_settings = get_runtime_settings()
    auto_policy = runtime_settings["auto_mode_policy"]
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

    # Judge AI is intentionally asynchronous whenever it is enabled. Provider
    # fallbacks can outlive the bot client's POST timeout, so the card is always
    # inserted first. Direct Auto mode skips the Judge entirely; the other Auto
    # policies remain pending until the background decision arrives.
    direct_auto = auto and auto_policy == "direct"
    auto_candidate = auto and not direct_auto
    run_judge = auto_candidate or (
        not auto and runtime_settings["manual_judge_policy"] == "enabled"
    )
    judge_payload = dict(
        platform=platform,
        last_message=last_message,
        customer_message=customer_message,
        client_profile=client_profile,
        fake_profile=fake_profile,
        reply=final_reply,
    )

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
            "final_reply": final_reply if direct_auto else None,
            "status": "approved" if direct_auto else "pending",
            "created_at": body.get("created_at") or now,
            "decided_at": now if direct_auto else None,
            "sent_at": None,
            "error": None,
            "auto": direct_auto,
            "auto_policy": auto_policy if auto else None,
            "meeting_guard": meeting_guard,
            "contains_meeting": contains_meeting,
            "judge_score": None,
            "judge_verdict": None,
            "judge_reasoning": None,
            "judge_analysis": None,
            "judge_provider": None,
            "judge_error": None,
            "judge_enabled": run_judge,
            "judge_pending": run_judge,
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
    if not auto_candidate and not direct_auto:
        push_notifications.queue_approval(created_request, pending_count)
    if run_judge:
        _judge_pool.submit(
            _complete_judge_analysis,
            req_id,
            judge_payload,
            auto_candidate,
            final_reply,
            auto_policy,
        )
    return jsonify({"id": req_id, "auto": direct_auto, "meeting_guard": meeting_guard}), 201


@app.get("/api/push/config")
def push_config():
    return jsonify({
        "enabled": push_notifications.is_available(),
        "public_key": push_notifications.PUBLIC_KEY,
        "subscriptions": push_notifications.subscription_count(),
    })


@app.post("/api/push/test")
def push_test():
    if not push_notifications.is_available():
        return jsonify({"ok": False, "error": "Web Push is not configured"}), 503
    if push_notifications.subscription_count() == 0:
        return jsonify({"ok": False, "error": "No phone is subscribed"}), 409
    try:
        result = push_notifications.queue_payload({
            "title": "Zenox system check",
            "body": "Live phone notifications are working correctly.",
            "icon": "/static/icons/zenox-192-v2.png",
            "badge": "/static/icons/zenox-192-v2.png",
            "tag": "zenox-system-check",
            "url": "/",
            "pendingCount": 0,
        }).result(timeout=20)
    except Exception as error:
        return jsonify({"ok": False, "error": f"Notification test failed: {error}"}), 502
    if not result.get("sent"):
        return jsonify({
            "ok": False,
            "error": result.get("error") or "The push service did not accept the notification",
            **result,
        }), 502
    return jsonify({"ok": True, **result})


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


@app.get("/api/settings")
def get_settings():
    """Return operator policies shared by every platform process."""
    return jsonify(get_runtime_settings())


@app.post("/api/settings")
def save_settings():
    body = request.get_json(force=True, silent=True) or {}
    try:
        settings = update_runtime_settings(body)
    except ValueError as error:
        return jsonify({"ok": False, "error": str(error)}), 400
    except OSError as error:
        return jsonify({"ok": False, "error": f"Could not save settings: {error}"}), 500
    _bump_state()
    return jsonify({"ok": True, "settings": settings})


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
    if platform not in PLATFORM_BY_SLUG:
        return jsonify({"error": f"unknown platform '{platform}'"}), 404
    with _lock:
        override = _mode_overrides.get(platform)
        effective = override or _mode
    return jsonify({"platform": platform, "override": override, "mode": _mode, "effective": effective})


@app.post("/api/mode/override")
def set_mode_override():
    body = request.get_json(force=True, silent=True) or {}
    platform = (body.get("platform") or "").strip().lower()
    if platform not in PLATFORM_BY_SLUG:
        return jsonify({"error": f"unknown platform '{platform}'"}), 404
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
        {
            "slug": platform.slug,
            "name": platform.label,
            "color": platform.color,
            "self_managed": platform.self_managed,
        }
        for platform in PLATFORMS
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
        previous = _status.get(platform)
        current = {
            "pid": pid,
            "state": body.get("state") or "unknown",
            "detail": body.get("detail") or "",
            "retry_count": retry_count,
            "warning": body.get("warning") or "",
            "checkpoint": body.get("checkpoint") or "",
            "updated_at": _now(),
        }
        _status[platform] = current
        # Periodic status posts are heartbeats as well as state reports. Keep
        # their timestamp current, but rebuild every dashboard only when the
        # meaningful workflow state changed.
        changed = previous is None or any(
            previous.get(key) != current.get(key)
            for key in current
            if key != "updated_at"
        )
    if changed:
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
        # Uptime is display-only and changes every second. Ignoring it here
        # prevents a full WebSocket snapshot and DOM rebuild every poll while
        # still pushing every meaningful lifecycle change immediately.
        def semantic(value):
            if not isinstance(value, dict):
                return value
            return {
                key: semantic(item)
                for key, item in value.items()
                if key != "uptime"
            }

        changed = semantic(result) != semantic(previous)
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
        "bots": _all_bot_statuses(),
    }


if sock:
    @sock.route("/ws/live")
    def ws_live(ws):
        last_version = None
        try:
            while True:
                send_snapshot = False
                with _state_cond:
                    if last_version is None or _state_version != last_version:
                        send_snapshot = True
                    else:
                        _state_cond.wait(timeout=15.0)
                        send_snapshot = _state_version != last_version
                    last_version = _state_version
                if send_snapshot:
                    ws.send(json.dumps(_full_snapshot()))
                else:
                    with _lock:
                        status = dict(_status)
                    ws.send(json.dumps({
                        "type": "heartbeat",
                        "version": last_version,
                        "status": status,
                    }))
        except ConnectionClosed:
            pass
        except Exception:
            pass

    @sock.route("/ws/approval/<request_id>")
    def ws_approval(ws, request_id):
        """Small decision stream for one bot; never sends dashboard history."""
        last_payload = None
        try:
            while True:
                with _lock:
                    item = _requests.get(request_id)
                    current = (
                        {key: value for key, value in item.items() if key != "_seq"}
                        if item else None
                    )
                payload = json.dumps(
                    {"type": "approval", "request": current},
                    ensure_ascii=False,
                    sort_keys=True,
                )
                if payload != last_payload:
                    ws.send(payload)
                    last_payload = payload
                else:
                    ws.send(json.dumps({"type": "heartbeat"}))
                if current and current.get("status") in {
                    "approved", "rejected", "cancelled", "skip_requested",
                }:
                    return
                with _state_cond:
                    _state_cond.wait(timeout=15.0)
        except ConnectionClosed:
            pass
        except Exception:
            pass


@app.post("/api/control/command")
def control_command():
    body = request.get_json(force=True, silent=True) or {}
    cmd = (body.get("cmd") or "").strip().lower()
    target = (body.get("target") or "all").strip().lower()
    if not cmd:
        return jsonify({"ok": False, "error": "cmd is required"}), 400
    if target != "all" and target not in PLATFORM_BY_SLUG:
        return jsonify({"ok": False, "error": f"unknown platform '{target}'"}), 404
    if cmd == "pools" and target not in {"justlo", "linduu", "gnoxx"}:
        return jsonify({
            "ok": False,
            "error": "Pools is available only for Justlo, Linduu, and Gnoxx",
        }), 400

    # Pause flags are deliberately file-based inside every worker, so the web
    # app can control them even when the optional launcher API is offline.
    if cmd in ("pause", "resume"):
        statuses = _all_bot_statuses()
        targets = (
            [slug for slug, status in statuses.items() if status.get("running")]
            if target == "all"
            else [target]
        )
        for slug in targets:
            flag = pause_flag_path(slug)
            if cmd == "pause":
                flag.touch()
            else:
                flag.unlink(missing_ok=True)
        _bump_state()
        verb = "Paused" if cmd == "pause" else "Resumed"
        return jsonify({
            "ok": True,
            "output": f"{verb} {len(targets)} running bot(s) from the web dashboard.",
        })

    if cmd in ("checkinall", "checkinsall", "ca"):
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

    control_available = bool(_control_status_cached().get("available"))
    # Pools is always handled by the current dashboard code so it works even
    # when the long-running launcher predates this command. Other browser
    # operations stay local only when launcher control is unavailable.
    if cmd == "pools" or (
        not control_available and cmd in ("fix", "chameleon", "extractor")
    ):
        statuses = _all_bot_statuses()
        targets = (
            [slug for slug, status in statuses.items() if status.get("running")]
            if target == "all"
            else [target]
        )
        try:
            output = asyncio.run(run_browser_operation(cmd, targets))
            if cmd == "pools":
                pause_flag_path(target).unlink(missing_ok=True)
                _bump_state()
            return jsonify({"ok": True, "output": output})
        except Exception as error:
            return jsonify({"ok": False, "error": f"{cmd.title()} failed: {error}"}), 500

    if not control_available and cmd == "stop" and target != "all":
        return bots_stop(target)

    try:
        r = httpx.post(
            f"{LAUNCHER_CONTROL_URL}/control/command",
            json={"cmd": cmd, "target": target},
            # fix/checkins/checkinall drive real Chrome tabs and can take a while
            timeout=120,
        )
        if cmd == "pools" and r.is_success:
            # Pools is a recovery/resume action. Clear a pending dashboard pause
            # only after the real browser click succeeds.
            pause_flag_path(target).unlink(missing_ok=True)
            _bump_state()
        return Response(r.content, status=r.status_code, mimetype="application/json")
    except Exception as e:
        return jsonify({"ok": False, "error": f"Launcher control server unreachable at {LAUNCHER_CONTROL_URL}: {e}"}), 502


# ── Dashboard page ──────────────────────────────────────────────────────────

_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
<meta name="theme-color" content="#09090b" />
<meta name="apple-mobile-web-app-capable" content="yes" />
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent" />
<meta name="apple-mobile-web-app-title" content="Zenox" />
<link rel="manifest" href="/static/manifest.webmanifest" />
<link rel="icon" type="image/png" href="/static/icons/zenox-192-v2.png" />
<link rel="apple-touch-icon" href="/static/icons/zenox-180-v2.png" />
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
    width: var(--sidebar-w); flex: none; height: 100vh; height: 100dvh; position: sticky; top: 0;
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
  .stat-card.actionable {
    width: 100%; color: inherit; font: inherit; text-align: left; cursor: pointer;
    transition: transform .15s ease, border-color .15s ease, box-shadow .15s ease;
  }
  .stat-card.actionable:hover:not(:disabled) { transform: translateY(-1px); border-color: var(--warning); }
  .stat-card.actionable:focus-visible { outline: 2px solid var(--warning); outline-offset: 2px; }
  .stat-card.actionable:disabled { cursor: default; opacity: .65; }
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

  .cards-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(560px, 1fr)); gap: 14px; }

  .card {
    background: var(--card); border: 1px solid var(--border);
    border-left: 3px solid var(--border);
    border-left-color: color-mix(in srgb, var(--pc, var(--border)) 70%, var(--border));
    border-radius: var(--radius);
    padding: 16px 18px; box-shadow: 0 1px 2px rgba(0,0,0,.25);
  }
  .card.review-target { animation: review-target-pulse 1.4s ease-out; }
  @keyframes review-target-pulse {
    0%, 35% { border-color: var(--warning); box-shadow: 0 0 0 4px rgba(234,179,8,.24), 0 14px 36px rgba(0,0,0,.28); }
    100% { box-shadow: 0 1px 2px rgba(0,0,0,.25); }
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
  .message-judge-grid {
    display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(220px, .65fr);
    align-items: stretch; gap: 12px; margin: 0 0 14px;
  }
  .message-pane { min-width: 0; }
  .judge-panel {
    --judge-color: var(--muted-foreground); position: relative; overflow: hidden;
    border: 1px solid color-mix(in srgb, var(--judge-color) 38%, var(--border));
    border-radius: 12px; padding: 12px; background:
      linear-gradient(145deg, color-mix(in srgb, var(--judge-color) 9%, var(--card)), rgba(10,12,18,.72));
    box-shadow: inset 0 1px 0 rgba(255,255,255,.035);
  }
  .judge-panel.excellent { --judge-color: var(--success); }
  .judge-panel.review { --judge-color: var(--warning); }
  .judge-panel.risk, .judge-panel.error { --judge-color: var(--destructive); }
  .judge-panel::before {
    content: ""; position: absolute; inset: 0 0 auto; height: 2px;
    background: linear-gradient(90deg, transparent, var(--judge-color), transparent);
  }
  .judge-head { display: flex; align-items: center; gap: 10px; }
  .judge-score {
    display: grid; place-items: center; width: 54px; height: 54px; flex: none;
    border-radius: 50%; border: 2px solid var(--judge-color); color: var(--judge-color);
    background: color-mix(in srgb, var(--judge-color) 9%, transparent);
    box-shadow: 0 0 18px color-mix(in srgb, var(--judge-color) 18%, transparent);
    font-size: 18px; line-height: 1; font-weight: 800;
  }
  .judge-score small { display: block; font-size: 8px; margin-top: -8px; opacity: .72; }
  .judge-eyebrow { color: var(--muted-foreground); font-size: 9px; font-weight: 750; text-transform: uppercase; letter-spacing: .09em; }
  .judge-verdict { margin-top: 2px; color: var(--foreground); font-size: 13px; line-height: 1.25; font-weight: 750; }
  .judge-reason { margin: 10px 0 0; color: var(--muted-foreground); font-size: 11.5px; line-height: 1.5; }
  .judge-reason strong { color: var(--foreground); }
  .judge-analysis { margin-top: 10px; border-top: 1px solid var(--border); }
  .judge-analysis summary {
    cursor: pointer; list-style: none; user-select: none; padding: 9px 0 0;
    color: var(--judge-color); font-size: 11px; font-weight: 700;
    display: flex; align-items: center; justify-content: space-between; gap: 8px;
  }
  .judge-analysis summary::-webkit-details-marker { display: none; }
  .judge-analysis summary::after { content: "+"; font-size: 15px; transition: transform .18s ease; }
  .judge-analysis[open] summary::after { content: "−"; transform: rotate(180deg); }
  .judge-analysis-body {
    padding: 9px 0 1px; color: var(--muted-foreground); font-size: 11.5px;
    line-height: 1.55; border-top: 1px solid color-mix(in srgb, var(--judge-color) 18%, transparent);
    margin-top: 8px;
  }
  @media (max-width: 760px) {
    .cards-grid { grid-template-columns: 1fr; }
    .message-judge-grid { grid-template-columns: 1fr; }
  }
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
  .btn-pause { background: var(--primary); color: #fff; }
  .btn-pause.paused { background: var(--success); color: #052e16; }
  .btn-pause:disabled { opacity: .55; cursor: default; }
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
    body { display: block; min-height: 100vh; min-height: 100dvh; }

    #mobileBar {
      display: flex; align-items: center; gap: 12px;
      position: sticky; top: 0; z-index: 30;
      min-height: 60px;
      padding: max(8px, env(safe-area-inset-top)) max(14px, env(safe-area-inset-right)) 8px max(14px, env(safe-area-inset-left));
      background: rgba(9,11,18,.94); border-bottom: 1px solid var(--border);
      backdrop-filter: blur(18px) saturate(125%);
    }
    #hamburger {
      display: flex; flex-direction: column; justify-content: center; gap: 4px;
      flex: none; width: 44px; height: 44px; padding: 0; border-radius: 10px;
      background: var(--accent); border: 1px solid var(--border); cursor: pointer;
    }
    #hamburger span { display: block; width: 16px; height: 2px; background: var(--foreground); margin: 0 auto; border-radius: 2px; }
    #mobileBar .brand-text { font-weight: 600; font-size: 14.5px; }
    #mobileBar .brand-dot { width: 8px; height: 8px; border-radius: 999px; background: var(--success); box-shadow: 0 0 0 3px rgba(34,197,94,.18); }
    #mobileBar .mobile-pending {
      margin-left: auto; min-width: 0; max-width: 42vw; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
      font-size: 12px; font-weight: 700; color: var(--warning);
      background: rgba(234,179,8,.15); border: 0; border-radius: 999px; padding: 3px 10px; cursor: pointer;
    }
    #mobileBar .mobile-pending:disabled { cursor: default; opacity: .65; }

    #sidebar {
      position: fixed; top: 0; bottom: 0; left: 0; z-index: 50; width: min(84vw, 300px);
      height: 100vh; height: 100dvh; padding-bottom: env(safe-area-inset-bottom);
      transform: translateX(-100%); transition: transform .22s ease;
      box-shadow: 8px 0 24px rgba(0,0,0,.4);
      overscroll-behavior: contain;
    }
    #sidebar.open { transform: translateX(0); }
    #backdrop.open {
      display: block; position: fixed; inset: 0; z-index: 40;
      background: rgba(0,0,0,.55); backdrop-filter: blur(1px);
    }

    #main {
      width: 100%;
      padding: 16px max(14px, env(safe-area-inset-right)) calc(48px + env(safe-area-inset-bottom)) max(14px, env(safe-area-inset-left));
    }
    .page-head h1 { font-size: 18px; }
    .page-head p { font-size: 13px; }

    .stats { grid-template-columns: repeat(2, 1fr); gap: 8px; margin-bottom: 22px; }
    .stat-card { padding: 12px 13px; }
    .stat-card .value { font-size: 20px; }

    .cards-grid { grid-template-columns: 1fr; }
    .card { padding: 14px; }
    .card:hover { transform: none; }

    .platform-head h2 { font-size: 14.5px; }

    .btn-money, .btn-pause { min-height: 44px; }
    .system-ctrl-actions { align-items: stretch; flex-direction: column; }
    .system-ctrl-status { overflow-wrap: anywhere; }

    /* Full-width, stacked action buttons are far easier to hit accurately
       with a thumb than two small side-by-side buttons. */
    .actions { flex-direction: column; align-items: stretch; }
    .actions button { width: 100%; padding: 12px 14px; font-size: 14px; }
    .hint { order: 3; text-align: center; }

    /* iOS Safari auto-zooms the page when a focused input's font is under
       16px — keep the textarea at 16px so approving on a phone doesn't
       trigger an unwanted zoom-in. */
    textarea.reply-input { font-size: 16px; min-height: 100px; }

    .nav-item, .sound-toggle, .mode-toggle { min-height: 44px; }
    .nav-item { padding: 10px 12px; font-size: 14px; }

    .profile-cols { display: block; }
    .profile-col { min-width: 0; width: 100%; }
    .profile-col + .profile-col { margin-top: 14px; }
    .profile-row { display: grid; grid-template-columns: minmax(78px, 36%) minmax(0, 1fr); }
    .profile-row .k { min-width: 0; }
    .profile-row > span:last-child, .last-message, .proposed, .en-box, .history-reply, .history-en {
      min-width: 0; overflow-wrap: anywhere; word-break: break-word;
    }
    .extracted-data summary { min-height: 44px; flex-wrap: wrap; }
  }

  @media (max-width: 420px) {
    .modal-actions { flex-direction: column-reverse; }
    .modal-actions button { width: 100%; min-height: 44px; }
    #modalRoot { padding: 14px; padding-bottom: max(14px, env(safe-area-inset-bottom)); }
    #toastRoot { right: 10px; bottom: max(10px, env(safe-area-inset-bottom)); width: calc(100vw - 20px); }
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
  #main { position: relative; z-index: 1; }
  #sidebar, #mobileBar { z-index: 1; }
  @media (max-width: 860px) {
    #mobileBar { z-index: 30; }
    #sidebar { z-index: 50; }
  }
  #sidebar {
    background: rgba(9,11,18,.88); backdrop-filter: blur(18px) saturate(125%);
    box-shadow: 18px 0 50px rgba(0,0,0,.18);
  }
  .card, .stat-card, .test-box, .system-ctrl-box, .workflow-status, .history-card {
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

  /* ── shadcn-inspired application shell ───────────────────────────────
     The dashboard is server-rendered, so these native component classes use
     the same neutral tokens, inset layout, menu groups, cards and switches
     without adding a React runtime to the bot control process. */
  :root {
    --background: #09090b; --foreground: #fafafa; --card: #18181b;
    --muted: #18181b; --muted-foreground: #a1a1aa; --accent: #27272a;
    --border: #27272a; --input: #27272a; --primary: #6366f1;
    --primary-foreground: #fafafa; --ring: #6366f1; --radius: 9px;
    --sidebar-w: 272px;
  }
  body {
    background:
      radial-gradient(circle at 18% -12%, rgba(56,189,248,.12), transparent 34%),
      radial-gradient(circle at 88% 8%, rgba(99,102,241,.14), transparent 30%),
      linear-gradient(145deg, #05070c 0%, #090b12 52%, #070910 100%);
    padding: 8px; gap: 8px;
    color: var(--foreground);
  }
  #sidebar {
    height: calc(100vh - 16px); height: calc(100dvh - 16px); top: 8px;
    background: rgba(9,11,18,.9); border: 1px solid var(--border); border-radius: 12px;
    box-shadow: 18px 0 50px rgba(0,0,0,.18); backdrop-filter: blur(18px) saturate(125%); overflow: hidden;
  }
  .sidebar-header { min-height: 68px; padding: 14px; border-bottom: 1px solid var(--border); }
  .brand-mark {
    display: grid; place-items: center; width: 34px; height: 34px; flex: none;
    border-radius: 9px; background: var(--primary); color: var(--primary-foreground);
    font-size: 15px; font-weight: 800; letter-spacing: -.04em;
  }
  .brand-text { display: block; font-size: 14px; line-height: 1.2; font-weight: 700; }
  .brand-caption { display: block; margin-top: 2px; color: var(--muted-foreground); font-size: 11px; }
  .brand-sub {
    margin-left: auto; padding: 2px 7px; border: 1px solid rgba(34,197,94,.25);
    border-radius: 999px; color: var(--success); background: rgba(34,197,94,.08);
    font-size: 9px; font-weight: 700; text-transform: uppercase; letter-spacing: .06em;
  }
  .workspace-nav, .platform-nav { padding: 8px; flex: none; }
  .platform-nav { flex: 1; min-height: 0; overflow-y: auto; border-top: 1px solid var(--border); }
  .nav-label { padding: 7px 8px 5px; font-size: 10px; letter-spacing: .08em; font-weight: 700; }
  .sidebar-link, .nav-item {
    display: flex; align-items: center; gap: 9px; width: 100%; min-height: 36px;
    padding: 7px 9px; border-radius: 7px; border: 0; background: transparent;
    color: var(--muted-foreground); text-decoration: none; font-size: 12.5px; font-weight: 520;
  }
  .sidebar-link:hover, .nav-item:hover { background: var(--accent); color: var(--foreground); }
  .sidebar-link.active { background: rgba(99,102,241,.16); color: #c7d2fe; }
  .sidebar-icon { display: grid; place-items: center; width: 18px; height: 18px; color: currentColor; font-size: 16px; }
  .sidebar-badge {
    margin-left: auto; min-width: 20px; padding: 0 6px; border-radius: 999px;
    background: var(--primary); color: #fff; text-align: center; font-size: 10px; font-weight: 700;
  }
  .sidebar-group { padding: 8px; border-top: 1px solid var(--border); border-bottom: 0; }
  .mode-toggle, .preference-row {
    width: 100%; min-height: 46px; display: flex; align-items: center; gap: 10px;
    padding: 7px 9px; border: 0; border-radius: 7px; background: transparent;
    color: var(--foreground); text-align: left; font: inherit; cursor: pointer;
  }
  .mode-toggle:hover, .preference-row:hover { background: var(--accent); filter: none; }
  .mode-copy, .preference-copy { flex: 1; min-width: 0; display: flex; flex-direction: column; }
  .mode-toggle-label, .preference-copy strong { color: var(--foreground); font-size: 12.5px; font-weight: 600; }
  .mode-toggle-hint, .preference-copy small { margin: 1px 0 0; color: var(--muted-foreground); font-size: 10.5px; line-height: 1.25; }
  .mode-toggle.auto .mode-toggle-label { color: var(--foreground); }
  .mode-switch, .ui-switch {
    position: relative; flex: none; width: 32px; height: 18px; border-radius: 999px;
    background: #3f3f46; border: 1px solid transparent; transition: background .16s;
  }
  .mode-switch::after, .ui-switch::after {
    content: ""; position: absolute; width: 14px; height: 14px; top: 1px; left: 1px;
    border-radius: 999px; background: #fafafa; transition: transform .16s;
  }
  .mode-toggle.auto .mode-switch, .preference-row.checked .ui-switch { background: var(--primary); }
  .mode-toggle.auto .mode-switch::after, .preference-row.checked .ui-switch::after {
    transform: translateX(14px); background: #fff;
  }
  .preference-icon {
    display: grid; place-items: center; width: 28px; height: 28px; flex: none;
    border: 1px solid var(--border); border-radius: 7px; background: var(--muted);
    color: var(--muted-foreground); font-size: 13px; font-weight: 700;
  }
  .preference-row:disabled { opacity: .5; cursor: not-allowed; }
  .sidebar-foot { padding: 9px 12px; background: rgba(9,11,18,.72); }
  .live-badge { margin: 0; border: 0; background: transparent; padding: 3px; }

  #main {
    max-width: none; min-height: calc(100vh - 16px); min-height: calc(100dvh - 16px);
    padding: 30px clamp(20px, 3vw, 44px) 64px; border: 1px solid var(--border);
    border-radius: 12px; background: rgba(9,11,18,.82);
  }
  .page-head { margin-bottom: 18px; }
  .page-head h1 { font-size: 24px; font-weight: 700; letter-spacing: -.035em; }
  .page-head p { max-width: 760px; margin-top: 4px; color: var(--muted-foreground); }
  .system-status { margin-bottom: 22px; }
  .system-status .chip { background: transparent; padding: 5px 9px; }
  .stats { grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; margin-bottom: 18px; }
  .stat-card, .system-ctrl-box, .card, .history-card, .workflow-status, .test-box {
    background: var(--card); border: 1px solid var(--border); box-shadow: none; backdrop-filter: none;
  }
  .stat-card { padding: 15px; border-radius: var(--radius); }
  .stat-card .label { margin: 0; font-size: 11px; font-weight: 500; }
  .stat-card .value { margin-top: 7px; font-size: 24px; font-weight: 700; }
  .stat-card.actionable:hover:not(:disabled) { transform: none; border-color: #52525b; box-shadow: none; }
  .system-ctrl-box { padding: 16px; margin-bottom: 28px; }
  .system-ctrl-head { justify-content: space-between; }
  .system-ctrl-head h2 { font-size: 14px; }
  .system-ctrl-actions button, .ctrl-btn, .actions button {
    border: 1px solid var(--border); border-radius: 7px; min-height: 34px;
    box-shadow: none; font-weight: 600;
  }
  .btn-pause { background: var(--primary); color: #fff; }
  .btn-pause.paused { background: var(--success); color: #052e16; }
  .btn-money { background: var(--warning); color: #1c1500; }
  button:hover:not(:disabled) { transform: none; box-shadow: none; filter: brightness(1.08); }
  .platform-section { margin-bottom: 32px; }
  .platform-head { margin-bottom: 10px; }
  .platform-head h2 { color: var(--foreground); font-size: 14px; }
  .pc-dot { width: 8px; height: 8px; box-shadow: none; }
  .card { border-radius: var(--radius); }
  .card::after { display: none; }
  .card:hover { transform: none; border-color: #3f3f46; box-shadow: none; }
  .history-card { border-left-width: 1px; border-radius: var(--radius); }
  .toast { border-left-width: 1px; box-shadow: 0 10px 30px rgba(0,0,0,.35); }
  .queue-skeleton { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin: 8px 0 30px; }
  .queue-skeleton span, .skeleton-card, .app-loading .stat-card .value {
    position: relative; overflow: hidden; background: #18181b;
  }
  .queue-skeleton span { display: block; min-height: 190px; border: 1px solid var(--border); border-radius: var(--radius); }
  .skeleton-card { min-height: 58px; }
  .skeleton-card.short { width: 72%; }
  .queue-skeleton span::after, .skeleton-card::after, .app-loading .stat-card .value::after {
    content: ""; position: absolute; inset: 0; transform: translateX(-100%);
    background: linear-gradient(90deg, transparent, rgba(255,255,255,.055), transparent);
    animation: skeleton-wave 1.25s ease-in-out infinite;
  }
  .app-loading .stat-card .value { width: 42px; height: 27px; border-radius: 6px; color: transparent !important; }
  @keyframes skeleton-wave { to { transform: translateX(100%); } }

  @media (max-width: 1000px) { .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
  @media (max-width: 860px) {
    body { padding: 0; }
    #sidebar { top: 0; height: 100vh; height: 100dvh; border-radius: 0 12px 12px 0; }
    #main { min-height: calc(100vh - 60px); border: 0; border-radius: 0; padding-top: 22px; }
    #mobileBar { background: rgba(9,9,11,.94); }
    .queue-skeleton { grid-template-columns: 1fr; }
  }
</style>
</head>
<body class="app-loading">
<div id="toastRoot" aria-live="polite"></div>
<div id="modalRoot"></div>
<header id="mobileBar">
  <button id="hamburger" onclick="toggleDrawer()" aria-label="Toggle platform menu" aria-controls="sidebar" aria-expanded="false"><span></span><span></span><span></span></button>
  <span class="brand-dot"></span>
  <span class="brand-text">Chat Approval</span>
  <button type="button" class="mobile-pending" id="mobilePendingBadge" onclick="goToPendingReview()" disabled>0 pending</button>
</header>
<div id="backdrop" onclick="closeDrawer()" aria-hidden="true"></div>

<aside id="sidebar">
  <div class="brand sidebar-header">
    <span class="brand-mark">Z</span>
    <span><span class="brand-text">Zenox</span><span class="brand-caption">Operations</span></span>
    <span class="brand-sub">Live</span>
  </div>
  <nav class="workspace-nav">
    <div class="nav-label">Workspace</div>
    <a href="/" class="sidebar-link active" aria-current="page">
      <span class="sidebar-icon">⌁</span><span>Approval Queue</span><span class="sidebar-badge" id="sidebarPendingBadge">0</span>
    </a>
    <a href="/bots" class="sidebar-link"><span class="sidebar-icon">◫</span><span>Bots</span></a>
    <a href="/settings" class="sidebar-link"><span class="sidebar-icon">⚙</span><span>Settings</span></a>
    <a href="/api-keys" class="sidebar-link"><span class="sidebar-icon">⌘</span><span>API Keys</span></a>
  </nav>
  <div class="mode-box sidebar-group">
    <div class="nav-label">Review mode</div>
    <button id="modeToggle" class="mode-toggle" onclick="toggleMode()">
      <span class="mode-copy"><span class="mode-toggle-label" id="modeToggleLabel">Manual Review</span><span class="mode-toggle-hint" id="modeToggleHint">Replies wait for approval</span></span>
      <span class="mode-switch" aria-hidden="true"></span>
    </button>
  </div>
  <div class="sidebar-group preferences">
    <div class="nav-label">Preferences</div>
    <button id="soundToggle" class="preference-row" onclick="toggleSound()" aria-pressed="true">
      <span class="preference-icon">♪</span><span class="preference-copy"><strong>Sound</strong><small id="soundToggleStatus">On</small></span><span class="ui-switch" aria-hidden="true"></span>
    </button>
    <button id="pushToggle" class="preference-row" onclick="togglePushNotifications()" aria-pressed="false">
      <span class="preference-icon">◉</span><span class="preference-copy"><strong>Notifications</strong><small id="pushToggleStatus">Off</small></span><span class="ui-switch" aria-hidden="true"></span>
    </button>
  </div>
  <nav class="platform-nav">
    <div class="nav-label">Platforms</div>
    <div id="navList"></div>
  </nav>
  <div class="sidebar-foot"><span class="live-badge" id="liveBadge" title="Dashboard connection"><span class="dot"></span><span id="liveBadgeLabel">Connecting…</span></span></div>
</aside>

<main id="main">
  <div class="page-head">
    <h1>Approval Queue</h1>
    <p>Review every AI reply before it's pasted and sent — German is what actually goes out, English is a translation for review.</p>
  </div>

  <div class="system-status" id="systemStatus"></div>

  <div class="stats">
    <button type="button" class="stat-card warn actionable" id="pendingReviewCard" onclick="goToPendingReview()" disabled aria-label="Open the next pending approval"><div class="label">Pending review</div><div class="value" id="statPending">0</div></button>
    <div class="stat-card ok"><div class="label">Sent today</div><div class="value" id="statSent">0</div></div>
    <div class="stat-card bad"><div class="label">Rejected today</div><div class="value" id="statRejected">0</div></div>
    <div class="stat-card"><div class="label">Platforms active</div><div class="value" id="statPlatforms">0</div></div>
  </div>

  <div class="system-ctrl-box">
    <div class="system-ctrl-head">
      <h2>Bot Controls</h2>
      <span class="test-box-sub">Restart/fix individual bots below, or fill the earnings calculator automatically from every live account.</span>
    </div>
    <div class="system-ctrl-actions">
      <button class="btn-pause" id="globalPauseBtn" onclick="toggleGlobalPause()" disabled>⏸ Pause all bots</button>
      <span class="system-ctrl-status" id="globalPauseStatus"></span>
      <button class="btn-money" id="checkinAllBtn" onclick="runCheckinAll()">Check-in All Calculator</button>
      <span class="system-ctrl-status" id="checkinAllStatus"></span>
    </div>
    <div class="ctrl-output" id="checkinAllOutput">
      <div class="ctrl-output-head"><b>checkinall</b><button class="close-x" onclick="document.getElementById('checkinAllOutput').classList.remove('show')">✕</button></div>
      <div class="ctrl-output-body" id="checkinAllOutputBody"></div>
    </div>
    <div class="ctrl-unavailable" id="ctrlUnavailableNote" style="display:none">
      Advanced launcher controls are offline. Pause, Resume, Fix, Chameleon, Extractor, Pools, Stop, and Check-in All still work from the web app; Restart requires the launcher.
    </div>
  </div>

  <div id="sections" aria-live="polite" aria-busy="true">
    <div class="queue-skeleton"><span></span><span></span><span></span></div>
  </div>

  <div class="platform-section">
    <div class="platform-head"><h2>Recent activity</h2><hr /></div>
    <div id="history" class="history-list"><div class="history-card skeleton-card"></div><div class="history-card skeleton-card short"></div></div>
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
  document.getElementById("hamburger").setAttribute("aria-expanded", "true");
  document.body.classList.add("drawer-open");
}
function closeDrawer() {
  document.getElementById("sidebar").classList.remove("open");
  document.getElementById("backdrop").classList.remove("open");
  document.getElementById("hamburger").setAttribute("aria-expanded", "false");
  document.body.classList.remove("drawer-open");
}
function toggleDrawer() {
  document.getElementById("sidebar").classList.contains("open") ? closeDrawer() : openDrawer();
}
document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeDrawer(); });
window.addEventListener("resize", () => { if (window.innerWidth > 860) closeDrawer(); });

function goToSection(name) {
  document.getElementById(slug(name)).scrollIntoView({ behavior: "smooth", block: "start" });
  closeDrawer();
}

function goToPendingReview() {
  const card = nextPendingReviewId
    ? document.getElementById(`approval-${nextPendingReviewId}`)
    : document.querySelector("#sections .card[data-id]");
  if (!card) {
    toast("No approvals are waiting", { type: "info", duration: 2200 });
    return;
  }
  closeDrawer();
  card.scrollIntoView({ behavior: "smooth", block: "center" });
  card.setAttribute("tabindex", "-1");
  card.focus({ preventScroll: true });
  card.classList.remove("review-target");
  void card.offsetWidth;
  card.classList.add("review-target");
  window.setTimeout(() => card.classList.remove("review-target"), 1500);
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
let serviceWorkerRegistration = null;
let currentPushSubscription = null;
let pushConfigured = false;
let pushSubscriptionCount = 0;
let nextPendingReviewId = null;

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
  btn.classList.toggle("checked", soundEnabled);
  btn.setAttribute("aria-pressed", soundEnabled ? "true" : "false");
  const status = document.getElementById("soundToggleStatus");
  if (status) status.textContent = soundEnabled ? "On · plays a test when enabled" : "Off";
}

function toggleSound() {
  soundEnabled = !soundEnabled;
  localStorage.setItem("approvalSoundEnabled", soundEnabled ? "1" : "0");
  unlockAudio();
  if (soundEnabled) playNotifySound();
  renderSoundToggle();
  if (soundEnabled) toast("Sound enabled", { type: "success", detail: "The test chime just played.", duration: 2200 });
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
  btn.classList.toggle("checked", state === "enabled");
  btn.setAttribute("aria-pressed", state === "enabled" ? "true" : "false");
  const labels = {
    enabled: "On · test sent when enabled",
    disabled: "Off",
    "needs-install": "Add app to Home Screen first",
    denied: "Blocked in device settings",
    unconfigured: "Push server not configured",
    unsupported: "Unavailable in this browser",
  };
  const status = document.getElementById("pushToggleStatus");
  if (status) status.textContent = labels[state] || "Off";
  btn.title = detail || "";
}

async function initPushNotifications() {
  try {
    const configRes = await fetch("/api/push/config");
    const config = await configRes.json();
    pushConfigured = !!config.enabled && !!config.public_key;
    pushSubscriptionCount = Number(config.subscriptions || 0);
    if (!pushConfigured) {
      renderPushToggle("unconfigured", "Restart the dashboard after installing requirements and configuring VAPID.");
      return;
    }
  } catch (error) {
    renderPushToggle("unconfigured", String(error));
    return;
  }
  if (!("serviceWorker" in navigator) || !("PushManager" in window) || !("Notification" in window)) {
    renderPushToggle("unsupported", "This browser does not support Web Push.");
    return;
  }
  if (isIOS() && !isStandaloneApp()) {
    renderPushToggle("needs-install", "In Safari, use Share → Add to Home Screen, then open the Zenox icon.");
    return;
  }
  try {
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
      const response = await fetch("/api/push/subscribe", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ endpoint })
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok || !result.ok) throw new Error(result.error || `HTTP ${response.status}`);
      await currentPushSubscription.unsubscribe();
      currentPushSubscription = null;
      pushSubscriptionCount = Number(result.subscriptions || 0);
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
    pushSubscriptionCount = Number(result.subscriptions || pushSubscriptionCount || 1);
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
    ? "Replies send immediately"
    : "Replies wait for approval";
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
  paused:             { label: "Paused",               color: "var(--warning)" },
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
  // authority for online/offline. Workflow telemetry provides the richer
  // Waiting/Generating/etc. label. If a long browser operation outlives the
  // freshness window, retain that last known stage instead of collapsing to
  // a vague "Running" state while the launcher still confirms the process.
  if (processStopped) {
    const label = process.state === "dead" ? "Process stopped" : "Stopped";
    return { live: false, label, color: "var(--border)", detail: "", state: "offline", retryCount: 0, warning: "", checkpoint: "" };
  }
  if (processRunning && process.paused) {
    const pauseConfirmed = telemetryFresh && s.state === "paused";
    return {
      live: true, label: pauseConfirmed ? "Paused" : "Pause requested", color: "var(--warning)",
      detail: pauseConfirmed ? "Paused at a safe checkpoint" : "Will pause at the next safe checkpoint",
      state: "paused",
      retryCount: 0, warning: "", checkpoint: "paused",
    };
  }
  if (processRunning && s && !telemetryFresh) {
    const meta = STATE_META[s.state] || { label: s.state, color: "var(--info)" };
    const age = Number.isFinite(updatedMs) ? timeAgo(s.updated_at) : "an unknown time ago";
    return {
      live: true, label: meta.label, color: meta.color,
      detail: `${s.detail || meta.label} · last update ${age}; process is online`, state: s.state,
      retryCount: Number(s.retry_count || 0), warning: s.warning || "", checkpoint: s.checkpoint || "",
    };
  }
  if (processRunning && !s) {
    return {
      live: true, label: "Running", color: "var(--success)",
      detail: "Bot process is online; waiting for its first workflow update", state: "starting",
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
  starting: 0, waiting: 0, waiting_for_chat: 0, idle: 0, paused: 0, chat_detected: 1,
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
let globalPauseRunning = false;
let controlPlatformState = {}; // slug -> {state, uptime, crashes, exit_code}
let botProcessState = {}; // dashboard-managed bot Popen/CDP state from /api/bots/status
// Last command output per platform, kept here (not just in the DOM) because
// renderSections() fully rebuilds #sections on every poll — without this the
// panel would vanish again 1.5s after a command finished.
let controlOutputs = new Map(); // slug -> output text

function updateGlobalPauseControl() {
  const btn = document.getElementById("globalPauseBtn");
  const status = document.getElementById("globalPauseStatus");
  if (!btn || !status) return;
  const active = Object.values(botProcessState).filter(st => st && (st.state === "running" || st.running === true));
  const pausedCount = active.filter(st => st.paused).length;
  const allPaused = active.length > 0 && pausedCount === active.length;
  btn.dataset.action = allPaused ? "resume" : "pause";
  btn.textContent = allPaused ? "▶ Resume all bots" : "⏸ Pause all bots";
  btn.classList.toggle("paused", allPaused);
  btn.disabled = globalPauseRunning || !active.length;
  status.textContent = !active.length
    ? "No bots running"
    : allPaused
      ? `Pause requested for all ${active.length} bots`
      : pausedCount
        ? `Pause requested for ${pausedCount}/${active.length} bots`
        : `${active.length} bots running`;
}

async function toggleGlobalPause() {
  const btn = document.getElementById("globalPauseBtn");
  if (!btn || btn.disabled) return;
  const cmd = btn.dataset.action === "resume" ? "resume" : "pause";
  globalPauseRunning = true;
  updateGlobalPauseControl();
  const dismiss = toast(cmd === "pause" ? "Pausing all bots…" : "Resuming all bots…", { type: "info", duration: 0 });
  try {
    const res = await fetch("/api/control/command", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cmd, target: "all" }),
    });
    const data = await res.json().catch(() => ({}));
    dismiss();
    if (!res.ok || !data.ok) {
      toast(`Could not ${cmd} all bots`, { type: "error", detail: data.error || `HTTP ${res.status}` });
      return;
    }
    for (const state of Object.values(botProcessState)) {
      if (state && (state.state === "running" || state.running === true)) state.paused = cmd === "pause";
    }
    toast(cmd === "pause" ? "Pause requested for all bots" : "All bots resumed", {
      type: "success",
      detail: cmd === "pause" ? "Each bot will stop at its next safe checkpoint." : "",
      duration: 3200,
    });
  } catch (e) {
    dismiss();
    toast(`Could not ${cmd} all bots`, { type: "error", detail: "Request failed — check your connection." });
  } finally {
    globalPauseRunning = false;
    updateGlobalPauseControl();
    refresh();
  }
}

const CTRL_BUTTONS = [
  { cmd: "restart",   label: "Restart" },
  { cmd: "fix",       label: "Fix" },
  { cmd: "chameleon", label: "Chameleon" },
  { cmd: "extractor", label: "Extractor" },
  { cmd: "stop",      label: "Stop", cls: "danger" },
];
const POOLS_PLATFORMS = new Set(["justlo", "linduu", "gnoxx"]);

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
  updateGlobalPauseControl();
  const note = document.getElementById("ctrlUnavailableNote");
  if (note) note.style.display = controlsAvailable ? "none" : "block";
  renderSystemStatus();
}

function fmtUptime(seconds) {
  if (seconds == null) return "";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h${Math.floor((seconds % 3600) / 60)}m`;
}

const CTRL_VERBS = {
  restart: "Restarting", fix: "Running Fix on", chameleon: "Opening Chameleon tab for",
  extractor: "Opening Extractor tab for", pools: "Opening Pools for", stop: "Stopping",
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
    if (cmd === "pools") {
      closeControlOutput(slug);
    } else {
      showControlOutput(slug, data.ok ? (data.output || "(no output)") : `Error: ${data.error || res.status}`);
    }
  } catch (e) {
    dismiss();
    toast(`${slug}: ${cmd} failed`, { type: "error", detail: "Request failed — check your connection." });
    if (cmd !== "pools") showControlOutput(slug, `Request failed: ${e}`);
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
  const slug = name.toLowerCase();
  const st = controlPlatformState[slug] || botProcessState[slug];
  let pillHtml = "";
  if (st) {
    const label = st.state === "running"
      ? `${st.paused ? "PAUSED" : "RUNNING"} ${fmtUptime(st.uptime)}${st.crashes ? ` · ${st.crashes} crash${st.crashes === 1 ? "" : "es"}` : ""}`
      : st.state === "dead" ? `DEAD (exit ${st.exit_code})` : "STOPPED";
    pillHtml = `<span class="ctrl-status-pill ${escapeHtml(st.state)}">${escapeHtml(label)}</span>`;
  }
  let platformButtons = [...CTRL_BUTTONS];
  if (POOLS_PLATFORMS.has(slug)) {
    platformButtons.splice(4, 0, { cmd: "pools", label: "Pools" });
  }
  const availableButtons = controlsAvailable
    ? platformButtons
    : platformButtons.filter(b => ["fix", "chameleon", "extractor", "pools", "stop"].includes(b.cmd));
  const buttons = availableButtons.map(b =>
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

// Judge side panel, shared by pending and auto-pilot cards. It is deliberately
// always rendered: unavailable/failed judging must be visible, never silently
// confused with a missing UI feature.
function judgePanelHtml(r) {
  const hasScore = r.judge_score !== null && r.judge_score !== undefined && Number.isFinite(Number(r.judge_score));
  const disabled = r.judge_enabled === false;
  if (disabled) return "";
  const score = hasScore ? Number(r.judge_score) : null;
  const state = hasScore ? (score === 10 ? "excellent" : (score >= 7 ? "review" : "risk")) : (r.judge_error ? "error" : "unchecked");
  const verdict = hasScore ? (r.judge_verdict || (score === 10 ? "Ready to send" : "Human review advised")) : (r.judge_error ? "Judge unavailable" : "Not evaluated");
  const reason = r.judge_reasoning || r.judge_error || "No Judge result is attached to this request. New cards receive a live score after the updated server starts.";
  const analysis = r.judge_analysis || r.judge_reasoning || r.judge_error || "There is no analysis for this request.";
  const disclosureKey = `judge:${r.id}`;
  return `
    <aside class="judge-panel ${state}" aria-label="AI Judge result">
      <div class="judge-head">
        <div class="judge-score">${hasScore ? score : "—"}${hasScore ? "<small>/10</small>" : ""}</div>
        <div>
          <div class="judge-eyebrow">AI Judge</div>
          <div class="judge-verdict">${escapeHtml(verdict)}</div>
          ${r.judge_provider ? `<div class="api-key-meta">via ${escapeHtml(r.judge_provider)}</div>` : ""}
        </div>
      </div>
      <p class="judge-reason"><strong>Why:</strong> ${escapeHtml(reason)}</p>
      <details class="judge-analysis" data-ui-key="${disclosureKey}" ${openDisclosurePanels.has(disclosureKey) ? "open" : ""}>
        <summary>View full analysis</summary>
        <div class="judge-analysis-body">${escapeHtml(analysis)}</div>
      </details>
    </aside>
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
      <div class="message-judge-grid">
        <div class="message-pane">
          ${lastMessage ? `
            <div class="field-label customer-label">Last Message <span class="lang-tag tag-de">DE</span></div>
            <div class="de-box">${escapeHtml(lastMessage)}</div>
            ${lastMessageEn ? `
              <div class="translation-row">
                <span class="lang-tag tag-en">EN</span>
                <span class="en-box">${escapeHtml(lastMessageEn)}</span>
              </div>` : ""}
          ` : '<div class="empty-state">No message context supplied.</div>'}
        </div>
        ${judgePanelHtml(r)}
      </div>
      <div class="card-divider"></div>
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
      ${judgePanelHtml(r)}
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
  // Snapshots arrive newest-first. Review the oldest request first so no card
  // can remain buried while newer approvals continue to arrive.
  nextPendingReviewId = pending.length ? pending[pending.length - 1].id : null;
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
  const runningOrder = order.filter(name => detectorFor(name).live);

  document.getElementById("sections").innerHTML = runningOrder.length ? runningOrder.map(name => {
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
  }).join("") : '<div class="empty-state">No bots are running. Start one from the Bots page.</div>';

  // Sidebar nav, same order.
  document.getElementById("navList").innerHTML = runningOrder.map(name => {
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
  document.getElementById("statPlatforms").textContent = runningOrder.length;
  const pendingReviewCard = document.getElementById("pendingReviewCard");
  pendingReviewCard.disabled = !pending.length;
  pendingReviewCard.setAttribute("aria-label", pending.length
    ? `Open the oldest of ${pending.length} pending approvals`
    : "No pending approvals");
  const mobilePendingBadge = document.getElementById("mobilePendingBadge");
  mobilePendingBadge.textContent = `${pending.length} pending`;
  mobilePendingBadge.disabled = !pending.length;

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
  renderModeToggle();

  if (data.control) {
    controlsAvailable = !!data.control.available;
    controlPlatformState = data.control.platforms || {};
  }
  if (data.bots) botProcessState = data.bots;
  updateGlobalPauseControl();
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

  const pendingList = (data.pending || []).filter(r => detectorFor(r.platform).live);
  const currentPendingIds = new Set(pendingList.map(r => r.id));
  if (knownPendingIds !== null) {
    for (const id of currentPendingIds) {
      if (!knownPendingIds.has(id)) { playNotifySound(); break; }
    }
  }
  knownPendingIds = currentPendingIds;
  renderSections(pendingList, autoByPlatform);
  const sidebarPending = document.getElementById("sidebarPendingBadge");
  if (sidebarPending) sidebarPending.textContent = String(pendingList.length);
  const sections = document.getElementById("sections");
  if (sections) sections.setAttribute("aria-busy", "false");
  document.body.classList.remove("app-loading");
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
  refreshControlStatus();
  try {
    const readJson = async url => {
      const response = await fetch(url);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response.json();
    };
    const results = await Promise.allSettled([
      readJson("/api/requests?status=pending"),
      readJson("/api/requests?status=approved,rejected,cancelled,skip_requested,skipped,transferred,sent,failed&limit=300"),
      readJson("/api/status"),
      readJson("/api/mode"),
      readJson("/api/bots/status"),
    ]);
    if (results[0].status !== "fulfilled" || results[1].status !== "fulfilled") return;
    applySnapshot({
      status: results[2].status === "fulfilled" ? results[2].value : liveStatus,
      mode: results[3].status === "fulfilled" ? (results[3].value.mode || currentMode) : currentMode,
      control: { available: controlsAvailable, platforms: controlPlatformState },
      bots: results[4].status === "fulfilled" ? results[4].value : botProcessState,
      history: results[1].value,
      pending: results[0].value,
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
let deferredSnapshot = null;

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
    try {
      const snapshot = JSON.parse(ev.data);
      if (snapshot.type === "heartbeat") {
        if (snapshot.status) liveStatus = snapshot.status;
        renderSystemStatus();
        return;
      }
      if (document.activeElement && document.activeElement.classList.contains("reply-input")) {
        deferredSnapshot = snapshot;
        return;
      }
      applySnapshot(snapshot);
    } catch (e) {}
  };
  socket.onclose = () => { liveConnected = false; setLiveIndicator(false); scheduleReconnect(); };
  socket.onerror = () => { try { socket.close(); } catch (e) {} };
}

document.addEventListener("focusout", event => {
  if (!event.target.classList || !event.target.classList.contains("reply-input") || !deferredSnapshot) return;
  const snapshot = deferredSnapshot;
  deferredSnapshot = null;
  requestAnimationFrame(() => applySnapshot(snapshot));
});

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
  connectLive();
  refresh();
  // Only polls while the live socket is down — see setLiveIndicator() above.
  setInterval(() => { if (!liveConnected) refresh(); }, 3000);
}

init();
</script>
</body>
</html>
"""




# ── Bots launcher page ───────────────────────────────────────────────────────
# The single place to start/stop every supported platform and configure
# Approval (human review vs fully automatic) independently — no terminal
# commands needed. Each card is self-contained: changing one never touches
# another platform's process or Approval setting.
_BOTS_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
<title>Bots — Launcher</title>
<link rel="icon" type="image/png" href="/static/icons/zenox-192-v2.png" />
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
  * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
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
    body {
      min-height: 100vh; min-height: 100dvh;
      padding: max(16px, env(safe-area-inset-top)) max(14px, env(safe-area-inset-right)) calc(42px + env(safe-area-inset-bottom)) max(14px, env(safe-area-inset-left));
    }
    .bots-grid { grid-template-columns: 1fr; }
    .bot-card { min-width: 0; padding: 15px 14px; }
    .topbar { align-items: flex-start; margin-bottom: 18px; }
    .topbar > div { min-width: 0; flex: 1 1 210px; }
    .back-link { min-height: 44px; display: inline-flex; align-items: center; }
    .start-stop-row { gap: 10px; }
    .start-stop-row button { flex: 1; min-width: 0; min-height: 46px; }
    .toggle-row { align-items: flex-start; flex-wrap: wrap; }
    .toggle-name { padding-top: 11px; }
    .toggle-switch-btn { min-height: 44px; max-width: 100%; }
    .toggle-switch-label { overflow-wrap: anywhere; }
    details.steps-block summary { min-height: 44px; display: flex; align-items: center; }
    ol.steps-list { padding-left: 22px; overflow-wrap: anywhere; }
    .bot-card-head { align-items: flex-start; }
    .status-pill { flex: none; max-width: 50%; text-align: center; overflow-wrap: anywhere; }
    .toast { overflow-wrap: anywhere; }
  }
  @media (max-width: 420px) {
    .modal-actions { flex-direction: column-reverse; }
    .modal-actions button { width: 100%; min-height: 44px; }
    #modalRoot { padding: 14px; padding-bottom: max(14px, env(safe-area-inset-bottom)); }
    #toastRoot { right: 10px; bottom: max(10px, env(safe-area-inset-bottom)); width: calc(100vw - 20px); }
  }
  @media (prefers-reduced-motion: reduce) {
    *, *::before, *::after { animation-duration: .01ms !important; transition-duration: .01ms !important; }
  }
  /* shadcn-style page composition shared with the Approval Queue. */
  body {
    max-width: 1440px; margin: 0 auto; padding: 32px clamp(18px,4vw,54px) 64px;
    background:
      radial-gradient(circle at 18% -12%, rgba(56,189,248,.12), transparent 34%),
      radial-gradient(circle at 88% 8%, rgba(99,102,241,.14), transparent 30%),
      linear-gradient(145deg, #05070c 0%, #090b12 52%, #070910 100%);
    color: #fafafa;
  }
  .topbar, .bots-grid { z-index: auto; }
  .topbar { align-items: flex-start; margin-bottom: 26px; }
  .topbar h1 { font-size: 24px; font-weight: 700; letter-spacing: -.035em; }
  .back-link { border-radius: 7px; background: #18181b; color: #fafafa; }
  .bots-grid { grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 12px; }
  .bot-card {
    padding: 17px; border-radius: 10px;
    background: linear-gradient(145deg, rgba(24,24,30,.92), rgba(12,15,24,.9)); border-color: #27272a;
    box-shadow: 0 14px 36px rgba(0,0,0,.2); backdrop-filter: blur(12px);
  }
  .bot-card::after { display: none; }
  .bot-card:hover { transform: none; border-color: #3f3f46; box-shadow: none; }
  .status-pill { border-radius: 999px; background: #18181b; }
  .start-stop-row button { min-height: 36px; border-radius: 7px; }
  .btn-start { background: var(--success); color: #052e16; }
  .btn-stop { background: transparent; }
  .toggle-switch-btn { min-height: 34px; border-radius: 7px; background: #18181b; padding: 5px 10px; }
  details.steps-block { border-radius: 8px; background: #09090b; }
  button:hover:not(:disabled) { transform: none; box-shadow: none; }
  .spinner {
    display:inline-block; width:12px; height:12px; margin-right:6px; vertical-align:-2px;
    border:2px solid currentColor; border-right-color:transparent; border-radius:999px;
    animation:spin .65s linear infinite;
  }
  @keyframes spin { to { transform:rotate(360deg); } }
</style>
</head>
<body>
<div id="toastRoot" aria-live="polite"></div>
<div id="modalRoot"></div>
<div class="topbar">
  <a class="back-link" href="/">&larr; Approval Dashboard</a>
  <a class="back-link" href="/settings">Settings</a>
  <div>
    <h1>Bots</h1>
    <p>Start, stop, and configure every platform from one place. Each card controls only its own bot.</p>
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

const PLATFORMS = __PLATFORM_DATA__;

// Per-platform client-side state, kept separate per card by construction --
// every fetch/render/action below is always scoped to one `slug`, never "all".
const state = {};
for (const p of PLATFORMS) {
  state[p.slug] = { approvalEffective: "manual", running: false, managedBy: null, liveDetail: "", error: "", starting: false, startingAt: 0, stopping: false, stepsOpen: false, loaded: false };
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
  if (!PLATFORMS.find(p => p.slug === slug)?.selfManaged) {
    return [
      "Open the platform and Chameleon browser sessions",
      "Log in when needed and wait for an incoming conversation",
      "Capture the conversation and inject it into Chameleon-AI AgentWorkspace",
      "Extract the customer data and generate a policy-compliant reply",
      approvalStep,
      "Paste the approved reply, send it, reset Chameleon, and wait for the next chat",
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
  const busy = s.starting || s.stopping;
  const statusClass = !s.loaded ? "external" : (busy ? "external" : (running ? (s.managedBy === "external" ? "external" : "running") : ""));
  const statusText = !s.loaded ? "Loading…" : (s.starting ? "Starting…" : (s.stopping ? "Stopping…" : (running ? (s.managedBy === "external" ? "Running (external)" : "Running") : "Stopped")));
  const steps = stepsFor(slug, s.approvalEffective);

  return `
    <div class="bot-card" data-slug="${slug}">
      <div class="bot-card-head">
        <h2>${escapeHtml(label)}</h2>
        <span class="status-pill ${statusClass}">${statusText}</span>
      </div>
      <div class="live-line">${escapeHtml(s.liveDetail || "")}</div>

      <div class="start-stop-row">
        <button class="btn-start" ${(running || busy || !s.loaded) ? "disabled" : ""} onclick="startBot('${slug}')">${s.starting ? '<span class="spinner"></span>Starting' : "Start"}</button>
        <button class="btn-stop" ${(!running || busy || !s.loaded) ? "disabled" : ""} onclick="stopBot('${slug}')">${s.stopping ? '<span class="spinner"></span>Stopping' : "Stop"}</button>
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

      <div class="error-note" id="err-${slug}">${escapeHtml(s.error || "")}</div>
    </div>
  `;
}

function render() {
  document.getElementById("botsGrid").innerHTML = PLATFORMS.map(p => renderCard(p.slug, p.label)).join("");
}

function showError(slug, msg) {
  state[slug].error = msg;
  const el = document.getElementById(`err-${slug}`);
  if (el) el.textContent = msg;
}

async function startBot(slug) {
  showError(slug, "");
  state[slug].starting = true;
  state[slug].startingAt = Date.now();
  state[slug].liveDetail = "Starting bot and preparing its browser session…";
  render();
  try {
    const res = await fetch(`/api/bots/${slug}/start`, { method: "POST" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) {
      state[slug].starting = false;
      showError(slug, data.error || `HTTP ${res.status}`);
    }
  } catch (e) {
    state[slug].starting = false;
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
      state[p.slug].loaded = true;
      if (s.running || Date.now() - state[p.slug].startingAt > 180_000) {
        state[p.slug].starting = false;
      }
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

let botsLiveSocket = null;
let botsLiveConnected = false;
let botsReconnectDelay = 1000;

function applyBotSnapshot(snapshot) {
  const bots = snapshot.bots || {};
  const statuses = snapshot.status || {};
  for (const p of PLATFORMS) {
    const bot = bots[p.slug] || {};
    state[p.slug].running = !!bot.running;
    state[p.slug].managedBy = bot.managed_by || null;
    state[p.slug].loaded = true;
    if (bot.running || Date.now() - state[p.slug].startingAt > 180_000) state[p.slug].starting = false;
    const entry = statuses[p.label] || statuses[p.label.toLowerCase()];
    if (entry) state[p.slug].liveDetail = `${entry.state}${entry.detail ? " — " + entry.detail : ""}`;
  }
  render();
}

function connectBotsLive() {
  if (!("WebSocket" in window)) return;
  try {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    botsLiveSocket = new WebSocket(`${proto}//${location.host}/ws/live`);
  } catch (error) {
    setTimeout(connectBotsLive, botsReconnectDelay);
    botsReconnectDelay = Math.min(botsReconnectDelay * 1.6, 15000);
    return;
  }
  botsLiveSocket.onopen = () => { botsLiveConnected = true; botsReconnectDelay = 1000; };
  botsLiveSocket.onmessage = event => {
    try {
      const snapshot = JSON.parse(event.data);
      if (snapshot.type !== "heartbeat") applyBotSnapshot(snapshot);
    } catch (error) {}
  };
  botsLiveSocket.onclose = () => {
    botsLiveConnected = false;
    setTimeout(connectBotsLive, botsReconnectDelay);
    botsReconnectDelay = Math.min(botsReconnectDelay * 1.6, 15000);
  };
  botsLiveSocket.onerror = () => { try { botsLiveSocket.close(); } catch (error) {} };
}

async function loadInitial() {
  await Promise.allSettled(PLATFORMS.map(async p => {
    try {
      const res = await fetch(`/api/mode/override?platform=${p.slug}`);
      const data = await res.json();
      if (data.effective) state[p.slug].approvalEffective = data.effective;
    } catch (e) { /* keep default */ }
  }));
  await refreshStatus();
  connectBotsLive();
  setInterval(() => { if (!botsLiveConnected) refreshStatus(); }, 3000);
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
    platform_data = [
        {
            "slug": platform.slug,
            "label": platform.label,
            "selfManaged": platform.self_managed,
        }
        for platform in PLATFORMS
    ]
    return Response(
        _BOTS_PAGE.replace("__PLATFORM_DATA__", json.dumps(platform_data)),
        mimetype="text/html",
    )


@app.get("/api-keys")
def api_keys_page():
    return render_template("api_keys.html")


@app.get("/settings")
def settings_page():
    return render_template("settings.html")


def main():
    print(f"[ApprovalServer] Dashboard running at http://{HOST}:{PORT}")
    if not (AUTH_USER and AUTH_PASS):
        print("[ApprovalServer] WARNING: APPROVAL_USER/APPROVAL_PASS not set -- "
              "no login required. Fine on 127.0.0.1, unsafe on the public internet.")
    app.run(host=HOST, port=PORT, threaded=True)


if __name__ == "__main__":
    main()
