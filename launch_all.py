#!/usr/bin/env python3
"""
Full-system launcher — one command to rule them all.

Steps performed automatically:
  1. Start Chrome instances (skips any that are already running)
  2. Wait for all CDP ports to respond
  3. Open tabs + log in to every platform in parallel
  4. Launch all bots with smart crash-restart

Restart policy:
  exit code 0 = clean/setup exit → do NOT restart (needs manual intervention)
  exit code 1 = runtime crash    → restart with exponential backoff

Usage:
    python launch_all.py                        # launch all platforms
    python launch_all.py gold plat              # launch only gold + platin
    python launch_all.py diamond s69 gold2      # launch only those three
"""

import sys
import io
import asyncio
import contextlib
import logging
import subprocess
import time
import threading
import queue
from pathlib import Path

# Force UTF-8 stdout so bot output with non-ASCII chars (German etc.) prints correctly
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
from importlib import import_module

from flask import Flask, jsonify, request
from playwright.async_api import async_playwright
from core.launcher import is_cdp_ready, start_chrome, wait_for_cdp, ensure_approval_server, CONTROL_SERVER_PORT
from core.login import login_mod_site, login_chameleon, check_chameleon, force_extractor_tab, check_stats

BASE_DIR = Path(__file__).parent

# React platforms need Chrome started + mod-site/chameleon login done for them
# (Phases 1–2 below). The self-managed ones bring up their own Chrome and log
# themselves in inside their run() (xkuss_bot / justlo_bot), so they skip Phase 2.
REACT_PLATFORMS   = ["gold", "gold2", "gold3", "diamond", "platin", "s69", "ml"]
SELF_MANAGED      = ["xkuss", "justlo", "linduu"]
KNOWN_PLATFORMS   = REACT_PLATFORMS + SELF_MANAGED
# `python launch_all.py` with no args starts exactly these eight.
DEFAULT_PLATFORMS = ["gold", "ml", "platin", "s69", "diamond", "xkuss", "justlo", "linduu"]
# Stats-capable platforms ('checkins'/'checkinall' read the React mod-site dialog).
ALL_PLATFORMS     = REACT_PLATFORMS

_CLI_ALIASES = {"plat": "platin", "g2": "gold2", "g3": "gold3", "lindu": "linduu", "just": "justlo"}

def _resolve_platforms(args: list[str]) -> list[str]:
    resolved = []
    for a in args:
        name = _CLI_ALIASES.get(a.lower(), a.lower())
        if name not in KNOWN_PLATFORMS:
            print(f"[Launcher] Unknown platform '{a}'. Options: {', '.join(KNOWN_PLATFORMS)}")
            sys.exit(1)
        if name not in resolved:
            resolved.append(name)
    return resolved or DEFAULT_PLATFORMS

PLATFORMS = _resolve_platforms(sys.argv[1:])

RESTART_BASE_DELAY = 10    # seconds for first restart after crash
RESTART_MAX_DELAY  = 300   # cap backoff at 5 minutes
CRASH_WINDOW       = 300   # reset crash counter if bot ran this long without crashing

_COLORS = {
    "gold":    "\033[33m",
    "gold2":   "\033[93m",
    "gold3":   "\033[95m",
    "diamond": "\033[36m",
    "platin":  "\033[37m",
    "s69":     "\033[35m",
    "ml":      "\033[32m",
    "xkuss":   "\033[31m",
    "justlo":  "\033[34m",
    "linduu":  "\033[92m",
}

_ALIASES = _CLI_ALIASES
_RESET = "\033[0m"
_BOLD  = "\033[1m"

_HELP = (
    f"{_BOLD}Available commands:{_RESET}\n"
    "  status                    — show all bot statuses\n"
    "  stop  [platform|all]      — stop one or all bots\n"
    "  restart [platform|all]    — restart one or all bots\n"
    "  chameleon [platform|all]  — check if Chameleon tab is on the right page\n"
    "  extractor [platform|all]  — force-activate the Chat Extractor tab\n"
    "  fix [platform|all]        — full Chameleon re-setup: navigate + login + select chat + extractor tab\n"
    "  checkins [platform|all]   — open 'Meine Statistiken' and report money made (Ins + ASA Outs)\n"
    "  checkinall                — save/update a Desktop note (checkinall.txt) with money made across all accounts\n"
    "  help                      — show this list\n"
    "  quit / exit               — stop everything and exit\n"
    f"  Active platforms: {', '.join(PLATFORMS)}  (aliases: plat=platin, g2=gold2, lindu=linduu)\n"
    f"  Launch specific: python launch_all.py gold justlo linduu\n"
    f"  Every command above is also available as a button on the approval dashboard."
)


# ── Command execution / output capture ──────────────────────────────────────────
# Every command below (typed in this terminal OR clicked as a dashboard button,
# see the control API in run_bots()) runs through _execute_captured() so both
# entry points share one code path: it serializes commands with _cmd_lock (a
# terminal 'restart' and a dashboard button click can never race each other),
# and mirrors everything printed into a string so the HTTP response can show the
# dashboard exactly what the terminal would have seen.

_cmd_lock = threading.Lock()


class _Tee(io.TextIOBase):
    """Writes to every given stream — lets a command's prints go to the real
    console (unchanged terminal UX) and into an in-memory buffer at once."""

    def __init__(self, *streams):
        self._streams = streams

    def write(self, s):
        for st in self._streams:
            st.write(s)
        return len(s)

    def flush(self):
        for st in self._streams:
            st.flush()


def _execute_captured(fn, *args, **kwargs) -> str:
    buf = io.StringIO()
    with _cmd_lock:
        with contextlib.redirect_stdout(_Tee(sys.stdout, buf)):
            fn(*args, **kwargs)
    return buf.getvalue()


# ── Phase 1: Chrome ────────────────────────────────────────────────────────────

def _port(cfg) -> int:
    return int(cfg.cdp_url.rsplit(":", 1)[-1])


def start_chrome_instances() -> dict:
    procs = {}
    for name in PLATFORMS:
        cfg  = import_module(f"configs.{name}").config
        port = _port(cfg)
        if is_cdp_ready(port):
            print(f"[Launcher] {name.upper()}: Chrome already running on port {port}.")
        else:
            profile = str(BASE_DIR / "profiles" / name)
            print(f"[Launcher] {name.upper()}: starting Chrome on port {port}...")
            procs[name] = start_chrome(profile, port)
    return procs


def wait_all_cdp():
    print()
    failed = []
    for name in PLATFORMS:
        cfg  = import_module(f"configs.{name}").config
        port = _port(cfg)
        if wait_for_cdp(port, timeout=30):
            print(f"[Launcher] {name.upper()}: Chrome ready on port {port}.")
        else:
            print(f"[ERROR] {name.upper()}: Chrome did not respond on port {port} after 30 s.")
            failed.append(name)
    if failed:
        sys.exit(1)


# ── Phase 2: Browser sessions ──────────────────────────────────────────────────

async def _find_or_open_tab(context, url_pattern: str):
    for page in context.pages:
        if url_pattern in page.url:
            return page
    return await context.new_page()


async def _setup_platform(playwright, name: str, cfg):
    browser = await playwright.chromium.connect_over_cdp(cfg.cdp_url)
    context = browser.contexts[0]

    tab1 = await _find_or_open_tab(context, cfg.tab1_pattern)
    await login_mod_site(tab1, cfg.tab1_url, cfg.username, cfg.password, cfg.platform)

    tab2 = await _find_or_open_tab(context, cfg.tab2_pattern)
    await login_chameleon(tab2, cfg.chameleon_email, cfg.chameleon_password, cfg.platform, cfg.chameleon_chat)

    print(f"[{cfg.platform}] Both tabs ready.", flush=True)


async def setup_all_browsers():
    # Only React platforms are logged in here. xkuss/justlo/linduu sign themselves
    # in inside their own run() (different sites + login flows), so we skip them.
    react = [n for n in PLATFORMS if n in REACT_PLATFORMS]
    skipped = [n for n in PLATFORMS if n in SELF_MANAGED]
    if skipped:
        print(f"[Launcher] {', '.join(s.upper() for s in skipped)} log in themselves "
              f"when their bot starts — skipping browser-side login for them.", flush=True)
    if not react:
        return

    async with async_playwright() as p:
        tasks = [
            _setup_platform(p, name, import_module(f"configs.{name}").config)
            for name in react
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    errors = [(react[i], r) for i, r in enumerate(results) if isinstance(r, Exception)]
    for name, err in errors:
        print(f"[Launcher] WARNING — {name.upper()} setup failed: {err}", flush=True)

    if len(errors) == len(react):
        print("[Launcher] All React platforms failed to set up. Aborting.")
        sys.exit(1)


# ── Phase 3: Bots ──────────────────────────────────────────────────────────────

def _stream_output(platform: str, proc: subprocess.Popen):
    color = _COLORS.get(platform, "")
    for line in proc.stdout:
        print(f"{color}{line}{_RESET}", end="", flush=True)


def _launch_bot(platform: str) -> subprocess.Popen:
    import os
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    proc = subprocess.Popen(
        [sys.executable, "-u", "run_bot.py", platform],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        env=env,
    )
    threading.Thread(target=_stream_output, args=(platform, proc), daemon=True).start()
    return proc


def run_bots():
    """Launch all bots as subprocesses with smart restart on crash."""
    procs:        dict[str, subprocess.Popen] = {name: _launch_bot(name) for name in PLATFORMS}
    start_times:  dict[str, float]            = {name: time.time()        for name in PLATFORMS}
    crash_counts: dict[str, int]              = {name: 0                  for name in PLATFORMS}
    stopped:      set[str]                    = set()
    stop_all      = threading.Event()
    cmd_q         = queue.Queue()

    print(f"[Launcher] All bots running. Type 'help' for commands or Ctrl+C to stop everything.\n")
    print(_HELP + "\n")

    def _listen():
        while not stop_all.is_set():
            try:
                line = input()
            except EOFError:
                break
            cmd_q.put(line.strip())

    threading.Thread(target=_listen, daemon=True).start()

    def _resolve(cmd, target):
        """Validates and resolves `target` ('all' or one platform name) for
        `cmd`, mirroring each command's own scope: a currently-running bot for
        stop/restart, any launched platform for chameleon/extractor/fix, or any
        React platform at all (launched or not — checkins just connects over
        CDP directly) for checkins. Returns (valid_names, options_list) —
        options_list is what to show in an 'unknown platform' message."""
        if cmd in ("stop", "restart"):
            targets = PLATFORMS if target == "all" else [target]
            return [n for n in targets if n in procs], PLATFORMS
        if cmd in ("chameleon", "extractor", "fix"):
            targets = PLATFORMS if target == "all" else [target]
            return [n for n in targets if n in PLATFORMS], PLATFORMS
        if cmd in ("checkins", "checkin", "ins"):
            targets = ALL_PLATFORMS if target == "all" else [target]
            return [n for n in targets if n in ALL_PLATFORMS], ALL_PLATFORMS
        return [], []

    def _cmd_status():
        print(f"{_BOLD}[Status]{_RESET}")
        for name in PLATFORMS:
            if name in stopped:
                st = "STOPPED (clean exit — needs manual restart)"
            elif procs[name].poll() is None:
                uptime = int(time.time() - start_times[name])
                st = f"RUNNING   uptime={uptime}s   crashes={crash_counts[name]}"
            else:
                st = f"DEAD (exit code {procs[name].poll()})"
            color = _COLORS.get(name, "")
            print(f"  {color}{name.upper()}{_RESET:<20s}  {st}")

    def _cmd_stop(valid):
        for name in valid:
            if procs[name].poll() is None:
                procs[name].terminate()
                print(f"[Launcher] {name.upper()} stopped.", flush=True)
            stopped.add(name)
        if set(PLATFORMS).issubset(stopped):
            stop_all.set()

    def _cmd_restart(valid):
        for name in valid:
            if procs[name].poll() is None:
                procs[name].terminate()
                time.sleep(1)
            stopped.discard(name)
            crash_counts[name] = 0
            procs[name]        = _launch_bot(name)
            start_times[name]  = time.time()
            print(f"[Launcher] {name.upper()} restarted.", flush=True)

    def _cmd_chameleon(valid):
        async def _check_all(names):
            async with async_playwright() as p:
                for name in names:
                    cfg = import_module(f"configs.{name}").config
                    try:
                        browser = await p.chromium.connect_over_cdp(cfg.cdp_url)
                        context = browser.contexts[0]
                        tab = next(
                            (pg for pg in context.pages if cfg.tab2_pattern in pg.url),
                            None,
                        )
                        if tab is None:
                            print(f"[{cfg.platform}]  Chameleon tab not found.", flush=True)
                        else:
                            print(f"[{cfg.platform}] Checking Chameleon...", flush=True)
                            await check_chameleon(tab, cfg.platform)
                    except Exception as exc:
                        print(f"[{name.upper()}] Could not connect: {exc}", flush=True)
        asyncio.run(_check_all(valid))

    def _cmd_extractor(valid):
        async def _force_extractor(names):
            async with async_playwright() as p:
                for name in names:
                    cfg = import_module(f"configs.{name}").config
                    try:
                        browser = await p.chromium.connect_over_cdp(cfg.cdp_url)
                        context = browser.contexts[0]
                        tab = next(
                            (pg for pg in context.pages if cfg.tab2_pattern in pg.url),
                            None,
                        )
                        if tab is None:
                            print(f"[{cfg.platform}]  Chameleon tab not found.", flush=True)
                        else:
                            await force_extractor_tab(tab, cfg.platform)
                    except Exception as exc:
                        print(f"[{name.upper()}] Could not connect: {exc}", flush=True)
        asyncio.run(_force_extractor(valid))

    def _cmd_fix(valid):
        async def _fix_chameleon(names):
            async with async_playwright() as p:
                for name in names:
                    cfg = import_module(f"configs.{name}").config
                    try:
                        browser = await p.chromium.connect_over_cdp(cfg.cdp_url)
                        context = browser.contexts[0]
                        tab = next(
                            (pg for pg in context.pages if cfg.tab2_pattern in pg.url),
                            None,
                        )
                        if tab is None:
                            print(f"[{cfg.platform}] Chameleon tab not found — opening new tab.", flush=True)
                            tab = await context.new_page()
                        else:
                            print(f"[{cfg.platform}] Chameleon tab found: {tab.url[:80]}", flush=True)
                        print(f"[{cfg.platform}] Running full Chameleon setup...", flush=True)
                        await login_chameleon(
                            tab,
                            cfg.chameleon_email,
                            cfg.chameleon_password,
                            cfg.platform,
                            cfg.chameleon_chat,
                        )
                        print(f"[{cfg.platform}] Fix done. URL: {tab.url}", flush=True)
                    except Exception as exc:
                        print(f"[{name.upper()}] Fix failed: {exc}", flush=True)
        asyncio.run(_fix_chameleon(valid))

    def _cmd_checkins(valid):
        async def _check_ins(names):
            from core.checkinall import IN_VALUE, ASA_OUT_VALUE
            grand_ins = grand_asa_outs = 0
            grand_money = 0.0
            rows = []
            async with async_playwright() as p:
                for name in names:
                    cfg   = import_module(f"configs.{name}").config
                    label = cfg.platform
                    try:
                        browser = await p.chromium.connect_over_cdp(cfg.cdp_url)
                        context = browser.contexts[0]
                        tab = next(
                            (pg for pg in context.pages if cfg.tab1_pattern in pg.url),
                            None,
                        )
                        if tab is None:
                            print(f"[{label}] Mod-site tab not found.", flush=True)
                            rows.append((label, 0, 0, 0.0, False))
                            continue
                        stats = await check_stats(tab, cfg.platform)
                        if not stats:
                            rows.append((label, 0, 0, 0.0, False))
                            continue
                        ins      = stats.get("Ins", 0)
                        asa_outs = stats.get("ASA Outs", 0)
                        money    = ins * IN_VALUE + asa_outs * ASA_OUT_VALUE
                        grand_ins      += ins
                        grand_asa_outs += asa_outs
                        grand_money    += money
                        rows.append((label, ins, asa_outs, money, True))
                        detail = "  ".join(f"{k}={v}" for k, v in stats.items())
                        print(f"[{label}] {detail}", flush=True)
                        print(
                            f"[{label}] Ins ({ins}) x {IN_VALUE:.2f} DT + "
                            f"ASA Outs ({asa_outs}) x {ASA_OUT_VALUE:.2f} DT = {money:,.2f} DT",
                            flush=True,
                        )
                    except Exception as exc:
                        print(f"[{label}] Could not read stats: {exc}", flush=True)
                        rows.append((label, 0, 0, 0.0, False))
            if rows:
                read = sum(1 for r in rows if r[4])
                print(f"\n{_BOLD}[checkins] Summary{_RESET}", flush=True)
                for plat, ins, asa, money, ok in rows:
                    if ok:
                        print(f"  {plat:<10s} Ins={ins:<6d} ASA Outs={asa:<6d} = {money:,.2f} DT", flush=True)
                    else:
                        print(f"  {plat:<10s} (stats unavailable — bot not running / tab not found)", flush=True)
                print(
                    f"  {_BOLD}TOTAL across {read} of {len(rows)} account(s): "
                    f"Ins={grand_ins} + ASA Outs={grand_asa_outs} "
                    f"= {grand_money:,.2f} DT{_RESET}",
                    flush=True,
                )
        asyncio.run(_check_ins(valid))

    def _cmd_checkinall():
        from core.checkinall import gather_and_write
        text, path = asyncio.run(gather_and_write([p for p in ALL_PLATFORMS if p != "gold3"]))
        print(text, flush=True)
        print(f"[checkinall] Note saved to {path}", flush=True)

    # Shared by the terminal parser and the HTTP control API below so a typed
    # command and a dashboard button click run through the exact same code.
    _PER_TARGET_CMDS = {
        "stop": _cmd_stop, "restart": _cmd_restart, "chameleon": _cmd_chameleon,
        "extractor": _cmd_extractor, "fix": _cmd_fix,
        "checkins": _cmd_checkins, "checkin": _cmd_checkins, "ins": _cmd_checkins,
    }

    def _handle_commands():
        while not cmd_q.empty():
            raw    = cmd_q.get_nowait()
            parts  = raw.lower().split()
            if not parts:
                continue
            cmd    = parts[0]
            target = parts[1] if len(parts) > 1 else "all"
            target = _ALIASES.get(target, target)

            if cmd in ("help", "?"):
                print(_HELP)

            elif cmd == "status":
                _execute_captured(_cmd_status)

            elif cmd in _PER_TARGET_CMDS:
                valid, options = _resolve(cmd, target)
                if not valid:
                    print(f"[Launcher] Unknown platform '{target}'. Options: {', '.join(options)}")
                    continue
                _execute_captured(_PER_TARGET_CMDS[cmd], valid)

            elif cmd in ("checkinall", "checkinsall", "ca"):
                _execute_captured(_cmd_checkinall)

            elif cmd in ("quit", "exit"):
                print(f"[Launcher] Quit received — stopping all bots...")
                stop_all.set()

            else:
                print(f"[Launcher] Unknown command '{cmd}'. Type 'help' for options.")

    # ── Control API — lets the approval dashboard drive these same commands as
    # buttons instead of typing them here. Local-only (127.0.0.1); every
    # command runs through _execute_captured so it's serialized with anything
    # typed in this terminal and the response carries exactly what would have
    # printed here.
    control_app = Flask(__name__)
    logging.getLogger("werkzeug").setLevel(logging.ERROR)  # keep Flask's request logging out of this console

    @control_app.get("/control/health")
    def _control_health():
        return jsonify({"ok": True})

    @control_app.get("/control/status")
    def _control_status():
        with _cmd_lock:
            platforms = {}
            for name in PLATFORMS:
                if name in stopped:
                    platforms[name] = {"state": "stopped"}
                elif procs[name].poll() is None:
                    platforms[name] = {
                        "state": "running",
                        "uptime": int(time.time() - start_times[name]),
                        "crashes": crash_counts[name],
                    }
                else:
                    platforms[name] = {"state": "dead", "exit_code": procs[name].poll()}
        return jsonify({"ok": True, "platforms": platforms, "all_platforms": ALL_PLATFORMS})

    @control_app.post("/control/command")
    def _control_command():
        body   = request.get_json(force=True, silent=True) or {}
        cmd    = (body.get("cmd") or "").strip().lower()
        target = (body.get("target") or "all").strip().lower()
        target = _ALIASES.get(target, target)

        if cmd == "status":
            return jsonify({"ok": True, "output": _execute_captured(_cmd_status)})
        if cmd in ("checkinall", "checkinsall", "ca"):
            return jsonify({"ok": True, "output": _execute_captured(_cmd_checkinall)})
        if cmd in _PER_TARGET_CMDS:
            valid, options = _resolve(cmd, target)
            if not valid:
                return jsonify({"ok": False, "error": f"Unknown platform '{target}'. Options: {', '.join(options)}"}), 400
            return jsonify({"ok": True, "output": _execute_captured(_PER_TARGET_CMDS[cmd], valid)})
        return jsonify({"ok": False, "error": f"Unknown command '{cmd}'."}), 400

    threading.Thread(
        target=lambda: control_app.run(host="127.0.0.1", port=CONTROL_SERVER_PORT, threaded=True, use_reloader=False),
        daemon=True,
    ).start()
    print(f"[Launcher] Control API for the dashboard's buttons: http://127.0.0.1:{CONTROL_SERVER_PORT}\n")

    def _interruptible_sleep(seconds: float):
        deadline = time.time() + seconds
        while time.time() < deadline and not stop_all.is_set():
            time.sleep(1)
            _handle_commands()

    try:
        while not stop_all.is_set():
            _interruptible_sleep(5)
            if stop_all.is_set():
                break

            for name in list(procs):
                if name in stopped:
                    continue
                proc = procs[name]
                ret  = proc.poll()
                if ret is None:
                    continue

                uptime = time.time() - start_times[name]

                if ret == 0:
                    print(
                        f"[Launcher] {name.upper()} exited cleanly "
                        f"(code 0, uptime {uptime:.0f}s). NOT restarting — manual action needed.",
                        flush=True,
                    )
                    stopped.add(name)
                    continue

                if uptime > CRASH_WINDOW:
                    crash_counts[name] = 0

                crash_counts[name] += 1
                count = crash_counts[name]
                delay = min(RESTART_BASE_DELAY * (2 ** (count - 1)), RESTART_MAX_DELAY)

                print(
                    f"[Launcher] {name.upper()} crashed "
                    f"(code {ret}, crash #{count}, uptime {uptime:.0f}s). "
                    f"Restarting in {delay:.0f}s...",
                    flush=True,
                )
                _interruptible_sleep(delay)
                if stop_all.is_set():
                    break
                procs[name]       = _launch_bot(name)
                start_times[name] = time.time()
                print(f"[Launcher] {name.upper()} restarted.", flush=True)

    except KeyboardInterrupt:
        print(f"\n{_BOLD}[Launcher] Ctrl+C — stopping all bots...{_RESET}")
    finally:
        stop_all.set()
        for proc in procs.values():
            proc.terminate()
        time.sleep(2)
        for proc in procs.values():
            if proc.poll() is None:
                proc.kill()
        print("[Launcher] All bots stopped.")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    label = "all platforms" if PLATFORMS == DEFAULT_PLATFORMS else f"selected: {', '.join(PLATFORMS)}"
    print(f"{_BOLD}[Launcher] ===== Full System Launch — {label} ====={_RESET}\n")

    print(f"{_BOLD}[Phase 0] Approval dashboard{_RESET}")
    approval_proc = ensure_approval_server()

    print(f"\n{_BOLD}[Phase 1] Chrome instances{_RESET}")
    chrome_procs = start_chrome_instances()

    print(f"\n{_BOLD}[Phase 1] Waiting for Chrome to be ready...{_RESET}")
    wait_all_cdp()

    print(f"\n{_BOLD}[Phase 2] Browser sessions (login if needed){_RESET}")
    asyncio.run(setup_all_browsers())

    print(f"\n{_BOLD}[Phase 3] Starting bots{_RESET}\n")
    try:
        run_bots()
    finally:
        for proc in chrome_procs.values():
            if proc.poll() is None:
                proc.terminate()
        # Only stop the approval dashboard if this launcher started it itself.
        if approval_proc is not None and approval_proc.poll() is None:
            approval_proc.terminate()


if __name__ == "__main__":
    main()
