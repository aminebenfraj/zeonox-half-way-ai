#!/usr/bin/env python3
"""
Launch all platform bots simultaneously.

Commands (type while running):
  status                — show all bot statuses
  stop  [platform|all]  — stop one or all bots
  restart [platform|all]— restart one or all bots
  pause  [platform|all] — pause one or all bots (finishes current send, then waits)
  resume [platform|all] — resume one or all paused bots, right where they left off
  help                  — show this list
  quit / exit           — stop everything and exit

Restart policy:
  exit code 0 = clean/setup exit → do NOT restart (needs manual intervention)
  exit code 1 = runtime crash    → restart with exponential backoff

Usage:
    python start_all.py                        # launch all platforms
    python start_all.py gold plat              # launch only gold + platin
    python start_all.py diamond s69 gold2      # launch only those three
"""

import asyncio
import subprocess
import sys
import io
import time
import threading
import queue
from importlib import import_module

# Force UTF-8 stdout so bot output with non-ASCII chars (German etc.) prints correctly
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from playwright.async_api import async_playwright
from core.login import force_extractor_tab, check_stats
from core.bot import pause_flag_path
from core.launcher import ensure_approval_server

ALL_PLATFORMS = ["gold", "gold2", "gold3", "diamond", "platin", "s69", "ml"]

_ALIASES = {"plat": "platin", "g2": "gold2", "g3": "gold3"}

def _resolve_platforms(args: list[str]) -> list[str]:
    resolved = []
    for a in args:
        name = _ALIASES.get(a.lower(), a.lower())
        if name not in ALL_PLATFORMS:
            print(f"[Launcher] Unknown platform '{a}'. Options: {', '.join(ALL_PLATFORMS)}")
            sys.exit(1)
        if name not in resolved:
            resolved.append(name)
    return resolved or ALL_PLATFORMS

PLATFORMS = _resolve_platforms(sys.argv[1:])

RESTART_BASE_DELAY = 10
RESTART_MAX_DELAY  = 300
CRASH_WINDOW       = 300

_COLORS = {
    "gold":    "\033[33m",
    "gold2":   "\033[93m",
    "gold3":   "\033[95m",
    "diamond": "\033[36m",
    "platin":  "\033[37m",
    "s69":     "\033[35m",
    "ml":      "\033[32m",
}

_RESET = "\033[0m"
_BOLD  = "\033[1m"

_HELP = (
    f"{_BOLD}Available commands:{_RESET}\n"
    "  status                    — show all bot statuses\n"
    "  stop  [platform|all]      — stop one or all bots\n"
    "  restart [platform|all]    — restart one or all bots\n"
    "  pause  [platform|all]     — pause one or all bots (finishes current send, then waits)\n"
    "  resume [platform|all]     — resume one or all paused bots, right where they left off\n"
    "  extractor [platform|all]  — force-activate the Chat Extractor tab\n"
    "  checkins [platform|all]   — open 'Meine Statistiken' and report money made (Ins + ASA Outs)\n"
    "  checkinall                — save/update a Desktop note (checkinall.txt) with money made across all accounts\n"
    "  help                      — show this list\n"
    "  quit / exit               — stop everything and exit\n"
    f"  Active platforms: {', '.join(PLATFORMS)}  (aliases: plat=platin, g2=gold2)\n"
    f"  Launch specific: python start_all.py gold plat s69"
)


def _color(platform: str, text: str) -> str:
    c = _COLORS.get(platform.lower(), "")
    return f"{c}{text}{_RESET}" if c else text


def _stream_output(platform: str, proc: subprocess.Popen):
    color = _COLORS.get(platform.lower(), "")
    for line in proc.stdout:
        print(f"{color}{line}{_RESET}", end="", flush=True)


def _launch(platform: str) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-u", "run_bot.py", platform],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    threading.Thread(target=_stream_output, args=(platform, proc), daemon=True).start()
    return proc


def main():
    label = "all platforms" if len(PLATFORMS) == len(ALL_PLATFORMS) else f"selected: {', '.join(PLATFORMS)}"
    print(f"{_BOLD}[Launcher] Starting {len(PLATFORMS)} bot(s) — {label}{_RESET}\n")

    approval_proc = ensure_approval_server()
    print()
    print(_HELP + "\n")

    procs:        dict[str, subprocess.Popen] = {}
    start_times:  dict[str, float]            = {}
    crash_counts: dict[str, int]              = {}
    stopped:      set[str]                    = set()
    stop_all      = threading.Event()
    cmd_q         = queue.Queue()

    # Clear any pause flags left over from a previous session so a fresh launch
    # never starts up silently paused.
    for p in PLATFORMS:
        pause_flag_path(p).unlink(missing_ok=True)

    for p in PLATFORMS:
        procs[p]        = _launch(p)
        start_times[p]  = time.time()
        crash_counts[p] = 0

    print(f"[Launcher] All bots started. Type 'help' for commands or Ctrl+C to stop all.\n")

    def _listen():
        while not stop_all.is_set():
            try:
                line = input()
            except EOFError:
                break
            cmd_q.put(line.strip())

    threading.Thread(target=_listen, daemon=True).start()

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
                print(f"{_BOLD}[Status]{_RESET}")
                for name in PLATFORMS:
                    if name in stopped:
                        st = "STOPPED (clean exit — needs manual restart)"
                    elif procs[name].poll() is None:
                        uptime = int(time.time() - start_times[name])
                        st = f"RUNNING   uptime={uptime}s   crashes={crash_counts[name]}"
                        if pause_flag_path(name).exists():
                            st += "   [PAUSED]"
                    else:
                        st = f"DEAD (exit code {procs[name].poll()})"
                    print(f"  {_color(name, name.upper()):<20s}  {st}")

            elif cmd == "stop":
                targets = PLATFORMS if target == "all" else [target]
                valid   = [n for n in targets if n in procs]
                if not valid:
                    print(f"[Launcher] Unknown platform '{target}'. Options: {', '.join(PLATFORMS)}")
                    continue
                for name in valid:
                    if procs[name].poll() is None:
                        procs[name].terminate()
                        print(f"[Launcher] {_color(name, name.upper())} stopped.", flush=True)
                    stopped.add(name)
                if set(PLATFORMS).issubset(stopped):
                    stop_all.set()

            elif cmd == "restart":
                targets = PLATFORMS if target == "all" else [target]
                valid   = [n for n in targets if n in procs]
                if not valid:
                    print(f"[Launcher] Unknown platform '{target}'. Options: {', '.join(PLATFORMS)}")
                    continue
                for name in valid:
                    if procs[name].poll() is None:
                        procs[name].terminate()
                        time.sleep(1)
                    stopped.discard(name)
                    crash_counts[name] = 0
                    procs[name]        = _launch(name)
                    start_times[name]  = time.time()
                    print(f"[Launcher] {_color(name, name.upper())} restarted.", flush=True)

            elif cmd == "pause":
                targets = PLATFORMS if target == "all" else [target]
                valid   = [n for n in targets if n in PLATFORMS]
                if not valid:
                    print(f"[Launcher] Unknown platform '{target}'. Options: {', '.join(PLATFORMS)}")
                    continue
                for name in valid:
                    pause_flag_path(name).touch()
                    print(f"[Launcher] {_color(name, name.upper())} paused.", flush=True)

            elif cmd == "resume":
                targets = PLATFORMS if target == "all" else [target]
                valid   = [n for n in targets if n in PLATFORMS]
                if not valid:
                    print(f"[Launcher] Unknown platform '{target}'. Options: {', '.join(PLATFORMS)}")
                    continue
                for name in valid:
                    pause_flag_path(name).unlink(missing_ok=True)
                    print(f"[Launcher] {_color(name, name.upper())} resumed.", flush=True)

            elif cmd == "extractor":
                targets = PLATFORMS if target == "all" else [target]
                valid   = [n for n in targets if n in PLATFORMS]
                if not valid:
                    print(f"[Launcher] Unknown platform '{target}'. Options: {', '.join(PLATFORMS)}")
                    continue
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

            elif cmd in ("checkins", "checkin", "ins"):
                targets = ALL_PLATFORMS if target == "all" else [target]
                valid   = [n for n in targets if n in ALL_PLATFORMS]
                if not valid:
                    print(f"[Launcher] Unknown platform '{target}'. Options: {', '.join(ALL_PLATFORMS)}")
                    continue
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
                                    f"[{label}] Ins ({ins}) × ${IN_VALUE:.2f} + "
                                    f"ASA Outs ({asa_outs}) × ${ASA_OUT_VALUE:.2f} = ${money:,.2f}",
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
                                print(f"  {plat:<10s} Ins={ins:<6d} ASA Outs={asa:<6d} = ${money:,.2f}", flush=True)
                            else:
                                print(f"  {plat:<10s} (stats unavailable — bot not running / tab not found)", flush=True)
                        print(
                            f"  {_BOLD}TOTAL across {read} of {len(rows)} account(s): "
                            f"Ins={grand_ins} + ASA Outs={grand_asa_outs} "
                            f"= ${grand_money:,.2f}{_RESET}",
                            flush=True,
                        )
                asyncio.run(_check_ins(valid))

            elif cmd in ("checkinall", "checkinsall", "ca"):
                from core.checkinall import gather_and_write
                text, path = asyncio.run(gather_and_write(ALL_PLATFORMS))
                print(text, flush=True)
                print(f"[checkinall] Note saved to {path}", flush=True)

            elif cmd in ("quit", "exit"):
                print(f"[Launcher] Quit received — stopping all bots...")
                stop_all.set()

            else:
                print(f"[Launcher] Unknown command '{cmd}'. Type 'help' for options.")

    def _interruptible_sleep(seconds: float):
        """Sleep in 1-second chunks so commands stay responsive."""
        deadline = time.time() + seconds
        while time.time() < deadline and not stop_all.is_set():
            time.sleep(1)
            _handle_commands()

    try:
        while not stop_all.is_set():
            _interruptible_sleep(5)
            if stop_all.is_set():
                break

            for platform in list(procs):
                if platform in stopped:
                    continue
                proc = procs[platform]
                ret  = proc.poll()
                if ret is None:
                    continue

                uptime = time.time() - start_times[platform]

                if ret == 0:
                    print(
                        f"[Launcher] {_color(platform, platform.upper())} exited cleanly "
                        f"(code 0, uptime {uptime:.0f}s). NOT restarting — manual action needed.",
                        flush=True,
                    )
                    stopped.add(platform)
                    continue

                if uptime > CRASH_WINDOW:
                    crash_counts[platform] = 0

                crash_counts[platform] += 1
                count = crash_counts[platform]
                delay = min(RESTART_BASE_DELAY * (2 ** (count - 1)), RESTART_MAX_DELAY)

                print(
                    f"[Launcher] {_color(platform, platform.upper())} crashed "
                    f"(code {ret}, crash #{count}, uptime {uptime:.0f}s). "
                    f"Restarting in {delay:.0f}s...",
                    flush=True,
                )
                _interruptible_sleep(delay)
                if stop_all.is_set():
                    break
                procs[platform]       = _launch(platform)
                start_times[platform] = time.time()
                print(f"[Launcher] {_color(platform, platform.upper())} restarted.", flush=True)

    except KeyboardInterrupt:
        print(f"\n{_BOLD}[Launcher] Ctrl+C received — stopping all bots...{_RESET}")
    finally:
        stop_all.set()
        for proc in procs.values():
            proc.terminate()
        time.sleep(2)
        for proc in procs.values():
            if proc.poll() is None:
                proc.kill()
        # Only stop the approval dashboard if this launcher started it itself.
        if approval_proc is not None and approval_proc.poll() is None:
            approval_proc.terminate()
        print("[Launcher] All bots stopped.")


if __name__ == "__main__":
    main()
