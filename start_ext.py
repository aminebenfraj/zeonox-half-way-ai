#!/usr/bin/env python3
"""
Launch the ExtJS-console bots (justlo + linduu) together.

These two share the old ExtJS moderation console but each logs in on its own
site, so — unlike the React platforms in launch_all.py — every bot brings up its
own Chrome and signs itself in (see core/justlo_bot.py). This launcher just
spawns the run_bot.py subprocesses and keeps them alive with crash-restart.

Commands (type while running):
  status                — show bot statuses
  stop  [platform|all]  — stop one or all bots
  restart [platform|all]— restart one or all bots
  help                  — show this list
  quit / exit           — stop everything and exit

Restart policy:
  exit code 0 = clean/setup exit → do NOT restart (needs manual intervention)
  exit code 1 = runtime crash    → restart with exponential backoff

Usage:
    python start_ext.py                 # launch justlo + linduu
    python start_ext.py justlo          # launch only justlo
    python start_ext.py linduu          # launch only linduu
"""

import sys
import io
import subprocess
import time
import threading
import queue

# Force UTF-8 stdout so bot output with non-ASCII chars (German etc.) prints correctly
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ALL_PLATFORMS = ["justlo", "linduu"]


def _resolve_platforms(args):
    resolved = []
    for a in args:
        name = a.lower()
        if name not in ALL_PLATFORMS:
            print(f"[Launcher] Unknown platform '{a}'. Options: {', '.join(ALL_PLATFORMS)}")
            sys.exit(1)
        if name not in resolved:
            resolved.append(name)
    return resolved or ALL_PLATFORMS


PLATFORMS = _resolve_platforms(sys.argv[1:])

RESTART_BASE_DELAY = 10    # seconds for first restart after crash
RESTART_MAX_DELAY  = 300   # cap backoff at 5 minutes
CRASH_WINDOW       = 300   # reset crash counter if a bot ran this long without crashing

_COLORS = {
    "justlo": "\033[34m",   # blue
    "linduu": "\033[32m",   # green
}
_RESET = "\033[0m"
_BOLD  = "\033[1m"

_HELP = (
    f"{_BOLD}Available commands:{_RESET}\n"
    "  status                 — show all bot statuses\n"
    "  stop  [platform|all]   — stop one or all bots\n"
    "  restart [platform|all] — restart one or all bots\n"
    "  help                   — show this list\n"
    "  quit / exit            — stop everything and exit\n"
    f"  Active platforms: {', '.join(PLATFORMS)}"
)


def _color(platform, text):
    c = _COLORS.get(platform.lower(), "")
    return f"{c}{text}{_RESET}" if c else text


def _stream_output(platform, proc):
    color = _COLORS.get(platform.lower(), "")
    for line in proc.stdout:
        print(f"{color}{line}{_RESET}", end="", flush=True)


def _launch(platform):
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


def main():
    label = "all" if len(PLATFORMS) == len(ALL_PLATFORMS) else ", ".join(PLATFORMS)
    print(f"{_BOLD}[Launcher] Starting ExtJS bots — {label}{_RESET}\n")
    print(_HELP + "\n")

    procs        = {}
    start_times  = {}
    crash_counts = {}
    stopped      = set()
    stop_all     = threading.Event()
    cmd_q        = queue.Queue()

    for p in PLATFORMS:
        procs[p]        = _launch(p)
        start_times[p]  = time.time()
        crash_counts[p] = 0

    print("[Launcher] All bots started. Type 'help' for commands or Ctrl+C to stop all.\n")

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
            raw   = cmd_q.get_nowait()
            parts = raw.lower().split()
            if not parts:
                continue
            cmd    = parts[0]
            target = parts[1] if len(parts) > 1 else "all"

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

            elif cmd in ("quit", "exit"):
                print("[Launcher] Quit received — stopping all bots...")
                stop_all.set()

            else:
                print(f"[Launcher] Unknown command '{cmd}'. Type 'help' for options.")

    def _interruptible_sleep(seconds):
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
        print("[Launcher] All bots stopped.")


if __name__ == "__main__":
    main()
