#!/usr/bin/env python3
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

APPROVAL_SERVER_PORT = int(os.environ.get("APPROVAL_SERVER_PORT", "8799"))

# launch_all.py's in-process control API (restart/stop/fix/checkinall/... as
# HTTP instead of typed terminal commands) — see run_bots() in launch_all.py.
# Local-only by design: it drives real Chrome/CDP connections on this machine,
# so it's only reachable (and only useful) from whatever is running alongside
# the bots, e.g. the dashboard's control panel when both run on the same host.
CONTROL_SERVER_PORT = int(os.environ.get("CONTROL_SERVER_PORT", "8800"))

_CHROME_CANDIDATES = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
]


def _find_chrome() -> str:
    override = os.environ.get("CHROME_EXE")
    if override:
        return override
    for path in _CHROME_CANDIDATES:
        if os.path.isfile(path):
            return path
    raise FileNotFoundError(
        "Chrome not found. Set the CHROME_EXE environment variable to its full path."
    )


def is_cdp_ready(port: int) -> bool:
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/json", timeout=1)
        return True
    except Exception:
        return False


def start_chrome(profile_dir: str, port: int) -> subprocess.Popen:
    chrome = _find_chrome()
    return subprocess.Popen(
        [
            chrome,
            f"--remote-debugging-port={port}",
            f"--user-data-dir={profile_dir}",
            # Keep computed CSS geometry identical to a normal 100%-scale
            # extension capture. Without an explicit factor, this Windows
            # setup launches bot Chrome at 0.8: getComputedStyle reports a
            # 1px border as 1.25px and viewport dimensions are inflated.
            "--force-device-scale-factor=1",
            "--no-first-run",
            "--no-default-browser-check",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def wait_for_cdp(port: int, timeout: int = 30) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_cdp_ready(port):
            return True
        time.sleep(0.5)
    return False


# ── Approval dashboard ──────────────────────────────────────────────────────
# Every bot blocks on this local server (approval_server.py) before sending a
# reply, so it has to be up before any bot starts. See core/approval.py.

def is_approval_server_ready(port: int = APPROVAL_SERVER_PORT) -> bool:
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=1)
        return True
    except Exception:
        return False


def start_approval_server(port: int = APPROVAL_SERVER_PORT) -> subprocess.Popen:
    base_dir = Path(__file__).resolve().parent.parent
    # Default to listening on all interfaces (not just 127.0.0.1) so the
    # dashboard is reachable from a phone/other device on the same network —
    # an explicit HOST already set in the environment always wins. This is the
    # approval dashboard only; the control API (CONTROL_SERVER_PORT) stays
    # hardcoded to 127.0.0.1 in launch_all.py regardless.
    env = {"HOST": "0.0.0.0", **os.environ, "APPROVAL_SERVER_PORT": str(port)}
    return subprocess.Popen(
        [sys.executable, "-u", str(base_dir / "approval_server.py")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )


def wait_for_approval_server(port: int = APPROVAL_SERVER_PORT, timeout: int = 15) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_approval_server_ready(port):
            return True
        time.sleep(0.5)
    return False


def _lan_ip() -> str | None:
    """Best-effort local network IP (e.g. 192.168.1.23), for printing a
    phone-reachable URL alongside the localhost one. Doesn't actually send
    anything -- connect() on a UDP socket just makes the OS pick the outbound
    interface/address. Returns None if there's no network route at all."""
    import socket
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return None


def ensure_approval_server(port: int = APPROVAL_SERVER_PORT):
    """Start the approval dashboard if it isn't already running.

    Returns the Popen handle if this call started it (caller is responsible for
    terminating it on shutdown), or None if it was already running, or if
    APPROVAL_SERVER_URL points somewhere other than this machine (a cloud
    dashboard), in which case there's nothing local to launch.
    """
    remote_url = os.environ.get("APPROVAL_SERVER_URL", "")
    if remote_url and "127.0.0.1" not in remote_url and "localhost" not in remote_url:
        print(f"[Launcher] APPROVAL_SERVER_URL={remote_url} -- using that instead of a local dashboard.")
        return None

    if is_approval_server_ready(port):
        print(f"[Launcher] Approval dashboard already running at http://127.0.0.1:{port}")
        return None

    print(f"[Launcher] Starting approval dashboard on port {port}...")
    proc = start_approval_server(port)
    if wait_for_approval_server(port):
        print(f"[Launcher] Approval dashboard ready: http://127.0.0.1:{port}  "
              f"(open this in a browser — replies wait here for your Approve/Reject)")
        lan_ip = _lan_ip()
        if lan_ip:
            print(f"[Launcher] Same-network devices (e.g. your phone): http://{lan_ip}:{port}")
    else:
        print("[ERROR] Approval dashboard did not come up in time. Bots will stall waiting for it.")
    return proc


# ── Control API (launch_all.py's restart/stop/fix/checkinall buttons) ──────────

def is_control_server_ready(port: int = CONTROL_SERVER_PORT) -> bool:
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/control/health", timeout=1)
        return True
    except Exception:
        return False
