"""Windows process/window policy shared by every Zenox launch path."""

from __future__ import annotations

import subprocess
import sys

from core.runtime_settings import get_runtime_settings


def background_runtime_enabled() -> bool:
    return get_runtime_settings()["runtime_visibility"] == "background"


def process_window_kwargs(*, visible_new_console: bool = False) -> dict:
    """Return safe Popen flags for the selected runtime visibility."""
    if sys.platform != "win32":
        return {}
    if not background_runtime_enabled():
        return {
            "creationflags": subprocess.CREATE_NEW_CONSOLE,
        } if visible_new_console else {}

    startup = subprocess.STARTUPINFO()
    startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startup.wShowWindow = subprocess.SW_HIDE
    return {
        "creationflags": subprocess.CREATE_NO_WINDOW,
        "startupinfo": startup,
    }


def apply_current_console_visibility() -> bool:
    """Hide/show this process' console and report whether one was available."""
    if sys.platform != "win32":
        return False
    try:
        import ctypes

        window = ctypes.windll.kernel32.GetConsoleWindow()
        if window:
            ctypes.windll.user32.ShowWindow(
                window,
                0 if background_runtime_enabled() else 5,  # SW_HIDE / SW_SHOW
            )
            return True
    except Exception:
        pass
    return False
