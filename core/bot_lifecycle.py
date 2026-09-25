"""Small client for the local bot supervisor used by dashboard routes."""

from pathlib import Path
import subprocess
import sys
import threading

import httpx

from core.process_visibility import process_window_kwargs


class LauncherClient:
    """Send lifecycle commands and bootstrap the supervisor when necessary."""

    def __init__(self, base_url: str, project_dir: Path):
        self.base_url = base_url.rstrip("/")
        self.project_dir = project_dir
        self._process: subprocess.Popen | None = None
        self._initial_platform: str | None = None
        self._start_lock = threading.Lock()

    def command(self, command: str, platform: str) -> tuple[dict, int]:
        try:
            response = httpx.post(
                f"{self.base_url}/control/command",
                json={"cmd": command, "target": platform},
                timeout=120,
            )
            return response.json(), response.status_code
        except Exception as error:
            return {
                "ok": False,
                "error": f"Launcher control server unreachable: {error}",
            }, 502

    def spawn(self, platform: str) -> subprocess.Popen:
        """Start one central supervisor from a standalone dashboard session."""
        with self._start_lock:
            if self._process is not None and self._process.poll() is None:
                if self._initial_platform != platform:
                    raise RuntimeError(
                        "The launcher is still starting. Wait until its controls connect, then try again."
                    )
                return self._process
            popen_kwargs = process_window_kwargs(visible_new_console=True)
            self._process = subprocess.Popen(
                [
                    sys.executable,
                    "-u",
                    str(self.project_dir / "launch_all.py"),
                    platform,
                ],
                cwd=str(self.project_dir),
                **popen_kwargs,
            )
            self._initial_platform = platform
            return self._process
