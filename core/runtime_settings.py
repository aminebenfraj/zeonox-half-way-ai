"""Small, process-safe runtime settings shared by the dashboard and bots.

The dashboard and bot workers are separate processes, so these operator choices
live in one local JSON file instead of process memory. Reads are intentionally
cheap and defensive: a missing/corrupt file always falls back to the current
production behaviour.
"""

from __future__ import annotations

import json
import os
import random
import threading
from pathlib import Path


SETTINGS_PATH = Path(__file__).resolve().parent.parent / ".runtime_settings.json"

DEFAULT_SETTINGS = {
    # Current behaviour: 10/10 auto-sends; anything else waits for a person.
    "auto_mode_policy": "judge_manual_fallback",
    # Current behaviour: manual approval cards receive a Judge score.
    "manual_judge_policy": "enabled",
    # Current behaviour on Justlo/Linduu/Gnoxx.
    "fc_contact_policy": "skip",
    # Current behaviour: launcher consoles and Chrome windows are visible.
    "runtime_visibility": "visible",
    # Current behaviour: wait a human-like random 15–20 seconds after paste.
    "send_delay_mode": "random",
    "send_delay_seconds": 15,
}

SETTING_OPTIONS = {
    "auto_mode_policy": {
        "judge_manual_fallback",
        "judge_regenerate",
        "direct",
    },
    "manual_judge_policy": {"enabled", "disabled"},
    "fc_contact_policy": {"skip", "answer"},
    "runtime_visibility": {"visible", "background"},
    "send_delay_mode": {"random", "fixed"},
}

_write_lock = threading.Lock()


def _validated(raw: object) -> dict[str, object]:
    result = dict(DEFAULT_SETTINGS)
    if not isinstance(raw, dict):
        return result
    for key, allowed in SETTING_OPTIONS.items():
        value = raw.get(key)
        if value in allowed:
            result[key] = value
    fixed_seconds = raw.get("send_delay_seconds")
    if not isinstance(fixed_seconds, bool):
        try:
            fixed_seconds = int(fixed_seconds)
        except (TypeError, ValueError):
            fixed_seconds = None
        if fixed_seconds is not None and 0 <= fixed_seconds <= 300:
            result["send_delay_seconds"] = fixed_seconds
    return result


def get_runtime_settings() -> dict[str, object]:
    try:
        return _validated(json.loads(SETTINGS_PATH.read_text(encoding="utf-8")))
    except (OSError, ValueError, TypeError):
        return dict(DEFAULT_SETTINGS)


def update_runtime_settings(changes: dict) -> dict[str, object]:
    unknown = set(changes) - (set(SETTING_OPTIONS) | {"send_delay_seconds"})
    if unknown:
        raise ValueError(f"Unknown setting: {sorted(unknown)[0]}")
    for key, value in changes.items():
        if key == "send_delay_seconds":
            try:
                value = int(value)
            except (TypeError, ValueError) as error:
                raise ValueError("Send delay must be a whole number of seconds") from error
            if isinstance(changes[key], bool) or not 0 <= value <= 300:
                raise ValueError("Send delay must be between 0 and 300 seconds")
            changes[key] = value
        elif value not in SETTING_OPTIONS[key]:
            raise ValueError(f"Invalid value for {key}")

    with _write_lock:
        current = get_runtime_settings()
        current.update(changes)
        temporary = SETTINGS_PATH.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(current, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, SETTINGS_PATH)
    return current


def get_send_delay_seconds() -> int:
    """Resolve the live send delay; bot processes do not need a restart."""
    settings = get_runtime_settings()
    if settings["send_delay_mode"] == "fixed":
        return int(settings["send_delay_seconds"])
    return random.randint(15, 20)
