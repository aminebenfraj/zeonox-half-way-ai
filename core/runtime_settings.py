"""Small, process-safe runtime settings shared by the dashboard and bots.

The dashboard and bot workers are separate processes, so these operator choices
live in one local JSON file instead of process memory. Reads are intentionally
cheap and defensive: a missing/corrupt file always falls back to the current
production behaviour.
"""

from __future__ import annotations

import json
import os
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
    "send_delay_random_min": 15,
    "send_delay_random_max": 20,
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
    for key in ("send_delay_seconds", "send_delay_random_min", "send_delay_random_max"):
        number = raw.get(key)
        if isinstance(number, bool):
            continue
        try:
            number = int(number)
        except (TypeError, ValueError):
            continue
        if 0 <= number <= 300:
            result[key] = number
    if result["send_delay_random_min"] > result["send_delay_random_max"]:
        result["send_delay_random_min"] = DEFAULT_SETTINGS["send_delay_random_min"]
        result["send_delay_random_max"] = DEFAULT_SETTINGS["send_delay_random_max"]
    return result


def get_runtime_settings() -> dict[str, object]:
    try:
        return _validated(json.loads(SETTINGS_PATH.read_text(encoding="utf-8")))
    except (OSError, ValueError, TypeError):
        return dict(DEFAULT_SETTINGS)


def update_runtime_settings(changes: dict) -> dict[str, object]:
    number_keys = {
        "send_delay_seconds",
        "send_delay_random_min",
        "send_delay_random_max",
    }
    unknown = set(changes) - (set(SETTING_OPTIONS) | number_keys)
    if unknown:
        raise ValueError(f"Unknown setting: {sorted(unknown)[0]}")
    for key, value in changes.items():
        if key in number_keys:
            try:
                value = int(value)
            except (TypeError, ValueError) as error:
                raise ValueError("Send delays must be whole numbers of seconds") from error
            if isinstance(changes[key], bool) or not 0 <= value <= 300:
                raise ValueError("Send delays must be between 0 and 300 seconds")
            changes[key] = value
        elif value not in SETTING_OPTIONS[key]:
            raise ValueError(f"Invalid value for {key}")

    with _write_lock:
        current = get_runtime_settings()
        current.update(changes)
        if current["send_delay_random_min"] > current["send_delay_random_max"]:
            raise ValueError("Random minimum delay cannot be greater than the maximum")
        temporary = SETTINGS_PATH.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(current, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, SETTINGS_PATH)
    return current


def get_send_delay_seconds() -> int:
    """Compatibility export; bot modules use ``core.send_delay`` directly."""
    from core.send_delay import get_send_delay_seconds as resolve

    return resolve()
