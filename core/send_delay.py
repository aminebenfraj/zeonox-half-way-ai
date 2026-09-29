"""Resolve the live message send delay without relying on module reloads.

The launcher is long-lived and may already have imported an older
``core.runtime_settings`` module when bot modules are loaded on demand.  Read
the shared JSON file directly here so newly started bots work without forcing
the launcher itself to restart, and so timer changes remain live.
"""

from __future__ import annotations

import json
import random
from pathlib import Path


SETTINGS_PATH = Path(__file__).resolve().parent.parent / ".runtime_settings.json"


def _whole_seconds(value: object, default: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        value = int(value)
    except (TypeError, ValueError):
        return default
    return value if 0 <= value <= 300 else default


def get_send_delay_seconds() -> int:
    try:
        raw = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        raw = {}
    if not isinstance(raw, dict):
        raw = {}

    if raw.get("send_delay_mode") == "fixed":
        return _whole_seconds(raw.get("send_delay_seconds"), 15)

    minimum = _whole_seconds(raw.get("send_delay_random_min"), 15)
    maximum = _whole_seconds(raw.get("send_delay_random_max"), 20)
    if minimum > maximum:
        minimum, maximum = 15, 20
    return random.randint(minimum, maximum)
