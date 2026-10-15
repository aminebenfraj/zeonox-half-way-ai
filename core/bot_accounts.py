"""Persistent additional bot accounts created from the dashboard.

The original eleven platform configs remain the built-in accounts.  Records in
``.bot_accounts.json`` are extra instances: each receives a dedicated Chrome
profile and CDP port so multiple logins for the same platform can run together.
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
STORE_PATH = BASE_DIR / ".bot_accounts.json"
_STORE_LOCK = threading.RLock()
_FIRST_DYNAMIC_PORT = 9300
_LAST_DYNAMIC_PORT = 65535
MAX_LABEL_CHARS = 80


def _clean_label(label: str) -> str:
    label = str(label or "").strip()
    if not label:
        raise ValueError("Bot name is required")
    if len(label) > MAX_LABEL_CHARS or any(char in label for char in "\r\n\0"):
        raise ValueError(f"Bot name must be {MAX_LABEL_CHARS} characters or fewer")
    return label


def _read_unlocked() -> list[dict]:
    if not STORE_PATH.exists():
        return []
    try:
        payload = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []


def _write_unlocked(accounts: list[dict]) -> None:
    temporary = STORE_PATH.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(accounts, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, STORE_PATH)


def list_accounts() -> list[dict]:
    with _STORE_LOCK:
        return [dict(item) for item in _read_unlocked()]


def get_account(account_id: str) -> dict | None:
    account_id = (account_id or "").strip().lower()
    with _STORE_LOCK:
        account = next(
            (item for item in _read_unlocked() if item.get("id") == account_id),
            None,
        )
        return dict(account) if account else None


def _next_port(accounts: list[dict], reserved_ports: set[int]) -> int:
    occupied = reserved_ports | {
        int(item["cdp_port"])
        for item in accounts
        if str(item.get("cdp_port", "")).isdigit()
    }
    for port in range(_FIRST_DYNAMIC_PORT, _LAST_DYNAMIC_PORT + 1):
        if port not in occupied:
            return port
    raise RuntimeError("No free Chrome debugging port is available")


def add_account(
    platform_type: str,
    platform_label: str,
    username: str,
    password: str,
    reserved_ports: set[int],
    label: str = "",
) -> dict:
    platform_type = platform_type.strip().lower()
    username = username.strip()
    requested_label = label
    with _STORE_LOCK:
        accounts = _read_unlocked()
        if any(
            item.get("platform_type") == platform_type
            and str(item.get("username", "")).casefold() == username.casefold()
            for item in accounts
        ):
            raise ValueError("That username already has a bot for this platform")

        used_numbers = {
            int(item.get("account_number", 0))
            for item in accounts
            if item.get("platform_type") == platform_type
            and str(item.get("account_number", "")).isdigit()
        }
        account_number = 2
        while account_number in used_numbers:
            account_number += 1

        label = _clean_label(requested_label or f"{platform_label} account {account_number}")
        if any(str(item.get("label", "")).casefold() == label.casefold() for item in accounts):
            raise ValueError("A bot with that name already exists")

        account = {
            "id": f"{platform_type}-{uuid.uuid4().hex[:10]}",
            "platform_type": platform_type,
            "label": label,
            "account_number": account_number,
            "username": username,
            "password": password,
            "cdp_port": _next_port(accounts, reserved_ports),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        accounts.append(account)
        _write_unlocked(accounts)
        return dict(account)


def update_account(
    account_id: str,
    username: str,
    password: str = "",
    label: str | None = None,
) -> dict:
    account_id = account_id.strip().lower()
    username = username.strip()
    with _STORE_LOCK:
        accounts = _read_unlocked()
        index = next((i for i, item in enumerate(accounts) if item.get("id") == account_id), None)
        if index is None:
            raise KeyError(account_id)
        platform_type = accounts[index].get("platform_type")
        if any(
            i != index
            and item.get("platform_type") == platform_type
            and str(item.get("username", "")).casefold() == username.casefold()
            for i, item in enumerate(accounts)
        ):
            raise ValueError("That username already has a bot for this platform")
        if label is not None:
            label = _clean_label(label)
            if any(
                i != index
                and str(item.get("label", "")).casefold() == label.casefold()
                for i, item in enumerate(accounts)
            ):
                raise ValueError("A bot with that name already exists")
            accounts[index]["label"] = label
        accounts[index]["username"] = username
        if password:
            accounts[index]["password"] = password
        _write_unlocked(accounts)
        return dict(accounts[index])


def delete_account(account_id: str) -> dict:
    account_id = account_id.strip().lower()
    with _STORE_LOCK:
        accounts = _read_unlocked()
        account = next((item for item in accounts if item.get("id") == account_id), None)
        if account is None:
            raise KeyError(account_id)
        _write_unlocked([item for item in accounts if item.get("id") != account_id])
        return dict(account)
