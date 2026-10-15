"""Persistent reusable instructions for Chameleon reply regeneration."""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
STORE_PATH = BASE_DIR / ".instruction_prompts.json"
MAX_NAME_CHARS = 80
MAX_PROMPT_CHARS = 4000
_STORE_LOCK = threading.RLock()


def _read_unlocked() -> list[dict]:
    if not STORE_PATH.exists():
        return []
    try:
        payload = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [dict(item) for item in payload if isinstance(item, dict)]


def _write_unlocked(prompts: list[dict]) -> None:
    temporary = STORE_PATH.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(prompts, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, STORE_PATH)


def _validate(name: object, prompt: object) -> tuple[str, str]:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Prompt name is required")
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError("Instruction prompt is required")
    name = name.strip()
    prompt = prompt.strip()
    if len(name) > MAX_NAME_CHARS:
        raise ValueError(f"Prompt name must be {MAX_NAME_CHARS} characters or fewer")
    if len(prompt) > MAX_PROMPT_CHARS:
        raise ValueError(f"Instruction prompt must be {MAX_PROMPT_CHARS} characters or fewer")
    return name, prompt


def list_instruction_prompts() -> list[dict]:
    with _STORE_LOCK:
        return _read_unlocked()


def save_instruction_prompt(name: object, prompt: object, prompt_id: object = None) -> dict:
    name, prompt = _validate(name, prompt)
    normalized_id = str(prompt_id or "").strip().lower()
    now = datetime.now(timezone.utc).isoformat()
    with _STORE_LOCK:
        prompts = _read_unlocked()
        if normalized_id:
            index = next(
                (i for i, item in enumerate(prompts) if item.get("id") == normalized_id),
                None,
            )
            if index is None:
                raise KeyError(normalized_id)
            prompts[index].update({"name": name, "prompt": prompt, "updated_at": now})
            saved = dict(prompts[index])
        else:
            saved = {
                "id": uuid.uuid4().hex[:12],
                "name": name,
                "prompt": prompt,
                "created_at": now,
                "updated_at": now,
            }
            prompts.append(saved)
        _write_unlocked(prompts)
        return dict(saved)


def delete_instruction_prompt(prompt_id: object) -> dict:
    normalized_id = str(prompt_id or "").strip().lower()
    with _STORE_LOCK:
        prompts = _read_unlocked()
        match = next((item for item in prompts if item.get("id") == normalized_id), None)
        if match is None:
            raise KeyError(normalized_id)
        _write_unlocked([item for item in prompts if item.get("id") != normalized_id])
        return dict(match)
