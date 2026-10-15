"""Persistent operator-editable prompt used by the Mistral reply Judge."""

from __future__ import annotations

import os
import threading
from pathlib import Path

from prompts import JUDGE_SYSTEM_PROMPT


PROMPT_PATH = Path(__file__).resolve().parent.parent / ".judge_prompt.txt"
MAX_PROMPT_CHARS = 50_000
_write_lock = threading.Lock()


def get_judge_prompt() -> str:
    """Return the saved custom prompt, falling back to the built-in default."""
    try:
        prompt = PROMPT_PATH.read_text(encoding="utf-8").strip()
    except OSError:
        return JUDGE_SYSTEM_PROMPT
    return prompt or JUDGE_SYSTEM_PROMPT


def judge_prompt_is_customized() -> bool:
    try:
        return bool(PROMPT_PATH.read_text(encoding="utf-8").strip())
    except OSError:
        return False


def save_judge_prompt(prompt: object) -> str:
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError("Judge prompt cannot be empty")
    prompt = prompt.strip()
    if len(prompt) > MAX_PROMPT_CHARS:
        raise ValueError(f"Judge prompt must be {MAX_PROMPT_CHARS:,} characters or fewer")
    with _write_lock:
        temporary = PROMPT_PATH.with_suffix(".tmp")
        temporary.write_text(prompt + "\n", encoding="utf-8")
        os.replace(temporary, PROMPT_PATH)
    return prompt


def reset_judge_prompt() -> str:
    with _write_lock:
        PROMPT_PATH.unlink(missing_ok=True)
    return JUDGE_SYSTEM_PROMPT
