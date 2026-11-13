"""Minimal HTTP adapter for OpenAI-compatible text generation."""

import httpx


def openai_chat_completion(
    *,
    url: str,
    api_key: str,
    model: str,
    messages: list[dict[str, str]],
    max_tokens: int,
    timeout: float = 20.0,
    extra_headers: dict[str, str] | None = None,
    response_format: dict[str, str] | None = None,
) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        **(extra_headers or {}),
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0,
        # Mistral requires top_p=1 when temperature=0 (greedy sampling).
        # Omitting it currently returns HTTP 400 even when the API key is valid.
        "top_p": 1,
        "max_tokens": max_tokens,
        "stream": False,
    }
    if response_format:
        payload["response_format"] = response_format
    response = httpx.post(
        url,
        headers=headers,
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()
    content = response.json()["choices"][0]["message"]["content"]
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        # Newer Mistral responses can use OpenAI-style structured content
        # blocks: [{"type": "text", "text": "..."}].
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                text = block.get("text") or block.get("content")
                if isinstance(text, str):
                    parts.append(text)
        return "".join(parts).strip()
    return str(content or "").strip()
