"""Read conversation context already extracted by the real Chameleon workspace.

This is intentionally not a second/local extractor.  After a bot pastes the
platform HTML into Chameleon, the AgentWorkspace exposes its parsed payload in
the JSON tab.  We read that payload for the approval dashboard, then restore
the Chat tab before reply generation continues.
"""

import json

from playwright.async_api import Error as PlaywrightError


def _text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if item not in (None, ""))
    return str(value).strip()


def _flatten_profile(raw) -> dict:
    if not isinstance(raw, dict):
        return {}

    out = {}
    standard = (
        ("username", "Username"),
        ("gender", "Geschlecht"),
        ("looking_for_gender", "Sucht Geschlecht"),
        ("age", "Alter"),
        ("birthdate", "Geburtsdatum"),
        ("location", "Ort"),
        ("postal_code", "PLZ"),
        ("region", "Region"),
        ("relationship_status", "Beziehungsstatus"),
        ("looking_for", "Sucht"),
        ("bio", "Bio"),
        ("notes", "Notizen"),
    )
    for key, label in standard:
        value = _text(raw.get(key))
        if value:
            out[label] = value

    details = raw.get("additional_details") or raw.get("details") or {}
    if isinstance(details, dict):
        for key, value in details.items():
            value = _text(value)
            if value:
                out[str(key)] = value

    for key, label in (("dialog_info", "Dialoginformationen"), ("global_info", "Globale Info")):
        value = _text(raw.get(key))
        if value:
            out[label] = value
    return out


def _message_text(message) -> str:
    if isinstance(message, dict):
        return _text(message.get("text") or message.get("message"))
    return _text(message)


def _is_customer(message: dict) -> bool:
    sender = _text(message.get("sender")).lower()
    if sender:
        return sender in ("client", "customer", "kunde")
    return not bool(message.get("has_moderator"))


def normalize_chameleon_data(raw: dict) -> dict:
    """Normalize Xkuss and Justlo/Linduu/Gnoxx JSON into dashboard fields."""
    if not isinstance(raw, dict):
        raw = {}
    messages = raw.get("messages") or raw.get("conversation") or []
    messages = messages if isinstance(messages, list) else []

    last_message = _message_text(raw.get("last_message"))
    if not last_message and messages:
        last_message = _message_text(messages[-1])

    last_customer = _text(raw.get("last_client_message"))
    if not last_customer:
        for message in reversed(messages):
            if isinstance(message, dict) and _is_customer(message):
                last_customer = _message_text(message)
                if last_customer:
                    break

    return {
        "last_message": last_message,
        "last_customer_message": last_customer,
        "client_profile": _flatten_profile(raw.get("client") or raw.get("client_information")),
        "fake_profile": _flatten_profile(raw.get("fake_account")),
    }


async def read_extracted_data(tab2, timeout_ms: int = 5_000) -> dict:
    """Read Chameleon's extracted JSON and always return to its Chat tab."""
    empty = {
        "last_message": "",
        "last_customer_message": "",
        "client_profile": {},
        "fake_profile": {},
    }
    try:
        json_button = tab2.get_by_role("button", name="JSON", exact=True)
        await json_button.click()
        payload = tab2.locator("pre").last
        await payload.wait_for(state="visible", timeout=timeout_ms)
        raw = json.loads(await payload.inner_text())
        return normalize_chameleon_data(raw)
    except (PlaywrightError, json.JSONDecodeError, TypeError, ValueError):
        return empty
    finally:
        try:
            await tab2.get_by_role("button", name="Chat", exact=True).click()
        except PlaywrightError:
            pass
