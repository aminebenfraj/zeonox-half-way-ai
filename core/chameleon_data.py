"""Read conversation context already extracted by the real Chameleon workspace.

This is intentionally not a second/local extractor.  After a bot pastes the
platform HTML into Chameleon, the AgentWorkspace exposes its parsed payload in
the JSON tab.  We read that payload for the approval dashboard, then restore
the Chat tab before reply generation continues.
"""

import json
import re

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


def _normalized_key(value) -> str:
    return re.sub(r"[^a-z0-9]", "", _text(value).casefold())


def _field(record: dict, *aliases):
    """Read a field while tolerating German/English and snake/camel case keys."""
    if not isinstance(record, dict):
        return None
    wanted = {_normalized_key(alias) for alias in aliases}
    for key, value in record.items():
        if _normalized_key(key) in wanted and value not in (None, ""):
            return value
    return None


def _identity(value) -> str:
    if isinstance(value, dict):
        value = _field(value, "username", "user_name", "name", "nickname", "nick", "label")
    text = _text(value).casefold().strip()
    return re.sub(r"\s+", " ", text).lstrip("@")


def _profile_username(profile) -> str:
    return _identity(_field(
        profile or {}, "username", "user_name", "name", "nickname", "nick",
    ))


def _bool_value(value) -> bool | None:
    if isinstance(value, bool):
        return value
    normalized = _identity(value)
    if normalized in {"true", "1", "yes", "ja"}:
        return True
    if normalized in {"false", "0", "no", "nein"}:
        return False
    return None


def _is_customer(
    message: dict,
    client_username: str = "",
    fake_username: str = "",
) -> bool:
    """Resolve the message side from its route and the extracted identities.

    Justlo-family rows expose ``Von`` and ``An`` names.  Chameleon may preserve
    those names under German or English keys, or place the username directly in
    ``sender``.  Match those values to the already-extracted Client and Fake
    Account usernames before falling back to the older moderator flag.
    """
    source = _identity(_field(
        message, "sender", "from", "von", "author", "source",
        "sender_username", "from_username", "from_name",
    ))
    target = _identity(_field(
        message, "recipient", "to", "an", "receiver", "target",
        "recipient_username", "to_username", "to_name",
    ))
    client_username = _identity(client_username)
    fake_username = _identity(fake_username)

    if source in {"client", "customer", "kunde", "user", "member"}:
        return True
    if source in {"fake", "fake account", "fake_account", "moderator", "operator", "agent"}:
        return False

    if client_username and source == client_username:
        return True
    if fake_username and source == fake_username:
        return False

    # The recipient is enough when the source was omitted: client -> fake and
    # fake -> client are the only valid directions in this conversation grid.
    if fake_username and target == fake_username:
        return True
    if client_username and target == client_username:
        return False

    explicit_client = _bool_value(_field(message, "is_client", "is_customer", "from_client"))
    if explicit_client is not None:
        return explicit_client
    has_moderator = _bool_value(_field(message, "has_moderator", "is_moderator"))
    return not has_moderator if has_moderator is not None else False


def normalize_chameleon_data(raw: dict) -> dict:
    """Normalize Xkuss and Justlo/Linduu/Gnoxx JSON into dashboard fields."""
    if not isinstance(raw, dict):
        raw = {}
    client_raw = _field(raw, "client", "client_information", "client_info") or {}
    fake_raw = _field(raw, "fake_account", "fake", "fake_information", "fake_info") or {}
    client_username = _profile_username(client_raw)
    fake_username = _profile_username(fake_raw)

    messages = _field(raw, "messages", "conversation", "chat") or []
    messages = messages if isinstance(messages, list) else []

    last_message = _message_text(raw.get("last_message"))
    if not last_message and messages:
        last_message = _message_text(messages[-1])

    # Recompute this from the routed messages.  The extractor's cached
    # last_client_message is precisely what becomes wrong when a username in
    # Von/An was mistaken for a generic moderator sender.
    last_customer = ""
    for message in reversed(messages):
        if (
            isinstance(message, dict)
            and _is_customer(message, client_username, fake_username)
        ):
            last_customer = _message_text(message)
            if last_customer:
                break
    if not last_customer:
        last_customer = _text(_field(raw, "last_client_message", "last_customer_message"))

    # Normalized transcript for the meeting-alert analyzer -- which side sent
    # each message matters more than anything else there (see
    # approval_server.py's _analyze_meeting_alert), so resolve it once here.
    normalized_messages = [
        {
            "sender": (
                "client"
                if _is_customer(message, client_username, fake_username)
                else "fake_account"
            ),
            "text": _message_text(message),
        }
        for message in messages
        if isinstance(message, dict) and _message_text(message)
    ]

    return {
        "last_message": last_message,
        "last_customer_message": last_customer,
        "client_profile": _flatten_profile(client_raw),
        "fake_profile": _flatten_profile(fake_raw),
        "messages": normalized_messages,
    }


async def read_extracted_data(tab2, timeout_ms: int = 5_000) -> dict:
    """Read Chameleon's extracted JSON and always return to its Chat tab."""
    empty = {
        "last_message": "",
        "last_customer_message": "",
        "client_profile": {},
        "fake_profile": {},
        "messages": [],
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
