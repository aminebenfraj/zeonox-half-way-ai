"""Persistent Web Push subscriptions for approval notifications.

The browser owns each subscription.  This module stores those subscription
objects locally and signs notifications with a VAPID private key that never
leaves this PC.  Sending happens on a small worker pool so a slow Apple/Google
push endpoint can never delay a bot's approval request.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import threading
from pathlib import Path

try:
    from pywebpush import WebPushException, webpush
except ImportError:  # Dashboard still works; /api/push/config explains why.
    WebPushException = Exception
    webpush = None


BASE_DIR = Path(__file__).resolve().parent.parent


def _project_path(value: str, default: str) -> Path:
    path = Path(value or default)
    return path if path.is_absolute() else BASE_DIR / path


PRIVATE_KEY_PATH = _project_path(
    os.environ.get("VAPID_PRIVATE_KEY", ""), "secrets/private_key.pem"
)
SUBSCRIPTIONS_PATH = _project_path(
    os.environ.get("PUSH_SUBSCRIPTIONS_FILE", ""), ".push_subscriptions.json"
)
PUBLIC_KEY = os.environ.get("VAPID_PUBLIC_KEY", "").strip()
VAPID_SUBJECT = os.environ.get(
    "VAPID_SUBJECT", "mailto:notifications@example.invalid"
).strip()

_lock = threading.Lock()
_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=2, thread_name_prefix="web-push"
)


def is_available() -> bool:
    return bool(webpush and PUBLIC_KEY and PRIVATE_KEY_PATH.is_file())


def _load_locked() -> list[dict]:
    if not SUBSCRIPTIONS_PATH.is_file():
        return []
    try:
        data = json.loads(SUBSCRIPTIONS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict) and item.get("endpoint")]


def _save_locked(items: list[dict]) -> None:
    SUBSCRIPTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = SUBSCRIPTIONS_PATH.with_suffix(SUBSCRIPTIONS_PATH.suffix + ".tmp")
    temp_path.write_text(json.dumps(items, indent=2), encoding="utf-8")
    temp_path.replace(SUBSCRIPTIONS_PATH)


def subscription_count() -> int:
    with _lock:
        return len(_load_locked())


def add_subscription(subscription: dict) -> int:
    endpoint = str(subscription.get("endpoint") or "").strip()
    keys = subscription.get("keys") or {}
    if not endpoint.startswith("https://"):
        raise ValueError("subscription endpoint must use HTTPS")
    if not keys.get("p256dh") or not keys.get("auth"):
        raise ValueError("subscription keys are missing")

    clean = {
        "endpoint": endpoint,
        "expirationTime": subscription.get("expirationTime"),
        "keys": {"p256dh": keys["p256dh"], "auth": keys["auth"]},
    }
    with _lock:
        items = [s for s in _load_locked() if s.get("endpoint") != endpoint]
        items.append(clean)
        _save_locked(items)
        return len(items)


def remove_subscription(endpoint: str) -> int:
    endpoint = str(endpoint or "").strip()
    with _lock:
        items = [s for s in _load_locked() if s.get("endpoint") != endpoint]
        _save_locked(items)
        return len(items)


def _send_payload(payload: dict, endpoint: str | None = None) -> dict:
    if not is_available():
        return {"sent": 0, "removed": 0, "error": "Web Push is not configured"}

    with _lock:
        subscriptions = _load_locked()
    if endpoint:
        subscriptions = [s for s in subscriptions if s.get("endpoint") == endpoint]

    sent = 0
    expired: set[str] = set()
    for subscription in subscriptions:
        try:
            webpush(
                subscription_info=subscription,
                data=json.dumps(payload, ensure_ascii=False),
                vapid_private_key=str(PRIVATE_KEY_PATH),
                vapid_claims={"sub": VAPID_SUBJECT},
                # Approval requests are time-sensitive. Ask Apple/Google's push
                # relay to deliver immediately instead of batching them as a
                # normal-priority background update.
                headers={"Urgency": "high"},
                ttl=86400,
                timeout=15,
            )
            sent += 1
        except WebPushException as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status in (404, 410):
                expired.add(subscription.get("endpoint", ""))
            else:
                print(f"[WebPush] Delivery failed ({status or 'network'}): {exc}")
        except Exception as exc:
            print(f"[WebPush] Delivery failed: {exc}")

    if expired:
        with _lock:
            remaining = [s for s in _load_locked() if s.get("endpoint") not in expired]
            _save_locked(remaining)
    return {"sent": sent, "removed": len(expired)}


def queue_payload(payload: dict, endpoint: str | None = None):
    """Queue a push without blocking the request that triggered it."""
    return _pool.submit(_send_payload, payload, endpoint)


def queue_approval(request_data: dict, pending_count: int):
    message = (
        request_data.get("last_message")
        or request_data.get("customer_message")
        or "A generated reply is waiting for your decision."
    )
    message = " ".join(str(message).split())
    if len(message) > 180:
        message = message[:177] + "..."

    request_id = request_data["id"]
    return queue_payload(
        {
            "title": f"{request_data.get('platform', 'Chat')} needs approval",
            "body": message,
            "icon": "/static/icons/zenox-192-v2.png",
            "badge": "/static/icons/zenox-192-v2.png",
            "tag": f"approval-{request_id}",
            "url": f"/?approval={request_id}",
            "requestId": request_id,
            "pendingCount": pending_count,
        }
    )


def queue_test(endpoint: str):
    return queue_payload(
        {
            "title": "Zenox notifications enabled",
            "body": "Your phone will alert you when a reply needs approval.",
            "icon": "/static/icons/zenox-192-v2.png",
            "badge": "/static/icons/zenox-192-v2.png",
            "tag": "zenox-notification-test",
            "url": "/",
            "pendingCount": 0,
        },
        endpoint,
    )
