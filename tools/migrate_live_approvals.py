"""Snapshot a running in-memory dashboard into its SQLite approval store."""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.request
from pathlib import Path

from dotenv import load_dotenv
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.approval_store import ApprovalStore


load_dotenv(ROOT / ".env")


def _read_json(base_url: str, path: str):
    request = urllib.request.Request(f"{base_url.rstrip('/')}{path}")
    username = os.environ.get("APPROVAL_USER") or ""
    password = os.environ.get("APPROVAL_PASS") or ""
    if username and password:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        request.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--url",
        default=os.environ.get("APPROVAL_SERVER_URL", "http://127.0.0.1:8799"),
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=Path(
            os.environ.get("APPROVAL_DB_PATH")
            or (ROOT / "secrets" / "approval_state.sqlite3")
        ),
    )
    args = parser.parse_args()

    pending = _read_json(args.url, "/api/requests?status=pending")
    history = _read_json(
        args.url,
        "/api/requests?status=approved,sending,rejected,cancelled,skip_requested,skipped,transferred,sent,failed&limit=300",
    )
    requests = list(history) + list(pending)
    requests.sort(key=lambda item: (str(item.get("created_at") or ""), str(item.get("id") or "")))
    for sequence, item in enumerate(requests):
        item["_seq"] = sequence

    store = ApprovalStore(args.database)
    store.replace_requests(requests)
    mode = _read_json(args.url, "/api/mode").get("mode") or "manual"
    store.save_state("mode", mode)

    overrides = {}
    for platform in _read_json(args.url, "/api/platforms"):
        slug = platform["slug"]
        item = _read_json(args.url, f"/api/mode/override?platform={slug}")
        if item.get("override") in ("manual", "auto"):
            overrides[str(platform["name"]).casefold()] = item["override"]
    store.save_state("mode_overrides", overrides)

    print(
        f"Migrated {len(requests)} request(s), including {len(pending)} pending, "
        f"to {args.database}"
    )


if __name__ == "__main__":
    main()
