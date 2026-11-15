"""Durable SQLite storage for approval lifecycles and dashboard state."""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable


class ApprovalStore:
    """Small thread-safe store used by the approval dashboard.

    Each approval remains a normal dictionary in memory for fast dashboard
    rendering.  Every lifecycle transition is synchronously upserted before
    the API response returns, so a process restart can rebuild the same queue
    with the same request IDs.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @contextmanager
    def _connection(self):
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize_schema(self) -> None:
        with self._lock, self._connection() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS approval_requests (
                    id TEXT PRIMARY KEY,
                    seq INTEGER NOT NULL,
                    platform TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_approval_requests_seq
                    ON approval_requests(seq);
                CREATE INDEX IF NOT EXISTS idx_approval_requests_status
                    ON approval_requests(status);

                CREATE TABLE IF NOT EXISTS dashboard_state (
                    key TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

    @staticmethod
    def _request_values(item: dict) -> tuple[str, int, str, str, str]:
        request_id = str(item["id"])
        sequence = int(item.get("_seq") or 0)
        platform = str(item.get("platform") or "unknown")
        status = str(item.get("status") or "unknown")
        payload = {key: value for key, value in item.items() if key != "_seq"}
        return (
            request_id,
            sequence,
            platform,
            status,
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )

    def upsert_request(self, item: dict) -> None:
        values = self._request_values(item)
        with self._lock, self._connection() as connection:
            connection.execute(
                """
                INSERT INTO approval_requests
                    (id, seq, platform, status, payload_json, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                    seq = excluded.seq,
                    platform = excluded.platform,
                    status = excluded.status,
                    payload_json = excluded.payload_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                values,
            )

    def delete_request(self, request_id: str) -> None:
        with self._lock, self._connection() as connection:
            connection.execute(
                "DELETE FROM approval_requests WHERE id = ?",
                (str(request_id),),
            )

    def replace_requests(self, items: Iterable[dict]) -> None:
        values = [self._request_values(item) for item in items]
        with self._lock, self._connection() as connection:
            connection.execute("DELETE FROM approval_requests")
            connection.executemany(
                """
                INSERT INTO approval_requests
                    (id, seq, platform, status, payload_json, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                values,
            )

    def load_requests(self) -> list[dict]:
        with self._lock, self._connection() as connection:
            rows = connection.execute(
                """
                SELECT seq, payload_json
                FROM approval_requests
                ORDER BY seq ASC
                """
            ).fetchall()
        requests = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            payload["_seq"] = int(row["seq"])
            requests.append(payload)
        return requests

    def save_state(self, key: str, value) -> None:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        with self._lock, self._connection() as connection:
            connection.execute(
                """
                INSERT INTO dashboard_state (key, payload_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (key, payload),
            )

    def load_state(self, key: str, default=None):
        with self._lock, self._connection() as connection:
            row = connection.execute(
                "SELECT payload_json FROM dashboard_state WHERE key = ?",
                (key,),
            ).fetchone()
        return default if row is None else json.loads(row["payload_json"])

