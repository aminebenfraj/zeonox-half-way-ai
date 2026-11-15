import itertools
import tempfile
import unittest
from pathlib import Path

import approval_server
from core.approval_store import ApprovalStore


class SQLiteRecoveryTests(unittest.TestCase):
    def test_server_restores_same_request_id_and_review_modes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "approval_state.sqlite3"
            store = ApprovalStore(database_path)
            stored_request = {
                "id": "live-approval-id",
                "_seq": 12,
                "platform": "Linduu",
                "status": "pending",
                "reply": "Still waiting",
                "created_at": "2026-11-13T17:28:18",
            }
            store.upsert_request(stored_request)
            store.save_state("mode", "auto")
            store.save_state("mode_overrides", {"linduu": "manual"})

            saved_path = approval_server._APPROVAL_DB_PATH
            saved_store = approval_server._request_store
            saved_requests = dict(approval_server._requests)
            saved_mode = approval_server._mode
            saved_overrides = dict(approval_server._mode_overrides)
            try:
                approval_server._APPROVAL_DB_PATH = database_path
                approval_server._request_store = None
                with approval_server._lock:
                    approval_server._requests.clear()
                    approval_server._mode = "manual"
                    approval_server._mode_overrides.clear()

                approval_server._initialize_request_store()

                self.assertEqual(
                    approval_server._requests["live-approval-id"]["reply"],
                    "Still waiting",
                )
                self.assertEqual(approval_server._mode, "auto")
                self.assertEqual(
                    approval_server._mode_overrides,
                    {"linduu": "manual"},
                )
                self.assertEqual(next(approval_server._order), 13)
            finally:
                approval_server._APPROVAL_DB_PATH = saved_path
                approval_server._request_store = saved_store
                with approval_server._lock:
                    approval_server._requests.clear()
                    approval_server._requests.update(saved_requests)
                    approval_server._mode = saved_mode
                    approval_server._mode_overrides.clear()
                    approval_server._mode_overrides.update(saved_overrides)
                    next_sequence = max(
                        (int(item.get("_seq") or 0) for item in saved_requests.values()),
                        default=-1,
                    ) + 1
                    approval_server._order = itertools.count(next_sequence)


if __name__ == "__main__":
    unittest.main()
