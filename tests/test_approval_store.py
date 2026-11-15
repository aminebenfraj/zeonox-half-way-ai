import tempfile
import unittest
from pathlib import Path

from core.approval_store import ApprovalStore


class ApprovalStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = ApprovalStore(Path(self.temp_dir.name) / "approvals.sqlite3")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_request_round_trip_preserves_complete_payload_and_order(self):
        request = {
            "id": "request-1",
            "_seq": 7,
            "platform": "Justlo FR",
            "status": "pending",
            "reply": "Réponse française",
            "client_profile": {"city": "Paris"},
            "messages": [{"sender": "client", "text": "Bonjour"}],
            "judge_score": 9,
        }

        self.store.upsert_request(request)

        self.assertEqual(self.store.load_requests(), [request])

    def test_lifecycle_update_replaces_the_existing_request(self):
        request = {
            "id": "request-1",
            "_seq": 1,
            "platform": "Gnoxx",
            "status": "pending",
            "reply": "First",
        }
        self.store.upsert_request(request)
        request.update({"status": "approved", "final_reply": "Edited"})
        self.store.upsert_request(request)

        loaded = self.store.load_requests()

        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["status"], "approved")
        self.assertEqual(loaded[0]["final_reply"], "Edited")

    def test_dashboard_state_survives_reopening_the_database(self):
        self.store.save_state("mode", "auto")
        self.store.save_state("mode_overrides", {"gnoxx": "manual"})

        reopened = ApprovalStore(self.store.path)

        self.assertEqual(reopened.load_state("mode"), "auto")
        self.assertEqual(
            reopened.load_state("mode_overrides"),
            {"gnoxx": "manual"},
        )


if __name__ == "__main__":
    unittest.main()
