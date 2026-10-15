import unittest
from unittest.mock import patch

import approval_server


class ApprovalRequestDedupTests(unittest.TestCase):
    def setUp(self):
        with approval_server._lock:
            approval_server._requests.clear()
            approval_server._mode = "manual"
            approval_server._mode_overrides.clear()
        self.client = approval_server.app.test_client()

    def tearDown(self):
        with approval_server._lock:
            approval_server._requests.clear()

    def _post(self, platform="Gnoxx", reply="First candidate"):
        return self.client.post(
            "/api/requests",
            json={
                "platform": platform,
                "reply": reply,
                "last_message": "The same live conversation",
                "customer_message": "The same live conversation",
                "messages": [
                    {"sender": "client", "text": "The same live conversation"}
                ],
            },
        )

    @patch.object(approval_server.push_notifications, "queue_approval")
    @patch.object(approval_server, "_analyze_meeting_alert", return_value={
        "ok": True,
        "requested": False,
        "reason": "",
        "tone": "",
        "direction": "",
        "client_expectation": "",
    })
    @patch.object(approval_server, "_translate_de_en", return_value=None)
    @patch.object(approval_server, "get_runtime_settings", return_value={
        "auto_mode_policy": "direct",
        "manual_judge_policy": "disabled",
    })
    def test_second_pending_request_for_same_platform_is_rejected(
        self, _settings, _translate, _analyze, queue_approval
    ):
        first = self._post(reply="Candidate one")
        second = self._post(reply="Candidate two")

        self.assertEqual(first.status_code, 201)
        self.assertEqual(second.status_code, 409)
        self.assertTrue(second.get_json()["duplicate"])
        self.assertEqual(second.get_json()["id"], first.get_json()["id"])
        self.assertEqual(queue_approval.call_count, 1)
        with approval_server._lock:
            pending = [
                item for item in approval_server._requests.values()
                if item["status"] == "pending"
            ]
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["reply"], "Candidate one")

    @patch.object(approval_server.push_notifications, "queue_approval")
    @patch.object(approval_server, "_analyze_meeting_alert", return_value={
        "ok": True,
        "requested": False,
        "reason": "",
        "tone": "",
        "direction": "",
        "client_expectation": "",
    })
    @patch.object(approval_server, "_translate_de_en", return_value=None)
    @patch.object(approval_server, "get_runtime_settings", return_value={
        "auto_mode_policy": "direct",
        "manual_judge_policy": "disabled",
    })
    def test_decided_request_does_not_block_next_conversation(
        self, _settings, _translate, _analyze, _queue_approval
    ):
        first = self._post()
        request_id = first.get_json()["id"]
        cancelled = self.client.post(f"/api/requests/{request_id}/cancel")
        second = self._post(reply="Next conversation candidate")

        self.assertEqual(cancelled.status_code, 200)
        self.assertEqual(second.status_code, 201)
        self.assertNotEqual(second.get_json()["id"], request_id)

    @patch.object(approval_server.push_notifications, "queue_approval")
    @patch.object(approval_server, "_analyze_meeting_alert", return_value={
        "ok": True,
        "requested": False,
        "reason": "",
        "tone": "",
        "direction": "",
        "client_expectation": "",
    })
    @patch.object(approval_server, "_translate_de_en", return_value=None)
    @patch.object(approval_server, "get_runtime_settings", return_value={
        "auto_mode_policy": "direct",
        "manual_judge_policy": "disabled",
    })
    def test_approved_but_not_sent_request_still_blocks_duplicate(
        self, _settings, _translate, _analyze, _queue_approval
    ):
        first = self._post()
        request_id = first.get_json()["id"]
        approved = self.client.post(f"/api/requests/{request_id}/approve")
        duplicate = self._post(reply="Late duplicate")

        self.assertEqual(approved.status_code, 200)
        self.assertEqual(duplicate.status_code, 409)
        self.assertEqual(duplicate.get_json()["id"], request_id)

    @patch.object(approval_server.push_notifications, "queue_approval")
    @patch.object(approval_server, "_analyze_meeting_alert", return_value={
        "ok": True,
        "requested": False,
        "reason": "",
        "tone": "",
        "direction": "",
        "client_expectation": "",
    })
    @patch.object(approval_server, "_translate_de_en", return_value=None)
    @patch.object(approval_server, "get_runtime_settings", return_value={
        "auto_mode_policy": "direct",
        "manual_judge_policy": "disabled",
    })
    def test_each_platform_has_its_own_pending_conversation(
        self, _settings, _translate, _analyze, _queue_approval
    ):
        self.assertEqual(self._post(platform="Gnoxx").status_code, 201)
        self.assertEqual(self._post(platform="Linduu").status_code, 201)


if __name__ == "__main__":
    unittest.main()
