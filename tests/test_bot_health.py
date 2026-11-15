import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import approval_server


class BotHealthTests(unittest.TestCase):
    def setUp(self):
        with approval_server._lock:
            self.saved_requests = dict(approval_server._requests)
            self.saved_status = dict(approval_server._status)
            approval_server._requests.clear()
            approval_server._status.clear()
        with approval_server._health_alert_lock:
            approval_server._active_health_alerts.clear()

    def tearDown(self):
        with approval_server._lock:
            approval_server._requests.clear()
            approval_server._requests.update(self.saved_requests)
            approval_server._status.clear()
            approval_server._status.update(self.saved_status)
        with approval_server._health_alert_lock:
            approval_server._active_health_alerts.clear()

    def _patch_dependencies(self, *, bot_states, launcher_states=None):
        return patch.multiple(
            approval_server,
            _all_bot_statuses=lambda: bot_states,
            _control_status_cached=lambda: {
                "available": True,
                "platforms": launcher_states or {},
            },
            _is_cdp_ready_for_status=lambda _port: True,
            _api_health_snapshot=lambda: [
                {"configured": True, "state": "ready", "label": "Groq", "detail": ""},
                {"configured": True, "state": "ready", "label": "Mistral", "detail": ""},
            ],
            list_accounts=lambda: [],
        )

    def test_health_contains_dashboard_fields_and_component_status(self):
        with approval_server._lock:
            approval_server._status["Gnoxx"] = {
                "state": "approval",
                "detail": "Waiting for approval",
                "warning": "",
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "components": {
                    "chrome": "ok",
                    "moderation_console": "ok",
                    "chameleon": "ok",
                    "api": "ok",
                },
            }
            approval_server._requests["request-1"] = {
                "id": "request-1",
                "_seq": 1,
                "platform": "Gnoxx",
                "status": "pending",
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "error": None,
            }

        bot_states = {
            slug: {"running": slug == "gnoxx", "managed_by": "launcher"}
            for slug in approval_server.PLATFORM_BY_SLUG
        }
        with self._patch_dependencies(
            bot_states=bot_states,
            launcher_states={"gnoxx": {"state": "running", "crashes": 2}},
        ):
            health = approval_server._bot_health_snapshot()["gnoxx"]

        self.assertEqual(health["overall"], "busy")
        self.assertEqual(health["stage"], "approval")
        self.assertEqual(health["restart_count"], 2)
        self.assertEqual(health["pending_approvals"], 1)
        self.assertFalse(health["stale"])
        self.assertEqual(health["components"]["chrome"]["state"], "ok")
        self.assertEqual(health["components"]["moderation_console"]["state"], "ok")
        self.assertEqual(health["components"]["chameleon"]["state"], "ok")
        self.assertEqual(health["components"]["api"]["state"], "ok")

    def test_stale_chameleon_and_overdue_approval_create_meaningful_alerts(self):
        old = datetime.now() - timedelta(seconds=approval_server.APPROVAL_ALERT_AFTER_SECONDS + 30)
        with approval_server._lock:
            approval_server._status["Linduu"] = {
                "state": "error",
                "detail": "Chameleon workspace tab 'Justlo' is unavailable",
                "warning": "workspace not found",
                "updated_at": old.isoformat(timespec="seconds"),
                "components": {"chameleon": "error"},
            }
            approval_server._requests["request-2"] = {
                "id": "request-2",
                "_seq": 2,
                "platform": "Linduu",
                "status": "pending",
                "created_at": old.isoformat(timespec="seconds"),
                "error": None,
            }

        bot_states = {
            slug: {"running": slug == "linduu", "managed_by": "launcher"}
            for slug in approval_server.PLATFORM_BY_SLUG
        }
        with self._patch_dependencies(
            bot_states=bot_states,
            launcher_states={"linduu": {"state": "running", "crashes": 3}},
        ):
            health = approval_server._bot_health_snapshot()["linduu"]

        kinds = {alert["kind"] for alert in health["alerts"]}
        self.assertEqual(health["overall"], "critical")
        self.assertTrue(health["stale"])
        self.assertIn("crash_loop", kinds)
        self.assertIn("stale", kinds)
        self.assertIn("chameleon", kinds)
        self.assertIn("approval_wait", kinds)

    @patch.object(approval_server.push_notifications, "queue_payload")
    @patch.object(approval_server, "_bot_health_snapshot")
    def test_alerts_are_sent_once_until_the_problem_clears(self, health_snapshot, queue_payload):
        unhealthy = {
            "gnoxx": {
                "label": "Gnoxx",
                "pending_approvals": 0,
                "alerts": [{"kind": "crash_loop", "message": "Restarted 3 times"}],
            }
        }
        healthy = {
            "gnoxx": {"label": "Gnoxx", "pending_approvals": 0, "alerts": []}
        }
        health_snapshot.side_effect = [unhealthy, unhealthy, healthy, unhealthy]

        approval_server._evaluate_health_alerts()
        approval_server._evaluate_health_alerts()
        approval_server._evaluate_health_alerts()
        approval_server._evaluate_health_alerts()

        self.assertEqual(queue_payload.call_count, 2)


if __name__ == "__main__":
    unittest.main()
