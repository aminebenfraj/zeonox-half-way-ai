import unittest
from unittest.mock import AsyncMock, patch

import approval_server
from configs.linduu_us import config
from core.bot import _is_fatal, _stop_after_browser_disconnect
from core.justlo_bot import JustloBot
from core.platforms import PLATFORM_BY_SLUG, SELF_MANAGED_PLATFORM_SLUGS


class LinduuUsPlatformTests(unittest.TestCase):
    def test_platform_is_registered_as_self_managed(self):
        platform = PLATFORM_BY_SLUG["linduu_us"]

        self.assertEqual(platform.label, "Linduu US")
        self.assertTrue(platform.self_managed)
        self.assertFalse(platform.enabled_by_default)
        self.assertIn("linduu_us", SELF_MANAGED_PLATFORM_SLUGS)
        self.assertEqual(approval_server._SELF_MANAGED_CDP_PORTS["linduu_us"], 9234)

    def test_uses_supplied_linduu_us_pages_and_classic_login_form(self):
        self.assertEqual(config.tab1_url, "https://www.linduu.us/auth/index/login")
        self.assertEqual(config.login_url, config.tab1_url)
        self.assertEqual(config.mod_url, "https://www.linduu.us/community-mod/")
        self.assertEqual(config.sel_login_user, "#username")
        self.assertEqual(config.sel_login_pass, "#password")
        self.assertEqual(config.sel_login_btn, "#login_btn")
        self.assertTrue(config.console_via_goto)

    def test_has_dedicated_browser_and_chameleon_workspace(self):
        self.assertEqual(config.cdp_url, "http://127.0.0.1:9234")
        self.assertEqual(config.profile, "linduu_us")
        self.assertEqual(config.chameleon_chat, "Linduu US")
        self.assertEqual(config.chameleon_workspace_tab, "Justlo")
        self.assertIn("Reply (English)", config.reply_language_labels)
        self.assertNotIn("Antwort (Deutsch)", config.reply_language_labels)
        self.assertIs(JustloBot, __import__("configs.linduu_us", fromlist=["bot_class"]).bot_class)

    def test_asa_two_three_rule_remains_enabled(self):
        self.assertTrue(config.asa_2_3_nudge_enabled)

    def test_dashboard_marks_english_as_authoritative_send_language(self):
        self.assertEqual(
            approval_server._reply_language_for_platform("Linduu US"),
            ("English", "EN"),
        )

    @patch.object(
        approval_server,
        "run_browser_operation",
        new_callable=AsyncMock,
        return_value="Pools opened",
    )
    def test_dashboard_pools_control_accepts_linduu_us(self, operation):
        response = approval_server.app.test_client().post(
            "/api/control/command",
            json={"cmd": "pools", "target": "linduu_us"},
        )

        self.assertEqual(response.status_code, 200)
        operation.assert_awaited_once_with("pools", ["linduu_us"])


class BrowserCloseLifecycleTests(unittest.IsolatedAsyncioTestCase):
    @patch("core.bot.report_status", new_callable=AsyncMock)
    async def test_browser_close_exits_cleanly_without_launcher_restart(self, status):
        error = RuntimeError("Browser has been closed")
        messages = []

        self.assertTrue(_is_fatal(error))
        with self.assertRaises(SystemExit) as stopped:
            await _stop_after_browser_disconnect("Linduu US", messages.append, error)

        self.assertEqual(stopped.exception.code, 0)
        status.assert_awaited_once_with(
            "Linduu US",
            "stopped",
            "Browser window closed by operator",
            checkpoint="stopped",
        )
        self.assertIn("without reopening", messages[0])


if __name__ == "__main__":
    unittest.main()
