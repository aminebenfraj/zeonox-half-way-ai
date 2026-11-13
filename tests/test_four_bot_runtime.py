import unittest

import approval_server
from configs.gnoxx import config as gnoxx_config
from configs.justlo_fr import config as justlo_fr_config
from configs.linduu import config as linduu_config
from configs.linduu_us import config as linduu_us_config
from core.platforms import PLATFORM_BY_SLUG, SELF_MANAGED_PLATFORM_SLUGS


class FourBotRuntimeTests(unittest.TestCase):
    def test_all_four_bots_are_registered_as_self_managed(self):
        expected = {
            "linduu": "Linduu",
            "linduu_us": "Linduu US",
            "gnoxx": "Gnoxx",
            "justlo_fr": "Justlo FR",
        }

        for slug, label in expected.items():
            with self.subTest(slug=slug):
                self.assertEqual(PLATFORM_BY_SLUG[slug].label, label)
                self.assertIn(slug, SELF_MANAGED_PLATFORM_SLUGS)

    def test_each_bot_targets_its_real_chameleon_workspace_tab(self):
        self.assertEqual(linduu_config.chameleon_workspace_tab, "Justlo")
        self.assertEqual(linduu_us_config.chameleon_workspace_tab, "Justlo")
        self.assertEqual(gnoxx_config.chameleon_workspace_tab, "GNox")
        self.assertEqual(justlo_fr_config.chameleon_workspace_tab, "Justlo FR")

    def test_home_dashboard_prefers_a_live_worker_over_stale_launcher_dead_state(self):
        page = approval_server._PAGE

        self.assertIn("function effectiveProcessState(platformSlug)", page)
        self.assertIn("if (bot && bot.running === true) return bot;", page)
        self.assertIn("const process = effectiveProcessState(platformSlug);", page)
        self.assertIn("const st = effectiveProcessState(slug);", page)


if __name__ == "__main__":
    unittest.main()
