import unittest
from unittest.mock import AsyncMock

from configs.gnoxx import config as gnoxx_config
from configs.justlo_fr import config as justlo_fr_config
from configs.linduu import config as linduu_config
from configs.linduu_us import config as linduu_us_config
from core.justlo_login import PAGE_CHAT, PAGE_WAITING, classify_page


class JustloFamilyDetectionTests(unittest.IsolatedAsyncioTestCase):
    def test_all_platforms_use_the_stable_conversation_grid_id(self):
        for config in (
            linduu_config,
            linduu_us_config,
            gnoxx_config,
            justlo_fr_config,
        ):
            with self.subTest(platform=config.platform):
                self.assertTrue(
                    config.sel_conv_grid.startswith("#conversation-grid-body")
                )
                self.assertIn("#gridview-1055-body", config.sel_conv_grid)

    async def test_loaded_conversation_is_classified_as_chat(self):
        page = AsyncMock()
        page.query_selector.side_effect = lambda selector: (
            object() if selector == linduu_config.sel_play_btn else None
        )
        page.evaluate.return_value = True

        phase = await classify_page(page, linduu_config)

        self.assertEqual(phase, PAGE_CHAT)
        detector_selectors = page.evaluate.await_args.args[1]
        self.assertEqual(detector_selectors["grid"], linduu_config.sel_conv_grid)

    async def test_empty_console_is_classified_as_waiting(self):
        page = AsyncMock()
        page.query_selector.side_effect = lambda selector: (
            object() if selector == linduu_us_config.sel_pause_btn else None
        )
        page.evaluate.return_value = False

        phase = await classify_page(page, linduu_us_config)

        self.assertEqual(phase, PAGE_WAITING)


if __name__ == "__main__":
    unittest.main()
