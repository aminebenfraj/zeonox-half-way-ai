import unittest
from unittest.mock import AsyncMock, Mock, patch

from core.justlo_bot import JustloBot, JustloConfig, PlaywrightTimeout
from core.justlo_login import _do_login


class UsExtractorTabTests(unittest.IsolatedAsyncioTestCase):
    async def test_login_redirect_to_console_does_not_fill_missing_form(self):
        cfg = JustloConfig(platform='Justlo US', username='test', sel_login_link='')
        page = Mock(url='https://www.justlo.us/community-mod/')
        with patch('core.justlo_login._dismiss_popups', new_callable=AsyncMock), patch('core.justlo_login._in_console', new_callable=AsyncMock, return_value=True):
            await _do_login(page, cfg, '[test]')
        page.locator.assert_not_called()

    async def check_rename(self, platform, configured, actual):
        bot = JustloBot(JustloConfig(platform=platform, chameleon_workspace_tab=configured))
        page = Mock()
        missing = AsyncMock()
        missing.wait_for.side_effect = PlaywrightTimeout('Tab was renamed')
        missing.count.return_value = 0
        actual_tab = AsyncMock()
        actual_tab.count.return_value = 1
        actual_tab.is_visible.return_value = True
        actual_tab.get_attribute.side_effect = ['inactive', 'active']
        page.get_by_role.side_effect = lambda role, name=None, exact=None: actual_tab if name == actual else missing
        surface = AsyncMock()
        page.locator.return_value = surface

        self.assertTrue(await bot._ensure_extractor_tab_active(page))
        actual_tab.click.assert_awaited_once()
        self.assertEqual(surface.wait_for.await_count, 2)

    async def test_both_us_bots_accept_both_known_tab_labels(self):
        for platform in ('Justlo US', 'Linduu US'):
            for configured, actual in (('Justlo', 'Justlo US'), ('Justlo US', 'Justlo')):
                with self.subTest(platform=platform, actual=actual):
                    await self.check_rename(platform, configured, actual)

    async def test_missing_extractor_does_not_select_unrelated_tab(self):
        bot = JustloBot(JustloConfig(platform='Linduu US', chameleon_workspace_tab='Justlo'))
        page = Mock()
        missing = AsyncMock()
        missing.wait_for.side_effect = PlaywrightTimeout('No extractor')
        missing.count.return_value = 0
        missing.all_text_contents.return_value = ['Solo', 'Team', 'Qualität']
        page.get_by_role.return_value = missing
        self.assertFalse(await bot._ensure_extractor_tab_active(page))
        missing.click.assert_not_awaited()
