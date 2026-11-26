import unittest
from unittest.mock import AsyncMock, patch

import approval_server
from configs.justlo_us import config
from core.platforms import PLATFORM_BY_SLUG


class JustloUsTests(unittest.TestCase):
    def test_workspace_settings_reject_invalid_values_before_writing(self):
        client = approval_server.app.test_client()
        with patch.object(approval_server, 'set_key') as save:
            response = client.post('/api/settings/justlo-us', json={'chat': 'Good', 'tab': 'bad\nname'})
            self.assertEqual(response.status_code, 400)
            save.assert_not_called()

    def test_workspace_settings_save_and_reload(self):
        import tempfile
        from pathlib import Path
        import os
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ), patch.object(approval_server, '_ENV_PATH', Path(directory) / '.env'):
            client = approval_server.app.test_client()
            response = client.post('/api/settings/justlo-us', json={'chat': 'English workspace', 'tab': 'Justlo'})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(client.get('/api/settings/justlo-us').json, {'chat': 'English workspace', 'tab': 'Justlo'})

    def test_isolated_english_platform(self):
        self.assertTrue(PLATFORM_BY_SLUG['justlo_us'].self_managed)
        self.assertEqual(approval_server._SELF_MANAGED_CDP_PORTS['justlo_us'], 9235)
        self.assertEqual(approval_server._BUILTIN_CDP_PORTS['justlo_us'], 9235)
        self.assertEqual(config.profile, 'justlo_us')
        self.assertEqual(config.login_url, 'https://www.justlo.us/login')
        self.assertEqual(config.chameleon_html_adapter, 'fr_extjs')
        self.assertEqual(approval_server._reply_language_for_platform('Justlo US'), ('English', 'EN'))

    @patch.object(approval_server, 'run_browser_operation', new_callable=AsyncMock, return_value='Pools opened')
    def test_pools_routes_to_new_platform(self, operation):
        response = approval_server.app.test_client().post('/api/control/command', json={'cmd': 'pools', 'target': 'justlo_us'})
        self.assertEqual(response.status_code, 200)
        operation.assert_awaited_once_with('pools', ['justlo_us'])
