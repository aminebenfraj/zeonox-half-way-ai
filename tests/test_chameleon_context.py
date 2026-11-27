import unittest
from unittest.mock import AsyncMock, Mock, patch

import approval_server
from core.chameleon_data import normalize_chameleon_data, read_extracted_data
from core.justlo_bot import JustloBot, JustloConfig


class ChameleonContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_failure_raises_instead_of_returning_empty_context(self):
        page = Mock()
        button = AsyncMock()
        page.get_by_role.return_value = button
        payload = AsyncMock()
        payload.inner_text.return_value = '{broken'
        page.locator.return_value.last = payload
        with patch('core.chameleon_data.asyncio.sleep', new_callable=AsyncMock):
            with self.assertRaisesRegex(RuntimeError, 'current Chameleon conversation'):
                await read_extracted_data(page)
        self.assertEqual(payload.inner_text.await_count, 3)
        self.assertEqual(page.get_by_role.call_args.kwargs['name'], 'Chat')

    async def test_wrong_chat_with_overlapping_name_is_not_accepted(self):
        bot = JustloBot(JustloConfig(chameleon_chat='Justlo US'))
        page = Mock()
        combo = AsyncMock()
        combo.count.return_value = 1
        combo.first.inner_text = AsyncMock(return_value='Other Justlo US workspace')
        page.locator.return_value = combo
        self.assertFalse(await bot._is_chat_selected(page))

    def test_fc_and_transcript_are_normalized_from_one_payload(self):
        result = normalize_chameleon_data({'message_type': 'FC', 'client_information': {'username': 'client'}, 'fake_account': {'username': 'fake'}, 'conversation': [{'sender': 'client', 'text': 'Hello'}, {'sender': 'fake_account', 'text': 'Hi'}]})
        self.assertEqual(result['reply_type'], 'FC')
        self.assertEqual(len(result['messages']), 2)

    def test_new_source_conversation_does_not_reuse_old_pending_card(self):
        old = {'id': 'old', 'platform': 'Linduu', 'status': 'pending', 'conversation_key': 'previous'}
        with patch.dict(approval_server._requests, {'old': old}, clear=True), patch.object(approval_server, '_persist_request_locked'):
            self.assertIsNone(approval_server._request_for_conversation_locked('Linduu', 'current'))
            self.assertEqual(old['status'], 'cancelled')

    def test_same_source_conversation_reuses_pending_card(self):
        old = {'id': 'old', 'platform': 'Linduu', 'status': 'pending', 'conversation_key': 'current'}
        with patch.dict(approval_server._requests, {'old': old}, clear=True):
            self.assertIs(approval_server._request_for_conversation_locked('Linduu', 'current'), old)
