import contextlib
import io
import unittest
from unittest.mock import patch

import approval_server as server
from core.approval import _decision_result, ApprovalCancelled
from core.platforms import PLATFORMS


class ApprovalModesTests(unittest.TestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(server._requests, {}, clear=True))
        self.stack.enter_context(patch.dict(server._mode_overrides, {}, clear=True))
        self.stack.enter_context(patch.object(server, '_mode', 'manual'))
        for name in ('_persist_request_locked', '_persist_dashboard_state_locked', '_bump_state'):
            self.stack.enter_context(patch.object(server, name))
        self.stack.enter_context(patch.object(server.push_notifications, 'queue_approval'))
        self.stack.enter_context(patch.object(server, '_translate_de_en', return_value=None))
        self.stack.enter_context(patch.object(server, '_analyze_meeting_alert', return_value={'ok': True}))
        self.settings = {'auto_mode_policy': 'judge_manual_fallback', 'manual_judge_policy': 'enabled'}
        self.stack.enter_context(patch.object(server, 'get_runtime_settings', side_effect=lambda: self.settings))
        self.guard = self.stack.enter_context(patch.object(server, '_classify_message_with_groq', return_value={'ok': True, 'contains_meeting': False, 'action': 'approve', 'reply': ''}))
        self.judge = self.stack.enter_context(patch.object(server, '_judge_reply'))
        self.stack.enter_context(patch.object(server._judge_pool, 'submit', side_effect=lambda function, *args: function(*args)))
        self.stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        self.client = server.app.test_client()

    def create(self, platform):
        response = self.client.post('/api/requests', json={'platform': platform.label, 'reply': 'A test reply for review.', 'customer_message': 'Hello there', 'last_message': 'Hello there'})
        self.assertEqual(response.status_code, 201)
        return response.json['id']

    def test_all_platforms_manual_auto_and_provider_failures(self):
        cases = [
            ('manual', 'direct', 'disabled', 10, True, 'pending'),
            ('manual', 'direct', 'enabled', 10, True, 'pending'),
            ('auto', 'direct', 'disabled', 10, True, 'approved'),
            ('auto', 'judge_manual_fallback', 'enabled', 10, True, 'approved'),
            ('auto', 'judge_manual_fallback', 'enabled', 7, True, 'pending'),
            ('auto', 'judge_manual_fallback', 'enabled', None, True, 'pending'),
            ('auto', 'judge_always_manual', 'enabled', 10, True, 'pending'),
            ('auto', 'judge_regenerate', 'enabled', 7, True, 'rejected'),
            ('auto', 'judge_regenerate', 'enabled', None, True, 'pending'),
            ('auto', 'direct', 'disabled', 10, False, 'pending'),
        ]
        for platform in PLATFORMS:
            for mode, policy, manual_judge, score, guard_ok, expected in cases:
                with self.subTest(platform=platform.slug, mode=mode, policy=policy, score=score, guard_ok=guard_ok):
                    server._requests.clear()
                    server._mode = mode
                    self.settings.update(auto_mode_policy=policy, manual_judge_policy=manual_judge)
                    self.judge.return_value = {'ok': score is not None, 'score': score, 'error': 'Test provider failure' if score is None else None}
                    self.guard.return_value = {'ok': guard_ok, 'contains_meeting': False, 'action': 'approve', 'reply': '', 'error': 'Test guard failure'}
                    request_id = self.create(platform)
                    self.assertEqual(server._requests[request_id]['status'], expected)
                    claim = self.client.post(f'/api/requests/{request_id}/sending')
                    self.assertEqual(claim.status_code, 200 if expected == 'approved' else 409)

    def test_every_bot_override_wins_over_global_mode(self):
        self.settings.update(auto_mode_policy='direct', manual_judge_policy='disabled')
        for platform in PLATFORMS:
            for global_mode, local_mode, expected in [('auto', 'manual', 'pending'), ('manual', 'auto', 'approved')]:
                with self.subTest(platform=platform.slug, override=local_mode):
                    server._requests.clear()
                    server._mode = global_mode
                    response = self.client.post('/api/mode/override', json={'platform': platform.slug, 'mode': local_mode})
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.json['effective'], local_mode)
                    request_id = self.create(platform)
                    self.assertEqual(server._requests[request_id]['status'], expected)
                    self.client.post('/api/mode/override', json={'platform': platform.slug, 'mode': 'default'})

    def test_manual_approve_uses_edited_reply_for_every_bot(self):
        self.settings.update(manual_judge_policy='disabled')
        for platform in PLATFORMS:
            with self.subTest(platform=platform.slug):
                server._requests.clear()
                request_id = self.create(platform)
                self.assertIsNone(_decision_result(server._requests[request_id], 'Original', request_id))
                response = self.client.post(f'/api/requests/{request_id}/approve', json={'edited_reply': 'Human edited reply'})
                self.assertEqual(response.status_code, 200)
                result = _decision_result(server._requests[request_id], 'Original', request_id)
                self.assertEqual(result, (True, 'Human edited reply', request_id))
                self.assertEqual(self.client.post(f'/api/requests/{request_id}/sending').status_code, 200)

    def test_manual_cancel_never_returns_sendable_text(self):
        self.settings.update(manual_judge_policy='disabled')
        request_id = self.create(PLATFORMS[0])
        self.client.post(f'/api/requests/{request_id}/cancel')
        with self.assertRaises(ApprovalCancelled):
            _decision_result(server._requests[request_id], 'Original', request_id)
        self.assertEqual(self.client.post(f'/api/requests/{request_id}/sending').status_code, 409)

    def test_judge_finishing_after_switch_to_manual_cannot_auto_approve(self):
        server._requests['late'] = {'id': 'late', 'platform': 'Linduu', 'status': 'pending'}
        server._mode = 'manual'
        self.judge.return_value = {'ok': True, 'score': 10}
        server._complete_judge_analysis('late', {'platform': 'Linduu'}, True, 'Reply', 'judge_manual_fallback')
        self.assertEqual(server._requests['late']['status'], 'pending')

    def test_direct_mode_switch_to_manual_during_guard_waits_for_review(self):
        server._mode = 'auto'
        self.settings.update(auto_mode_policy='direct', manual_judge_policy='disabled')
        def delayed_guard(*args):
            server._mode = 'manual'
            return {'ok': True, 'contains_meeting': False, 'action': 'approve', 'reply': ''}
        self.guard.side_effect = delayed_guard
        request_id = self.create(PLATFORMS[0])
        self.assertEqual(server._requests[request_id]['status'], 'pending')

    def test_manual_switch_holds_automatic_countdown_for_every_bot(self):
        self.settings.update(auto_mode_policy='direct', manual_judge_policy='disabled')
        for platform in PLATFORMS:
            with self.subTest(platform=platform.slug):
                server._requests.clear()
                server._mode = 'auto'
                request_id = self.create(platform)
                self.assertEqual(server._requests[request_id]['status'], 'approved')
                self.client.post('/api/mode', json={'mode': 'manual'})
                self.assertEqual(server._requests[request_id]['status'], 'pending')
                self.assertEqual(server._requests[request_id]['send_cancel_revision'], 1)
                self.assertEqual(self.client.post(f'/api/requests/{request_id}/sending').status_code, 409)
