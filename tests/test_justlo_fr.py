import unittest
from unittest.mock import Mock, patch

import approval_server
from configs.justlo_fr import config
from configs.gnoxx import config as gnoxx_config
from core.bot import _HTML_SERIALIZER_JS
from core.chameleon_data import normalize_chameleon_data
from core.justlo_bot import JustloConfig, _GET_CONFIGURED_REPLY_JS
from core.platforms import PLATFORM_BY_SLUG, SELF_MANAGED_PLATFORM_SLUGS


class JustloFrenchPlatformTests(unittest.TestCase):
    def test_gnoxx_uses_its_dedicated_chameleon_chat(self):
        self.assertEqual(gnoxx_config.chameleon_chat, "GNox")

    def test_platform_is_registered_as_self_managed(self):
        platform = PLATFORM_BY_SLUG["justlo_fr"]

        self.assertEqual(platform.label, "Justlo FR")
        self.assertTrue(platform.self_managed)
        self.assertIn("justlo_fr", SELF_MANAGED_PLATFORM_SLUGS)

    def test_login_and_eye_navigation_match_supplied_pages(self):
        self.assertEqual(config.login_url, "https://www.justlo.fr/login")
        self.assertIn("Nom d'utilisateur", config.sel_login_user)
        self.assertIn("Mot de passe", config.sel_login_pass)
        self.assertIn("Se connecter", config.sel_login_btn)
        self.assertIn("{username}", config.sel_mod_menu_btn)
        self.assertIn("Alter Mod", config.sel_mod_link)

    def test_french_chameleon_chat_and_reply_card_are_selected(self):
        self.assertEqual(config.chameleon_chat, "Justlo FR")
        self.assertEqual(config.chameleon_workspace_tab, "Justlo FR")
        self.assertEqual(config.chameleon_html_adapter, "fr_extjs")
        self.assertFalse(config.asa_2_3_nudge_enabled)
        self.assertEqual(config.additional_instructions, "")
        self.assertTrue(
            any("Französisch" in label or "Français" in label
                for label in config.reply_language_labels)
        )

    def test_existing_justlo_family_default_remains_german(self):
        self.assertEqual(
            JustloConfig().reply_language_labels,
            ("Antwort (Deutsch)",),
        )
        self.assertTrue(JustloConfig().asa_2_3_nudge_enabled)

    def test_alternate_draft_reply_card_is_supported(self):
        self.assertIn("entwurf (nicht freigegeben)", _GET_CONFIGURED_REPLY_JS)
        self.assertIn("draft (not approved)", _GET_CONFIGURED_REPLY_JS)

    def test_scraped_extjs_html_preserves_message_row_classes(self):
        self.assertIn("'class'", _HTML_SERIALIZER_JS)

    def test_french_chameleon_profile_shape_is_normalized(self):
        normalized = normalize_chameleon_data({
            "fake_account": {
                "username": "fake-user",
                "fields": {"age": "30"},
            },
            "client_details": {
                "username": "client-user",
                "fields": {"city": "Paris"},
            },
            "messages": [],
            "last_real_message": "customer message",
        })

        self.assertEqual(normalized["fake_profile"]["Username"], "fake-user")
        self.assertEqual(normalized["client_profile"]["Username"], "client-user")
        self.assertEqual(normalized["client_profile"]["city"], "Paris")
        self.assertEqual(normalized["last_customer_message"], "customer message")

    def test_dashboard_marks_french_as_authoritative_send_language(self):
        self.assertEqual(
            approval_server._reply_language_for_platform("Justlo FR"),
            ("French", "FR"),
        )

    @patch.object(approval_server, "_control_status_cached", return_value={"available": True})
    @patch.object(approval_server.httpx, "post")
    def test_restart_cancels_the_old_pending_card(self, post, _control_status):
        post.return_value = Mock(
            content=b'{"ok":true}',
            status_code=200,
            is_success=True,
        )
        request_id = "justlo-fr-stale-restart-card"
        with approval_server._lock:
            approval_server._requests[request_id] = {
                "id": request_id,
                "platform": "Justlo FR",
                "status": "pending",
            }
        try:
            response = approval_server.app.test_client().post(
                "/api/control/command",
                json={"cmd": "restart", "target": "justlo_fr"},
            )
            self.assertEqual(response.status_code, 200)
            with approval_server._lock:
                self.assertEqual(
                    approval_server._requests[request_id]["status"],
                    "cancelled",
                )
        finally:
            with approval_server._lock:
                approval_server._requests.pop(request_id, None)
                approval_server._status.pop("Justlo FR", None)

    @patch.object(
        approval_server,
        "_translate_en_to_language_with_mistral",
        return_value="Bonjour, comment vas tu ?",
    )
    def test_dashboard_translates_english_edits_back_to_french(self, translate):
        request_id = "justlo-fr-translation-test"
        with approval_server._lock:
            approval_server._requests[request_id] = {
                "id": request_id,
                "platform": "Justlo FR",
                "status": "pending",
                "reply": "Salut",
                "reply_en": "Hi",
                "reply_language": "French",
                "reply_language_code": "FR",
            }
        try:
            response = approval_server.app.test_client().post(
                f"/api/requests/{request_id}/translate-to-german",
                json={"english_reply": "Hello, how are you?"},
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()["reply"], "Bonjour, comment vas tu ?")
            translate.assert_called_once_with("Hello, how are you?", "French")
        finally:
            with approval_server._lock:
                approval_server._requests.pop(request_id, None)


if __name__ == "__main__":
    unittest.main()
