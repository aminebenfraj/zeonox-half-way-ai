import unittest
from unittest.mock import patch

import approval_server


class ChatHtmlToJsonTests(unittest.TestCase):
    def setUp(self):
        self.client = approval_server.app.test_client()

    def test_page_and_sidebar_link_are_available(self):
        page = self.client.get("/chat-html-to-json")
        dashboard = self.client.get("/")

        self.assertEqual(page.status_code, 200)
        self.assertIn("Last message", page.get_data(as_text=True))
        self.assertIn("English translation", page.get_data(as_text=True))
        self.assertIn('/chat-html-to-json', dashboard.get_data(as_text=True))

    @patch.object(approval_server, "_translate_de_en", return_value="How are you?")
    def test_translation_endpoint_returns_english(self, translate):
        response = self.client.post(
            "/api/chat-html-to-json/translate",
            json={"text": "Wie geht es dir?"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["translation"], "How are you?")
        translate.assert_called_once_with("Wie geht es dir?", "original-language")

    @patch.object(approval_server, "_translate_de_en", return_value=None)
    def test_translation_endpoint_reports_unavailable_provider(self, _translate):
        response = self.client.post(
            "/api/chat-html-to-json/translate",
            json={"text": "Bonjour"},
        )

        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.get_json()["ok"])


if __name__ == "__main__":
    unittest.main()
