import unittest
from unittest.mock import Mock, patch

import approval_server


class CardTranslationFallbackTests(unittest.TestCase):
    @patch.dict(approval_server.os.environ, {"MISTRAL_API_KEY": "test-key"})
    @patch.object(approval_server, "_translate_en_to_language_with_groq")
    @patch.object(approval_server, "openai_chat_completion")
    def test_groq_primary_path_does_not_wait_for_mistral(
        self, mistral, groq_primary
    ):
        groq_primary.return_value = "Hallo, wie geht es dir?"

        result = approval_server._translate_en_to_language_with_mistral(
            "Hello, how are you?", "German"
        )

        self.assertEqual(result, "Hallo, wie geht es dir?")
        mistral.assert_not_called()
        groq_primary.assert_called_once_with("Hello, how are you?", "German")

    @patch.dict(approval_server.os.environ, {"MISTRAL_API_KEY": "test-key"})
    @patch.object(approval_server, "_translate_en_to_language_with_groq")
    @patch.object(approval_server, "openai_chat_completion")
    def test_groq_failure_uses_one_mistral_fallback(
        self, mistral, groq_primary
    ):
        groq_primary.side_effect = RuntimeError("provider unavailable")
        mistral.return_value = "Bonjour, comment allez vous ?"

        result = approval_server._translate_en_to_language_with_mistral(
            "Hello, how are you?", "French"
        )

        self.assertEqual(result, "Bonjour, comment allez vous ?")
        mistral.assert_called_once()
        self.assertEqual(mistral.call_args.kwargs["timeout"], 12.0)

    @patch.dict(approval_server.os.environ, {"MISTRAL_API_KEY": "test-key"})
    @patch.object(approval_server, "_translate_en_to_language_with_groq")
    @patch.object(approval_server, "openai_chat_completion")
    def test_empty_mistral_fallback_reports_both_failures(
        self, mistral, groq_primary
    ):
        groq_primary.side_effect = RuntimeError("Groq unavailable")
        mistral.return_value = ""

        with self.assertRaisesRegex(RuntimeError, "Groq translation failed"):
            approval_server._translate_en_to_language_with_mistral(
                "Good morning", "German"
            )

        mistral.assert_called_once()

    @patch.object(approval_server, "_get_groq_test_client")
    def test_groq_fallback_returns_provider_translation(self, get_client):
        response = Mock()
        response.choices = [Mock(message=Mock(content="Guten Abend"))]
        client = Mock()
        client.chat.completions.create.return_value = response
        get_client.return_value = client

        result = approval_server._translate_en_to_language_with_groq(
            "Good evening", "German"
        )

        self.assertEqual(result, "Guten Abend")
        request = client.chat.completions.create.call_args.kwargs
        self.assertEqual(request["reasoning_effort"], "low")
        self.assertEqual(request["messages"][-1]["content"], "Good evening")


if __name__ == "__main__":
    unittest.main()
