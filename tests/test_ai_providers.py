import unittest
from unittest.mock import Mock, patch

from core.ai_providers import openai_chat_completion


class OpenAICompatibleProviderTests(unittest.TestCase):
    @patch("core.ai_providers.httpx.post")
    def test_greedy_requests_set_mistral_compatible_top_p(self, post):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "choices": [{"message": {"content": "OK"}}],
        }
        post.return_value = response

        result = openai_chat_completion(
            url="https://api.mistral.ai/v1/chat/completions",
            api_key="test-key",
            model="mistral-large-latest",
            messages=[{"role": "user", "content": "Reply only with OK."}],
            max_tokens=8,
        )

        self.assertEqual(result, "OK")
        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["temperature"], 0)
        self.assertEqual(payload["top_p"], 1)

    @patch("core.ai_providers.httpx.post")
    def test_structured_mistral_content_blocks_are_joined(self, post):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "choices": [{"message": {"content": [
                {"type": "text", "text": "O"},
                {"type": "text", "text": "K"},
            ]}}],
        }
        post.return_value = response

        result = openai_chat_completion(
            url="https://api.mistral.ai/v1/chat/completions",
            api_key="test-key",
            model="mistral-large-latest",
            messages=[{"role": "user", "content": "Reply only with OK."}],
            max_tokens=8,
        )

        self.assertEqual(result, "OK")


if __name__ == "__main__":
    unittest.main()
