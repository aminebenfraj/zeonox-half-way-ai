import unittest

from core.chameleon_data import normalize_chameleon_data
from core.justlo_bot import _GET_CHAMELEON_REPLY_TYPE_JS


class ChameleonReplyTypeReaderTests(unittest.TestCase):
    def test_chameleon_json_dia_is_authoritative(self):
        normalized = normalize_chameleon_data({
            "message_type": "DIA",
            "is_anstupser": False,
        })

        self.assertEqual(normalized["reply_type"], "DIA")

    def test_chameleon_json_asa_is_normalized_for_dashboard(self):
        normalized = normalize_chameleon_data({"message_type": "ASA"})

        self.assertEqual(normalized["reply_type"], "ASA Follow-up")

    def test_reads_result_badge_instead_of_entire_detected_row(self):
        """The row's unchecked nudge control also contains the word ASA."""
        self.assertIn("detectedRow.querySelectorAll('span, div, p')", _GET_CHAMELEON_REPLY_TYPE_JS)
        self.assertIn("!el.closest('label, button')", _GET_CHAMELEON_REPLY_TYPE_JS)
        self.assertNotIn("detectedRow.textContent", _GET_CHAMELEON_REPLY_TYPE_JS)


if __name__ == "__main__":
    unittest.main()
