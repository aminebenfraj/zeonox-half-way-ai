"""Normalization applied to replies before review and sending."""

import re


# Chameleon occasionally uses punctuation that should not reach the approval
# queue or the platform reply box.  Include the common typographic apostrophe
# variants as well as the plain ASCII apostrophe requested by the operator.
_REMOVED_REPLY_PUNCTUATION = str.maketrans("", "", "'\u2018\u2019\u02bc\u2026\u2014")


def strip_disallowed_reply_punctuation(text: str) -> str:
    """Remove apostrophes, the ellipsis glyph, and em dashes from a reply.

    Removing an em dash can leave doubled spaces behind, so horizontal
    whitespace is normalized while line breaks are preserved.
    """
    cleaned = str(text or "").translate(_REMOVED_REPLY_PUNCTUATION)
    cleaned = re.sub(r"[^\S\r\n]+", " ", cleaned)
    return cleaned.strip()
