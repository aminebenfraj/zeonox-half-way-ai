"""Normalization applied to replies before review and sending."""

import re


# Chameleon occasionally uses punctuation that should not reach the approval
# queue or the platform reply box. Include the common typographic apostrophe
# and dash variants as well as the plain ASCII forms requested by the operator.
_REMOVED_REPLY_PUNCTUATION = str.maketrans(
    "",
    "",
    "'\u2018\u2019\u02bc\u2026-\u2010\u2011\u2012\u2013\u2014\u2015\u2212",
)


def strip_disallowed_reply_punctuation(text: str) -> str:
    """Remove apostrophes, the ellipsis glyph, and dash variants from a reply.

    Removing a dash can leave doubled spaces behind, so horizontal
    whitespace is normalized while line breaks are preserved.
    """
    cleaned = str(text or "").translate(_REMOVED_REPLY_PUNCTUATION)
    cleaned = re.sub(r"[^\S\r\n]+", " ", cleaned)
    return cleaned.strip()
