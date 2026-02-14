"""Female grammar fix: replace male forms (согласен, уверен, etc.) with female forms.
Used by Pipeline 1 and Pipeline 2. No DB/Telegram dependencies."""
from __future__ import annotations

import re

# Order: "не согласен"/"не уверен" FIRST (before single words). \s+ for multi-space/newline.
_FEMALE_GRAMMAR_REPLACEMENTS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bне\s+согласен\b"), "не согласна"),
    (re.compile(r"\bНе\s+согласен\b"), "Не согласна"),
    (re.compile(r"\bне\s+уверен\b"), "не уверена"),
    (re.compile(r"\bНе\s+уверен\b"), "Не уверена"),
    (re.compile(r"\bсогласен\b"), "согласна"),
    (re.compile(r"\bСогласен\b"), "Согласна"),
    (re.compile(r"\bуверен\b"), "уверена"),
    (re.compile(r"\bУверен\b"), "Уверена"),
    (re.compile(r"\bготов\b"), "готова"),
    (re.compile(r"\bГотов\b"), "Готова"),
    (re.compile(r"\bправ\b"), "права"),
    (re.compile(r"\bПрав\b"), "Права"),
]

FEMALE_GRAMMAR_FIX_PREFIX_CHARS = 80


def fix_female_grammar_in_reply(text: str, prefix_chars: int = FEMALE_GRAMMAR_FIX_PREFIX_CHARS) -> str:
    """Replace male forms with female forms (согласен→согласна, etc.).
    Applies only to the first prefix_chars to avoid corrupting quotes later.
    Uses word boundaries to avoid corrupting parts of words (e.g. 'прав' in 'справедливо')."""
    if not text or not text.strip():
        return text
    n = prefix_chars
    if len(text) <= n:
        prefix, rest = text, ""
    else:
        prefix, rest = text[:n], text[n:]
    result_prefix = prefix
    for pattern, replacement in _FEMALE_GRAMMAR_REPLACEMENTS:
        result_prefix = pattern.sub(replacement, result_prefix)
    return result_prefix + rest
