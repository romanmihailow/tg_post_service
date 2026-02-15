"""Gender grammar fix: replace forms by persona gender.
Female: согласен→согласна, уверен→уверена, etc.
Male: согласна→согласен, уверена→уверен, etc.
Used by Pipeline 1 and Pipeline 2. No DB/Telegram dependencies."""

from __future__ import annotations

import re
from typing import Tuple

VALID_GENDERS = frozenset({"male", "female"})
PREFIX_CHARS = 80

# Female context: male→female forms. Order: "не …" first, then single words.
_FEMALE_REPLACEMENTS: list[tuple[re.Pattern[str], str]] = [
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

# Male context: female→male forms. Order: "не …" first, then single words.
_MALE_REPLACEMENTS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bне\s+согласна\b"), "не согласен"),
    (re.compile(r"\bНе\s+согласна\b"), "Не согласен"),
    (re.compile(r"\bне\s+уверена\b"), "не уверен"),
    (re.compile(r"\bНе\s+уверена\b"), "Не уверен"),
    (re.compile(r"\bсогласна\b"), "согласен"),
    (re.compile(r"\bСогласна\b"), "Согласен"),
    (re.compile(r"\bуверена\b"), "уверен"),
    (re.compile(r"\bУверена\b"), "Уверен"),
    (re.compile(r"\bготова\b"), "готов"),
    (re.compile(r"\bГотова\b"), "Готов"),
    (re.compile(r"\bправа\b"), "прав"),
    (re.compile(r"\bПрава\b"), "Прав"),
]


def fix_gender_grammar(
    text: str,
    gender: str,
    prefix_chars: int = PREFIX_CHARS,
) -> Tuple[str, bool]:
    """Apply gender grammar fix to text prefix. Returns (new_text, changed).

    - Empty text: return as-is, changed=False.
    - Invalid gender: return as-is, changed=False (do not auto-fix).
    - gender=female: apply FEMALE_REPLACEMENTS (male→female) to prefix only.
    - gender=male: apply MALE_REPLACEMENTS (female→male) to prefix only.
    """
    if not text or not text.strip():
        return (text, False)

    if gender not in VALID_GENDERS:
        return (text, False)

    if len(text) <= prefix_chars:
        prefix, rest = text, ""
    else:
        prefix, rest = text[:prefix_chars], text[prefix_chars:]

    replacements = _FEMALE_REPLACEMENTS if gender == "female" else _MALE_REPLACEMENTS
    result_prefix = prefix
    for pattern, replacement in replacements:
        result_prefix = pattern.sub(replacement, result_prefix)

    new_text = result_prefix + rest
    changed = new_text != text
    return (new_text, changed)
