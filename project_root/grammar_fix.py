"""Gender grammar fix: replace forms by persona gender.
Female: согласен→согласна, уверен→уверена, etc.
Male: согласна→согласен, уверена→уверен, etc.
Used by Pipeline 1 and Pipeline 2. No DB/Telegram dependencies."""

from __future__ import annotations

import re
from typing import Tuple

VALID_GENDERS = frozenset({"male", "female"})
PREFIX_CHARS = 80

# Female context: male→female forms. Order: "не …", "бы …" first, then single words.
_FEMALE_REPLACEMENTS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bне\s+согласен\b"), "не согласна"),
    (re.compile(r"\bНе\s+согласен\b"), "Не согласна"),
    (re.compile(r"\bне\s+уверен\b"), "не уверена"),
    (re.compile(r"\bНе\s+уверен\b"), "Не уверена"),
    (re.compile(r"\bне\s+удивлён\b"), "не удивлена"),
    (re.compile(r"\bНе\s+удивлён\b"), "Не удивлена"),
    (re.compile(r"\bне\s+удивлен\b"), "не удивлена"),
    (re.compile(r"\bНе\s+удивлен\b"), "Не удивлена"),
    (re.compile(r"\bбы\s+сказал\b"), "бы сказала"),
    (re.compile(r"\bБы\s+сказал\b"), "Бы сказала"),
    (re.compile(r"\bбы\s+уточнил\b"), "бы уточнила"),
    (re.compile(r"\bбы\s+поспорил\b"), "бы поспорила"),
    (re.compile(r"\bбы\s+добавил\b"), "бы добавила"),
    (re.compile(r"\bбы\s+отметил\b"), "бы отметила"),
    (re.compile(r"\bбы\s+подумал\b"), "бы подумала"),
    (re.compile(r"\bбы\s+считал\b"), "бы считала"),
    (re.compile(r"\bбы\s+хотел\b"), "бы хотела"),
    (re.compile(r"\bбы\s+сделал\b"), "бы сделала"),
    (re.compile(r"\bбы\s+решил\b"), "бы решила"),
    (re.compile(r"\bбы\s+написал\b"), "бы написала"),
    (re.compile(r"\bбы\s+ответил\b"), "бы ответила"),
    (re.compile(r"\bбы\s+согласился\b"), "бы согласилась"),
    (re.compile(r"\bбы\s+думал\b"), "бы думала"),
    (re.compile(r"\bСказал\s+бы\b"), "Сказала бы"),
    (re.compile(r"\bсказал\s+бы\b"), "сказала бы"),
    (re.compile(r"\bУточнил\s+бы\b"), "Уточнила бы"),
    (re.compile(r"\bПоспорил\s+бы\b"), "Поспорила бы"),
    (re.compile(r"\bДобавил\s+бы\b"), "Добавила бы"),
    (re.compile(r"\bПодумал\s+бы\b"), "Подумала бы"),
    (re.compile(r"\bсогласен\b"), "согласна"),
    (re.compile(r"\bСогласен\b"), "Согласна"),
    (re.compile(r"\bуверен\b"), "уверена"),
    (re.compile(r"\bУверен\b"), "Уверена"),
    (re.compile(r"\bудивлён\b"), "удивлена"),
    (re.compile(r"\bУдивлён\b"), "Удивлена"),
    (re.compile(r"\bудивлен\b"), "удивлена"),
    (re.compile(r"\bУдивлен\b"), "Удивлена"),
    (re.compile(r"\bготов\b"), "готова"),
    (re.compile(r"\bГотов\b"), "Готова"),
    (re.compile(r"\bправ\b"), "права"),
    (re.compile(r"\bПрав\b"), "Права"),
]

# Male context: female→male forms. Order: "не …", "бы …" first, then single words.
_MALE_REPLACEMENTS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bне\s+согласна\b"), "не согласен"),
    (re.compile(r"\bНе\s+согласна\b"), "Не согласен"),
    (re.compile(r"\bне\s+уверена\b"), "не уверен"),
    (re.compile(r"\bНе\s+уверена\b"), "Не уверен"),
    (re.compile(r"\bне\s+удивлена\b"), "не удивлён"),
    (re.compile(r"\bНе\s+удивлена\b"), "Не удивлён"),
    (re.compile(r"\bбы\s+сказала\b"), "бы сказал"),
    (re.compile(r"\bбы\s+уточнила\b"), "бы уточнил"),
    (re.compile(r"\bбы\s+поспорила\b"), "бы поспорил"),
    (re.compile(r"\bбы\s+добавила\b"), "бы добавил"),
    (re.compile(r"\bбы\s+отметила\b"), "бы отметил"),
    (re.compile(r"\bбы\s+подумала\b"), "бы подумал"),
    (re.compile(r"\bбы\s+считала\b"), "бы считал"),
    (re.compile(r"\bбы\s+хотела\b"), "бы хотел"),
    (re.compile(r"\bбы\s+сделала\b"), "бы сделал"),
    (re.compile(r"\bбы\s+решила\b"), "бы решил"),
    (re.compile(r"\bбы\s+написала\b"), "бы написал"),
    (re.compile(r"\bбы\s+ответила\b"), "бы ответил"),
    (re.compile(r"\bбы\s+согласилась\b"), "бы согласился"),
    (re.compile(r"\bбы\s+думала\b"), "бы думал"),
    (re.compile(r"\bСказала\s+бы\b"), "Сказал бы"),
    (re.compile(r"\bсказала\s+бы\b"), "сказал бы"),
    (re.compile(r"\bУточнила\s+бы\b"), "Уточнил бы"),
    (re.compile(r"\bПоспорила\s+бы\b"), "Поспорил бы"),
    (re.compile(r"\bДобавила\s+бы\b"), "Добавил бы"),
    (re.compile(r"\bПодумала\s+бы\b"), "Подумал бы"),
    (re.compile(r"\bсогласна\b"), "согласен"),
    (re.compile(r"\bСогласна\b"), "Согласен"),
    (re.compile(r"\bуверена\b"), "уверен"),
    (re.compile(r"\bУверена\b"), "Уверен"),
    (re.compile(r"\bудивлена\b"), "удивлён"),
    (re.compile(r"\bУдивлена\b"), "Удивлён"),
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
