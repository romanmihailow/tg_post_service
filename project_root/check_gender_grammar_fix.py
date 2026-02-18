"""Self-check for gender grammar fix. Run: python -m project_root.check_gender_grammar_fix"""

from __future__ import annotations

from project_root.grammar_fix import fix_gender_grammar, VALID_GENDERS


def main() -> None:
    # Male cases: female forms -> male forms
    assert fix_gender_grammar("Не уверена.", "male") == ("Не уверен.", True)
    assert fix_gender_grammar("не согласна, но...", "male") == ("не согласен, но...", True)
    assert fix_gender_grammar("Права, конечно", "male") == ("Прав, конечно", True)
    assert fix_gender_grammar("согласна.", "male") == ("согласен.", True)
    assert fix_gender_grammar("готова помочь", "male") == ("готов помочь", True)

    # Female cases: male forms -> female forms
    assert fix_gender_grammar("Не уверен.", "female") == ("Не уверена.", True)
    assert fix_gender_grammar("согласен", "female") == ("согласна", True)
    assert fix_gender_grammar("Уверен, что да", "female") == ("Уверена, что да", True)
    assert fix_gender_grammar("Прав, конечно", "female") == ("Права, конечно", True)
    assert fix_gender_grammar("готов помочь", "female") == ("готова помочь", True)
    assert fix_gender_grammar("Не удивлён, сейчас сезон.", "female") == ("Не удивлена, сейчас сезон.", True)
    assert fix_gender_grammar("Не удивлена, это нормально.", "male") == ("Не удивлён, это нормально.", True)

    # Invalid gender: text unchanged, changed=False
    out, changed = fix_gender_grammar("Не уверена.", "unknown")
    assert out == "Не уверена."
    assert changed is False

    out, changed = fix_gender_grammar("согласна", "invalid")
    assert out == "согласна"
    assert changed is False

    # Empty: unchanged
    out, changed = fix_gender_grammar("", "male")
    assert out == ""
    assert changed is False

    out, changed = fix_gender_grammar("   ", "female")
    assert out == "   "
    assert changed is False

    # No change needed
    out, changed = fix_gender_grammar("Согласен.", "male")
    assert out == "Согласен."
    assert changed is False

    out, changed = fix_gender_grammar("Согласна.", "female")
    assert out == "Согласна."
    assert changed is False

    # Prefix limit (80 chars): only first 80 chars are processed
    long_text = "не уверена. " + "x" * 100
    out, changed = fix_gender_grammar(long_text, "male")
    assert out.startswith("не уверен")
    assert changed is True

    print("check_gender_grammar_fix: all asserts passed")


if __name__ == "__main__":
    main()
