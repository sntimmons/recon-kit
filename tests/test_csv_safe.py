"""
tests/test_csv_safe.py - Proof-of-coverage for CSV injection prevention.

Run:
    python -m pytest tests/test_csv_safe.py -v
    # or directly:
    python tests/test_csv_safe.py
"""
from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from csv_safe import sanitize_csv_value, sanitize_row, sanitize_dict_row, safe_to_csv


# ---------------------------------------------------------------------------
# sanitize_csv_value
# ---------------------------------------------------------------------------

def test_formula_equals():
    assert sanitize_csv_value("=SUM(A1:A5)") == "\t=SUM(A1:A5)"

def test_formula_plus():
    assert sanitize_csv_value("+1234DANGEROUS") == "\t+1234DANGEROUS"

def test_formula_at():
    assert sanitize_csv_value("@SUM") == "\t@SUM"

def test_formula_minus_non_numeric():
    assert sanitize_csv_value("-HYPERLINK(\"http://evil.com\")") == "\t-HYPERLINK(\"http://evil.com\")"

def test_numeric_minus_passes_through():
    # Salary value "-5000" must NOT be prefixed — it is a number
    assert sanitize_csv_value("-5000") == "-5000"

def test_numeric_plus_passes_through():
    assert sanitize_csv_value("+3.14") == "+3.14"

def test_numeric_with_commas_passes_through():
    assert sanitize_csv_value("-1,234.56") == "-1,234.56"

def test_leading_space_formula():
    # Leading space then formula start → still dangerous
    assert sanitize_csv_value("  =EVIL()") == "\t  =EVIL()"

def test_leading_space_numeric_passes():
    # Leading space + numeric — the stripped form is numeric, safe
    assert sanitize_csv_value("  -5000") == "  -5000"

def test_plain_text_unchanged():
    assert sanitize_csv_value("John Smith") == "John Smith"

def test_empty_string_unchanged():
    assert sanitize_csv_value("") == ""

def test_none_unchanged():
    assert sanitize_csv_value(None) is None

def test_int_unchanged():
    assert sanitize_csv_value(42) == 42

def test_float_unchanged():
    assert sanitize_csv_value(3.14) == 3.14


# ---------------------------------------------------------------------------
# sanitize_row
# ---------------------------------------------------------------------------

def test_sanitize_row_mixed():
    row = ["normal", "=EVIL()", -5000, "+HYPERLINK()"]
    result = sanitize_row(row)
    assert result[0] == "normal"
    assert result[1] == "\t=EVIL()"
    assert result[2] == -5000       # int — unchanged
    assert result[3] == "\t+HYPERLINK()"

def test_sanitize_row_returns_list():
    assert isinstance(sanitize_row(("a", "b")), list)


# ---------------------------------------------------------------------------
# sanitize_dict_row
# ---------------------------------------------------------------------------

def test_sanitize_dict_row():
    row = {"name": "=SUM()", "salary": "-5000", "dept": "HR"}
    result = sanitize_dict_row(row)
    assert result["name"] == "\t=SUM()"
    assert result["salary"] == "-5000"   # numeric string — safe
    assert result["dept"] == "HR"


# ---------------------------------------------------------------------------
# safe_to_csv (via csv.reader round-trip)
# ---------------------------------------------------------------------------

def test_safe_to_csv_sanitizes(tmp_path):
    try:
        import pandas as pd
    except ImportError:
        print("SKIP: pandas not installed")
        return

    df = pd.DataFrame({
        "name":   ["Alice", "=EVIL()", "+HYPERLINK()"],
        "salary": [50000, "-5000", "+3.14"],
        "dept":   ["HR", "@sales", "Eng"],
    })

    out = tmp_path / "out.csv"
    safe_to_csv(df, out)

    rows = list(csv.DictReader(out.open(encoding="utf-8")))

    assert rows[0]["name"] == "Alice"
    assert rows[1]["name"] == "\t=EVIL()"
    assert rows[2]["name"] == "\t+HYPERLINK()"

    # Numeric strings preserved
    assert rows[0]["salary"] == "50000"
    assert rows[1]["salary"] == "-5000"
    assert rows[2]["salary"] == "+3.14"

    # @ prefix injected
    assert rows[1]["dept"] == "\t@sales"


# ---------------------------------------------------------------------------
# Before / after examples (printed when run directly)
# ---------------------------------------------------------------------------

_EXAMPLES = [
    ("=SUM(A1:A5)",            "\\t=SUM(A1:A5)       [formula neutralised]"),
    ("+1234EVIL",               "\\t+1234EVIL          [formula neutralised]"),
    ("-5000",                   "-5000               [numeric — unchanged]"),
    ("+3.14",                   "+3.14               [numeric — unchanged]"),
    ("-1,234.56",               "-1,234.56           [numeric — unchanged]"),
    ("@SUM",                    "\\t@SUM               [formula neutralised]"),
    ("  =EVIL()",               "\\t  =EVIL()          [leading-space formula neutralised]"),
    ("  -5000",                 "  -5000             [leading-space numeric — unchanged]"),
    ("John Smith",              "John Smith          [plain text — unchanged]"),
]

if __name__ == "__main__":
    import traceback

    tests = [
        test_formula_equals, test_formula_plus, test_formula_at,
        test_formula_minus_non_numeric, test_numeric_minus_passes_through,
        test_numeric_plus_passes_through, test_numeric_with_commas_passes_through,
        test_leading_space_formula, test_leading_space_numeric_passes,
        test_plain_text_unchanged, test_empty_string_unchanged,
        test_none_unchanged, test_int_unchanged, test_float_unchanged,
        test_sanitize_row_mixed, test_sanitize_row_returns_list,
        test_sanitize_dict_row,
    ]

    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except Exception:
            print(f"  FAIL  {t.__name__}")
            traceback.print_exc()
            failed += 1

    # safe_to_csv needs tmp dir
    import tempfile, pathlib
    with tempfile.TemporaryDirectory() as td:
        test_safe_to_csv_sanitizes(pathlib.Path(td))
        print("  PASS  test_safe_to_csv_sanitizes")
        passed += 1

    print(f"\n{passed} passed, {failed} failed\n")

    print("Before / After examples")
    print("-" * 60)
    print(f"  {'INPUT':<30}  OUTPUT")
    print(f"  {'-'*28}  {'-'*28}")
    for inp, desc in _EXAMPLES:
        out = sanitize_csv_value(inp)
        out_repr = repr(out).replace("\\t", "\\t")
        print(f"  {inp!r:<30}  {out_repr}")
