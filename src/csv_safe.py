"""
csv_safe.py — CSV injection prevention helpers.

Drop-in replacements for every CSV write in both engines:
  • safe_to_csv()       replaces df.to_csv()
  • sanitize_row()      wraps csv.writer.writerow() lists
  • sanitize_dict_row() wraps csv.DictWriter.writerow() dicts
  • sanitize_csv_value() for one-off use

Background
----------
Excel, Google Sheets, and LibreOffice Calc execute cell content as a formula
when it begins with =, +, -, or @.  An attacker who controls a field value in
source HR data can embed =HYPERLINK("http://evil.com","click") in e.g. a job
title; when the correction CSV is opened, the link is live.

Fix: prefix any string whose first non-space character is =, +, -, or @ with a
tab (\t).  Spreadsheet apps treat tab-prefixed cells as plain text.

Numeric strings are passed through unchanged so downstream consumers that
re-parse the CSV obtain the correct number (e.g. salary "-5000" stays "-5000").
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

# Characters that trigger formula execution in spreadsheet applications.
_FORMULA_STARTS: frozenset[str] = frozenset(("=", "+", "-", "@"))


def sanitize_csv_value(value: object) -> object:
    """Return a CSV-injection-safe version of *value*.

    Rules
    -----
    * Non-string values (int, float, None, bool, …) are returned unchanged.
    * Empty strings are returned unchanged.
    * A string whose first non-space character is =, +, -, or @ is prefixed
      with \\t unless it is a valid numeric string.
    * Leading spaces are preserved in the output (only used for the check).
    """
    if not isinstance(value, str) or not value:
        return value

    stripped = value.lstrip(" ")
    if not stripped or stripped[0] not in _FORMULA_STARTS:
        return value

    # Numeric strings (e.g. "-5000", "+3.14", "-1,234.56") are safe — they are
    # numbers, not formulas.  Pass them through so CSV consumers see the number.
    try:
        float(stripped.replace(",", ""))
        return value
    except ValueError:
        pass

    return "\t" + value


def sanitize_row(row: list | tuple) -> list:
    """Return a new list with every element sanitized (for csv.writer.writerow)."""
    return [sanitize_csv_value(v) for v in row]


def sanitize_dict_row(row: dict) -> dict:
    """Return a new dict with every value sanitized (for csv.DictWriter.writerow)."""
    return {k: sanitize_csv_value(v) for k, v in row.items()}


def sanitize_dataframe(df: "pd.DataFrame") -> "pd.DataFrame":
    """Return a copy of *df* with all object-dtype columns sanitized."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(
                lambda v: sanitize_csv_value(v) if isinstance(v, str) else v
            )
    return df


def safe_to_csv(df: "pd.DataFrame", path: object, **kwargs: object) -> None:
    """Sanitize *df* then write to CSV.

    Drop-in for ``df.to_csv(path, index=False)``.  Any keyword arguments
    accepted by ``DataFrame.to_csv`` are forwarded unchanged.
    """
    kwargs.setdefault("index", False)
    sanitize_dataframe(df).to_csv(path, **kwargs)
