"""
smoke_check_schema.py — Validates schema_validator logic (no live DB required).

Assertions
----------
1. validate_schema passes on a test DB with all required columns.
2. validate_schema fails (exit 2) when a required column is missing.
3. validate_schema fails (exit 2) when the DB file does not exist.

Note: Assertion 1 uses an in-memory test DB, not the live audit.db, so this
check passes at any pipeline stage.  Run the schema_validator directly against
the live DB with:
    venv/Scripts/python.exe audit/schema_validator.py

Run with:
    venv/Scripts/python.exe audit/smoke_check_schema.py
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

_HERE = Path(__file__).resolve().parent
ROOT  = _HERE.parent

sys.path.insert(0, str(_HERE))

from schema_validator import validate_schema, REQUIRED_COLS


def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(1)


def _make_test_db(columns: list[str]) -> Path:
    """Create a temporary SQLite DB with matched_pairs view containing given columns."""
    tf = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tf.close()
    path = Path(tf.name)
    col_sql = ", ".join(f'"{c}" TEXT' for c in columns)
    con = sqlite3.connect(str(path))
    con.execute(f"CREATE TABLE matched_pairs_raw ({col_sql});")
    con.execute("CREATE VIEW matched_pairs AS SELECT * FROM matched_pairs_raw;")
    con.commit()
    con.close()
    return path


def main() -> None:
    print("=" * 60)
    print("  SMOKE CHECK: schema_validator")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Assertion 1: passes on a test DB with all required columns
    # ------------------------------------------------------------------
    extra_cols = ["old_full_name_norm", "new_full_name_norm", "old_dob", "new_dob"]
    all_cols = list(dict.fromkeys(REQUIRED_COLS + extra_cols))  # dedup, preserve order
    good_db = _make_test_db(all_cols)
    try:
        exit_code = None
        try:
            validate_schema(good_db)
            exit_code = 0
        except SystemExit as e:
            exit_code = e.code
        if exit_code != 0:
            _fail(f"Assertion 1: expected exit 0 for complete schema, got {exit_code}")
    finally:
        good_db.unlink(missing_ok=True)
    print(f"  [PASS] Assertion 1: validate_schema passes on complete schema ({len(all_cols)} cols)")

    # ------------------------------------------------------------------
    # Assertion 2: fails with exit 2 when a required column is missing
    # ------------------------------------------------------------------
    incomplete_cols = [c for c in REQUIRED_COLS if c != "confidence"]
    bad_db = _make_test_db(incomplete_cols)
    try:
        exit_code = None
        try:
            validate_schema(bad_db)
            exit_code = 0
        except SystemExit as e:
            exit_code = e.code
        if exit_code != 2:
            _fail(f"Assertion 2: expected exit 2 for missing column, got {exit_code}")
    finally:
        bad_db.unlink(missing_ok=True)
    print(f"  [PASS] Assertion 2: validate_schema exits 2 when 'confidence' column is missing")

    # ------------------------------------------------------------------
    # Assertion 3: fails with exit 2 when DB file does not exist
    # ------------------------------------------------------------------
    nonexistent = Path("/tmp/definitely_does_not_exist_recon.db")
    exit_code = None
    try:
        validate_schema(nonexistent)
        exit_code = 0
    except SystemExit as e:
        exit_code = e.code
    if exit_code != 2:
        _fail(f"Assertion 3: expected exit 2 for missing DB file, got {exit_code}")
    print(f"  [PASS] Assertion 3: validate_schema exits 2 when DB file does not exist")

    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")
    print(f"\n  Tip: validate live DB with: venv/Scripts/python.exe audit/schema_validator.py")


if __name__ == "__main__":
    main()
