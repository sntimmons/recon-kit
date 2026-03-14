"""
smoke_check_exports.py - Verifies DIY XLOOKUP export outputs.

Assertions
----------
1. build_diy_exports runs without error (exit 0).
2. Output files exist (xlookup_keys.csv, wide_compare.csv).
3. All required stable headers are present in each output file.
   Extra columns (mm_*, old_<extra>, new_<extra>) are allowed - exact match
   is NOT required so that extra_fields additions don't break this check.
4. wide_compare row count == matched_pairs count from DB.

Run:
    venv/Scripts/python.exe audit/exports/smoke_check_exports.py
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

_HERE   = Path(__file__).resolve().parent    # audit/exports/
ROOT    = _HERE.parents[1]
DB_PATH = ROOT / "audit" / "audit.db"
OUT_DIR = _HERE / "out"

EXPECTED_FILES = ["xlookup_keys.csv", "wide_compare.csv"]

# Required stable headers - extra columns may appear after these.
# Use a list to preserve intended order for documentation, but check as a set.
REQUIRED_HEADERS = {
    "xlookup_keys.csv": [
        "pair_id", "match_source", "confidence", "match_key",
        "old_worker_id", "new_worker_id",
        "old_recon_id", "new_recon_id",
        "old_full_name_norm", "new_full_name_norm",
    ],
    "wide_compare.csv": [
        "pair_id", "match_source", "confidence",
        "action", "reason", "fix_types", "summary", "priority_score",
        "old_full_name_norm", "new_full_name_norm",
        "old_worker_status", "new_worker_status",
        "old_worker_type", "new_worker_type",
        "old_hire_date", "new_hire_date",
        "old_position", "new_position",
        "old_district", "new_district",
        "old_location_state", "new_location_state",
        "old_location", "new_location",
        "old_salary", "new_salary",
        "old_payrate", "new_payrate",
        "salary_delta", "salary_ratio", "payrate_delta",
        "status_changed", "hire_date_changed", "job_org_changed",
        "needs_review", "suggested_action",
    ],
}


def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(1)


def _pass(msg: str) -> None:
    print(f"  [PASS] {msg}")


def main() -> None:
    print("=" * 60)
    print("  SMOKE CHECK: DIY XLOOKUP Exports")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Assertion 1: build_diy_exports runs without error
    # ------------------------------------------------------------------
    print("\n  Running build_diy_exports.main() ...")
    sys.path.insert(0, str(_HERE))
    import build_diy_exports
    try:
        build_diy_exports.main(argv=[])
    except SystemExit as exc:
        if exc.code != 0:
            _fail(f"Assertion 1 FAILED: build_diy_exports.main() exited with code {exc.code}")
    print()
    _pass("Assertion 1: build_diy_exports runs without error")

    # ------------------------------------------------------------------
    # Assertion 2: Output files exist
    # ------------------------------------------------------------------
    missing_files = [f for f in EXPECTED_FILES if not (OUT_DIR / f).exists()]
    if missing_files:
        _fail(f"Assertion 2 FAILED: missing output files: {missing_files}")
    _pass(f"Assertion 2: all {len(EXPECTED_FILES)} output files exist")

    # ------------------------------------------------------------------
    # Assertion 3: Required headers present (subset check - extras allowed)
    # ------------------------------------------------------------------
    header_failures: list[str] = []
    for fname, required_cols in REQUIRED_HEADERS.items():
        df = pd.read_csv(str(OUT_DIR / fname), nrows=0)
        actual_set   = set(df.columns)
        missing_cols = [c for c in required_cols if c not in actual_set]
        if missing_cols:
            header_failures.append(
                f"{fname}: missing required columns: {missing_cols}"
            )
        else:
            # Informational: report any extra columns present
            extra_cols = [c for c in df.columns if c not in set(required_cols)]
            if extra_cols:
                print(f"  [INFO] {fname}: extra columns present: {extra_cols}")

    if header_failures:
        _fail("Assertion 3 FAILED: missing required headers:\n  " + "\n  ".join(header_failures))
    _pass(f"Assertion 3: all required headers present in {len(REQUIRED_HEADERS)} files")

    # ------------------------------------------------------------------
    # Assertion 4: wide_compare row count == matched_pairs count from DB
    # ------------------------------------------------------------------
    if not DB_PATH.exists():
        print("  [SKIP] Assertion 4: audit.db not found")
    else:
        con = sqlite3.connect(str(DB_PATH))
        try:
            (expected_count,) = con.execute("SELECT COUNT(*) FROM matched_pairs").fetchone()
        finally:
            con.close()

        wide_df = pd.read_csv(str(OUT_DIR / "wide_compare.csv"))
        actual_count = len(wide_df)

        if actual_count != expected_count:
            _fail(
                f"Assertion 4 FAILED: wide_compare has {actual_count:,} rows "
                f"but matched_pairs has {expected_count:,} rows"
            )
        _pass(f"Assertion 4: wide_compare rows ({actual_count:,}) == matched_pairs rows ({expected_count:,})")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n  File size summary:")
    for fname in EXPECTED_FILES:
        df = pd.read_csv(str(OUT_DIR / fname))
        print(f"    {fname:<25}  {len(df):>8,} rows  ({len(df.columns)} cols)")

    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")


if __name__ == "__main__":
    main()
