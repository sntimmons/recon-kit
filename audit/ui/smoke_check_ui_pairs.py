"""
smoke_check_ui_pairs.py - Verify ui_pairs.csv exists and has expected structure.

Assertions
----------
1. audit/ui/ui_pairs.csv exists.
2. File has at least 1 data row.
3. All required columns are present (including ui_contract_version).
4. 'action' column contains only APPROVE or REVIEW values.
5. Mismatch boolean columns contain only True/False values.
6. ui_contract_version column is present and all sampled values equal 'v1'.

Extra fields (mm_*, old_<field>, new_<field>) are optional - the check does
NOT fail if they are absent, and does NOT fail if they are present.

Exits 0 on pass, 2 on fail.

Run:
    venv/Scripts/python.exe audit/ui/smoke_check_ui_pairs.py [--csv PATH]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

_HERE    = Path(__file__).resolve().parent   # audit/ui/
ROOT     = _HERE.parents[1]                  # repo root
CSV_PATH = _HERE / "ui_pairs.csv"

_REQUIRED_COLS = [
    # Contract marker
    "ui_contract_version",
    # Identifiers
    "pair_id", "match_source", "old_worker_id", "new_worker_id",
    # Gating
    "fix_types", "action", "reason", "confidence", "min_confidence",
    "priority_score", "summary",
    # Mismatch booleans
    "has_salary_mismatch", "has_payrate_mismatch", "has_status_mismatch",
    "has_hire_date_mismatch", "has_job_org_mismatch",
    # Side-by-side
    "old_salary", "new_salary",
    "old_hire_date", "new_hire_date",
    "old_worker_status", "new_worker_status",
]

_BOOL_COLS = [
    "has_salary_mismatch", "has_payrate_mismatch", "has_status_mismatch",
    "has_hire_date_mismatch", "has_job_org_mismatch",
]

_VALID_BOOL = {"true", "false", "True", "False", "TRUE", "FALSE", "1", "0"}


def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(2)


def _pass(msg: str) -> None:
    print(f"  [PASS] {msg}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Smoke-check ui_pairs.csv.")
    parser.add_argument("--csv", default=None, metavar="PATH",
                        help=f"Path to ui_pairs.csv (default: {CSV_PATH}).")
    args = parser.parse_args(argv)

    csv_path = Path(args.csv) if args.csv else CSV_PATH

    print("=" * 60)
    print("  SMOKE CHECK: UI Pairs")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Assertion 1: file exists
    # ------------------------------------------------------------------
    if not csv_path.exists():
        _fail(
            f"Assertion 1: {csv_path} not found.\n"
            "  Run: venv/Scripts/python.exe audit/ui/build_ui_pairs.py"
        )
    _pass(f"Assertion 1: file exists: {csv_path.relative_to(ROOT)}")

    # ------------------------------------------------------------------
    # Assertion 2: at least 1 data row
    # ------------------------------------------------------------------
    df = pd.read_csv(str(csv_path), dtype=str, keep_default_na=False)
    if len(df) == 0:
        _fail("Assertion 2: ui_pairs.csv has 0 rows.")
    _pass(f"Assertion 2: {len(df):,} rows present.")

    # ------------------------------------------------------------------
    # Assertion 3: required columns present
    # ------------------------------------------------------------------
    cols = set(df.columns)
    missing = [c for c in _REQUIRED_COLS if c not in cols]
    if missing:
        _fail(f"Assertion 3: missing required columns: {sorted(missing)}")
    _pass(f"Assertion 3: all {len(_REQUIRED_COLS)} required columns present.")

    # Report optional extra fields found (informational only - not a failure)
    mm_cols_found   = [c for c in df.columns if c.startswith("mm_")]
    extra_old_found = [c for c in df.columns if c.startswith("old_") and c not in cols]
    if mm_cols_found:
        print(f"  [INFO] extra mm_ columns present: {mm_cols_found}")

    # ------------------------------------------------------------------
    # Assertion 4: action column values
    # ------------------------------------------------------------------
    bad_actions = df.loc[~df["action"].isin(["APPROVE", "REVIEW"]), "action"].unique().tolist()
    if bad_actions:
        _fail(f"Assertion 4: unexpected values in 'action' column: {bad_actions[:10]}")
    n_approve = int((df["action"] == "APPROVE").sum())
    n_review  = int((df["action"] == "REVIEW").sum())
    _pass(f"Assertion 4: action values OK  (APPROVE={n_approve:,}, REVIEW={n_review:,}).")

    # ------------------------------------------------------------------
    # Assertion 5: mismatch boolean columns
    # ------------------------------------------------------------------
    for col in _BOOL_COLS:
        if col not in cols:
            continue
        bad = df.loc[~df[col].str.strip().isin(_VALID_BOOL), col].unique().tolist()
        if bad:
            _fail(f"Assertion 5: non-boolean values in '{col}': {bad[:5]}")
    _pass("Assertion 5: mismatch boolean columns contain valid values.")

    # Also validate any mm_<field> columns if present - same boolean rule
    for col in mm_cols_found:
        if col not in df.columns:
            continue
        bad = df.loc[~df[col].str.strip().isin(_VALID_BOOL), col].unique().tolist()
        if bad:
            _fail(f"Assertion 5 (extra): non-boolean values in '{col}': {bad[:5]}")

    # ------------------------------------------------------------------
    # Assertion 6: ui_contract_version = 'v1' in first 10 rows (and all rows)
    # ------------------------------------------------------------------
    if "ui_contract_version" not in cols:
        _fail("Assertion 6: 'ui_contract_version' column missing.")

    # Sample first 10 rows, then check the entire file
    sample = df["ui_contract_version"].head(10)
    bad_sample = sample[sample != "v1"].tolist()
    if bad_sample:
        _fail(f"Assertion 6: unexpected ui_contract_version values in first 10 rows: {bad_sample}")

    # Full-file check
    bad_versions = df.loc[df["ui_contract_version"] != "v1", "ui_contract_version"].unique().tolist()
    if bad_versions:
        _fail(f"Assertion 6: unexpected ui_contract_version values: {bad_versions[:5]}")

    _pass("Assertion 6: ui_contract_version is 'v1' in all rows.")

    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()
