"""
smoke_check_mapping.py — Quick sanity check for mapping pipeline outputs.

Assertions
----------
1. mapped_old.csv and mapped_new.csv exist and are non-empty.
2. Required normalized columns are present in both files.
3. No completely blank worker_id column (would indicate mapping failure).

Run with:
    venv/Scripts/python.exe tests/smoke_check_mapping.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT  = ROOT / "outputs"

# Columns that mapping.py must produce on both sides.
_REQUIRED_COLS = [
    "worker_id",
    "full_name_norm",
    "dob",
    "hire_date",
    "salary",
    "worker_status",
]


def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    print("=" * 60)
    print("  SMOKE CHECK: mapping outputs")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Assertion 1: files exist and are non-empty
    # ------------------------------------------------------------------
    for fname in ("mapped_old.csv", "mapped_new.csv"):
        p = OUT / fname
        if not p.exists():
            _fail(f"Assertion 1: {fname} not found at {p}")
        if p.stat().st_size == 0:
            _fail(f"Assertion 1: {fname} is empty")
    print("  [PASS] Assertion 1: mapped_old.csv and mapped_new.csv exist and are non-empty")

    # ------------------------------------------------------------------
    # Assertion 2: required normalized columns present on both sides
    # ------------------------------------------------------------------
    for fname in ("mapped_old.csv", "mapped_new.csv"):
        df = pd.read_csv(OUT / fname, dtype="string", nrows=5)
        missing = [c for c in _REQUIRED_COLS if c not in df.columns]
        if missing:
            _fail(f"Assertion 2: {fname} missing required columns: {missing}")
    print(f"  [PASS] Assertion 2: all required columns present ({_REQUIRED_COLS})")

    # ------------------------------------------------------------------
    # Assertion 3: worker_id column is not entirely blank
    # ------------------------------------------------------------------
    for fname in ("mapped_old.csv", "mapped_new.csv"):
        df = pd.read_csv(OUT / fname, dtype="string")
        n_non_blank = df["worker_id"].dropna().str.strip().replace("", pd.NA).dropna().shape[0]
        if n_non_blank == 0:
            _fail(f"Assertion 3: {fname} has no non-blank worker_id values")
    print("  [PASS] Assertion 3: worker_id column has non-blank values in both files")

    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")


if __name__ == "__main__":
    main()
