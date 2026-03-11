"""
smoke_check_matcher.py - Quick sanity check for matcher.py outputs.

Assertions
----------
1. matched_raw.csv and match_report.json exist and are non-empty.
2. Required columns present in matched_raw.csv (including confidence).
3. match_source distribution is non-empty and contains at least one known tier.
4. confidence column present; worker_id rows have confidence == 1.0.

Run with:
    venv/Scripts/python.exe tests/smoke_check_matcher.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT  = ROOT / "outputs"

_REQUIRED_COLS = [
    "old_worker_id",
    "new_worker_id",
    "match_source",
    "confidence",
]

_KNOWN_TIERS = {"worker_id", "recon_id", "pk", "last4_dob", "dob_name", "name_hire_date"}


def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    print("=" * 60)
    print("  SMOKE CHECK: matcher outputs")
    print("=" * 60)

    matched_raw = OUT / "matched_raw.csv"
    match_report = OUT / "match_report.json"

    # ------------------------------------------------------------------
    # Assertion 1: files exist and are non-empty
    # ------------------------------------------------------------------
    for p in (matched_raw, match_report):
        if not p.exists():
            _fail(f"Assertion 1: {p.name} not found at {p}")
        if p.stat().st_size == 0:
            _fail(f"Assertion 1: {p.name} is empty")
    print("  [PASS] Assertion 1: matched_raw.csv and match_report.json exist")

    # ------------------------------------------------------------------
    # Assertion 2: required columns in matched_raw.csv
    # ------------------------------------------------------------------
    df = pd.read_csv(matched_raw, dtype="string")
    missing = [c for c in _REQUIRED_COLS if c not in df.columns]
    if missing:
        hint = ""
        if "confidence" in missing:
            hint = "\n  Tip: re-run 'venv/Scripts/python.exe -m src.matcher' to regenerate with confidence scores."
        _fail(f"Assertion 2: matched_raw.csv missing required columns: {missing}{hint}")
    print(f"  [PASS] Assertion 2: required columns present ({_REQUIRED_COLS})")

    # ------------------------------------------------------------------
    # Assertion 3: match_source distribution
    # ------------------------------------------------------------------
    if len(df) == 0:
        _fail("Assertion 3: matched_raw.csv has no data rows")
    sources = set(df["match_source"].dropna().str.strip().unique())
    if not sources:
        _fail("Assertion 3: match_source column is entirely blank")
    unknown = sources - _KNOWN_TIERS
    if unknown:
        print(f"  [warn] Assertion 3: unknown match_source values: {unknown}")
    print(f"  [PASS] Assertion 3: match_source distribution: {sources}")

    # ------------------------------------------------------------------
    # Assertion 4: confidence column - worker_id rows have 1.0
    # ------------------------------------------------------------------
    wid_rows = df[df["match_source"].str.strip().str.lower() == "worker_id"]
    if len(wid_rows) > 0:
        bad_conf = wid_rows[
            pd.to_numeric(wid_rows["confidence"], errors="coerce").fillna(-1).round(4) != 1.0
        ]
        if len(bad_conf) > 0:
            _fail(
                f"Assertion 4: {len(bad_conf)} worker_id rows have confidence != 1.0; "
                f"sample: {bad_conf[['old_worker_id', 'confidence']].head(3).to_dict()}"
            )
    non_wid = df[df["match_source"].str.strip().str.lower() != "worker_id"]
    if len(non_wid) > 0:
        bad_range = non_wid[
            pd.to_numeric(non_wid["confidence"], errors="coerce")
            .apply(lambda v: pd.isna(v) or not (0.0 <= v <= 1.0))
        ]
        if len(bad_range) > 0:
            _fail(
                f"Assertion 4: {len(bad_range)} non-worker_id rows have confidence out of [0,1] "
                f"or missing; sample: {bad_range[['old_worker_id', 'match_source', 'confidence']].head(3).to_dict()}"
            )
    print(
        f"  [PASS] Assertion 4: confidence in [0,1] for all rows "
        f"(worker_id={len(wid_rows):,}, other={len(non_wid):,})"
    )

    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")


if __name__ == "__main__":
    main()
