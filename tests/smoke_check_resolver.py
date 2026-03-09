"""
smoke_check_resolver.py — Quick sanity check for resolve_matched_raw.py outputs.

Assertions
----------
1. matched_raw.csv exists (resolve overwrites it in-place).
2. No duplicate non-blank old_worker_id values (1-to-1 guarantee).
3. No duplicate non-blank new_worker_id values (1-to-1 guarantee).
4. pair_id column present and has no duplicates.

Run with:
    venv/Scripts/python.exe tests/smoke_check_resolver.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MATCHED_RAW = ROOT / "outputs" / "matched_raw.csv"


def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    print("=" * 60)
    print("  SMOKE CHECK: resolver outputs")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Assertion 1: matched_raw.csv exists
    # ------------------------------------------------------------------
    if not MATCHED_RAW.exists():
        _fail(f"Assertion 1: matched_raw.csv not found at {MATCHED_RAW}")
    if MATCHED_RAW.stat().st_size == 0:
        _fail("Assertion 1: matched_raw.csv is empty")
    print("  [PASS] Assertion 1: matched_raw.csv exists")

    df = pd.read_csv(MATCHED_RAW, dtype="string")

    def _non_blank(series: pd.Series) -> pd.Series:
        return series.dropna().str.strip().replace("", pd.NA).dropna()

    # ------------------------------------------------------------------
    # Assertion 2: no duplicate non-blank old_worker_id
    # ------------------------------------------------------------------
    if "old_worker_id" not in df.columns:
        _fail("Assertion 2: old_worker_id column missing from matched_raw.csv")
    old_ids = _non_blank(df["old_worker_id"])
    dups = old_ids[old_ids.duplicated(keep=False)]
    if len(dups) > 0:
        _fail(
            f"Assertion 2: {len(dups)} duplicate old_worker_id values after resolve; "
            f"sample: {list(dups.unique()[:5])}"
        )
    print(f"  [PASS] Assertion 2: no duplicate non-blank old_worker_id ({len(old_ids):,} unique IDs)")

    # ------------------------------------------------------------------
    # Assertion 3: no duplicate non-blank new_worker_id
    # ------------------------------------------------------------------
    if "new_worker_id" not in df.columns:
        _fail("Assertion 3: new_worker_id column missing from matched_raw.csv")
    new_ids = _non_blank(df["new_worker_id"])
    dups = new_ids[new_ids.duplicated(keep=False)]
    if len(dups) > 0:
        _fail(
            f"Assertion 3: {len(dups)} duplicate new_worker_id values after resolve; "
            f"sample: {list(dups.unique()[:5])}"
        )
    print(f"  [PASS] Assertion 3: no duplicate non-blank new_worker_id ({len(new_ids):,} unique IDs)")

    # ------------------------------------------------------------------
    # Assertion 4: pair_id present and unique
    # ------------------------------------------------------------------
    if "pair_id" not in df.columns:
        _fail("Assertion 4: pair_id column missing from matched_raw.csv")
    pair_ids = _non_blank(df["pair_id"])
    dups = pair_ids[pair_ids.duplicated(keep=False)]
    if len(dups) > 0:
        _fail(
            f"Assertion 4: {len(dups)} duplicate pair_id values; "
            f"sample: {list(dups.unique()[:5])}"
        )
    print(f"  [PASS] Assertion 4: pair_id present and unique ({len(pair_ids):,} rows)")

    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")


if __name__ == "__main__":
    main()
