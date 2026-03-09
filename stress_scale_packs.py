# stress_scale_packs.py
"""Scale an existing test pack by N times to create stress-test variants.

Usage
-----
    venv/Scripts/python.exe stress_scale_packs.py [pack_name] [scale ...]

    pack_name  (default: international_names)
    scales     (default: 5 10 25)

How scaling works
-----------------
For each replicate r (1-indexed, 1 .. N):

  1. Non-blank worker_id rows get "_S{r}" appended in BOTH old and new.
     Tier-1 (worker_id join) still works because each rep's IDs are unique.

  2. The dob year is incremented by (r - 1) in BOTH old and new.
     Rep 1 is unchanged (bump = 0), rep 2 adds 1 year, rep 3 adds 2, etc.

Why bump dob instead of last4_ssn?
    After N replicates the same (last4, dob) appears N times in each side.
    _safe_unique_keys() silently drops any non-unique key, so Tier 2 would
    return 0 matches for every blank-worker_id row.  Adding (r-1) to the
    birth year makes every replicate's last4+dob key globally unique while
    keeping the old and new sides perfectly in sync, so the tier structure
    is preserved.

Trap behaviour at scale
-----------------------
  Trap 1 (Ana Martinez collision, two distinct last4s):
      The two rows always have different last4 values (6037 vs 3405),
      so each matches its correct counterpart via Tier 2 within the
      replicate.  No cross-replicate collision.

  Trap 2 (Ian Sanders, corrupt last4 in NEW → Tier 4 fallback):
      OLD last4=2148 / NEW last4=9999.  With the same dob bump on both
      sides the Tier-2 keys are "2148|<dob+r>" vs "9999|<dob+r>" — still
      different, so Tier 2 still fails.  The fallback key is
      "ian sanders|<dob+r>|" which is unique in each replicate, so Tier 4
      fires correctly.

  Trap 3 (Xochitl Dominguez vs Chelsea Lopez, same last4+dob):
      Both rows in OLD and NEW receive the same dob bump, so their
      Tier-2 key still matches within the replicate.  The suspicious-match
      flag (name_sim < 0.60) fires once per replicate.

Expected scorecard at scale N (base = international_names):
    tier1_worker_id  : 11 * N   (5 Group A + 6 Clean)
    tier2_last4_dob  :  8 * N   (5 Group B + 2 Trap1 + 1 Trap3)
    tier4_fallback   :  1 * N   (Trap 2)
    resolved_pairs   : 20 * N
    suspicious       :  1 * N   (Trap 3)
    q0_dup_old/new   :  0 / 0
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO      = Path(__file__).parent
PACKS_DIR = REPO / "test_packs"

# Raw CSV columns expected / written (same as generate_*.py)
RAW_COLS = [
    "first_name", "last_name", "position", "dob", "hire_date",
    "location", "salary", "payrate", "worker_status", "worker_type",
    "district", "last4_ssn", "address", "worker_id",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bump_dob_year(dob: str, years: int) -> str:
    """Increment the YYYY component of a YYYY-MM-DD string by *years*."""
    if years == 0 or not dob or len(dob) < 4:
        return dob
    try:
        return str(int(dob[:4]) + years) + dob[4:]
    except ValueError:
        return dob


def _scale_df(df: pd.DataFrame, rep: int) -> pd.DataFrame:
    """Return one replicate copy of df with unique worker_ids and bumped dob."""
    bump = rep - 1
    out  = df.copy()

    # Suffix non-blank worker_ids
    mask = out["worker_id"].str.strip() != ""
    out.loc[mask, "worker_id"] = out.loc[mask, "worker_id"] + f"_S{rep}"

    # Bump dob year (rep 1 is unchanged)
    if bump > 0:
        out["dob"] = out["dob"].apply(lambda d: _bump_dob_year(d, bump))

    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def scale_pack(pack_name: str, scales: list[int]) -> None:
    """Generate scaled variants of *pack_name* for each N in *scales*."""
    src = PACKS_DIR / pack_name
    if not src.exists():
        raise FileNotFoundError(f"Pack not found: {src}")

    old_src = pd.read_csv(src / "old.csv", dtype=str).fillna("")
    new_src = pd.read_csv(src / "new.csv", dtype=str).fillna("")

    # Ensure required columns exist
    for df in (old_src, new_src):
        if "worker_id" not in df.columns:
            df["worker_id"] = ""
        if "dob" not in df.columns:
            df["dob"] = ""

    # Only write columns present in the source (preserves extra cols if any)
    write_cols = [c for c in RAW_COLS if c in old_src.columns]

    for n in scales:
        old_scaled = pd.concat(
            [_scale_df(old_src, r) for r in range(1, n + 1)],
            ignore_index=True,
        )
        new_scaled = pd.concat(
            [_scale_df(new_src, r) for r in range(1, n + 1)],
            ignore_index=True,
        )

        out_dir = PACKS_DIR / f"{pack_name}_S{n}"
        out_dir.mkdir(parents=True, exist_ok=True)
        old_scaled[write_cols].to_csv(out_dir / "old.csv", index=False)
        new_scaled[write_cols].to_csv(out_dir / "new.csv", index=False)

        print(
            f"  [{pack_name}_S{n:<3}]  "
            f"old={len(old_scaled):>5} rows  new={len(new_scaled):>5} rows"
            f"  ->  {out_dir}"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    args = sys.argv[1:]

    pack_name = "international_names"
    scales    = [5, 10, 25]

    if args:
        if not args[0].lstrip("-").isdigit():
            pack_name = args[0]
            args = args[1:]
        if args:
            scales = [int(a) for a in args]

    print(f"[stress] Scaling '{pack_name}'  x {scales}")
    scale_pack(pack_name, scales)
    print("[stress] Done.")


if __name__ == "__main__":
    main()
