# src/apply_name_gate_decisions.py
"""Apply manual decisions from a Tier-2 name-gate review CSV into matched_raw.csv.

Workflow
--------
1. Reviewer opens outputs/review_tier2_name_gate.csv (one row per gated pair).
2. Reviewer adds a ``decision`` column and marks rows MATCH / YES / TRUE to approve.
3. Run this script — approved rows are appended to matched_raw.csv.
4. Re-run resolve_matched_raw.py to rebuild 1-to-1 pairs with updated counts.

Column contract
---------------
* Only columns already present in matched_raw are kept from the review file.
* matched_raw columns absent from the review file are filled with "":
    worker_id, k_last4_dob_old, k_last4_dob_new,
    k_last4_year_l3, fallback_key,
    old_worker_id, new_worker_id, match_source, pair_id
  (resolve_matched_raw.py will re-derive the last four on its next run.)

Usage
-----
  # production defaults
  venv/Scripts/python.exe src/apply_name_gate_decisions.py

  # test-pack overrides
  venv/Scripts/python.exe src/apply_name_gate_decisions.py \\
      --review  test_packs/international_names/outputs/review_tier2_name_gate.csv \\
      --matched test_packs/international_names/outputs/matched_raw.csv
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pandas as pd

# -------------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------------
_APPROVE_VALUES: frozenset[str] = frozenset({"match", "yes", "true", "1"})

_DEFAULT_REVIEW  = "outputs/review_tier2_name_gate.csv"
_DEFAULT_MATCHED = "outputs/matched_raw.csv"


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------
def _blankify(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize NaN / 'nan' / 'none' / 'null' cells to empty string."""
    for col in df.columns:
        s = df[col].fillna("").astype(str).str.strip()
        df[col] = s.where(~s.str.lower().isin(["nan", "none", "null"]), "")
    return df


def _check_no_dup(combined: pd.DataFrame, col: str, out_dir: Path) -> None:
    """Raise if any non-blank value in *col* appears more than once."""
    if col not in combined.columns:
        return
    non_blank = combined[combined[col].astype(str).str.strip() != ""]
    dupes = non_blank[non_blank[col].duplicated(keep=False)]
    if dupes.empty:
        return
    evidence_path = out_dir / f"_dup_integrity_{col}.csv"
    dupes.to_csv(evidence_path, index=False)
    raise ValueError(
        f"Integrity failure: duplicate '{col}' values after append "
        f"({len(dupes)} rows). Evidence written to: {evidence_path}"
    )


# -------------------------------------------------------------------------
# Main entry point
# -------------------------------------------------------------------------
def apply_decisions(
    review_csv: str,
    matched_raw_csv: str,
    out_csv: str | None = None,
) -> None:
    review_path  = Path(review_csv)
    matched_path = Path(matched_raw_csv)
    out_path     = Path(out_csv) if out_csv else matched_path

    if not review_path.exists():
        raise FileNotFoundError(f"Review CSV not found: {review_path}")
    if not matched_path.exists():
        raise FileNotFoundError(f"matched_raw CSV not found: {matched_path}")

    review  = _blankify(pd.read_csv(review_path,  dtype=str))
    matched = _blankify(pd.read_csv(matched_path, dtype=str))

    matched_cols = list(matched.columns)

    # ------------------------------------------------------------------
    # Filter: keep only rows where decision is MATCH / YES / TRUE
    # ------------------------------------------------------------------
    if "decision" not in review.columns:
        print("[apply_decisions] No 'decision' column found in review file.")
        print("[apply_decisions] Add a 'decision' column with MATCH/YES/TRUE to approve rows.")
        print("[apply_decisions] approved_rows       : 0")
        return

    approved = review[review["decision"].str.strip().str.lower().isin(_APPROVE_VALUES)].copy()
    approved_rows = len(approved)
    print(f"[apply_decisions] approved_rows       : {approved_rows}")

    if approved_rows == 0:
        print("[apply_decisions] Nothing to apply.")
        return

    # ------------------------------------------------------------------
    # Skip pairs already present in matched_raw (dedup by recon_id pair)
    # ------------------------------------------------------------------
    existing_pairs: frozenset[tuple[str, str]] = frozenset()
    if "recon_id_old" in matched.columns and "recon_id_new" in matched.columns:
        existing_pairs = frozenset(
            zip(
                matched["recon_id_old"].astype(str),
                matched["recon_id_new"].astype(str),
            )
        )

    def _is_new(row: pd.Series) -> bool:
        key = (
            str(row.get("recon_id_old", "")),
            str(row.get("recon_id_new", "")),
        )
        return key not in existing_pairs

    is_new_mask      = approved.apply(_is_new, axis=1)
    skipped_existing = int((~is_new_mask).sum())
    to_add           = approved[is_new_mask].copy()
    added_rows       = len(to_add)

    print(f"[apply_decisions] skipped_existing    : {skipped_existing}")
    print(f"[apply_decisions] added_rows          : {added_rows}")

    if added_rows == 0:
        print("[apply_decisions] All approved rows already present — nothing written.")
        return

    # ------------------------------------------------------------------
    # Project to matched_raw column layout
    # Fill columns that exist in matched_raw but not in the review file with "".
    # Drop review-only columns (e.g. 'notes').
    # ------------------------------------------------------------------
    for col in matched_cols:
        if col not in to_add.columns:
            to_add[col] = ""
    to_add = to_add[matched_cols]

    # ------------------------------------------------------------------
    # Backup original matched_raw, then append
    # ------------------------------------------------------------------
    bak_path = matched_path.with_suffix(".csv.bak")
    shutil.copy2(matched_path, bak_path)
    print(f"[apply_decisions] backup written      : {bak_path.name}")

    combined = pd.concat([matched, to_add], ignore_index=True)

    # ------------------------------------------------------------------
    # Integrity: no duplicate non-blank worker_id_old / worker_id_new
    # ------------------------------------------------------------------
    _check_no_dup(combined, "worker_id_old", out_path.parent)
    _check_no_dup(combined, "worker_id_new", out_path.parent)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------
    combined.to_csv(out_path, index=False)
    print(f"[apply_decisions] final matched_raw   : {len(combined)} rows")
    print(f"[apply_decisions] written to          : {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Apply name-gate review decisions into matched_raw.csv"
    )
    parser.add_argument(
        "--review",
        default=_DEFAULT_REVIEW,
        help=f"Path to review_tier2_name_gate.csv (default: {_DEFAULT_REVIEW})",
    )
    parser.add_argument(
        "--matched",
        default=_DEFAULT_MATCHED,
        help=f"Path to matched_raw.csv to update (default: {_DEFAULT_MATCHED})",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output path (default: overwrite --matched in place)",
    )
    args = parser.parse_args()
    apply_decisions(args.review, args.matched, args.out)
