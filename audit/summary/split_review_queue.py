"""
audit/summary/split_review_queue.py - Split review queue by department.

Reads review_queue.csv (the full pipeline review queue) and writes:
  review_queue_<dept>.csv    - one file per distinct old_district value
  review_queue_summary.csv   - department name, record count, highest priority score

Files are only written when more than one department exists in the review queue.
The per-department files use a filesystem-safe slug of the department name.

Usage:
  python audit/summary/split_review_queue.py \\
         --rq   <review_queue.csv> \\
         --out  <output_directory>
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_slug(name: str, max_len: int = 40) -> str:
    """Convert a department name to a filesystem-safe slug."""
    slug = str(name).strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")
    if not slug:
        slug = "unknown"
    return slug[:max_len]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def split_review_queue(rq_path: Path, out_dir: Path) -> dict:
    """
    Split review_queue.csv by old_district and write per-department files.

    Returns:
        {
          "n_total":       int,
          "n_depts":       int,
          "dept_files":    list[str],   # filenames written
          "summary_file":  str | None,  # summary CSV filename or None
        }
    """
    if not rq_path.exists():
        print(f"[split_rq] review_queue.csv not found: {rq_path}", file=sys.stderr)
        return {"n_total": 0, "n_depts": 0, "dept_files": [], "summary_file": None}

    rq = pd.read_csv(rq_path, dtype=str, low_memory=False)
    if rq.empty:
        print("[split_rq] review_queue.csv is empty - nothing to split.")
        return {"n_total": 0, "n_depts": 0, "dept_files": [], "summary_file": None}

    # Department column: old_district preferred, fallback to new_district
    if "old_district" in rq.columns:
        dept_col = "old_district"
    elif "new_district" in rq.columns:
        dept_col = "new_district"
    else:
        print("[split_rq] no district column found - skipping department split.")
        return {"n_total": len(rq), "n_depts": 0, "dept_files": [], "summary_file": None}

    rq["_dept"] = rq[dept_col].fillna("").str.strip()

    depts = sorted(rq["_dept"].unique())
    n_depts = len(depts)

    if n_depts <= 1:
        print(f"[split_rq] only {n_depts} department(s) - skipping per-department split.")
        return {"n_total": len(rq), "n_depts": n_depts, "dept_files": [], "summary_file": None}

    out_dir.mkdir(parents=True, exist_ok=True)
    dept_files: list[str] = []
    summary_rows: list[dict] = []

    # Priority score column for "highest priority" summary
    prio_col = "priority_score" if "priority_score" in rq.columns else None

    for dept in depts:
        subset = rq[rq["_dept"] == dept].drop(columns=["_dept"]).copy()
        slug   = _safe_slug(dept) if dept else "no_department"
        fname  = f"review_queue_{slug}.csv"
        fpath  = out_dir / fname
        subset.to_csv(fpath, index=False)
        dept_files.append(fname)

        max_prio = None
        if prio_col:
            prio_vals = pd.to_numeric(subset[prio_col], errors="coerce")
            if not prio_vals.isna().all():
                max_prio = int(prio_vals.max())

        summary_rows.append({
            "department":     dept or "(none)",
            "record_count":   len(subset),
            "highest_priority_score": max_prio if max_prio is not None else "",
        })

    # Write summary
    summary_df   = pd.DataFrame(summary_rows).sort_values(
        "record_count", ascending=False
    )
    summary_path = out_dir / "review_queue_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    print(
        f"[split_rq] {len(rq):,} rows split across {n_depts} departments. "
        f"Wrote {len(dept_files)} per-dept files + review_queue_summary.csv"
    )
    return {
        "n_total":      len(rq),
        "n_depts":      n_depts,
        "dept_files":   dept_files,
        "summary_file": "review_queue_summary.csv",
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Split review queue by department.")
    parser.add_argument("--rq",  required=True, help="Path to review_queue.csv")
    parser.add_argument("--out", required=True, help="Output directory")
    args = parser.parse_args(argv)

    split_review_queue(Path(args.rq), Path(args.out))


if __name__ == "__main__":
    main()
