"""
root_cause_hire_date.py - Explain the hire_date default pattern.

Reads audit/audit.db and config/policy.yaml to identify pairs where one or
both hire dates match a suspicious default-date pattern (2026-02-*, 2026-03-*,
or any month listed in policy.yaml patterns.hire_date_default_months).

Outputs (written to --out-dir, default audit/summary/)
-------------------------------------------------------
  root_cause_hire_date_defaults.csv
      pattern, total_rows, pct_of_all_pairs, match_source_breakdown_json

  root_cause_hire_date_samples.csv
      pattern, pair_id, old_worker_id, new_worker_id,
      old_hire_date, new_hire_date, match_source

Console output: key counts and file paths.

Exit codes: 0 success, 2 DB missing / query error.

Run
---
  venv/Scripts/python.exe audit/summary/root_cause_hire_date.py [--db PATH] [--out-dir PATH]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent   # audit/summary/
ROOT  = _HERE.parents[1]                  # repo root

sys.path.insert(0, str(_HERE))
from config_loader import load_policy      # noqa: E402

DB_PATH = ROOT / "audit" / "audit.db"
OUT_DIR = _HERE

# Fallback patterns if policy.yaml is not configured
_DEFAULT_MONTHS = ["2026-02", "2026-03"]

_SAMPLE_COLS = [
    "pattern", "pair_id", "old_worker_id", "new_worker_id",
    "old_hire_date", "new_hire_date", "match_source",
]

_SAMPLES_PER_PATTERN = 20   # max sample rows per pattern


def _get_patterns(policy: dict) -> list[str]:
    """Extract hire_date_default_months from policy, fall back to defaults."""
    months = (
        policy.get("sanity_gate", {})
              .get("patterns", {})
              .get("hire_date_default_months", _DEFAULT_MONTHS)
    )
    return [str(m).strip() for m in months if m]


def _date_matches(date_val, prefix: str) -> bool:
    """Return True if date_val (string) starts with the YYYY-MM prefix."""
    if not date_val:
        return False
    return str(date_val).strip().startswith(prefix)


def _analyse(mp: pd.DataFrame, patterns: list[str], total_pairs: int) -> tuple[list[dict], list[dict]]:
    """
    For each pattern, count matching pairs and collect samples.

    A pair matches a pattern if old_hire_date OR new_hire_date starts with
    the pattern prefix AND the pair has a hire_date mismatch.
    """
    summary_rows: list[dict] = []
    sample_rows:  list[dict] = []

    for pat in patterns:
        # Rows where hire dates differ AND at least one side is a default
        mask_mismatch = mp["old_hire_date"].astype(str).str.strip() != mp["new_hire_date"].astype(str).str.strip()
        mask_default  = (
            mp["old_hire_date"].astype(str).str.startswith(pat)
            | mp["new_hire_date"].astype(str).str.startswith(pat)
        )
        matched = mp[mask_mismatch & mask_default].copy()

        count = len(matched)
        pct   = (count / total_pairs * 100) if total_pairs > 0 else 0.0

        # Match-source breakdown
        src_breakdown: dict[str, int] = {}
        if count > 0:
            src_breakdown = (
                matched["match_source"]
                .fillna("unknown")
                .value_counts()
                .to_dict()
            )

        summary_rows.append({
            "pattern":                   pat,
            "total_rows":                count,
            "pct_of_all_pairs":          round(pct, 4),
            "match_source_breakdown_json": json.dumps(src_breakdown),
        })

        # Collect samples (no names, just worker_ids and dates)
        sample_pool = matched.head(_SAMPLES_PER_PATTERN)
        for _, row in sample_pool.iterrows():
            sample_rows.append({
                "pattern":        pat,
                "pair_id":        row.get("pair_id", ""),
                "old_worker_id":  row.get("old_worker_id", ""),
                "new_worker_id":  row.get("new_worker_id", ""),
                "old_hire_date":  row.get("old_hire_date", ""),
                "new_hire_date":  row.get("new_hire_date", ""),
                "match_source":   row.get("match_source", ""),
            })

    return summary_rows, sample_rows


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Root-cause analysis for hire_date default patterns.",
    )
    parser.add_argument("--db",      default=None, metavar="PATH",
                        help=f"SQLite DB path (default: {DB_PATH}).")
    parser.add_argument("--out-dir", default=None, metavar="PATH",
                        help=f"Output directory (default: {OUT_DIR}).")
    args = parser.parse_args(argv)

    db_path = Path(args.db)      if args.db      else DB_PATH
    out_dir = Path(args.out_dir) if args.out_dir else OUT_DIR

    if not db_path.exists():
        print(f"[error] DB not found: {db_path}", file=sys.stderr)
        sys.exit(2)

    # Load policy for patterns
    policy   = load_policy()
    patterns = _get_patterns(policy)

    # Query matched_pairs
    con = sqlite3.connect(str(db_path))
    try:
        try:
            mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)
        except Exception as exc:
            print(f"[error] cannot query matched_pairs: {exc}", file=sys.stderr)
            sys.exit(2)
    finally:
        con.close()

    required = ["pair_id", "old_hire_date", "new_hire_date", "match_source",
                "old_worker_id", "new_worker_id"]
    missing = [c for c in required if c not in mp.columns]
    if missing:
        print(f"[error] matched_pairs missing columns: {sorted(missing)}", file=sys.stderr)
        sys.exit(2)

    total_pairs = len(mp)
    print(f"[root_cause_hire_date] {total_pairs:,} pairs loaded.")
    print(f"  patterns to check: {patterns}")

    summary_rows, sample_rows = _analyse(mp, patterns, total_pairs)

    out_dir.mkdir(parents=True, exist_ok=True)

    # Write summary CSV
    defaults_path = out_dir / "root_cause_hire_date_defaults.csv"
    pd.DataFrame(summary_rows, columns=[
        "pattern", "total_rows", "pct_of_all_pairs", "match_source_breakdown_json",
    ]).to_csv(str(defaults_path), index=False)

    # Write samples CSV
    samples_path = out_dir / "root_cause_hire_date_samples.csv"
    (
        pd.DataFrame(sample_rows, columns=_SAMPLE_COLS)
        if sample_rows
        else pd.DataFrame(columns=_SAMPLE_COLS)
    ).to_csv(str(samples_path), index=False)

    # Console summary
    W = 60
    print()
    print("=" * W)
    print("  HIRE DATE DEFAULT ROOT CAUSE")
    print("=" * W)
    print(f"  total matched pairs : {total_pairs:,}")
    for r in summary_rows:
        pct_str = f"{r['pct_of_all_pairs']:.2f}%"
        print(f"  {r['pattern']:<12}  {r['total_rows']:>7,} rows  ({pct_str} of total)")
        bkdn = json.loads(r["match_source_breakdown_json"])
        for src, cnt in sorted(bkdn.items(), key=lambda x: -x[1]):
            print(f"    {src:<18}  {cnt:>6,}")
    print("=" * W)
    print(f"  wrote: {defaults_path.relative_to(ROOT)}")
    print(f"  wrote: {samples_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
