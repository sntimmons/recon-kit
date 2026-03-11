from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "audit" / "audit.db"

# Allow importing gating modules from audit/summary/
_SUMMARY_DIR = Path(__file__).resolve().parent / "summary"
sys.path.insert(0, str(_SUMMARY_DIR))

import gating
from confidence_policy import policy_summary, is_auto_approve_source

_REQUIRED_COLS = [
    "pair_id",
    "match_source",
    "old_worker_id",
    "new_worker_id",
    "old_full_name_norm",
    "old_salary",
    "new_salary",
    "old_worker_status",
    "new_worker_status",
    "old_hire_date",
    "new_hire_date",
]


def _parse_num(x) -> float | None:
    if x is None:
        return None
    s = str(x).strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _hdr(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


def main() -> None:
    if not DB_PATH.exists():
        print(f"[error] audit.db not found at {DB_PATH}", file=sys.stderr)
        sys.exit(2)

    con = sqlite3.connect(str(DB_PATH))
    try:
        try:
            mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)
        except Exception as exc:
            print(f"[error] could not query matched_pairs: {exc}", file=sys.stderr)
            sys.exit(2)
    finally:
        con.close()

    cols = set(mp.columns)
    missing = [c for c in _REQUIRED_COLS if c not in cols]
    if missing:
        print(
            f"[error] matched_pairs is missing required columns: {sorted(missing)}",
            file=sys.stderr,
        )
        sys.exit(2)

    total = len(mp)

    # ------------------------------------------------------------------
    # Overview
    # ------------------------------------------------------------------
    _hdr("RECONCILIATION SUMMARY")
    print(f"  database  : {DB_PATH}")
    print(f"  total matched pairs : {total:,}")

    # ------------------------------------------------------------------
    # match_source breakdown
    # ------------------------------------------------------------------
    _hdr("MATCH SOURCE BREAKDOWN")
    src_counts = (
        mp["match_source"]
        .fillna("unknown")
        .replace("", "unknown")
        .value_counts()
        .reset_index()
    )
    src_counts.columns = ["match_source", "count"]
    src_counts["pct"] = (src_counts["count"] / total * 100).round(1)
    for _, row in src_counts.iterrows():
        bar = "#" * max(1, int(row["pct"] / 2))
        print(f"  {row['match_source']:<20s}  {row['count']:>7,}  ({row['pct']:5.1f}%)  {bar}")

    # ------------------------------------------------------------------
    # Salary analysis
    # ------------------------------------------------------------------
    _hdr("SALARY ANALYSIS")

    q = mp.copy()
    q["_old_sal"] = q["old_salary"].map(_parse_num)
    q["_new_sal"] = q["new_salary"].map(_parse_num)

    both_valid = q["_old_sal"].notna() & q["_new_sal"].notna()
    differ = both_valid & (q["_old_sal"] != q["_new_sal"])

    mismatch_rows = q[differ].copy()
    mismatch_rows["_delta"] = mismatch_rows["_new_sal"] - mismatch_rows["_old_sal"]

    n_both = int(both_valid.sum())
    n_mismatch = int(differ.sum())
    n_increase = int((mismatch_rows["_delta"] > 0).sum())
    n_decrease = int((mismatch_rows["_delta"] < 0).sum())

    print(f"  rows with both salaries parseable  : {n_both:,}")
    print(f"  salary mismatches                  : {n_mismatch:,}")
    print(f"    increases (new > old)             : {n_increase:,}")
    print(f"    decreases (new < old)             : {n_decrease:,}")

    if n_mismatch > 0:
        d = mismatch_rows["_delta"]
        print(f"\n  delta statistics (new - old, mismatches only):")
        print(f"    min    : {d.min():>12,.2f}")
        print(f"    p10    : {d.quantile(0.10):>12,.2f}")
        print(f"    median : {d.median():>12,.2f}")
        print(f"    avg    : {d.mean():>12,.2f}")
        print(f"    p90    : {d.quantile(0.90):>12,.2f}")
        print(f"    max    : {d.max():>12,.2f}")

        print(f"\n  top 10 largest absolute salary deltas:")
        top10 = mismatch_rows.nlargest(10, "_delta", keep="all")[
            ["old_worker_id", "new_worker_id", "old_full_name_norm", "old_salary", "new_salary", "_delta"]
        ]
        bot10 = mismatch_rows.nsmallest(10, "_delta", keep="all")[
            ["old_worker_id", "new_worker_id", "old_full_name_norm", "old_salary", "new_salary", "_delta"]
        ]
        top10_abs = pd.concat([top10, bot10]).drop_duplicates()
        top10_abs = top10_abs.reindex(
            top10_abs["_delta"].abs().sort_values(ascending=False).index
        ).head(10)
        print(
            f"  {'old_worker_id':<14}  {'new_worker_id':<14}  {'name':<30}  "
            f"{'old_salary':>12}  {'new_salary':>12}  {'delta':>12}"
        )
        print("  " + "-" * 100)
        for _, row in top10_abs.iterrows():
            print(
                f"  {str(row['old_worker_id']):<14}  {str(row['new_worker_id']):<14}  "
                f"  {str(row['old_full_name_norm'])[:30]:<30}  "
                f"  {str(row['old_salary']):>12}  {str(row['new_salary']):>12}  "
                f"  {row['_delta']:>12,.2f}"
            )
    else:
        print("  no salary mismatches found.")

    # ------------------------------------------------------------------
    # Status mismatches
    # ------------------------------------------------------------------
    _hdr("WORKER STATUS MISMATCHES")

    status_diff = mp[
        mp["old_worker_status"].fillna("").str.strip()
        != mp["new_worker_status"].fillna("").str.strip()
    ]
    print(f"  total status mismatches : {len(status_diff):,}")
    if len(status_diff) > 0:
        print(f"\n  top 10 examples:")
        cols_show = ["old_worker_id", "new_worker_id", "old_full_name_norm", "old_worker_status", "new_worker_status"]
        cols_show = [c for c in cols_show if c in mp.columns]
        top = status_diff[cols_show].head(10)
        print(f"  {'old_worker_id':<14}  {'new_worker_id':<14}  {'name':<30}  {'old_status':<20}  {'new_status':<20}")
        print("  " + "-" * 104)
        for _, row in top.iterrows():
            print(
                f"  {str(row.get('old_worker_id', '')):<14}  "
                f"{str(row.get('new_worker_id', '')):<14}  "
                f"  {str(row.get('old_full_name_norm', ''))[:30]:<30}  "
                f"  {str(row.get('old_worker_status', '')):<20}  "
                f"  {str(row.get('new_worker_status', '')):<20}"
            )

    # ------------------------------------------------------------------
    # Hire date mismatches
    # ------------------------------------------------------------------
    _hdr("HIRE DATE MISMATCHES")

    hire_diff = mp[
        mp["old_hire_date"].fillna("").str.strip()
        != mp["new_hire_date"].fillna("").str.strip()
    ]
    print(f"  total hire date mismatches : {len(hire_diff):,}")
    if len(hire_diff) > 0:
        print(f"\n  top 10 examples:")
        cols_show = ["old_worker_id", "new_worker_id", "old_full_name_norm", "old_hire_date", "new_hire_date"]
        cols_show = [c for c in cols_show if c in mp.columns]
        top = hire_diff[cols_show].head(10)
        print(f"  {'old_worker_id':<14}  {'new_worker_id':<14}  {'name':<30}  {'old_hire_date':<14}  {'new_hire_date':<14}")
        print("  " + "-" * 96)
        for _, row in top.iterrows():
            print(
                f"  {str(row.get('old_worker_id', '')):<14}  "
                f"{str(row.get('new_worker_id', '')):<14}  "
                f"  {str(row.get('old_full_name_norm', ''))[:30]:<30}  "
                f"  {str(row.get('old_hire_date', '')):<14}  "
                f"  {str(row.get('new_hire_date', '')):<14}"
            )

    # ------------------------------------------------------------------
    # Confidence gating policy
    # ------------------------------------------------------------------
    _hdr("CONFIDENCE GATING POLICY")
    for line in policy_summary():
        print(f"  {line}")

    # ------------------------------------------------------------------
    # Gating summary - classify all rows and aggregate
    # ------------------------------------------------------------------
    _hdr("GATING SUMMARY BY FIX TYPE")

    if "confidence" not in mp.columns:
        mp = mp.copy()
        mp["confidence"] = None

    # Accumulators:  fix_type → {APPROVE: int, REVIEW: int}
    ft_counts: dict[str, dict[str, int]] = {}
    # match_source → {APPROVE: int, REVIEW: int}
    src_gate: dict[str, dict[str, int]] = {}

    for r in mp.to_dict(orient="records"):
        result    = gating.classify_all(r)
        fix_types = result["fix_types"]
        if not fix_types:
            continue
        action = result["action"]
        src    = str(r.get("match_source", "unknown")).strip() or "unknown"

        for ft in fix_types:
            ft_counts.setdefault(ft, {"APPROVE": 0, "REVIEW": 0})
            # Per-fix action (not overall action) for per-fix breakdown
            ft_action = result["per_fix"][ft]["action"]
            ft_counts[ft][ft_action] += 1

        src_gate.setdefault(src, {"APPROVE": 0, "REVIEW": 0})
        src_gate[src][action] += 1

    if not ft_counts:
        print("  No mismatches detected.")
    else:
        total_mismatch_rows = sum(v["APPROVE"] + v["REVIEW"] for v in src_gate.values())
        # Deduplicate: rows counted once per fix_type, but summary by source is per-row
        print(f"  {'fix_type':<14}  {'total':>8}  {'APPROVE':>8}  {'REVIEW':>8}  {'%approve':>8}")
        print("  " + "-" * 56)
        for ft in ["salary", "payrate", "status", "hire_date", "job_org"]:
            if ft not in ft_counts:
                continue
            ap = ft_counts[ft]["APPROVE"]
            rv = ft_counts[ft]["REVIEW"]
            tot = ap + rv
            pct = ap / tot * 100 if tot else 0
            print(f"  {ft:<14}  {tot:>8,}  {ap:>8,}  {rv:>8,}  {pct:>7.1f}%")

    # ------------------------------------------------------------------
    # Gating summary by match_source
    # ------------------------------------------------------------------
    _hdr("GATING SUMMARY BY MATCH SOURCE")

    if not src_gate:
        print("  No mismatch rows found.")
    else:
        print(f"  {'match_source':<20}  {'rows w/mismatch':>16}  {'APPROVE':>8}  {'REVIEW':>8}  {'%approve':>8}")
        print("  " + "-" * 66)
        for src in sorted(src_gate):
            ap  = src_gate[src]["APPROVE"]
            rv  = src_gate[src]["REVIEW"]
            tot = ap + rv
            pct = ap / tot * 100 if tot else 0
            auto = " (auto)" if is_auto_approve_source(src) else ""
            print(f"  {src + auto:<20}  {tot:>16,}  {ap:>8,}  {rv:>8,}  {pct:>7.1f}%")

    print("\n[done] reconciliation summary complete.")


if __name__ == "__main__":
    main()
