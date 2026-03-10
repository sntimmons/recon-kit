"""
sanity_checks.py — Sanity pack analyzer for matched_pairs mismatches.

Reads matched_pairs from audit/audit.db, computes mismatch distributions,
detects suspicious default patterns, and writes diagnostic CSVs.

Outputs (written to audit/summary/ by default)
-----------------------------------------------
  sanity_salary_buckets.csv    — salary-delta distribution by bucket
  sanity_hire_date_diff.csv    — hire-date day-gap distribution by bucket
  sanity_suspicious_defaults.csv — rows matching known bad-data patterns

Public API
----------
  run_sanity_checks(db_path, out_dir) -> dict
      Run analysis, write CSVs, print report, and return a structured dict
      with counts/rates for use by the sanity gate evaluator.

Run:
    venv/Scripts/python.exe audit/summary/sanity_checks.py [--db PATH] [--out-dir PATH] [--json-out PATH]
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import Counter
from datetime import date
from pathlib import Path

_HERE   = Path(__file__).resolve().parent    # audit/summary/
ROOT    = _HERE.parents[1]                   # repo root
DB_PATH = ROOT / "audit" / "audit.db"
OUT_DIR = _HERE

# ---------------------------------------------------------------------------
# Required columns — hard-fail if any missing
# ---------------------------------------------------------------------------
_REQUIRED_COLS = [
    "pair_id", "match_source", "old_worker_id", "new_worker_id", "old_full_name_norm",
    "old_salary", "new_salary", "old_payrate", "new_payrate",
    "old_worker_status", "new_worker_status",
    "old_hire_date", "new_hire_date",
    "old_position", "new_position",
    "old_district", "new_district",
    "old_location_state", "new_location_state",
]

# ---------------------------------------------------------------------------
# Bucket definitions
# ---------------------------------------------------------------------------
_SALARY_BUCKETS: list[tuple[str, float, float | None]] = [
    ("0-100",          0.0,       100.0),
    ("100-1,000",    100.0,     1_000.0),
    ("1,000-5,000",  1_000.0,   5_000.0),
    ("5,000-20,000", 5_000.0,  20_000.0),
    ("20,000+",     20_000.0,      None),   # None = unbounded
]

_HIRE_DATE_BUCKETS: list[tuple[str, int, int | None]] = [
    ("1-7",              1,    7),
    ("8-30",             8,   30),
    ("31-180",          31,  180),
    ("181-365",        181,  365),
    ("366-1095 (1-3yr)", 366, 1095),
    ("1096+ (3yr+)",  1096,  None),          # None = unbounded
]

# Salary values known to be mapping placeholders in this dataset
_SUSPICIOUS_SALARY_VALUES = frozenset({40_000.0, 40_003.0, 40_013.0, 40_073.0})

# Hire-date prefixes that indicate extraction-time defaults
_SUSPICIOUS_HIRE_DATE_PREFIXES = ("2026-02-", "2026-03-")

# Minimum share of total matched pairs for a single new_hire_date value to be
# flagged as a "wave" (data import where everyone received the same date).
# Set lower than the gate threshold so borderline cases appear in the CSV.
_HIRE_DATE_WAVE_MIN_RATE: float = 0.01

_SUSP_COLS = [
    "issue_type", "pair_id", "match_source",
    "old_worker_id", "new_worker_id", "old_full_name_norm",
    "old_hire_date", "new_hire_date",
    "old_salary", "new_salary", "notes",
]


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_float(val) -> float | None:
    if val is None:
        return None
    s = str(val).strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_date(val) -> date | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _pct(values: list[float], p: float) -> float:
    """Interpolated percentile without external libraries."""
    if not values:
        return 0.0
    sv = sorted(values)
    n  = len(sv)
    idx = p / 100.0 * (n - 1)
    lo  = int(idx)
    hi  = min(lo + 1, n - 1)
    return round(sv[lo] * (1.0 - (idx - lo)) + sv[hi] * (idx - lo), 2)


def _bucket_salary(abs_delta: float) -> str | None:
    for label, lo, hi in _SALARY_BUCKETS:
        if hi is None:
            if abs_delta >= lo:
                return label
        elif lo <= abs_delta < hi:
            return label
    return None


def _bucket_hire_date(abs_days: int) -> str | None:
    for label, lo, hi in _HIRE_DATE_BUCKETS:
        if hi is None:
            if abs_days >= lo:
                return label
        elif lo <= abs_days <= hi:
            return label
    return None


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def _analyse(rows: list[dict]) -> tuple[dict, dict, list[dict], dict]:
    """
    Returns (salary_data, hire_data, suspicious_rows, mismatch_counts).

    salary_data  : {bucket_label -> {"deltas": list, "increase": int, "decrease": int}}
    hire_data    : {bucket_label -> {"count": int, "examples": list[str]}}
    suspicious   : list of row dicts for suspicious defaults CSV
    counts       : {"salary": int, "hire_date": int, "status": int}
    """
    salary_data: dict[str, dict] = {
        label: {"deltas": [], "increase": 0, "decrease": 0}
        for label, _, _ in _SALARY_BUCKETS
    }
    hire_data: dict[str, dict] = {
        label: {"count": 0, "examples": []}
        for label, _, _ in _HIRE_DATE_BUCKETS
    }
    suspicious: list[dict] = []
    counts = {"salary": 0, "hire_date": 0, "status": 0}

    for r in rows:
        pid   = str(r.get("pair_id", ""))
        ms    = str(r.get("match_source", ""))
        o_wid = str(r.get("old_worker_id", "") or "")
        n_wid = str(r.get("new_worker_id", "") or "")
        o_name = str(r.get("old_full_name_norm", "") or "")

        # ------------------------------------------------------------------
        # Salary
        # ------------------------------------------------------------------
        o_sal = _parse_float(r.get("old_salary"))
        n_sal = _parse_float(r.get("new_salary"))

        if o_sal is not None and n_sal is not None and o_sal != n_sal:
            counts["salary"] += 1
            abs_d  = abs(n_sal - o_sal)
            bucket = _bucket_salary(abs_d)
            if bucket:
                bd = salary_data[bucket]
                bd["deltas"].append(abs_d)
                if n_sal > o_sal:
                    bd["increase"] += 1
                else:
                    bd["decrease"] += 1

        # Suspicious salary default values
        if o_sal is not None and o_sal in _SUSPICIOUS_SALARY_VALUES:
            suspicious.append({
                "issue_type":         "salary_suspicious_default",
                "pair_id":            pid,
                "match_source":       ms,
                "old_worker_id":      o_wid,
                "new_worker_id":      n_wid,
                "old_full_name_norm": o_name,
                "old_hire_date":      str(r.get("old_hire_date", "") or ""),
                "new_hire_date":      str(r.get("new_hire_date", "") or ""),
                "old_salary":         str(r.get("old_salary", "") or ""),
                "new_salary":         str(r.get("new_salary", "") or ""),
                "notes":              f"old_salary={o_sal:.0f} is a known placeholder value",
            })

        # ------------------------------------------------------------------
        # Hire date
        # ------------------------------------------------------------------
        o_hd_str = str(r.get("old_hire_date", "") or "").strip()
        n_hd_str = str(r.get("new_hire_date", "") or "").strip()
        o_hd     = _parse_date(o_hd_str)
        n_hd     = _parse_date(n_hd_str)

        if o_hd is not None and n_hd is not None and o_hd != n_hd:
            counts["hire_date"] += 1
            abs_days = abs((n_hd - o_hd).days)
            bucket   = _bucket_hire_date(abs_days)
            if bucket:
                bd = hire_data[bucket]
                bd["count"] += 1
                if len(bd["examples"]) < 3:
                    bd["examples"].append(pid)

        # Suspicious hire-date default prefixes
        for prefix in _SUSPICIOUS_HIRE_DATE_PREFIXES:
            if o_hd_str.startswith(prefix):
                suspicious.append({
                    "issue_type":         f"hire_date_default_{prefix.rstrip('-')}",
                    "pair_id":            pid,
                    "match_source":       ms,
                    "old_worker_id":      o_wid,
                    "new_worker_id":      n_wid,
                    "old_full_name_norm": o_name,
                    "old_hire_date":      o_hd_str,
                    "new_hire_date":      n_hd_str,
                    "old_salary":         str(r.get("old_salary", "") or ""),
                    "new_salary":         str(r.get("new_salary", "") or ""),
                    "notes":              f"old_hire_date={o_hd_str} matches extraction-default prefix {prefix!r}",
                })
                break    # only flag once per row per hire-date rule

        # ------------------------------------------------------------------
        # Status
        # ------------------------------------------------------------------
        o_st = str(r.get("old_worker_status", "") or "").strip().lower()
        n_st = str(r.get("new_worker_status", "") or "").strip().lower()
        if o_st and n_st and o_st != n_st:
            counts["status"] += 1

    # ------------------------------------------------------------------
    # Hire-date wave detection: flag any single new_hire_date that appears
    # in more than _HIRE_DATE_WAVE_MIN_RATE of all matched pairs.
    # A wave indicates a bulk import where everyone received the same date.
    # ------------------------------------------------------------------
    total = len(rows)
    if total > 0:
        new_hd_counter: Counter = Counter(
            str(r.get("new_hire_date", "") or "").strip()
            for r in rows
            if str(r.get("new_hire_date", "") or "").strip()
        )
        wave_dates: set[str] = {
            nd for nd, cnt in new_hd_counter.items()
            if cnt / total >= _HIRE_DATE_WAVE_MIN_RATE
        }
        if wave_dates:
            for r in rows:
                n_hd_str = str(r.get("new_hire_date", "") or "").strip()
                if n_hd_str in wave_dates:
                    wave_cnt  = new_hd_counter[n_hd_str]
                    wave_rate = wave_cnt / total
                    suspicious.append({
                        "issue_type":         "hire_date_wave",
                        "pair_id":            str(r.get("pair_id", "")),
                        "match_source":       str(r.get("match_source", "")),
                        "old_worker_id":      str(r.get("old_worker_id", "") or ""),
                        "new_worker_id":      str(r.get("new_worker_id", "") or ""),
                        "old_full_name_norm": str(r.get("old_full_name_norm", "") or ""),
                        "old_hire_date":      str(r.get("old_hire_date", "") or ""),
                        "new_hire_date":      n_hd_str,
                        "old_salary":         str(r.get("old_salary", "") or ""),
                        "new_salary":         str(r.get("new_salary", "") or ""),
                        "notes": (
                            f"new_hire_date={n_hd_str} shared by "
                            f"{wave_cnt:,}/{total:,} pairs "
                            f"({wave_rate:.2%}) — possible import wave"
                        ),
                    })

    return salary_data, hire_data, suspicious, counts


# ---------------------------------------------------------------------------
# CSV writers
# ---------------------------------------------------------------------------

def _write_salary_buckets(path: Path, salary_data: dict) -> None:
    with open(str(path), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "bucket_label", "count",
            "increase_count", "decrease_count",
            "avg_abs_delta", "p50_abs_delta", "p90_abs_delta",
        ])
        for label, _, _ in _SALARY_BUCKETS:
            bd     = salary_data[label]
            deltas = bd["deltas"]
            n      = len(deltas)
            if n == 0:
                w.writerow([label, 0, 0, 0, 0.0, 0.0, 0.0])
            else:
                avg_d = round(sum(deltas) / n, 2)
                w.writerow([
                    label, n, bd["increase"], bd["decrease"],
                    avg_d, _pct(deltas, 50), _pct(deltas, 90),
                ])


def _write_hire_date_buckets(path: Path, hire_data: dict) -> None:
    with open(str(path), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["days_diff_bucket", "count", "examples"])
        for label, _, _ in _HIRE_DATE_BUCKETS:
            bd       = hire_data[label]
            examples = "|".join(bd["examples"]) if bd["examples"] else ""
            w.writerow([label, bd["count"], examples])


def _write_suspicious(path: Path, rows: list[dict]) -> None:
    with open(str(path), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_SUSP_COLS)
        w.writeheader()
        for row in rows:
            w.writerow(row)


# ---------------------------------------------------------------------------
# Console report
# ---------------------------------------------------------------------------

def _print_report(
    total: int,
    counts: dict,
    salary_data: dict,
    hire_data: dict,
    suspicious: list[dict],
) -> None:
    W = 68
    print()
    print("=" * W)
    print("  SANITY CHECK REPORT")
    print("=" * W)
    print(f"  Total matched pairs            : {total:>8,}")
    print()
    print("  MISMATCH COUNTS")
    print(f"    salary mismatches             : {counts['salary']:>8,}")
    print(f"    hire_date mismatches          : {counts['hire_date']:>8,}")
    print(f"    status mismatches             : {counts['status']:>8,}")

    # Salary bucket table
    print()
    print("  SALARY MISMATCH BUCKETS  (abs delta)")
    hdr = f"  {'bucket':<18} {'count':>7} {'increase':>9} {'decrease':>9} {'avg_delta':>10} {'p50':>8} {'p90':>8}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for label, _, _ in _SALARY_BUCKETS:
        bd     = salary_data[label]
        deltas = bd["deltas"]
        n      = len(deltas)
        if n == 0:
            print(f"  {label:<18} {0:>7} {0:>9} {0:>9} {'—':>10} {'—':>8} {'—':>8}")
        else:
            avg_d = round(sum(deltas) / n, 2)
            p50   = _pct(deltas, 50)
            p90   = _pct(deltas, 90)
            print(
                f"  {label:<18} {n:>7,} {bd['increase']:>9,} {bd['decrease']:>9,}"
                f" {avg_d:>10,.2f} {p50:>8,.2f} {p90:>8,.2f}"
            )

    # Hire date bucket table
    print()
    print("  HIRE DATE DIFF BUCKETS  (abs days between old and new hire_date)")
    print(f"  {'bucket':<24} {'count':>7}  examples (pair_id)")
    print("  " + "-" * 60)
    for label, _, _ in _HIRE_DATE_BUCKETS:
        bd       = hire_data[label]
        examples = "|".join(bd["examples"]) if bd["examples"] else "—"
        print(f"  {label:<24} {bd['count']:>7,}  {examples}")

    # Suspicious defaults
    print()
    print("  SUSPICIOUS DEFAULTS")
    issue_counts = Counter(s["issue_type"] for s in suspicious)
    if not issue_counts:
        print("    (none detected)")
    else:
        for issue_type, cnt in sorted(issue_counts.items(), key=lambda x: -x[1]):
            print(f"    {issue_type:<40} : {cnt:>6,} rows")
            examples = [s for s in suspicious if s["issue_type"] == issue_type][:3]
            for ex in examples:
                print(
                    f"      pair_id={ex['pair_id']}"
                    f"  old_wid={ex['old_worker_id']}"
                    f"  new_wid={ex['new_worker_id']}"
                )

    print()
    print("=" * W)
    print("  [done] sanity_checks complete.")
    print("=" * W)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_wave_dates(rows: list[dict], min_rate: float = _HIRE_DATE_WAVE_MIN_RATE) -> frozenset[str]:
    """
    Return the set of new_hire_date values that appear in >= min_rate of all rows.

    A wave date indicates a bulk import where many records received the same date.
    Returns a frozenset of ISO date strings (empty if none detected).

    Public API — called by build_diy_exports, build_review_queue, build_workbook
    to pass wave context into classify_all() so individual records are routed to REVIEW.
    """
    total = len(rows)
    if total == 0:
        return frozenset()
    counter: Counter = Counter(
        str(r.get("new_hire_date", "") or "").strip()
        for r in rows
        if str(r.get("new_hire_date", "") or "").strip()
    )
    return frozenset(nd for nd, cnt in counter.items() if cnt / total >= min_rate)


def run_sanity_checks(db_path: Path, out_dir: Path) -> dict:
    """
    Run the full sanity check analysis and return a structured results dict.

    Connects to DB, validates required columns, analyses all rows, writes
    three CSVs to out_dir, prints the console report, and returns:

        {
          "total_pairs": int,
          "mismatch_counts": {"salary": int, "hire_date": int, "status": int},
          "suspicious": {
            "hire_date_default_2026_02": {"count": int, "rate": float},
            "salary_suspicious_default": {"count": int, "rate": float},
            ...
          },
          "files_written": {
            "sanity_salary_buckets.csv": str_path,
            "sanity_hire_date_diff.csv": str_path,
            "sanity_suspicious_defaults.csv": str_path,
          },
        }

    Issue-type keys in "suspicious" use underscores (hyphens replaced) so they
    match policy.yaml threshold keys (e.g. hire_date_default_2026_02).

    Exits with code 2 on DB-not-found or missing required columns.
    """
    if not db_path.exists():
        print(f"[error] DB not found: {db_path}", file=sys.stderr)
        sys.exit(2)

    # ------------------------------------------------------------------
    # Connect, validate columns
    # ------------------------------------------------------------------
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        try:
            cur = con.execute("SELECT * FROM matched_pairs LIMIT 1")
        except Exception as exc:
            print(f"[error] cannot query matched_pairs: {exc}", file=sys.stderr)
            sys.exit(2)

        # Derive actual columns from cursor description (works even if 0 rows returned)
        actual_cols: set[str] = set()
        if cur.description:
            actual_cols = {col[0] for col in cur.description}
        else:
            pragma = con.execute("PRAGMA table_info(matched_pairs)").fetchall()
            actual_cols = {row["name"] for row in pragma}

        missing = [c for c in _REQUIRED_COLS if c not in actual_cols]
        if missing:
            print(
                f"[error] matched_pairs missing required columns: {sorted(missing)}",
                file=sys.stderr,
            )
            sys.exit(2)

        cur = con.execute("SELECT * FROM matched_pairs")
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        con.close()

    total = len(rows)
    print(f"[sanity_checks] {total:,} matched pairs loaded from {db_path.name}")

    # ------------------------------------------------------------------
    # Analyse
    # ------------------------------------------------------------------
    salary_data, hire_data, suspicious, counts = _analyse(rows)

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------
    out_dir.mkdir(parents=True, exist_ok=True)

    salary_path = out_dir / "sanity_salary_buckets.csv"
    _write_salary_buckets(salary_path, salary_data)
    print(f"  wrote: sanity_salary_buckets.csv")

    hire_path = out_dir / "sanity_hire_date_diff.csv"
    _write_hire_date_buckets(hire_path, hire_data)
    print(f"  wrote: sanity_hire_date_diff.csv")

    susp_path = out_dir / "sanity_suspicious_defaults.csv"
    _write_suspicious(susp_path, suspicious)
    print(f"  wrote: sanity_suspicious_defaults.csv  ({len(suspicious):,} rows)")

    # ------------------------------------------------------------------
    # Console report
    # ------------------------------------------------------------------
    _print_report(total, counts, salary_data, hire_data, suspicious)

    # ------------------------------------------------------------------
    # Build structured results dict
    # Normalize issue_type keys: replace hyphens with underscores so they
    # align with policy.yaml format (hire_date_default_2026_02 etc.)
    # ------------------------------------------------------------------
    issue_counts = Counter(s["issue_type"] for s in suspicious)
    suspicious_dict: dict[str, dict] = {}
    for issue_type, cnt in issue_counts.items():
        norm_key = issue_type.replace("-", "_")
        rate = round(cnt / total, 6) if total > 0 else 0.0
        suspicious_dict[norm_key] = {"count": cnt, "rate": rate}

    # ------------------------------------------------------------------
    # Fix 6: Health metrics — deterministic match rate and active/$0 count.
    # approve_rate is NOT computed here (requires gating engine) — it is
    # added by run_sanity_gate.py after classify_all pass.
    # ------------------------------------------------------------------
    _DET_SOURCES: frozenset[str] = frozenset({"worker_id", "pk", "recon_id"})
    det_count = sum(
        1 for r in rows
        if str(r.get("match_source", "") or "").strip().lower() in _DET_SOURCES
    )
    det_rate = round(det_count / total, 6) if total > 0 else 0.0

    active_zero_salary_count = sum(
        1 for r in rows
        if str(r.get("new_worker_status", "") or "").strip().lower() == "active"
        and (_parse_float(r.get("new_salary")) or 0.0) == 0.0
    )

    return {
        "total_pairs":     total,
        "mismatch_counts": counts,
        "suspicious":      suspicious_dict,
        "health_metrics": {
            "det_count":              det_count,
            "det_rate":               det_rate,
            "active_zero_salary":     active_zero_salary_count,
            # approve_rate / approve_count populated by run_sanity_gate.py
        },
        "files_written": {
            "sanity_salary_buckets.csv":      str(salary_path),
            "sanity_hire_date_diff.csv":      str(hire_path),
            "sanity_suspicious_defaults.csv": str(susp_path),
        },
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Sanity pack analyzer for matched_pairs mismatches.",
    )
    parser.add_argument(
        "--db", default=None, metavar="PATH",
        help=f"SQLite database path (default: {DB_PATH}).",
    )
    parser.add_argument(
        "--out-dir", default=None, metavar="PATH",
        help=f"Output directory for CSVs (default: {OUT_DIR}).",
    )
    parser.add_argument(
        "--json-out", default=None, metavar="PATH",
        help="Optional path to write the results dict as JSON.",
    )
    args = parser.parse_args(argv)

    db_path = Path(args.db)      if args.db      else DB_PATH
    out_dir = Path(args.out_dir) if args.out_dir else OUT_DIR

    results = run_sanity_checks(db_path, out_dir)

    if args.json_out:
        json_path = Path(args.json_out)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(str(json_path), "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        print(f"  wrote: {json_path}")

    sys.exit(0)


if __name__ == "__main__":
    main()
