"""
generate_corrections.py — Workday-ready Correction File Generator.

Reads matched_pairs from audit/audit.db, applies the gating engine per fix_type,
and writes APPROVE-only correction CSVs to audit/corrections/out/.

Output files
------------
  corrections_salary.csv    — APPROVE salary changes
  corrections_status.csv    — APPROVE status changes
  corrections_hire_date.csv — APPROVE hire-date changes
  corrections_job_org.csv   — APPROVE position/district/location changes
  review_needed.csv         — rows where ANY fix_type is REVIEW
  corrections_manifest.csv  — one row per correction generated (all fix types)

Run:
    venv/Scripts/python.exe audit/corrections/generate_corrections.py [--dry-run] [--out-dir PATH]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter
from datetime import date
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Import gating engine from audit/summary/
# ---------------------------------------------------------------------------
_HERE        = Path(__file__).resolve().parent       # audit/corrections/
_SUMMARY_DIR = _HERE.parent / "summary"              # audit/summary/
sys.path.insert(0, str(_SUMMARY_DIR))

from gating import (
    infer_fix_types,
    classify_all,
    classify_row,
    salary_delta,
    payrate_delta,
    build_summary_str,
    _parse_confidence,
    _parse_num,
    _norm,
)
from sanity_checks import detect_wave_dates

ROOT    = _HERE.parents[1]          # repo root
DB_PATH = ROOT / "audit" / "audit.db"
OUT_DIR = _HERE / "out"

TODAY = date.today().isoformat()    # YYYY-MM-DD, determined once at import time

# ---------------------------------------------------------------------------
# Required columns for corrections to be possible
# ---------------------------------------------------------------------------
_REQUIRED_COLS = [
    "pair_id", "match_source",
    "old_worker_id", "new_worker_id",
    "old_salary", "new_salary",
    "old_payrate", "new_payrate",
    "old_worker_status", "new_worker_status",
    "old_hire_date", "new_hire_date",
    "old_position", "new_position",
    "old_district", "new_district",
    "old_location_state", "new_location_state",
]

# ---------------------------------------------------------------------------
# Column schemas (exact order as per spec)
# ---------------------------------------------------------------------------
_SALARY_COLS = [
    "worker_id", "effective_date", "compensation_amount",
    "currency", "reason", "pair_id", "match_source", "confidence", "summary",
]
_STATUS_COLS = [
    "worker_id", "effective_date", "worker_status",
    "reason", "pair_id", "match_source", "confidence", "summary",
]
_HIRE_DATE_COLS = [
    "worker_id", "hire_date",
    "reason", "pair_id", "match_source", "confidence", "summary",
]
_JOB_ORG_COLS = [
    "worker_id", "effective_date", "position", "district",
    "location_state", "location",
    "reason", "pair_id", "match_source", "confidence", "summary",
]
_REVIEW_COLS = [
    "worker_id", "pair_id", "match_source", "fix_types",
    "action", "reason", "confidence", "min_confidence", "summary",
]
_MANIFEST_COLS = [
    "correction_type", "worker_id", "pair_id", "match_source",
    "fix_types", "action", "confidence", "summary", "output_file",
]

# Rows held back from corrections (overall REVIEW/REJECT_MATCH or Active/$0 salary)
_HELD_COLS = [
    "worker_id", "pair_id", "match_source", "fix_types",
    "overall_action", "hold_reason", "confidence", "summary",
]


# ---------------------------------------------------------------------------
# Row builders — one per correction type
# ---------------------------------------------------------------------------

def _conf_str(row: dict) -> str:
    c = _parse_confidence(row.get("confidence"))
    return "" if c is None else str(round(c, 4))


def _build_salary_row(row: dict, summary: str) -> dict:
    return {
        "worker_id":            row.get("new_worker_id", ""),
        "effective_date":       TODAY,
        "compensation_amount":  row.get("new_salary", ""),
        "currency":             "USD",
        "reason":               "RECON_SALARY_CORRECTION",
        "pair_id":              row.get("pair_id", ""),
        "match_source":         row.get("match_source", ""),
        "confidence":           _conf_str(row),
        "summary":              summary,
    }


def _build_status_row(row: dict, summary: str) -> dict:
    return {
        "worker_id":     row.get("new_worker_id", ""),
        "effective_date": TODAY,
        "worker_status": row.get("new_worker_status", ""),
        "reason":        "RECON_STATUS_CORRECTION",
        "pair_id":       row.get("pair_id", ""),
        "match_source":  row.get("match_source", ""),
        "confidence":    _conf_str(row),
        "summary":       summary,
    }


def _build_hire_date_row(row: dict, summary: str) -> dict:
    return {
        "worker_id":    row.get("new_worker_id", ""),
        "hire_date":    row.get("new_hire_date", ""),
        "reason":       "RECON_HIRE_DATE_CORRECTION",
        "pair_id":      row.get("pair_id", ""),
        "match_source": row.get("match_source", ""),
        "confidence":   _conf_str(row),
        "summary":      summary,
    }


def _build_job_org_row(row: dict, summary: str, has_location: bool) -> dict:
    return {
        "worker_id":      row.get("new_worker_id", ""),
        "effective_date": TODAY,
        "position":       row.get("new_position", ""),
        "district":       row.get("new_district", ""),
        "location_state": row.get("new_location_state", ""),
        "location":       row.get("new_location", "") if has_location else "",
        "reason":         "RECON_JOB_ORG_CORRECTION",
        "pair_id":        row.get("pair_id", ""),
        "match_source":   row.get("match_source", ""),
        "confidence":     _conf_str(row),
        "summary":        summary,
    }


def _build_review_row(row: dict, result: dict, summary: str) -> dict:
    """One row per pair_id that has ANY REVIEW fix_type."""
    fix_types = result["fix_types"]

    review_reasons: list[str] = []
    min_confs: list[float] = []
    for ft, fr in result["per_fix"].items():
        if fr["action"] == "REVIEW":
            review_reasons.append(f"{ft}:{fr['reason']}")
            if fr.get("min_confidence") is not None:
                min_confs.append(fr["min_confidence"])

    overall_reason = "|".join(review_reasons)
    max_min_conf   = max(min_confs) if min_confs else ""
    min_conf_str   = "" if max_min_conf == "" else str(round(max_min_conf, 4))

    return {
        "worker_id":      row.get("new_worker_id", ""),
        "pair_id":        row.get("pair_id", ""),
        "match_source":   row.get("match_source", ""),
        "fix_types":      "|".join(fix_types),
        "action":         "REVIEW",
        "reason":         overall_reason,
        "confidence":     _conf_str(row),
        "min_confidence": min_conf_str,
        "summary":        summary,
    }


def _build_held_row(row: dict, result: dict, hold_reason: str, summary: str) -> dict:
    """One row per pair held back from corrections due to REVIEW/REJECT_MATCH or Active/$0."""
    return {
        "worker_id":      row.get("new_worker_id", ""),
        "pair_id":        row.get("pair_id", ""),
        "match_source":   row.get("match_source", ""),
        "fix_types":      "|".join(result.get("fix_types", [])),
        "overall_action": result.get("action", ""),
        "hold_reason":    hold_reason,
        "confidence":     _conf_str(row),
        "summary":        summary,
    }


def _build_manifest_row(
    correction_type: str,
    row: dict,
    fix_types: list[str],
    action: str,
    summary: str,
    output_file: str,
) -> dict:
    return {
        "correction_type": correction_type,
        "worker_id":       row.get("new_worker_id", ""),
        "pair_id":         row.get("pair_id", ""),
        "match_source":    row.get("match_source", ""),
        "fix_types":       "|".join(fix_types),
        "action":          action,
        "confidence":      _conf_str(row),
        "summary":         summary,
        "output_file":     output_file,
    }


# ---------------------------------------------------------------------------
# CSV injection sanitization
# ---------------------------------------------------------------------------

# Characters that cause spreadsheet applications (Excel, Google Sheets) to
# interpret a cell as a formula when they appear as the first character.
_FORMULA_CHARS = frozenset("=+-@\t")


def _safe_str(v) -> str:
    """Prefix formula-starting values with a tab to prevent CSV injection.

    Excel and Google Sheets treat cells whose first character is =, +, -, @
    as formulas.  Prepending \\t (horizontal tab) causes the application to
    render the value as plain text without altering its visible content after
    the leading whitespace is trimmed by most viewers.
    """
    if v is None:
        return ""
    s = str(v)
    if s and s[0] in _FORMULA_CHARS:
        return "\t" + s
    return s


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

def _write(rows: list[dict], cols: list[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    # Sanitize all string-typed columns to prevent formula injection when
    # the output CSV is opened in Excel or Google Sheets.
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(lambda v: _safe_str(v) if pd.notna(v) else v)
    df.to_csv(str(path), index=False)
    print(f"  wrote: {path.relative_to(ROOT)}  ({len(df):,} rows)")


# ---------------------------------------------------------------------------
# Dry-run report
# ---------------------------------------------------------------------------

def _print_dry_run_report(
    total_in: int,
    mismatch_count: int,
    salary_rows: list[dict],
    status_rows: list[dict],
    hire_date_rows: list[dict],
    job_org_rows: list[dict],
    review_rows: list[dict],
    manifest_rows: list[dict],
    held_rows: list[dict],
    only_approved: bool,
    include_review_needed: bool,
) -> None:
    W = 60
    print("\n" + "=" * W)
    print("  DRY RUN -- Correction File Generator")
    print("  No files will be written.")
    print("=" * W)
    print(f"  only_approved          : {only_approved}")
    print(f"  include_review_needed  : {include_review_needed}")
    print(f"  matched_pairs total    : {total_in:>8,}")
    print(f"  rows with mismatches   : {mismatch_count:>8,}")
    print(f"  held (not corrected)   : {len(held_rows):>8,}")
    print()
    print("  Would write:")
    print(f"    corrections_salary.csv     : {len(salary_rows):>8,} rows")
    print(f"    corrections_status.csv     : {len(status_rows):>8,} rows")
    print(f"    corrections_hire_date.csv  : {len(hire_date_rows):>8,} rows")
    print(f"    corrections_job_org.csv    : {len(job_org_rows):>8,} rows")
    rn_str = f"{len(review_rows):>8,}" if include_review_needed else "    SKIP"
    print(f"    review_needed.csv          : {rn_str} rows")
    print(f"    held_corrections.csv       : {len(held_rows):>8,} rows")
    print(f"    corrections_manifest.csv   : {len(manifest_rows):>8,} rows")
    total_corr = len(salary_rows) + len(status_rows) + len(hire_date_rows) + len(job_org_rows)
    print(f"    (manifest = {total_corr:,} rows; review_needed/held are NOT in manifest)")

    if held_rows:
        print()
        print("  held_corrections breakdown by hold_reason:")
        hold_counts = Counter(r["hold_reason"] for r in held_rows)
        for reason, cnt in sorted(hold_counts.items(), key=lambda x: -x[1]):
            print(f"    {reason:<40} : {cnt:>6,}")

    if review_rows:
        print()
        print("  review_needed breakdown by match_source:")
        src_counts = Counter(r["match_source"] for r in review_rows)
        for src, cnt in sorted(src_counts.items(), key=lambda x: -x[1]):
            print(f"    {src:<32} : {cnt:>6,}")

        print()
        print("  review_needed breakdown by fix_type:")
        ft_counts: Counter = Counter()
        for r in review_rows:
            for ft in str(r.get("fix_types", "")).split("|"):
                if ft:
                    ft_counts[ft] += 1
        for ft, cnt in sorted(ft_counts.items(), key=lambda x: -x[1]):
            print(f"    {ft:<32} : {cnt:>6,}")

    print()
    print("=" * W)
    print("  DRY RUN complete. No files written.")
    print("=" * W)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Workday-ready Correction File Generator.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Simulate: compute counts but do not write any files.",
    )
    parser.add_argument(
        "--db", default=None, metavar="PATH",
        help=f"SQLite database path (default: {DB_PATH}).",
    )
    parser.add_argument(
        "--out-dir", default=None, metavar="PATH",
        help=f"Output directory (default: {OUT_DIR}).",
    )
    parser.add_argument(
        "--only-approved", action=argparse.BooleanOptionalAction, default=True,
        help="Write only APPROVE rows to correction files (default: True).",
    )
    parser.add_argument(
        "--include-review-needed", action=argparse.BooleanOptionalAction, default=True,
        help="Write review_needed.csv (default: True).",
    )
    parser.add_argument(
        "--strict", action="store_true",
        help="Exit with code 2 if required DB columns are missing (default behavior).",
    )
    args = parser.parse_args(argv)

    db_path              = Path(args.db) if args.db else DB_PATH
    out_dir              = Path(args.out_dir) if args.out_dir else OUT_DIR
    dry_run              = args.dry_run
    only_approved        = args.only_approved
    include_review_needed = args.include_review_needed

    if not db_path.exists():
        print(f"[error] audit.db not found: {db_path}", file=sys.stderr)
        sys.exit(2)

    con = sqlite3.connect(str(db_path))
    try:
        try:
            mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)
        except Exception as exc:
            print(f"[error] cannot query matched_pairs: {exc}", file=sys.stderr)
            sys.exit(2)
    finally:
        con.close()

    # Column validation
    cols = set(mp.columns)
    missing = [c for c in _REQUIRED_COLS if c not in cols]
    if missing:
        print(f"[error] matched_pairs missing required columns: {sorted(missing)}", file=sys.stderr)
        sys.exit(2)

    # Optional columns
    if "confidence" not in mp.columns:
        mp = mp.copy()
        mp["confidence"] = None
    has_location = "new_location" in mp.columns

    total_in = len(mp)
    mode_tag = " [DRY RUN]" if dry_run else ""
    print(f"[generate_corrections{mode_tag}] {total_in:,} matched pairs loaded.")
    if not dry_run:
        try:
            out_dir_display = out_dir.relative_to(ROOT)
        except ValueError:
            out_dir_display = out_dir
        print(f"  effective_date         : {TODAY}")
        print(f"  has_location col       : {has_location}")
        print(f"  only_approved          : {only_approved}")
        print(f"  include_review_needed  : {include_review_needed}")
        print(f"  output dir             : {out_dir_display}")

    # Pre-materialise rows so detect_wave_dates can scan the full dataset once.
    all_rows   = mp.to_dict(orient="records")
    wave_dates = detect_wave_dates(all_rows)
    if wave_dates:
        print(f"  wave dates detected    : {sorted(wave_dates)}")

    # Accumulators
    salary_rows:    list[dict] = []
    status_rows:    list[dict] = []
    hire_date_rows: list[dict] = []
    job_org_rows:   list[dict] = []
    review_rows:    list[dict] = []
    held_rows:      list[dict] = []
    manifest_rows:  list[dict] = []

    review_pair_ids: set[str] = set()
    mismatch_count = 0

    for r in all_rows:
        result         = classify_all(r, wave_dates=wave_dates)
        fix_types      = result["fix_types"]
        overall_action = result["action"]

        if not fix_types and overall_action == "APPROVE":
            continue

        mismatch_count += 1
        summary = build_summary_str(r, fix_types)

        # -----------------------------------------------------------------------
        # Fix 2: APPROVE gate — hold all corrections for REVIEW or REJECT_MATCH
        # pairs.  A pair with overall_action=REVIEW means at least one field
        # change needs human sign-off; we must not stage any corrections until
        # the reviewer approves.  REJECT_MATCH pairs should never be corrected.
        # -----------------------------------------------------------------------
        if overall_action in ("REVIEW", "REJECT_MATCH"):
            held_rows.append(
                _build_held_row(r, result, f"overall_{overall_action}", summary)
            )
            # Still populate review_needed so reviewers can find REVIEW pairs.
            if overall_action == "REVIEW":
                pair_id = str(r.get("pair_id", ""))
                if pair_id not in review_pair_ids:
                    review_pair_ids.add(pair_id)
                    review_rows.append(_build_review_row(r, result, summary))
            continue   # <-- no correction rows for this pair

        # From here: overall_action == "APPROVE" — route per fix_type.
        for ft, gate in result["per_fix"].items():
            if only_approved and gate["action"] != "APPROVE":
                continue

            if ft == "salary":
                # ---------------------------------------------------------------
                # Fix 1: CRITICAL — never stage a salary correction that would
                # write $0 (or blank) onto an Active worker.  Such a record
                # indicates a mapping failure or data artefact.
                # ---------------------------------------------------------------
                new_sal    = _parse_num(r.get("new_salary"))
                new_status = _norm(r.get("new_worker_status", ""))
                if new_status == "active" and (new_sal is None or new_sal == 0.0):
                    held_rows.append(
                        _build_held_row(r, result, "active_zero_salary_blocked", summary)
                    )
                    continue   # skip this fix_type only; other fix_types may still proceed

                salary_rows.append(_build_salary_row(r, summary))
                manifest_rows.append(_build_manifest_row(
                    "salary", r, fix_types, gate["action"], summary, "corrections_salary.csv"
                ))

            elif ft == "status":
                status_rows.append(_build_status_row(r, summary))
                manifest_rows.append(_build_manifest_row(
                    "status", r, fix_types, gate["action"], summary, "corrections_status.csv"
                ))

            elif ft == "hire_date":
                hire_date_rows.append(_build_hire_date_row(r, summary))
                manifest_rows.append(_build_manifest_row(
                    "hire_date", r, fix_types, gate["action"], summary, "corrections_hire_date.csv"
                ))

            elif ft == "job_org":
                job_org_rows.append(_build_job_org_row(r, summary, has_location))
                manifest_rows.append(_build_manifest_row(
                    "job_org", r, fix_types, gate["action"], summary, "corrections_job_org.csv"
                ))

    # -----------------------------------------------------------------------
    # Output: dry-run report OR write files
    # -----------------------------------------------------------------------
    if dry_run:
        _print_dry_run_report(
            total_in, mismatch_count,
            salary_rows, status_rows, hire_date_rows,
            job_org_rows, review_rows, manifest_rows,
            held_rows, only_approved, include_review_needed,
        )
        return

    print(f"\n[generate_corrections] writing output files ...")
    _write(salary_rows,    _SALARY_COLS,    out_dir / "corrections_salary.csv")
    _write(status_rows,    _STATUS_COLS,    out_dir / "corrections_status.csv")
    _write(hire_date_rows, _HIRE_DATE_COLS, out_dir / "corrections_hire_date.csv")
    _write(job_org_rows,   _JOB_ORG_COLS,  out_dir / "corrections_job_org.csv")
    if include_review_needed:
        _write(review_rows, _REVIEW_COLS,   out_dir / "review_needed.csv")
    _write(held_rows,      _HELD_COLS,      out_dir / "held_corrections.csv")
    _write(manifest_rows,  _MANIFEST_COLS,  out_dir / "corrections_manifest.csv")

    total_corrections = (
        len(salary_rows) + len(status_rows) + len(hire_date_rows) + len(job_org_rows)
    )
    print(f"\n[generate_corrections] complete.")
    print(f"  total correction rows : {total_corrections:,}")
    print(f"    salary    : {len(salary_rows):,}")
    print(f"    status    : {len(status_rows):,}")
    print(f"    hire_date : {len(hire_date_rows):,}")
    print(f"    job_org   : {len(job_org_rows):,}")
    if include_review_needed:
        print(f"  review_needed         : {len(review_rows):,}")
    print(f"  held (not corrected)  : {len(held_rows):,}")
    print(f"  manifest rows         : {len(manifest_rows):,}")


if __name__ == "__main__":
    main()
