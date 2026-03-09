"""
build_workbook.py — Full Excel workbook export.

Sheets
------
  1) Summary              — key statistics
  2) All_Matches          — full dataset (all rows, all wide_compare columns)
  3) Salary_Mismatches    — rows where fix_types contains "salary"
  4) Status_Mismatches    — rows where fix_types contains "status"
  5) HireDate_Mismatches  — rows where fix_types contains "hire_date"
  6) JobOrg_Mismatches    — rows where fix_types contains "job_org"
  7) Review_Queue         — review_queue.csv if present, else action==REVIEW filter
  8) Corrections_Manifest — corrections_manifest.csv if present, else placeholder

Source priority for All_Matches data:
  1. audit/exports/out/wide_compare.csv  (preferred)
  2. matched_pairs view in audit.db      (fallback, computes gating on the fly)

Uses openpyxl write_only=True mode for streaming writes — no MemoryError on large
datasets.  Formatting: bold/coloured header row via WriteOnlyCell; freeze_panes and
auto_filter are not available in write_only mode.

Run:
    venv/Scripts/python.exe audit/summary/build_workbook.py [--out PATH] [--wide PATH] [--db PATH]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    from openpyxl import Workbook
    from openpyxl.cell.cell import WriteOnlyCell
    from openpyxl.styles import Font, PatternFill
    from openpyxl.utils import get_column_letter
except ImportError:
    print(
        "[error] openpyxl not installed. "
        "Run: venv/Scripts/pip.exe install openpyxl",
        file=sys.stderr,
    )
    sys.exit(2)

_HERE = Path(__file__).resolve().parent    # audit/summary/
sys.path.insert(0, str(_HERE))

from gating import (
    classify_all,
    salary_delta,
    payrate_delta,
    build_summary_str,
    _parse_confidence,
    _norm,
)
from config_loader import load_policy, load_pii_config

ROOT         = _HERE.parents[1]
DB_PATH      = ROOT / "audit" / "audit.db"
WIDE_CSV     = ROOT / "audit" / "exports" / "out" / "wide_compare.csv"
REVIEW_CSV   = _HERE / "review_queue.csv"
MANIFEST_CSV = ROOT / "audit" / "corrections" / "out" / "corrections_manifest.csv"
OUT_PATH     = _HERE / "recon_workbook.xlsx"

# ---------------------------------------------------------------------------
# Styling constants
# ---------------------------------------------------------------------------
_HDR_FILL     = PatternFill(start_color="BDD7EE", end_color="BDD7EE", fill_type="solid")
_HDR_FONT     = Font(bold=True)
_SUM_HDR_FONT = Font(bold=True, size=11)


# ---------------------------------------------------------------------------
# Sheet writer helpers (write_only compatible)
# ---------------------------------------------------------------------------

def _header_cell(ws, value, font=None) -> WriteOnlyCell:
    """Return a WriteOnlyCell with header formatting."""
    cell = WriteOnlyCell(ws, value=value)
    cell.font = font or _HDR_FONT
    cell.fill = _HDR_FILL
    return cell


def _write_df_to_sheet(ws, df: pd.DataFrame) -> None:
    """Write DataFrame to a write_only worksheet.

    Header row is bold+coloured via WriteOnlyCell.
    Data rows are written as plain value lists for streaming performance.
    freeze_panes, auto_filter, and column_dimensions are not available in
    write_only mode.
    """
    cols = list(df.columns)
    if not cols:
        return

    # Header row with bold/colour formatting
    ws.append([_header_cell(ws, c) for c in cols])

    # Data rows — no per-cell formatting for streaming performance
    for row_tuple in df.itertuples(index=False, name=None):
        ws.append(list(row_tuple))


def _write_summary_sheet(ws, all_df: pd.DataFrame, db_path: Path, wide_src: str) -> None:
    """Write the Summary sheet as a key-value table (write_only compatible)."""

    def _kv(label: str = "", value="", bold: bool = False, font=None) -> None:
        if (bold or font) and label:
            lc = WriteOnlyCell(ws, value=label)
            lc.font = font or _HDR_FONT
            ws.append([lc, value])
        else:
            ws.append([label, value])

    _kv("Reconciliation Workbook", "", font=_SUM_HDR_FONT)
    _kv("Generated", datetime.now().strftime("%Y-%m-%d %H:%M"))
    _kv("Source DB", str(db_path.name))
    _kv("Data source", wide_src)
    _kv()

    total = len(all_df)
    _kv("MATCHED PAIRS", "", bold=True)
    _kv("  Total rows", total)
    if "action" in all_df.columns:
        n_approve = int((all_df["action"] == "APPROVE").sum())
        n_review  = int((all_df["action"] == "REVIEW").sum())
        _kv("  APPROVE", n_approve)
        _kv("  REVIEW", n_review)
    _kv()

    if "match_source" in all_df.columns:
        _kv("BY MATCH SOURCE", "", bold=True)
        for src, cnt in all_df["match_source"].value_counts().items():
            _kv(f"  {src}", int(cnt))
        _kv()

    if "fix_types" in all_df.columns:
        _kv("MISMATCH TYPES", "", bold=True)
        for ft, label in [
            ("salary",    "Salary changes"),
            ("status",    "Status changes"),
            ("hire_date", "Hire-date changes"),
            ("job_org",   "Job/org changes"),
        ]:
            cnt = int(all_df["fix_types"].str.contains(ft, na=False).sum())
            _kv(f"  {label}", cnt)
        no_change = int((all_df["fix_types"].fillna("") == "").sum())
        _kv("  No changes", no_change)
        _kv()

    if "salary_delta" in all_df.columns:
        sal_d = pd.to_numeric(all_df["salary_delta"], errors="coerce").dropna()
        if len(sal_d) > 0:
            _kv("SALARY DELTA STATS", "", bold=True)
            _kv("  Rows with salary change", len(sal_d))
            _kv("  Mean delta",   round(float(sal_d.mean()), 2))
            _kv("  Median delta", round(float(sal_d.median()), 2))
            _kv("  Max increase", round(float(sal_d.max()), 2))
            _kv("  Max decrease", round(float(sal_d.min()), 2))
            _kv()

    _kv("SHEETS", "", bold=True)
    sheet_info = [
        ("All_Matches",         total),
        ("Salary_Mismatches",   int(all_df["fix_types"].str.contains("salary",    na=False).sum()) if "fix_types" in all_df.columns else "?"),
        ("Status_Mismatches",   int(all_df["fix_types"].str.contains("status",    na=False).sum()) if "fix_types" in all_df.columns else "?"),
        ("HireDate_Mismatches", int(all_df["fix_types"].str.contains("hire_date", na=False).sum()) if "fix_types" in all_df.columns else "?"),
        ("JobOrg_Mismatches",   int(all_df["fix_types"].str.contains("job_org",   na=False).sum()) if "fix_types" in all_df.columns else "?"),
    ]
    for name, cnt in sheet_info:
        _kv(f"  {name}", cnt)


# ---------------------------------------------------------------------------
# Fallback: compute wide_compare from DB
# ---------------------------------------------------------------------------

def _str_eq(a, b) -> bool:
    return _norm(a) == _norm(b)


def _salary_ratio(old_sal, new_sal):
    try:
        o = float(str(old_sal or "").replace(",", "").replace("$", ""))
        n = float(str(new_sal or "").replace(",", "").replace("$", ""))
        return None if o == 0 else round(n / o, 6)
    except Exception:
        return None


def _priority_score(row: dict, fix_types: list[str], sal_d, result: dict) -> int:
    score = 0
    if "status" in fix_types:
        score += 50
    if sal_d is not None:
        if abs(sal_d) >= 5000:
            score += 30
        if abs(sal_d) >= 1000:
            score += 15
    if "hire_date" in fix_types:
        score += 20
    if "job_org" in fix_types:
        if not _str_eq(row.get("old_position"), row.get("new_position")):
            score += 10
        if not _str_eq(row.get("old_district"), row.get("new_district")):
            score += 8
        if not _str_eq(row.get("old_location_state"), row.get("new_location_state")):
            score += 6
    if _norm(row.get("match_source", "")) != "worker_id":
        score += 10
    if _parse_confidence(row.get("confidence")) is None:
        score += 10
    if len(fix_types) > 1:
        score += 5
    return score


def _load_wide_from_db(db_path: Path) -> pd.DataFrame:
    """Compute wide_compare columns directly from matched_pairs (fallback)."""
    print("[build_workbook] computing gating from DB (this may take a moment) ...")
    con = sqlite3.connect(str(db_path))
    try:
        mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)
    finally:
        con.close()

    if "confidence" not in mp.columns:
        mp["confidence"] = None

    # PII guard: suppress DOB from DB-sourced wide data if configured.
    pii_cfg = load_pii_config(load_policy())
    if not pii_cfg.get("include_dob_in_exports", True):
        dob_cols = [c for c in ["old_dob", "new_dob"] if c in mp.columns]
        if dob_cols:
            mp = mp.drop(columns=dob_cols)
            print(f"[build_workbook] [pii] suppressed DOB columns: {dob_cols}")

    has_loc = "new_location" in mp.columns
    has_wt  = "old_worker_type" in mp.columns

    rows = []
    for r in mp.to_dict(orient="records"):
        result    = classify_all(r)
        fix_types = result["fix_types"]
        sal_d     = salary_delta(r)
        pay_d     = payrate_delta(r)
        prio      = _priority_score(r, fix_types, sal_d, result)

        rows.append({
            "pair_id":            r.get("pair_id", ""),
            "match_source":       r.get("match_source", ""),
            "confidence":         r.get("confidence"),
            "action":             result["action"],
            "reason":             result["reason"],
            "fix_types":          "|".join(fix_types),
            "summary":            build_summary_str(r, fix_types) if fix_types else "no_changes",
            "priority_score":     prio,
            "old_full_name_norm": r.get("old_full_name_norm", ""),
            "new_full_name_norm": r.get("new_full_name_norm", ""),
            "old_worker_status":  r.get("old_worker_status", ""),
            "new_worker_status":  r.get("new_worker_status", ""),
            "old_worker_type":    r.get("old_worker_type", "") if has_wt else "",
            "new_worker_type":    r.get("new_worker_type", "") if has_wt else "",
            "old_hire_date":      r.get("old_hire_date", ""),
            "new_hire_date":      r.get("new_hire_date", ""),
            "old_position":       r.get("old_position", ""),
            "new_position":       r.get("new_position", ""),
            "old_district":       r.get("old_district", ""),
            "new_district":       r.get("new_district", ""),
            "old_location_state": r.get("old_location_state", ""),
            "new_location_state": r.get("new_location_state", ""),
            "old_location":       r.get("old_location", "") if has_loc else "",
            "new_location":       r.get("new_location", "") if has_loc else "",
            "old_salary":         r.get("old_salary"),
            "new_salary":         r.get("new_salary"),
            "old_payrate":        r.get("old_payrate"),
            "new_payrate":        r.get("new_payrate"),
            "salary_delta":       sal_d,
            "salary_ratio":       _salary_ratio(r.get("old_salary"), r.get("new_salary")),
            "payrate_delta":      pay_d,
            "status_changed":     not _str_eq(r.get("old_worker_status"), r.get("new_worker_status")),
            "hire_date_changed":  not _str_eq(r.get("old_hire_date"), r.get("new_hire_date")),
            "job_org_changed":    "job_org" in fix_types,
            "needs_review":       result["action"] == "REVIEW",
            "suggested_action":   result["action"],
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Full Excel workbook export.")
    parser.add_argument(
        "--out", default=None, metavar="PATH",
        help=f"Output workbook path (default: {OUT_PATH}).",
    )
    parser.add_argument(
        "--wide", default=None, metavar="PATH",
        help=f"wide_compare.csv path (default: {WIDE_CSV}).",
    )
    parser.add_argument(
        "--db", default=None, metavar="PATH",
        help=f"SQLite database path (default: {DB_PATH}).",
    )
    args = parser.parse_args(argv)

    out_path  = Path(args.out) if args.out else OUT_PATH
    wide_path = Path(args.wide) if args.wide else WIDE_CSV
    db_path   = Path(args.db) if args.db else DB_PATH

    if not db_path.exists():
        print(f"[error] DB not found: {db_path}", file=sys.stderr)
        sys.exit(2)

    # ------------------------------------------------------------------
    # Load main dataset
    # ------------------------------------------------------------------
    if wide_path.exists():
        print(f"[build_workbook] loading {wide_path.name} ...")
        all_df   = pd.read_csv(str(wide_path))
        wide_src = str(wide_path.relative_to(ROOT)) if wide_path.is_relative_to(ROOT) else str(wide_path)
    else:
        print(f"[build_workbook] {wide_path.name} not found — computing from DB ...")
        all_df   = _load_wide_from_db(db_path)
        wide_src = f"{db_path.name} (gating computed on the fly)"

    total = len(all_df)
    print(f"[build_workbook] {total:,} rows loaded.")

    # Ensure numeric columns are stored as numbers
    for num_col in [
        "salary_delta", "salary_ratio", "payrate_delta",
        "old_salary", "new_salary", "old_payrate", "new_payrate",
        "priority_score", "confidence",
    ]:
        if num_col in all_df.columns:
            all_df[num_col] = pd.to_numeric(all_df[num_col], errors="coerce")

    # ------------------------------------------------------------------
    # Load auxiliary sheets
    # ------------------------------------------------------------------
    if REVIEW_CSV.exists():
        review_df  = pd.read_csv(str(REVIEW_CSV))
        review_src = REVIEW_CSV.name
    elif "action" in all_df.columns:
        review_df  = all_df[all_df["action"] == "REVIEW"].copy()
        review_src = "filtered from All_Matches (action==REVIEW)"
    else:
        review_df  = pd.DataFrame()
        review_src = "unavailable"

    if MANIFEST_CSV.exists():
        manifest_df  = pd.read_csv(str(MANIFEST_CSV))
        manifest_src = MANIFEST_CSV.name
    else:
        manifest_df  = pd.DataFrame([
            {"note": "corrections_manifest.csv not found — run generate_corrections.py first"}
        ])
        manifest_src = "placeholder"

    # ------------------------------------------------------------------
    # Build mismatch filter sheets
    # ------------------------------------------------------------------
    def _fix_filter(ft: str) -> pd.DataFrame:
        if "fix_types" not in all_df.columns:
            return pd.DataFrame(columns=all_df.columns)
        return all_df[all_df["fix_types"].str.contains(ft, na=False)].copy()

    salary_df  = _fix_filter("salary")
    status_df  = _fix_filter("status")
    hire_df    = _fix_filter("hire_date")
    job_org_df = _fix_filter("job_org")

    # Extra_Field_Mismatches — rows where any mm_<field> column is True
    mm_cols = [c for c in all_df.columns if c.startswith("mm_")]
    if mm_cols:
        def _mm_true(series: pd.Series) -> pd.Series:
            return series.astype(str).str.lower().isin(["true", "1"])
        mm_mask = pd.DataFrame({c: _mm_true(all_df[c]) for c in mm_cols}).any(axis=1)
        extra_mismatch_df: pd.DataFrame | None = all_df[mm_mask].copy()
    else:
        extra_mismatch_df = None

    print(f"  review_queue src       : {review_src}  ({len(review_df):,} rows)")
    print(f"  manifest src           : {manifest_src}  ({len(manifest_df):,} rows)")
    print(f"  Salary_Mismatches      : {len(salary_df):,}")
    print(f"  Status_Mismatches      : {len(status_df):,}")
    print(f"  HireDate_Mismatches    : {len(hire_df):,}")
    print(f"  JobOrg_Mismatches      : {len(job_org_df):,}")
    if extra_mismatch_df is not None:
        print(f"  Extra_Field_Mismatches : {len(extra_mismatch_df):,}  (mm_ cols: {mm_cols})")

    # ------------------------------------------------------------------
    # Build workbook (write_only streaming — no MemoryError on large sets)
    # ------------------------------------------------------------------
    print(f"\n[build_workbook] writing workbook (streaming mode) ...")
    wb = Workbook(write_only=True)

    ws_sum = wb.create_sheet("Summary")
    _write_summary_sheet(ws_sum, all_df, db_path, wide_src)
    print(f"  wrote: Summary")

    data_sheets = [
        ("All_Matches",          all_df),
        ("Salary_Mismatches",    salary_df),
        ("Status_Mismatches",    status_df),
        ("HireDate_Mismatches",  hire_df),
        ("JobOrg_Mismatches",    job_org_df),
        ("Review_Queue",         review_df),
        ("Corrections_Manifest", manifest_df),
    ]
    if extra_mismatch_df is not None:
        data_sheets.insert(-1, ("Extra_Field_Mismatches", extra_mismatch_df))

    for sheet_name, df in data_sheets:
        ws = wb.create_sheet(sheet_name)
        _write_df_to_sheet(ws, df)
        print(f"  wrote: {sheet_name:<25}  ({len(df):,} rows)")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(str(out_path))

    try:
        display_path = out_path.relative_to(ROOT)
    except ValueError:
        display_path = out_path
    print(f"\n[build_workbook] saved: {display_path}")


if __name__ == "__main__":
    main()
