"""
audit/internal_audit.py — Single-file internal data quality audit.

Usage:
    venv/Scripts/python.exe audit/internal_audit.py --file path/to/data.csv --out-dir path/to/output/

Outputs (all written to --out-dir):
    internal_audit_report.json      — machine-readable summary (for API)
    internal_audit_report.csv       — human-readable full report
    internal_audit_duplicates.csv   — duplicate worker_id records
    internal_audit_blanks.csv       — per-column blank/null rates
    internal_audit_suspicious.csv   — suspicious default values detected
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from collections import Counter, defaultdict

import pandas as pd


# ---------------------------------------------------------------------------
# Suspicious default detection
# ---------------------------------------------------------------------------
SUSPICIOUS_SALARIES = {40000, 40003, 40013, 40073, 50000, 60000, 99999, 100000}
SUSPICIOUS_HIRE_DATE_PREFIXES = ["2026-02", "2026-03", "1900-", "1970-01-01", "2000-01-01"]
SUSPICIOUS_STATUSES = {"unknown", "n/a", "na", "null", "none", "test"}


def _norm_str(x) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and pd.isna(x):
        return ""
    return str(x).strip()


def _detect_suspicious(df: pd.DataFrame) -> list[dict]:
    rows = []

    # Salary suspicious defaults
    if "salary" in df.columns:
        sal = pd.to_numeric(df["salary"], errors="coerce")
        for val in SUSPICIOUS_SALARIES:
            mask = sal == val
            if mask.any():
                rows.append({
                    "check": "salary_suspicious_default",
                    "value": str(val),
                    "count": int(mask.sum()),
                    "pct": round(mask.sum() / len(df) * 100, 2),
                    "description": f"Salary exactly {val} — common system default",
                })

    # Hire date suspicious defaults
    for col in ["hire_date", "start_date", "date_hired"]:
        if col not in df.columns:
            continue
        dates = df[col].astype(str)
        for prefix in SUSPICIOUS_HIRE_DATE_PREFIXES:
            mask = dates.str.startswith(prefix, na=False)
            if mask.any():
                rows.append({
                    "check": "hire_date_suspicious_default",
                    "value": prefix,
                    "count": int(mask.sum()),
                    "pct": round(mask.sum() / len(df) * 100, 2),
                    "description": f"Hire date starting with {prefix} — possible system default",
                })

    # Suspicious worker statuses
    if "worker_status" in df.columns:
        statuses = df["worker_status"].astype(str).str.lower().str.strip()
        for s in SUSPICIOUS_STATUSES:
            mask = statuses == s
            if mask.any():
                rows.append({
                    "check": "status_suspicious_value",
                    "value": s,
                    "count": int(mask.sum()),
                    "pct": round(mask.sum() / len(df) * 100, 2),
                    "description": f"Worker status '{s}' — placeholder or test value",
                })

    return rows


def _detect_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    results = {}
    dup_frames = []

    for col in ["worker_id", "email", "last4_ssn"]:
        if col not in df.columns:
            continue
        col_data = df[col].astype(str).str.strip()
        nonnull = col_data[col_data.notna() & (col_data != "") & (col_data.str.lower() != "nan")]
        dupes = nonnull[nonnull.duplicated(keep=False)]
        if len(dupes) > 0:
            results[col] = {
                "duplicate_values": int(dupes.nunique()),
                "duplicate_records": int(len(dupes)),
            }
            subset = df.loc[dupes.index].copy()
            subset["_dup_field"] = col
            dup_frames.append(subset)

    dup_df = pd.concat(dup_frames, ignore_index=True) if dup_frames else pd.DataFrame()
    return dup_df, results


def _field_completeness(df: pd.DataFrame) -> list[dict]:
    rows = []
    total = len(df)
    if total == 0:
        return rows

    # Priority fields to always check if present
    priority = [
        "worker_id", "first_name", "last_name", "full_name", "email",
        "date_of_birth", "dob", "hire_date", "salary", "payrate",
        "worker_status", "position", "job_title", "department",
        "location", "manager_id", "last4_ssn", "recon_id",
    ]
    checked = set()
    order = [c for c in priority if c in df.columns] + [c for c in df.columns if c not in priority]

    for col in order:
        if col in checked:
            continue
        checked.add(col)
        series = df[col]
        blank_mask = (
            series.isna()
            | (series.astype(str).str.strip() == "")
            | (series.astype(str).str.lower().str.strip().isin(["nan", "none", "null", "n/a", "na"]))
        )
        blank_count = int(blank_mask.sum())
        rows.append({
            "field": col,
            "total": total,
            "blank_count": blank_count,
            "blank_pct": round(blank_count / total * 100, 2),
            "filled_pct": round((total - blank_count) / total * 100, 2),
        })

    return rows


def _salary_distribution(df: pd.DataFrame) -> list[dict]:
    if "salary" not in df.columns:
        return []
    sal = pd.to_numeric(df["salary"], errors="coerce").dropna()
    if len(sal) == 0:
        return []
    buckets = [
        ("0-1k",   0,     1000),
        ("1k-25k", 1000,  25000),
        ("25k-60k",25000, 60000),
        ("60k-100k",60000,100000),
        ("100k-200k",100000,200000),
        ("200k+",  200000, float("inf")),
    ]
    rows = []
    for label, lo, hi in buckets:
        mask = (sal >= lo) & (sal < hi)
        if mask.any():
            rows.append({
                "bucket": label,
                "count": int(mask.sum()),
                "min": round(float(sal[mask].min()), 2),
                "max": round(float(sal[mask].max()), 2),
                "median": round(float(sal[mask].median()), 2),
            })
    return rows


def _status_distribution(df: pd.DataFrame) -> list[dict]:
    for col in ["worker_status", "status", "employment_status"]:
        if col in df.columns:
            counts = df[col].astype(str).str.strip().str.lower().value_counts()
            return [{"status": k, "count": int(v)} for k, v in counts.items()]
    return []


def run_internal_audit(file_path: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[internal_audit] reading: {file_path}")
    try:
        df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
        df = df.replace("", pd.NA)
    except Exception as e:
        print(f"[internal_audit] ERROR reading file: {e}", file=sys.stderr)
        sys.exit(1)

    total_rows = len(df)
    total_cols = len(df.columns)
    print(f"[internal_audit] loaded {total_rows} rows x {total_cols} columns")

    # ---- Run checks ----
    dup_df, dup_summary    = _detect_duplicates(df)
    suspicious             = _detect_suspicious(df)
    completeness           = _field_completeness(df)
    salary_dist            = _salary_distribution(df)
    status_dist            = _status_distribution(df)

    # ---- Summary ----
    total_blank_fields = sum(r["blank_count"] for r in completeness)
    total_possible     = total_rows * total_cols
    overall_completeness = round((total_possible - total_blank_fields) / total_possible * 100, 1) if total_possible > 0 else 0.0

    issues = []
    for col, info in dup_summary.items():
        issues.append(f"Duplicate {col}: {info['duplicate_records']} records across {info['duplicate_values']} values")
    for s in suspicious:
        issues.append(f"{s['check']}: {s['count']} records ({s['pct']}%) — {s['description']}")
    for r in completeness:
        if r["blank_pct"] > 20 and r["field"] in ["worker_id", "full_name", "first_name", "last_name", "email"]:
            issues.append(f"High blank rate on {r['field']}: {r['blank_pct']}%")

    summary = {
        "total_rows":           total_rows,
        "total_columns":        total_cols,
        "overall_completeness": overall_completeness,
        "duplicate_checks":     dup_summary,
        "suspicious_count":     len(suspicious),
        "issue_count":          len(issues),
        "issues":               issues,
        "columns":              list(df.columns),
    }

    # ---- Write outputs ----
    # 1. JSON report (for API)
    report_json = out_dir / "internal_audit_report.json"
    report_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[internal_audit] wrote: {report_json}")

    # 2. Completeness CSV
    blanks_csv = out_dir / "internal_audit_blanks.csv"
    _write_csv(blanks_csv, completeness)
    print(f"[internal_audit] wrote: {blanks_csv}")

    # 3. Duplicates CSV
    if not dup_df.empty:
        dup_csv = out_dir / "internal_audit_duplicates.csv"
        dup_df.to_csv(dup_csv, index=False)
        print(f"[internal_audit] wrote: {dup_csv} ({len(dup_df)} rows)")

    # 4. Suspicious defaults CSV
    if suspicious:
        susp_csv = out_dir / "internal_audit_suspicious.csv"
        _write_csv(susp_csv, suspicious)
        print(f"[internal_audit] wrote: {susp_csv}")

    # 5. Human-readable report CSV
    report_rows = []
    report_rows.append({"section": "SUMMARY", "field": "total_rows", "value": str(total_rows), "note": ""})
    report_rows.append({"section": "SUMMARY", "field": "total_columns", "value": str(total_cols), "note": ""})
    report_rows.append({"section": "SUMMARY", "field": "overall_completeness_pct", "value": str(overall_completeness), "note": ""})
    report_rows.append({"section": "SUMMARY", "field": "issues_found", "value": str(len(issues)), "note": "; ".join(issues[:5])})

    for col, info in dup_summary.items():
        report_rows.append({
            "section": "DUPLICATES",
            "field": col,
            "value": str(info["duplicate_records"]),
            "note": f"{info['duplicate_values']} duplicate values",
        })

    for s in suspicious:
        report_rows.append({
            "section": "SUSPICIOUS",
            "field": s["check"],
            "value": f"{s['count']} ({s['pct']}%)",
            "note": s["description"],
        })

    for r in completeness:
        report_rows.append({
            "section": "COMPLETENESS",
            "field": r["field"],
            "value": f"{r['filled_pct']}% filled",
            "note": f"{r['blank_count']} blank of {r['total']}",
        })

    report_csv = out_dir / "internal_audit_report.csv"
    _write_csv(report_csv, report_rows)
    print(f"[internal_audit] wrote: {report_csv}")

    print(f"[internal_audit] complete. {len(issues)} issues found.")
    return summary


def _write_csv(path: Path, rows: list[dict]):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Single-file internal data quality audit")
    parser.add_argument("--file",    required=True, help="Path to the CSV file to audit")
    parser.add_argument("--out-dir", required=True, help="Directory to write outputs to")
    args = parser.parse_args()

    result = run_internal_audit(Path(args.file), Path(args.out_dir))
    sys.exit(0)
