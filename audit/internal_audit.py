"""
audit/internal_audit.py - Single-file internal data quality audit.

Usage:
    python audit/internal_audit.py --file path/to/data.csv --out-dir path/to/output/

Outputs (all written to --out-dir):
    internal_audit_report.json          - machine-readable summary (for API / PDF)
    internal_audit_report.csv           - human-readable full report with severities
    internal_audit_duplicates.csv       - duplicate-record details
    internal_audit_blanks.csv           - per-column completeness rates
    internal_audit_suspicious.csv       - suspicious default values detected
    internal_audit_distributions.csv    - salary and status distributions
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "audit" / "summary"))

from config_loader import load_internal_audit_config, load_policy

DEFAULT_SUSPICIOUS_SALARIES = [40000, 40003, 40013, 40073, 50000, 60000, 99999, 100000]
DEFAULT_SUSPICIOUS_HIRE_DATE_PREFIXES = ["2026-02", "2026-03", "1900-", "1970-01-01", "2000-01-01"]
DEFAULT_SUSPICIOUS_STATUSES = ["unknown", "n/a", "na", "null", "none", "test"]
DEFAULT_DUPLICATE_FIELDS = ["worker_id", "email", "last4_ssn"]
DEFAULT_HIGH_BLANK_RATE_THRESHOLD = 0.20

SEVERITY_ORDER = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]


def _norm_str(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def _norm_lower(x) -> str:
    return _norm_str(x).lower()


def _safe_json_value(x):
    val = _norm_str(x)
    return val if val != "" else ""


def _read_input(file_path: Path, sheet_name: int | str = 0) -> pd.DataFrame:
    ext = file_path.suffix.lower()
    if ext in (".xlsx", ".xls", ".xlsm", ".xlsb"):
        return pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
    return pd.read_csv(file_path, dtype=str, keep_default_na=False)


def _load_config() -> dict:
    try:
        policy = load_policy(ROOT / "config" / "policy.yaml")
        cfg = load_internal_audit_config(policy)
    except Exception:
        cfg = {}
    return {
        "high_blank_rate_threshold": float(cfg.get("high_blank_rate_threshold", DEFAULT_HIGH_BLANK_RATE_THRESHOLD) or DEFAULT_HIGH_BLANK_RATE_THRESHOLD),
        "suspicious_salary_values": list(cfg.get("suspicious_salary_values", DEFAULT_SUSPICIOUS_SALARIES) or DEFAULT_SUSPICIOUS_SALARIES),
        "suspicious_hire_date_prefixes": list(cfg.get("suspicious_hire_date_prefixes", DEFAULT_SUSPICIOUS_HIRE_DATE_PREFIXES) or DEFAULT_SUSPICIOUS_HIRE_DATE_PREFIXES),
        "suspicious_status_values": [_norm_lower(v) for v in (cfg.get("suspicious_status_values", DEFAULT_SUSPICIOUS_STATUSES) or DEFAULT_SUSPICIOUS_STATUSES)],
        "duplicate_check_fields": [str(v).strip() for v in (cfg.get("duplicate_check_fields", DEFAULT_DUPLICATE_FIELDS) or DEFAULT_DUPLICATE_FIELDS) if str(v).strip()],
    }


def _blank_mask(series: pd.Series) -> pd.Series:
    as_str = series.astype(str).str.strip()
    return (
        series.isna()
        | (as_str == "")
        | (as_str.str.lower().isin(["nan", "none", "null", "n/a", "na"]))
    )


def _sample_columns(df: pd.DataFrame, extra: list[str] | None = None) -> list[str]:
    cols: list[str] = []
    for col in (extra or []) + [
        "worker_id",
        "full_name",
        "first_name",
        "last_name",
        "email",
        "worker_status",
        "status",
        "employment_status",
        "salary",
        "hire_date",
        "start_date",
        "date_hired",
        "last4_ssn",
    ]:
        if col in df.columns and col not in cols:
            cols.append(col)
    return cols[:6]


def _sample_rows(df: pd.DataFrame, mask: pd.Series, extra: list[str] | None = None, limit: int = 5) -> list[dict]:
    sample_cols = _sample_columns(df, extra=extra)
    rows: list[dict] = []
    for idx, row in df.loc[mask, sample_cols].head(limit).iterrows():
        item = {"row_number": int(idx) + 2}
        for col in sample_cols:
            item[col] = _safe_json_value(row.get(col))
        rows.append(item)
    return rows


def _status_column(df: pd.DataFrame) -> str | None:
    for col in ["worker_status", "status", "employment_status"]:
        if col in df.columns:
            return col
    return None


def _completeness_severity(field: str, blank_pct: float, threshold_pct: float) -> str:
    if blank_pct <= 0:
        return "LOW"
    if blank_pct > threshold_pct:
        if field == "worker_id":
            return "CRITICAL"
        if field == "full_name":
            return "HIGH"
        return "MEDIUM"
    return "LOW"


def _check_severity(check_key: str, field: str | None = None) -> str:
    if check_key in {"duplicate_worker_id", "duplicate_last4_ssn", "active_zero_salary"}:
        return "CRITICAL"
    if check_key in {
        "salary_suspicious_default",
        "hire_date_suspicious_default",
        "status_suspicious_value",
    }:
        return "HIGH"
    if check_key == "duplicate_email":
        return "MEDIUM"
    if check_key == "high_blank_rate":
        if field == "worker_id":
            return "CRITICAL"
        if field == "full_name":
            return "HIGH"
        return "MEDIUM"
    return "LOW"


def _detect_duplicates(df: pd.DataFrame, duplicate_fields: list[str]) -> tuple[pd.DataFrame, dict, list[dict]]:
    results: dict[str, dict] = {}
    dup_frames: list[pd.DataFrame] = []
    findings: list[dict] = []

    for col in duplicate_fields:
        if col not in df.columns:
            continue
        col_data = df[col].astype(str).str.strip()
        nonnull = col_data[(col_data != "") & (col_data.str.lower() != "nan")]
        dupes = nonnull[nonnull.duplicated(keep=False)]
        if dupes.empty:
            continue

        results[col] = {
            "duplicate_values": int(dupes.nunique()),
            "duplicate_records": int(len(dupes)),
        }
        subset = df.loc[dupes.index].copy()
        subset["_dup_field"] = col
        dup_frames.append(subset)

        findings.append(
            {
                "section": "DUPLICATES",
                "check_key": f"duplicate_{col}",
                "check_name": f"Duplicate {col}",
                "field": col,
                "severity": _check_severity(f"duplicate_{col}"),
                "count": int(len(dupes)),
                "pct": round(len(dupes) / len(df) * 100, 2) if len(df) else 0.0,
                "description": f"{len(dupes)} records share duplicate {col} values across {dupes.nunique()} repeated values.",
                "sample_rows": _sample_rows(df, df.index.isin(dupes.index), extra=[col]),
            }
        )

    dup_df = pd.concat(dup_frames, ignore_index=True) if dup_frames else pd.DataFrame()
    return dup_df, results, findings


def _detect_active_zero_salary(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    status_col = _status_column(df)
    if not status_col or "salary" not in df.columns:
        return findings

    statuses = df[status_col].astype(str).str.strip().str.lower()
    salaries = pd.to_numeric(df["salary"], errors="coerce")
    mask = (statuses == "active") & (salaries == 0)
    if mask.any():
        findings.append(
            {
                "section": "CRITICAL_CHECKS",
                "check_key": "active_zero_salary",
                "check_name": "Active employee $0 salary",
                "field": "salary",
                "severity": "CRITICAL",
                "count": int(mask.sum()),
                "pct": round(mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Active employees with a $0 salary were detected. This is a critical payroll and compliance risk.",
                "sample_rows": _sample_rows(df, mask, extra=[status_col, "salary"]),
            }
        )
    return findings


def _detect_suspicious(df: pd.DataFrame, config: dict) -> list[dict]:
    rows: list[dict] = []

    if "salary" in df.columns:
        sal = pd.to_numeric(df["salary"], errors="coerce")
        for val in config["suspicious_salary_values"]:
            mask = sal == float(val)
            if mask.any():
                rows.append(
                    {
                        "section": "SUSPICIOUS",
                        "check_key": "salary_suspicious_default",
                        "check_name": "Suspicious salary default values",
                        "field": "salary",
                        "severity": "HIGH",
                        "value": str(val),
                        "count": int(mask.sum()),
                        "pct": round(mask.sum() / len(df) * 100, 2),
                        "description": f"Salary exactly {val} - common system default",
                        "sample_rows": _sample_rows(df, mask, extra=["salary"]),
                    }
                )

    for col in ["hire_date", "start_date", "date_hired"]:
        if col not in df.columns:
            continue
        dates = df[col].astype(str)
        for prefix in config["suspicious_hire_date_prefixes"]:
            mask = dates.str.startswith(prefix, na=False)
            if mask.any():
                rows.append(
                    {
                        "section": "SUSPICIOUS",
                        "check_key": "hire_date_suspicious_default",
                        "check_name": "Suspicious hire date defaults",
                        "field": col,
                        "severity": "HIGH",
                        "value": prefix,
                        "count": int(mask.sum()),
                        "pct": round(mask.sum() / len(df) * 100, 2),
                        "description": f"{col} starting with {prefix} - possible system default",
                        "sample_rows": _sample_rows(df, mask, extra=[col]),
                    }
                )

    status_col = _status_column(df)
    if status_col:
        statuses = df[status_col].astype(str).str.lower().str.strip()
        for value in config["suspicious_status_values"]:
            mask = statuses == value
            if mask.any():
                rows.append(
                    {
                        "section": "SUSPICIOUS",
                        "check_key": "status_suspicious_value",
                        "check_name": "Suspicious status placeholders",
                        "field": status_col,
                        "severity": "HIGH",
                        "value": value,
                        "count": int(mask.sum()),
                        "pct": round(mask.sum() / len(df) * 100, 2),
                        "description": f"{status_col} value '{value}' appears to be a placeholder or test value",
                        "sample_rows": _sample_rows(df, mask, extra=[status_col]),
                    }
                )

    return rows


def _field_completeness(df: pd.DataFrame, threshold_pct: float) -> tuple[list[dict], list[dict]]:
    rows: list[dict] = []
    findings: list[dict] = []
    total = len(df)
    if total == 0:
        return rows, findings

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
        blank = _blank_mask(series)
        blank_count = int(blank.sum())
        blank_pct = round(blank_count / total * 100, 2)
        filled_pct = round((total - blank_count) / total * 100, 2)
        severity = _completeness_severity(col, blank_pct, threshold_pct * 100)
        rows.append(
            {
                "field": col,
                "total": total,
                "blank_count": blank_count,
                "blank_pct": blank_pct,
                "filled_pct": filled_pct,
                "severity": severity,
            }
        )
        if blank_pct > threshold_pct * 100:
            findings.append(
                {
                    "section": "COMPLETENESS",
                    "check_key": "high_blank_rate",
                    "check_name": f"High blank rate - {col}",
                    "field": col,
                    "severity": _check_severity("high_blank_rate", field=col),
                    "count": blank_count,
                    "pct": blank_pct,
                    "description": f"{col} is blank on {blank_count} rows ({blank_pct}%).",
                    "sample_rows": _sample_rows(df, blank, extra=[col]),
                }
            )

    return rows, findings


def _salary_distribution(df: pd.DataFrame) -> list[dict]:
    if "salary" not in df.columns:
        return []
    total = len(df)
    sal = pd.to_numeric(df["salary"], errors="coerce").dropna()
    if len(sal) == 0:
        return []
    buckets = [
        ("0-1k", 0, 1000),
        ("1k-25k", 1000, 25000),
        ("25k-60k", 25000, 60000),
        ("60k-100k", 60000, 100000),
        ("100k-200k", 100000, 200000),
        ("200k+", 200000, float("inf")),
    ]
    rows = []
    for label, lo, hi in buckets:
        mask = (sal >= lo) & (sal < hi)
        count = int(mask.sum())
        rows.append(
            {
                "section": "SALARY DISTRIBUTION",
                "bucket": label,
                "status_value": "",
                "count": count,
                "pct": round(count / total * 100, 2) if total else 0.0,
            }
        )
    return rows


def _status_distribution(df: pd.DataFrame) -> list[dict]:
    status_col = _status_column(df)
    if not status_col:
        return []
    total = len(df)
    counts = df[status_col].astype(str).str.strip().str.lower().value_counts(dropna=False)
    rows = []
    for status_value, count in counts.items():
        rows.append(
            {
                "section": "STATUS DISTRIBUTION",
                "bucket": "",
                "status_value": status_value,
                "count": int(count),
                "pct": round(int(count) / total * 100, 2) if total else 0.0,
            }
        )
    return rows


def _severity_counts(findings: list[dict]) -> dict[str, int]:
    counts = {key: 0 for key in SEVERITY_ORDER}
    for finding in findings:
        sev = str(finding.get("severity") or "LOW").upper()
        counts[sev] = counts.get(sev, 0) + 1
    return counts


def _issue_strings(findings: list[dict]) -> list[str]:
    out = []
    for finding in findings:
        out.append(
            f"[{finding['severity']}] {finding['check_name']}: {finding['count']} "
            f"({finding['pct']}%) - {finding['description']}"
        )
    return out


def _group_findings_for_pdf(findings: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str], dict] = {}
    for finding in findings:
        key = (finding["check_key"], finding["check_name"])
        if key not in grouped:
            grouped[key] = {
                "check_key": finding["check_key"],
                "check_name": finding["check_name"],
                "severity": finding["severity"],
                "count": 0,
                "description": finding["description"],
                "sample_rows": [],
            }
        grouped[key]["count"] += int(finding.get("count", 0))
        for row in finding.get("sample_rows", []):
            if len(grouped[key]["sample_rows"]) >= 5:
                break
            grouped[key]["sample_rows"].append(row)
    return list(grouped.values())


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    if not rows:
        if fieldnames:
            with path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
            return
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames or list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def run_internal_audit(file_path: Path, out_dir: Path, *, source_name: str | None = None, sheet_name: int | str = 0) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    config = _load_config()

    print(f"[internal_audit] reading: {file_path}")
    try:
        df = _read_input(file_path, sheet_name=sheet_name)
        df = df.replace("", pd.NA)
        df.columns = [str(c).strip() for c in df.columns]
        try:
            from src.mapping import _apply_aliases  # noqa: PLC0415

            df = _apply_aliases(df)
        except Exception:
            pass
    except Exception as exc:
        print(f"[internal_audit] ERROR reading file: {exc}", file=sys.stderr)
        sys.exit(1)

    total_rows = len(df)
    total_cols = len(df.columns)
    print(f"[internal_audit] loaded {total_rows} rows x {total_cols} columns")

    dup_df, dup_summary, dup_findings = _detect_duplicates(df, config["duplicate_check_fields"])
    suspicious = _detect_suspicious(df, config)
    active_zero_findings = _detect_active_zero_salary(df)
    completeness, completeness_findings = _field_completeness(df, config["high_blank_rate_threshold"])
    salary_dist = _salary_distribution(df)
    status_dist = _status_distribution(df)

    findings = dup_findings + active_zero_findings + suspicious + completeness_findings
    severity_counts = _severity_counts(findings)

    total_blank_fields = sum(r["blank_count"] for r in completeness)
    total_possible = total_rows * total_cols
    overall_completeness = round((total_possible - total_blank_fields) / total_possible * 100, 1) if total_possible > 0 else 0.0

    summary = {
        "source_filename": source_name or file_path.name,
        "total_rows": total_rows,
        "total_columns": total_cols,
        "overall_completeness": overall_completeness,
        "duplicate_checks": dup_summary,
        "suspicious_count": len(suspicious),
        "issue_count": len(findings),
        "issues": _issue_strings(findings),
        "columns": list(df.columns),
        "severity_counts": severity_counts,
        "findings": findings,
        "findings_for_pdf": _group_findings_for_pdf(findings),
    }

    report_json = out_dir / "internal_audit_report.json"
    report_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[internal_audit] wrote: {report_json}")

    blanks_csv = out_dir / "internal_audit_blanks.csv"
    _write_csv(
        blanks_csv,
        completeness,
        fieldnames=["field", "total", "blank_count", "blank_pct", "filled_pct", "severity"],
    )
    print(f"[internal_audit] wrote: {blanks_csv}")

    if not dup_df.empty:
        dup_csv = out_dir / "internal_audit_duplicates.csv"
        dup_df.to_csv(dup_csv, index=False)
        print(f"[internal_audit] wrote: {dup_csv} ({len(dup_df)} rows)")

    suspicious_csv = out_dir / "internal_audit_suspicious.csv"
    suspicious_rows = [
        {
            "check_name": row["check_name"],
            "field": row.get("field", ""),
            "value": row.get("value", ""),
            "count": row["count"],
            "pct": row["pct"],
            "severity": row["severity"],
            "description": row["description"],
        }
        for row in suspicious + active_zero_findings
    ]
    _write_csv(
        suspicious_csv,
        suspicious_rows,
        fieldnames=["check_name", "field", "value", "count", "pct", "severity", "description"],
    )
    print(f"[internal_audit] wrote: {suspicious_csv}")

    distributions_csv = out_dir / "internal_audit_distributions.csv"
    _write_csv(
        distributions_csv,
        salary_dist + status_dist,
        fieldnames=["section", "bucket", "status_value", "count", "pct"],
    )
    print(f"[internal_audit] wrote: {distributions_csv}")

    report_rows = [
        {"section": "SUMMARY", "field": "total_rows", "value": str(total_rows), "note": "", "severity": "LOW"},
        {"section": "SUMMARY", "field": "total_columns", "value": str(total_cols), "note": "", "severity": "LOW"},
        {"section": "SUMMARY", "field": "overall_completeness_pct", "value": str(overall_completeness), "note": "", "severity": "LOW"},
        {
            "section": "SUMMARY",
            "field": "severity_counts",
            "value": json.dumps(severity_counts),
            "note": "",
            "severity": "LOW",
        },
    ]

    for finding in findings:
        report_rows.append(
            {
                "section": finding["section"],
                "field": finding["check_name"],
                "value": f"{finding['count']} ({finding['pct']}%)",
                "note": finding["description"],
                "severity": finding["severity"],
            }
        )

    for row in completeness:
        report_rows.append(
            {
                "section": "COMPLETENESS",
                "field": row["field"],
                "value": f"{row['filled_pct']}% filled",
                "note": f"{row['blank_count']} blank of {row['total']}",
                "severity": row["severity"],
            }
        )

    report_csv = out_dir / "internal_audit_report.csv"
    _write_csv(report_csv, report_rows, fieldnames=["section", "field", "value", "note", "severity"])
    print(f"[internal_audit] wrote: {report_csv}")
    print(f"[internal_audit] complete. {len(findings)} issues found.")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Single-file internal data quality audit")
    parser.add_argument("--file", required=True, help="Path to the file to audit")
    parser.add_argument("--out-dir", required=True, help="Directory to write outputs to")
    parser.add_argument("--source-name", default="", help="Original uploaded filename for report display")
    parser.add_argument("--sheet-name", default="0", help="Excel sheet name or index (default: 0)")
    args = parser.parse_args()

    sheet_name: int | str = int(args.sheet_name) if str(args.sheet_name).lstrip("-").isdigit() else args.sheet_name
    run_internal_audit(
        Path(args.file),
        Path(args.out_dir),
        source_name=args.source_name or None,
        sheet_name=sheet_name,
    )
    sys.exit(0)
