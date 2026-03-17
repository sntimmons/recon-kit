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
from datetime import datetime
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
DEFAULT_SALARY_OUTLIER_THRESHOLD = 2.5
DEFAULT_SALARY_OUTLIER_MIN_DEPT_SIZE = 5
DEFAULT_PAY_EQUITY_VARIANCE_THRESHOLD = 0.30
DEFAULT_PAY_EQUITY_MIN_GROUP_SIZE = 3

SEVERITY_ORDER = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

# Column alias map: normalized_source_name -> canonical_name
# All checks use df_norm (built from this map). Original df used only for raw CSV output.
ALIASES: dict[str, str] = {
    "employee_id": "worker_id",
    "emp_id": "worker_id",
    "associate_id": "worker_id",
    "staff_id": "worker_id",
    "status": "worker_status",
    "employment_status": "worker_status",
    "emp_status": "worker_status",
    "salary": "salary",
    "annual_salary": "salary",
    "base_salary": "salary",
    "annual_base_pay": "salary",
    "email": "email",
    "email_address": "email",
    "join_date": "hire_date",
    "start_date": "hire_date",
    "date_hired": "hire_date",
    "original_hire_date": "hire_date",
    "phone": "phone",
    "phone_number": "phone",
    "mobile": "phone",
    "age": "age",
    "dob": "date_of_birth",
    "department": "department",
    "dept": "department",
    "business_unit": "department",
    "department_region": "department",
    "manager_id": "manager_id",
    "manager_worker_id": "manager_id",
    "first_name": "first_name",
    "fname": "first_name",
    "last_name": "last_name",
    "lname": "last_name",
}


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


def _check_catalog() -> list[dict]:
    return [
        {"check_key": "duplicate_worker_id", "check_name": "Duplicate worker_id", "severity": "CRITICAL"},
        {"check_key": "duplicate_email", "check_name": "Duplicate email", "severity": "MEDIUM"},
        {"check_key": "duplicate_last4_ssn", "check_name": "Duplicate last4_ssn", "severity": "CRITICAL"},
        {"check_key": "active_zero_salary", "check_name": "Active employees with $0 or missing salary", "severity": "CRITICAL"},
        {"check_key": "salary_suspicious_default", "check_name": "Suspicious salary default values", "severity": "HIGH"},
        {"check_key": "hire_date_suspicious_default", "check_name": "Suspicious hire date defaults", "severity": "HIGH"},
        {"check_key": "status_suspicious_value", "check_name": "Suspicious status placeholders", "severity": "HIGH"},
        {"check_key": "impossible_dates", "check_name": "Impossible dates", "severity": "HIGH"},
        {"check_key": "status_hire_date_mismatch", "check_name": "Status and termination-date mismatches", "severity": "HIGH"},
        {"check_key": "missing_manager", "check_name": "Active employees with no manager", "severity": "MEDIUM"},
        {"check_key": "manager_loop", "check_name": "Manager reporting loops", "severity": "HIGH"},
        {"check_key": "salary_outlier", "check_name": "Salary outliers by department", "severity": "MEDIUM"},
        {"check_key": "pay_equity_flag", "check_name": "Pay equity variance flags", "severity": "HIGH"},
        {"check_key": "ghost_employee_indicator", "check_name": "Ghost employee indicators", "severity": "CRITICAL"},
        {"check_key": "duplicate_name_different_id", "check_name": "Duplicate names with different IDs", "severity": "MEDIUM"},
        {"check_key": "suspicious_round_salary", "check_name": "Suspicious round number salaries", "severity": "LOW"},
        {"check_key": "phone_invalid", "check_name": "Invalid phone numbers", "severity": "HIGH"},
        {"check_key": "status_no_terminated", "check_name": "No terminated employees found", "severity": "MEDIUM"},
        {"check_key": "status_high_pending", "check_name": "Unusually high Pending status rate", "severity": "MEDIUM"},
        {"check_key": "age_uniformity", "check_name": "Age uniformity - possible placeholder data", "severity": "MEDIUM"},
        {"check_key": "combined_field", "check_name": "Combined field detected", "severity": "LOW"},
    ]


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
        "salary_outlier_threshold": float(cfg.get("salary_outlier_threshold", DEFAULT_SALARY_OUTLIER_THRESHOLD) or DEFAULT_SALARY_OUTLIER_THRESHOLD),
        "salary_outlier_min_dept_size": int(cfg.get("salary_outlier_min_dept_size", DEFAULT_SALARY_OUTLIER_MIN_DEPT_SIZE) or DEFAULT_SALARY_OUTLIER_MIN_DEPT_SIZE),
        "pay_equity_variance_threshold": float(cfg.get("pay_equity_variance_threshold", DEFAULT_PAY_EQUITY_VARIANCE_THRESHOLD) or DEFAULT_PAY_EQUITY_VARIANCE_THRESHOLD),
        "pay_equity_min_group_size": int(cfg.get("pay_equity_min_group_size", DEFAULT_PAY_EQUITY_MIN_GROUP_SIZE) or DEFAULT_PAY_EQUITY_MIN_GROUP_SIZE),
        "ghost_employee_check": bool(cfg.get("ghost_employee_check", True)),
        "manager_loop_check": bool(cfg.get("manager_loop_check", True)),
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


def _first_present(df: pd.DataFrame, names: list[str]) -> str | None:
    lowered = {str(col).strip().lower(): col for col in df.columns}
    for col in names:
        if col in df.columns:
            return col
        match = lowered.get(str(col).strip().lower())
        if match:
            return match
    return None


def _name_columns(df: pd.DataFrame) -> list[str]:
    cols = []
    for col in ["full_name", "first_name", "last_name"]:
        if col in df.columns:
            cols.append(col)
    return cols


def _record_label(row: pd.Series) -> str:
    for col in ["worker_id", "full_name", "first_name", "last_name", "email"]:
        val = _norm_str(row.get(col))
        if val:
            return val
    return "Unknown"


def _numeric_series(df: pd.DataFrame, col: str | None) -> pd.Series:
    if not col or col not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def _date_series(df: pd.DataFrame, col: str | None) -> pd.Series:
    if not col or col not in df.columns:
        return pd.Series([pd.NaT] * len(df), index=df.index)
    return pd.to_datetime(df[col], errors="coerce")


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
    if check_key in {
        "duplicate_worker_id",
        "duplicate_last4_ssn",
        "active_zero_salary",
        "ghost_employee_indicator",
    }:
        return "CRITICAL"
    if check_key in {
        "salary_suspicious_default",
        "hire_date_suspicious_default",
        "status_suspicious_value",
        "impossible_dates",
        "status_hire_date_mismatch",
        "manager_loop",
        "pay_equity_flag",
    }:
        return "HIGH"
    if check_key in {
        "duplicate_email", "missing_manager", "salary_outlier", "duplicate_name_different_id",
        "status_no_terminated", "status_high_pending", "age_uniformity",
    }:
        return "MEDIUM"
    if check_key == "phone_invalid":
        return "HIGH"
    if check_key in {"suspicious_round_salary", "combined_field"}:
        return "LOW"
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
    salary_blank = _blank_mask(df["salary"])
    mask = (statuses == "active") & ((salaries == 0) | salary_blank)
    if mask.any():
        findings.append(
            {
                "section": "CRITICAL_CHECKS",
                "check_key": "active_zero_salary",
                "check_name": "Active employees with $0 or missing salary",
                "field": "salary",
                "severity": "CRITICAL",
                "count": int(mask.sum()),
                "pct": round(mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Active employee has $0 or missing salary - likely a data entry error",
                "sample_rows": _sample_rows(df, mask, extra=[status_col, "salary"]),
            }
        )
    return findings


def _detect_impossible_dates(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    hire_col = _first_present(df, ["hire_date", "start_date", "date_hired"])
    dob_col = _first_present(df, ["date_of_birth", "dob", "birth_date"])
    term_col = _first_present(df, ["termination_date", "term_date", "end_date"])
    status_col = _status_column(df)

    today = pd.Timestamp(datetime.now().date())
    hire_dates = _date_series(df, hire_col)
    dob_dates = _date_series(df, dob_col)
    term_dates = _date_series(df, term_col)
    status_norm = df[status_col].astype(str).str.strip().str.lower() if status_col else pd.Series("", index=df.index)

    rows: list[dict] = []
    if hire_col:
        masks = [
            (hire_dates > today, hire_col, "Hire date is in the future"),
            (hire_dates < pd.Timestamp("1950-01-01"), hire_col, "Hire date is before 1950-01-01"),
        ]
        for mask, field_name, reason in masks:
            for idx in df.index[mask.fillna(False)]:
                rows.append(
                    {
                        "row_number": int(idx) + 2,
                        "worker_id": _safe_json_value(df.at[idx, "worker_id"]) if "worker_id" in df.columns else "",
                        "field_name": field_name,
                        "field_value": _safe_json_value(df.at[idx, field_name]),
                        "why_flagged": reason,
                    }
                )
    if dob_col:
        old_mask = dob_dates <= (today - pd.Timedelta(days=365 * 100))
        young_mask = dob_dates >= (today - pd.Timedelta(days=365 * 16))
        masks = [
            (old_mask, dob_col, "DOB implies age over 100"),
            (young_mask, dob_col, "DOB implies age under 16"),
        ]
        if hire_col:
            masks.append((dob_dates > hire_dates, dob_col, "DOB is after hire date"))
        for mask, field_name, reason in masks:
            for idx in df.index[mask.fillna(False)]:
                rows.append(
                    {
                        "row_number": int(idx) + 2,
                        "worker_id": _safe_json_value(df.at[idx, "worker_id"]) if "worker_id" in df.columns else "",
                        "field_name": field_name,
                        "field_value": _safe_json_value(df.at[idx, field_name]),
                        "why_flagged": reason,
                    }
                )
    if term_col:
        masks = []
        if hire_col:
            masks.append((term_dates < hire_dates, term_col, "Termination date is before hire date"))
        if status_col:
            masks.append(((term_dates > today) & (status_norm == "terminated"), term_col, "Termination date is in the future while status is Terminated"))
        for mask, field_name, reason in masks:
            for idx in df.index[mask.fillna(False)]:
                rows.append(
                    {
                        "row_number": int(idx) + 2,
                        "worker_id": _safe_json_value(df.at[idx, "worker_id"]) if "worker_id" in df.columns else "",
                        "field_name": field_name,
                        "field_value": _safe_json_value(df.at[idx, field_name]),
                        "why_flagged": reason,
                    }
                )

    if rows:
        findings.append(
            {
                "section": "DATE_CHECKS",
                "check_key": "impossible_dates",
                "check_name": "Impossible dates",
                "field": hire_col or dob_col or term_col or "",
                "severity": "HIGH",
                "count": len(rows),
                "pct": round(len(rows) / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Dates were found that are impossible or internally inconsistent.",
                "sample_rows": rows[:5],
            }
        )
    return findings


def _detect_status_hire_date_mismatch(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    status_col = _status_column(df)
    term_col = _first_present(df, ["termination_date", "term_date", "end_date"])
    if not status_col or not term_col:
        return findings

    statuses = df[status_col].astype(str).str.strip().str.lower()
    term_blank = _blank_mask(df[term_col])
    active_with_term = (statuses == "active") & ~term_blank
    terminated_without_term = (statuses == "terminated") & term_blank

    if active_with_term.any():
        findings.append(
            {
                "section": "STATUS_CHECKS",
                "check_key": "status_hire_date_mismatch",
                "check_name": "Termination date but Active status",
                "field": status_col,
                "severity": "HIGH",
                "count": int(active_with_term.sum()),
                "pct": round(active_with_term.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Employee has termination date but is marked Active",
                "sample_rows": _sample_rows(df, active_with_term, extra=[status_col, term_col]),
            }
        )
    if terminated_without_term.any():
        findings.append(
            {
                "section": "STATUS_CHECKS",
                "check_key": "status_hire_date_mismatch",
                "check_name": "Terminated status without termination date",
                "field": status_col,
                "severity": "HIGH",
                "count": int(terminated_without_term.sum()),
                "pct": round(terminated_without_term.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Employee is marked Terminated but has no termination date",
                "sample_rows": _sample_rows(df, terminated_without_term, extra=[status_col, term_col]),
            }
        )
    return findings


def _detect_missing_manager(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    status_col = _status_column(df)
    manager_col = _first_present(df, ["manager_id", "manager", "supervisor_id", "Manager_ID", "ManagerEmployeeID", "Manager_Worker_ID"])
    if not status_col or not manager_col:
        return findings
    statuses = df[status_col].astype(str).str.strip().str.lower()
    mask = (statuses == "active") & _blank_mask(df[manager_col])
    if mask.any():
        findings.append(
            {
                "section": "ORG_CHECKS",
                "check_key": "missing_manager",
                "check_name": "Active employees with no manager",
                "field": manager_col,
                "severity": "MEDIUM",
                "count": int(mask.sum()),
                "pct": round(mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Active employee has no manager assigned",
                "sample_rows": _sample_rows(df, mask, extra=[manager_col, _first_present(df, ["department", "business_unit", "dept", "district"])]),
            }
        )
    return findings


def _detect_manager_loops(df: pd.DataFrame, enabled: bool) -> list[dict]:
    findings: list[dict] = []
    worker_col = _first_present(df, ["worker_id"])
    manager_col = _first_present(df, ["manager_id", "manager", "supervisor_id", "Manager_ID", "ManagerEmployeeID", "Manager_Worker_ID"])
    if not enabled or not worker_col or not manager_col:
        return findings

    worker_ids = df[worker_col].astype(str).str.strip()
    manager_ids = df[manager_col].astype(str).str.strip()
    links = {}
    for idx in df.index:
        wid = worker_ids.at[idx]
        mid = manager_ids.at[idx]
        if wid and wid.lower() != "nan" and mid and mid.lower() != "nan":
            links[wid] = mid

    cycles: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()

    for wid, mid in links.items():
        if wid == mid:
            key = tuple(sorted([wid]))
            if key not in seen:
                seen.add(key)
                cycles.append([wid])
            continue
        if mid in links and links.get(mid) == wid:
            key = tuple(sorted([wid, mid]))
            if key not in seen:
                seen.add(key)
                cycles.append([wid, mid])
        mid2 = links.get(mid)
        if mid2 and mid2 in links and links.get(mid2) == wid:
            key = tuple(sorted([wid, mid, mid2]))
            if key not in seen:
                seen.add(key)
                cycles.append([wid, mid, mid2])

    if cycles:
        sample_rows = [
            {
                "cycle_length": len(cycle),
                "employee_ids": " -> ".join(cycle + [cycle[0]]) if len(cycle) > 1 else cycle[0],
            }
            for cycle in cycles[:5]
        ]
        findings.append(
            {
                "section": "ORG_CHECKS",
                "check_key": "manager_loop",
                "check_name": "Manager reporting loops",
                "field": manager_col,
                "severity": "HIGH",
                "count": len(cycles),
                "pct": round(len(cycles) / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Manager reporting loop detected - these employees report to each other",
                "sample_rows": sample_rows,
            }
        )
    return findings


def _detect_salary_outliers(df: pd.DataFrame, config: dict) -> list[dict]:
    findings: list[dict] = []
    dept_col = _first_present(df, ["department", "business_unit", "dept", "district", "Department_Name"])
    if "salary" not in df.columns or not dept_col or len(df) < 10:
        return findings

    salary = pd.to_numeric(df["salary"], errors="coerce")
    dept = df[dept_col].astype(str).str.strip()
    threshold = float(config["salary_outlier_threshold"])
    min_size = int(config["salary_outlier_min_dept_size"])

    rows = []
    for dept_name, idxs in dept.groupby(dept).groups.items():
        if not dept_name or dept_name.lower() == "nan":
            continue
        dept_salary = salary.loc[idxs].dropna()
        if len(dept_salary) < min_size:
            continue
        median = float(dept_salary.median())
        if median <= 0:
            continue
        high_mask = salary.loc[idxs] > (median * threshold)
        low_mask = salary.loc[idxs] < (median * 0.3)
        flagged = salary.loc[idxs][(high_mask | low_mask).fillna(False)]
        for idx, value in flagged.items():
            ratio = float(value) / median if median else 0.0
            rows.append(
                {
                    "row_number": int(idx) + 2,
                    "worker_id": _safe_json_value(df.at[idx, "worker_id"]) if "worker_id" in df.columns else "",
                    "department": dept_name,
                    "salary": _safe_json_value(df.at[idx, "salary"]),
                    "median_salary": f"{median:.2f}",
                    "note": f"Salary is {ratio:.2f}x the department median - verify this is correct",
                }
            )
    if rows:
        findings.append(
            {
                "section": "SALARY_CHECKS",
                "check_key": "salary_outlier",
                "check_name": "Salary outliers by department",
                "field": dept_col,
                "severity": "MEDIUM",
                "count": len(rows),
                "pct": round(len(rows) / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Salary outliers were detected relative to department medians.",
                "sample_rows": rows[:5],
            }
        )
    return findings


def _detect_pay_equity_flags(df: pd.DataFrame, config: dict) -> list[dict]:
    findings: list[dict] = []
    title_col = _first_present(df, ["job_title", "position", "position_title", "Job_Title"])
    dept_col = _first_present(df, ["department", "business_unit", "dept", "district", "Department_Name"])
    if "salary" not in df.columns or not title_col or not dept_col:
        return findings

    salary = pd.to_numeric(df["salary"], errors="coerce")
    min_size = int(config["pay_equity_min_group_size"])
    threshold = float(config["pay_equity_variance_threshold"])
    group_df = pd.DataFrame({
        "salary": salary,
        "title": df[title_col].astype(str).str.strip(),
        "department": df[dept_col].astype(str).str.strip(),
    })
    group_df = group_df.dropna(subset=["salary"])
    group_df = group_df[(group_df["title"] != "") & (group_df["department"] != "")]
    if group_df.empty:
        return findings

    group_rows = []
    for (title, dept_name), grp in group_df.groupby(["title", "department"]):
        if len(grp) < min_size:
            continue
        median = float(grp["salary"].median())
        if median <= 0:
            continue
        variance = (float(grp["salary"].max()) - float(grp["salary"].min())) / median
        if variance > threshold:
            group_rows.append(
                {
                    "title": title,
                    "department": dept_name,
                    "employees": int(len(grp)),
                    "min_salary": f"{float(grp['salary'].min()):.2f}",
                    "max_salary": f"{float(grp['salary'].max()):.2f}",
                    "median_salary": f"{median:.2f}",
                    "variance_pct": f"{variance * 100:.1f}%",
                }
            )

    if group_rows:
        findings.append(
            {
                "section": "PAY_EQUITY",
                "check_key": "pay_equity_flag",
                "check_name": "Pay equity variance flags",
                "field": title_col,
                "severity": "HIGH",
                "count": len(group_rows),
                "pct": round(len(group_rows) / max(len(group_df), 1) * 100, 2),
                "description": "Salary variance of more than 30% within the same title and department - review for pay equity compliance",
                "sample_rows": group_rows[:5],
                "group_rows": group_rows,
            }
        )
    return findings


def _detect_ghost_employees(df: pd.DataFrame, enabled: bool) -> list[dict]:
    findings: list[dict] = []
    status_col = _status_column(df)
    dept_col = _first_present(df, ["department", "business_unit", "dept", "district", "Department_Name"])
    manager_col = _first_present(df, ["manager_id", "manager", "supervisor_id", "Manager_ID", "ManagerEmployeeID", "Manager_Worker_ID"])
    hire_col = _first_present(df, ["hire_date", "start_date", "date_hired"])
    if not enabled or not status_col or "salary" not in df.columns or not dept_col or not manager_col or not hire_col:
        return findings

    statuses = df[status_col].astype(str).str.strip().str.lower()
    salaries = pd.to_numeric(df["salary"], errors="coerce")
    mask = (
        (statuses == "active")
        & ((salaries == 0) | _blank_mask(df["salary"]))
        & _blank_mask(df[dept_col])
        & _blank_mask(df[manager_col])
        & ~_blank_mask(df[hire_col])
    )
    if mask.any():
        findings.append(
            {
                "section": "CRITICAL_CHECKS",
                "check_key": "ghost_employee_indicator",
                "check_name": "Ghost employee indicators",
                "field": status_col,
                "severity": "CRITICAL",
                "count": int(mask.sum()),
                "pct": round(mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Possible ghost employee - active status with no salary, no department, and no manager",
                "sample_rows": _sample_rows(df, mask, extra=[status_col, "salary", dept_col, manager_col, hire_col]),
            }
        )
    return findings


def _detect_duplicate_name_different_id(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    worker_col = _first_present(df, ["worker_id"])
    first_col = _first_present(df, ["first_name"])
    last_col = _first_present(df, ["last_name"])
    full_name_col = _first_present(df, ["full_name"])
    if not worker_col or ((not first_col or not last_col) and not full_name_col):
        return findings

    working = pd.DataFrame(index=df.index)
    if full_name_col:
        working["name_key"] = df[full_name_col].astype(str).str.strip().str.lower()
    else:
        working["name_key"] = (
            df[first_col].astype(str).str.strip().str.lower() + "|" + df[last_col].astype(str).str.strip().str.lower()
        )
    working["worker_id"] = df[worker_col].astype(str).str.strip()
    working = working[(working["name_key"] != "") & (working["worker_id"] != "")]
    if working.empty:
        return findings

    rows = []
    for _, grp in working.groupby("name_key"):
        if grp["worker_id"].nunique() > 1 and len(grp) > 1:
            sample_idx = grp.index[:5]
            for idx in sample_idx:
                rows.append(
                    {
                        "row_number": int(idx) + 2,
                        "worker_id": _safe_json_value(df.at[idx, worker_col]),
                        "name": _safe_json_value(df.at[idx, full_name_col]) if full_name_col else f"{_safe_json_value(df.at[idx, first_col])} {_safe_json_value(df.at[idx, last_col])}".strip(),
                    }
                )
    if rows:
        findings.append(
            {
                "section": "IDENTITY_CHECKS",
                "check_key": "duplicate_name_different_id",
                "check_name": "Duplicate names with different IDs",
                "field": worker_col,
                "severity": "MEDIUM",
                "count": len(rows),
                "pct": round(len(rows) / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Two employees share the same name with different IDs - verify these are different people",
                "sample_rows": rows[:5],
            }
        )
    return findings


def _detect_suspicious_round_salary(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    status_col = _status_column(df)
    dept_col = _first_present(df, ["department", "business_unit", "dept", "district", "Department_Name"])
    if not status_col or "salary" not in df.columns:
        return findings

    statuses = df[status_col].astype(str).str.strip().str.lower()
    active_mask = statuses == "active"
    salary = pd.to_numeric(df["salary"], errors="coerce")
    valid = salary[active_mask & salary.notna()]
    if valid.empty:
        return findings

    repeated_counts = valid.value_counts()
    exact_values = {1, 10, 100, 1000, 10000, 12345, 99999, 999999}
    dept_medians = {}
    if dept_col:
        dept_series = df[dept_col].astype(str).str.strip()
        for dept_name, idxs in dept_series.groupby(dept_series).groups.items():
            dept_salary = salary.loc[idxs].dropna()
            if dept_name and dept_name.lower() != "nan" and not dept_salary.empty:
                dept_medians[dept_name] = float(dept_salary.median())
    rows = []
    for idx in df.index[active_mask.fillna(False)]:
        value = salary.at[idx]
        if pd.isna(value):
            continue
        count = int(repeated_counts.get(value, 0))
        if count <= 3:
            continue
        int_value = int(value)
        is_exact = int_value in exact_values
        is_round = str(int_value).endswith("00000")
        if not is_exact and not is_round:
            continue
        significant = True
        if dept_col:
            dept_name = _norm_str(df.at[idx, dept_col])
            median = dept_medians.get(dept_name)
            if median and median > 0:
                significant = abs(float(value) - median) / median > 0.2
        if not significant:
            continue
        rows.append(
            {
                "row_number": int(idx) + 2,
                "worker_id": _safe_json_value(df.at[idx, "worker_id"]) if "worker_id" in df.columns else "",
                "salary": _safe_json_value(df.at[idx, "salary"]),
                "repeated_count": count,
            }
        )
    if rows:
        findings.append(
            {
                "section": "SALARY_CHECKS",
                "check_key": "suspicious_round_salary",
                "check_name": "Suspicious round number salaries",
                "field": "salary",
                "severity": "LOW",
                "count": len(rows),
                "pct": round(len(rows) / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Salary appears to be a placeholder value - verify this is correct",
                "sample_rows": rows[:5],
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


def _detect_phone_invalid(df: pd.DataFrame) -> list[dict]:
    """Flag phone numbers that are impossible values (negative, zero, wrong digit count, repeated)."""
    findings: list[dict] = []
    if "phone" not in df.columns:
        return findings
    blank = _blank_mask(df["phone"])
    col = df["phone"].astype(str).str.strip()
    nonnull = col[~blank]
    if nonnull.empty:
        return findings

    flagged_idx: list[int] = []
    for idx, val in nonnull.items():
        try:
            num = float(val)
            digits = val.replace("-", "").replace("+", "").replace(".", "").replace(" ", "")
            digit_only = "".join(c for c in digits if c.isdigit())
            n_digits = len(digit_only)
            is_negative = num < 0
            is_zero = num == 0
            too_short = n_digits < 7
            too_long = n_digits > 15
            all_same = len(set(digit_only)) == 1 if digit_only else False
            if is_negative or is_zero or too_short or too_long or all_same:
                flagged_idx.append(idx)
        except (ValueError, TypeError):
            pass

    if flagged_idx:
        mask = df.index.isin(flagged_idx)
        # get sample value for description
        sample_val = df.at[flagged_idx[0], "phone"] if flagged_idx else ""
        findings.append(
            {
                "section": "CONTACT_CHECKS",
                "check_key": "phone_invalid",
                "check_name": "Invalid phone numbers",
                "field": "phone",
                "severity": "HIGH",
                "count": len(flagged_idx),
                "pct": round(len(flagged_idx) / len(df) * 100, 2) if len(df) else 0.0,
                "description": f"{len(flagged_idx)} phone numbers contain impossible values (e.g. {sample_val}). Cannot be real phone numbers.",
                "sample_rows": _sample_rows(df, mask, extra=["phone"]),
                "_sample_value": str(sample_val),
            }
        )
    return findings


def _detect_status_no_terminated(df: pd.DataFrame) -> list[dict]:
    """Flag if a file of 50+ records has zero terminated employees."""
    findings: list[dict] = []
    status_col = _status_column(df)
    if not status_col or len(df) <= 50:
        return findings
    statuses = df[status_col].astype(str).str.strip().str.lower()
    term_keywords = {"terminated", "term", "separated", "resigned", "dismissed"}
    has_terminated = statuses.isin(term_keywords).any()
    if not has_terminated:
        n = len(df)
        findings.append(
            {
                "section": "STATUS_CHECKS",
                "check_key": "status_no_terminated",
                "check_name": "No terminated employees found",
                "field": status_col,
                "severity": "MEDIUM",
                "count": 1,
                "pct": 0.0,
                "description": f"No terminated employees found in {n:,} records. File may be incomplete or limited to active/pending only.",
                "sample_rows": [],
            }
        )
    return findings


def _detect_status_high_pending(df: pd.DataFrame) -> list[dict]:
    """Flag if more than 25% of records have status = Pending."""
    findings: list[dict] = []
    status_col = _status_column(df)
    if not status_col:
        return findings
    statuses = df[status_col].astype(str).str.strip().str.lower()
    total = len(statuses)
    if total == 0:
        return findings
    pending_count = int((statuses == "pending").sum())
    pct = round(pending_count / total * 100, 1)
    if pct > 25:
        findings.append(
            {
                "section": "STATUS_CHECKS",
                "check_key": "status_high_pending",
                "check_name": "Unusually high Pending status rate",
                "field": status_col,
                "severity": "MEDIUM",
                "count": pending_count,
                "pct": pct,
                "description": f"{pct}% of employees ({pending_count:,} of {total:,}) have Pending status - unusually high. Indicates incomplete data entry.",
                "sample_rows": _sample_rows(df, statuses == "pending", extra=[status_col]),
            }
        )
    return findings


def _detect_age_uniformity(df: pd.DataFrame) -> list[dict]:
    """Flag if age column has suspiciously few distinct values - likely placeholder data."""
    findings: list[dict] = []
    if "age" not in df.columns:
        return findings
    blank = _blank_mask(df["age"])
    age_raw = df["age"].astype(str).str.strip()[~blank]
    nonnull = age_raw
    if len(nonnull) < 50:
        return findings
    unique_count = nonnull.nunique()
    if unique_count / len(nonnull) < 0.05:
        value_list = sorted(nonnull.unique().tolist())[:10]
        value_str = ", ".join(str(v) for v in value_list)
        findings.append(
            {
                "section": "DATA_QUALITY",
                "check_key": "age_uniformity",
                "check_name": "Age uniformity - possible placeholder data",
                "field": "age",
                "severity": "MEDIUM",
                "count": len(nonnull),
                "pct": round(len(nonnull) / len(df) * 100, 2) if len(df) else 0.0,
                "description": f"Only {unique_count} distinct age values across {len(nonnull):,} employees. Values: {value_str}. Likely placeholder data.",
                "sample_rows": [],
                "_unique_count": unique_count,
                "_value_list": value_list,
            }
        )
    return findings


def _detect_combined_field(df: pd.DataFrame) -> list[dict]:
    """Flag text columns where 80%+ of values use a hyphen to combine two categorical fields."""
    findings: list[dict] = []
    for col in df.columns:
        blank = _blank_mask(df[col])
        series = df[col].astype(str).str.strip()
        nonnull = series[~blank]
        if len(nonnull) < 20:
            continue
        has_hyphen = nonnull.str.contains("-", regex=False)
        if has_hyphen.mean() < 0.80:
            continue
        # Both parts must be categorical (< 20 unique values each)
        with_hyphen = nonnull[has_hyphen]
        left_parts = with_hyphen.str.split("-", n=1).str[0].str.strip()
        right_parts = with_hyphen.str.split("-", n=1).str[1].str.strip()
        if left_parts.nunique() >= 20 or right_parts.nunique() >= 20:
            continue
        example = with_hyphen.iloc[0] if not with_hyphen.empty else ""
        findings.append(
            {
                "section": "DATA_QUALITY",
                "check_key": "combined_field",
                "check_name": f"Combined field detected - {col}",
                "field": col,
                "severity": "LOW",
                "count": int(has_hyphen.sum()),
                "pct": round(has_hyphen.mean() * 100, 1),
                "description": f"Column '{col}' appears to combine two fields using a hyphen separator (e.g. '{example}'). Must be split before system load.",
                "sample_rows": [],
                "_example": str(example),
                "_col": col,
            }
        )
    return findings


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


def _check_counts(findings: list[dict]) -> list[dict]:
    counts_by_key: dict[str, int] = {}
    for finding in findings:
        key = str(finding.get("check_key") or "")
        counts_by_key[key] = counts_by_key.get(key, 0) + int(finding.get("count", 0) or 0)
    rows = []
    for item in _check_catalog():
        rows.append(
            {
                "check_key": item["check_key"],
                "check_name": item["check_name"],
                "severity": item["severity"],
                "count": counts_by_key.get(item["check_key"], 0),
            }
        )
    return rows


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
                "group_rows": [],
            }
        grouped[key]["count"] += int(finding.get("count", 0))
        for row in finding.get("sample_rows", []):
            if len(grouped[key]["sample_rows"]) >= 5:
                break
            grouped[key]["sample_rows"].append(row)
        for row in finding.get("group_rows", []):
            if len(grouped[key]["group_rows"]) >= 20:
                break
            grouped[key]["group_rows"].append(row)
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

    # Build normalized column map: source col -> canonical name
    _col_norm_map: dict[str, str] = {}
    for _col in df.columns:
        _cn = str(_col).strip().lower().replace(" ", "_")
        _col_norm_map[_col] = ALIASES.get(_cn, _cn)
    df_norm = df.rename(columns=_col_norm_map)

    total_rows = len(df_norm)
    total_cols = len(df_norm.columns)
    print(f"[internal_audit] loaded {total_rows} rows x {total_cols} columns")

    dup_df, dup_summary, dup_findings = _detect_duplicates(df_norm, config["duplicate_check_fields"])
    suspicious = _detect_suspicious(df_norm, config)
    active_zero_findings = _detect_active_zero_salary(df_norm)
    impossible_date_findings = _detect_impossible_dates(df_norm)
    status_mismatch_findings = _detect_status_hire_date_mismatch(df_norm)
    missing_manager_findings = _detect_missing_manager(df_norm)
    manager_loop_findings = _detect_manager_loops(df_norm, config["manager_loop_check"])
    salary_outlier_findings = _detect_salary_outliers(df_norm, config)
    pay_equity_findings = _detect_pay_equity_flags(df_norm, config)
    ghost_employee_findings = _detect_ghost_employees(df_norm, config["ghost_employee_check"])
    duplicate_name_findings = _detect_duplicate_name_different_id(df_norm)
    suspicious_round_salary_findings = _detect_suspicious_round_salary(df_norm)
    phone_findings = _detect_phone_invalid(df_norm)
    status_no_term_findings = _detect_status_no_terminated(df_norm)
    status_high_pending_findings = _detect_status_high_pending(df_norm)
    age_uniformity_findings = _detect_age_uniformity(df_norm)
    combined_field_findings = _detect_combined_field(df_norm)
    completeness, completeness_findings = _field_completeness(df_norm, config["high_blank_rate_threshold"])
    salary_dist = _salary_distribution(df_norm)
    status_dist = _status_distribution(df_norm)

    findings = (
        dup_findings
        + active_zero_findings
        + phone_findings
        + suspicious
        + impossible_date_findings
        + status_mismatch_findings
        + status_no_term_findings
        + status_high_pending_findings
        + missing_manager_findings
        + manager_loop_findings
        + salary_outlier_findings
        + pay_equity_findings
        + ghost_employee_findings
        + duplicate_name_findings
        + suspicious_round_salary_findings
        + age_uniformity_findings
        + combined_field_findings
        + completeness_findings
    )
    severity_counts = _severity_counts(findings)
    check_counts = _check_counts(findings)

    total_blank_fields = sum(r["blank_count"] for r in completeness)
    total_possible = total_rows * total_cols
    overall_completeness = round((total_possible - total_blank_fields) / total_possible * 100, 1) if total_possible > 0 else 0.0

    # Build status distribution dict for PDF workforce snapshot
    status_col = _status_column(df_norm)
    status_breakdown: dict[str, int] = {}
    if status_col:
        status_breakdown = df_norm[status_col].astype(str).str.strip().value_counts().to_dict()

    summary = {
        "source_filename": source_name or file_path.name,
        "total_rows": total_rows,
        "total_columns": total_cols,
        "overall_completeness": overall_completeness,
        "duplicate_checks": dup_summary,
        "suspicious_count": len(suspicious),
        "issue_count": len(findings),
        "issues": _issue_strings(findings),
        "columns": list(df_norm.columns),
        "severity_counts": severity_counts,
        "check_counts": check_counts,
        "findings": findings,
        "findings_for_pdf": _group_findings_for_pdf(findings),
        "completeness_rows": completeness,
        "status_breakdown": status_breakdown,
    }

    report_json = out_dir / "internal_audit_report.json"
    report_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[internal_audit] wrote: {report_json}")

    # C-1: renamed from internal_audit_blanks.csv
    completeness_csv = out_dir / "internal_audit_completeness.csv"
    _write_csv(
        completeness_csv,
        completeness,
        fieldnames=["field", "total", "blank_count", "blank_pct", "filled_pct", "severity"],
    )
    print(f"[internal_audit] wrote: {completeness_csv}")

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
        for row in suspicious + active_zero_findings + phone_findings
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

    # C-2: Rebuild internal_audit_data.csv with 3 clearly labeled sections
    # Section 1: AUDIT SUMMARY
    data_rows: list[dict] = [
        {"section": "=== AUDIT SUMMARY ===", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": "", "issue_description": "", "recommended_action": ""},
        {"section": "Run ID", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": summary.get("source_filename", ""), "issue_description": "", "recommended_action": ""},
        {"section": "Date", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": datetime.now().strftime("%Y-%m-%d"), "issue_description": "", "recommended_action": ""},
        {"section": "Total Records", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": str(total_rows), "issue_description": "", "recommended_action": ""},
        {"section": "CRITICAL Issues", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": str(severity_counts.get("CRITICAL", 0)), "issue_description": "", "recommended_action": ""},
        {"section": "HIGH Issues", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": str(severity_counts.get("HIGH", 0)), "issue_description": "", "recommended_action": ""},
        {"section": "MEDIUM Issues", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": str(severity_counts.get("MEDIUM", 0)), "issue_description": "", "recommended_action": ""},
        {"section": "LOW Issues", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": str(severity_counts.get("LOW", 0)), "issue_description": "", "recommended_action": ""},
        {"section": "", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": "", "issue_description": "", "recommended_action": ""},
        # Section 2: ALL FINDINGS
        {"section": "=== ALL FINDINGS ===", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": "", "issue_description": "", "recommended_action": ""},
    ]
    for finding in findings:
        for srow in (finding.get("sample_rows") or []):
            data_rows.append({
                "section": "FINDING",
                "check_name": finding.get("check_name", ""),
                "severity": finding.get("severity", ""),
                "employee_id": str(srow.get("worker_id", srow.get("employee_id", ""))),
                "first_name": str(srow.get("first_name", "")),
                "last_name": str(srow.get("last_name", "")),
                "field_flagged": finding.get("field", ""),
                "value_found": str(srow.get(finding.get("field", ""), "")),
                "issue_description": finding.get("description", ""),
                "recommended_action": "",
            })
    # Section 3: CLEAN RECORDS - records with no findings
    flagged_indices: set = set()
    for finding in findings:
        for srow in (finding.get("sample_rows") or []):
            if "row_number" in srow:
                flagged_indices.add(int(srow["row_number"]) - 2)
    data_rows.append({"section": "", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": "", "issue_description": "", "recommended_action": ""})
    data_rows.append({"section": "=== CLEAN RECORDS (sample - passed all checks) ===", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": "", "issue_description": "", "recommended_action": ""})
    status_col_n = _status_column(df_norm)
    hire_col_n = _first_present(df_norm, ["hire_date"])
    clean_count = 0
    for idx in df_norm.index:
        if clean_count >= 50:
            break
        data_rows.append({
            "section": "CLEAN",
            "check_name": "",
            "severity": "PASS",
            "employee_id": str(df_norm.at[idx, "worker_id"]) if "worker_id" in df_norm.columns else "",
            "first_name": str(df_norm.at[idx, "first_name"]) if "first_name" in df_norm.columns else "",
            "last_name": str(df_norm.at[idx, "last_name"]) if "last_name" in df_norm.columns else "",
            "field_flagged": "",
            "value_found": "",
            "issue_description": "",
            "recommended_action": "",
        })
        clean_count += 1

    data_csv = out_dir / "internal_audit_data.csv"
    _write_csv(
        data_csv,
        data_rows,
        fieldnames=["section", "check_name", "severity", "employee_id", "first_name", "last_name", "field_flagged", "value_found", "issue_description", "recommended_action"],
    )
    print(f"[internal_audit] wrote: {data_csv}")
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
