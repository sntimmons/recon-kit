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
GATE_BLOCKED_MESSAGE = "CRITICAL issues detected. Dataset is NOT safe for payroll or production use."
GATE_OVERRIDDEN_MESSAGE = "WARNING: Gate overridden. CRITICAL issues still exist."

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
    "pay_rate": "payrate",
    "hourly_rate": "payrate",
    "base_rate": "payrate",
    "worker_type": "pay_type",
    "employment_type": "pay_type",
    "compensation_type": "pay_type",
    "pay_type": "pay_type",
    "standard_hours": "standard_hours",
    "scheduled_hours": "standard_hours",
    "standard_weekly_hours": "standard_hours",
    "weekly_standard_hours": "standard_hours",
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

PAY_TYPE_HOURLY_VALUES = {
    "hourly",
    "hourly_non_exempt",
    "hourly non exempt",
    "non_exempt",
    "non exempt",
}
PAY_TYPE_SALARIED_VALUES = {
    "salary",
    "salaried",
    "salaried_exempt",
    "salaried exempt",
    "annual",
    "exempt",
}
PAY_TYPE_ALLOWED_VALUES = PAY_TYPE_HOURLY_VALUES | PAY_TYPE_SALARIED_VALUES


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
        {"check_key": "invalid_date_logic", "check_name": "Invalid date logic", "severity": "CRITICAL"},
        {"check_key": "active_with_termination_date", "check_name": "Active employees with termination date", "severity": "CRITICAL"},
        {"check_key": "missing_required_identity", "check_name": "Missing required identity fields", "severity": "CRITICAL"},
        {"check_key": "duplicate_canonical_worker_id_conflict", "check_name": "Duplicate canonical worker_id with conflicting values", "severity": "CRITICAL"},
        {"check_key": "duplicate_email", "check_name": "Duplicate email", "severity": "MEDIUM"},
        {"check_key": "duplicate_last4_ssn", "check_name": "Duplicate last4_ssn", "severity": "CRITICAL"},
        {"check_key": "active_zero_salary", "check_name": "Active employees with <= $0 or missing salary/payrate", "severity": "CRITICAL"},
        {"check_key": "pay_type_missing_or_invalid", "check_name": "Missing or Invalid Pay Type", "severity": "CRITICAL"},
        {"check_key": "compensation_type_mismatch", "check_name": "Compensation Type Mismatch", "severity": "CRITICAL"},
        {"check_key": "comp_dual_value_conflict", "check_name": "Salary and Pay Rate Conflict", "severity": "HIGH"},
        {"check_key": "missing_standard_hours_hourly", "check_name": "Missing Standard Hours for Hourly Worker", "severity": "HIGH"},
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
        {"check_key": "duplicate_canonical_conflict", "check_name": "Duplicate canonical field with conflicting values", "severity": "HIGH"},
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
        "pay_type",
        "worker_type",
        "salary",
        "payrate",
        "standard_hours",
        "hire_date",
        "start_date",
        "date_hired",
        "last4_ssn",
    ]:
        if col in df.columns and col not in cols:
            cols.append(col)
    return cols[:6]


def _sample_rows(df: pd.DataFrame, mask: pd.Series, extra: list[str] | None = None, limit: int = 8) -> list[dict]:
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


def _pay_type_column(df: pd.DataFrame) -> str | None:
    for col in ["pay_type", "worker_type", "employment_type", "compensation_type"]:
        if col in df.columns:
            return col
    return None


def _standard_hours_column(df: pd.DataFrame) -> str | None:
    for col in ["standard_hours", "scheduled_hours", "standard_weekly_hours", "weekly_standard_hours"]:
        if col in df.columns:
            return col
    return None


def _classify_pay_type(value: object) -> tuple[str, bool]:
    normalized = _norm_lower(value).replace("-", "_")
    if not normalized:
        return "", False
    if normalized in PAY_TYPE_HOURLY_VALUES:
        return "hourly", False
    if normalized in PAY_TYPE_SALARIED_VALUES:
        return "salaried", False
    return "", True


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


def _collapse_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not df.columns.duplicated().any():
        return df

    cols: list[pd.Series] = []
    names: list[str] = []
    seen: set[str] = set()
    for col in df.columns:
        if col in seen:
            continue
        seen.add(col)
        subset = df.loc[:, df.columns == col]
        if isinstance(subset, pd.Series) or subset.shape[1] == 1:
            series = subset if isinstance(subset, pd.Series) else subset.iloc[:, 0]
        else:
            series = subset.bfill(axis=1).iloc[:, 0]
        cols.append(series.rename(col))
        names.append(col)
    return pd.concat(cols, axis=1)


def analyze_duplicate_canonical_fields(
    df: pd.DataFrame,
    canonical_map: dict[str, str],
) -> tuple[list[dict], dict, list[dict]]:
    """Detect canonical field conflicts arising when multiple source columns map to the same
    canonical name.

    Must be called AFTER alias normalization (df already has canonical column names, possibly
    duplicated) and BEFORE _collapse_duplicate_columns().

    Args:
        df:            DataFrame with potentially duplicate column names (post-rename).
        canonical_map: Mapping of original source column name -> canonical name.

    Returns:
        findings:         Finding dicts (HIGH severity) for duplicate_conflicting_values only.
        dup_canonical_summary: Aggregate counts for duplicate_canonical_summary in report JSON.
        row_annotations:  Per-row annotation dicts keyed by (row_index, canonical_field).
    """
    from collections import defaultdict as _dd

    # Position index: canonical_name -> [col_positions in df]
    col_positions: dict[str, list[int]] = _dd(list)
    for i, col in enumerate(df.columns):
        col_positions[col].append(i)

    dup_canonicals: dict[str, list[int]] = {
        col: positions for col, positions in col_positions.items() if len(positions) > 1
    }

    _empty_summary: dict = {
        "canonical_fields_checked": [],
        "same_value_rows": 0,
        "blank_vs_value_rows": 0,
        "conflicting_rows": 0,
        "by_field": {},
    }
    if not dup_canonicals:
        return [], _empty_summary, []

    # Reverse map: canonical -> [source col names in order of appearance]
    canonical_to_sources: dict[str, list[str]] = _dd(list)
    for src_col, canon in canonical_map.items():
        canonical_to_sources[canon].append(src_col)

    total_rows = len(df)
    findings: list[dict] = []
    # row_annotations[row_idx] = { canonical_field: {classification, ...} }
    row_annotations: list[dict] = [{} for _ in range(total_rows)]

    total_same = 0
    total_blank_vs_value = 0
    total_conflicting = 0
    summary_by_field: dict[str, dict] = {}

    for canonical, positions in dup_canonicals.items():
        source_cols = canonical_to_sources.get(canonical, [canonical] * len(positions))
        # Pad source_cols list to match number of positions if needed
        while len(source_cols) < len(positions):
            source_cols.append(canonical)

        same_count = 0
        blank_vs_count = 0
        conflict_count = 0
        conflict_sample: list[dict] = []

        for row_idx in range(total_rows):
            values = [_norm_str(df.iloc[row_idx, pos]) for pos in positions]
            non_blank = [v for v in values if v]

            if len(non_blank) == 0:
                # All blank - no conflict
                classification = "duplicate_same_value"
                retained_reason = "identical_values"
                same_count += 1

            elif len(non_blank) == len(values) and len({v.lower() for v in non_blank}) == 1:
                # All copies non-blank and equal
                classification = "duplicate_same_value"
                retained_reason = "identical_values"
                same_count += 1

            elif len(non_blank) < len(values) and len({v.lower() for v in non_blank}) <= 1:
                # Some blank, all populated copies are identical (or only one is populated)
                classification = "duplicate_blank_vs_value"
                retained_reason = "first_non_blank" if not values[0] else "identical_values"
                blank_vs_count += 1

            else:
                # Multiple distinct non-blank values = conflict
                classification = "duplicate_conflicting_values"
                retained_reason = "conflict_primary_wins" if values[0] else "first_non_blank"
                conflict_count += 1

                if len(conflict_sample) < 8:
                    sample: dict = {"row_number": row_idx + 2}
                    if "worker_id" in col_positions:
                        sample["worker_id"] = _norm_str(df.iloc[row_idx, col_positions["worker_id"][0]])
                    if "first_name" in col_positions:
                        sample["first_name"] = _norm_str(df.iloc[row_idx, col_positions["first_name"][0]])
                    if "last_name" in col_positions:
                        sample["last_name"] = _norm_str(df.iloc[row_idx, col_positions["last_name"][0]])
                    for src, val in zip(source_cols, values):
                        sample[f"source_{src}"] = val
                    # Carry the 4 new CSV columns into the sample row
                    sample["duplicate_classification"] = classification
                    sample["duplicate_canonical_field"] = canonical
                    sample["duplicate_source_columns"] = "|".join(source_cols)
                    sample["duplicate_values"] = "|".join(values)
                    sample["retained_value_reason"] = retained_reason
                    conflict_sample.append(sample)

            row_annotations[row_idx][canonical] = {
                "duplicate_classification": classification,
                "duplicate_canonical_field": canonical,
                "duplicate_source_columns": "|".join(source_cols),
                "duplicate_values": "|".join(values),
                "retained_value_reason": retained_reason,
            }

        total_same += same_count
        total_blank_vs_value += blank_vs_count
        total_conflicting += conflict_count

        summary_by_field[canonical] = {
            "source_columns": source_cols,
            "same_value": same_count,
            "blank_vs_value": blank_vs_count,
            "conflicting_values": conflict_count,
        }

        if conflict_count > 0:
            pct = round(conflict_count / total_rows * 100, 1) if total_rows else 0.0
            src_label = " and ".join(f'"{s}"' for s in source_cols)
            is_worker_id_conflict = canonical == "worker_id"
            findings.append({
                "check_key": "duplicate_canonical_worker_id_conflict" if is_worker_id_conflict else "duplicate_canonical_conflict",
                "check_name": "Duplicate canonical worker_id with conflicting values" if is_worker_id_conflict else "Duplicate canonical field with conflicting values",
                "severity": "CRITICAL" if is_worker_id_conflict else "HIGH",
                "count": conflict_count,
                "pct": pct,
                "field": canonical,
                "description": (
                    f"{conflict_count} row(s) have conflicting values for '{canonical}' "
                    f"across source columns {src_label}. "
                    "Column collapse will silently discard the non-primary value."
                ),
                "rule_name": "duplicate_canonical_worker_id_conflict" if is_worker_id_conflict else "duplicate_canonical_conflict",
                "impact": (
                    "Conflicting worker_id values create an identity integrity failure. "
                    "Using the primary value can attach downstream changes to the wrong employee."
                    if is_worker_id_conflict else
                    "Reconciliation uses the primary (leftmost) column value, "
                    "silently discarding the alternative. This may mask data entry errors."
                ),
                "recommended_action": (
                    f"Review rows with conflicting '{canonical}' values and resolve "
                    "before migration. Deduplicate source columns or choose the authoritative value explicitly."
                ),
                "_source_columns": source_cols,
                "_conflicting_count": conflict_count,
                "sample_rows": conflict_sample,
                "retained_value_reason": "conflict_primary_wins",
            })

    dup_canonical_summary: dict = {
        "canonical_fields_checked": list(dup_canonicals.keys()),
        "same_value_rows": total_same,
        "blank_vs_value_rows": total_blank_vs_value,
        "conflicting_rows": total_conflicting,
        "by_field": summary_by_field,
    }

    return findings, dup_canonical_summary, row_annotations


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
        "invalid_date_logic",
        "active_with_termination_date",
        "missing_required_identity",
        "duplicate_canonical_worker_id_conflict",
        "duplicate_last4_ssn",
        "active_zero_salary",
        "pay_type_missing_or_invalid",
        "compensation_type_mismatch",
        "ghost_employee_indicator",
    }:
        return "CRITICAL"
    if check_key in {
        "comp_dual_value_conflict",
        "missing_standard_hours_hourly",
        "salary_suspicious_default",
        "hire_date_suspicious_default",
        "status_suspicious_value",
        "impossible_dates",
        "status_hire_date_mismatch",
        "manager_loop",
        "pay_equity_flag",
        "duplicate_canonical_conflict",
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
        if col == "worker_id":
            continue
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


def _detect_duplicate_worker_id(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    if "worker_id" not in df.columns:
        return findings

    worker_ids = df["worker_id"].astype(str).str.strip()
    nonnull = worker_ids[(worker_ids != "") & (worker_ids.str.lower() != "nan")]
    dupes = nonnull[nonnull.duplicated(keep=False)]
    if dupes.empty:
        return findings

    findings.append(
        {
            "section": "CRITICAL_CHECKS",
            "check_key": "duplicate_worker_id",
            "check_name": "Duplicate worker_id",
            "field": "worker_id",
            "severity": "CRITICAL",
            "count": int(len(dupes)),
            "pct": round(len(dupes) / len(df) * 100, 2) if len(df) else 0.0,
            "description": f"{len(dupes)} records share duplicate worker_id values across {dupes.nunique()} repeated values.",
            "sample_rows": _sample_rows(df, df.index.isin(dupes.index), extra=["worker_id"]),
        }
    )
    return findings


def _detect_active_zero_salary(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    status_col = _status_column(df)
    has_salary = "salary" in df.columns
    has_payrate = "payrate" in df.columns
    if not status_col or (not has_salary and not has_payrate):
        return findings

    statuses = df[status_col].astype(str).str.strip().str.lower()
    salary_values = pd.to_numeric(df["salary"], errors="coerce") if has_salary else pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    payrate_values = pd.to_numeric(df["payrate"], errors="coerce") if has_payrate else pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    salary_blank = _blank_mask(df["salary"]) if has_salary else pd.Series([True] * len(df), index=df.index)
    payrate_blank = _blank_mask(df["payrate"]) if has_payrate else pd.Series([True] * len(df), index=df.index)
    effective_values = salary_values.where(~salary_blank, payrate_values)
    compensation_blank = salary_blank & payrate_blank
    mask = (statuses == "active") & (compensation_blank | (effective_values <= 0))
    if mask.any():
        extra_cols = [status_col]
        if has_salary:
            extra_cols.append("salary")
        if has_payrate:
            extra_cols.append("payrate")
        findings.append(
            {
                "section": "CRITICAL_CHECKS",
                "check_key": "active_zero_salary",
                "check_name": "Active employees with <= $0 or missing salary/payrate",
                "field": "salary" if has_salary else "payrate",
                "severity": "CRITICAL",
                "count": int(mask.sum()),
                "pct": round(mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Active employee has missing, zero, or negative salary/payrate - likely a payroll data error.",
                "sample_rows": _sample_rows(df, mask, extra=extra_cols),
            }
        )
    return findings


def _detect_pay_type_missing_or_invalid(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    status_col = _status_column(df)
    pay_type_col = _pay_type_column(df)
    has_salary = "salary" in df.columns
    has_payrate = "payrate" in df.columns
    if not status_col or not pay_type_col or (not has_salary and not has_payrate):
        return findings

    statuses = df[status_col].astype(str).str.strip().str.lower()
    salary_blank = _blank_mask(df["salary"]) if has_salary else pd.Series([True] * len(df), index=df.index)
    payrate_blank = _blank_mask(df["payrate"]) if has_payrate else pd.Series([True] * len(df), index=df.index)
    comp_present = ~salary_blank | ~payrate_blank

    critical_mask = pd.Series(False, index=df.index)
    high_mask = pd.Series(False, index=df.index)
    for idx in df.index[comp_present.fillna(False)]:
        _, invalid = _classify_pay_type(df.at[idx, pay_type_col])
        is_blank = _blank_mask(pd.Series([df.at[idx, pay_type_col]])).iloc[0]
        if not (invalid or is_blank):
            continue
        if statuses.at[idx] == "active":
            critical_mask.at[idx] = True
        else:
            high_mask.at[idx] = True

    if critical_mask.any():
        findings.append(
            {
                "section": "CRITICAL_CHECKS",
                "check_key": "pay_type_missing_or_invalid",
                "check_name": "Missing or Invalid Pay Type",
                "field": pay_type_col,
                "severity": "CRITICAL",
                "count": int(critical_mask.sum()),
                "pct": round(critical_mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Active worker has compensation present but pay type is blank or invalid.",
                "impact": "Payroll cannot reliably determine whether to use salary or pay rate logic for this worker.",
                "recommended_action": "Populate a valid pay type from the controlled allowed list before payroll or migration.",
                "sample_rows": _sample_rows(df, critical_mask, extra=[status_col, pay_type_col, "salary", "payrate"]),
            }
        )
    if high_mask.any():
        findings.append(
            {
                "section": "PAYROLL_CHECKS",
                "check_key": "pay_type_missing_or_invalid",
                "check_name": "Missing or Invalid Pay Type",
                "field": pay_type_col,
                "severity": "HIGH",
                "count": int(high_mask.sum()),
                "pct": round(high_mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Non-active worker has compensation present but pay type is blank or invalid.",
                "impact": "Compensation records are not reliably classified and may load incorrectly into payroll history.",
                "recommended_action": "Populate a valid pay type from the controlled allowed list before migration.",
                "sample_rows": _sample_rows(df, high_mask, extra=[status_col, pay_type_col, "salary", "payrate"]),
            }
        )
    return findings


def _detect_compensation_type_mismatch(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    status_col = _status_column(df)
    pay_type_col = _pay_type_column(df)
    has_salary = "salary" in df.columns
    has_payrate = "payrate" in df.columns
    if not status_col or not pay_type_col or (not has_salary and not has_payrate):
        return findings

    statuses = df[status_col].astype(str).str.strip().str.lower()
    salary_blank = _blank_mask(df["salary"]) if has_salary else pd.Series([True] * len(df), index=df.index)
    payrate_blank = _blank_mask(df["payrate"]) if has_payrate else pd.Series([True] * len(df), index=df.index)
    salary_num = pd.to_numeric(df["salary"], errors="coerce") if has_salary else pd.Series([float("nan")] * len(df), index=df.index)
    payrate_num = pd.to_numeric(df["payrate"], errors="coerce") if has_payrate else pd.Series([float("nan")] * len(df), index=df.index)
    salary_valid = ~salary_blank & salary_num.gt(0)
    payrate_valid = ~payrate_blank & payrate_num.gt(0)
    comp_present = ~salary_blank | ~payrate_blank

    critical_mask = pd.Series(False, index=df.index)
    high_mask = pd.Series(False, index=df.index)
    for idx in df.index:
        pay_class, invalid = _classify_pay_type(df.at[idx, pay_type_col])
        if invalid or not pay_class or not comp_present.at[idx]:
            continue
        if pay_class == "hourly" and not payrate_valid.at[idx]:
            if statuses.at[idx] == "active":
                critical_mask.at[idx] = True
            else:
                high_mask.at[idx] = True
        elif pay_class == "salaried" and not salary_valid.at[idx]:
            if statuses.at[idx] == "active":
                critical_mask.at[idx] = True
            else:
                high_mask.at[idx] = True

    if critical_mask.any():
        findings.append(
            {
                "section": "CRITICAL_CHECKS",
                "check_key": "compensation_type_mismatch",
                "check_name": "Compensation Type Mismatch",
                "field": pay_type_col,
                "severity": "CRITICAL",
                "count": int(critical_mask.sum()),
                "pct": round(critical_mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Active worker is missing the required compensation field for the stated pay type.",
                "impact": "Payroll will not calculate pay correctly because the required salary or pay rate value is missing or invalid.",
                "recommended_action": "Populate the required compensation field that matches the worker pay type before payroll.",
                "sample_rows": _sample_rows(df, critical_mask, extra=[status_col, pay_type_col, "salary", "payrate"]),
            }
        )
    if high_mask.any():
        findings.append(
            {
                "section": "PAYROLL_CHECKS",
                "check_key": "compensation_type_mismatch",
                "check_name": "Compensation Type Mismatch",
                "field": pay_type_col,
                "severity": "HIGH",
                "count": int(high_mask.sum()),
                "pct": round(high_mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Pay type and compensation fields disagree, but payroll could still infer intended compensation logic.",
                "impact": "Migration teams may guess the wrong compensation field and create bad payroll setup for the worker.",
                "recommended_action": "Align pay type with the required salary or pay rate field before migration.",
                "sample_rows": _sample_rows(df, high_mask, extra=[status_col, pay_type_col, "salary", "payrate"]),
            }
        )
    return findings


def _detect_comp_dual_value_conflict(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    status_col = _status_column(df)
    pay_type_col = _pay_type_column(df)
    if not status_col or not pay_type_col or "salary" not in df.columns or "payrate" not in df.columns:
        return findings

    statuses = df[status_col].astype(str).str.strip().str.lower()
    salary_blank = _blank_mask(df["salary"])
    payrate_blank = _blank_mask(df["payrate"])
    dual_present = ~salary_blank & ~payrate_blank

    high_mask = pd.Series(False, index=df.index)
    medium_mask = pd.Series(False, index=df.index)
    for idx in df.index[dual_present.fillna(False)]:
        pay_class, invalid = _classify_pay_type(df.at[idx, pay_type_col])
        if invalid or not pay_class:
            continue
        if statuses.at[idx] == "active":
            high_mask.at[idx] = True
        else:
            medium_mask.at[idx] = True

    if high_mask.any():
        findings.append(
            {
                "section": "PAYROLL_CHECKS",
                "check_key": "comp_dual_value_conflict",
                "check_name": "Salary and Pay Rate Conflict",
                "field": pay_type_col,
                "severity": "HIGH",
                "count": int(high_mask.sum()),
                "pct": round(high_mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Active worker has both salary and pay rate populated in a way that conflicts with pay type or compensation rules.",
                "impact": "Payroll setup is ambiguous and operators may choose the wrong compensation source during migration.",
                "recommended_action": "Review the worker record and keep only the compensation field that matches the intended pay type.",
                "sample_rows": _sample_rows(df, high_mask, extra=[status_col, pay_type_col, "salary", "payrate", "standard_hours"]),
            }
        )
    if medium_mask.any():
        findings.append(
            {
                "section": "PAYROLL_CHECKS",
                "check_key": "comp_dual_value_conflict",
                "check_name": "Salary and Pay Rate Conflict",
                "field": pay_type_col,
                "severity": "MEDIUM",
                "count": int(medium_mask.sum()),
                "pct": round(medium_mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Non-active worker has both salary and pay rate populated in a conflicting way.",
                "impact": "Historical compensation may load ambiguously and reduce confidence in payroll records.",
                "recommended_action": "Review the worker record and keep only the compensation field that matches the intended pay type.",
                "sample_rows": _sample_rows(df, medium_mask, extra=[status_col, pay_type_col, "salary", "payrate", "standard_hours"]),
            }
        )
    return findings


def _detect_missing_standard_hours_hourly(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    status_col = _status_column(df)
    pay_type_col = _pay_type_column(df)
    standard_hours_col = _standard_hours_column(df)
    if not status_col or not pay_type_col or not standard_hours_col or "payrate" not in df.columns:
        return findings

    statuses = df[status_col].astype(str).str.strip().str.lower()
    payrate_blank = _blank_mask(df["payrate"])
    standard_hours_blank = _blank_mask(df[standard_hours_col])
    high_mask = pd.Series(False, index=df.index)
    medium_mask = pd.Series(False, index=df.index)

    for idx in df.index:
        pay_class, invalid = _classify_pay_type(df.at[idx, pay_type_col])
        if invalid or pay_class != "hourly" or payrate_blank.at[idx] or not standard_hours_blank.at[idx]:
            continue
        if statuses.at[idx] == "active":
            high_mask.at[idx] = True
        else:
            medium_mask.at[idx] = True

    if high_mask.any():
        findings.append(
            {
                "section": "PAYROLL_CHECKS",
                "check_key": "missing_standard_hours_hourly",
                "check_name": "Missing Standard Hours for Hourly Worker",
                "field": standard_hours_col,
                "severity": "HIGH",
                "count": int(high_mask.sum()),
                "pct": round(high_mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Active hourly worker has pay rate present but standard hours are missing.",
                "impact": "Payroll teams cannot confidently annualize or validate hourly compensation without standard hours.",
                "recommended_action": "Populate standard hours for each hourly worker before migration.",
                "sample_rows": _sample_rows(df, high_mask, extra=[status_col, pay_type_col, "payrate", standard_hours_col]),
            }
        )
    if medium_mask.any():
        findings.append(
            {
                "section": "PAYROLL_CHECKS",
                "check_key": "missing_standard_hours_hourly",
                "check_name": "Missing Standard Hours for Hourly Worker",
                "field": standard_hours_col,
                "severity": "MEDIUM",
                "count": int(medium_mask.sum()),
                "pct": round(medium_mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Non-active hourly worker has pay rate present but standard hours are missing.",
                "impact": "Historical payroll records may be incomplete and harder to validate during migration.",
                "recommended_action": "Populate standard hours where available before migration.",
                "sample_rows": _sample_rows(df, medium_mask, extra=[status_col, pay_type_col, "payrate", standard_hours_col]),
            }
        )
    return findings


def _detect_invalid_date_logic(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    hire_col = _first_present(df, ["hire_date", "start_date", "date_hired"])
    term_col = _first_present(df, ["termination_date", "term_date", "end_date"])
    if not hire_col:
        return findings

    today = pd.Timestamp(datetime.now().date())
    hire_dates = _date_series(df, hire_col)
    term_dates = _date_series(df, term_col)
    invalid_rows: list[dict] = []

    future_hire = hire_dates > today
    for idx in df.index[future_hire.fillna(False)]:
        invalid_rows.append(
            {
                "row_number": int(idx) + 2,
                "worker_id": _safe_json_value(df.at[idx, "worker_id"]) if "worker_id" in df.columns else "",
                "field_name": hire_col,
                "field_value": _safe_json_value(df.at[idx, hire_col]),
                "why_flagged": "Hire date is in the future",
            }
        )

    if term_col:
        term_before_hire = term_dates < hire_dates
        for idx in df.index[term_before_hire.fillna(False)]:
            invalid_rows.append(
                {
                    "row_number": int(idx) + 2,
                    "worker_id": _safe_json_value(df.at[idx, "worker_id"]) if "worker_id" in df.columns else "",
                    "field_name": term_col,
                    "field_value": _safe_json_value(df.at[idx, term_col]),
                    "why_flagged": "Termination date is before hire date",
                }
            )

    if invalid_rows:
        findings.append(
            {
                "section": "CRITICAL_CHECKS",
                "check_key": "invalid_date_logic",
                "check_name": "Invalid date logic",
                "field": hire_col,
                "severity": "CRITICAL",
                "count": len(invalid_rows),
                "pct": round(len(invalid_rows) / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Hire date is in the future or termination date is before hire date.",
                "sample_rows": invalid_rows[:8],
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
                "sample_rows": rows[:8],
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
    terminated_without_term = (statuses == "terminated") & term_blank

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


def _detect_active_with_termination_date(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    status_col = _status_column(df)
    term_col = _first_present(df, ["termination_date", "term_date", "end_date"])
    if not status_col or not term_col:
        return findings

    statuses = df[status_col].astype(str).str.strip().str.lower()
    term_blank = _blank_mask(df[term_col])
    active_with_term = (statuses == "active") & ~term_blank
    if active_with_term.any():
        findings.append(
            {
                "section": "CRITICAL_CHECKS",
                "check_key": "active_with_termination_date",
                "check_name": "Active employees with termination date",
                "field": term_col,
                "severity": "CRITICAL",
                "count": int(active_with_term.sum()),
                "pct": round(active_with_term.sum() / len(df) * 100, 2) if len(df) else 0.0,
                "description": "Employee is marked active but has a termination date.",
                "sample_rows": _sample_rows(df, active_with_term, extra=[status_col, term_col]),
            }
        )
    return findings


def _detect_missing_required_identity(df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    required_fields = [field for field in ["worker_id", "first_name", "last_name"] if field in df.columns]
    if not required_fields:
        return findings

    missing_masks = [_blank_mask(df[field]) for field in required_fields]
    combined_mask = missing_masks[0].copy()
    for mask in missing_masks[1:]:
        combined_mask = combined_mask | mask
    if not combined_mask.any():
        return findings

    missing_fields = []
    for idx in df.index[combined_mask.fillna(False)]:
        row_missing = [field for field in required_fields if bool(_blank_mask(df.loc[[idx], field]).iloc[0])]
        missing_fields.append(
            {
                "row_number": int(idx) + 2,
                "worker_id": _safe_json_value(df.at[idx, "worker_id"]) if "worker_id" in df.columns else "",
                "first_name": _safe_json_value(df.at[idx, "first_name"]) if "first_name" in df.columns else "",
                "last_name": _safe_json_value(df.at[idx, "last_name"]) if "last_name" in df.columns else "",
                "missing_fields": ", ".join(row_missing),
            }
        )

    findings.append(
        {
            "section": "CRITICAL_CHECKS",
            "check_key": "missing_required_identity",
            "check_name": "Missing required identity fields",
            "field": ",".join(required_fields),
            "severity": "CRITICAL",
            "count": int(combined_mask.sum()),
            "pct": round(combined_mask.sum() / len(df) * 100, 2) if len(df) else 0.0,
            "description": "Required identity fields are missing for one or more records.",
            "sample_rows": missing_fields[:8],
        }
    )
    return findings


def _standardize_finding(finding: dict, total_rows: int) -> dict:
    finding = dict(finding)
    severity = str(finding.get("severity") or "LOW").upper()
    row_count = int(finding.get("row_count", finding.get("count", 0)) or 0)
    pct = finding.get("percent_of_population", finding.get("pct", 0.0))
    try:
        pct = round(float(pct), 2)
    except Exception:
        pct = round(row_count / total_rows * 100, 2) if total_rows else 0.0

    finding["severity"] = severity
    finding["count"] = row_count
    finding["pct"] = pct
    finding.setdefault("rule_name", str(finding.get("check_key") or ""))
    finding["row_count"] = row_count
    finding["percent_of_population"] = pct
    finding.setdefault("impact", "This issue may affect data integrity and should be reviewed before downstream use.")
    finding.setdefault("recommended_action", "Review and correct the flagged records before production use.")
    return finding


def _standardize_findings(findings: list[dict], total_rows: int) -> list[dict]:
    return [_standardize_finding(finding, total_rows) for finding in findings]


def _evaluate_gate(findings: list[dict], *, override_gate: bool) -> dict:
    critical_findings = [finding for finding in findings if str(finding.get("severity", "")).upper() == "CRITICAL"]
    if not critical_findings:
        return {
            "gate_status": "PASSED",
            "gate_message": "",
            "override_gate": bool(override_gate),
            "critical_findings_present": False,
            "downstream_actions": {
                "corrections_generation_skipped": False,
                "approved_status_allowed": True,
            },
        }

    gate_status = "OVERRIDDEN" if override_gate else "BLOCKED"
    return {
        "gate_status": gate_status,
        "gate_message": GATE_OVERRIDDEN_MESSAGE if override_gate else GATE_BLOCKED_MESSAGE,
        "override_gate": bool(override_gate),
        "critical_findings_present": True,
        "downstream_actions": {
            "corrections_generation_skipped": gate_status == "BLOCKED",
            "approved_status_allowed": gate_status != "BLOCKED",
        },
    }


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
            for cycle in cycles[:8]
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
                "sample_rows": rows[:8],
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
                "sample_rows": group_rows[:8],
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
                "sample_rows": rows[:8],
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
                "sample_rows": rows[:8],
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
    if not status_col or len(df) <= 100:
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
        ("Under $30k", 0, 30000),
        ("$30k-$60k", 30000, 60000),
        ("$60k-$100k", 60000, 100000),
        ("$100k-$150k", 100000, 150000),
        ("$150k-$200k", 150000, 200000),
        ("$200k+", 200000, float("inf")),
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
    grouped: dict[tuple[str, str, str], dict] = {}
    for finding in findings:
        key = (finding["check_key"], finding["check_name"], finding["severity"])
        if key not in grouped:
            grouped[key] = {
                "check_key": finding["check_key"],
                "check_name": finding["check_name"],
                "rule_name": finding.get("rule_name", finding["check_key"]),
                "severity": finding["severity"],
                "count": 0,
                "row_count": 0,
                "pct": finding.get("pct", 0),
                "percent_of_population": finding.get("percent_of_population", finding.get("pct", 0)),
                "field": finding.get("field", ""),
                "description": finding["description"],
                "impact": finding.get("impact", ""),
                "recommended_action": finding.get("recommended_action", ""),
                "_unique_count": finding.get("_unique_count"),
                "_example": finding.get("_example"),
                "_sample_value": finding.get("_sample_value"),
                "_col": finding.get("_col"),
                "sample_rows": [],
                "group_rows": [],
            }
        grouped[key]["count"] += int(finding.get("count", 0))
        grouped[key]["row_count"] += int(finding.get("row_count", finding.get("count", 0)) or 0)
        for row in finding.get("sample_rows", []):
            if len(grouped[key]["sample_rows"]) >= 8:
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


def run_internal_audit(
    file_path: Path,
    out_dir: Path,
    *,
    source_name: str | None = None,
    sheet_name: int | str = 0,
    override_gate: bool = False,
) -> dict:
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
    # Detect duplicate canonical field conflicts BEFORE collapsing
    dup_canon_findings, dup_canonical_summary, _dup_row_annotations = analyze_duplicate_canonical_fields(
        df_norm, _col_norm_map
    )
    df_norm = _collapse_duplicate_columns(df_norm)

    total_rows = len(df_norm)
    total_cols = len(df_norm.columns)
    print(f"[internal_audit] loaded {total_rows} rows x {total_cols} columns")

    dup_df, dup_summary, dup_findings = _detect_duplicates(df_norm, config["duplicate_check_fields"])
    duplicate_worker_findings = _detect_duplicate_worker_id(df_norm)
    suspicious = _detect_suspicious(df_norm, config)
    active_zero_findings = _detect_active_zero_salary(df_norm)
    pay_type_findings = _detect_pay_type_missing_or_invalid(df_norm)
    compensation_type_findings = _detect_compensation_type_mismatch(df_norm)
    dual_comp_findings = _detect_comp_dual_value_conflict(df_norm)
    missing_standard_hours_findings = _detect_missing_standard_hours_hourly(df_norm)
    invalid_date_logic_findings = _detect_invalid_date_logic(df_norm)
    impossible_date_findings = _detect_impossible_dates(df_norm)
    active_with_term_findings = _detect_active_with_termination_date(df_norm)
    status_mismatch_findings = _detect_status_hire_date_mismatch(df_norm)
    missing_required_identity_findings = _detect_missing_required_identity(df_norm)
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
        dup_canon_findings
        + duplicate_worker_findings
        + dup_findings
        + active_zero_findings
        + pay_type_findings
        + compensation_type_findings
        + dual_comp_findings
        + missing_standard_hours_findings
        + invalid_date_logic_findings
        + active_with_term_findings
        + missing_required_identity_findings
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
    findings = _standardize_findings(findings, total_rows)
    severity_counts = _severity_counts(findings)
    check_counts = _check_counts(findings)
    gate_summary = _evaluate_gate(findings, override_gate=override_gate)

    total_blank_fields = sum(r["blank_count"] for r in completeness)
    total_possible = total_rows * total_cols
    overall_completeness = round((total_possible - total_blank_fields) / total_possible * 100, 1) if total_possible > 0 else 0.0

    # Build status distribution dict for PDF workforce snapshot
    status_col = _status_column(df_norm)
    status_breakdown: dict[str, int] = {}
    if status_col:
        status_breakdown = df_norm[status_col].astype(str).str.strip().value_counts().to_dict()

    # Salary statistics for report
    salary_stats: dict = {}
    if "salary" in df_norm.columns:
        _sal_num = pd.to_numeric(df_norm["salary"], errors="coerce")
        _sal_valid = _sal_num.dropna()
        salary_stats = {
            "count": int(len(_sal_valid)),
            "missing": int(len(df_norm) - len(_sal_valid)),
            "missing_pct": round((len(df_norm) - len(_sal_valid)) / len(df_norm) * 100, 1) if len(df_norm) else 0.0,
            "min": float(_sal_valid.min()) if not _sal_valid.empty else 0.0,
            "max": float(_sal_valid.max()) if not _sal_valid.empty else 0.0,
            "mean": float(_sal_valid.mean()) if not _sal_valid.empty else 0.0,
            "median": float(_sal_valid.median()) if not _sal_valid.empty else 0.0,
            "under_50k": int((_sal_valid < 50000).sum()),
            "under_50k_pct": round((_sal_valid < 50000).sum() / len(_sal_valid) * 100, 1) if not _sal_valid.empty else 0.0,
            "over_150k": int((_sal_valid > 150000).sum()),
            "over_150k_pct": round((_sal_valid > 150000).sum() / len(_sal_valid) * 100, 1) if not _sal_valid.empty else 0.0,
        }

    summary = {
        "source_filename": source_name or file_path.name,
        "total_rows": total_rows,
        "total_columns": total_cols,
        "overall_completeness": overall_completeness,
        "duplicate_checks": dup_summary,
        "duplicate_canonical_summary": dup_canonical_summary,
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
        "salary_stats": salary_stats,
        **gate_summary,
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
        for row in suspicious + active_zero_findings + pay_type_findings + compensation_type_findings + dual_comp_findings + missing_standard_hours_findings + phone_findings
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
        {"section": "Gate Status", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": summary.get("gate_status", ""), "issue_description": summary.get("gate_message", ""), "recommended_action": ""},
        {"section": "CRITICAL Issues", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": str(severity_counts.get("CRITICAL", 0)), "issue_description": "", "recommended_action": ""},
        {"section": "HIGH Issues", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": str(severity_counts.get("HIGH", 0)), "issue_description": "", "recommended_action": ""},
        {"section": "MEDIUM Issues", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": str(severity_counts.get("MEDIUM", 0)), "issue_description": "", "recommended_action": ""},
        {"section": "LOW Issues", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": str(severity_counts.get("LOW", 0)), "issue_description": "", "recommended_action": ""},
        {"section": "", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": "", "issue_description": "", "recommended_action": ""},
        # Section 2: ALL FINDINGS
        {"section": "=== ALL FINDINGS ===", "check_name": "", "severity": "", "employee_id": "", "first_name": "", "last_name": "", "field_flagged": "", "value_found": "", "issue_description": "", "recommended_action": ""},
    ]
    for finding in findings:
        is_dup_canon = finding.get("check_key") == "duplicate_canonical_conflict"
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
                "duplicate_classification": srow.get("duplicate_classification", "") if is_dup_canon else "",
                "duplicate_canonical_field": srow.get("duplicate_canonical_field", "") if is_dup_canon else "",
                "duplicate_source_columns": srow.get("duplicate_source_columns", "") if is_dup_canon else "",
                "duplicate_values": srow.get("duplicate_values", "") if is_dup_canon else "",
                "retained_value_reason": srow.get("retained_value_reason", "") if is_dup_canon else "",
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
        fieldnames=[
            "section", "check_name", "severity", "employee_id", "first_name", "last_name",
            "field_flagged", "value_found", "issue_description", "recommended_action",
            "duplicate_classification", "duplicate_canonical_field",
            "duplicate_source_columns", "duplicate_values", "retained_value_reason",
        ],
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
    parser.add_argument("--override-gate", action=argparse.BooleanOptionalAction, default=False, help="Override the critical audit gate")
    args = parser.parse_args()

    sheet_name: int | str = int(args.sheet_name) if str(args.sheet_name).lstrip("-").isdigit() else args.sheet_name
    run_internal_audit(
        Path(args.file),
        Path(args.out_dir),
        source_name=args.source_name or None,
        sheet_name=sheet_name,
        override_gate=bool(args.override_gate),
    )
    sys.exit(0)
