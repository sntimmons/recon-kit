"""
build_internal_audit_workbook.py  –  Excel workbook export for internal audit runs.

Sheets (in order):
  1. Executive_Summary  — all findings, mirrors PDF; first tab HR managers see
  2. Fix_List           — action table: every issue, priority-ranked, Blocking? column
  3. Findings_*         — one tab per issue type that has row-level data
  4. Findings_Index     — master index with detail-sheet cross-references
  5. Data_Quality_Score — total records, completeness %, gate status, severity counts

Design rules:
  • Counts + severity come exclusively from internal_audit_report.json (same source as PDF).
  • CRITICAL finding detail: full row expansion from source data.
  • HIGH/MEDIUM finding detail: sample rows from internal_audit_data.csv + note.
  • Dataset-level findings (status_no_terminated): no detail tab.
  • No technical check_key labels visible to HR users.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

try:
    from openpyxl import load_workbook
    from openpyxl.styles import Alignment, Font, PatternFill
except ImportError:
    print("[error] openpyxl not installed", file=sys.stderr)
    sys.exit(2)

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from audit import internal_audit as ia


# ── Row caps ──────────────────────────────────────────────────────────────────
DETAIL_ROW_CAP = 50_000   # max rows per Findings_* detail sheet

# ── Styling ───────────────────────────────────────────────────────────────────
HEADER_FILL = "D9EAF7"

SEV_FILLS: dict[str, str] = {
    "CRITICAL": "FFCCCC",   # red
    "HIGH":     "FFE0CC",   # orange
    "MEDIUM":   "FFF3CC",   # yellow
    "LOW":      "E8F5E9",   # light green
}

# ── Priority system ───────────────────────────────────────────────────────────
SEVERITY_PRIORITY: dict[str, int] = {
    "CRITICAL": 1,
    "HIGH":     2,
    "MEDIUM":   3,
    "LOW":      4,
}
BLOCKING_SEVERITIES = {"CRITICAL", "HIGH"}

# ── Dataset-level check_keys ──────────────────────────────────────────────────
# For these, count=1 means "the dataset has this issue", not "1 employee affected."
DATASET_LEVEL_CHECKS: set[str] = {"status_no_terminated"}

# ── Check metadata lookup ─────────────────────────────────────────────────────
# Maps check_key → (issue_name, business_impact, required_action, detail_sheet | None)
# detail_sheet = None → dataset-level, no per-row tab created.
_M = {
    # CRITICAL
    "duplicate_worker_id": (
        "Duplicate Worker ID",
        "Data for the wrong employee could be modified or payroll routed incorrectly during migration.",
        "Identify each duplicated ID, assign a unique Worker ID to each employee, and update all references before use.",
        "Findings_Dup_WorkerID",
    ),
    "duplicate_canonical_worker_id_conflict": (
        "Duplicate Worker ID (Source Column Conflict)",
        "Two source columns map to Worker ID with conflicting values — only one can be correct.",
        "Decide which source column is authoritative, remove the other, and ensure each employee has exactly one Worker ID.",
        "Findings_Dup_WorkerID",   # merged into the same detail tab
    ),
    "active_zero_salary": (
        "Missing or Invalid Salary",
        "Active employees will not receive pay, causing immediate payroll failures.",
        "Enter a valid positive salary or payrate for every flagged active employee before any payroll run.",
        "Findings_Missing_Salary",
    ),
    "invalid_date_logic": (
        "Invalid Dates",
        "Service calculations, payroll timing, and benefits eligibility will produce wrong results.",
        "Correct the hire and termination dates for every flagged record so the employment timeline is valid.",
        "Findings_Invalid_Dates",
    ),
    "active_with_termination_date": (
        "Active Employees with Termination Dates",
        "Employees may be excluded from payroll or benefits workflows despite being actively employed.",
        "For each flagged employee, either remove the termination date or change their status to Terminated.",
        "Findings_Active_Term",
    ),
    "missing_required_identity": (
        "Missing Required Identity Fields",
        "Employees cannot be reliably identified or matched in the target HRIS system.",
        "Populate Worker ID, First Name, and Last Name for every employee before any system load.",
        "Findings_Missing_ID",
    ),
    # HIGH
    "phone_invalid": (
        "Invalid Phone Numbers",
        "Employees cannot be contacted for benefits, emergencies, or onboarding — and corrupted phone data suggests other fields may also be affected.",
        "Re-export the file from the source system and verify the phone field mapping is correct.",
        "Findings_Invalid_Phone",
    ),
    "salary_suspicious_default": (
        "Suspicious Salary Defaults",
        "Payroll will use incorrect compensation amounts, causing underpayment or overpayment.",
        "Verify the correct salary for each flagged employee from HR records or payroll before migrating.",
        "Findings_Salary_Defaults",
    ),
    "hire_date_suspicious_default": (
        "Suspicious Hire Dates",
        "Seniority, benefits eligibility, and tenure calculations will be wrong for every flagged employee.",
        "Research the correct hire date for each flagged record from offer letters or original employment records.",
        "Findings_Hire_Defaults",
    ),
    "impossible_dates": (
        "Impossible Dates",
        "Tenure, benefits, and compliance calculations will fail or produce inaccurate results.",
        "Correct each impossible date by reviewing the employee's source records.",
        "Findings_Impossible_Dates",
    ),
    "manager_loop": (
        "Manager Reporting Loops",
        "The organisational hierarchy is circular — automated approval workflows will deadlock.",
        "Review each loop and correct the manager assignment so every path ends at a top-level employee with no manager.",
        "Findings_Mgr_Loops",
    ),
    "pay_equity_flag": (
        "Pay Equity Variance",
        "Salary differences above 30% within the same role may indicate inequitable pay that creates legal exposure.",
        "Review each flagged role-department group and document a legitimate reason for every variance above 30%.",
        "Findings_Pay_Equity",
    ),
    "ghost_employee_indicator": (
        "Ghost Employee Indicators",
        "Records show characteristics consistent with payroll fraud — active status with no salary, department, or manager.",
        "Investigate each flagged record immediately with direct managers and payroll records.",
        "Findings_Ghost_Employees",
    ),
    "duplicate_canonical_conflict": (
        "Duplicate Field Mapping Conflict",
        "Multiple source columns resolve to the same HRIS field with conflicting values.",
        "Identify the authoritative source column for each field and remove or consolidate the others.",
        None,   # dataset-wide issue, no per-row tab
    ),
    # MEDIUM
    "duplicate_email": (
        "Duplicate Emails",
        "HRIS login, notifications, and self-service access will break for every employee sharing an address.",
        "Assign a unique, valid email address to each employee before loading data into any system.",
        "Findings_Dup_Email",
    ),
    "duplicate_name_different_id": (
        "Duplicate Names with Different IDs",
        "The matching engine may link corrections to the wrong person during reconciliation.",
        "Review each name pair — merge records if they are the same person, or add a distinguishing identifier if they are different people.",
        "Findings_Dup_Names",
    ),
    "status_no_terminated": (
        "No Terminated Employees",
        "Employment history appears incomplete, which affects compliance reporting and rehire eligibility tracking.",
        "Confirm whether terminated employees are intentionally excluded; if not, re-export the full workforce history.",
        None,   # dataset-level, no rows to show
    ),
    "status_high_pending": (
        "High Pending Status Rate",
        "Pending employees cannot be classified for payroll eligibility, headcount, or benefits enrollment.",
        "Update every Pending employee to the correct final status (Active, Inactive, or Terminated) before migration.",
        "Findings_Pending_Status",
    ),
    "age_uniformity": (
        "Age Data Issues",
        "Age-dependent benefits thresholds, compliance reporting, and retirement planning will be incorrect.",
        "Replace placeholder ages with actual employee date-of-birth data from the source system.",
        "Findings_Age_Data",
    ),
    "high_blank_rate": (
        "Missing Data",   # qualified with field name at runtime
        "Incomplete records reduce reporting accuracy and may cause downstream processing failures.",
        "Fill in missing values from the source system before using this data for any operational purpose.",
        "Findings_Completeness",
    ),
    "salary_outlier": (
        "Salary Outliers by Department",
        "Outlier salaries distort compensation analysis and may indicate data entry errors.",
        "Review each flagged salary against role and seniority benchmarks and correct any errors.",
        "Findings_Salary_Outliers",
    ),
    "missing_manager": (
        "Missing Manager Assignment",
        "The organisational hierarchy is incomplete — approval workflows and reporting structures will not function.",
        "Assign a valid manager ID to every active employee who lacks one.",
        "Findings_Missing_Mgr",
    ),
    # LOW
    "suspicious_round_salary": (
        "Suspicious Round Salaries",
        "Compensation data may be inaccurate, leading to incorrect pay or reporting.",
        "Verify the correct salary for each flagged employee with payroll records.",
        "Findings_Round_Salary",
    ),
    "combined_field": (
        "Combined Field",   # qualified with field name at runtime
        "The column cannot be mapped directly to any standard HRIS field without splitting.",
        "Split the column into two separate fields before loading into any target system.",
        None,   # affects the whole column, not individual rows
    ),
}

_DEFAULT_META = (
    None,   # issue_name → use check_name from JSON
    "This issue may affect data integrity and should be reviewed before migration or reporting.",
    "Review and correct all flagged records before using this data for any operational purpose.",
    None,   # no detail sheet
)


def _meta(check_key: str, check_name_fallback: str, field: str = "") -> tuple[str, str, str, str | None]:
    """Return (issue_name, business_impact, required_action, detail_sheet)."""
    base = _M.get(check_key, _DEFAULT_META)
    issue_name = base[0] or check_name_fallback
    # Qualify per-field issues
    if check_key in ("high_blank_rate", "combined_field") and field:
        issue_name = f"{base[0] or check_name_fallback} — {field.replace('_', ' ').title()}"
    return issue_name, base[1], base[2], base[3]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _read_and_normalize(
    file_path: Path, sheet_name: int | str = 0
) -> tuple[pd.DataFrame, dict[str, str], list[dict]]:
    df = ia._read_input(file_path, sheet_name=sheet_name)
    df = df.replace("", pd.NA)
    df.columns = [str(c).strip() for c in df.columns]
    try:
        from src.mapping import _apply_aliases  # noqa: PLC0415
        df = _apply_aliases(df)
    except Exception:
        pass
    col_map: dict[str, str] = {}
    for col in df.columns:
        norm = str(col).strip().lower().replace(" ", "_")
        col_map[col] = ia.ALIASES.get(norm, norm)
    df_norm_pre = df.rename(columns=col_map)
    _, _, row_annotations = ia.analyze_duplicate_canonical_fields(df_norm_pre, col_map)
    df_norm = ia._collapse_duplicate_columns(df_norm_pre)
    return df_norm, col_map, row_annotations


def _load_summary(run_dir: Path) -> dict:
    report_path = run_dir / "internal_audit_report.json"
    if not report_path.exists():
        raise FileNotFoundError(f"internal_audit_report.json not found in {run_dir}")
    return json.loads(report_path.read_text(encoding="utf-8"))


def _load_data_csv(run_dir: Path) -> pd.DataFrame | None:
    """Load internal_audit_data.csv and return only the audit-result rows."""
    path = run_dir / "internal_audit_data.csv"
    if not path.exists():
        return None
    raw = pd.read_csv(path)
    return raw[raw["check_name"].notna() & raw["severity"].notna()].copy()


def _humanize(col: str) -> str:
    return str(col).replace("_", " ").strip().title()


def _fmt(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.rename(columns={c: _humanize(c) for c in df.columns})


def _safe(val: object) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    return "" if s.lower() in ("nan", "nat", "none", "<na>") else s


def _records_affected(finding: dict) -> int | str:
    check_key = str(finding.get("check_key", ""))
    count = int(finding.get("count", finding.get("row_count", 0)) or 0)
    if check_key in DATASET_LEVEL_CHECKS:
        return "Dataset Level"
    return count


def _ordered_findings(summary: dict) -> list[dict]:
    """
    Return findings sorted CRITICAL→LOW then by count descending.
    Source of truth: findings_for_pdf or findings from JSON.
    """
    findings = summary.get("findings_for_pdf") or summary.get("findings") or []
    def _sort_key(f: dict) -> tuple[int, int]:
        sev = str(f.get("severity", "")).upper()
        cnt = int(f.get("count", f.get("row_count", 0)) or 0)
        return (SEVERITY_PRIORITY.get(sev, 9), -cnt)
    return sorted(findings, key=_sort_key)


# ── Sheet 1: Executive_Summary ────────────────────────────────────────────────

def _executive_summary_sheet(summary: dict) -> pd.DataFrame:
    """
    One row per finding, ordered by severity then count.
    Mirrors the PDF findings-by-severity section.
    Uses human-readable language throughout.
    """
    rows = []
    for finding in _ordered_findings(summary):
        check_key  = str(finding.get("check_key", ""))
        check_name = str(finding.get("check_name", ""))
        sev        = str(finding.get("severity", "")).upper()
        field      = str(finding.get("field", ""))
        ra         = _records_affected(finding)
        description = str(finding.get("description", ""))

        issue_name, business_impact, required_action, _ = _meta(check_key, check_name, field)

        rows.append({
            "Severity":         sev,
            "Issue Name":       issue_name,
            "Records Affected": ra,
            "Description":      description,
            "Business Impact":  business_impact,
            "Required Action":  required_action,
        })

    if not rows:
        return pd.DataFrame([{"Note": "No issues found — dataset passed all checks."}])
    return pd.DataFrame(rows)


# ── Sheet 2: Fix_List ─────────────────────────────────────────────────────────

def _fix_list_sheet(summary: dict) -> pd.DataFrame:
    """
    All issues sorted by Priority Rank then count.
    Columns designed for HR action: what's wrong, how serious, what to do, blocking?
    """
    rows = []
    for finding in _ordered_findings(summary):
        check_key  = str(finding.get("check_key", ""))
        check_name = str(finding.get("check_name", ""))
        sev        = str(finding.get("severity", "")).upper()
        field      = str(finding.get("field", ""))
        ra         = _records_affected(finding)

        issue_name, _, required_action, _ = _meta(check_key, check_name, field)

        rows.append({
            "Issue Name":       issue_name,
            "Severity":         sev,
            "Records Affected": ra,
            "Priority Rank":    SEVERITY_PRIORITY.get(sev, 9),
            "What To Do":       required_action,
            "Blocking?":        "Yes" if sev in BLOCKING_SEVERITIES else "No",
        })

    if not rows:
        return pd.DataFrame([{"Note": "No issues found — dataset passed all checks."}])
    return pd.DataFrame(rows)


# ── Sheet 3: Findings_Detail (per issue) ─────────────────────────────────────

DETAIL_COLUMNS = ["Worker ID", "First Name", "Last Name",
                  "Issue Name", "Issue Description", "Current Value", "Expected Fix"]


def _detail_note_row(msg: str) -> dict:
    return {"Worker ID": "ℹ Note", "First Name": None, "Last Name": None,
            "Issue Name": None, "Issue Description": msg,
            "Current Value": None, "Expected Fix": None}


def _build_sample_detail(
    data_csv_df: pd.DataFrame | None,
    check_name: str,
    finding: dict,
) -> pd.DataFrame:
    """Build a detail tab from internal_audit_data.csv sample rows."""
    if data_csv_df is None or data_csv_df.empty:
        total = int(finding.get("count", finding.get("row_count", 0)) or 0)
        return pd.DataFrame([_detail_note_row(
            f"No row-level sample data available. This issue affects {total:,} records. "
            "Export internal_audit_data.csv for the full list."
        )])

    rows = data_csv_df[data_csv_df["check_name"] == check_name].copy()
    if rows.empty:
        return pd.DataFrame()

    total = int(finding.get("count", finding.get("row_count", 0)) or 0)

    detail = pd.DataFrame({
        "Worker ID":       rows["employee_id"].map(_safe),
        "First Name":      rows["first_name"].map(_safe),
        "Last Name":       rows["last_name"].map(_safe),
        "Issue Name":      rows["check_name"].map(_safe),
        "Issue Description": rows["issue_description"].map(_safe),
        "Current Value":   rows["value_found"].map(_safe),
        "Expected Fix":    rows["recommended_action"].map(_safe),
    }).reset_index(drop=True)

    shown = len(detail)
    if total > shown:
        note = _detail_note_row(
            f"Showing {shown} sample records of {total:,} total. "
            "See internal_audit_data.csv for the complete list."
        )
        detail = pd.concat([pd.DataFrame([note]), detail], ignore_index=True)

    return detail


def _build_full_detail_dup_worker_id(
    df: pd.DataFrame,
    summary: dict,
    row_annotations: list[dict],
) -> pd.DataFrame:
    frames = []

    # Canonical conflict rows
    for idx, anns in enumerate(row_annotations):
        det = (anns or {}).get("worker_id")
        if not det or det.get("duplicate_classification") != "duplicate_conflicting_values":
            continue
        try:
            row = df.iloc[idx]
        except IndexError:
            continue
        frames.append({
            "Worker ID":       _safe(row.get("worker_id", "")),
            "First Name":      _safe(row.get("first_name", "")),
            "Last Name":       _safe(row.get("last_name", "")),
            "Issue Name":      "Duplicate Worker ID (Source Column Conflict)",
            "Issue Description": "Two source columns map to Worker ID with conflicting values.",
            "Current Value":   _safe(det.get("duplicate_values", "")),
            "Expected Fix":    "Decide which source column is authoritative and remove the other.",
        })

    # Standard duplicate worker_id rows
    if "worker_id" in df.columns:
        series = df["worker_id"].astype(str).str.strip()
        nonblank = series[(series != "") & (series.str.lower() != "nan")]
        dup_idx = nonblank[nonblank.duplicated(keep=False)].index
        for orig_idx in dup_idx:
            row = df.loc[orig_idx]
            frames.append({
                "Worker ID":       _safe(row.get("worker_id", "")),
                "First Name":      _safe(row.get("first_name", "")),
                "Last Name":       _safe(row.get("last_name", "")),
                "Issue Name":      "Duplicate Worker ID",
                "Issue Description": "Multiple employees share this Worker ID.",
                "Current Value":   _safe(row.get("worker_id", "")),
                "Expected Fix":    "Assign a unique Worker ID to each employee.",
            })

    if not frames:
        return pd.DataFrame()
    result = pd.DataFrame(frames, columns=DETAIL_COLUMNS)
    if len(result) > DETAIL_ROW_CAP:
        note = _detail_note_row(f"Truncated to {DETAIL_ROW_CAP:,} rows. Full list in internal_audit_data.csv.")
        result = pd.concat([pd.DataFrame([note]), result.head(DETAIL_ROW_CAP)], ignore_index=True)
    return result


def _build_full_detail_salary(df: pd.DataFrame) -> pd.DataFrame:
    status_col  = ia._status_column(df)
    has_salary  = "salary" in df.columns
    has_payrate = "payrate" in df.columns
    if not status_col or (not has_salary and not has_payrate):
        return pd.DataFrame()

    statuses = df[status_col].astype(str).str.strip().str.lower()
    sal_vals  = pd.to_numeric(df["salary"],  errors="coerce") if has_salary  else pd.Series([float("nan")] * len(df), index=df.index)
    pay_vals  = pd.to_numeric(df["payrate"], errors="coerce") if has_payrate else pd.Series([float("nan")] * len(df), index=df.index)
    sal_blank = ia._blank_mask(df["salary"])  if has_salary  else pd.Series([True] * len(df), index=df.index)
    pay_blank = ia._blank_mask(df["payrate"]) if has_payrate else pd.Series([True] * len(df), index=df.index)
    effective = sal_vals.where(~sal_blank, pay_vals)
    comp_blank = sal_blank & pay_blank
    mask = (statuses == "active") & (comp_blank | (effective <= 0))

    rows = []
    for orig_idx in df.index[mask.fillna(False)]:
        row = df.loc[orig_idx]
        sal = _safe(row.get("salary", ""))
        pay = _safe(row.get("payrate", ""))
        current = "Missing" if (not sal and not pay) else (f"Salary: {sal}" if sal else f"Payrate: {pay}")
        rows.append({
            "Worker ID":       _safe(row.get("worker_id", "")),
            "First Name":      _safe(row.get("first_name", "")),
            "Last Name":       _safe(row.get("last_name", "")),
            "Issue Name":      "Missing or Invalid Salary",
            "Issue Description": "Active employee has no valid salary or payrate.",
            "Current Value":   current,
            "Expected Fix":    "Enter a valid positive salary or payrate.",
        })

    if not rows:
        return pd.DataFrame()
    result = pd.DataFrame(rows, columns=DETAIL_COLUMNS)
    if len(result) > DETAIL_ROW_CAP:
        note = _detail_note_row(f"Truncated to {DETAIL_ROW_CAP:,} rows. Full list in internal_audit_data.csv.")
        result = pd.concat([pd.DataFrame([note]), result.head(DETAIL_ROW_CAP)], ignore_index=True)
    return result


def _build_full_detail_invalid_dates(df: pd.DataFrame) -> pd.DataFrame:
    hire_col = ia._first_present(df, ["hire_date", "start_date", "date_hired"])
    term_col  = ia._first_present(df, ["termination_date", "term_date", "end_date"])
    if not hire_col:
        return pd.DataFrame()

    today = pd.Timestamp.now().normalize().tz_localize(None)
    hire_dates = ia._date_series(df, hire_col)
    rows = []

    future_mask = hire_dates > today
    for orig_idx in df.index[future_mask.fillna(False)]:
        row = df.loc[orig_idx]
        rows.append({
            "Worker ID":       _safe(row.get("worker_id", "")),
            "First Name":      _safe(row.get("first_name", "")),
            "Last Name":       _safe(row.get("last_name", "")),
            "Issue Name":      "Invalid Dates",
            "Issue Description": "Hire date is set in the future.",
            "Current Value":   _safe(row.get(hire_col, "")),
            "Expected Fix":    "Correct the hire date to today or earlier.",
        })

    if term_col:
        term_dates = ia._date_series(df, term_col)
        tbh_mask = (term_dates < hire_dates) & term_dates.notna() & hire_dates.notna()
        for orig_idx in df.index[tbh_mask.fillna(False)]:
            row = df.loc[orig_idx]
            rows.append({
                "Worker ID":       _safe(row.get("worker_id", "")),
                "First Name":      _safe(row.get("first_name", "")),
                "Last Name":       _safe(row.get("last_name", "")),
                "Issue Name":      "Invalid Dates",
                "Issue Description": "Termination date is before hire date.",
                "Current Value":   (
                    f"Hired: {_safe(row.get(hire_col, ''))}  |  "
                    f"Terminated: {_safe(row.get(term_col, ''))}"
                ),
                "Expected Fix":    "Ensure termination date is on or after the hire date.",
            })

    if not rows:
        return pd.DataFrame()
    result = pd.DataFrame(rows, columns=DETAIL_COLUMNS)
    if len(result) > DETAIL_ROW_CAP:
        note = _detail_note_row(f"Truncated to {DETAIL_ROW_CAP:,} rows.")
        result = pd.concat([pd.DataFrame([note]), result.head(DETAIL_ROW_CAP)], ignore_index=True)
    return result


def _build_full_detail_active_term(df: pd.DataFrame) -> pd.DataFrame:
    status_col = ia._status_column(df)
    term_col   = ia._first_present(df, ["termination_date", "term_date", "end_date"])
    if not status_col or not term_col:
        return pd.DataFrame()

    statuses  = df[status_col].astype(str).str.strip().str.lower()
    term_blank = ia._blank_mask(df[term_col])
    mask = (statuses == "active") & ~term_blank

    rows = []
    for orig_idx in df.index[mask.fillna(False)]:
        row = df.loc[orig_idx]
        rows.append({
            "Worker ID":       _safe(row.get("worker_id", "")),
            "First Name":      _safe(row.get("first_name", "")),
            "Last Name":       _safe(row.get("last_name", "")),
            "Issue Name":      "Active Employees with Termination Dates",
            "Issue Description": "Employee is marked Active but also has a termination date.",
            "Current Value":   f"Status: Active  |  Termination Date: {_safe(row.get(term_col, ''))}",
            "Expected Fix":    "Remove the termination date, or change status to Terminated.",
        })

    if not rows:
        return pd.DataFrame()
    result = pd.DataFrame(rows, columns=DETAIL_COLUMNS)
    if len(result) > DETAIL_ROW_CAP:
        note = _detail_note_row(f"Truncated to {DETAIL_ROW_CAP:,} rows.")
        result = pd.concat([pd.DataFrame([note]), result.head(DETAIL_ROW_CAP)], ignore_index=True)
    return result


def _build_full_detail_missing_id(df: pd.DataFrame) -> pd.DataFrame:
    required = [f for f in ["worker_id", "first_name", "last_name"] if f in df.columns]
    if not required:
        return pd.DataFrame()

    masks = [ia._blank_mask(df[f]) for f in required]
    combined = masks[0].copy()
    for m in masks[1:]:
        combined = combined | m

    _labels = {"worker_id": "Worker ID", "first_name": "First Name", "last_name": "Last Name"}
    rows = []
    for orig_idx in df.index[combined.fillna(False)]:
        row = df.loc[orig_idx]
        missing = [_labels.get(f, f) for f in required if _safe(row.get(f, "")) == ""]
        rows.append({
            "Worker ID":       _safe(row.get("worker_id", "")),
            "First Name":      _safe(row.get("first_name", "")),
            "Last Name":       _safe(row.get("last_name", "")),
            "Issue Name":      "Missing Required Identity Fields",
            "Issue Description": f"Required fields are blank: {', '.join(missing)}.",
            "Current Value":   "Missing: " + ", ".join(missing),
            "Expected Fix":    "Provide Worker ID, First Name, and Last Name for every employee.",
        })

    if not rows:
        return pd.DataFrame()
    result = pd.DataFrame(rows, columns=DETAIL_COLUMNS)
    if len(result) > DETAIL_ROW_CAP:
        note = _detail_note_row(f"Truncated to {DETAIL_ROW_CAP:,} rows.")
        result = pd.concat([pd.DataFrame([note]), result.head(DETAIL_ROW_CAP)], ignore_index=True)
    return result


# Full-detail dispatch table: check_key → builder function (takes df, summary, row_annotations)
_FULL_DETAIL_BUILDERS = {
    "duplicate_worker_id":                    lambda df, s, ra: _build_full_detail_dup_worker_id(df, s, ra),
    "duplicate_canonical_worker_id_conflict": lambda df, s, ra: _build_full_detail_dup_worker_id(df, s, ra),
    "active_zero_salary":                     lambda df, s, ra: _build_full_detail_salary(df),
    "invalid_date_logic":                     lambda df, s, ra: _build_full_detail_invalid_dates(df),
    "active_with_termination_date":           lambda df, s, ra: _build_full_detail_active_term(df),
    "missing_required_identity":              lambda df, s, ra: _build_full_detail_missing_id(df),
}


def _build_findings_detail_sheets(
    df: pd.DataFrame,
    summary: dict,
    row_annotations: list[dict],
    run_dir: Path,
) -> dict[str, pd.DataFrame]:
    """
    Returns { sheet_name: DataFrame } for all per-issue detail tabs.

    CRITICAL checks → full row expansion from source data.
    HIGH/MEDIUM checks → sample rows from internal_audit_data.csv + count note.
    Dataset-level checks (status_no_terminated) → no tab.
    """
    data_csv = _load_data_csv(run_dir)
    findings  = _ordered_findings(summary)

    result: dict[str, pd.DataFrame] = {}
    populated: set[str] = set()  # avoid building a sheet twice (e.g., dup_worker_id + canonical)

    for finding in findings:
        check_key  = str(finding.get("check_key", ""))
        check_name = str(finding.get("check_name", ""))
        field      = str(finding.get("field", ""))
        count      = int(finding.get("count", finding.get("row_count", 0)) or 0)

        if count == 0:
            continue

        _, _, _, detail_sheet = _meta(check_key, check_name, field)

        if not detail_sheet:
            continue  # dataset-level; no tab
        if detail_sheet in populated:
            continue

        if check_key in _FULL_DETAIL_BUILDERS:
            frame = _FULL_DETAIL_BUILDERS[check_key](df, summary, row_annotations)
        else:
            frame = _build_sample_detail(data_csv, check_name, finding)

        if frame is not None and not frame.empty:
            result[detail_sheet] = frame
            populated.add(detail_sheet)

    return result


# ── Sheet 4: Findings_Index ───────────────────────────────────────────────────

def _findings_index_sheet(summary: dict, detail_sheet_names: set[str]) -> pd.DataFrame:
    """Master index: one row per finding, with detail-sheet cross-reference."""
    rows = []
    for finding in _ordered_findings(summary):
        check_key  = str(finding.get("check_key", ""))
        check_name = str(finding.get("check_name", ""))
        sev        = str(finding.get("severity", "")).upper()
        field      = str(finding.get("field", ""))
        ra         = _records_affected(finding)

        issue_name, _, _, detail_sheet = _meta(check_key, check_name, field)

        rows.append({
            "Issue Name":       issue_name,
            "Severity":         sev,
            "Records Affected": ra,
            "Detail Sheet":     detail_sheet if detail_sheet in detail_sheet_names else "—",
            "Description":      str(finding.get("description", "")),
        })

    if not rows:
        return pd.DataFrame([{"Note": "No findings recorded."}])
    return pd.DataFrame(rows)


# ── Sheet 5: Data_Quality_Score ───────────────────────────────────────────────

def _data_quality_score_sheet(summary: dict) -> pd.DataFrame:
    sev_counts = summary.get("severity_counts") or {}
    rows = [
        {"Metric": "Total Records",           "Value": summary.get("total_rows", 0)},
        {"Metric": "Completeness %",          "Value": summary.get("overall_completeness", 0)},
        {"Metric": "Gate Status",             "Value": summary.get("gate_status", "—")},
        {"Metric": "Gate Message",            "Value": summary.get("gate_message", "")},
        {"Metric": "Critical Issues",         "Value": sev_counts.get("CRITICAL", 0)},
        {"Metric": "High Issues",             "Value": sev_counts.get("HIGH", 0)},
        {"Metric": "Medium Issues",           "Value": sev_counts.get("MEDIUM", 0)},
        {"Metric": "Low Issues",              "Value": sev_counts.get("LOW", 0)},
        {"Metric": "Source File",             "Value": summary.get("source_filename", "")},
    ]
    return pd.DataFrame(rows)


# ── Sheet assembly ────────────────────────────────────────────────────────────

def _build_sheets(
    file_path: Path, sheet_name: int | str, run_dir: Path
) -> dict[str, pd.DataFrame]:
    summary = _load_summary(run_dir)
    df, _, row_annotations = _read_and_normalize(file_path, sheet_name=sheet_name)

    # Build detail sheets first (to know which sheet names exist for Findings_Index)
    detail_sheets = _build_findings_detail_sheets(df, summary, row_annotations, run_dir)

    sheets: dict[str, pd.DataFrame] = {}

    # 1. Executive_Summary
    sheets["Executive_Summary"] = _executive_summary_sheet(summary)

    # 2. Fix_List
    sheets["Fix_List"] = _fix_list_sheet(summary)

    # 3. Per-issue Findings_* detail tabs (sorted by PDF order)
    sheets.update({name: _fmt(frame) for name, frame in detail_sheets.items()})

    # 4. Findings_Index
    sheets["Findings_Index"] = _findings_index_sheet(summary, set(detail_sheets))

    # 5. Data_Quality_Score
    sheets["Data_Quality_Score"] = _data_quality_score_sheet(summary)

    return sheets


# ── Excel styling pass ────────────────────────────────────────────────────────

def _sev_fill(sev: str) -> PatternFill | None:
    color = SEV_FILLS.get(str(sev).upper())
    if not color:
        return None
    return PatternFill(fill_type="solid", fgColor=color)


def _autosize_and_style(out_path: Path, sev_col_sheets: dict[str, list[str]]) -> None:
    """
    sev_col_sheets: maps sheet_name → list of column headers that contain severity values
                    (used to color-code those cells).
    """
    wb = load_workbook(out_path)
    header_fill = PatternFill(fill_type="solid", fgColor=HEADER_FILL)
    header_font = Font(bold=True)

    for ws in wb.worksheets:
        # Header row
        if ws.max_row >= 1:
            for cell in ws[1]:
                cell.fill = header_fill
                cell.font = header_font

        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        # Column auto-width (sample first 200 rows for speed)
        for col_cells in ws.columns:
            letter = col_cells[0].column_letter
            length = 0
            for cell in col_cells[:200]:
                try:
                    length = max(length, len(str(cell.value or "")))
                except Exception:
                    continue
            ws.column_dimensions[letter].width = min(max(length + 2, 12), 50)

        # Severity-based row / cell coloring
        sev_cols = sev_col_sheets.get(ws.title, [])
        if not sev_cols:
            continue

        # Build column-index map from headers
        col_idx: dict[str, int] = {}   # header → 0-based index
        for cell in ws[1]:
            h = str(cell.value or "").strip()
            if h:
                col_idx[h] = cell.column - 1

        for row_cells in ws.iter_rows(min_row=2):
            sev_val = ""
            for sev_col in sev_cols:
                idx = col_idx.get(sev_col)
                if idx is not None and idx < len(row_cells):
                    sev_val = str(row_cells[idx].value or "").strip().upper()
                    if sev_val:
                        break

            fill = _sev_fill(sev_val)
            if fill:
                for cell in row_cells:
                    cell.fill = fill

    wb.save(out_path)


# ── Entry points ──────────────────────────────────────────────────────────────

def build_workbook(
    file_path: Path, run_dir: Path, out_path: Path, sheet_name: int | str = 0
) -> None:
    sheets = _build_sheets(file_path, sheet_name, run_dir)

    # Sheets where severity cells should drive row coloring
    sev_col_sheets = {
        "Executive_Summary": ["Severity"],
        "Fix_List":          ["Severity"],
        "Findings_Index":    ["Severity"],
    }

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for key, frame in sheets.items():
            frame.to_excel(writer, sheet_name=key, index=False)

    _autosize_and_style(out_path, sev_col_sheets)
    print(f"[build_internal_audit_workbook] wrote: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build internal audit workbook")
    parser.add_argument("--file",       required=True, help="Source file used for the audit")
    parser.add_argument("--run-dir",    required=True, help="Directory containing internal audit outputs")
    parser.add_argument("--out",        required=True, help="Output workbook path")
    parser.add_argument("--sheet-name", default="0",  help="Excel sheet index or name")
    args = parser.parse_args()
    sheet_name: int | str = (
        int(args.sheet_name) if str(args.sheet_name).lstrip("-").isdigit() else args.sheet_name
    )
    build_workbook(Path(args.file), Path(args.run_dir), Path(args.out), sheet_name=sheet_name)


if __name__ == "__main__":
    main()
