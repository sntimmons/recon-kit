"""
build_internal_audit_exports.py - Full-row CSV exports and zip packaging for internal audit runs.

Produces:
  fix_duplicates_full.csv   - All duplicate Worker ID / Email / Name rows
  fix_salary_full.csv       - All active employees with missing or invalid compensation
  fix_identity_full.csv     - All employees missing required identity fields or with invalid phones
  fix_dates_full.csv        - All employees with invalid date logic
  fix_status_full.csv       - All employees with conflicting status or high pending rate
  fix_data_quality_full.csv - All employees with blank required fields (high_blank_rate check)
  clean_data_ready_for_review.csv - Full dataset with review metadata columns added
  review_required_rows.csv - Only rows that still require human review
  correction_salary.csv - Salary correction template for upload preparation
  correction_status.csv - Status correction template for upload preparation
  correction_dates.csv - Date correction template for upload preparation
  internal_audit_outputs.zip - Workbook + all generated fix_*_full.csv files

Coverage notes (printed to stdout after run):
  FULL COVERAGE  - Duplicates (Worker ID, Email, Name), Salary (zero/missing), Identity
                   (missing fields, phone heuristic), Dates (future hire, term before hire),
                   Status (active+term date), Data Quality (blank fields from JSON findings)
  SAMPLE ONLY    - suspicious_round_salary, salary_suspicious_default, hire_date_suspicious_default,
                   impossible_dates, manager_loop, missing_manager (use internal_audit_data.csv)
  NOT EXPORTABLE - age_uniformity (statistical), combined_field (column-level),
                   pay_equity_flag (dept-level), salary_outlier (dept-level),
                   ghost_employee_indicator (multi-factor heuristic), status_no_terminated (dataset-level)
"""

from __future__ import annotations

import argparse
import json
import sys
import zipfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from audit import internal_audit as ia


# ---------------------------------------------------------------------------
# Shared columns for all fix_*_full.csv files
# ---------------------------------------------------------------------------
FIX_COLUMNS = [
    "Worker ID",
    "First Name",
    "Last Name",
    "Issue Name",
    "Severity",
    "Reason",
    "Current Value",
    "Recommended Action",
    "Row Number",
]

# Optional context columns appended when data is present
CONTEXT_COLUMNS = [
    "Department",
    "Job Title",
    "Status",
    "Leave Status",
    "Worker Type",
    "Benefits Eligible",
    "Benefit Plan",
    "Coverage Level",
    "Dependent Count",
    "Benefits Start Date",
    "Benefits End Date",
    "Pay Type",
    "Salary",
    "Pay Rate",
    "Standard Hours",
    "Annualized Pay",
    "Annualized Difference",
    "Hire Date",
    "Termination Date",
    "Email",
]

REVIEW_METADATA_COLUMNS = [
    "Review Status",
    "Issue Count",
    "Issue Names",
    "Highest Severity",
    "Manual Review Required",
    "Recommended Next Step",
]

SEVERITY_RANK = {
    "CRITICAL": 1,
    "HIGH": 2,
    "MEDIUM": 3,
    "LOW": 4,
}

REVIEW_REQUIRED_PRIORITY = [
    "Worker ID",
    "First Name",
    "Last Name",
    "Review Status",
    "Highest Severity",
    "Issue Count",
    "Issue Names",
    "Recommended Next Step",
    "Department",
    "Job Title",
    "Status",
    "Leave Status",
    "Worker Type",
    "Benefits Eligible",
    "Benefit Plan",
    "Coverage Level",
    "Dependent Count",
    "Benefits Start Date",
    "Benefits End Date",
    "Pay Type",
    "Salary",
    "Pay Rate",
    "Standard Hours",
    "Hire Date",
    "Termination Date",
]

REVIEW_REQUIRED_ALWAYS_INCLUDE_SEVERITIES = {"CRITICAL", "HIGH"}

REVIEW_REQUIRED_MEDIUM_INCLUDE_NAMES = {
    "Duplicate Name - Different Worker ID",
}

REVIEW_REQUIRED_MEDIUM_INCLUDE_PREFIXES = (
    "Missing Data -",
)

REVIEW_REQUIRED_MEDIUM_EXCLUDE_NAMES = {
    "Duplicate Email",
    "Age Data Issues",
    "Combined Field",
    "No Terminated Employees",
}

CORRECTION_BASE_COLUMNS = [
    "Worker ID",
    "First Name",
    "Last Name",
    "Issue Name",
    "Current Value",
    "Corrected Value",
    "Effective Date",
    "Notes",
]

CORRECTION_SALARY_COLUMNS = [
    "Worker ID",
    "First Name",
    "Last Name",
    "Issue Name",
    "Related Issues",
    "Severity",
    "Current Pay Type",
    "Current Salary",
    "Current Pay Rate",
    "Corrected Pay Type",
    "Corrected Salary",
    "Corrected Pay Rate",
    "Effective Date",
    "Notes",
]

# ---------------------------------------------------------------------------
# Payroll issue presentation - primary issue hierarchy and display labels
# ---------------------------------------------------------------------------

# Priority order for selecting the primary (lead) issue in payroll correction rows.
# First matching issue in a group becomes the primary label shown to HR reviewers.
_PAYROLL_ISSUE_PRIORITY = [
    "Implausible Hourly Pay Rate",        # CRITICAL: payrate <= 0 - always the clearest lead
    "Implausible Annual Salary",          # CRITICAL: salary <= 0 - always the clearest lead
    "Missing or Invalid Salary",          # CRITICAL: active with no valid comp
    "Missing or Invalid Pay Type",        # pay type is absent or unrecognised
    "Compensation Type Mismatch",         # pay type and comp field conflict
    "Salary and Pay Rate Conflict",       # both fields populated, one is wrong
    "Missing Standard Hours for Hourly Worker",
]

# Human-readable display labels for the two implausible-value issues.
# These replace the "Implausible" wording in correction and review outputs.
_PAYROLL_DISPLAY_LABELS: dict[str, str] = {
    "Implausible Hourly Pay Rate": "Invalid Hourly Pay Rate",
    "Implausible Annual Salary":   "Invalid Annual Salary",
}

# Issues that are consequences of a zero/negative value and are suppressed
# from Related Issues and review issue_names rollups when an Implausible*
# CRITICAL issue is already the primary.  They add no new information.
_PAYROLL_ZERO_CONSEQUENCES = frozenset({
    "Missing or Invalid Salary",
    "Compensation Type Mismatch",
})

CORRECTION_FILE_CONFIG = {
    "correction_salary.csv": {
        "issue_names": {
            "Missing or Invalid Salary",
            "Missing or Invalid Pay Type",
            "Compensation Type Mismatch",
            "Implausible Hourly Pay Rate",
            "Implausible Annual Salary",
        },
        "extra_columns": ["Pay Type", "Salary", "Pay Rate", "Standard Hours"],
    },
    "correction_status.csv": {
        "issue_names": {"Pending Status", "Active Employee with Termination Date"},
        "extra_columns": ["Status"],
    },
    "correction_dates.csv": {
        "issue_names": {"Invalid Dates"},
        "extra_columns": ["Hire Date", "Termination Date"],
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(val: object) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    return "" if s.lower() in ("nan", "nat", "none", "<na>") else s


def _row_num(orig_idx: object) -> str:
    try:
        return str(int(orig_idx) + 2)
    except Exception:
        return ""


def _get(row: pd.Series, *keys: str) -> str:
    for key in keys:
        if key in row.index:
            v = _safe(row[key])
            if v:
                return v
    return ""


def _context(row: pd.Series) -> dict:
    return {
        "Department":        _get(row, "department", "district"),
        "Job Title":         _get(row, "job_title", "title", "position_title", "position"),
        "Status":            _get(row, "worker_status", "status"),
        "Leave Status":      _get(row, "leave_status", "absence_status", "loa_status"),
        "Worker Type":       _get(row, "worker_type", "employment_type", "pay_type"),
        "Benefits Eligible": _get(row, "benefits_eligible", "benefit_eligible", "benefits_eligibility", "benefit_eligibility"),
        "Benefit Plan":      _get(row, "benefit_plan", "benefits_plan", "benefit_plan_name", "plan_name"),
        "Coverage Level":    _get(row, "coverage_level", "benefit_coverage_level", "coverage_tier"),
        "Dependent Count":   _get(row, "dependent_count", "dependents", "covered_dependents"),
        "Benefits Start Date": _get(row, "benefits_start_date", "benefit_start_date", "coverage_start_date"),
        "Benefits End Date": _get(row, "benefits_end_date", "benefit_end_date", "coverage_end_date"),
        "Pay Type":          _get(row, "pay_type", "worker_type"),
        "Salary":            _get(row, "salary"),
        "Pay Rate":          _get(row, "payrate"),
        "Standard Hours":    _get(row, "standard_hours"),
        "Hire Date":         _get(row, "hire_date", "start_date", "date_hired"),
        "Termination Date":  _get(row, "termination_date", "term_date", "end_date"),
        "Email":             _get(row, "email"),
    }


def _make_row(
    row: pd.Series,
    orig_idx: object,
    issue_name: str,
    severity: str,
    why_flagged: str,
    current_value: str,
    fix_needed: str,
) -> dict:
    base = {
        "Worker ID":    _get(row, "worker_id"),
        "First Name":   _get(row, "first_name"),
        "Last Name":    _get(row, "last_name"),
        "Issue Name":   issue_name,
        "Severity":     severity,
        "Reason":       why_flagged,
        "Current Value": current_value,
        "Recommended Action": fix_needed,
        "Row Number":   _row_num(orig_idx),
    }
    base.update(_context(row))
    return base


def _trim_context(df: pd.DataFrame) -> pd.DataFrame:
    """Drop context columns that are entirely empty."""
    if df.empty:
        return df
    keep = list(FIX_COLUMNS)
    for col in CONTEXT_COLUMNS:
        if col in df.columns and df[col].map(_safe).any():
            keep.append(col)
    existing = [c for c in keep if c in df.columns]
    return df[existing].reset_index(drop=True)


def _unique_ordered(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        safe_value = _safe(value)
        if not safe_value or safe_value in seen:
            continue
        seen.add(safe_value)
        ordered.append(safe_value)
    return ordered


def _display_name(column: str) -> str:
    direct = {
        "worker_id": "Worker ID",
        "first_name": "First Name",
        "last_name": "Last Name",
        "department": "Department",
        "district": "Department",
        "job_title": "Job Title",
        "title": "Job Title",
        "position_title": "Job Title",
        "position": "Job Title",
        "worker_status": "Status",
        "status": "Status",
        "leave_status": "Leave Status",
        "absence_status": "Leave Status",
        "loa_status": "Leave Status",
        "worker_type": "Worker Type",
        "employment_type": "Worker Type",
        "benefits_eligible": "Benefits Eligible",
        "benefit_eligible": "Benefits Eligible",
        "benefits_eligibility": "Benefits Eligible",
        "benefit_eligibility": "Benefits Eligible",
        "benefit_plan": "Benefit Plan",
        "benefits_plan": "Benefit Plan",
        "benefit_plan_name": "Benefit Plan",
        "coverage_level": "Coverage Level",
        "benefit_coverage_level": "Coverage Level",
        "coverage_tier": "Coverage Level",
        "dependent_count": "Dependent Count",
        "dependents": "Dependent Count",
        "covered_dependents": "Dependent Count",
        "benefits_start_date": "Benefits Start Date",
        "benefit_start_date": "Benefits Start Date",
        "coverage_start_date": "Benefits Start Date",
        "benefits_end_date": "Benefits End Date",
        "benefit_end_date": "Benefits End Date",
        "coverage_end_date": "Benefits End Date",
        "pay_type": "Pay Type",
        "worker_type": "Pay Type",
        "salary": "Salary",
        "payrate": "Pay Rate",
        "standard_hours": "Standard Hours",
        "annualized_pay": "Annualized Pay",
        "annualized_difference": "Annualized Difference",
        "hire_date": "Hire Date",
        "start_date": "Hire Date",
        "date_hired": "Hire Date",
        "termination_date": "Termination Date",
        "term_date": "Termination Date",
        "end_date": "Termination Date",
    }
    return direct.get(column, column.replace("_", " ").title())


def _prepare_clean_base(df: pd.DataFrame) -> pd.DataFrame:
    base = df.copy()
    base["__row_number"] = [str(i + 2) for i in range(len(base))]
    rename_map: dict[str, str] = {}
    used: set[str] = set()
    for col in base.columns:
        if col == "__row_number":
            continue
        display = _display_name(col)
        if display in used:
            display = col.replace("_", " ").title()
        suffix = 2
        while display in used:
            display = f"{col.replace('_', ' ').title()} {suffix}"
            suffix += 1
        used.add(display)
        rename_map[col] = display
    return base.rename(columns=rename_map)


def _match_issue_rows_to_base(issue_rows: pd.DataFrame, clean_base: pd.DataFrame) -> pd.DataFrame:
    if issue_rows.empty:
        return issue_rows.iloc[0:0].copy()

    matched = issue_rows.copy()
    matched["__row_number"] = matched["Row Number"].map(_safe) if "Row Number" in matched.columns else ""

    if "Worker ID" in matched.columns and "Worker ID" in clean_base.columns:
        unique_worker_map: dict[str, str] = {}
        worker_counts = clean_base["Worker ID"].map(_safe).value_counts()
        for _, row in clean_base.iterrows():
            worker_id = _safe(row.get("Worker ID", ""))
            if worker_id and int(worker_counts.get(worker_id, 0)) == 1:
                unique_worker_map[worker_id] = _safe(row.get("__row_number", ""))
        missing_mask = matched["__row_number"].map(_safe) == ""
        matched.loc[missing_mask, "__row_number"] = matched.loc[missing_mask, "Worker ID"].map(
            lambda wid: unique_worker_map.get(_safe(wid), "")
        )

    valid_rows = set(clean_base["__row_number"].map(_safe))
    matched = matched[matched["__row_number"].map(_safe).isin(valid_rows)].copy()
    return matched.reset_index(drop=True)


def _recommended_next_step(actions: list[str]) -> str:
    ordered = _unique_ordered(actions)
    if not ordered:
        return "No action needed"
    if len(ordered) == 1:
        return ordered[0]
    if len(ordered) == 2:
        return "; ".join(ordered)
    return "; ".join(ordered[:2]) + "; plus additional review items"


def _split_issue_names(issue_names_text: object) -> list[str]:
    return [part.strip() for part in _safe(issue_names_text).split(";") if _safe(part)]


def _is_medium_review_issue(issue_name: str) -> bool:
    safe_name = _safe(issue_name)
    if not safe_name:
        return False
    if safe_name in REVIEW_REQUIRED_MEDIUM_EXCLUDE_NAMES:
        return False
    if safe_name in REVIEW_REQUIRED_MEDIUM_INCLUDE_NAMES:
        return True
    return any(safe_name.startswith(prefix) for prefix in REVIEW_REQUIRED_MEDIUM_INCLUDE_PREFIXES)


def _should_include_in_review_required(highest_severity: object, issue_names_text: object) -> bool:
    severity = _safe(highest_severity).upper()
    issue_names = _split_issue_names(issue_names_text)

    if severity in REVIEW_REQUIRED_ALWAYS_INCLUDE_SEVERITIES:
        return True
    if severity != "MEDIUM":
        return False
    return any(_is_medium_review_issue(issue_name) for issue_name in issue_names)


def _aggregate_review_metadata(issue_rows: pd.DataFrame, clean_base: pd.DataFrame) -> pd.DataFrame:
    matched = _match_issue_rows_to_base(issue_rows, clean_base)
    if matched.empty:
        return pd.DataFrame(columns=["__row_number", *REVIEW_METADATA_COLUMNS])

    summaries: list[dict] = []
    for row_number, group in matched.groupby("__row_number", sort=False):
        raw_issue_names = _unique_ordered(group["Issue Name"].tolist()) if "Issue Name" in group.columns else []
        severities = _unique_ordered(group["Severity"].tolist()) if "Severity" in group.columns else []
        highest = min(severities, key=lambda sev: SEVERITY_RANK.get(_safe(sev).upper(), 99)) if severities else ""
        highest = _safe(highest).upper()

        # Collapse co-occurring payroll issues with the same zero/negative root
        # into a single primary label so issue_names does not become a junk drawer.
        issue_names = _deduplicate_salary_issues_for_review(raw_issue_names, group)

        # BLOCKED = must correct before payroll (CRITICAL severity only).
        # REVIEW  = must review before proceeding (HIGH severity or below).
        # This makes the distinction clear: BLOCKED rows need immediate correction;
        # REVIEW rows need a human decision.
        review_status = "Fix Now" if highest == "CRITICAL" else "Review Required"

        summaries.append({
            "__row_number": row_number,
            "Review Status": review_status,
            "Issue Count": len(issue_names),
            "Issue Names": "; ".join(issue_names),
            "Highest Severity": highest,
            "Manual Review Required": "Yes",
            "Recommended Next Step": _recommended_step_from_issues(issue_names, group),
        })

    return pd.DataFrame(summaries)


def _build_clean_review_exports(
    df: pd.DataFrame,
    category_frames: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    clean_base = _prepare_clean_base(df)
    issue_frames = [frame for frame in category_frames.values() if frame is not None and not frame.empty]
    if issue_frames:
        issue_rows = pd.concat(issue_frames, ignore_index=True, sort=False)
    else:
        issue_rows = pd.DataFrame(columns=FIX_COLUMNS + CONTEXT_COLUMNS)

    metadata = _aggregate_review_metadata(issue_rows, clean_base)
    clean = clean_base.merge(metadata, on="__row_number", how="left")

    clean["Review Status"] = clean["Review Status"].fillna("Clean")
    clean["Issue Count"] = clean["Issue Count"].fillna(0).astype(int)
    clean["Issue Names"] = clean["Issue Names"].fillna("")
    clean["Highest Severity"] = clean["Highest Severity"].fillna("")
    clean["Manual Review Required"] = clean["Manual Review Required"].fillna("No")
    clean["Recommended Next Step"] = clean["Recommended Next Step"].fillna("No action needed")

    source_columns = [c for c in clean.columns if c not in {"__row_number", *REVIEW_METADATA_COLUMNS}]
    clean_priority_columns = [
        "Worker ID",
        "First Name",
        "Last Name",
        "Department",
        "Job Title",
        "Status",
        "Worker Type",
        "Pay Type",
        "Benefit Plan",
        "Coverage Level",
        "Hire Date",
        "Termination Date",
    ]
    ordered_source_columns = [c for c in clean_priority_columns if c in source_columns]
    remaining_source_columns = [c for c in source_columns if c not in ordered_source_columns]
    clean = clean[[
        *ordered_source_columns,
        *REVIEW_METADATA_COLUMNS,
        *remaining_source_columns,
    ]].reset_index(drop=True)

    review_required_mask = clean.apply(
        lambda row: _should_include_in_review_required(
            row.get("Highest Severity", ""),
            row.get("Issue Names", ""),
        ),
        axis=1,
    )
    review_required = clean[review_required_mask].copy()
    ordered_review_cols = [c for c in REVIEW_REQUIRED_PRIORITY if c in review_required.columns]
    remaining_review_cols = [
        c for c in review_required.columns
        if c not in set(ordered_review_cols + REVIEW_METADATA_COLUMNS)
    ]
    review_required = review_required[[
        *ordered_review_cols,
        *REVIEW_METADATA_COLUMNS,
        *remaining_review_cols,
    ]].reset_index(drop=True)

    return clean, review_required


def _write_csv(df: pd.DataFrame, path: Path) -> int:
    """Write dataframe to CSV, return row count (excluding header)."""
    if df.empty:
        return 0
    df.to_csv(path, index=False, encoding="utf-8")
    return len(df)


def _write_required_csv(df: pd.DataFrame, path: Path) -> int:
    """Always write required CSV outputs, even if only the header is present."""
    df.to_csv(path, index=False, encoding="utf-8")
    return len(df)


def _remove_if_exists(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass


def _group_issue_rows(issue_frame: pd.DataFrame) -> list[pd.DataFrame]:
    if issue_frame.empty:
        return []

    keyed = issue_frame.copy()
    keyed["__group_key"] = keyed["Row Number"].map(_safe)
    missing_mask = keyed["__group_key"] == ""
    keyed.loc[missing_mask, "__group_key"] = (
        keyed.loc[missing_mask, "Worker ID"].map(_safe) + "|" +
        keyed.loc[missing_mask, "First Name"].map(_safe) + "|" +
        keyed.loc[missing_mask, "Last Name"].map(_safe)
    )
    return [group.copy() for _, group in keyed.groupby("__group_key", sort=False)]


def _first_nonblank(group: pd.DataFrame, column: str) -> str:
    if column not in group.columns:
        return ""
    for value in group[column].tolist():
        safe_value = _safe(value)
        if safe_value:
            return safe_value
    return ""


def _primary_salary_correction(
    issue_names: list[str],
    pay_type: str,
    group: pd.DataFrame,
) -> tuple[str, list[str], str]:
    """Determine the primary issue label, related issues, and lead note for a
    payroll correction row.

    Returns (primary_label, related_issues, note).

    Logic:
    - Walk _PAYROLL_ISSUE_PRIORITY to find the first matching issue.
    - For Implausible* issues, only treat them as primary when CRITICAL (zero/neg).
      HIGH outlier rows must not become primary in correction files; they are
      filtered out before this function is called.
    - Consequence issues (Missing or Invalid Salary, Compensation Type Mismatch)
      are suppressed from Related Issues when the primary is an Implausible* issue
      because they add no new information for the reviewer.
    - Note is a single plain-English action statement matched to primary + pay type.
    """
    pt = pay_type.strip().lower() if pay_type else ""

    # Build a quick severity lookup for this group
    sev_for_issue: dict[str, str] = {}
    if "Issue Name" in group.columns and "Severity" in group.columns:
        for _, r in group.iterrows():
            name = _safe(r.get("Issue Name", ""))
            sev  = _safe(r.get("Severity", "")).upper()
            if name and name not in sev_for_issue:
                sev_for_issue[name] = sev

    primary_raw = ""
    for candidate in _PAYROLL_ISSUE_PRIORITY:
        if candidate not in issue_names:
            continue
        if candidate in ("Implausible Hourly Pay Rate", "Implausible Annual Salary"):
            if sev_for_issue.get(candidate, "") == "CRITICAL":
                primary_raw = candidate
                break
        else:
            primary_raw = candidate
            break
    if not primary_raw:
        primary_raw = issue_names[0] if issue_names else ""

    primary_label = _PAYROLL_DISPLAY_LABELS.get(primary_raw, primary_raw)
    is_implausible_primary = primary_raw in ("Implausible Hourly Pay Rate", "Implausible Annual Salary")

    # Build related issues list - drop primary raw name and optionally suppress consequences
    related: list[str] = []
    for name in issue_names:
        if name == primary_raw:
            continue
        if is_implausible_primary and name in _PAYROLL_ZERO_CONSEQUENCES:
            continue
        related.append(name)

    # Build single clean lead action note
    if primary_raw == "Implausible Hourly Pay Rate":
        note = "Enter a valid positive hourly pay rate before payroll processing."
    elif primary_raw == "Implausible Annual Salary":
        note = "Enter a valid positive annual salary before payroll processing."
    elif primary_raw == "Missing or Invalid Salary":
        if pt == "hourly":
            note = "Enter a valid positive hourly pay rate before payroll processing."
        elif pt == "salaried":
            note = "Enter a valid positive annual salary before payroll processing."
        else:
            note = "Enter a valid positive salary or pay rate before payroll processing."
    elif primary_raw == "Missing or Invalid Pay Type":
        note = "Enter the correct pay type from the allowed list before payroll processing."
    elif primary_raw == "Compensation Type Mismatch":
        if pt == "hourly":
            note = "Enter a valid hourly pay rate that matches the worker pay type."
        elif pt == "salaried":
            note = "Enter a valid annual salary that matches the worker pay type."
        else:
            note = "Enter the correct compensation value for the worker pay type."
    elif primary_raw == "Salary and Pay Rate Conflict":
        if pt == "hourly":
            note = "Remove the salary value - this worker should have a pay rate only."
        elif pt == "salaried":
            note = "Remove the pay rate value - this worker should have a salary only."
        else:
            note = "Keep only the compensation field that matches the intended pay type."
    elif primary_raw == "Missing Standard Hours for Hourly Worker":
        note = "Populate standard hours for this hourly worker before migration."
    else:
        note = _safe(issue_names[0]) if issue_names else ""

    return primary_label, related, note


def _deduplicate_salary_issues_for_review(
    raw_names: list[str],
    group: pd.DataFrame,
) -> list[str]:
    """Collapse co-occurring payroll issues that share the same zero/negative root
    into a single primary label for review_required and clean_data outputs.

    Only applied when a CRITICAL Implausible* issue is present - because in that
    case Missing or Invalid Salary and Compensation Type Mismatch are consequences
    of the same single problem, not independent issues.

    Non-payroll issues in the list are preserved unchanged.
    """
    payroll_set = set(_PAYROLL_ISSUE_PRIORITY)
    has_payroll = any(n in payroll_set for n in raw_names)
    if not has_payroll:
        return raw_names

    # Check for critical implausible issues
    sev_for_issue: dict[str, str] = {}
    if "Issue Name" in group.columns and "Severity" in group.columns:
        for _, r in group.iterrows():
            name = _safe(r.get("Issue Name", ""))
            sev  = _safe(r.get("Severity", "")).upper()
            if name and name not in sev_for_issue:
                sev_for_issue[name] = sev

    has_crit_hourly = sev_for_issue.get("Implausible Hourly Pay Rate", "") == "CRITICAL"
    has_crit_annual = sev_for_issue.get("Implausible Annual Salary",   "") == "CRITICAL"

    if not has_crit_hourly and not has_crit_annual:
        return raw_names

    result: list[str] = []
    suppress: set[str] = set()

    if has_crit_hourly:
        result.append("Invalid Hourly Pay Rate")
        suppress |= {"Implausible Hourly Pay Rate"} | _PAYROLL_ZERO_CONSEQUENCES

    if has_crit_annual:
        result.append("Invalid Annual Salary")
        suppress |= {"Implausible Annual Salary"} | _PAYROLL_ZERO_CONSEQUENCES

    for name in raw_names:
        if name not in suppress:
            result.append(name)

    return _unique_ordered(result)


def _recommended_step_from_issues(
    issue_names: list[str],
    group: pd.DataFrame,
) -> str:
    """Generate a single clean recommended_next_step from deduplicated issue names.

    For payroll correction issues the step is derived from the primary issue
    rather than from raw Fix Needed text, which can be repetitive when multiple
    issues share the same root.
    """
    if not issue_names:
        return "No action needed"

    payroll_set = set(_PAYROLL_DISPLAY_LABELS.values()) | set(_PAYROLL_ISSUE_PRIORITY)
    primary = issue_names[0]

    if primary in payroll_set:
        # Determine pay type from the group if available
        pt = ""
        pt_col = None
        for c in ("Pay Type", "pay_type", "worker_type"):
            if c in group.columns:
                pt_col = c
                break
        if pt_col:
            for val in group[pt_col].tolist():
                v = _safe(val).lower()
                if v:
                    pt = v
                    break

        if primary == "Invalid Hourly Pay Rate":
            return "Enter a valid positive hourly pay rate before payroll processing."
        if primary == "Invalid Annual Salary":
            return "Enter a valid positive annual salary before payroll processing."
        if primary == "Missing or Invalid Salary":
            if pt == "hourly":
                return "Enter a valid positive hourly pay rate before payroll processing."
            if pt == "salaried":
                return "Enter a valid positive annual salary before payroll processing."
            return "Enter a valid positive salary or pay rate before payroll processing."
        if primary == "Implausible Hourly Pay Rate":
            return "Review the hourly pay rate and confirm it is within the expected range."
        if primary == "Implausible Annual Salary":
            return "Review the annual salary - confirm it is within the expected range."
        if primary in ("Missing or Invalid Pay Type", "Compensation Type Mismatch"):
            return "Confirm the pay type and enter the matching compensation value."

    # Non-payroll issues: use original fix-needed style joined step
    fix_needed_raw = (
        _unique_ordered(group["Recommended Action"].tolist())
        if "Recommended Action" in group.columns else []
    )
    return _recommended_next_step(fix_needed_raw)


def _build_correction_template(issue_frame: pd.DataFrame, filename: str) -> pd.DataFrame:
    config = CORRECTION_FILE_CONFIG[filename]
    allowed_issue_names = config["issue_names"]
    extra_columns = config["extra_columns"]

    if issue_frame.empty:
        return pd.DataFrame(columns=[*CORRECTION_BASE_COLUMNS, *extra_columns])

    filtered = issue_frame[issue_frame["Issue Name"].isin(allowed_issue_names)].copy()
    if filtered.empty:
        return pd.DataFrame(columns=[*CORRECTION_BASE_COLUMNS, *extra_columns])

    if filename == "correction_salary.csv":
        rows: list[dict] = []
        for group in _group_issue_rows(filtered):
            correction_group = group.copy()
            # Keep only CRITICAL implausible-value rows; drop HIGH outlier-only rows.
            # This preserves the rule that outlier-only records stay review-only.
            if (correction_group["Issue Name"] == "Implausible Hourly Pay Rate").any():
                correction_group = correction_group[
                    ~(
                        (correction_group["Issue Name"] == "Implausible Hourly Pay Rate")
                        & (correction_group["Severity"] != "CRITICAL")
                    )
                ]
            if (correction_group["Issue Name"] == "Implausible Annual Salary").any():
                correction_group = correction_group[
                    ~(
                        (correction_group["Issue Name"] == "Implausible Annual Salary")
                        & (correction_group["Severity"] != "CRITICAL")
                    )
                ]
            if correction_group.empty:
                continue

            all_issue_names = _unique_ordered(correction_group["Issue Name"].tolist())
            current_pay_type = _first_nonblank(correction_group, "Pay Type")
            severity = _first_nonblank(correction_group, "Severity")
            current_salary = _first_nonblank(correction_group, "Salary")
            current_pay_rate = _first_nonblank(correction_group, "Pay Rate")

            # Determine primary issue, related issues, and clean lead note
            primary_label, related_issues, note = _primary_salary_correction(
                all_issue_names, current_pay_type, correction_group
            )

            rows.append({
                "Worker ID":          _first_nonblank(correction_group, "Worker ID"),
                "First Name":         _first_nonblank(correction_group, "First Name"),
                "Last Name":          _first_nonblank(correction_group, "Last Name"),
                "Issue Name":         primary_label,
                "Related Issues":     "; ".join(related_issues) if related_issues else "",
                "Severity":           severity,
                "Current Pay Type":   current_pay_type,
                "Current Salary":     current_salary,
                "Current Pay Rate":   current_pay_rate,
                "Corrected Pay Type": "",
                "Corrected Salary":   "",
                "Corrected Pay Rate": "",
                "Effective Date":     "",
                "Notes":              note,
            })

        return pd.DataFrame(rows, columns=CORRECTION_SALARY_COLUMNS).reset_index(drop=True)

    rows: list[dict] = []
    for group in _group_issue_rows(filtered):
        issue_names = _unique_ordered(group["Issue Name"].tolist())
        current_values = _unique_ordered(group["Current Value"].tolist())
        notes = _unique_ordered(group["Recommended Action"].tolist())

        row = {
            "Worker ID": _first_nonblank(group, "Worker ID"),
            "First Name": _first_nonblank(group, "First Name"),
            "Last Name": _first_nonblank(group, "Last Name"),
            "Issue Name": "; ".join(issue_names),
            "Current Value": "; ".join(current_values),
            "Corrected Value": "",
            "Effective Date": "",
            "Notes": "; ".join(notes),
        }
        for column in extra_columns:
            row[column] = _first_nonblank(group, column)
        rows.append(row)

    return pd.DataFrame(rows, columns=[*CORRECTION_BASE_COLUMNS, *extra_columns]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Input loading
# ---------------------------------------------------------------------------

def _read_and_normalize(file_path: Path, sheet_name: int | str = 0) -> tuple[pd.DataFrame, list[dict]]:
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
    df_pre = df.rename(columns=col_map)
    _, _, row_annotations = ia.analyze_duplicate_canonical_fields(df_pre, col_map)
    df_norm = ia._collapse_duplicate_columns(df_pre)
    return df_norm, row_annotations


def _load_summary(run_dir: Path) -> dict:
    p = run_dir / "internal_audit_report.json"
    if not p.exists():
        raise FileNotFoundError(f"internal_audit_report.json not found in {run_dir}")
    return json.loads(p.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Category 1: Duplicates
# - duplicate_worker_id              CRITICAL  full mask
# - duplicate_canonical_worker_id_conflict  CRITICAL  full mask
# - duplicate_email                  MEDIUM    full mask
# - duplicate_name_different_id      MEDIUM    sample from JSON (no reliable full mask)
# ---------------------------------------------------------------------------

def _build_duplicates(df: pd.DataFrame, row_annotations: list[dict], summary: dict) -> pd.DataFrame:
    rows: list[dict] = []

    # --- duplicate_worker_id + canonical conflict (CRITICAL) ---
    # Canonical conflict rows
    for idx, anns in enumerate(row_annotations):
        det = (anns or {}).get("worker_id")
        if not det or det.get("duplicate_classification") != "duplicate_conflicting_values":
            continue
        try:
            source_row = df.iloc[idx]
        except IndexError:
            continue
        rows.append(_make_row(
            source_row, idx,
            issue_name="Duplicate Worker ID - Source Column Conflict",
            severity="CRITICAL",
            why_flagged="Two source columns map to Worker ID with conflicting values.",
            current_value=_safe(det.get("duplicate_values", "")),
            fix_needed="Choose the authoritative Worker ID value and remove the conflicting column.",
        ))

    # Standard duplicate worker_id
    if "worker_id" in df.columns:
        series = df["worker_id"].astype(str).str.strip()
        nonblank = series[(series != "") & (series.str.lower() != "nan")]
        dup_idx = nonblank[nonblank.duplicated(keep=False)].index
        for orig_idx in dup_idx:
            source_row = df.loc[orig_idx]
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Duplicate Worker ID",
                severity="CRITICAL",
                why_flagged="Worker ID appears on more than one employee record.",
                current_value=_safe(source_row.get("worker_id", "")),
                fix_needed="Assign a unique Worker ID to each affected employee.",
            ))

    # --- duplicate_email (MEDIUM) full mask ---
    if "email" in df.columns:
        email_series = df["email"].astype(str).str.strip().str.lower()
        nonblank_email = email_series[(email_series != "") & (email_series != "nan")]
        dup_email_idx = nonblank_email[nonblank_email.duplicated(keep=False)].index
        for orig_idx in dup_email_idx:
            source_row = df.loc[orig_idx]
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Duplicate Email",
                severity="MEDIUM",
                why_flagged="Email address appears on more than one employee record.",
                current_value=_safe(source_row.get("email", "")),
                fix_needed="Assign a unique, valid email address to this employee.",
            ))

    # --- duplicate_name_different_id (MEDIUM) from JSON sample_rows ---
    # Full mask is not reliable (same name does not always mean duplicate person)
    for finding in (summary.get("findings_for_pdf") or summary.get("findings") or []):
        if str(finding.get("check_key", "")) != "duplicate_name_different_id":
            continue
        for sr in (finding.get("sample_rows") or []):
            first = _safe(sr.get("first_name", ""))
            last = _safe(sr.get("last_name", ""))
            if not first and not last:
                full = _safe(sr.get("name", ""))
                parts = full.split()
                first = parts[0] if parts else ""
                last = " ".join(parts[1:]) if len(parts) > 1 else ""
            wid = _safe(sr.get("worker_id", ""))
            rows.append({
                "Worker ID":    wid,
                "First Name":   first,
                "Last Name":    last,
                "Issue Name":   "Duplicate Name - Different Worker ID",
                "Severity":     "MEDIUM",
                "Reason":       "This employee name appears with different Worker IDs and needs review.",
                "Current Value": f"{first} {last}".strip() + (f" | Worker ID: {wid}" if wid else ""),
                "Recommended Action": "Confirm whether this is a duplicate person or different employees with the same name.",
                "Row Number":   _safe(sr.get("row_number", "")),
                "Department":   _safe(sr.get("department", "")),
                "Status":       _safe(sr.get("status", "")),
                "Salary":       _safe(sr.get("salary", "")),
                "Pay Rate":     _safe(sr.get("payrate", "")),
                "Hire Date":    _safe(sr.get("hire_date", "")),
                "Termination Date": _safe(sr.get("termination_date", "")),
                "Email":        _safe(sr.get("email", "")),
            })
        break  # only one finding per check_key expected

    if not rows:
        return pd.DataFrame()
    return _trim_context(pd.DataFrame(rows))


# ---------------------------------------------------------------------------
# Category 2: Salary
# - active_zero_salary       CRITICAL  full mask
# - salary_suspicious_default HIGH     sample from JSON
# - suspicious_round_salary  LOW       sample from JSON
# ---------------------------------------------------------------------------

def _build_salary(df: pd.DataFrame, summary: dict) -> pd.DataFrame:
    rows: list[dict] = []

    status_col = ia._status_column(df)
    pay_type_col = ia._pay_type_column(df) if hasattr(ia, "_pay_type_column") else ia._first_present(df, ["pay_type", "worker_type"])
    standard_hours_col = ia._standard_hours_column(df) if hasattr(ia, "_standard_hours_column") else ia._first_present(df, ["standard_hours"])
    has_salary = "salary" in df.columns
    has_payrate = "payrate" in df.columns
    statuses = df[status_col].astype(str).str.strip().str.lower() if status_col else pd.Series("", index=df.index)
    sal_vals = (
        pd.to_numeric(df["salary"], errors="coerce") if has_salary
        else pd.Series([float("nan")] * len(df), index=df.index)
    )
    pay_vals = (
        pd.to_numeric(df["payrate"], errors="coerce") if has_payrate
        else pd.Series([float("nan")] * len(df), index=df.index)
    )
    sal_blank = (
        ia._blank_mask(df["salary"]) if has_salary
        else pd.Series([True] * len(df), index=df.index)
    )
    pay_blank = (
        ia._blank_mask(df["payrate"]) if has_payrate
        else pd.Series([True] * len(df), index=df.index)
    )
    payrate_valid = ~pay_blank & pay_vals.gt(0)
    salary_valid = ~sal_blank & sal_vals.gt(0)
    comp_present = ~sal_blank | ~pay_blank
    config = ia._load_config() if hasattr(ia, "_load_config") else {}
    hourly_min = float(config.get("hourly_payrate_min", ia.DEFAULT_HOURLY_PAYRATE_MIN))
    hourly_max = float(config.get("hourly_payrate_max", ia.DEFAULT_HOURLY_PAYRATE_MAX))
    salary_min = float(config.get("salaried_salary_min", ia.DEFAULT_SALARIED_SALARY_MIN))
    salary_max = float(config.get("salaried_salary_max", ia.DEFAULT_SALARIED_SALARY_MAX))
    annualized_threshold_pct = float(config.get("annualized_comp_mismatch_pct", getattr(ia, "DEFAULT_ANNUALIZED_COMP_MISMATCH_PCT", 0.10)))

    # --- active_zero_salary (CRITICAL) full mask ---
    if status_col and (has_salary or has_payrate):
        effective = sal_vals.where(~sal_blank, pay_vals)
        comp_blank = sal_blank & pay_blank
        mask = (statuses == "active") & (comp_blank | (effective <= 0))

        for orig_idx in df.index[mask.fillna(False)]:
            source_row = df.loc[orig_idx]
            sal = _safe(source_row.get("salary", ""))
            pay = _safe(source_row.get("payrate", ""))
            if sal and pay:
                current_value = f"Salary: {sal} | Pay Rate: {pay}"
            elif sal:
                current_value = f"Salary: {sal}"
            elif pay:
                current_value = f"Pay Rate: {pay}"
            else:
                current_value = "Missing compensation value"
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Missing or Invalid Salary",
                severity="CRITICAL",
                why_flagged="Active employee has a missing, zero, or invalid salary or pay rate.",
                current_value=current_value,
                fix_needed="Enter a valid positive salary or pay rate before payroll processing.",
            ))

    # --- pay_type_missing_or_invalid ---
    if status_col and pay_type_col and (has_salary or has_payrate):
        for orig_idx in df.index[comp_present.fillna(False)]:
            source_row = df.loc[orig_idx]
            pay_class, invalid = ia._classify_pay_type(source_row.get(pay_type_col, "")) if hasattr(ia, "_classify_pay_type") else ("", False)
            pay_type_blank = ia._blank_mask(pd.Series([source_row.get(pay_type_col, "")])).iloc[0]
            if not (invalid or pay_type_blank):
                continue
            severity = "CRITICAL" if statuses.at[orig_idx] == "active" else "HIGH"
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Missing or Invalid Pay Type",
                severity=severity,
                why_flagged="Compensation is present but pay type is blank or invalid.",
                current_value=_safe(source_row.get(pay_type_col, "")) or "Missing pay type",
                fix_needed="Populate a valid pay type from the controlled allowed list before payroll or migration.",
            ))

    # --- compensation_type_mismatch ---
    if status_col and pay_type_col and (has_salary or has_payrate):
        for orig_idx in df.index[comp_present.fillna(False)]:
            source_row = df.loc[orig_idx]
            pay_class, invalid = ia._classify_pay_type(source_row.get(pay_type_col, "")) if hasattr(ia, "_classify_pay_type") else ("", False)
            if invalid or not pay_class:
                continue
            is_active = statuses.at[orig_idx] == "active"
            if pay_class == "hourly" and not payrate_valid.at[orig_idx]:
                severity = "CRITICAL" if is_active else "HIGH"
                rows.append(_make_row(
                    source_row, orig_idx,
                    issue_name="Compensation Type Mismatch",
                    severity=severity,
                    why_flagged="Hourly worker is missing a valid pay rate for the stated pay type.",
                    current_value=f"Pay Type: {_safe(source_row.get(pay_type_col, ''))} | Salary: {_safe(source_row.get('salary', ''))} | Pay Rate: {_safe(source_row.get('payrate', ''))}",
                    fix_needed="Populate a valid pay rate that matches the worker pay type before payroll.",
                ))
            elif pay_class == "salaried" and not salary_valid.at[orig_idx]:
                severity = "CRITICAL" if is_active else "HIGH"
                rows.append(_make_row(
                    source_row, orig_idx,
                    issue_name="Compensation Type Mismatch",
                    severity=severity,
                    why_flagged="Salaried worker is missing a valid salary for the stated pay type.",
                    current_value=f"Pay Type: {_safe(source_row.get(pay_type_col, ''))} | Salary: {_safe(source_row.get('salary', ''))} | Pay Rate: {_safe(source_row.get('payrate', ''))}",
                    fix_needed="Populate a valid salary that matches the worker pay type before payroll.",
                ))

    # --- comp_dual_value_conflict ---
    if status_col and pay_type_col and has_salary and has_payrate:
        dual_present = ~sal_blank & ~pay_blank
        for orig_idx in df.index[dual_present.fillna(False)]:
            source_row = df.loc[orig_idx]
            pay_class, invalid = ia._classify_pay_type(source_row.get(pay_type_col, "")) if hasattr(ia, "_classify_pay_type") else ("", False)
            if invalid or not pay_class:
                continue
            severity = "HIGH" if statuses.at[orig_idx] == "active" else "MEDIUM"
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Salary and Pay Rate Conflict",
                severity=severity,
                why_flagged="Both salary and pay rate are populated for this worker and conflict with the stated pay type.",
                current_value=f"Pay Type: {_safe(source_row.get(pay_type_col, ''))} | Salary: {_safe(source_row.get('salary', ''))} | Pay Rate: {_safe(source_row.get('payrate', ''))}",
                fix_needed="Review the worker record and keep only the compensation field that matches the intended pay type.",
            ))

    # --- missing_standard_hours_hourly ---
    if status_col and pay_type_col and standard_hours_col and has_payrate:
        standard_hours_blank = ia._blank_mask(df[standard_hours_col])
        for orig_idx in df.index:
            source_row = df.loc[orig_idx]
            pay_class, invalid = ia._classify_pay_type(source_row.get(pay_type_col, "")) if hasattr(ia, "_classify_pay_type") else ("", False)
            if invalid or pay_class != "hourly" or pay_blank.at[orig_idx] or not standard_hours_blank.at[orig_idx]:
                continue
            severity = "HIGH" if statuses.at[orig_idx] == "active" else "MEDIUM"
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Missing Standard Hours for Hourly Worker",
                severity=severity,
                why_flagged="Hourly worker has a pay rate present but standard hours are missing.",
                current_value=f"Pay Rate: {_safe(source_row.get('payrate', ''))} | Standard Hours: {_safe(source_row.get(standard_hours_col, '')) or 'Missing'}",
                fix_needed="Populate standard hours for each hourly worker before migration.",
            ))

    # --- hourly_implausible_payrate ---
    if pay_type_col and has_payrate:
        for orig_idx in df.index[~pay_blank.fillna(True)]:
            source_row = df.loc[orig_idx]
            pay_class, invalid = ia._classify_pay_type(source_row.get(pay_type_col, "")) if hasattr(ia, "_classify_pay_type") else ("", False)
            if invalid or pay_class != "hourly":
                continue
            value = pay_vals.at[orig_idx]
            if pd.isna(value):
                continue
            if value <= 0:
                rows.append(_make_row(
                    source_row, orig_idx,
                    issue_name="Implausible Hourly Pay Rate",
                    severity="CRITICAL",
                    why_flagged="Hourly worker has a zero or negative pay rate.",
                    current_value=f"Pay Rate: {_safe(source_row.get('payrate', ''))}",
                    fix_needed="Enter a valid positive pay rate before payroll.",
                ))
            elif value < hourly_min or value > hourly_max:
                rows.append(_make_row(
                    source_row, orig_idx,
                    issue_name="Implausible Hourly Pay Rate",
                    severity="HIGH",
                    why_flagged=f"Hourly worker pay rate is outside the configured review range of {hourly_min:g} to {hourly_max:g}.",
                    current_value=f"Pay Rate: {_safe(source_row.get('payrate', ''))}",
                    fix_needed=f"Review the hourly pay rate and confirm it belongs within the configured range of {hourly_min:g} to {hourly_max:g}.",
                ))

    # --- salaried_implausible_salary ---
    if pay_type_col and has_salary:
        for orig_idx in df.index[~sal_blank.fillna(True)]:
            source_row = df.loc[orig_idx]
            pay_class, invalid = ia._classify_pay_type(source_row.get(pay_type_col, "")) if hasattr(ia, "_classify_pay_type") else ("", False)
            if invalid or pay_class != "salaried":
                continue
            value = sal_vals.at[orig_idx]
            if pd.isna(value):
                continue
            if value <= 0:
                rows.append(_make_row(
                    source_row, orig_idx,
                    issue_name="Implausible Annual Salary",
                    severity="CRITICAL",
                    why_flagged="Salaried worker has a zero or negative annual salary.",
                    current_value=f"Salary: {_safe(source_row.get('salary', ''))}",
                    fix_needed="Enter a valid positive salary before payroll.",
                ))
            elif value < salary_min or value > salary_max:
                rows.append(_make_row(
                    source_row, orig_idx,
                    issue_name="Implausible Annual Salary",
                    severity="HIGH",
                    why_flagged=f"Salaried worker salary is outside the configured review range of {salary_min:g} to {salary_max:g}.",
                    current_value=f"Salary: {_safe(source_row.get('salary', ''))}",
                    fix_needed=f"Review the salary and confirm it belongs within the configured range of {salary_min:g} to {salary_max:g}.",
                ))

    # --- annualized_comp_mismatch ---
    if pay_type_col and standard_hours_col and has_salary and has_payrate:
        standard_hours_blank = ia._blank_mask(df[standard_hours_col])
        standard_hours_vals = pd.to_numeric(df[standard_hours_col], errors="coerce")
        for orig_idx in df.index:
            source_row = df.loc[orig_idx]
            pay_class, invalid = ia._classify_pay_type(source_row.get(pay_type_col, "")) if hasattr(ia, "_classify_pay_type") else ("", False)
            if invalid or not pay_class or sal_blank.at[orig_idx] or pay_blank.at[orig_idx] or standard_hours_blank.at[orig_idx]:
                continue
            salary_value = sal_vals.at[orig_idx]
            payrate_value = pay_vals.at[orig_idx]
            standard_hours_value = standard_hours_vals.at[orig_idx]
            if pd.isna(salary_value) or pd.isna(payrate_value) or pd.isna(standard_hours_value):
                continue
            if salary_value <= 0 or payrate_value <= 0 or standard_hours_value <= 0:
                continue

            annualized_pay = payrate_value * standard_hours_value * 52
            annualized_difference = abs(annualized_pay - salary_value)
            mismatch_pct = annualized_difference / max(abs(salary_value), 1.0)
            if mismatch_pct < annualized_threshold_pct:
                continue

            row = _make_row(
                source_row, orig_idx,
                issue_name="Annualized Compensation Mismatch",
                severity="HIGH",
                why_flagged=f"Salary and annualized pay rate differ by at least {annualized_threshold_pct:.0%}.",
                current_value=f"Salary: {_safe(source_row.get('salary', ''))} | Pay Rate: {_safe(source_row.get('payrate', ''))} | Standard Hours: {_safe(source_row.get(standard_hours_col, ''))}",
                fix_needed="Review salary, pay rate, and standard hours together before payroll or migration.",
            )
            row["Annualized Pay"] = f"{annualized_pay:.2f}"
            row["Annualized Difference"] = f"{annualized_difference:.2f}"
            rows.append(row)

    # --- pay_context_sanity_check ---
    leave_status_col = ia._leave_status_column(df) if hasattr(ia, "_leave_status_column") else ia._first_present(df, ["leave_status", "absence_status", "loa_status"])
    term_col = ia._first_present(df, ["termination_date", "term_date", "end_date"])
    if status_col and pay_type_col and leave_status_col and (has_salary or has_payrate):
        leave_statuses = df[leave_status_col].astype(str).str.strip().str.lower()
        active_leave_terms = {"active", "open", "working"}
        inactive_leave_terms = {"terminated", "inactive", "leave", "leave_of_absence", "loa", "suspended"}
        for orig_idx in df.index[comp_present.fillna(False)]:
            source_row = df.loc[orig_idx]
            pay_class, invalid = ia._classify_pay_type(source_row.get(pay_type_col, "")) if hasattr(ia, "_classify_pay_type") else ("", False)
            if invalid or not pay_class:
                continue
            status_value = statuses.at[orig_idx]
            leave_value = leave_statuses.at[orig_idx]
            if not leave_value:
                continue
            mismatch = (
                (status_value == "active" and leave_value in inactive_leave_terms)
                or (status_value in {"terminated", "inactive"} and leave_value in active_leave_terms)
            )
            if not mismatch:
                continue
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Payroll Context Mismatch",
                severity="HIGH",
                why_flagged="Worker status, leave status, and compensation context disagree in a way that should be reviewed.",
                current_value=(
                    f"Status: {_safe(source_row.get(status_col, ''))} | "
                    f"Leave Status: {_safe(source_row.get(leave_status_col, ''))} | "
                    f"Pay Type: {_safe(source_row.get(pay_type_col, ''))} | "
                    f"Salary: {_safe(source_row.get('salary', ''))} | "
                    f"Pay Rate: {_safe(source_row.get('payrate', ''))}"
                ),
                fix_needed="Review worker status, leave status, and compensation together before payroll or migration.",
            ))

    # --- salary_suspicious_default + suspicious_round_salary from JSON samples ---
    sample_keys = {"salary_suspicious_default", "suspicious_round_salary"}
    name_map = {
        "salary_suspicious_default": "Suspicious Salary Default",
        "suspicious_round_salary":   "Suspicious Round Salary",
    }
    sev_map = {
        "salary_suspicious_default": "HIGH",
        "suspicious_round_salary":   "LOW",
    }
    why_map = {
        "salary_suspicious_default": "Salary matches a common placeholder default value.",
        "suspicious_round_salary":   "Salary is a suspiciously round number that may be a placeholder.",
    }
    fix_map = {
        "salary_suspicious_default": "Verify the correct salary from HR records before migrating.",
        "suspicious_round_salary":   "Verify the correct salary with payroll records.",
    }

    for finding in (summary.get("findings_for_pdf") or summary.get("findings") or []):
        ck = str(finding.get("check_key", ""))
        if ck not in sample_keys:
            continue
        for sr in (finding.get("sample_rows") or []):
            wid = _safe(sr.get("worker_id", ""))
            first = _safe(sr.get("first_name", ""))
            last = _safe(sr.get("last_name", ""))
            if not first and not last:
                full = _safe(sr.get("name", ""))
                parts = full.split()
                first = parts[0] if parts else ""
                last = " ".join(parts[1:]) if len(parts) > 1 else ""
            sal = _safe(sr.get("salary", _safe(sr.get("value_found", ""))))
            rows.append({
                "Worker ID":    wid,
                "First Name":   first,
                "Last Name":    last,
                "Issue Name":   name_map[ck],
                "Severity":     sev_map[ck],
                "Reason":       why_map[ck],
                "Current Value": sal,
                "Recommended Action": fix_map[ck],
                "Row Number":   _safe(sr.get("row_number", "")),
                "Department":   _safe(sr.get("department", "")),
                "Status":       _safe(sr.get("status", "")),
                "Salary":       sal,
                "Pay Rate":     _safe(sr.get("payrate", "")),
                "Hire Date":    _safe(sr.get("hire_date", "")),
                "Termination Date": _safe(sr.get("termination_date", "")),
                "Email":        _safe(sr.get("email", "")),
            })

    if not rows:
        return pd.DataFrame()
    return _trim_context(pd.DataFrame(rows))


# ---------------------------------------------------------------------------
# Category 3: Identity
# - missing_required_identity  CRITICAL  full mask
# - phone_invalid              HIGH      digit-count heuristic (approximates audit logic)
# ---------------------------------------------------------------------------

def _build_identity(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    # --- missing_required_identity (CRITICAL) full mask ---
    id_fields = [f for f in ["worker_id", "first_name", "last_name"] if f in df.columns]
    if id_fields:
        masks = [ia._blank_mask(df[f]) for f in id_fields]
        combined = masks[0].copy()
        for m in masks[1:]:
            combined = combined | m
        labels = {"worker_id": "Worker ID", "first_name": "First Name", "last_name": "Last Name"}
        for orig_idx in df.index[combined.fillna(False)]:
            source_row = df.loc[orig_idx]
            missing = [
                labels.get(f, f) for f in id_fields
                if _safe(source_row.get(f, "")) == ""
            ]
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Missing Required Identity Fields",
                severity="CRITICAL",
                why_flagged=f"Required identity fields are missing: {', '.join(missing)}.",
                current_value="Missing: " + ", ".join(missing),
                fix_needed="Populate Worker ID, First Name, and Last Name for this employee.",
            ))

    # --- phone_invalid (HIGH) digit-count heuristic ---
    phone_col = ia._first_present(df, ["phone", "phone_number", "mobile", "cell"])
    if phone_col:
        phone_series = df[phone_col].astype(str)
        for orig_idx in df.index:
            raw = _safe(df.loc[orig_idx, phone_col])
            if not raw:
                continue
            digits = "".join(c for c in raw if c.isdigit())
            # Flag if digit count is outside plausible range (7-15 digits)
            if len(digits) < 7 or len(digits) > 15:
                source_row = df.loc[orig_idx]
                rows.append(_make_row(
                    source_row, orig_idx,
                    issue_name="Invalid Phone Number",
                    severity="HIGH",
                    why_flagged="Phone number has an implausible digit count (expected 7-15 digits).",
                    current_value=raw,
                    fix_needed="Correct the phone number from the source record or a verified employee record.",
                ))

    if not rows:
        return pd.DataFrame()
    return _trim_context(pd.DataFrame(rows))


# ---------------------------------------------------------------------------
# Category 4: Dates
# - invalid_date_logic  CRITICAL  full mask (future hire, term before hire)
# ---------------------------------------------------------------------------

def _build_dates(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    hire_col = ia._first_present(df, ["hire_date", "start_date", "date_hired"])
    term_col = ia._first_present(df, ["termination_date", "term_date", "end_date"])
    if not hire_col:
        return pd.DataFrame()

    today = pd.Timestamp.now().normalize().tz_localize(None)
    hire_dates = ia._date_series(df, hire_col)

    # Future hire dates
    future_mask = hire_dates > today
    for orig_idx in df.index[future_mask.fillna(False)]:
        source_row = df.loc[orig_idx]
        rows.append(_make_row(
            source_row, orig_idx,
            issue_name="Invalid Dates",
            severity="CRITICAL",
            why_flagged="Hire date is set in the future.",
            current_value=_safe(source_row.get(hire_col, "")),
            fix_needed="Correct the hire date to today or earlier.",
        ))

    # Termination before hire
    if term_col:
        term_dates = ia._date_series(df, term_col)
        tbh_mask = (term_dates < hire_dates) & term_dates.notna() & hire_dates.notna()
        for orig_idx in df.index[tbh_mask.fillna(False)]:
            source_row = df.loc[orig_idx]
            hire_str = _safe(source_row.get(hire_col, ""))
            term_str = _safe(source_row.get(term_col, ""))
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Invalid Dates",
                severity="CRITICAL",
                why_flagged="Termination date is earlier than hire date.",
                current_value=f"Hire Date: {hire_str} | Termination Date: {term_str}",
                fix_needed="Update the dates so the termination date is on or after the hire date.",
            ))

    if not rows:
        return pd.DataFrame()
    return _trim_context(pd.DataFrame(rows))


# ---------------------------------------------------------------------------
# Category 5: Status
# - active_with_termination_date  CRITICAL  full mask
# - status_high_pending           HIGH      full mask on "pending" status
# ---------------------------------------------------------------------------

def _build_status(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    status_col = ia._status_column(df)
    term_col   = ia._first_present(df, ["termination_date", "term_date", "end_date"])

    if not status_col:
        return pd.DataFrame()

    statuses = df[status_col].astype(str).str.strip().str.lower()

    # --- active_with_termination_date (CRITICAL) ---
    if term_col:
        term_blank = ia._blank_mask(df[term_col])
        active_term_mask = (statuses == "active") & ~term_blank
        for orig_idx in df.index[active_term_mask.fillna(False)]:
            source_row = df.loc[orig_idx]
            term_str = _safe(source_row.get(term_col, ""))
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Active Employee with Termination Date",
                severity="CRITICAL",
                why_flagged="Employee is marked Active but also has a termination date.",
                current_value=f"Status: Active | Termination Date: {term_str}",
                fix_needed="Remove the termination date or change the worker status to Terminated.",
            ))

    # --- status_high_pending (HIGH) full mask ---
    pending_mask = statuses == "pending"
    for orig_idx in df.index[pending_mask.fillna(False)]:
        source_row = df.loc[orig_idx]
        rows.append(_make_row(
            source_row, orig_idx,
            issue_name="Pending Status",
            severity="HIGH",
            why_flagged="Worker status is Pending and needs a final employment status.",
            current_value=_safe(source_row.get(status_col, "")),
            fix_needed="Update to the correct final status (Active, Inactive, or Terminated).",
        ))

    if not rows:
        return pd.DataFrame()
    return _trim_context(pd.DataFrame(rows))


# ---------------------------------------------------------------------------
# Category 6: Data Quality
# - high_blank_rate  MEDIUM  full mask per flagged field (sourced from JSON findings)
# ---------------------------------------------------------------------------

def _build_data_quality(df: pd.DataFrame, summary: dict) -> pd.DataFrame:
    rows: list[dict] = []

    for finding in (summary.get("findings_for_pdf") or summary.get("findings") or []):
        if str(finding.get("check_key", "")) != "high_blank_rate":
            continue
        field = str(finding.get("field", "")).strip()
        if not field or field not in df.columns:
            continue

        field_label = field.replace("_", " ").title()
        blank_mask = ia._blank_mask(df[field])
        for orig_idx in df.index[blank_mask.fillna(False)]:
            source_row = df.loc[orig_idx]
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name=f"Missing Data - {field_label}",
                severity="MEDIUM",
                why_flagged=f"{field_label} is blank on this record.",
                current_value=f"Blank {field_label}",
                fix_needed=f"Populate {field_label} from the source system.",
            ))

    status_col = ia._status_column(df)
    eligible_col = ia._benefits_eligible_column(df) if hasattr(ia, "_benefits_eligible_column") else ia._first_present(df, ["benefits_eligible"])
    plan_col = ia._benefit_plan_column(df) if hasattr(ia, "_benefit_plan_column") else ia._first_present(df, ["benefit_plan"])
    coverage_col = ia._coverage_level_column(df) if hasattr(ia, "_coverage_level_column") else ia._first_present(df, ["coverage_level"])
    dependent_col = ia._dependent_count_column(df) if hasattr(ia, "_dependent_count_column") else ia._first_present(df, ["dependent_count"])
    benefits_start_col = ia._benefits_start_date_column(df) if hasattr(ia, "_benefits_start_date_column") else ia._first_present(df, ["benefits_start_date"])
    benefits_end_col = ia._benefits_end_date_column(df) if hasattr(ia, "_benefits_end_date_column") else ia._first_present(df, ["benefits_end_date"])
    term_col = ia._first_present(df, ["termination_date", "term_date", "end_date"])
    statuses = df[status_col].astype(str).str.strip().str.lower() if status_col else pd.Series("", index=df.index)

    # --- benefits_enrolled_not_eligible ---
    if status_col and eligible_col and plan_col:
        for orig_idx in df.index:
            source_row = df.loc[orig_idx]
            eligibility = ia._classify_benefits_eligible(source_row.get(eligible_col, "")) if hasattr(ia, "_classify_benefits_eligible") else ""
            if eligibility != "not_eligible" or not ia._benefit_plan_present(source_row.get(plan_col, "")):
                continue
            severity = "CRITICAL" if statuses.at[orig_idx] == "active" else "HIGH"
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Enrolled in Benefits but Not Eligible",
                severity=severity,
                why_flagged="Worker is enrolled in benefits but marked as not eligible.",
                current_value=f"Benefits Eligible: {_safe(source_row.get(eligible_col, ''))} | Benefit Plan: {_safe(source_row.get(plan_col, ''))}",
                fix_needed="Remove benefit enrollment or correct eligibility status.",
            ))

    # --- benefits_eligible_not_enrolled ---
    if status_col and eligible_col and plan_col:
        for orig_idx in df.index:
            source_row = df.loc[orig_idx]
            eligibility = ia._classify_benefits_eligible(source_row.get(eligible_col, "")) if hasattr(ia, "_classify_benefits_eligible") else ""
            if eligibility != "eligible" or ia._benefit_plan_present(source_row.get(plan_col, "")):
                continue
            severity = "HIGH" if statuses.at[orig_idx] == "active" else "MEDIUM"
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Eligible for Benefits but Not Enrolled",
                severity=severity,
                why_flagged="Worker is eligible for benefits but has no enrollment.",
                current_value=f"Benefits Eligible: {_safe(source_row.get(eligible_col, ''))} | Benefit Plan: Missing",
                fix_needed="Confirm if employee should be enrolled or update eligibility.",
            ))

    # --- invalid_coverage_level ---
    if status_col and plan_col and coverage_col:
        for orig_idx in df.index:
            source_row = df.loc[orig_idx]
            if not ia._benefit_plan_present(source_row.get(plan_col, "")):
                continue
            coverage_class = ia._classify_coverage_level(source_row.get(coverage_col, "")) if hasattr(ia, "_classify_coverage_level") else ""
            if coverage_class not in {"", "invalid"}:
                continue
            severity = "HIGH" if statuses.at[orig_idx] == "active" else "MEDIUM"
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Invalid Coverage Level",
                severity=severity,
                why_flagged="Coverage level is missing or not recognized.",
                current_value=f"Benefit Plan: {_safe(source_row.get(plan_col, ''))} | Coverage Level: {_safe(source_row.get(coverage_col, '')) or 'Missing'}",
                fix_needed="Assign a valid coverage level for the selected plan.",
            ))

    # --- dependents_without_coverage ---
    if plan_col and coverage_col and dependent_col:
        dependent_vals = pd.to_numeric(df[dependent_col], errors="coerce").fillna(0)
        for orig_idx in df.index:
            source_row = df.loc[orig_idx]
            if not ia._benefit_plan_present(source_row.get(plan_col, "")) or dependent_vals.at[orig_idx] <= 0:
                continue
            coverage_class = ia._classify_coverage_level(source_row.get(coverage_col, "")) if hasattr(ia, "_classify_coverage_level") else ""
            if coverage_class != "employee_only":
                continue
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Dependents Without Proper Coverage",
                severity="HIGH",
                why_flagged="Dependents are present but coverage does not include them.",
                current_value=(
                    f"Benefit Plan: {_safe(source_row.get(plan_col, ''))} | "
                    f"Coverage Level: {_safe(source_row.get(coverage_col, ''))} | "
                    f"Dependent Count: {_safe(source_row.get(dependent_col, ''))}"
                ),
                fix_needed="Update coverage level to include dependents if applicable.",
            ))

    # --- benefits_after_termination ---
    if plan_col and term_col:
        term_blank = ia._blank_mask(df[term_col])
        for orig_idx in df.index[~term_blank.fillna(True)]:
            source_row = df.loc[orig_idx]
            if not ia._benefit_plan_present(source_row.get(plan_col, "")):
                continue
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Benefits Active After Termination",
                severity="CRITICAL",
                why_flagged="Benefits appear active after employee termination.",
                current_value=f"Benefit Plan: {_safe(source_row.get(plan_col, ''))} | Termination Date: {_safe(source_row.get(term_col, ''))}",
                fix_needed="End benefits coverage as of termination date.",
            ))

    # --- benefits_start_before_hire ---
    if status_col and benefits_start_col:
        hire_col = ia._first_present(df, ["hire_date", "start_date", "date_hired"])
        if hire_col:
            hire_dates = ia._date_series(df, hire_col)
            benefits_start_dates = ia._date_series(df, benefits_start_col)
            for orig_idx in df.index:
                source_row = df.loc[orig_idx]
                hire_date = hire_dates.at[orig_idx]
                benefits_start_date = benefits_start_dates.at[orig_idx]
                if pd.isna(hire_date) or pd.isna(benefits_start_date) or benefits_start_date >= hire_date:
                    continue
                severity = "HIGH" if statuses.at[orig_idx] == "active" else "MEDIUM"
                rows.append(_make_row(
                    source_row, orig_idx,
                    issue_name="Benefits Start Date Before Hire Date",
                    severity=severity,
                    why_flagged="Benefits start date is earlier than the hire date.",
                    current_value=f"Hire Date: {_safe(source_row.get(hire_col, ''))} | Benefits Start Date: {_safe(source_row.get(benefits_start_col, ''))}",
                    fix_needed="Correct the benefits start date so it does not begin before the hire date.",
                ))

    # --- benefits_after_termination_window ---
    if plan_col and term_col:
        grace_days = int((ia._load_config()).get("benefits_termination_grace_days", 30))
        termination_dates = ia._date_series(df, term_col)
        benefits_end_dates = ia._date_series(df, benefits_end_col) if benefits_end_col else pd.Series([pd.NaT] * len(df), index=df.index)
        for orig_idx in df.index:
            source_row = df.loc[orig_idx]
            if not ia._benefit_plan_present(source_row.get(plan_col, "")):
                continue
            termination_date = termination_dates.at[orig_idx]
            if pd.isna(termination_date):
                continue
            benefits_end_date = benefits_end_dates.at[orig_idx]
            if pd.isna(benefits_end_date):
                should_flag = True
            else:
                should_flag = benefits_end_date > termination_date + pd.Timedelta(days=grace_days)
            if not should_flag:
                continue
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Benefits Active Too Long After Termination",
                severity="HIGH",
                why_flagged=f"Benefits appear to remain active beyond the allowed post-termination window of {grace_days} days, or no benefits end date is present.",
                current_value=f"Termination Date: {_safe(source_row.get(term_col, ''))} | Benefits End Date: {_safe(source_row.get(benefits_end_col, '')) or 'Missing'}",
                fix_needed="Review benefits end timing and close coverage within the allowed post-termination window when appropriate.",
            ))

    # --- benefits_waiting_period_violation ---
    if status_col and benefits_start_col:
        hire_col = ia._first_present(df, ["hire_date", "start_date", "date_hired"])
        if hire_col:
            waiting_days = int((ia._load_config()).get("benefits_waiting_period_days", 30))
            hire_dates = ia._date_series(df, hire_col)
            benefits_start_dates = ia._date_series(df, benefits_start_col)
            for orig_idx in df.index:
                source_row = df.loc[orig_idx]
                hire_date = hire_dates.at[orig_idx]
                benefits_start_date = benefits_start_dates.at[orig_idx]
                if pd.isna(hire_date) or pd.isna(benefits_start_date):
                    continue
                if benefits_start_date < hire_date:
                    continue
                if benefits_start_date >= hire_date + pd.Timedelta(days=waiting_days):
                    continue
                severity = "HIGH" if statuses.at[orig_idx] == "active" else "MEDIUM"
                rows.append(_make_row(
                    source_row, orig_idx,
                    issue_name="Benefits Waiting Period Violation",
                    severity=severity,
                    why_flagged=f"Benefits start date begins earlier than the configured waiting period of {waiting_days} days.",
                    current_value=f"Hire Date: {_safe(source_row.get(hire_col, ''))} | Benefits Start Date: {_safe(source_row.get(benefits_start_col, ''))}",
                    fix_needed=f"Review the hire date and benefits start date and confirm the waiting period should be at least {waiting_days} days.",
                ))

    # --- employment_type_eligibility_conflict ---
    worker_type_col = ia._first_present(df, ["worker_type", "employment_type", "pay_type"])
    if status_col and worker_type_col and eligible_col:
        for orig_idx in df.index:
            source_row = df.loc[orig_idx]
            worker_type_class = ia._classify_employment_type_for_benefits(source_row.get(worker_type_col, "")) if hasattr(ia, "_classify_employment_type_for_benefits") else ""
            if not worker_type_class:
                continue
            eligibility = ia._classify_benefits_eligible(source_row.get(eligible_col, "")) if hasattr(ia, "_classify_benefits_eligible") else ""
            has_plan = ia._benefit_plan_present(source_row.get(plan_col, "")) if plan_col else False
            if not (
                (worker_type_class == "part_time" and eligibility == "eligible")
                or (worker_type_class in {"contractor", "temporary"} and (eligibility == "eligible" or has_plan))
            ):
                continue
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Employment Type Benefits Eligibility Conflict",
                severity="HIGH",
                why_flagged="Worker type and benefits eligibility context disagree in a way that should be reviewed.",
                current_value=(
                    f"Worker Type: {_safe(source_row.get(worker_type_col, ''))} | "
                    f"Benefits Eligible: {_safe(source_row.get(eligible_col, ''))} | "
                    f"Benefit Plan: {_safe(source_row.get(plan_col, ''))}"
                ),
                fix_needed="Review worker type, eligibility status, and benefit enrollment together before migration.",
            ))

    # --- multiple_active_benefit_plans ---
    worker_id_col = ia._first_present(df, ["worker_id", "employee_id"])
    if worker_id_col and plan_col:
        def _plan_tokens(value: object) -> list[str]:
            raw = _safe(value)
            if not raw:
                return []
            normalized = raw.replace("|", ";").replace(",", ";")
            tokens = [str(token).strip().lower() for token in normalized.split(";")]
            return [token for token in tokens if token and token not in {"none", "no plan", "not enrolled", "waived", "waive", "declined", "decline"}]

        flagged_ids: set[str] = set()
        for orig_idx in df.index:
            worker_id = _safe(df.loc[orig_idx].get(worker_id_col, ""))
            if worker_id and len(set(_plan_tokens(df.loc[orig_idx].get(plan_col, "")))) > 1:
                flagged_ids.add(worker_id)
        grouped_plans: dict[str, set[str]] = {}
        for orig_idx in df.index:
            worker_id = _safe(df.loc[orig_idx].get(worker_id_col, ""))
            if not worker_id:
                continue
            grouped_plans.setdefault(worker_id, set()).update(_plan_tokens(df.loc[orig_idx].get(plan_col, "")))
        for worker_id, plans in grouped_plans.items():
            if len(plans) > 1:
                flagged_ids.add(worker_id)
        for orig_idx in df.index:
            source_row = df.loc[orig_idx]
            if _safe(source_row.get(worker_id_col, "")) not in flagged_ids:
                continue
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Multiple Active Benefit Plans",
                severity="HIGH",
                why_flagged="Worker appears to have multiple active benefit plan values that should be reviewed.",
                current_value=f"Benefit Plan: {_safe(source_row.get(plan_col, ''))}",
                fix_needed="Review the active benefit plan setup and keep only the correct plan values for each worker.",
            ))

    # --- coverage_vs_dependent_sanity ---
    if coverage_col and dependent_col:
        dependent_vals = pd.to_numeric(df[dependent_col], errors="coerce").fillna(0)
        for orig_idx in df.index:
            source_row = df.loc[orig_idx]
            coverage_class = ia._classify_coverage_level(source_row.get(coverage_col, "")) if hasattr(ia, "_classify_coverage_level") else ""
            dependent_count = dependent_vals.at[orig_idx]
            if not (
                (coverage_class == "includes_dependents" and dependent_count <= 0)
                or (coverage_class == "employee_only" and dependent_count > 0)
            ):
                continue
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Coverage Level vs Dependent Count Mismatch",
                severity="HIGH",
                why_flagged="Coverage level and dependent count do not align in a way that suggests the benefit setup should be reviewed.",
                current_value=f"Coverage Level: {_safe(source_row.get(coverage_col, ''))} | Dependent Count: {_safe(source_row.get(dependent_col, ''))}",
                fix_needed="Review dependent count and coverage level together and confirm the selected coverage tier is correct.",
            ))

    if not rows:
        return pd.DataFrame()
    return _trim_context(pd.DataFrame(rows))


# ---------------------------------------------------------------------------
# Zip packaging
# ---------------------------------------------------------------------------

def _create_zip(run_dir: Path, workbook_path: Path, csv_files: list[Path]) -> Path:
    """
    Build a slim, human-readable ZIP for internal audit outputs.

    Included (in order, only if non-empty):
      1. Internal Audit Report.pdf
      2. Internal Audit Workbook.xlsx
      3. Internal Audit Data.csv
      4. Data Completeness.csv
      5. Salary and Status Summary.csv
      6. Suspicious Values.csv
      7. Duplicate Records.csv
      8. Salary Issues to Fix.csv
      9. Status Issues to Fix.csv
      10. Data Quality Issues to Fix.csv
      11. Clean Employee Data.csv
    """
    # Map actual filenames → human-facing names inside the zip
    candidates: list[tuple[Path, str]] = []
    def _add(rel_name: str, display_name: str) -> None:
        p = run_dir / rel_name
        if p.exists() and p.is_file() and p.stat().st_size > 0:
            candidates.append((p, display_name))

    _add("internal_audit_report.pdf",        "01 - Internal Audit Report.pdf")
    _add(workbook_path.name,                 "02 - Internal Audit Workbook.xlsx")
    _add("internal_audit_data.csv",          "03 - Internal Audit Data.csv")
    _add("internal_audit_completeness.csv",  "04 - Data Completeness.csv")
    _add("internal_audit_distributions.csv", "05 - Salary and Status Summary.csv")
    _add("internal_audit_suspicious.csv",    "06 - Suspicious Values.csv")
    _add("internal_audit_duplicates.csv",    "07 - Duplicate Records.csv")
    _add("fix_salary_full.csv",              "08 - Salary Issues to Fix.csv")
    _add("fix_status_full.csv",              "09 - Status Issues to Fix.csv")
    _add("fix_data_quality_full.csv",        "10 - Data Quality Issues to Fix.csv")
    _add("clean_data_ready_for_review.csv",  "11 - Clean Employee Data.csv")

    zip_path = run_dir / "internal_audit_outputs.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for src, arc in candidates:
            zf.write(src, arcname=arc)
    return zip_path


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Build internal audit CSV exports and zip package.")
    parser.add_argument("--file",       required=True, help="Path to source employee data file")
    parser.add_argument("--run-dir",    required=True, help="Path to the audit run directory")
    parser.add_argument("--workbook",   required=True, help="Path to internal_audit_workbook.xlsx")
    parser.add_argument("--sheet-name", default="0",   help="Sheet name or index in source file")
    args = parser.parse_args()

    file_path    = Path(args.file)
    run_dir      = Path(args.run_dir)
    workbook_path = Path(args.workbook)

    sheet_name: int | str = args.sheet_name
    try:
        sheet_name = int(sheet_name)
    except ValueError:
        pass

    print("[exports] Loading source data and audit summary...")
    df, row_annotations = _read_and_normalize(file_path, sheet_name=sheet_name)
    summary = _load_summary(run_dir)

    total_rows = len(df)
    print(f"[exports] Source rows loaded: {total_rows:,}")

    # --- Build each category ---
    categories = {
        "fix_duplicates_full.csv":   lambda: _build_duplicates(df, row_annotations, summary),
        "fix_salary_full.csv":       lambda: _build_salary(df, summary),
        "fix_identity_full.csv":     lambda: _build_identity(df),
        "fix_dates_full.csv":        lambda: _build_dates(df),
        "fix_status_full.csv":       lambda: _build_status(df),
        "fix_data_quality_full.csv": lambda: _build_data_quality(df, summary),
    }

    generated_csvs: list[Path] = []
    coverage_report: list[str] = []
    category_frames: dict[str, pd.DataFrame] = {}

    for filename, builder in categories.items():
        out_path = run_dir / filename
        try:
            frame = builder()
            category_frames[filename] = frame if frame is not None else pd.DataFrame()
            if frame is not None and not frame.empty:
                count = _write_csv(frame, out_path)
                generated_csvs.append(out_path)
                coverage_report.append(f"  {filename}: {count:,} rows written")
                print(f"[exports] {filename}: {count:,} rows")
            else:
                coverage_report.append(f"  {filename}: 0 rows - no issues found (file omitted from zip)")
                print(f"[exports] {filename}: 0 rows (omitted)")
        except Exception as exc:
            category_frames[filename] = pd.DataFrame()
            coverage_report.append(f"  {filename}: ERROR - {exc}")
            print(f"[exports] ERROR building {filename}: {exc}", file=sys.stderr)

    clean_review_frames = {
        "clean_data_ready_for_review.csv": None,
        "review_required_rows.csv": None,
    }
    try:
        clean_frame, review_frame = _build_clean_review_exports(df, category_frames)
        clean_review_frames["clean_data_ready_for_review.csv"] = clean_frame
        clean_review_frames["review_required_rows.csv"] = review_frame
    except Exception as exc:
        coverage_report.append(f"  clean review exports: ERROR - {exc}")
        print(f"[exports] ERROR building clean review exports: {exc}", file=sys.stderr)

    for filename, frame in clean_review_frames.items():
        if frame is None:
            continue
        out_path = run_dir / filename
        count = _write_required_csv(frame, out_path)
        if out_path.exists() and out_path.stat().st_size > 0:
            generated_csvs.append(out_path)
        coverage_report.append(f"  {filename}: {count:,} rows written")
        print(f"[exports] {filename}: {count:,} rows")

    correction_sources = {
        "correction_salary.csv": category_frames.get("fix_salary_full.csv", pd.DataFrame()),
        "correction_status.csv": category_frames.get("fix_status_full.csv", pd.DataFrame()),
        "correction_dates.csv": category_frames.get("fix_dates_full.csv", pd.DataFrame()),
    }

    for filename, source_frame in correction_sources.items():
        out_path = run_dir / filename
        template = _build_correction_template(source_frame, filename)
        if template.empty:
            _remove_if_exists(out_path)
            coverage_report.append(f"  {filename}: 0 rows - no correction template generated")
            print(f"[exports] {filename}: 0 rows (omitted)")
            continue

        count = _write_csv(template, out_path)
        generated_csvs.append(out_path)
        coverage_report.append(f"  {filename}: {count:,} rows written")
        print(f"[exports] {filename}: {count:,} rows")

    # --- Create zip ---
    print("[exports] Creating internal_audit_outputs.zip...")
    zip_path = _create_zip(run_dir, workbook_path, generated_csvs)
    zip_size_kb = zip_path.stat().st_size // 1024 if zip_path.exists() else 0

    # --- Coverage report ---
    print("\n[exports] ============================================================")
    print("[exports] COVERAGE REPORT")
    print("[exports] ============================================================")
    for line in coverage_report:
        print(f"[exports]{line}")
    print(f"[exports]  internal_audit_outputs.zip: {zip_size_kb:,} KB")
    print("[exports]")
    print("[exports] REVIEW REQUIRED ROW POLICY:")
    print("[exports]   Always include severities: CRITICAL, HIGH")
    print("[exports]   Include MEDIUM only for: Duplicate Name - Different Worker ID, Missing Data - <field>")
    print("[exports]   Exclude MEDIUM broad or informational issues from review rows: Duplicate Email, Age Data Issues, Combined Field, No Terminated Employees")
    print("[exports]   Exclude LOW findings from review rows by default")
    print("[exports]")
    print("[exports] FULL ROW COVERAGE:")
    print("[exports]   duplicate_worker_id            - FULL (mask on worker_id column)")
    print("[exports]   duplicate_canonical_conflict   - FULL (row_annotations from canonical analysis)")
    print("[exports]   duplicate_email                - FULL (mask on email column)")
    print("[exports]   duplicate_name_different_id    - SAMPLE (from JSON findings; no reliable full mask)")
    print("[exports]   active_zero_salary             - FULL (mask on status + salary/pay rate)")
    print("[exports]   salary_suspicious_default      - SAMPLE (from JSON findings)")
    print("[exports]   suspicious_round_salary        - SAMPLE (from JSON findings)")
    print("[exports]   missing_required_identity      - FULL (blank mask on worker_id, first_name, last_name)")
    print("[exports]   phone_invalid                  - FULL (digit-count heuristic; approximates audit)")
    print("[exports]   invalid_date_logic             - FULL (future hire + term-before-hire mask)")
    print("[exports]   active_with_termination_date   - FULL (mask on status + termination_date)")
    print("[exports]   status_high_pending            - FULL (mask on status == pending)")
    print("[exports]   high_blank_rate                - FULL (blank mask per field from JSON findings)")
    print("[exports]")
    print("[exports] NOT EXPORTABLE (statistical / dataset-level):")
    print("[exports]   age_uniformity                 - statistical pattern, no per-row mask")
    print("[exports]   combined_field                 - column-level issue, not per-row")
    print("[exports]   pay_equity_flag                - department-level statistical")
    print("[exports]   salary_outlier                 - department-level statistical")
    print("[exports]   ghost_employee_indicator       - multi-factor heuristic")
    print("[exports]   status_no_terminated           - dataset-level flag")
    print("[exports] ============================================================")
    print(f"[exports] Done. Zip at: {zip_path}")


if __name__ == "__main__":
    main()
