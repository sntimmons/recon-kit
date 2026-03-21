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
    "Why Flagged",
    "Current Value",
    "Fix Needed",
    "Row Number",
]

# Optional context columns appended when data is present
CONTEXT_COLUMNS = [
    "Department",
    "Status",
    "Pay Type",
    "Salary",
    "Payrate",
    "Standard Hours",
    "Hire Date",
    "Termination Date",
    "Email",
]

REVIEW_METADATA_COLUMNS = [
    "review_status",
    "issue_count",
    "issue_names",
    "highest_severity",
    "requires_manual_review",
    "recommended_next_step",
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
    "Department",
    "Status",
    "Pay Type",
    "Salary",
    "Payrate",
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

CORRECTION_FILE_CONFIG = {
    "correction_salary.csv": {
        "issue_names": {
            "Missing or Invalid Salary",
            "Missing or Invalid Pay Type",
            "Compensation Type Mismatch",
        },
        "extra_columns": ["Pay Type", "Salary", "Payrate", "Standard Hours"],
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
        "Department":        _get(row, "department"),
        "Status":            _get(row, "worker_status", "status"),
        "Pay Type":          _get(row, "pay_type", "worker_type"),
        "Salary":            _get(row, "salary"),
        "Payrate":           _get(row, "payrate"),
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
        "Why Flagged":  why_flagged,
        "Current Value": current_value,
        "Fix Needed":   fix_needed,
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
        "worker_status": "Status",
        "status": "Status",
        "pay_type": "Pay Type",
        "worker_type": "Pay Type",
        "salary": "Salary",
        "payrate": "Payrate",
        "standard_hours": "Standard Hours",
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
        issue_names = _unique_ordered(group["Issue Name"].tolist()) if "Issue Name" in group.columns else []
        fix_needed = _unique_ordered(group["Fix Needed"].tolist()) if "Fix Needed" in group.columns else []
        severities = _unique_ordered(group["Severity"].tolist()) if "Severity" in group.columns else []
        highest = min(severities, key=lambda sev: SEVERITY_RANK.get(_safe(sev).upper(), 99)) if severities else ""
        highest = _safe(highest).upper()
        review_status = "BLOCKED" if highest in {"CRITICAL", "HIGH"} else "REVIEW"

        summaries.append({
            "__row_number": row_number,
            "review_status": review_status,
            "issue_count": len(issue_names),
            "issue_names": "; ".join(issue_names),
            "highest_severity": highest,
            "requires_manual_review": "Yes",
            "recommended_next_step": _recommended_next_step(fix_needed),
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

    clean["review_status"] = clean["review_status"].fillna("CLEAN")
    clean["issue_count"] = clean["issue_count"].fillna(0).astype(int)
    clean["issue_names"] = clean["issue_names"].fillna("")
    clean["highest_severity"] = clean["highest_severity"].fillna("")
    clean["requires_manual_review"] = clean["requires_manual_review"].fillna("No")
    clean["recommended_next_step"] = clean["recommended_next_step"].fillna("No action needed")

    source_columns = [c for c in clean.columns if c not in {"__row_number", *REVIEW_METADATA_COLUMNS}]
    clean = clean[[
        *source_columns,
        *REVIEW_METADATA_COLUMNS,
    ]].reset_index(drop=True)

    review_required_mask = clean.apply(
        lambda row: _should_include_in_review_required(
            row.get("highest_severity", ""),
            row.get("issue_names", ""),
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
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return len(df)


def _write_required_csv(df: pd.DataFrame, path: Path) -> int:
    """Always write required CSV outputs, even if only the header is present."""
    df.to_csv(path, index=False, encoding="utf-8-sig")
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


def _build_correction_template(issue_frame: pd.DataFrame, filename: str) -> pd.DataFrame:
    config = CORRECTION_FILE_CONFIG[filename]
    allowed_issue_names = config["issue_names"]
    extra_columns = config["extra_columns"]

    if issue_frame.empty:
        return pd.DataFrame(columns=[*CORRECTION_BASE_COLUMNS, *extra_columns])

    filtered = issue_frame[issue_frame["Issue Name"].isin(allowed_issue_names)].copy()
    if filtered.empty:
        return pd.DataFrame(columns=[*CORRECTION_BASE_COLUMNS, *extra_columns])

    rows: list[dict] = []
    for group in _group_issue_rows(filtered):
        issue_names = _unique_ordered(group["Issue Name"].tolist())
        current_values = _unique_ordered(group["Current Value"].tolist())
        notes = _unique_ordered(group["Fix Needed"].tolist())

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
                "Why Flagged":  "This employee name appears with different Worker IDs and needs review.",
                "Current Value": f"{first} {last}".strip() + (f" | Worker ID: {wid}" if wid else ""),
                "Fix Needed":   "Confirm whether this is a duplicate person or different employees with the same name.",
                "Row Number":   _safe(sr.get("row_number", "")),
                "Department":   _safe(sr.get("department", "")),
                "Status":       _safe(sr.get("status", "")),
                "Salary":       _safe(sr.get("salary", "")),
                "Payrate":      _safe(sr.get("payrate", "")),
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
                current_value = f"Salary: {sal} | Payrate: {pay}"
            elif sal:
                current_value = f"Salary: {sal}"
            elif pay:
                current_value = f"Payrate: {pay}"
            else:
                current_value = "Missing compensation value"
            rows.append(_make_row(
                source_row, orig_idx,
                issue_name="Missing or Invalid Salary",
                severity="CRITICAL",
                why_flagged="Active employee has a missing, zero, or invalid salary/payrate.",
                current_value=current_value,
                fix_needed="Enter a valid positive salary or payrate before payroll processing.",
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
                    why_flagged="Hourly worker is missing a valid payrate for the stated pay type.",
                    current_value=f"Pay Type: {_safe(source_row.get(pay_type_col, ''))} | Salary: {_safe(source_row.get('salary', ''))} | Payrate: {_safe(source_row.get('payrate', ''))}",
                    fix_needed="Populate a valid payrate that matches the worker pay type before payroll.",
                ))
            elif pay_class == "salaried" and not salary_valid.at[orig_idx]:
                severity = "CRITICAL" if is_active else "HIGH"
                rows.append(_make_row(
                    source_row, orig_idx,
                    issue_name="Compensation Type Mismatch",
                    severity=severity,
                    why_flagged="Salaried worker is missing a valid salary for the stated pay type.",
                    current_value=f"Pay Type: {_safe(source_row.get(pay_type_col, ''))} | Salary: {_safe(source_row.get('salary', ''))} | Payrate: {_safe(source_row.get('payrate', ''))}",
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
                why_flagged="Both salary and payrate are populated for this worker and conflict with the stated pay type.",
                current_value=f"Pay Type: {_safe(source_row.get(pay_type_col, ''))} | Salary: {_safe(source_row.get('salary', ''))} | Payrate: {_safe(source_row.get('payrate', ''))}",
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
                why_flagged="Hourly worker has payrate present but standard hours are missing.",
                current_value=f"Payrate: {_safe(source_row.get('payrate', ''))} | Standard Hours: {_safe(source_row.get(standard_hours_col, '')) or 'Missing'}",
                fix_needed="Populate standard hours for each hourly worker before migration.",
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
                "Why Flagged":  why_map[ck],
                "Current Value": sal,
                "Fix Needed":   fix_map[ck],
                "Row Number":   _safe(sr.get("row_number", "")),
                "Department":   _safe(sr.get("department", "")),
                "Status":       _safe(sr.get("status", "")),
                "Salary":       sal,
                "Payrate":      _safe(sr.get("payrate", "")),
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

    if not rows:
        return pd.DataFrame()
    return _trim_context(pd.DataFrame(rows))


# ---------------------------------------------------------------------------
# Zip packaging
# ---------------------------------------------------------------------------

def _create_zip(run_dir: Path, workbook_path: Path, csv_files: list[Path]) -> Path:
    zip_path = run_dir / "internal_audit_outputs.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if workbook_path.exists() and workbook_path.stat().st_size > 0:
            zf.write(workbook_path, workbook_path.name)
        for csv_path in csv_files:
            if csv_path.exists() and csv_path.stat().st_size > 0:
                zf.write(csv_path, csv_path.name)
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
    print("[exports]   active_zero_salary             - FULL (mask on status + salary/payrate)")
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
