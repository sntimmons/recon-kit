"""
build_internal_audit_exports.py - Full-row CSV exports and zip packaging for internal audit runs.

Produces:
  fix_duplicates_full.csv   - All duplicate Worker ID / Email / Name rows
  fix_salary_full.csv       - All active employees with missing or invalid compensation
  fix_identity_full.csv     - All employees missing required identity fields or with invalid phones
  fix_dates_full.csv        - All employees with invalid date logic
  fix_status_full.csv       - All employees with conflicting status or high pending rate
  fix_data_quality_full.csv - All employees with blank required fields (high_blank_rate check)
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
    "Salary",
    "Payrate",
    "Hire Date",
    "Termination Date",
    "Email",
]


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
        "Salary":            _get(row, "salary"),
        "Payrate":           _get(row, "payrate"),
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


def _write_csv(df: pd.DataFrame, path: Path) -> int:
    """Write dataframe to CSV, return row count (excluding header)."""
    if df.empty:
        return 0
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return len(df)


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

    # --- active_zero_salary (CRITICAL) full mask ---
    status_col = ia._status_column(df)
    has_salary = "salary" in df.columns
    has_payrate = "payrate" in df.columns

    if status_col and (has_salary or has_payrate):
        statuses = df[status_col].astype(str).str.strip().str.lower()
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

    for filename, builder in categories.items():
        out_path = run_dir / filename
        try:
            frame = builder()
            if frame is not None and not frame.empty:
                count = _write_csv(frame, out_path)
                generated_csvs.append(out_path)
                coverage_report.append(f"  {filename}: {count:,} rows written")
                print(f"[exports] {filename}: {count:,} rows")
            else:
                coverage_report.append(f"  {filename}: 0 rows - no issues found (file omitted from zip)")
                print(f"[exports] {filename}: 0 rows (omitted)")
        except Exception as exc:
            coverage_report.append(f"  {filename}: ERROR - {exc}")
            print(f"[exports] ERROR building {filename}: {exc}", file=sys.stderr)

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
