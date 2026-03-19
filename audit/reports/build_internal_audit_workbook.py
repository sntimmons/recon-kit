"""
build_internal_audit_workbook.py - Excel workbook export for internal audit runs.

Creates:
  internal_audit_workbook.xlsx

Sheets (in order):

  User-facing:
  - Quick_Summary        ← Start here: plain-English overview of every issue type + count
  - Fix_List             ← Primary working sheet: one row per employee needing action
  - Fix_List_Salary      ← Auto-generated when salary issues exceed scale thresholds
  - Fix_List_Duplicates  ← Auto-generated when duplicate issues exceed scale thresholds
  - Fix_List_Data_Quality← Auto-generated when date/identity/status issues exceed thresholds

  Technical / secondary (kept for reference):
  - Audit_Summary
  - Findings_Index
  - Duplicate_Groups
  - Compensation_Detail
  - Technical_Summary
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

try:
    from openpyxl import load_workbook
    from openpyxl.styles import Font, PatternFill
except ImportError:
    print("[error] openpyxl not installed", file=sys.stderr)
    sys.exit(2)

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from audit import internal_audit as ia


# ── Scale constants ────────────────────────────────────────────────────────────
EXPAND_COUNT_THRESHOLD = 2000
EXPAND_PCT_THRESHOLD = 0.10
DUPLICATE_SAMPLE_LIMIT = 5
COMPENSATION_DETAIL_CAP = 500

# Fix_List splitting thresholds
FIX_LIST_ROW_CAP = 20_000       # total Fix_List rows before triggering category split
CATEGORY_ROW_CAP = 10_000       # rows per issue category before splitting that category
EXCEL_SHEET_ROW_CAP = 50_000    # rows per sheet before numbered splitting (e.g. _1, _2)

# ── Styling constants ──────────────────────────────────────────────────────────
HEADER_FILL = "D9EAF7"

PRIORITY_ROW_FILLS: dict[str, str] = {
    "High":   "FFCCCC",  # soft red
    "Medium": "FFE5CC",  # soft orange
    "Low":    "FFFFCC",  # soft yellow
}
MISSING_CELL_FILL = "FFD9B3"    # stronger orange for cells with missing values

# ── Translation maps (technical → human-readable) ─────────────────────────────
SEVERITY_TO_PRIORITY: dict[str, str] = {
    "CRITICAL": "High",
    "HIGH":     "High",
    "MEDIUM":   "Medium",
    "LOW":      "Low",
}

# Maps check_key to a plain-English issue type shown to HR users
CHECK_KEY_TO_ISSUE_TYPE: dict[str, str] = {
    "duplicate_worker_id":                    "Duplicate Worker ID",
    "duplicate_canonical_worker_id_conflict": "Duplicate Worker ID",
    "active_zero_salary":                     "Missing or Invalid Salary",
    "invalid_date_logic":                     "Invalid Date",
    "active_with_termination_date":           "Status Conflict",
    "missing_required_identity":              "Missing Employee Information",
}

# Maps issue type to its category sub-sheet when splitting is triggered
ISSUE_TYPE_TO_CATEGORY_SHEET: dict[str, str] = {
    "Duplicate Worker ID":           "Fix_List_Duplicates",
    "Missing or Invalid Salary":     "Fix_List_Salary",
    "Invalid Date":                  "Fix_List_Data_Quality",
    "Status Conflict":               "Fix_List_Data_Quality",
    "Missing Employee Information":  "Fix_List_Data_Quality",
}

# Plain-English "what to do" guidance per issue type (for Quick_Summary)
ISSUE_TYPE_GUIDANCE: dict[str, str] = {
    "Duplicate Worker ID":           "Assign a unique Worker ID to each employee.",
    "Missing or Invalid Salary":     "Enter a valid salary or payrate for all active employees.",
    "Invalid Date":                  "Correct hire or termination dates so the timeline is valid.",
    "Status Conflict":               "Remove the termination date or change the status to Terminated.",
    "Missing Employee Information":  "Fill in the required name and ID fields for each employee.",
}

# Names of all Fix_List-style sheets (used to apply priority highlighting)
FIX_LIST_SHEET_PREFIXES = ("Fix_List",)


# ── Helpers ────────────────────────────────────────────────────────────────────

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


def _humanize(col: str) -> str:
    return str(col).replace("_", " ").strip().title()


def _format_for_workbook(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.rename(columns={col: _humanize(col) for col in df.columns})


def _sheet_ready(df: pd.DataFrame, note: str) -> pd.DataFrame:
    if not df.empty:
        return df
    return pd.DataFrame([{"Note": note}])


def _concat_non_empty(frames: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def _should_expand(finding: dict, total_rows: int) -> bool:
    count = int(finding.get("row_count", finding.get("count", 0)) or 0)
    pct = float(finding.get("percent_of_population", finding.get("pct", 0.0)) or 0.0) / 100.0
    return not (count > EXPAND_COUNT_THRESHOLD or (total_rows > 0 and pct > EXPAND_PCT_THRESHOLD))


def _annotated_rows(
    df: pd.DataFrame,
    mask: pd.Series,
    *,
    check_name: str,
    severity: str,
    reason: str,
    suggested_action: str,
) -> pd.DataFrame:
    mask_series = mask if hasattr(mask, "fillna") else pd.Series(mask, index=df.index)
    subset = df.loc[mask_series.fillna(False)].copy()
    if subset.empty:
        return subset
    subset.insert(0, "suggested_action", suggested_action)
    subset.insert(0, "reason", reason)
    subset.insert(0, "severity", severity)
    subset.insert(0, "check_name", check_name)
    subset.insert(0, "row_number", subset.index + 2)
    return subset.reset_index(drop=True)


def _safe_str(val: object) -> str:
    """Convert a value to a clean string, returning empty string for null/nan."""
    if val is None:
        return ""
    s = str(val).strip()
    if s.lower() in ("nan", "nat", "none", "<na>"):
        return ""
    return s


# ── Quick_Summary sheet ────────────────────────────────────────────────────────

def _quick_summary_sheet(summary: dict) -> pd.DataFrame:
    """
    Plain-English overview for HR managers.
    No technical language, no severity codes.
    Columns: Issue Type | Count | What To Do
    """
    findings = summary.get("findings_for_pdf") or summary.get("findings") or []
    findings_map = {str(f.get("check_key", "")): f for f in findings}

    issue_counts: dict[str, int] = {}

    # Aggregate CRITICAL check counts into human-readable issue types
    for check_key, issue_type in CHECK_KEY_TO_ISSUE_TYPE.items():
        finding = findings_map.get(check_key)
        if not finding:
            continue
        count = int(finding.get("count", finding.get("row_count", 0)) or 0)
        if count > 0:
            issue_counts[issue_type] = issue_counts.get(issue_type, 0) + count

    # Fold any other HIGH findings into a generic bucket so nothing disappears
    mapped_keys = set(CHECK_KEY_TO_ISSUE_TYPE)
    for finding in findings:
        key = str(finding.get("check_key", ""))
        sev = str(finding.get("severity", "")).upper()
        if key not in mapped_keys and sev in ("HIGH",):
            count = int(finding.get("count", finding.get("row_count", 0)) or 0)
            if count > 0:
                issue_counts["Other Data Issues"] = issue_counts.get("Other Data Issues", 0) + count

    if not issue_counts:
        return pd.DataFrame([{
            "Issue Type":  "No issues found",
            "Count":       0,
            "What To Do":  "This dataset passed all checks. No action required.",
        }])

    rows = []
    for issue_type, count in issue_counts.items():
        rows.append({
            "Issue Type": issue_type,
            "Count":      count,
            "What To Do": ISSUE_TYPE_GUIDANCE.get(issue_type, "Review and correct these records."),
        })

    # Sort highest count first so the biggest problems appear at the top
    return pd.DataFrame(rows).sort_values("Count", ascending=False).reset_index(drop=True)


# ── Fix_List sheet ─────────────────────────────────────────────────────────────

FIX_LIST_COLUMNS = [
    "worker_id",
    "first_name",
    "last_name",
    "issue_type",
    "issue_description",
    "current_value",
    "expected_fix",
    "priority",
    "row_number",
]


def _fix_list_rows(
    df: pd.DataFrame,
    summary: dict,
    row_annotations: list[dict],
) -> pd.DataFrame:
    """
    Build a unified Fix_List covering ALL CRITICAL checks plus expandable HIGH findings.

    Each row = one employee with one issue.
    - Worker ID is the primary identifier.
    - Issue Type uses plain English (no CRITICAL/HIGH/MEDIUM).
    - Current Value shows the actual problematic value.
    - Expected Fix gives a clear instruction.
    - Priority maps severity to High / Medium / Low.
    - CRITICAL checks are always fully expanded (no _should_expand guard).
    - HIGH findings respect _should_expand to avoid noise from dataset-wide issues.
    """
    findings = summary.get("findings_for_pdf") or summary.get("findings") or []
    findings_map = {str(f.get("check_key", "")): f for f in findings}
    total_rows = int(summary.get("total_rows", len(df)) or len(df))

    frames: list[pd.DataFrame] = []

    def _build_frame(
        source_df: pd.DataFrame,
        mask: pd.Series,
        issue_type: str,
        description: str,
        current_value_fn,   # callable(row_series) → str
        expected_fix: str,
        priority: str = "High",
    ) -> pd.DataFrame:
        mask_s = (mask if hasattr(mask, "fillna") else pd.Series(mask, index=source_df.index)).fillna(False)
        subset = source_df.loc[mask_s]
        if subset.empty:
            return pd.DataFrame()

        rows = []
        for orig_idx, row in subset.iterrows():
            rows.append({
                "worker_id":         _safe_str(row.get("worker_id", "")),
                "first_name":        _safe_str(row.get("first_name", "")),
                "last_name":         _safe_str(row.get("last_name", "")),
                "issue_type":        issue_type,
                "issue_description": description,
                "current_value":     current_value_fn(row),
                "expected_fix":      expected_fix,
                "priority":          priority,
                "row_number":        int(orig_idx) + 2,
            })
        return pd.DataFrame(rows, columns=FIX_LIST_COLUMNS)

    # ── 1. Duplicate canonical worker_id conflicts (from alias normalization) ──
    conflict_finding = findings_map.get("duplicate_canonical_worker_id_conflict", {})
    if conflict_finding:
        conflict_rows = []
        for idx, annotations in enumerate(row_annotations):
            details = (annotations or {}).get("worker_id")
            if not details or details.get("duplicate_classification") != "duplicate_conflicting_values":
                continue
            try:
                row = df.iloc[idx]
            except IndexError:
                continue
            conflict_rows.append({
                "worker_id":         _safe_str(row.get("worker_id", "")),
                "first_name":        _safe_str(row.get("first_name", "")),
                "last_name":         _safe_str(row.get("last_name", "")),
                "issue_type":        "Duplicate Worker ID",
                "issue_description": (
                    "Multiple source columns map to Worker ID and they contain conflicting values. "
                    "Only one authoritative Worker ID value is permitted."
                ),
                "current_value":     _safe_str(details.get("duplicate_values", "")),
                "expected_fix":      "Resolve to a single authoritative Worker ID and remove the duplicate source column.",
                "priority":          "High",
                "row_number":        idx + 2,
            })
        if conflict_rows:
            frames.append(pd.DataFrame(conflict_rows, columns=FIX_LIST_COLUMNS))

    # ── 2. Duplicate worker_id ────────────────────────────────────────────────
    if "worker_id" in df.columns:
        series = df["worker_id"].astype(str).str.strip()
        nonblank = series[(series != "") & (series.str.lower() != "nan")]
        dup_mask = df.index.isin(nonblank[nonblank.duplicated(keep=False)].index)
        if dup_mask.any():
            frames.append(_build_frame(
                df, dup_mask,
                issue_type="Duplicate Worker ID",
                description=(
                    "Multiple employees share the same Worker ID. "
                    "Each employee must have a unique ID."
                ),
                current_value_fn=lambda r: _safe_str(r.get("worker_id", "")),
                expected_fix="Assign a unique Worker ID to each employee.",
            ))

    # ── 3. Active employees with zero / missing salary or payrate ─────────────
    status_col = ia._status_column(df)
    has_salary = "salary" in df.columns
    has_payrate = "payrate" in df.columns
    if status_col and (has_salary or has_payrate):
        statuses = df[status_col].astype(str).str.strip().str.lower()
        salary_vals = (
            pd.to_numeric(df["salary"], errors="coerce") if has_salary
            else pd.Series([float("nan")] * len(df), index=df.index)
        )
        payrate_vals = (
            pd.to_numeric(df["payrate"], errors="coerce") if has_payrate
            else pd.Series([float("nan")] * len(df), index=df.index)
        )
        sal_blank = ia._blank_mask(df["salary"]) if has_salary else pd.Series([True] * len(df), index=df.index)
        pay_blank = ia._blank_mask(df["payrate"]) if has_payrate else pd.Series([True] * len(df), index=df.index)
        effective = salary_vals.where(~sal_blank, payrate_vals)
        comp_blank = sal_blank & pay_blank
        comp_mask = (statuses == "active") & (comp_blank | (effective <= 0))

        def _salary_current_value(row: pd.Series) -> str:
            sal = _safe_str(row.get("salary", ""))
            pay = _safe_str(row.get("payrate", ""))
            if not sal and not pay:
                return "Missing"
            if sal:
                return f"Salary: {sal}"
            return f"Payrate: {pay}"

        if comp_mask.any():
            frames.append(_build_frame(
                df, comp_mask,
                issue_type="Missing or Invalid Salary",
                description=(
                    "This active employee has no valid salary or payrate on record. "
                    "A zero or missing pay value will block payroll processing."
                ),
                current_value_fn=_salary_current_value,
                expected_fix="Enter a valid positive salary or payrate.",
            ))

    # ── 4. Invalid date logic ─────────────────────────────────────────────────
    hire_col = ia._first_present(df, ["hire_date", "start_date", "date_hired"])
    term_col  = ia._first_present(df, ["termination_date", "term_date", "end_date"])

    if hire_col:
        today = pd.Timestamp.now().normalize().tz_localize(None)
        hire_dates = ia._date_series(df, hire_col)

        future_hire_mask = hire_dates > today
        if future_hire_mask.any():
            frames.append(_build_frame(
                df, future_hire_mask,
                issue_type="Invalid Date",
                description=(
                    "The hire date is set in the future, which is not valid for an existing employee."
                ),
                current_value_fn=lambda r: _safe_str(r.get(hire_col, "")),
                expected_fix="Correct the hire date to today or earlier.",
            ))

        if term_col:
            term_dates = ia._date_series(df, term_col)
            term_before_hire_mask = (term_dates < hire_dates) & term_dates.notna() & hire_dates.notna()
            if term_before_hire_mask.any():
                frames.append(_build_frame(
                    df, term_before_hire_mask,
                    issue_type="Invalid Date",
                    description=(
                        "The termination date is earlier than the hire date. "
                        "An employee cannot be terminated before they were hired."
                    ),
                    current_value_fn=lambda r: (
                        f"Hired: {_safe_str(r.get(hire_col, ''))}  |  "
                        f"Terminated: {_safe_str(r.get(term_col, ''))}"
                    ),
                    expected_fix="Ensure the termination date is on or after the hire date.",
                ))

    # ── 5. Active employees with a termination date ───────────────────────────
    if status_col and term_col:
        statuses_raw = df[status_col].astype(str).str.strip().str.lower()
        term_blank = ia._blank_mask(df[term_col])
        active_term_mask = (statuses_raw == "active") & ~term_blank
        if active_term_mask.any():
            frames.append(_build_frame(
                df, active_term_mask,
                issue_type="Status Conflict",
                description=(
                    "This employee is marked Active but also has a termination date, "
                    "which is contradictory."
                ),
                current_value_fn=lambda r: (
                    f"Status: Active  |  Termination Date: {_safe_str(r.get(term_col, ''))}"
                ),
                expected_fix="Remove the termination date, or change status to Terminated.",
            ))

    # ── 6. Missing required identity fields ───────────────────────────────────
    required_fields = [f for f in ["worker_id", "first_name", "last_name"] if f in df.columns]
    if required_fields:
        missing_masks = [ia._blank_mask(df[f]) for f in required_fields]
        identity_mask = missing_masks[0].copy()
        for m in missing_masks[1:]:
            identity_mask = identity_mask | m

        if identity_mask.any():
            _readable = {"worker_id": "Worker ID", "first_name": "First Name", "last_name": "Last Name"}

            def _identity_current_value(row: pd.Series) -> str:
                missing = [
                    _readable.get(f, f)
                    for f in required_fields
                    if _safe_str(row.get(f, "")) == ""
                ]
                return "Missing: " + ", ".join(missing) if missing else "—"

            frames.append(_build_frame(
                df, identity_mask,
                issue_type="Missing Employee Information",
                description=(
                    "One or more required identity fields (Worker ID, First Name, Last Name) "
                    "are blank. These must be populated before the record can be used."
                ),
                current_value_fn=_identity_current_value,
                expected_fix="Provide Worker ID, First Name, and Last Name for every employee.",
            ))

    # ── 7. Expandable HIGH findings (not already covered by CRITICAL checks) ──
    critical_check_keys = set(CHECK_KEY_TO_ISSUE_TYPE)
    for finding in findings:
        check_key = str(finding.get("check_key", ""))
        sev = str(finding.get("severity", "")).upper()
        if check_key in critical_check_keys or sev != "HIGH":
            continue
        if not _should_expand(finding, total_rows):
            continue
        # Generic HIGH row expansion — best-effort using check_name
        check_name = finding.get("check_name", check_key)
        desc = finding.get("description", check_name)
        action = finding.get("recommended_action", "Review and correct these records.")
        # We can't derive per-row detail without knowing the field, so skip row-level here.
        # These appear in Findings_Index with counts. Row-level requires per-check logic.

    return _concat_non_empty(frames)


# ── Fix_List scaling ───────────────────────────────────────────────────────────

def _apply_fix_list_scaling(fix_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Returns { sheet_name: DataFrame } for all Fix_List sheets.

    If total rows ≤ FIX_LIST_ROW_CAP AND no category exceeds CATEGORY_ROW_CAP:
      → Single "Fix_List" sheet containing everything.

    If thresholds are exceeded:
      → Fix_List is capped to High-priority rows (up to FIX_LIST_ROW_CAP) with a note.
      → Full detail routed to Fix_List_Salary, Fix_List_Duplicates, Fix_List_Data_Quality.
      → Any category sheet exceeding EXCEL_SHEET_ROW_CAP is further split into _1, _2, …
    """
    if fix_df.empty:
        return {"Fix_List": pd.DataFrame([{
            "Note": "No actionable findings for this audit run.",
        }])}

    total_rows = len(fix_df)
    category_counts = (
        fix_df["issue_type"].value_counts()
        if "issue_type" in fix_df.columns
        else pd.Series(dtype=int)
    )

    needs_split = (
        total_rows > FIX_LIST_ROW_CAP
        or (not category_counts.empty and (category_counts > CATEGORY_ROW_CAP).any())
    )

    if not needs_split:
        return {"Fix_List": fix_df.copy()}

    result: dict[str, pd.DataFrame] = {}

    # Trimmed Fix_List: High priority only, capped at FIX_LIST_ROW_CAP
    high_df = (
        fix_df[fix_df["priority"] == "High"].copy()
        if "priority" in fix_df.columns
        else fix_df.copy()
    )
    truncated = len(high_df) > FIX_LIST_ROW_CAP
    trimmed = high_df.head(FIX_LIST_ROW_CAP).copy()
    if truncated or len(fix_df) > FIX_LIST_ROW_CAP:
        note_cols = {col: None for col in trimmed.columns}
        note_cols["issue_type"] = "ℹ Note"
        note_cols["issue_description"] = (
            f"This sheet shows the first {FIX_LIST_ROW_CAP:,} high-priority rows only. "
            "See Fix_List_Salary, Fix_List_Duplicates, and Fix_List_Data_Quality for the full detail."
        )
        trimmed = pd.concat([pd.DataFrame([note_cols]), trimmed], ignore_index=True)
    result["Fix_List"] = trimmed

    # Build category sub-sheets
    category_frames: dict[str, list[pd.DataFrame]] = {}
    if "issue_type" in fix_df.columns:
        for issue_type, sheet_name in ISSUE_TYPE_TO_CATEGORY_SHEET.items():
            cat_df = fix_df[fix_df["issue_type"] == issue_type]
            if not cat_df.empty:
                category_frames.setdefault(sheet_name, []).append(cat_df)

    for sheet_name, frames_list in category_frames.items():
        combined = pd.concat(frames_list, ignore_index=True)
        if len(combined) <= EXCEL_SHEET_ROW_CAP:
            result[sheet_name] = combined
        else:
            chunk_num = 1
            for start in range(0, len(combined), EXCEL_SHEET_ROW_CAP):
                chunk = combined.iloc[start : start + EXCEL_SHEET_ROW_CAP].copy()
                result[f"{sheet_name}_{chunk_num}"] = chunk
                chunk_num += 1

    return result


# ── Technical_Summary sheet ────────────────────────────────────────────────────

def _technical_summary_sheet(summary: dict) -> pd.DataFrame:
    """System-level metadata kept separate from user-facing sheets."""
    rows: list[dict] = [
        {"Section": "Gate",     "Metric": "Gate Status",   "Value": summary.get("gate_status", "")},
        {"Section": "Gate",     "Metric": "Gate Message",  "Value": summary.get("gate_message", "")},
        {"Section": "Gate",     "Metric": "Override Gate", "Value": summary.get("override_gate", False)},
        {"Section": "Overview", "Metric": "Source File",   "Value": summary.get("source_filename", "")},
        {"Section": "Overview", "Metric": "Total Rows",    "Value": summary.get("total_rows", 0)},
        {"Section": "Overview", "Metric": "Completeness",  "Value": summary.get("overall_completeness", 0)},
    ]
    for sev, count in (summary.get("severity_counts") or {}).items():
        rows.append({"Section": "Severity Counts", "Metric": sev, "Value": count})
    for item in summary.get("check_counts") or []:
        rows.append({
            "Section": "Check Detail",
            "Metric":  f"{item.get('check_name')} ({item.get('severity')})",
            "Value":   item.get("count", 0),
        })
    return pd.DataFrame(rows)


# ── Existing secondary sheets ──────────────────────────────────────────────────

def _findings_index_df(summary: dict) -> pd.DataFrame:
    rows: list[dict] = []
    findings = summary.get("findings_for_pdf") or summary.get("findings") or []
    for finding in findings:
        rows.append({
            "check_name":            finding.get("check_name", ""),
            "severity":              finding.get("severity", ""),
            "count":                 int(finding.get("row_count", finding.get("count", 0)) or 0),
            "percent_of_population": float(finding.get("percent_of_population", finding.get("pct", 0.0)) or 0.0),
            "description":           finding.get("description", ""),
            "recommended_action":    finding.get("recommended_action", ""),
        })
    if not rows:
        return pd.DataFrame()
    sev_rank = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    df = pd.DataFrame(rows)
    df["_rank"] = df["severity"].map(lambda x: sev_rank.get(str(x).upper(), 9))
    df = df.sort_values(["_rank", "check_name"]).drop(columns=["_rank"]).reset_index(drop=True)
    return df


def _duplicate_groups_sheet(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    rows: list[dict] = []
    base_columns = list(df.columns)
    for column in config["duplicate_check_fields"]:
        if column not in df.columns:
            continue
        series = df[column].astype(str).str.strip()
        nonblank = series[(series != "") & (series.str.lower() != "nan")]
        dupes = nonblank[nonblank.duplicated(keep=False)]
        if dupes.empty:
            continue
        dup_df = df.loc[dupes.index].copy()
        dup_df["_duplicate_value"] = series.loc[dupes.index].values
        for dup_value, group in dup_df.groupby("_duplicate_value", sort=True):
            rows.append({
                "row_type":        "GROUP_HEADER",
                "duplicate_field": column,
                "duplicate_value": dup_value,
                "group_row_count": int(len(group)),
                "check_name":      f"Duplicate {column}",
                "severity":        "CRITICAL" if column in {"worker_id", "last4_ssn"} else "MEDIUM",
                "reason":          f"Rows share the same {column}.",
                "suggested_action": (
                    f"Review the group and resolve duplicate {column} values before downstream use."
                ),
            })
            for idx, (_, src_row) in enumerate(group.head(DUPLICATE_SAMPLE_LIMIT).iterrows(), start=1):
                item = {
                    "row_type":        "SAMPLE_ROW",
                    "duplicate_field": column,
                    "duplicate_value": dup_value,
                    "group_row_count": int(len(group)),
                    "sample_number":   idx,
                    "check_name":      f"Duplicate {column}",
                    "severity":        "CRITICAL" if column in {"worker_id", "last4_ssn"} else "MEDIUM",
                    "reason":          f"Representative row for duplicate {column} group.",
                    "suggested_action": f"Review the grouped duplicate {column} records together.",
                }
                item.update(src_row.to_dict())
                rows.append(item)
    if not rows:
        return pd.DataFrame()
    ordered_cols = [
        "row_type", "duplicate_field", "duplicate_value", "group_row_count", "sample_number",
        "check_name", "severity", "reason", "suggested_action",
    ] + [col for col in base_columns if col not in {"_duplicate_value"}]
    return pd.DataFrame(rows).reindex(columns=ordered_cols)


def _compensation_detail_sheet(df: pd.DataFrame) -> pd.DataFrame:
    status_col = ia._status_column(df)
    has_salary  = "salary" in df.columns
    has_payrate = "payrate" in df.columns
    if not status_col or (not has_salary and not has_payrate):
        return pd.DataFrame()

    statuses = df[status_col].astype(str).str.strip().str.lower()
    salary_values  = pd.to_numeric(df["salary"], errors="coerce")  if has_salary  else pd.Series([float("nan")] * len(df), index=df.index)
    payrate_values = pd.to_numeric(df["payrate"], errors="coerce") if has_payrate else pd.Series([float("nan")] * len(df), index=df.index)
    sal_blank  = ia._blank_mask(df["salary"])  if has_salary  else pd.Series([True] * len(df), index=df.index)
    pay_blank  = ia._blank_mask(df["payrate"]) if has_payrate else pd.Series([True] * len(df), index=df.index)
    effective  = salary_values.where(~sal_blank, payrate_values)
    comp_blank = sal_blank & pay_blank
    mask = (statuses == "active") & (comp_blank | (effective <= 0))

    detail = _annotated_rows(
        df, mask,
        check_name="Active employees with <= $0 or missing salary/payrate",
        severity="CRITICAL",
        reason="Active employee has missing, zero, or negative salary/payrate.",
        suggested_action="Verify and correct salary or payrate before payroll or production use.",
    )
    if detail.empty:
        return detail
    truncated = len(detail) > COMPENSATION_DETAIL_CAP
    detail = detail.head(COMPENSATION_DETAIL_CAP).copy()
    if truncated:
        note_row = {col: None for col in detail.columns}
        note_row["check_name"] = "Truncation Note"
        note_row["reason"] = f"Compensation detail truncated to {COMPENSATION_DETAIL_CAP} rows."
        note_row["suggested_action"] = (
            "Use the audit CSV outputs for the full row set if more detail is required."
        )
        detail = pd.concat([pd.DataFrame([note_row]), detail], ignore_index=True)
    return detail


def _audit_summary_sheet(summary: dict) -> pd.DataFrame:
    rows: list[dict] = [
        {"Section": "Overview", "Metric": "Source Filename",       "Value": summary.get("source_filename", "")},
        {"Section": "Overview", "Metric": "Gate Status",           "Value": summary.get("gate_status", "")},
        {"Section": "Overview", "Metric": "Gate Message",          "Value": summary.get("gate_message", "")},
        {"Section": "Overview", "Metric": "Total Rows",            "Value": summary.get("total_rows", 0)},
        {"Section": "Overview", "Metric": "Overall Completeness",  "Value": summary.get("overall_completeness", 0)},
    ]
    for severity, count in (summary.get("severity_counts") or {}).items():
        rows.append({"Section": "Severity Counts", "Metric": severity, "Value": count})
    for item in summary.get("check_counts") or []:
        rows.append({
            "Section": "Check Counts",
            "Metric":  f"{item.get('check_name')} ({item.get('severity')})",
            "Value":   item.get("count", 0),
        })
    return pd.DataFrame(rows)


# ── Sheet assembly ─────────────────────────────────────────────────────────────

def _build_sheets(
    file_path: Path, sheet_name: int | str, run_dir: Path
) -> dict[str, pd.DataFrame]:
    summary = _load_summary(run_dir)
    config = ia._load_config()
    df, _, row_annotations = _read_and_normalize(file_path, sheet_name=sheet_name)

    # ── Fix_List (raw, snake_case) ─────────────────────────────────────────────
    raw_fix_df = _fix_list_rows(df, summary, row_annotations)
    fix_sheets = _apply_fix_list_scaling(raw_fix_df)

    # Humanize Fix_List-style sheets
    formatted_fix_sheets = {
        name: _format_for_workbook(frame)
        for name, frame in fix_sheets.items()
    }

    # ── Sheet order ────────────────────────────────────────────────────────────
    sheets: dict[str, pd.DataFrame] = {}

    # 1. Quick_Summary — first tab users see
    sheets["Quick_Summary"] = _quick_summary_sheet(summary)

    # 2. Fix_List (and any split sheets)
    sheets.update(formatted_fix_sheets)

    # 3. Secondary / reference sheets (kept, not primary workflow)
    sheets["Audit_Summary"] = _audit_summary_sheet(summary)
    sheets["Findings_Index"] = _sheet_ready(
        _format_for_workbook(_findings_index_df(summary)),
        "No findings were recorded for this audit run.",
    )
    sheets["Duplicate_Groups"] = _sheet_ready(
        _format_for_workbook(_duplicate_groups_sheet(df, config)),
        "No duplicate groups were found for this audit run.",
    )
    sheets["Compensation_Detail"] = _sheet_ready(
        _format_for_workbook(_compensation_detail_sheet(df)),
        "No actionable compensation rows were found for this audit run.",
    )

    # 4. Technical_Summary — last tab
    sheets["Technical_Summary"] = _technical_summary_sheet(summary)

    return sheets


# ── Excel formatting pass ──────────────────────────────────────────────────────

def _is_fix_list_sheet(name: str) -> bool:
    return name.startswith("Fix_List")


def _autosize_and_style(out_path: Path, fix_list_sheet_names: set[str]) -> None:
    wb = load_workbook(out_path)

    priority_fills = {
        p: PatternFill(fill_type="solid", fgColor=color)
        for p, color in PRIORITY_ROW_FILLS.items()
    }
    missing_fill = PatternFill(fill_type="solid", fgColor=MISSING_CELL_FILL)
    header_fill  = PatternFill(fill_type="solid", fgColor=HEADER_FILL)

    for ws in wb.worksheets:
        # ── Header row ────────────────────────────────────────────────────────
        if ws.max_row >= 1:
            for cell in ws[1]:
                cell.fill = header_fill
                cell.font = Font(bold=True)

        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        # ── Column auto-width ─────────────────────────────────────────────────
        for col_cells in ws.columns:
            letter = col_cells[0].column_letter
            length = 0
            for cell in col_cells[:200]:
                try:
                    length = max(length, len(str(cell.value or "")))
                except Exception:
                    continue
            ws.column_dimensions[letter].width = min(max(length + 2, 12), 45)

        # ── Priority row highlighting (Fix_List sheets only) ──────────────────
        if ws.title not in fix_list_sheet_names:
            continue

        # Locate Priority and Current Value columns by header
        priority_idx: int | None = None
        current_value_idx: int | None = None
        for cell in ws[1]:
            header = str(cell.value or "").strip()
            if header == "Priority":
                priority_idx = cell.column - 1   # 0-based
            elif header == "Current Value":
                current_value_idx = cell.column - 1

        for row_cells in ws.iter_rows(min_row=2):
            priority_val = ""
            if priority_idx is not None and priority_idx < len(row_cells):
                priority_val = str(row_cells[priority_idx].value or "").strip()

            row_fill = priority_fills.get(priority_val)
            if row_fill:
                for cell in row_cells:
                    cell.fill = row_fill

            # Highlight missing/blank Current Value cells
            if current_value_idx is not None and current_value_idx < len(row_cells):
                cv_cell = row_cells[current_value_idx]
                cv_text = str(cv_cell.value or "").strip().lower()
                if cv_text in ("", "none", "nan") or cv_text.startswith("missing"):
                    cv_cell.fill = missing_fill

    wb.save(out_path)


# ── Entry points ───────────────────────────────────────────────────────────────

def build_workbook(
    file_path: Path, run_dir: Path, out_path: Path, sheet_name: int | str = 0
) -> None:
    sheets = _build_sheets(file_path, sheet_name, run_dir)
    fix_list_names = {name for name in sheets if _is_fix_list_sheet(name)}

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for key, frame in sheets.items():
            frame.to_excel(writer, sheet_name=key, index=False)

    _autosize_and_style(out_path, fix_list_names)
    print(f"[build_internal_audit_workbook] wrote: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build internal audit workbook")
    parser.add_argument("--file",       required=True, help="Source file used for the audit")
    parser.add_argument("--run-dir",    required=True, help="Directory containing internal audit outputs")
    parser.add_argument("--out",        required=True, help="Output workbook path")
    parser.add_argument("--sheet-name", default="0",   help="Excel sheet index or name")
    args = parser.parse_args()
    sheet_name: int | str = (
        int(args.sheet_name)
        if str(args.sheet_name).lstrip("-").isdigit()
        else args.sheet_name
    )
    build_workbook(Path(args.file), Path(args.run_dir), Path(args.out), sheet_name=sheet_name)


if __name__ == "__main__":
    main()
