"""
build_internal_audit_workbook.py - Excel workbook export for internal audit runs.

Creates:
  internal_audit_workbook.xlsx

Sheets:
  - Audit_Summary
  - Action_Required
  - Findings_Index
  - Duplicate_Groups
  - Compensation_Detail
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

try:
    from openpyxl import load_workbook
except ImportError:
    print("[error] openpyxl not installed", file=sys.stderr)
    sys.exit(2)

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from audit import internal_audit as ia


EXPAND_COUNT_THRESHOLD = 2000
EXPAND_PCT_THRESHOLD = 0.10
DUPLICATE_SAMPLE_LIMIT = 5
COMPENSATION_DETAIL_CAP = 500
HEADER_FILL = "D9EAF7"


def _read_and_normalize(file_path: Path, sheet_name: int | str = 0) -> tuple[pd.DataFrame, dict[str, str], list[dict]]:
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
    non_empty = [frame for frame in frames if not frame.empty]
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


def _findings_index_df(summary: dict) -> pd.DataFrame:
    rows: list[dict] = []
    findings = summary.get("findings_for_pdf") or summary.get("findings") or []
    for finding in findings:
        rows.append(
            {
                "check_name": finding.get("check_name", ""),
                "severity": finding.get("severity", ""),
                "count": int(finding.get("row_count", finding.get("count", 0)) or 0),
                "percent_of_population": float(finding.get("percent_of_population", finding.get("pct", 0.0)) or 0.0),
                "description": finding.get("description", ""),
                "recommended_action": finding.get("recommended_action", ""),
            }
        )
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
            rows.append(
                {
                    "row_type": "GROUP_HEADER",
                    "duplicate_field": column,
                    "duplicate_value": dup_value,
                    "group_row_count": int(len(group)),
                    "check_name": f"Duplicate {column}",
                    "severity": "CRITICAL" if column in {"worker_id", "last4_ssn"} else "MEDIUM",
                    "reason": f"Rows share the same {column}.",
                    "suggested_action": f"Review the group and resolve duplicate {column} values before downstream use.",
                }
            )
            for idx, (_, src_row) in enumerate(group.head(DUPLICATE_SAMPLE_LIMIT).iterrows(), start=1):
                item = {
                    "row_type": "SAMPLE_ROW",
                    "duplicate_field": column,
                    "duplicate_value": dup_value,
                    "group_row_count": int(len(group)),
                    "sample_number": idx,
                    "check_name": f"Duplicate {column}",
                    "severity": "CRITICAL" if column in {"worker_id", "last4_ssn"} else "MEDIUM",
                    "reason": f"Representative row for duplicate {column} group.",
                    "suggested_action": f"Review the grouped duplicate {column} records together.",
                }
                item.update(src_row.to_dict())
                rows.append(item)
    if not rows:
        return pd.DataFrame()
    ordered_cols = [
        "row_type",
        "duplicate_field",
        "duplicate_value",
        "group_row_count",
        "sample_number",
        "check_name",
        "severity",
        "reason",
        "suggested_action",
    ] + [col for col in base_columns if col not in {"_duplicate_value"}]
    return pd.DataFrame(rows).reindex(columns=ordered_cols)


def _compensation_detail_sheet(df: pd.DataFrame) -> pd.DataFrame:
    status_col = ia._status_column(df)
    has_salary = "salary" in df.columns
    has_payrate = "payrate" in df.columns
    if not status_col or (not has_salary and not has_payrate):
        return pd.DataFrame()

    statuses = df[status_col].astype(str).str.strip().str.lower()
    salary_values = pd.to_numeric(df["salary"], errors="coerce") if has_salary else pd.Series([float("nan")] * len(df), index=df.index)
    payrate_values = pd.to_numeric(df["payrate"], errors="coerce") if has_payrate else pd.Series([float("nan")] * len(df), index=df.index)
    salary_blank = ia._blank_mask(df["salary"]) if has_salary else pd.Series([True] * len(df), index=df.index)
    payrate_blank = ia._blank_mask(df["payrate"]) if has_payrate else pd.Series([True] * len(df), index=df.index)
    effective_values = salary_values.where(~salary_blank, payrate_values)
    comp_blank = salary_blank & payrate_blank
    mask = (statuses == "active") & (comp_blank | (effective_values <= 0))

    detail = _annotated_rows(
        df,
        mask,
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
        note_row["suggested_action"] = "Use the audit CSV outputs for the full row set if more detail is required."
        detail = pd.concat([pd.DataFrame([note_row]), detail], ignore_index=True)
    return detail


def _action_required_sheet(df: pd.DataFrame, summary: dict, row_annotations: list[dict]) -> pd.DataFrame:
    findings_map = {
        str(f.get("check_key")): f for f in (summary.get("findings_for_pdf") or summary.get("findings") or [])
    }
    total_rows = int(summary.get("total_rows", len(df)) or len(df))
    frames: list[pd.DataFrame] = []

    conflict_rows: list[dict] = []
    conflict_finding = findings_map.get("duplicate_canonical_worker_id_conflict", {})
    if _should_expand(conflict_finding, total_rows):
        for idx, annotations in enumerate(row_annotations):
            details = (annotations or {}).get("worker_id")
            if not details or details.get("duplicate_classification") != "duplicate_conflicting_values":
                continue
            base = df.iloc[idx].to_dict()
            row = {
                "row_number": idx + 2,
                "check_name": "Duplicate canonical worker_id with conflicting values",
                "severity": "CRITICAL",
                "reason": "Conflicting values were found for canonical field 'worker_id'.",
                "suggested_action": "Resolve the conflicting worker_id values and keep only the authoritative source column.",
                "duplicate_source_columns": details.get("duplicate_source_columns", ""),
                "duplicate_values": details.get("duplicate_values", ""),
                "retained_value_reason": details.get("retained_value_reason", ""),
            }
            row.update(base)
            conflict_rows.append(row)
        frames.append(pd.DataFrame(conflict_rows))

    dup_finding = findings_map.get("duplicate_worker_id", {})
    if _should_expand(dup_finding, total_rows) and "worker_id" in df.columns:
        series = df["worker_id"].astype(str).str.strip()
        nonblank = series[(series != "") & (series.str.lower() != "nan")]
        dupes = nonblank[nonblank.duplicated(keep=False)]
        dup_rows = _annotated_rows(
            df,
            df.index.isin(dupes.index),
            check_name="Duplicate worker_id",
            severity="CRITICAL",
            reason="Multiple rows share the same worker_id.",
            suggested_action="Resolve duplicate worker_id values before migration.",
        )
        frames.append(dup_rows)

    comp_rows = _compensation_detail_sheet(df)
    if not comp_rows.empty:
        # Keep payroll-critical rows in both sheets intentionally: Action_Required is the
        # auditor's main working tab, while Compensation_Detail remains a focused view.
        frames.append(comp_rows[comp_rows.get("check_name") != "Truncation Note"].copy())

    invalid_date_finding = findings_map.get("invalid_date_logic", {})
    if _should_expand(invalid_date_finding, total_rows):
        hire_col = ia._first_present(df, ["hire_date", "start_date", "date_hired"])
        term_col = ia._first_present(df, ["termination_date", "term_date", "end_date"])
        if hire_col:
            today = pd.Timestamp.now().normalize().tz_localize(None)
            hire_dates = ia._date_series(df, hire_col)
            future_hire_mask = hire_dates > today
            future_hire_rows = _annotated_rows(
                df,
                future_hire_mask,
                check_name="Invalid date logic",
                severity="CRITICAL",
                reason="Hire date is in the future.",
                suggested_action="Correct the hire date so it is not later than today.",
            )
            if not future_hire_rows.empty:
                future_hire_rows.insert(5, "field_name", hire_col)
                future_hire_rows.insert(6, "why_flagged", "Hire date is in the future")
                frames.append(future_hire_rows)

            if term_col:
                term_dates = ia._date_series(df, term_col)
                term_before_hire_mask = term_dates < hire_dates
                term_before_hire_rows = _annotated_rows(
                    df,
                    term_before_hire_mask,
                    check_name="Invalid date logic",
                    severity="CRITICAL",
                    reason="Termination date is before hire date.",
                    suggested_action="Correct the hire date or termination date so the employment timeline is valid.",
                )
                if not term_before_hire_rows.empty:
                    term_before_hire_rows.insert(5, "field_name", term_col)
                    term_before_hire_rows.insert(6, "why_flagged", "Termination date is before hire date")
                    frames.append(term_before_hire_rows)

    active_with_term_finding = findings_map.get("active_with_termination_date", {})
    if _should_expand(active_with_term_finding, total_rows):
        status_col = ia._status_column(df)
        term_col = ia._first_present(df, ["termination_date", "term_date", "end_date"])
        if status_col and term_col:
            statuses = df[status_col].astype(str).str.strip().str.lower()
            term_blank = ia._blank_mask(df[term_col])
            active_with_term_rows = _annotated_rows(
                df,
                (statuses == "active") & ~term_blank,
                check_name="Active employees with termination date",
                severity="CRITICAL",
                reason="Employee is marked active but has a termination date.",
                suggested_action="Correct either the worker status or the termination date before production use.",
            )
            if not active_with_term_rows.empty:
                frames.append(active_with_term_rows)

    missing_identity_finding = findings_map.get("missing_required_identity", {})
    if _should_expand(missing_identity_finding, total_rows):
        required_fields = [field for field in ["worker_id", "first_name", "last_name"] if field in df.columns]
        if required_fields:
            missing_masks = [ia._blank_mask(df[field]) for field in required_fields]
            combined_mask = missing_masks[0].copy()
            for mask in missing_masks[1:]:
                combined_mask = combined_mask | mask
            missing_identity_rows = _annotated_rows(
                df,
                combined_mask,
                check_name="Missing required identity fields",
                severity="CRITICAL",
                reason="One or more required identity fields are missing.",
                suggested_action="Populate worker_id, first_name, and last_name before production use.",
            )
            if not missing_identity_rows.empty:
                missing_fields = []
                for idx in missing_identity_rows["row_number"].astype(int) - 2:
                    row_missing = [field for field in required_fields if bool(ia._blank_mask(df.loc[[idx], field]).iloc[0])]
                    missing_fields.append(", ".join(row_missing))
                missing_identity_rows.insert(5, "missing_fields", missing_fields)
                frames.append(missing_identity_rows)

    return _concat_non_empty(frames)


def _summary_sheet(summary: dict) -> pd.DataFrame:
    rows: list[dict] = [
        {"Section": "Overview", "Metric": "Source Filename", "Value": summary.get("source_filename", "")},
        {"Section": "Overview", "Metric": "Gate Status", "Value": summary.get("gate_status", "")},
        {"Section": "Overview", "Metric": "Gate Message", "Value": summary.get("gate_message", "")},
        {"Section": "Overview", "Metric": "Total Rows", "Value": summary.get("total_rows", 0)},
        {"Section": "Overview", "Metric": "Overall Completeness", "Value": summary.get("overall_completeness", 0)},
    ]
    for severity, count in (summary.get("severity_counts") or {}).items():
        rows.append({"Section": "Severity Counts", "Metric": severity, "Value": count})
    for item in summary.get("check_counts") or []:
        rows.append(
            {
                "Section": "Check Counts",
                "Metric": f"{item.get('check_name')} ({item.get('severity')})",
                "Value": item.get("count", 0),
            }
        )
    return pd.DataFrame(rows)


def _build_sheets(file_path: Path, sheet_name: int | str, run_dir: Path) -> dict[str, pd.DataFrame]:
    summary = _load_summary(run_dir)
    config = ia._load_config()
    df, _, row_annotations = _read_and_normalize(file_path, sheet_name=sheet_name)

    sheets = {
        "Audit_Summary": _summary_sheet(summary),
        "Action_Required": _sheet_ready(
            _format_for_workbook(_action_required_sheet(df, summary, row_annotations)),
            "No critical row-level findings require direct auditor action for this run.",
        ),
        "Findings_Index": _sheet_ready(
            _format_for_workbook(_findings_index_df(summary)),
            "No findings were recorded for this audit run.",
        ),
        "Duplicate_Groups": _sheet_ready(
            _format_for_workbook(_duplicate_groups_sheet(df, config)),
            "No duplicate groups were found for this audit run.",
        ),
        "Compensation_Detail": _sheet_ready(
            _format_for_workbook(_compensation_detail_sheet(df)),
            "No actionable compensation rows were found for this audit run.",
        ),
    }
    return sheets


def _autosize_and_style(out_path: Path) -> None:
    wb = load_workbook(out_path)
    for ws in wb.worksheets:
        if ws.max_row >= 1:
            for cell in ws[1]:
                cell.fill = cell.fill.copy(fill_type="solid", start_color=HEADER_FILL, end_color=HEADER_FILL)
                cell.font = cell.font.copy(bold=True)
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        for col_cells in ws.columns:
            letter = col_cells[0].column_letter
            length = 0
            for cell in col_cells[:200]:
                try:
                    length = max(length, len(str(cell.value or "")))
                except Exception:
                    continue
            ws.column_dimensions[letter].width = min(max(length + 2, 12), 40)
    wb.save(out_path)


def build_workbook(file_path: Path, run_dir: Path, out_path: Path, sheet_name: int | str = 0) -> None:
    sheets = _build_sheets(file_path, sheet_name, run_dir)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for key, df in sheets.items():
            df.to_excel(writer, sheet_name=key, index=False)
    _autosize_and_style(out_path)
    print(f"[build_internal_audit_workbook] wrote: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build internal audit workbook")
    parser.add_argument("--file", required=True, help="Source file used for the audit")
    parser.add_argument("--run-dir", required=True, help="Directory containing internal audit outputs")
    parser.add_argument("--out", required=True, help="Output workbook path")
    parser.add_argument("--sheet-name", default="0", help="Excel sheet index or name")
    args = parser.parse_args()
    sheet_name: int | str = int(args.sheet_name) if str(args.sheet_name).lstrip("-").isdigit() else args.sheet_name
    build_workbook(Path(args.file), Path(args.run_dir), Path(args.out), sheet_name=sheet_name)


if __name__ == "__main__":
    main()
