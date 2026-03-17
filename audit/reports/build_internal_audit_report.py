"""
build_internal_audit_report.py - Professional 6-section PDF audit report.

Reads existing audit-mode artifacts:
  - internal_audit_report.json
  - internal_audit_completeness.csv
  - config/policy.yaml

Generates:
  dashboard_runs/{run_id}/internal_audit_report.pdf

Sections:
  1. Cover page (dark navy)
  2. Executive Summary
  3. Workforce Data Snapshot
  4. Findings by Severity (WHAT / WHY / ACTION)
  5. Data Completeness Analysis
  6. Recommended Action Plan
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from io import BytesIO
from pathlib import Path

from reportlab.lib.colors import HexColor, white
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.platypus import Paragraph

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT / "audit" / "summary"))
from config_loader import load_policy

# ---------------------------------------------------------------------------
# Page geometry (points)
# ---------------------------------------------------------------------------
PAGE_W, PAGE_H = 612, 792
LM = 54   # left margin
RM = 54   # right margin
TM = 48   # top margin (where section header bar starts)
BM = 48   # bottom margin
TW = PAGE_W - LM - RM  # text width = 504

# ---------------------------------------------------------------------------
# Design tokens
# ---------------------------------------------------------------------------
C_NAVY       = HexColor("#1C2B4A")
C_TEAL       = HexColor("#006B7D")
C_WHITE      = HexColor("#FFFFFF")
C_OFF_WHITE  = HexColor("#F8FAFC")
C_LIGHT_GRAY = HexColor("#EFF3F7")
C_MID_GRAY   = HexColor("#D0D8E4")
C_TEXT_DARK  = HexColor("#1A1A2E")
C_TEXT_MID   = HexColor("#4A5568")

C_CRIT_BG    = HexColor("#FEE2E2")
C_CRIT_FG    = HexColor("#991B1B")
C_CRIT_BADGE = HexColor("#DC2626")

C_HIGH_BG    = HexColor("#FEF3C7")
C_HIGH_FG    = HexColor("#92400E")
C_HIGH_BADGE = HexColor("#D97706")

C_MED_BG     = HexColor("#FEF9C3")
C_MED_FG     = HexColor("#713F12")
C_MED_BADGE  = HexColor("#CA8A04")

C_LOW_BG     = HexColor("#EFF6FF")
C_LOW_FG     = HexColor("#1E40AF")
C_LOW_BADGE  = HexColor("#3B82F6")

C_GOOD_BG    = HexColor("#DCFCE7")
C_GOOD_FG    = HexColor("#166534")

SEV_COLORS: dict[str, tuple] = {
    "CRITICAL": (C_CRIT_BG, C_CRIT_FG, C_CRIT_BADGE),
    "HIGH":     (C_HIGH_BG, C_HIGH_FG, C_HIGH_BADGE),
    "MEDIUM":   (C_MED_BG,  C_MED_FG,  C_MED_BADGE),
    "LOW":      (C_LOW_BG,  C_LOW_FG,  C_LOW_BADGE),
}

SEVERITY_ORDER = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

# Field priority classifications for completeness section
CRITICAL_FIELDS = {"worker_id", "first_name", "last_name", "salary", "worker_status", "hire_date", "email"}
STANDARD_FIELDS = {"department", "phone", "manager_id", "location", "cost_center"}

EFFORT_MAP = {"CRITICAL": "High", "HIGH": "High", "MEDIUM": "Medium", "LOW": "Low"}
OWNER_MAP  = {"CRITICAL": "HR Data Team", "HIGH": "HRIS Administrator", "MEDIUM": "HR Manager", "LOW": "HR Manager"}

# ---------------------------------------------------------------------------
# Paragraph styles
# ---------------------------------------------------------------------------

def _ps(name: str, font: str = "Helvetica", size: int = 10,
        color=None, leading: int | None = None, align: int = TA_LEFT) -> ParagraphStyle:
    return ParagraphStyle(
        name,
        fontName=font,
        fontSize=size,
        textColor=color or C_TEXT_DARK,
        leading=leading or max(12, int(size * 1.4)),
        alignment=align,
        wordWrap="CJK",
    )

STYLE_BODY      = _ps("body", size=10, color=C_TEXT_DARK, leading=14)
STYLE_BODY_MID  = _ps("body_mid", size=10, color=C_TEXT_MID, leading=14)
STYLE_CELL8     = _ps("cell8", size=8, color=C_TEXT_DARK, leading=11)
STYLE_CELL8_MID = _ps("cell8m", size=8, color=C_TEXT_MID, leading=11)
STYLE_CELL9     = _ps("cell9", size=9, color=C_TEXT_DARK, leading=12)
STYLE_CELL9_MID = _ps("cell9m", size=9, color=C_TEXT_MID, leading=12)
STYLE_LABEL     = _ps("label", font="Helvetica-Bold", size=8, color=C_TEAL, leading=11)

# ---------------------------------------------------------------------------
# Measurement canvas (for paragraph height estimation only - never renders)
# ---------------------------------------------------------------------------
_MEASURE_BUF: BytesIO | None = None
_MEASURE_C = None


def _measure_canvas():
    global _MEASURE_BUF, _MEASURE_C
    if _MEASURE_C is None:
        _MEASURE_BUF = BytesIO()
        _MEASURE_C = rl_canvas.Canvas(_MEASURE_BUF, pagesize=letter)
    return _MEASURE_C


def _para_height(text: str, style: ParagraphStyle, width: float) -> float:
    p = Paragraph(str(text), style)
    _, h = p.wrapOn(_measure_canvas(), width, 10000)
    return float(h)


# ---------------------------------------------------------------------------
# Core drawing primitives  (y convention: distance from page TOP)
# ---------------------------------------------------------------------------

def _rect(c, x: float, y_top: float, w: float, h: float,
          fill_color=None, stroke_color=None, stroke_w: float = 0.5) -> None:
    """Draw rectangle. y_top = distance from page top to top edge."""
    rl_y = PAGE_H - y_top - h
    if fill_color:
        c.setFillColor(fill_color)
    if stroke_color:
        c.setStrokeColor(stroke_color)
        c.setLineWidth(stroke_w)
    if fill_color and stroke_color:
        c.rect(x, rl_y, w, h, fill=1, stroke=1)
    elif fill_color:
        c.rect(x, rl_y, w, h, fill=1, stroke=0)
    elif stroke_color:
        c.rect(x, rl_y, w, h, fill=0, stroke=1)


def _text(c, x: float, y_baseline: float, text: str,
          font: str = "Helvetica", size: int = 10,
          color=None, align: str = "left") -> None:
    """Draw text. y_baseline = distance from page top to text baseline."""
    rl_y = PAGE_H - y_baseline
    c.setFillColor(color or C_TEXT_DARK)
    c.setFont(font, size)
    s = str(text)
    if align == "center":
        c.drawCentredString(x, rl_y, s)
    elif align == "right":
        c.drawRightString(x, rl_y, s)
    else:
        c.drawString(x, rl_y, s)


def _para(c, x: float, y_top: float, text: str,
          style: ParagraphStyle, width: float) -> float:
    """Draw paragraph. y_top = top of para from page top. Returns new y_top (below para)."""
    p = Paragraph(str(text), style)
    _, h = p.wrapOn(c, width, 10000)
    rl_y = PAGE_H - y_top - h
    p.drawOn(c, x, rl_y)
    return y_top + h


# ---------------------------------------------------------------------------
# Table drawing (all cells use Paragraph for word wrap)
# ---------------------------------------------------------------------------

def _draw_table(
    c,
    x: float,
    y_top: float,
    col_widths: list[float],
    rows: list[list],
    header_bg=None,
    alt_bg=None,
    row_bgs: list | None = None,
    pad: int = 4,
    font_size: int = 9,
    min_row_h: int = 20,
) -> float:
    """
    Draw a table with word-wrapped cells. Returns y after last row (from top).
    rows: list of lists of str or Paragraph.
    row_bgs: optional per-row background override (overrides alt_bg).
    """
    y = y_top
    for r_idx, row in enumerate(rows):
        is_header = r_idx == 0
        # Determine row background
        if row_bgs and r_idx < len(row_bgs) and row_bgs[r_idx] is not None:
            bg = row_bgs[r_idx]
        elif is_header and header_bg:
            bg = header_bg
        elif alt_bg:
            bg = alt_bg if r_idx % 2 == 0 else C_OFF_WHITE
        else:
            bg = C_OFF_WHITE

        # Build cell paragraphs and calculate row height
        cell_paras: list[tuple[Paragraph, float]] = []
        row_h = float(min_row_h)
        for c_idx, (cell, col_w) in enumerate(zip(row, col_widths)):
            if isinstance(cell, Paragraph):
                p = cell
            else:
                p = Paragraph(
                    str(cell) if cell is not None else "",
                    _ps(
                        f"tc{r_idx}_{c_idx}",
                        font="Helvetica-Bold" if is_header else "Helvetica",
                        size=font_size,
                        color=C_WHITE if is_header else C_TEXT_DARK,
                        leading=int(font_size * 1.3),
                    ),
                )
            available_w = col_w - pad * 2
            _, h = p.wrapOn(c, max(available_w, 1), 10000)
            row_h = max(row_h, h + pad * 2)
            cell_paras.append((p, col_w))

        # Draw row
        x_pos = x
        for p, col_w in cell_paras:
            _rect(c, x_pos, y, col_w, row_h, fill_color=bg, stroke_color=C_MID_GRAY, stroke_w=0.5)
            available_w = col_w - pad * 2
            _, h = p.wrapOn(c, max(available_w, 1), 10000)
            # Vertically center small content in the row
            v_offset = (row_h - h) / 2
            rl_y = PAGE_H - y - row_h + v_offset
            p.drawOn(c, x_pos + pad, rl_y)
            x_pos += col_w
        y += row_h
    return y


# ---------------------------------------------------------------------------
# Reusable section header bar
# ---------------------------------------------------------------------------

def _section_header(c, y_top: float, title: str) -> float:
    """Draw full-width navy header bar. Returns y after bar."""
    bar_h = 28
    _rect(c, LM, y_top, TW, bar_h, fill_color=C_NAVY)
    _text(c, LM + 12, y_top + 19, title, font="Helvetica-Bold", size=11, color=C_WHITE)
    return y_top + bar_h


# ---------------------------------------------------------------------------
# Footer (every page except cover)
# ---------------------------------------------------------------------------

def _footer(c, page_num: int, total_pages: int, org_name: str, run_id: str) -> None:
    text = f"CONFIDENTIAL - {org_name} - Internal Data Audit - Run {run_id} - Page {page_num} of {total_pages}"
    c.setFillColor(C_TEXT_MID)
    c.setFont("Helvetica", 8)
    c.drawCentredString(PAGE_W / 2, BM / 2 + 2, text)


# ---------------------------------------------------------------------------
# Section 1: Cover Page
# ---------------------------------------------------------------------------

def _draw_cover(c, run_id: str, summary: dict, org_name: str, date_str: str) -> None:
    # Full navy background
    c.setFillColor(C_NAVY)
    c.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)

    # Logo area: teal circle + "DW" monogram + "Data Whisperer"
    logo_circle_cx = LM + 20
    logo_circle_cy = PAGE_H - TM - 32
    c.setFillColor(C_TEAL)
    c.circle(logo_circle_cx, logo_circle_cy, 20, fill=1, stroke=0)
    c.setFillColor(C_WHITE)
    c.setFont("Helvetica-Bold", 14)
    c.drawCentredString(logo_circle_cx, logo_circle_cy - 5, "DW")
    c.setFont("Helvetica-Bold", 20)
    c.drawString(LM + 50, logo_circle_cy - 7, "Data Whisperer")

    # Main headline - centered vertically
    headline_rl_y = PAGE_H / 2 + 70
    c.setFillColor(C_WHITE)
    c.setFont("Helvetica-Bold", 28)
    c.drawCentredString(PAGE_W / 2, headline_rl_y, "INTERNAL HR DATA AUDIT REPORT")

    # Teal divider line
    div_rl_y = headline_rl_y - 22
    c.setStrokeColor(C_TEAL)
    c.setLineWidth(2)
    c.line(PAGE_W / 2 - 150, div_rl_y, PAGE_W / 2 + 150, div_rl_y)

    # Organization name
    c.setFillColor(HexColor("#BBCCDD"))
    c.setFont("Helvetica", 16)
    c.drawCentredString(PAGE_W / 2, div_rl_y - 32, org_name)

    # Prepared by
    c.setFillColor(HexColor("#8899AA"))
    c.setFont("Helvetica", 12)
    c.drawCentredString(PAGE_W / 2, div_rl_y - 58, "Prepared by Data Whisperer Engine")

    # Bottom teal band
    band_h = 44
    c.setFillColor(C_TEAL)
    c.rect(0, 0, PAGE_W, band_h, fill=1, stroke=0)
    c.setFillColor(C_WHITE)
    c.setFont("Helvetica", 11)
    fname = str(summary.get("source_filename", ""))
    c.drawString(LM, 16, f"File Audited: {fname}")
    c.drawCentredString(PAGE_W / 2, 16, f"Run ID: {run_id}")
    c.drawRightString(PAGE_W - RM, 16, f"Date: {date_str}")


# ---------------------------------------------------------------------------
# Finding text lookup (WHAT / WHY / ACTION per check_key)
# ---------------------------------------------------------------------------

def _finding_texts(check_key: str, finding: dict) -> tuple[str, str, str]:
    count = int(finding.get("count", 0))
    pct = finding.get("pct", 0)
    field = finding.get("field", "")
    example = finding.get("_example", "Dept-Region")
    unique_count = finding.get("_unique_count", "?")

    lookup: dict[str, tuple[str, str, str]] = {
        "duplicate_worker_id": (
            f"{count:,} employee records share a Worker ID with at least one other employee. "
            "Worker IDs must be unique - each ID should belong to exactly one person.",
            "Duplicate IDs are one of the most serious data integrity failures in HR data. "
            "During a system migration, duplicate IDs cause corrections to be applied to the wrong person, "
            "create ambiguous matches, and may result in one employee's data overwriting another's.",
            "Identify the root cause of each duplicate. Common causes are manual ID assignment errors, "
            "system import bugs, or records merged incorrectly. Assign a unique ID to each employee and "
            "update all references before this data is used for any migration or reporting.",
        ),
        "missing_worker_id": (
            f"{count:,} employee records have no Worker ID at all. "
            "These records exist in the system but cannot be identified, tracked, or matched.",
            "Records without an ID cannot be matched during a migration. They will appear as unmatched "
            "records and may be treated as new hires in the target system, creating phantom employees.",
            f"Assign a unique Worker ID to each of these {count:,} records before running any reconciliation "
            "or migration process. If the source system does not have IDs for these employees, work with "
            "the system administrator to generate and assign them.",
        ),
        "phone_invalid": (
            f"{count:,} employee phone numbers contain impossible values. "
            f"{finding.get('description', '')}",
            "Invalid phone numbers prevent HR from contacting employees for urgent matters. "
            "They also indicate a systemic data export or generation error - if phone numbers are wrong, "
            "other numeric fields in this file may also be unreliable.",
            "This appears to be a file generation error rather than individual data entry mistakes. "
            "Re-export the data from the source system. If the source system also shows invalid phone values, "
            "the phone field data needs to be re-collected from employees and re-entered.",
        ),
        "active_zero_salary": (
            f"{count:,} employees are marked as Active but have no salary or a $0 salary recorded in this file.",
            "An active employee with no salary will not receive correct pay if this data is used for payroll "
            "processing or loaded into a new system. This represents both a data integrity failure and a "
            "potential payroll risk.",
            f"Verify the correct salary for each of these {count:,} employees with their manager or HR records. "
            "If the employee is genuinely active, enter their correct salary before this data is used "
            "for any operational purpose.",
        ),
        "duplicate_email": (
            f"{count:,} employees share an email address with at least one other employee. "
            "This indicates a limited set of test or placeholder email addresses were used.",
            "Duplicate email addresses prevent system-generated notifications from reaching the right person. "
            "In Workday and similar systems, email is often used as a unique identifier for self-service access. "
            "Duplicates will block employee portal access and system notifications.",
            "Verify unique email addresses for all employees. If this file was generated for testing purposes, "
            "replace with real employee email addresses before using for any migration, reporting, or system load.",
        ),
        "duplicate_name_different_id": (
            f"{count:,} pairs of employees share the same first and last name but have different Worker IDs.",
            "Shared names with different IDs could represent duplicate records for the same person, or genuinely "
            "different people with the same name. Either way, they create identity matching risk during "
            "reconciliation - the engine may match the wrong person.",
            "Review each flagged pair. If they represent the same person, merge the records and assign one ID. "
            "If they are different people, add a distinguishing identifier such as middle name, employee suffix, "
            "or date of birth to differentiate them during matching.",
        ),
        "status_no_terminated": (
            "This file contains no terminated, separated, or resigned employees. "
            "Only Active, Inactive, and Pending statuses are present.",
            "Most HR files that represent a complete workforce include at least some terminated employees "
            "retained for compliance, historical reporting, or rehire eligibility tracking. "
            "A file with zero terminations may be missing a significant portion of the historical employee record.",
            "Confirm whether this file is intentionally limited to non-terminated employees. "
            "If it should include the full employee history, re-export from the source system with all "
            "employment statuses included. If this is intentional, document the scope limitation in your migration plan.",
        ),
        "status_high_pending": (
            f"{pct}% of employees ({count:,}) have a status of Pending. "
            "These are employees whose employment status has not been finalized in the system.",
            "A high Pending rate suggests incomplete data entry or a bulk import that was not followed up on. "
            "Pending employees cannot be reliably classified as active or inactive, which affects headcount "
            "reporting, payroll, and benefits eligibility.",
            f"Review all {count:,} Pending employees and update their status to the correct final value: "
            "Active, Inactive, or Terminated. Do not migrate or use this data for reporting until "
            "Pending statuses are resolved.",
        ),
        "age_uniformity": (
            f"Employee ages in this file show only {unique_count} distinct values across {count:,} employees. "
            "This pattern indicates that real employee ages were not captured - placeholder values were used instead.",
            "Placeholder ages will cause incorrect results in any analysis involving age, tenure, benefits eligibility "
            "(which depends on age thresholds), or retirement planning. If this data is used for compliance "
            "reporting, incorrect ages constitute inaccurate reporting.",
            "Replace placeholder ages with actual employee date of birth or age data from the source system "
            "or employee records. If the source system does not capture age or date of birth, establish a "
            "process to collect it before the next data cycle.",
        ),
        "combined_field": (
            f"The '{field}' field appears to combine two separate pieces of information using a hyphen separator. "
            f"Example value: '{example}'. Two attributes are stored in a single field rather than two separate columns.",
            "Combined fields make it impossible to filter, group, or analyze by just one of the two values "
            "without additional data processing. In Workday, department and location are separate fields and "
            "must be loaded separately. This field cannot be directly loaded into any standard HR system.",
            "Split this field into two separate columns before use. Create one column for the value before the "
            "hyphen and a second column for the value after the hyphen. Confirm the correct field mapping "
            "with the target system administrator.",
        ),
        "hire_date_suspicious_default": (
            f"{count:,} employees have hire dates that appear to be system defaults rather than real hire dates.",
            "Default hire dates cause incorrect seniority calculations, wrong benefits eligibility determinations, "
            "and inaccurate tenure reporting. In some compliance frameworks, hire date accuracy is legally required.",
            "Research the correct hire date for each flagged employee from offer letters, original employment "
            "records, or manager confirmation. Do not migrate records with default hire dates.",
        ),
        "suspicious_round_salary": (
            f"{count:,} employees have salary values that appear to be placeholder entries rather than real compensation.",
            "Placeholder salaries loaded into a new system will result in incorrect pay, wrong bonus calculations, "
            "and inaccurate compensation band analysis. This is both a data quality and a payroll risk.",
            "Verify the correct salary for each flagged employee with HR records or manager confirmation before migrating.",
        ),
        "pay_equity_flag": (
            f"{count:,} role-department groups show salary variance above 30% within the same title and department.",
            "Pay equity variance above 30% within the same role warrants review for potential discriminatory "
            "compensation patterns. This analysis does not include demographic data but identifies statistical "
            "anomalies that should be investigated.",
            "Review each flagged role-department group. Verify that salary differences reflect legitimate factors "
            "such as seniority, performance, or location. Document the rationale for any significant variance.",
        ),
        "ghost_employee_indicator": (
            f"{count:,} records show indicators of possible ghost employees: active status with no salary, "
            "no department, and no manager.",
            "Ghost employees are fictitious or former employees who continue to receive pay. These records "
            "have characteristics consistent with payroll fraud or data entry errors that could result in "
            "unauthorized payments.",
            "Investigate each flagged record immediately. Confirm employment status with direct managers "
            "and payroll. Remove any records that cannot be confirmed as legitimate active employees.",
        ),
    }

    default = (
        finding.get("description", ""),
        "This issue may affect data integrity and should be reviewed before migration or reporting.",
        "Review and correct all flagged records before using this data for any operational purpose.",
    )
    return lookup.get(check_key, default)


# ---------------------------------------------------------------------------
# Section 2: Executive Summary
# ---------------------------------------------------------------------------

def _draw_exec_summary(c, page_num: int, total_pages: int,
                       summary: dict, org_name: str, run_id: str, date_str: str) -> None:
    y = TM
    y = _section_header(c, y, "EXECUTIVE SUMMARY")
    y += 14

    counts = summary.get("severity_counts", {}) or {}
    total_rows = int(summary.get("total_rows", 0))
    col_count = int(summary.get("total_columns", 0))
    findings = [f for f in (summary.get("findings_for_pdf") or []) if f.get("count", 0) > 0]
    total_issues = len(findings)
    recs_affected = sum(f.get("count", 0) for f in findings)

    left_w = TW * 0.58
    right_x = LM + left_w + 10
    right_w = TW - left_w - 10

    # Left column: What We Audited heading + body
    _text(c, LM, y + 14, "What We Audited", font="Helvetica-Bold", size=14, color=C_NAVY)
    y_body = y + 22
    body_text = (
        f"This report presents the findings of a systematic data quality audit conducted on "
        f"{summary.get('source_filename', 'the uploaded file')}, containing <b>{total_rows:,}</b> employee "
        f"records across <b>{col_count}</b> data fields. The audit examined every record and every field to "
        f"identify data integrity issues, missing information, impossible values, and patterns that indicate "
        f"data quality problems.<br/><br/>"
        f"The audit was completed on {date_str} and covers {col_count} distinct data fields. Findings are "
        f"rated by severity: <b>Critical</b> issues require immediate action before this data can be used "
        f"for any operational purpose. <b>High</b> issues should be resolved before migration or reporting. "
        f"<b>Medium</b> issues represent data quality improvements. <b>Low</b> findings are informational."
    )
    y_after_body = _para(c, LM, y_body, body_text, STYLE_BODY_MID, left_w - 4)

    # Right column: 2x2 severity metric boxes
    box_w = (right_w - 6) / 2
    box_h = 52
    sev_boxes = [
        ("CRITICAL", counts.get("CRITICAL", 0), C_CRIT_BADGE),
        ("HIGH",     counts.get("HIGH", 0),     C_HIGH_BADGE),
        ("MEDIUM",   counts.get("MEDIUM", 0),   C_MED_BADGE),
        ("LOW",      counts.get("LOW", 0),       C_LOW_BADGE),
    ]
    box_y_start = y + 22
    for i, (sev, cnt, badge_color) in enumerate(sev_boxes):
        bx = right_x + (i % 2) * (box_w + 6)
        by = box_y_start + (i // 2) * (box_h + 6)
        _rect(c, bx, by, box_w, box_h, fill_color=C_LIGHT_GRAY, stroke_color=C_MID_GRAY)
        _text(c, bx + box_w / 2, by + 28, str(cnt),
              font="Helvetica-Bold", size=24, color=badge_color, align="center")
        _text(c, bx + box_w / 2, by + 44, sev,
              font="Helvetica", size=9, color=C_TEXT_MID, align="center")

    y = max(y_after_body, box_y_start + 2 * (box_h + 6)) + 16

    # Findings summary table
    _text(c, LM, y + 12, "Findings Summary", font="Helvetica-Bold", size=12, color=C_NAVY)
    y += 20
    col_widths_sum = [100, 100, 140, 164]
    sum_header = ["Severity", "Checks Fired", "Records Flagged", "Status"]
    sum_rows = [sum_header]
    for sev in SEVERITY_ORDER:
        sev_count = int(counts.get(sev, 0))
        checks_fired = sum(1 for f in findings if f.get("severity") == sev)
        recs = sum(int(f.get("count", 0)) for f in findings if f.get("severity") == sev)
        status_text = "REQUIRES ACTION" if sev_count > 0 else "PASS"
        sum_rows.append([sev, str(checks_fired), f"{recs:,}", status_text])

    y = _draw_table(c, LM, y, col_widths_sum, sum_rows, header_bg=C_NAVY, alt_bg=C_LIGHT_GRAY)
    y += 16

    # File profile table
    _text(c, LM, y + 12, "File Profile", font="Helvetica-Bold", size=12, color=C_NAVY)
    y += 20
    clean_records = max(0, total_rows - recs_affected)
    clean_pct = round(clean_records / total_rows * 100, 1) if total_rows else 0.0
    profile_data = [
        ["File name", str(summary.get("source_filename", ""))],
        ["Records analyzed", f"{total_rows:,}"],
        ["Fields examined", str(col_count)],
        ["Total issue types found", str(total_issues)],
        ["Records with issues", f"{recs_affected:,}"],
        ["Records clean", f"{clean_records:,} ({clean_pct}%)"],
        ["Audit engine version", "Data Whisperer v2.0"],
        ["Run ID", run_id],
    ]
    prof_header = ["Field", "Value"]
    prof_rows: list[list] = [prof_header]
    for label, val in profile_data:
        prof_rows.append([
            Paragraph(f"<b>{label}</b>", STYLE_CELL9_MID),
            Paragraph(str(val), STYLE_CELL9),
        ])
    y = _draw_table(c, LM, y, [252, 252], prof_rows, header_bg=C_NAVY, alt_bg=C_LIGHT_GRAY)
    _footer(c, page_num, total_pages, org_name, run_id)


# ---------------------------------------------------------------------------
# Section 3: Workforce Data Snapshot
# ---------------------------------------------------------------------------

def _draw_workforce_snapshot(c, page_num: int, total_pages: int,
                              summary: dict, org_name: str, run_id: str, date_str: str) -> None:
    y = TM
    y = _section_header(c, y, "WORKFORCE DATA SNAPSHOT")
    y += 10

    total_rows = int(summary.get("total_rows", 0))
    intro = (
        "Before examining data quality issues, this section presents a complete profile of the workforce data "
        "contained in this file. Understanding the composition of the data provides context for the findings that follow."
    )
    y = _para(c, LM, y, intro, STYLE_BODY_MID, TW)
    y += 12

    # Employment Status Distribution
    _text(c, LM, y + 11, "Employment Status Distribution", font="Helvetica-Bold", size=11, color=C_NAVY)
    y += 18
    status_breakdown = summary.get("status_breakdown", {}) or {}
    status_col_w = [120, 80, 100, 204]
    status_hdr = ["Status", "Count", "Percentage", "Notes"]
    status_rows: list[list] = [status_hdr]
    has_terminated = False
    for sv, cnt in sorted(status_breakdown.items(), key=lambda x: -int(x[1])):
        pct = round(int(cnt) / total_rows * 100, 1) if total_rows else 0.0
        sv_lower = str(sv).lower()
        if sv_lower in ("terminated", "term", "separated", "resigned", "dismissed"):
            has_terminated = True
        if sv_lower == "active":
            note = "Employees currently employed"
        elif sv_lower == "pending":
            note = "HIGH - exceeds normal threshold" if pct > 25 else "Status not finalized"
        elif sv_lower in ("terminated", "term", "separated", "resigned", "dismissed"):
            note = "Former employees retained for compliance"
        elif sv_lower == "inactive":
            note = "Former employees retained in system"
        else:
            note = "Non-standard status value"
        status_rows.append([str(sv), f"{int(cnt):,}", f"{pct}%", note])
    if not has_terminated:
        status_rows.append(["[Terminated]", "0", "0.0%", "WARNING - none found in file"])
    y = _draw_table(c, LM, y, status_col_w, status_rows, header_bg=C_NAVY, alt_bg=C_LIGHT_GRAY)
    y += 14

    # Data Field Inventory
    _text(c, LM, y + 11, "Fields Examined", font="Helvetica-Bold", size=11, color=C_NAVY)
    y += 18
    comp_rows = summary.get("completeness_rows", []) or []
    field_col_w = [140, 70, 80, 80, 134]
    field_hdr = ["Field Name", "Type", "Complete", "Null Count", "Notes"]
    field_rows: list[list] = [field_hdr]

    def _infer_type(fname: str) -> str:
        if fname in ("salary", "age", "payrate"):
            return "Numeric"
        if fname in ("hire_date", "date_of_birth"):
            return "Date"
        if fname in ("remote_work",):
            return "Boolean"
        return "Text"

    for row in comp_rows[:20]:
        fname = str(row.get("field", ""))
        filled_pct = float(row.get("filled_pct", 0))
        blank_count = int(row.get("blank_count", 0))
        if filled_pct >= 99:
            note = "Fully populated"
        elif filled_pct >= 95:
            note = "Near complete"
        elif filled_pct >= 80:
            note = "Review recommended"
        else:
            note = "Significant gap"
        field_rows.append([fname, _infer_type(fname), f"{filled_pct:.1f}%", str(blank_count), note])
    y = _draw_table(c, LM, y, field_col_w, field_rows, header_bg=C_NAVY, alt_bg=C_LIGHT_GRAY, font_size=8)
    _footer(c, page_num, total_pages, org_name, run_id)


# ---------------------------------------------------------------------------
# Section 4: Findings by Severity
# ---------------------------------------------------------------------------

def _draw_finding_block(
    c,
    y: float,
    finding: dict,
    org_name: str,
    run_id: str,
    page_num: int,
    total_pages: int,
    is_new_page: bool = False,
) -> tuple[float, int]:
    """Draw a single finding. Returns (new_y, new_page_num)."""
    check_key = str(finding.get("check_key", ""))
    check_name = str(finding.get("check_name", ""))
    severity = str(finding.get("severity", "MEDIUM"))
    count = int(finding.get("count", 0))
    sample_rows = list(finding.get("sample_rows", []) or [])

    _, _, badge_color = SEV_COLORS.get(severity, (C_LIGHT_GRAY, C_TEXT_DARK, C_TEXT_MID))
    what, why, action = _finding_texts(check_key, finding)

    body_w = TW - 14
    what_h = _para_height(f"<b>WHAT WAS FOUND</b><br/>{what}", STYLE_BODY, body_w) + 8
    why_h  = _para_height(f"<b>WHY IT MATTERS</b><br/>{why}", STYLE_BODY_MID, body_w) + 8
    act_h  = _para_height(f"<b>RECOMMENDED ACTION</b><br/>{action}", STYLE_BODY, body_w) + 8
    shown = sample_rows[:5]
    table_h = (18 + len(shown) * 22 + 14) if shown else 0
    total_h = 24 + 8 + what_h + why_h + act_h + table_h + 20

    # Page break if not enough space
    bottom_limit = PAGE_H - BM - 40
    if not is_new_page and y + total_h > bottom_limit:
        _footer(c, page_num, total_pages, org_name, run_id)
        c.showPage()
        page_num += 1
        y = TM

    # Finding header bar
    _rect(c, LM, y, TW, 24, fill_color=badge_color)
    badge_text = f" {severity} "
    c.setFillColor(C_WHITE)
    c.setFont("Helvetica-Bold", 9)
    badge_w = c.stringWidth(badge_text, "Helvetica-Bold", 9)
    c.drawString(LM + 8, PAGE_H - y - 16, badge_text)
    c.setFont("Helvetica-Bold", 10)
    name_display = check_name[:60] + "..." if len(check_name) > 63 else check_name
    c.drawString(LM + 8 + badge_w + 6, PAGE_H - y - 16, name_display)
    c.setFont("Helvetica", 9)
    c.drawRightString(PAGE_W - RM - 8, PAGE_H - y - 16, f"{count:,} records affected")
    y += 24 + 8

    # Left accent borders + WHAT / WHY / ACTION paragraphs
    _rect(c, LM, y, 3, what_h, fill_color=C_TEAL)
    y = _para(c, LM + 12, y, f"<b>WHAT WAS FOUND</b><br/>{what}", STYLE_BODY, body_w)
    y += 8

    _rect(c, LM, y, 3, why_h, fill_color=badge_color)
    y = _para(c, LM + 12, y, f"<b>WHY IT MATTERS</b><br/>{why}", STYLE_BODY_MID, body_w)
    y += 8

    _rect(c, LM, y, 3, act_h, fill_color=C_NAVY)
    y = _para(c, LM + 12, y, f"<b>RECOMMENDED ACTION</b><br/>{action}", STYLE_BODY, body_w)
    y += 8

    # Sample records table
    if shown:
        title_text = f"Sample of Flagged Records (showing {len(shown)} of {count:,})"
        _text(c, LM, y + 10, title_text, font="Helvetica-Bold", size=9, color=C_TEXT_MID)
        y += 16

        # Build column list from sample data keys (exclude internal keys starting with _)
        all_keys = list({k: None for row in shown for k in row.keys() if not str(k).startswith("_")}.keys())
        if "row_number" in all_keys:
            all_keys = ["row_number"] + [k for k in all_keys if k != "row_number"]
        col_count_t = min(len(all_keys), 5)
        all_keys = all_keys[:col_count_t]
        col_w_each = TW / col_count_t
        sample_col_widths = [col_w_each] * col_count_t
        sample_table = [all_keys] + [
            [Paragraph(str(row.get(k, "")), STYLE_CELL8) for k in all_keys]
            for row in shown
        ]
        y = _draw_table(c, LM, y, sample_col_widths, sample_table, header_bg=C_NAVY, alt_bg=C_LIGHT_GRAY, font_size=8)

        if count > 5:
            _text(c, LM, y + 10,
                  f"Full list of {count:,} affected records is available in internal_audit_data.csv",
                  font="Helvetica-Oblique", size=9, color=C_TEXT_MID)
            y += 16

    y += 14
    return y, page_num


def _draw_findings_section(
    c, start_page: int, total_pages: int,
    summary: dict, org_name: str, run_id: str
) -> int:
    """Draw all findings grouped by severity. Returns final page number."""
    findings_raw = [f for f in (summary.get("findings_for_pdf") or []) if int(f.get("count", 0)) > 0]
    sev_idx = {s: i for i, s in enumerate(SEVERITY_ORDER)}
    findings_sorted = sorted(findings_raw, key=lambda f: sev_idx.get(f.get("severity", "LOW"), 99))

    if not findings_sorted:
        y = TM
        y = _section_header(c, y, "FINDINGS BY SEVERITY")
        y += 20
        _text(c, LM, y + 12, "No data quality issues were found in this file.", font="Helvetica-Bold",
              size=12, color=C_GOOD_FG)
        _footer(c, start_page, total_pages, org_name, run_id)
        return start_page

    page_num = start_page
    y = TM
    first = True
    for finding in findings_sorted:
        if first:
            y = _section_header(c, y, "FINDINGS BY SEVERITY")
            y += 10
            first = False
        is_new = (y == TM)
        y, page_num = _draw_finding_block(c, y, finding, org_name, run_id, page_num, total_pages, is_new_page=is_new)

    _footer(c, page_num, total_pages, org_name, run_id)
    return page_num


# ---------------------------------------------------------------------------
# Section 5: Data Completeness Analysis
# ---------------------------------------------------------------------------

def _field_priority(field: str) -> str:
    if field in CRITICAL_FIELDS:
        return "Critical"
    if field in STANDARD_FIELDS:
        return "Standard"
    return "Optional"


def _draw_completeness(c, page_num: int, total_pages: int,
                       summary: dict, org_name: str, run_id: str) -> None:
    y = TM
    y = _section_header(c, y, "DATA COMPLETENESS ANALYSIS")
    y += 10

    total_rows = int(summary.get("total_rows", 0))
    intro = (
        f"This section shows the completeness of every data field across all {total_rows:,} employee records. "
        "A field is considered complete when it contains a real value - not blank, null, or a placeholder. "
        "Fields marked Critical are required for system migration and compliance reporting. "
        "Fields marked Standard are important but not blocking. Fields marked Optional are collected when available."
    )
    y = _para(c, LM, y, intro, STYLE_BODY_MID, TW)
    y += 12

    comp_rows = summary.get("completeness_rows", []) or []
    col_w_comp = [130, 70, 70, 60, 174]
    comp_hdr = ["Field Name", "Priority", "Complete", "Missing", "Assessment"]
    comp_table: list[list] = [comp_hdr]

    for row in comp_rows:
        field = str(row.get("field", ""))
        filled_pct = float(row.get("filled_pct", 0))
        blank_count = int(row.get("blank_count", 0))
        priority = _field_priority(field)

        # Color complete cell by priority + pct
        if priority == "Critical":
            cell_bg = C_CRIT_BG if filled_pct < 99 else C_GOOD_BG
            cell_fg = C_CRIT_FG if filled_pct < 99 else C_GOOD_FG
        elif priority == "Standard":
            cell_bg = C_HIGH_BG if filled_pct < 90 else C_GOOD_BG
            cell_fg = C_HIGH_FG if filled_pct < 90 else C_GOOD_FG
        else:
            cell_bg = C_MED_BG if filled_pct < 80 else C_GOOD_BG
            cell_fg = C_MED_FG if filled_pct < 80 else C_GOOD_FG

        if filled_pct >= 100:
            assessment = "Fully populated"
        elif filled_pct >= 99:
            assessment = f"Complete - {blank_count} record missing"
        elif filled_pct >= 95:
            assessment = "Near complete"
        elif filled_pct >= 80:
            assessment = "Review recommended"
        elif priority == "Critical":
            assessment = "BLOCKING - must resolve before migration"
        elif priority == "Standard":
            assessment = "Significant gap - investigate root cause"
        elif filled_pct < 50:
            assessment = "Severe gap - field may not be collected"
        else:
            assessment = "Review recommended"

        complete_cell = Paragraph(f"{filled_pct:.1f}%", _ps(f"cc_{field}", size=8, color=cell_fg))
        comp_table.append([
            Paragraph(field, STYLE_CELL8),
            Paragraph(priority, STYLE_CELL8),
            complete_cell,
            Paragraph(str(blank_count), STYLE_CELL8),
            Paragraph(assessment, STYLE_CELL8),
        ])

    y = _draw_table(c, LM, y, col_w_comp, comp_table, header_bg=C_NAVY, alt_bg=C_LIGHT_GRAY, font_size=8)
    y += 12

    # Summary text below table
    total_fields = len(comp_rows)
    full_fields = sum(1 for r in comp_rows if float(r.get("filled_pct", 0)) >= 100)
    overall_pct = float(summary.get("overall_completeness", 0))
    sum_text = (
        f"<b>Overall data completeness: {overall_pct}%</b><br/>"
        f"Fields fully populated: {full_fields} of {total_fields}<br/>"
        "Audit engine: Data Whisperer v2.0"
    )
    _para(c, LM, y, sum_text, STYLE_BODY_MID, TW)
    _footer(c, page_num, total_pages, org_name, run_id)


# ---------------------------------------------------------------------------
# Section 6: Action Plan
# ---------------------------------------------------------------------------

def _draw_action_plan(c, page_num: int, total_pages: int,
                      summary: dict, org_name: str, run_id: str) -> None:
    y = TM
    y = _section_header(c, y, "RECOMMENDED ACTION PLAN")
    y += 10

    intro = (
        "Based on the findings in this report, the following actions are recommended before this data is used "
        "for migration, reporting, or any operational purpose. Actions are listed in priority order."
    )
    y = _para(c, LM, y, intro, STYLE_BODY_MID, TW)
    y += 12

    findings = [f for f in (summary.get("findings_for_pdf") or []) if int(f.get("count", 0)) > 0]
    sev_idx = {s: i for i, s in enumerate(SEVERITY_ORDER)}
    findings_sorted = sorted(findings, key=lambda f: sev_idx.get(f.get("severity", "LOW"), 99))

    col_w_action = [50, 220, 90, 70, 74]
    action_hdr = ["Priority", "Action", "Affected Records", "Effort", "Owner"]
    action_rows: list[list] = [action_hdr]
    action_row_bgs: list = [None]  # None = header (handled by header_bg)
    for i, finding in enumerate(findings_sorted, 1):
        sev = str(finding.get("severity", "MEDIUM"))
        count = int(finding.get("count", 0))
        _, _, act = _finding_texts(str(finding.get("check_key", "")), finding)
        act_short = act[:130] + "..." if len(act) > 133 else act
        row_bg, _, _ = SEV_COLORS.get(sev, (C_LIGHT_GRAY, C_TEXT_DARK, C_TEXT_MID))
        priority_cell = Paragraph(
            str(i),
            _ps(f"pr{i}", font="Helvetica-Bold", size=10, align=TA_CENTER),
        )
        action_rows.append([
            priority_cell,
            Paragraph(act_short, STYLE_CELL8),
            Paragraph(f"{count:,}", STYLE_CELL8),
            Paragraph(EFFORT_MAP.get(sev, "Medium"), STYLE_CELL8),
            Paragraph(OWNER_MAP.get(sev, "HR Manager"), STYLE_CELL8),
        ])
        action_row_bgs.append(row_bg)

    y = _draw_table(c, LM, y, col_w_action, action_rows, header_bg=C_NAVY,
                    row_bgs=action_row_bgs, font_size=8)
    y += 20

    # Sign-off block
    box_h = 94
    if y + box_h > PAGE_H - BM - 10:
        y = PAGE_H - BM - box_h - 10  # push to bottom if tight
    _rect(c, LM, y, TW, box_h, fill_color=C_OFF_WHITE, stroke_color=C_MID_GRAY)
    disclaimer = (
        "This report was generated by the Data Whisperer internal audit engine. All findings are based on "
        "the data present in the uploaded file. This report does not constitute legal advice. "
        "For compliance questions, consult qualified employment counsel."
    )
    _para(c, LM + 10, y + 8, disclaimer, STYLE_BODY_MID, TW - 20)
    sig_y = y + box_h - 26
    _text(c, LM + 10, sig_y,
          "Reviewed by: _____________________   Date: _________",
          font="Helvetica", size=10, color=C_TEXT_DARK)
    _text(c, LM + 10, sig_y + 16,
          "Approved by: _____________________   Date: _________",
          font="Helvetica", size=10, color=C_TEXT_DARK)
    _footer(c, page_num, total_pages, org_name, run_id)


# ---------------------------------------------------------------------------
# Main render
# ---------------------------------------------------------------------------

def _render(out, run_id: str, summary: dict, org_name: str,
            total_pages: int, date_str: str) -> int:
    """Render full report. Returns actual final page number."""
    c = rl_canvas.Canvas(out, pagesize=letter)

    # Page 1: Cover (no footer)
    _draw_cover(c, run_id, summary, org_name, date_str)
    c.showPage()

    # Page 2: Executive Summary
    _draw_exec_summary(c, 2, total_pages, summary, org_name, run_id, date_str)
    c.showPage()

    # Page 3: Workforce Snapshot
    _draw_workforce_snapshot(c, 3, total_pages, summary, org_name, run_id, date_str)
    c.showPage()

    # Pages 4+: Findings (variable number of pages)
    findings_end_page = _draw_findings_section(c, 4, total_pages, summary, org_name, run_id)
    c.showPage()

    # Completeness
    completeness_page = findings_end_page + 1
    _draw_completeness(c, completeness_page, total_pages, summary, org_name, run_id)
    c.showPage()

    # Action Plan
    action_page = completeness_page + 1
    _draw_action_plan(c, action_page, total_pages, summary, org_name, run_id)
    c.showPage()

    c.save()
    return action_page


def _estimate_total_pages(summary: dict) -> int:
    """Quick estimate of page count before two-pass render."""
    findings = [f for f in (summary.get("findings_for_pdf") or []) if int(f.get("count", 0)) > 0]
    findings_pages = max(1, (len(findings) + 1) // 2)
    return 3 + findings_pages + 2  # cover + exec + workforce + findings + completeness + action


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _organization_name() -> str:
    try:
        policy = load_policy(ROOT / "config" / "policy.yaml")
        client = policy.get("client", {})
        return str(client.get("name") or policy.get("client_name") or "Confidential")
    except Exception:
        return "Confidential"


def build_pdf(run_id: str, run_dir: Path, out_path: Path) -> None:
    summary = _read_json(run_dir / "internal_audit_report.json")
    org_name = _organization_name()
    date_str = datetime.now().strftime("%B %d, %Y")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Pass 1: dry-run to count pages
    buf = BytesIO()
    actual_pages = _render(buf, run_id, summary, org_name, total_pages=99, date_str=date_str)

    # Pass 2: real render with correct total
    _render(str(out_path), run_id, summary, org_name, total_pages=actual_pages, date_str=date_str)
    print(f"[build_pdf] wrote {actual_pages}-page PDF: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Internal Data Audit PDF report")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    build_pdf(args.run_id, Path(args.run_dir), Path(args.out))


if __name__ == "__main__":
    main()
