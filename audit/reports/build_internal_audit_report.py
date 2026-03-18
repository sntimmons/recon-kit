"""
build_internal_audit_report.py - 5-page professional PDF audit report.

Pages:
  1. Cover               - full-page navy, 2x2 severity grid
  2. Executive Summary   - narrative + audit scope table + file list
  3. Findings by Severity - WHAT / WHY / ACTION + 8-row sample tables
  4. Data Completeness   - Critical / Standard / Optional groups + score
  5. Distributions       - salary bar chart + status + dept + observations

Absolute rules:
  - NO em dashes anywhere in text (use hyphens)
  - All table cells use Paragraph() - never plain strings
  - Column widths must sum to exactly 526pt
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from io import BytesIO
from pathlib import Path

from reportlab.lib.colors import HexColor
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
# Page geometry  (0.6-inch margins -> TW = 526pt)
# ---------------------------------------------------------------------------
PAGE_W, PAGE_H = letter          # 612 x 792
LM = 43                          # left margin
RM = 43                          # right margin
TM = 43                          # top margin
BM = 43                          # bottom margin
TW = PAGE_W - LM - RM           # 526pt usable width
CONTENT_BOTTOM = PAGE_H - BM    # 749 - don't draw below this

# ---------------------------------------------------------------------------
# Design tokens  (exact spec values)
# ---------------------------------------------------------------------------
NAVY      = HexColor("#0A1628")
TEAL      = HexColor("#00C2CB")
WHITE     = HexColor("#FFFFFF")
OFF_WHITE = HexColor("#F4F7FA")
CHARCOAL  = HexColor("#1E293B")
MUTED     = HexColor("#64748B")
RED       = HexColor("#DC2626")
ORANGE    = HexColor("#EA580C")
AMBER     = HexColor("#D97706")
SLATE     = HexColor("#475569")
GREEN     = HexColor("#16A34A")
LIGHT_GRAY = HexColor("#E8EDF4")

SEVERITY_ORDER = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
SEV_COLOR = {
    "CRITICAL": RED,
    "HIGH":     ORANGE,
    "MEDIUM":   AMBER,
    "LOW":      SLATE,
}
SEV_LETTER = {"CRITICAL": "C", "HIGH": "H", "MEDIUM": "M", "LOW": "L"}

# Field groups for completeness page
CRITICAL_FIELDS = ["worker_id", "first_name", "last_name", "salary",
                   "worker_status", "hire_date", "email"]
STANDARD_FIELDS = ["department", "location", "manager_id", "phone",
                   "cost_center", "position"]
OPTIONAL_FIELDS = ["middle_name", "age", "date_of_birth", "performance_score",
                   "remote_work", "suffix", "notes"]

# ---------------------------------------------------------------------------
# Paragraph styles
# ---------------------------------------------------------------------------

def _ps(name: str, font: str = "Helvetica", size: int = 10,
        color=None, leading: int | None = None,
        align: int = TA_LEFT) -> ParagraphStyle:
    return ParagraphStyle(
        name,
        fontName=font,
        fontSize=size,
        textColor=color or CHARCOAL,
        leading=leading or max(12, int(size * 1.4)),
        alignment=align,
        wordWrap="LTR",
    )


PS_BODY      = _ps("body",      size=10, color=CHARCOAL, leading=14)
PS_BODY_MUT  = _ps("body_mut",  size=10, color=MUTED,    leading=14)
PS_BODY_WHT  = _ps("body_wht",  size=10, color=WHITE,    leading=14)
PS_CELL10    = _ps("cell10",    size=10, color=CHARCOAL, leading=14)
PS_CELL9     = _ps("cell9",     size=9,  color=CHARCOAL, leading=13)
PS_CELL9B    = _ps("cell9b",    font="Helvetica-Bold", size=9, color=CHARCOAL, leading=13)
PS_CELL9MUT  = _ps("cell9m",    size=9,  color=MUTED,    leading=13)
PS_CELL9WHT  = _ps("cell9wh",   font="Helvetica-Bold", size=9, color=WHITE, leading=13)
PS_CELL8     = _ps("cell8",     size=8,  color=CHARCOAL, leading=12)
PS_CELL8MUT  = _ps("cell8m",    size=8,  color=MUTED,    leading=12)
PS_CELL8WHT  = _ps("cell8wh",   font="Helvetica-Bold", size=8, color=WHITE, leading=12)
PS_GREEN     = _ps("green",     size=9,  color=GREEN,    leading=13)
PS_RED       = _ps("pred",      size=9,  color=RED,      leading=13)
PS_ORANGE    = _ps("porange",   size=9,  color=ORANGE,   leading=13)
PS_AMBER     = _ps("pamber",    size=9,  color=AMBER,    leading=13)
PS_SLATE     = _ps("pslate",    size=9,  color=SLATE,    leading=13)

SEV_PS = {
    "CRITICAL": PS_RED,
    "HIGH":     PS_ORANGE,
    "MEDIUM":   PS_AMBER,
    "LOW":      PS_SLATE,
}

# Singleton measurement canvas (never renders - height estimation only)
_MC_BUF: BytesIO | None = None
_MC = None


def _mc():
    global _MC_BUF, _MC
    if _MC is None:
        _MC_BUF = BytesIO()
        _MC = rl_canvas.Canvas(_MC_BUF, pagesize=letter)
    return _MC


def _ph(text: str, style: ParagraphStyle, width: float) -> float:
    """Estimate paragraph height."""
    p = Paragraph(str(text), style)
    _, h = p.wrapOn(_mc(), max(width, 1), 10000)
    return float(h)


# ---------------------------------------------------------------------------
# Drawing primitives  (y convention: from PAGE TOP downward)
# ---------------------------------------------------------------------------

def _rect(c, x: float, y_top: float, w: float, h: float,
          fill=None, stroke=None, sw: float = 0.5) -> None:
    rl_y = PAGE_H - y_top - h
    if fill:
        c.setFillColor(fill)
    if stroke:
        c.setStrokeColor(stroke)
        c.setLineWidth(sw)
    if fill and stroke:
        c.rect(x, rl_y, w, h, fill=1, stroke=1)
    elif fill:
        c.rect(x, rl_y, w, h, fill=1, stroke=0)
    elif stroke:
        c.rect(x, rl_y, w, h, fill=0, stroke=1)


def _hrule(c, y_top: float, x: float = LM, w: float = TW,
           color=TEAL, lw: float = 1.0) -> None:
    c.setStrokeColor(color)
    c.setLineWidth(lw)
    rl_y = PAGE_H - y_top
    c.line(x, rl_y, x + w, rl_y)


def _txt(c, x: float, y_base: float, text: str,
         font: str = "Helvetica", size: int = 10,
         color=CHARCOAL, align: str = "left") -> None:
    """Draw text.  y_base = distance from page top to text baseline."""
    c.setFillColor(color)
    c.setFont(font, size)
    rl_y = PAGE_H - y_base
    s = str(text)
    if align == "center":
        c.drawCentredString(x, rl_y, s)
    elif align == "right":
        c.drawRightString(x, rl_y, s)
    else:
        c.drawString(x, rl_y, s)


def _para(c, x: float, y_top: float, text: str,
          style: ParagraphStyle, width: float) -> float:
    """Draw paragraph. Returns new y_top (below paragraph)."""
    p = Paragraph(str(text), style)
    _, h = p.wrapOn(c, max(width, 1), 10000)
    rl_y = PAGE_H - y_top - h
    p.drawOn(c, x, rl_y)
    return y_top + h


# ---------------------------------------------------------------------------
# Table drawing  (all cells Paragraph - zero text overflow guaranteed)
# ---------------------------------------------------------------------------

def _table(c, x: float, y_top: float, col_widths: list[float],
           rows: list[list], hdr_bg=None, alt_bg=OFF_WHITE,
           row_bgs: list | None = None, pad: int = 6,
           font_size: int = 9, min_h: int = 20) -> float:
    """
    Draw table.  Every cell is a Paragraph - enabling word wrap.
    Column widths MUST sum to TW (526pt) or a subset.
    Returns new y_top after last row.
    """
    y = y_top
    for r_i, row in enumerate(rows):
        is_hdr = (r_i == 0)
        if row_bgs and r_i < len(row_bgs) and row_bgs[r_i] is not None:
            bg = row_bgs[r_i]
        elif is_hdr and hdr_bg:
            bg = hdr_bg
        elif r_i % 2 == 1:
            bg = alt_bg
        else:
            bg = WHITE

        cell_data: list[tuple] = []
        row_h = float(min_h)
        for c_i, (cell, cw) in enumerate(zip(row, col_widths)):
            if isinstance(cell, Paragraph):
                p = cell
            else:
                if is_hdr:
                    st = _ps(f"h{r_i}{c_i}", font="Helvetica-Bold",
                             size=font_size, color=WHITE,
                             leading=int(font_size * 1.4))
                else:
                    st = _ps(f"c{r_i}{c_i}", size=font_size,
                             color=CHARCOAL, leading=int(font_size * 1.4))
                p = Paragraph(str(cell) if cell is not None else "", st)
            inner_w = max(cw - pad * 2, 1)
            _, h = p.wrapOn(c, inner_w, 10000)
            row_h = max(row_h, h + pad * 2)
            cell_data.append((p, cw))

        x_pos = x
        for p, cw in cell_data:
            _rect(c, x_pos, y, cw, row_h, fill=bg, stroke=MUTED, sw=0.5)
            inner_w = max(cw - pad * 2, 1)
            _, h = p.wrapOn(c, inner_w, 10000)
            v_off = (row_h - h) / 2
            rl_y = PAGE_H - y - row_h + v_off
            p.drawOn(c, x_pos + pad, rl_y)
            x_pos += cw
        y += row_h
    return y


# ---------------------------------------------------------------------------
# Page header bar (navy, pages 2+)  and  footer
# ---------------------------------------------------------------------------

def _page_header(c, page_num: int, total_pages: int, title: str) -> float:
    """Draw page header bar. Returns y_top after bar (start of content)."""
    bar_h = 30
    _rect(c, 0, 0, PAGE_W, bar_h, fill=NAVY)
    _txt(c, LM, 21, title, font="Helvetica-Bold", size=11, color=WHITE)
    _txt(c, PAGE_W - RM, 21, f"Page {page_num} of {total_pages}",
         font="Helvetica-Bold", size=11, color=WHITE, align="right")
    return bar_h + 12


def _footer(c, page_num: int, total_pages: int, org_name: str, run_id: str) -> None:
    _txt(c, PAGE_W / 2, PAGE_H - BM / 2 + 8,
         f"CONFIDENTIAL - {org_name} - Internal Data Audit - Run {run_id} - Page {page_num} of {total_pages}",
         font="Helvetica", size=8, color=MUTED, align="center")


# ---------------------------------------------------------------------------
# Section heading within a page (teal left border + bold text)
# ---------------------------------------------------------------------------

def _section_heading(c, y_top: float, text: str, color=NAVY) -> float:
    """Draw section heading with 3pt teal left border. Returns new y."""
    _rect(c, LM, y_top, 3, 20, fill=TEAL)
    _txt(c, LM + 10, y_top + 15, text, font="Helvetica-Bold", size=14, color=color)
    return y_top + 28


# ---------------------------------------------------------------------------
# PAGE 1: COVER
# ---------------------------------------------------------------------------

def _draw_cover(c, run_id: str, summary: dict, org_name: str, date_str: str) -> None:
    """Full-page navy cover. All y coords are ReportLab (from bottom)."""
    cx = PAGE_W / 2
    counts = summary.get("severity_counts", {}) or {}

    # Full navy background
    c.setFillColor(NAVY)
    c.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)

    # ---------- TOP BRANDING AREA ----------
    accent_bar_rl = PAGE_H - 88
    # Teal accent bar (40pt x 4pt)
    c.setFillColor(TEAL)
    c.rect(cx - 20, accent_bar_rl, 40, 4, fill=1, stroke=0)
    # "DATA  WHISPERER" - 8pt below accent bar
    c.setFillColor(TEAL)
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(cx, accent_bar_rl - 16, "DATA  WHISPERER")

    # ---------- HEADLINE ----------
    hl_rl = PAGE_H - 178
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 38)
    c.drawCentredString(cx, hl_rl, "Internal HR Data Audit")
    c.drawCentredString(cx, hl_rl - 48, "Report")

    # Org name
    c.setFillColor(HexColor("#F4F7FA"))
    c.setFont("Helvetica", 16)
    c.drawCentredString(cx, hl_rl - 84, org_name)

    # Teal horizontal rule (2pt, full TW)
    rule_rl = hl_rl - 104
    c.setStrokeColor(TEAL)
    c.setLineWidth(2)
    c.line(LM, rule_rl, LM + TW, rule_rl)

    # ---------- METADATA BLOCK ----------
    meta_rl = rule_rl - 24
    filename = str(summary.get("source_filename", ""))
    total_rows = int(summary.get("total_rows", 0))
    c.setFillColor(HexColor("#64748B"))
    c.setFont("Helvetica", 11)
    c.drawCentredString(cx, meta_rl,       f"File Audited: {filename}")
    c.drawCentredString(cx, meta_rl - 19,  f"Records Analyzed: {total_rows:,}")
    c.drawCentredString(cx, meta_rl - 38,  f"Audit Date: {date_str}")
    c.drawCentredString(cx, meta_rl - 57,  f"Run ID: {run_id}")

    # ---------- 2x2 SEVERITY GRID ----------
    grid_top_rl = meta_rl - 88   # top of grid in RL coords
    box_w = (TW - 14) / 2        # ~256pt per box
    box_h = 88
    gap = 14
    sev_items = [
        ("CRITICAL", int(counts.get("CRITICAL", 0)), RED),
        ("HIGH",     int(counts.get("HIGH",     0)), ORANGE),
        ("MEDIUM",   int(counts.get("MEDIUM",   0)), AMBER),
        ("LOW",      int(counts.get("LOW",       0)), SLATE),
    ]
    for i, (sev, cnt, col) in enumerate(sev_items):
        c_col = i % 2
        c_row = i // 2
        bx = LM + c_col * (box_w + gap)
        by_top = grid_top_rl - c_row * (box_h + gap)   # RL y of box top
        by_bottom = by_top - box_h                       # RL y of box bottom

        # Box background (slightly lighter navy)
        c.setFillColor(HexColor("#0D1E32"))
        c.rect(bx, by_bottom, box_w, box_h, fill=1, stroke=0)
        # Top border - 3pt severity color
        c.setStrokeColor(col)
        c.setLineWidth(3)
        c.line(bx, by_top, bx + box_w, by_top)
        # Large count (44pt)
        c.setFillColor(col)
        c.setFont("Helvetica-Bold", 44)
        c.drawCentredString(bx + box_w / 2, by_bottom + box_h - 54, str(cnt))
        # Severity name
        c.setFillColor(HexColor("#64748B"))
        c.setFont("Helvetica", 10)
        c.drawCentredString(bx + box_w / 2, by_bottom + 22, sev)
        # "issues found"
        c.setFont("Helvetica", 9)
        c.drawCentredString(bx + box_w / 2, by_bottom + 9, "issues found")

    # ---------- BOTTOM FOOTER ----------
    bottom_rule_rl = 56
    c.setStrokeColor(TEAL)
    c.setLineWidth(1)
    c.line(LM, bottom_rule_rl, LM + TW, bottom_rule_rl)
    c.setFillColor(HexColor("#64748B"))
    c.setFont("Helvetica", 9)
    c.drawCentredString(cx, 40, "CONFIDENTIAL - For Internal Use Only")
    c.drawCentredString(cx, 26, "Generated by Data Whisperer Reconciliation Engine")


# ---------------------------------------------------------------------------
# Finding text lookup  (WHAT / WHY / ACTION per check_key)
# ---------------------------------------------------------------------------

def _finding_texts(check_key: str, finding: dict) -> tuple[str, str, str]:
    count = int(finding.get("count", 0))
    pct   = finding.get("pct", 0)
    field = finding.get("field", "")
    example      = finding.get("_example", "Dept-Region")
    unique_count = finding.get("_unique_count", "?")
    sample_val   = finding.get("_sample_value", "")

    lookup: dict[str, tuple[str, str, str]] = {
        "duplicate_worker_id": (
            f"{count:,} employee records share a Worker ID with at least one other employee. "
            "Worker IDs must be unique - each ID should belong to exactly one person.",
            "Duplicate IDs are the most serious data integrity failure in HR data. "
            "During migration, duplicate IDs cause corrections to be applied to the wrong person, "
            "create ambiguous matches, and may result in one employee's data overwriting another's.",
            "Identify the root cause of each duplicate - common causes include manual ID assignment "
            "errors, system import bugs, or incorrectly merged records. Assign a unique ID to each "
            "employee and update all references before using this data for migration or reporting.",
        ),
        "active_zero_salary": (
            f"{count:,} employees are marked Active but have no salary or a $0 salary recorded. "
            "These employees would receive no pay if this data were used for payroll processing.",
            "An active employee with no salary will not receive correct pay if this data is loaded "
            "into a new system. This represents both a data integrity failure and a direct payroll "
            "risk that can result in missed pay, employee complaints, and regulatory exposure.",
            f"Verify the correct salary for each of these {count:,} employees with their manager "
            "or payroll records. Enter the correct salary before this data is used for any "
            "operational purpose, migration, or reporting.",
        ),
        "phone_invalid": (
            f"{count:,} employee phone numbers contain impossible values "
            + (f"(sample: {sample_val}). " if sample_val else ". ")
            + "These cannot be real phone numbers - the file may have a data generation error.",
            "Invalid phone numbers prevent HR from contacting employees for urgent matters such as "
            "benefits updates, emergencies, or onboarding steps. They also indicate a systemic "
            "export error - if phone data is wrong, other numeric fields may also be unreliable.",
            "Re-export from the source system and verify the phone field is mapped correctly. "
            "If the source system also shows invalid values, the phone data needs to be "
            "re-collected from employees and entered before migration.",
        ),
        "duplicate_email": (
            f"{count:,} employees share an email address with at least one other employee. "
            "This indicates test or placeholder email addresses were used rather than real ones.",
            "Duplicate email addresses prevent system-generated notifications from reaching the "
            "right person. In most HRIS platforms, email is used as a unique key for self-service "
            "access. Duplicates will block portal access and system notifications for affected employees.",
            "Verify and assign a unique email address to each employee. If this file was generated "
            "for testing, replace placeholder addresses with real employee emails before using "
            "this data for any migration, reporting, or system load.",
        ),
        "duplicate_name_different_id": (
            f"{count:,} pairs of employees share the same full name but have different Worker IDs. "
            "These may be duplicate records for one person, or genuinely different people with the same name.",
            "Shared names with different IDs create identity matching risk during reconciliation. "
            "The matching engine may match the wrong employee, resulting in one person's "
            "corrections being applied to another.",
            "Review each flagged pair. If they represent the same person, merge the records and "
            "assign one unique ID. If they are different people, add a distinguishing identifier "
            "such as middle name, employee suffix, or date of birth to differentiate them.",
        ),
        "status_no_terminated": (
            "This file contains no terminated, separated, or resigned employees. "
            "Only Active, Inactive, and Pending statuses are present.",
            "Most complete workforce files include at least some terminated employees for compliance, "
            "historical reporting, or rehire eligibility tracking. A file with zero terminations "
            "may be missing a significant portion of the employee record history.",
            "Confirm whether this file is intentionally limited to non-terminated employees. "
            "If it should include full employment history, re-export from the source system with "
            "all statuses. If this scope is intentional, document it in the migration plan.",
        ),
        "status_high_pending": (
            f"{pct}% of employees ({count:,}) have a status of Pending. "
            "These employees have not had their employment status finalized in the source system.",
            "A high Pending rate indicates incomplete data entry or a bulk import that was not "
            "followed up. Pending employees cannot be reliably classified as active or inactive, "
            "which affects headcount reporting, payroll eligibility, and benefits enrollment.",
            f"Review all {count:,} Pending employees and update each to the correct final status: "
            "Active, Inactive, or Terminated. Do not migrate or use this data for reporting "
            "until all Pending statuses are resolved.",
        ),
        "age_uniformity": (
            f"Employee ages in this file show only {unique_count} distinct values across "
            f"{count:,} employees. Real workforce data has natural age variation - this pattern "
            "indicates placeholder ages were used rather than actual employee data.",
            "Placeholder ages produce incorrect results in any analysis involving age, tenure, "
            "benefits eligibility (which uses age thresholds), or retirement planning. "
            "If this data is used for compliance reporting, incorrect ages constitute "
            "inaccurate disclosure.",
            "Replace placeholder ages with actual employee date of birth or age data from the "
            "source system or employment records. If the source system does not capture age, "
            "establish a process to collect it before the next data cycle.",
        ),
        "combined_field": (
            f"The '{field}' column appears to combine two separate data values using a hyphen "
            f"separator (example: '{example}'). Two attributes are packed into one field rather "
            "than stored as separate columns.",
            "Combined fields cannot be filtered, grouped, or analyzed by just one of the two "
            "values without additional processing. Standard HRIS platforms such as Workday treat "
            "department and location as separate fields and cannot load a combined value directly.",
            "Split this column into two separate columns before use. Create one column for the "
            "value before the hyphen and a second column for the value after. Confirm the correct "
            "field mapping with the target system administrator before loading.",
        ),
        "hire_date_suspicious_default": (
            f"{count:,} employees have hire dates that appear to be system defaults rather than "
            "real hire dates. These values match known placeholder patterns.",
            "Default hire dates cause incorrect seniority calculations, wrong benefits eligibility "
            "determinations, and inaccurate tenure reporting. In many compliance frameworks, "
            "hire date accuracy is a legal requirement.",
            "Research the correct hire date for each flagged employee from offer letters, "
            "original employment records, or manager confirmation. Do not migrate records "
            "with default hire dates until each is verified.",
        ),
        "salary_suspicious_default": (
            f"{count:,} employees have salary values that match known system default amounts. "
            "These are likely placeholder values rather than real compensation figures.",
            "Default salaries loaded into a new system produce incorrect pay, wrong bonus "
            "calculations, and inaccurate compensation band analysis. This is both a data "
            "quality issue and a direct payroll risk.",
            "Verify the correct salary for each flagged employee from HR records or manager "
            "confirmation before proceeding with any migration or payroll processing.",
        ),
        "suspicious_round_salary": (
            f"{count:,} active employees have salary values that appear to be placeholder "
            "entries - highly round numbers repeated many times across the dataset.",
            "Placeholder salaries in a new system result in incorrect pay, wrong bonus "
            "calculations, and inaccurate compensation reporting. Even a small number of "
            "incorrect salaries can trigger audit flags and regulatory questions.",
            "Verify the correct salary for each flagged employee. Cross-reference with payroll "
            "records, offer letters, or the most recent compensation review before migrating.",
        ),
        "pay_equity_flag": (
            f"{count:,} role-department groups show salary variance above 30% within the "
            "same job title and department. These groups warrant review.",
            "Pay equity variance above 30% within the same role may indicate potential "
            "discriminatory compensation patterns. This analysis does not include demographic "
            "data, but identifies statistical anomalies that require investigation before "
            "the data is certified as accurate.",
            "Review each flagged role-department group. Verify that salary differences reflect "
            "legitimate factors such as seniority, performance, or location differentials. "
            "Document the rationale for any variance exceeding 30% before submitting this "
            "dataset for compliance purposes.",
        ),
        "ghost_employee_indicator": (
            f"{count:,} records show indicators consistent with ghost employees: active status "
            "combined with no salary, no department, and no manager assignment.",
            "Ghost employees are fictitious or former employees who continue to draw pay. "
            "Records matching this profile have characteristics consistent with payroll fraud "
            "indicators or severe data entry errors that could result in unauthorized payments.",
            "Investigate each flagged record immediately. Confirm employment status with "
            "direct managers and payroll records. Remove or correct any record that cannot "
            "be verified as a legitimate active employee before any system migration.",
        ),
        "missing_manager": (
            f"{count:,} active employees have no manager assigned in this file.",
            "Employees without a manager assignment cannot be correctly placed in the "
            "organizational hierarchy. This affects reporting structures, approval workflows, "
            "and the ability to route HR actions to the correct approver.",
            "Assign a valid manager ID to each active employee without one. For executives "
            "or top-level employees who genuinely have no manager, document this in the "
            "migration design and configure the target system accordingly.",
        ),
        "impossible_dates": (
            f"{count:,} date values in this file are impossible or internally inconsistent - "
            "such as future hire dates, dates before 1950, or a date of birth after the hire date.",
            "Impossible dates cause calculation errors in seniority, benefits eligibility, "
            "and compliance reporting. They also indicate data quality problems in the source "
            "system that may affect other fields not checked by this audit.",
            "Correct each impossible date by reviewing the employee's source records. "
            "If the source system contains the same error, trace it back to the original "
            "data entry and correct at the source before any downstream use.",
        ),
    }

    default = (
        str(finding.get("description", "Issue detected requiring review.")),
        "This issue may affect data integrity and should be reviewed before migration or reporting.",
        "Review and correct all flagged records before using this data for any operational purpose.",
    )
    return lookup.get(check_key, default)


# ---------------------------------------------------------------------------
# PAGE 2: EXECUTIVE SUMMARY
# ---------------------------------------------------------------------------

def _draw_exec_summary(c, y_start: float, page_num: int, total_pages: int,
                       summary: dict, org_name: str, run_id: str,
                       date_str: str) -> None:
    y = y_start
    counts     = summary.get("severity_counts", {}) or {}
    total_rows = int(summary.get("total_rows", 0))
    col_count  = int(summary.get("total_columns", 0))
    findings   = [f for f in (summary.get("findings_for_pdf") or [])
                  if int(f.get("count", 0)) > 0]
    filename   = str(summary.get("source_filename", ""))
    critical   = int(counts.get("CRITICAL", 0))
    high       = int(counts.get("HIGH", 0))
    medium     = int(counts.get("MEDIUM", 0))

    # ---- What We Found ----
    y = _section_heading(c, y, "What We Found")
    y += 4

    # Build narrative
    narrative = (
        f"This audit analyzed <b>{total_rows:,}</b> employee records from <b>{filename}</b> "
        f"on {date_str}. The engine ran automated checks across <b>{col_count}</b> data fields, "
        "examining every record for data integrity issues, completeness gaps, impossible values, "
        "and patterns that indicate placeholder or corrupt data.<br/><br/>"
    )
    if critical > 0:
        narrative += (
            f"<b>{critical} critical issue{'s were' if critical > 1 else ' was'} found</b> "
            "requiring immediate attention before this data can be used for payroll processing, "
            "migration, or compliance reporting. "
        )
    if high > 0:
        narrative += (
            f"<b>{high} high-severity finding{'s' if high > 1 else ''}</b> should be resolved "
            "before this data is used for any downstream system load or reporting. "
        )
    if medium > 0:
        narrative += (
            f"<b>{medium} medium-severity finding{'s' if medium > 1 else ''}</b> represent data "
            "quality improvements that would strengthen the reliability of this dataset. "
        )
    if critical == 0 and high == 0:
        narrative += (
            "<b>No critical or high severity issues were identified.</b> This dataset shows "
            "acceptable data quality for the checks performed. "
        )
    y = _para(c, LM, y, narrative, PS_BODY, TW)
    y += 14

    # ---- Audit Scope table ----
    # Widths: 140 + 260 + 126 = 526pt
    y = _section_heading(c, y, "Audit Scope")
    y += 6

    def _scope_result(check_keys, label_pass="PASS"):
        for ck in check_keys:
            for f in findings:
                if f.get("check_key") == ck:
                    cnt = int(f.get("count", 0))
                    return Paragraph(f"{cnt:,} found", SEV_PS.get(f.get("severity", "LOW"), PS_CELL9))
        return Paragraph(label_pass, PS_GREEN)

    scope_cw = [140, 260, 126]   # = 526 exactly
    scope_rows = [
        [
            Paragraph("<b>Check Category</b>", PS_CELL9WHT),
            Paragraph("<b>What Was Checked</b>", PS_CELL9WHT),
            Paragraph("<b>Result</b>", PS_CELL9WHT),
        ],
        [
            Paragraph("Identity Integrity", PS_CELL9),
            Paragraph("Duplicate Worker IDs, missing IDs", PS_CELL9),
            _scope_result(["duplicate_worker_id", "missing_worker_id"]),
        ],
        [
            Paragraph("Contact Data", PS_CELL9),
            Paragraph("Duplicate emails, invalid phone numbers", PS_CELL9),
            _scope_result(["duplicate_email", "phone_invalid"]),
        ],
        [
            Paragraph("Employment Status", PS_CELL9),
            Paragraph("Status values, active/$0 salary, pending rate", PS_CELL9),
            _scope_result(["active_zero_salary", "status_no_terminated", "status_high_pending"]),
        ],
        [
            Paragraph("Compensation", PS_CELL9),
            Paragraph("Missing salary, suspicious defaults, outliers", PS_CELL9),
            _scope_result(["salary_suspicious_default", "suspicious_round_salary", "salary_outlier"]),
        ],
        [
            Paragraph("Demographic Data", PS_CELL9),
            Paragraph("Age completeness and variation, date validity", PS_CELL9),
            _scope_result(["age_uniformity", "impossible_dates"]),
        ],
        [
            Paragraph("Data Completeness", PS_CELL9),
            Paragraph("Blank fields across all columns", PS_CELL9),
            Paragraph(
                f"{summary.get('overall_completeness', 0):.1f}% complete",
                PS_GREEN if float(summary.get("overall_completeness", 0)) >= 95 else PS_AMBER,
            ),
        ],
        [
            Paragraph("Org Structure", PS_CELL9),
            Paragraph("Manager assignment, reporting loops", PS_CELL9),
            _scope_result(["missing_manager", "manager_loop"]),
        ],
        [
            Paragraph("Pay Equity", PS_CELL9),
            Paragraph("Salary variance within same role and department", PS_CELL9),
            _scope_result(["pay_equity_flag"]),
        ],
    ]
    y = _table(c, LM, y, scope_cw, scope_rows, hdr_bg=NAVY, font_size=9)
    y += 14

    # ---- Files in This Report ----
    y = _section_heading(c, y, "Files Included in This Download Package")
    y += 6
    file_lines = [
        "- Internal Audit Report (this document) - PDF",
        "- Internal Audit Data - full findings dataset - CSV",
        "- Data Completeness Breakdown - per-field analysis - CSV",
        "- Salary and Status Distributions - CSV",
        "- Suspicious Default Values - CSV",
    ]
    for line in file_lines:
        y = _para(c, LM + 10, y, line, PS_BODY_MUT, TW - 10)
        y += 2


# ---------------------------------------------------------------------------
# PAGE 3: FINDINGS BY SEVERITY
# ---------------------------------------------------------------------------

def _draw_findings(c, y_start: float, page_num: int, total_pages: int,
                   summary: dict, org_name: str, run_id: str) -> tuple[int, float]:
    """
    Draw findings. Handles page overflow.
    Returns (final_page_num, final_y).
    """
    y = y_start
    findings = [f for f in (summary.get("findings_for_pdf") or [])
                if int(f.get("count", 0)) > 0]
    # Group by severity
    by_sev: dict[str, list] = {s: [] for s in SEVERITY_ORDER}
    for f in findings:
        sev = str(f.get("severity", "LOW")).upper()
        by_sev.setdefault(sev, []).append(f)

    def _start_new_findings_page():
        nonlocal page_num, y
        c.showPage()
        page_num += 1
        y = _page_header(c, page_num, total_pages,
                         "FINDINGS BY SEVERITY (continued)")
        _footer(c, page_num, total_pages, org_name, run_id)

    first_sev_section = True
    for sev in SEVERITY_ORDER:
        sev_findings = by_sev.get(sev, [])
        if not sev_findings:
            continue

        # Teal rule between severity sections
        if not first_sev_section:
            if y + 30 > CONTENT_BOTTOM - 80:
                _start_new_findings_page()
            else:
                _hrule(c, y + 6, color=TEAL, lw=1)
                y += 18
        first_sev_section = False

        # ---- Severity section header bar (24pt, severity color) ----
        sev_col  = SEV_COLOR[sev]
        sev_ltr  = SEV_LETTER[sev]
        tot_recs = sum(int(f.get("count", 0)) for f in sev_findings)
        n_issues = len(sev_findings)

        # Need room for at least the header bar + one finding stub
        if y + 24 + 60 > CONTENT_BOTTOM:
            _start_new_findings_page()

        bar_y = y
        _rect(c, LM, bar_y, TW, 24, fill=sev_col)
        # Circle with letter
        circle_cx = LM + 14
        circle_cy_rl = PAGE_H - bar_y - 12
        c.setFillColor(HexColor("#FFFFFF"))
        c.circle(circle_cx, circle_cy_rl, 9, fill=1, stroke=0)
        c.setFillColor(sev_col)
        c.setFont("Helvetica-Bold", 10)
        c.drawCentredString(circle_cx, circle_cy_rl - 4, sev_ltr)
        # Title
        _txt(c, LM + 30, bar_y + 16,
             f"{sev} FINDINGS",
             font="Helvetica-Bold", size=11, color=WHITE)
        # Right: count
        _txt(c, LM + TW - 4, bar_y + 16,
             f"{n_issues} issue{'s' if n_issues != 1 else ''} - {tot_recs:,} records affected",
             font="Helvetica", size=10, color=WHITE, align="right")
        y = bar_y + 24 + 8

        # ---- Individual findings ----
        for finding in sev_findings:
            check_key  = str(finding.get("check_key", ""))
            check_name = str(finding.get("check_name", ""))
            count      = int(finding.get("count", 0))
            sample     = (finding.get("sample_rows") or [])[:8]
            what, why, action = _finding_texts(check_key, finding)

            # Estimate height before drawing
            est_h = (
                20    # check name heading
                + 12  # WHAT label
                + _ph(what, PS_BODY, TW - 20) + 4
                + 12  # WHY label
                + _ph(why, PS_BODY, TW - 20) + 4
                + 12  # ACTION label
                + _ph(action, PS_BODY, TW - 20) + 10
                + (min(len(sample), 8) + 1) * 22  # table
                + 16  # footer line
                + 18  # bottom spacing
            )
            if y + est_h > CONTENT_BOTTOM and y > y_start + 60:
                _start_new_findings_page()

            block_y = y
            # Left severity border on finding block
            _rect(c, LM, block_y, 3, est_h + 4, fill=sev_col)
            cx_inner = LM + 12
            inner_w  = TW - 12

            # Check name heading
            y = block_y + 4
            _txt(c, cx_inner, y + 14, check_name,
                 font="Helvetica-Bold", size=13, color=NAVY)
            y += 22

            # WHAT WAS FOUND
            _txt(c, cx_inner, y + 10, "WHAT WAS FOUND",
                 font="Helvetica-Bold", size=9, color=sev_col)
            y += 14
            y = _para(c, cx_inner, y, what, PS_BODY, inner_w)
            y += 6

            # WHY THIS MATTERS
            _txt(c, cx_inner, y + 10, "WHY THIS MATTERS",
                 font="Helvetica-Bold", size=9, color=sev_col)
            y += 14
            y = _para(c, cx_inner, y, why, PS_BODY, inner_w)
            y += 6

            # RECOMMENDED ACTION
            _txt(c, cx_inner, y + 10, "RECOMMENDED ACTION",
                 font="Helvetica-Bold", size=9, color=sev_col)
            y += 14
            y = _para(c, cx_inner, y, action, PS_BODY, inner_w)
            y += 10

            # Sample records table
            if sample:
                _txt(c, cx_inner, y + 10,
                     f"Sample Affected Records ({len(sample)} of {count:,} shown)",
                     font="Helvetica-Bold", size=9, color=MUTED)
                y += 14
                # Build sample table columns from available keys
                sample_keys = [k for k in sample[0].keys()
                               if k not in ("row_number", "why_flagged",
                                            "note", "median_salary", "repeated_count",
                                            "variance_pct", "employees",
                                            "min_salary", "max_salary", "median_salary",
                                            "cycle_length", "employee_ids")][:5]
                if not sample_keys:
                    sample_keys = list(sample[0].keys())[:5]
                # Add row# col
                all_keys = ["#"] + sample_keys
                n_cols = len(all_keys)
                # Calculate column widths: first col is narrow
                first_cw = 30
                rest_cw = round((inner_w - first_cw) / max(n_cols - 1, 1))
                # Adjust last to sum exactly
                cw_list = [first_cw] + [rest_cw] * (n_cols - 2) + [
                    inner_w - first_cw - rest_cw * max(n_cols - 2, 0)
                ]
                cw_list = [max(cw, 20) for cw in cw_list]
                # Header row
                hdr_row = [Paragraph(f"<b>{k.replace('_', ' ').title()}</b>", PS_CELL8WHT)
                           for k in all_keys]
                tbl_rows = [hdr_row]
                for sr in sample:
                    row_cells = [Paragraph(str(sr.get("row_number", "")), PS_CELL8)]
                    for k in sample_keys:
                        val = str(sr.get(k, ""))
                        if len(val) > 40:
                            val = val[:37] + "..."
                        row_cells.append(Paragraph(val, PS_CELL8))
                    tbl_rows.append(row_cells)
                y = _table(c, cx_inner, y, cw_list, tbl_rows,
                           hdr_bg=NAVY, font_size=8, pad=4, min_h=16)
                y += 4

            # Footer line
            _txt(c, cx_inner, y + 10,
                 f"Full findings available in internal_audit_data.csv ({count:,} records)",
                 font="Helvetica-Oblique", size=9, color=MUTED)
            y += 18

    return page_num, y


# ---------------------------------------------------------------------------
# PAGE 4: DATA COMPLETENESS ANALYSIS
# ---------------------------------------------------------------------------

def _draw_completeness(c, y_start: float, page_num: int, total_pages: int,
                        summary: dict) -> None:
    y = y_start
    total_rows  = int(summary.get("total_rows", 0))
    comp_rows   = summary.get("completeness_rows") or []
    overall_pct = float(summary.get("overall_completeness", 0))

    # Build lookup: field -> completeness row
    comp_map: dict[str, dict] = {}
    for row in comp_rows:
        comp_map[str(row.get("field", "")).lower()] = row

    # Intro paragraph
    intro = (
        f"This section measures how completely each field is populated across all "
        f"<b>{total_rows:,}</b> employee records. Fields are grouped by their importance "
        "to HR operations. Critical fields drive payroll, identity matching, and compliance "
        "reporting - any gap in these fields requires immediate attention."
    )
    y = _para(c, LM, y, intro, PS_BODY, TW)
    y += 16

    def _status_cell(blank_pct: float) -> Paragraph:
        bp = float(blank_pct)
        filled = 100.0 - bp
        if filled >= 99:
            return Paragraph("Complete", PS_GREEN)
        if filled >= 95:
            return Paragraph("Acceptable", PS_AMBER)
        if filled >= 80:
            return Paragraph("Needs Attention", PS_ORANGE)
        return Paragraph("Critical Gap", PS_RED)

    def _comp_table(c, y_top, title, fields, hdr_color=NAVY):
        # Header for the group
        _rect(c, LM, y_top, 3, 16, fill=hdr_color)
        _txt(c, LM + 10, y_top + 12, title,
             font="Helvetica-Bold", size=11, color=NAVY)
        y2 = y_top + 22
        # Widths: 150 + 130 + 110 + 136 = 526pt
        cw = [150, 130, 110, 136]
        hdr = [
            Paragraph("<b>Field Name</b>", PS_CELL9WHT),
            Paragraph("<b>Records Complete</b>", PS_CELL9WHT),
            Paragraph("<b>Blank Count</b>", PS_CELL9WHT),
            Paragraph("<b>Status</b>", PS_CELL9WHT),
        ]
        tbl = [hdr]
        for field in fields:
            row_data = comp_map.get(field.lower(), {})
            total_f  = int(row_data.get("total", total_rows) or total_rows)
            blank_c  = int(row_data.get("blank_count", 0) or 0)
            blank_pct = float(row_data.get("blank_pct", 0.0) or 0.0)
            filled_c = total_f - blank_c
            tbl.append([
                Paragraph(field, PS_CELL9),
                Paragraph(f"{filled_c:,} of {total_f:,}", PS_CELL9),
                Paragraph(str(blank_c), PS_CELL9),
                _status_cell(blank_pct),
            ])
        return _table(c, LM, y2, cw, tbl, hdr_bg=NAVY, font_size=9)

    y = _comp_table(c, y, "Critical Fields", CRITICAL_FIELDS)
    y += 14

    if y + 160 > CONTENT_BOTTOM:
        return   # safety - won't overflow for typical files

    y = _comp_table(c, y, "Standard Fields", STANDARD_FIELDS)
    y += 14

    # Optional fields - only show ones that exist in the file
    present_optional = [f for f in OPTIONAL_FIELDS if f.lower() in comp_map]
    if present_optional:
        y = _comp_table(c, y, "Optional Fields", present_optional)
        y += 14

    # ---- Overall completeness score ----
    y += 10
    score_color = GREEN if overall_pct >= 95 else (AMBER if overall_pct >= 85 else RED)
    # Center the large score
    _txt(c, PAGE_W / 2, y + 54, f"{overall_pct:.1f}%",
         font="Helvetica-Bold", size=48, color=score_color, align="center")
    _txt(c, PAGE_W / 2, y + 70, "Overall Data Completeness Score",
         font="Helvetica-Bold", size=12, color=CHARCOAL, align="center")
    n_fields = len(comp_rows)
    _txt(c, PAGE_W / 2, y + 85,
         f"Based on {n_fields} fields across {total_rows:,} records",
         font="Helvetica", size=10, color=MUTED, align="center")
    y += 100

    # Interpretation
    if overall_pct >= 95:
        interp = (
            f"An overall completeness of {overall_pct:.1f}% meets the generally accepted "
            "threshold of 95%+ for critical field completeness in HRIS migration readiness. "
            "Focus remaining effort on resolving any Critical field gaps flagged above."
        )
    elif overall_pct >= 85:
        interp = (
            f"An overall completeness of {overall_pct:.1f}% falls below the recommended 95% "
            "threshold. Targeted data collection and cleanup is required before this file "
            "meets migration readiness standards for most enterprise HRIS platforms."
        )
    else:
        interp = (
            f"An overall completeness of {overall_pct:.1f}% is below acceptable levels for "
            "any production migration. Significant data remediation is required. This dataset "
            "should not be used for any operational purpose until completeness is improved."
        )
    y = _para(c, LM, y, interp, PS_BODY_MUT, TW)


# ---------------------------------------------------------------------------
# PAGE 5: DATA DISTRIBUTIONS AND PATTERNS
# ---------------------------------------------------------------------------

def _draw_distributions(c, y_start: float, page_num: int, total_pages: int,
                         summary: dict, dist_data: list[dict]) -> None:
    y = y_start
    total_rows    = int(summary.get("total_rows", 0))
    status_bdown  = summary.get("status_breakdown") or {}
    salary_stats  = summary.get("salary_stats") or {}
    comp_rows     = summary.get("completeness_rows") or []

    # Parse distributions CSV data
    salary_buckets = [r for r in dist_data if r.get("section") == "SALARY DISTRIBUTION"]

    # ---- SALARY DISTRIBUTION ----
    y = _section_heading(c, y, "Salary Distribution")
    y += 4

    sal_count   = int(salary_stats.get("count", 0))
    sal_missing = int(salary_stats.get("missing", 0))

    if salary_buckets and sal_count > 0:
        intro_sal = (
            f"Salary data across {sal_count:,} employees shows the following distribution. "
            f"{sal_missing:,} records ({salary_stats.get('missing_pct', 0):.1f}%) "
            "have missing salary and are excluded from this analysis."
        )
        y = _para(c, LM, y, intro_sal, PS_BODY, TW)
        y += 8

        # Draw bar chart
        chart_h  = 110
        bar_area_x = LM + 48    # room for y-axis label
        bar_area_w = TW - 48
        n_bars     = len(salary_buckets)
        max_count  = max(int(b.get("count", 0)) for b in salary_buckets) or 1

        bar_group_w = bar_area_w / n_bars
        bar_w       = bar_group_w * 0.68
        bar_offset  = (bar_group_w - bar_w) / 2
        chart_bottom_y = y + chart_h    # y_top of chart bottom axis

        teal_shades = [
            HexColor("#006169"),
            HexColor("#007E87"),
            HexColor("#009BA5"),
            HexColor("#00B5BF"),
            HexColor("#00C2CB"),
            HexColor("#33CFD5"),
        ]

        # Y-axis
        _hrule(c, chart_bottom_y, x=bar_area_x, w=bar_area_w, color=MUTED, lw=0.5)
        # Y-axis label
        _txt(c, LM, y + chart_h / 2 + 12, "Count",
             font="Helvetica", size=8, color=MUTED, align="right")

        for i, bucket in enumerate(salary_buckets):
            cnt    = int(bucket.get("count", 0))
            pct    = float(bucket.get("pct", 0))
            bar_h  = (cnt / max_count) * (chart_h - 24) if max_count > 0 else 0
            bx     = bar_area_x + i * bar_group_w + bar_offset
            bar_y  = chart_bottom_y - bar_h   # y_top of bar

            bar_col = teal_shades[min(i, len(teal_shades) - 1)]
            if bar_h > 0:
                _rect(c, bx, bar_y, bar_w, bar_h, fill=bar_col)
            # Count label above bar
            if cnt > 0:
                _txt(c, bx + bar_w / 2, bar_y - 3,
                     str(cnt), font="Helvetica-Bold", size=7,
                     color=CHARCOAL, align="center")
                _txt(c, bx + bar_w / 2, bar_y + 9,
                     f"{pct:.0f}%", font="Helvetica", size=6,
                     color=MUTED, align="center")
            # Bucket label
            label = str(bucket.get("bucket", ""))
            _txt(c, bx + bar_w / 2, chart_bottom_y + 12,
                 label, font="Helvetica", size=7, color=MUTED, align="center")

        y = chart_bottom_y + 26

        # Salary stats table: 190 + 336 = 526pt
        sal_cw = [190, 336]
        sal_hdr = [
            Paragraph("<b>Metric</b>", PS_CELL9WHT),
            Paragraph("<b>Value</b>", PS_CELL9WHT),
        ]
        sal_rows_data = [
            ("Total employees with salary data", f"{sal_count:,}"),
            ("Missing salary", f"{sal_missing:,} ({salary_stats.get('missing_pct', 0):.1f}%)"),
            ("Minimum",        f"${salary_stats.get('min', 0):,.0f}"),
            ("Maximum",        f"${salary_stats.get('max', 0):,.0f}"),
            ("Mean",           f"${salary_stats.get('mean', 0):,.0f}"),
            ("Median",         f"${salary_stats.get('median', 0):,.0f}"),
            ("Employees earning under $50k",
             f"{salary_stats.get('under_50k', 0):,} ({salary_stats.get('under_50k_pct', 0):.1f}%)"),
            ("Employees earning over $150k",
             f"{salary_stats.get('over_150k', 0):,} ({salary_stats.get('over_150k_pct', 0):.1f}%)"),
        ]
        tbl = [sal_hdr] + [
            [Paragraph(k, PS_CELL9), Paragraph(v, PS_CELL9)]
            for k, v in sal_rows_data
        ]
        y = _table(c, LM, y, sal_cw, tbl, hdr_bg=NAVY, font_size=9)
        y += 14
    else:
        y = _para(c, LM, y, "No salary data present in this file.", PS_BODY_MUT, TW)
        y += 10

    # ---- EMPLOYMENT STATUS BREAKDOWN ----
    if y + 100 > CONTENT_BOTTOM:
        return  # skip if no room

    y = _section_heading(c, y, "Employment Status Breakdown")
    y += 6

    if status_bdown:
        total_status = sum(int(v) for v in status_bdown.values())
        # Sort by count desc
        sorted_status = sorted(status_bdown.items(), key=lambda x: int(x[1]), reverse=True)

        # Widths: 170 + 90 + 90 + 176 = 526pt
        st_cw = [170, 90, 90, 176]
        st_hdr = [
            Paragraph("<b>Status</b>",     PS_CELL9WHT),
            Paragraph("<b>Count</b>",      PS_CELL9WHT),
            Paragraph("<b>Percentage</b>", PS_CELL9WHT),
            Paragraph("<b>Distribution</b>", PS_CELL9WHT),
        ]
        st_rows = [st_hdr]
        for status_val, cnt in sorted_status[:12]:
            cnt   = int(cnt)
            pct   = round(cnt / total_status * 100, 1) if total_status else 0.0
            bar_w_pts = int(min(pct / 100 * 150, 150))
            bar_html  = (
                f'<font color="#00C2CB">{"| " * max(bar_w_pts // 8, 1)}</font>'
                f' {pct:.1f}%'
            )
            st_rows.append([
                Paragraph(str(status_val).title(), PS_CELL9),
                Paragraph(f"{cnt:,}", PS_CELL9),
                Paragraph(f"{pct:.1f}%", PS_CELL9),
                Paragraph(bar_html, PS_CELL9),
            ])
        y = _table(c, LM, y, st_cw, st_rows, hdr_bg=NAVY, font_size=9)
        y += 10

        # Status observations
        status_lower = {str(k).lower(): int(v) for k, v in status_bdown.items()}
        has_terminated = any(k in status_lower for k in
                              ["terminated", "term", "separated", "resigned"])
        pending_pct = round(
            status_lower.get("pending", 0) / total_status * 100, 1
        ) if total_status else 0.0
        active_pct = round(
            status_lower.get("active", 0) / total_status * 100, 1
        ) if total_status else 0.0

        obs = []
        if not has_terminated:
            obs.append("No terminated employees are present in this file.")
        if pending_pct > 25:
            obs.append(f"{pending_pct:.1f}% of records have Pending status - this is unusually high.")
        if active_pct < 50:
            obs.append(f"Active employees represent only {active_pct:.1f}% of records.")
        if obs:
            for ob in obs:
                y = _para(c, LM, y, f"- {ob}", PS_BODY_MUT, TW)
                y += 2
        y += 8

    # ---- KEY OBSERVATIONS ----
    if y + 80 > CONTENT_BOTTOM:
        return

    y = _section_heading(c, y, "Key Observations")
    y += 6

    comp_map = {str(r.get("field", "")).lower(): r for r in comp_rows}
    findings = [f for f in (summary.get("findings_for_pdf") or [])
                if int(f.get("count", 0)) > 0]
    total_issues = len(findings)
    critical_ct  = int((summary.get("severity_counts") or {}).get("CRITICAL", 0))
    high_ct      = int((summary.get("severity_counts") or {}).get("HIGH", 0))
    overall_comp = float(summary.get("overall_completeness", 0))

    observations = []

    if salary_stats.get("count"):
        sal_mean   = salary_stats.get("mean", 0)
        sal_min    = salary_stats.get("min", 0)
        sal_max    = salary_stats.get("max", 0)
        observations.append(
            f"Salary levels range from ${sal_min:,.0f} to ${sal_max:,.0f} "
            f"with a mean of ${sal_mean:,.0f}."
        )

    if total_issues > 0:
        observations.append(
            f"{total_issues} distinct issue types were identified across {total_rows:,} records. "
            f"{'Immediate action is required for critical and high findings.' if (critical_ct + high_ct) > 0 else ''}"
        )

    if overall_comp < 95:
        observations.append(
            f"Overall field completeness of {overall_comp:.1f}% is below the 95% migration "
            "readiness threshold. Data collection is needed before this file is production-ready."
        )
    else:
        observations.append(
            f"Overall field completeness of {overall_comp:.1f}% meets migration readiness standards."
        )

    # Phone completeness
    phone_row = comp_map.get("phone", {})
    if phone_row:
        phone_blank = int(phone_row.get("blank_count", 0) or 0)
        if phone_blank > 0:
            phone_pct = float(phone_row.get("blank_pct", 0) or 0)
            observations.append(
                f"{phone_pct:.1f}% of employee phone numbers are missing "
                f"({phone_blank:,} records)."
            )

    # Email completeness
    email_row = comp_map.get("email", {})
    if email_row:
        email_blank = int(email_row.get("blank_count", 0) or 0)
        if email_blank > 0:
            email_pct = float(email_row.get("blank_pct", 0) or 0)
            observations.append(
                f"{email_pct:.1f}% of employee email addresses are missing "
                f"({email_blank:,} records)."
            )

    for i, obs_text in enumerate(observations[:6]):
        y = _para(c, LM, y, f"- {obs_text}", PS_BODY, TW)
        y += 4


# ---------------------------------------------------------------------------
# Main render engine (two-pass for correct page totals)
# ---------------------------------------------------------------------------

def _render(output, run_id: str, summary: dict, org_name: str,
            total_pages: int, date_str: str, dist_data: list[dict]) -> int:
    """Render all pages. Returns total page count."""
    # Accept either a file path (str/Path) or a BytesIO for dry-run counting
    c = rl_canvas.Canvas(output, pagesize=letter)
    page_num = 1

    # ---- PAGE 1: COVER ----
    _draw_cover(c, run_id, summary, org_name, date_str)
    c.showPage()
    page_num += 1

    # ---- PAGE 2: EXECUTIVE SUMMARY ----
    y = _page_header(c, page_num, total_pages, "EXECUTIVE SUMMARY")
    _footer(c, page_num, total_pages, org_name, run_id)
    _draw_exec_summary(c, y, page_num, total_pages, summary, org_name, run_id, date_str)
    c.showPage()
    page_num += 1

    # ---- PAGE 3+: FINDINGS BY SEVERITY ----
    y = _page_header(c, page_num, total_pages, "FINDINGS BY SEVERITY")
    _footer(c, page_num, total_pages, org_name, run_id)
    page_num, _y = _draw_findings(c, y, page_num, total_pages, summary, org_name, run_id)
    c.showPage()
    page_num += 1

    # ---- PAGE N: DATA COMPLETENESS ----
    y = _page_header(c, page_num, total_pages, "DATA COMPLETENESS ANALYSIS")
    _footer(c, page_num, total_pages, org_name, run_id)
    _draw_completeness(c, y, page_num, total_pages, summary)
    c.showPage()
    page_num += 1

    # ---- PAGE N+1: DISTRIBUTIONS ----
    y = _page_header(c, page_num, total_pages, "DATA DISTRIBUTIONS AND PATTERNS")
    _footer(c, page_num, total_pages, org_name, run_id)
    _draw_distributions(c, y, page_num, total_pages, summary, dist_data)
    # No showPage - last page, c.save() finalizes it

    c.save()
    return page_num


def build_pdf(run_id: str, run_dir: Path, out_path: Path) -> int:
    """Load audit data and render 5-page PDF. Returns actual page count."""
    json_path = run_dir / "internal_audit_report.json"
    summary   = json.loads(json_path.read_text(encoding="utf-8"))

    # Load distributions CSV
    dist_data: list[dict] = []
    dist_path = run_dir / "internal_audit_distributions.csv"
    if dist_path.exists():
        with dist_path.open(encoding="utf-8") as f:
            dist_data = list(csv.DictReader(f))

    # Load org name from policy
    org_name = "Your Organization"
    try:
        policy   = load_policy(ROOT / "config" / "policy.yaml")
        org_name = str(
            (policy.get("client") or {}).get("name")
            or policy.get("client_name")
            or "Your Organization"
        )
    except Exception:
        pass

    date_str = datetime.now().strftime("%B %d, %Y")

    # Pass 1: render to memory to count pages
    buf1          = BytesIO()
    actual_pages  = _render(buf1, run_id, summary, org_name, 99, date_str, dist_data)

    # Pass 2: render to real file with correct total
    _render(str(out_path), run_id, summary, org_name, actual_pages, date_str, dist_data)
    print(f"[build_report] wrote: {out_path} ({actual_pages} pages)")
    return actual_pages


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Build internal audit PDF report")
    parser.add_argument("--run-id",  required=True,  help="Run ID")
    parser.add_argument("--run-dir", required=True,  help="Directory with audit artifacts")
    parser.add_argument("--out",     required=True,  help="Output PDF path")
    args = parser.parse_args()
    build_pdf(args.run_id, Path(args.run_dir), Path(args.out))


if __name__ == "__main__":
    main()
