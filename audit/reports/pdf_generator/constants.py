"""
constants.py - Design system constants for the Data Whisperer Reconciliation Audit PDF.

Single source of truth for ALL colors, fonts, and layout dimensions.
Both components.py and report.py import exclusively from here.

Color system (matches Internal HR Data Audit Report exactly)
------------------------------------------------------------
  Primary:    NAVY       #1B2A4A   headers, cover background
  Critical:   CRITICAL   #C0392B   deep red - active/$0, identity blocks
  High:       HIGH       #E67E22   orange   - salary, low-confidence
  Medium:     MEDIUM     #2980B9   blue     - hire date, payrate
  Low:        LOW        #27AE60   green    - passing, auto-approved
  Blocked:    BLOCKED    #6C3483   purple   - rejected/wrong-match
  Gate PASS:  PASS       #1E8449   dark green
  Gate WARN:  WARN       #D4AC0D   amber
  Gate FAIL:  FAIL       #922B21   dark red
"""
from __future__ import annotations

from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch

# ---------------------------------------------------------------------------
# Page geometry
# ---------------------------------------------------------------------------
PAGE_W, PAGE_H = letter          # 612 x 792 pt (letter)

LEFT_MARGIN   = 0.65 * inch      # 46.8 pt
RIGHT_MARGIN  = 0.65 * inch
TOP_MARGIN    = 0.75 * inch      # 54 pt
BOTTOM_MARGIN = 0.65 * inch      # 46.8 pt
FOOTER_HEIGHT = 0.4  * inch      # 28.8 pt

LM = LEFT_MARGIN
RM = RIGHT_MARGIN
TM = TOP_MARGIN
BM = BOTTOM_MARGIN
FH = FOOTER_HEIGHT

TEXT_WIDTH     = PAGE_W - LM - RM          # ~518 pt
TW             = TEXT_WIDTH

# Y bounds for page content (y_top convention = distance from page top)
PAGE_HEADER_H  = 32                        # height of the running page header bar
CONTENT_TOP    = PAGE_HEADER_H + 14        # start of content area below header
CONTENT_BOTTOM = PAGE_H - BM - FH - 8     # top of footer zone

# Cover page: top 42% is navy, rest is white
COVER_NAVY_H   = int(PAGE_H * 0.42)        # ~332 pt

# ---------------------------------------------------------------------------
# Brand palette
# ---------------------------------------------------------------------------
COLOR_NAVY       = HexColor("#1B2A4A")   # primary backgrounds, headers
COLOR_WHITE      = HexColor("#FFFFFF")
COLOR_LIGHT_GRAY = HexColor("#F5F6F8")   # alternating table rows, section bg
COLOR_MID_GRAY   = HexColor("#8A9BB0")   # secondary text, footer labels
COLOR_BORDER     = HexColor("#D0D7E2")   # table borders, dividers
COLOR_CHARCOAL   = HexColor("#1E293B")   # primary body text

# Severity badge colors (must match internal audit exactly)
COLOR_CRITICAL   = HexColor("#C0392B")   # deep red
COLOR_HIGH       = HexColor("#E67E22")   # orange
COLOR_MEDIUM     = HexColor("#2980B9")   # blue
COLOR_LOW        = HexColor("#27AE60")   # green
COLOR_BLOCKED    = HexColor("#6C3483")   # purple - rejected / blocked
COLOR_PASS       = HexColor("#1E8449")   # dark green - gate PASS
COLOR_WARN       = HexColor("#D4AC0D")   # amber - gate WARNING
COLOR_FAIL       = HexColor("#922B21")   # dark red - gate BLOCKED/FAIL

# Derived / shorthand
COLOR_APPROVED   = COLOR_PASS
COLOR_REVIEW     = COLOR_WARN
COLOR_REJECTED   = COLOR_BLOCKED
COLOR_HELD       = HexColor("#475569")   # slate - held records

# Teal accent (thin rules, left borders on section headings)
COLOR_TEAL       = HexColor("#00C2CB")

# Light tints for callout backgrounds (10% opacity of the base color)
COLOR_BLOCKED_TINT = HexColor("#F0E6FF")  # light purple callout bg

# ---------------------------------------------------------------------------
# Typography scale (pt)
# ---------------------------------------------------------------------------
FONT_H1    = 22    # cover main title
FONT_H2    = 14    # section headers
FONT_H3    = 11    # subsection headers
FONT_BODY  = 9     # body text, table content
FONT_SMALL = 7.5   # footer, captions, "N of M shown"
FONT_BADGE = 8     # severity badge letter (C / H / M / L / B)
FONT_META  = 10    # metadata / cover stats

FONT_REGULAR = "Helvetica"
FONT_BOLD    = "Helvetica-Bold"
FONT_OBLIQUE = "Helvetica-Oblique"

# ---------------------------------------------------------------------------
# Action labels and colors (recon engine action codes -> display)
# ---------------------------------------------------------------------------
ACTION_LABELS: dict[str, str] = {
    "APPROVE":      "Safe",
    "REVIEW":       "Needs Review",
    "REJECT_MATCH": "Wrong Match",
    "HELD":         "Held",
}

ACTION_COLORS: dict[str, object] = {
    "APPROVE":      COLOR_APPROVED,
    "REVIEW":       COLOR_REVIEW,
    "REJECT_MATCH": COLOR_REJECTED,
    "HELD":         COLOR_HELD,
}

# ---------------------------------------------------------------------------
# Match source labels
# ---------------------------------------------------------------------------
MATCH_SOURCE_LABELS: dict[str, str] = {
    "worker_id":      "Exact (Worker ID)",
    "pk":             "Name + DOB + Last4",
    "last4_dob":      "Last4 SSN + DOB",
    "dob_name":       "Name + DOB (fuzzy)",
    "name_hire_date": "Name + Hire Date",
    "recon_id":       "Recon ID",
}

# ---------------------------------------------------------------------------
# Fix-type labels
# ---------------------------------------------------------------------------
FIX_TYPE_LABELS: dict[str, str] = {
    "salary":    "Salary",
    "payrate":   "Pay Rate",
    "status":    "Status",
    "hire_date": "Hire Date",
    "job_org":   "Job / Org",
}

# ---------------------------------------------------------------------------
# Severity system (score-based + finding-type-based)
# ---------------------------------------------------------------------------
SEVERITY_LEVELS: list[dict] = [
    {"label": "CRITICAL", "letter": "C", "color": COLOR_CRITICAL, "threshold": 70},
    {"label": "HIGH",     "letter": "H", "color": COLOR_HIGH,     "threshold": 40},
    {"label": "MEDIUM",   "letter": "M", "color": COLOR_MEDIUM,   "threshold": 20},
    {"label": "LOW",      "letter": "L", "color": COLOR_LOW,      "threshold":  0},
]
# Special: rejected/blocked records always use BLOCKED badge
SEVERITY_BLOCKED = {"label": "BLOCKED", "letter": "B", "color": COLOR_BLOCKED, "threshold": -1}


def severity_for_score(score: float) -> dict:
    """Return severity dict for a numeric priority score."""
    for s in SEVERITY_LEVELS:
        if score >= s["threshold"]:
            return s
    return SEVERITY_LEVELS[-1]


# ---------------------------------------------------------------------------
# Gate status
# ---------------------------------------------------------------------------

def get_gate_status(safe_pct: float, n_az: int, n_rejected: int) -> tuple[str, object]:
    """
    Return (label, color) for the sanity gate badge.
    BLOCKED if any active/$0 or rejected records exist.
    PASS if approval rate >= 80%.
    WARNING otherwise.
    """
    if n_az > 0 or n_rejected > 0:
        return "BLOCKED", COLOR_FAIL
    elif safe_pct >= 80.0:
        return "PASS", COLOR_PASS
    else:
        return "WARNING", COLOR_WARN


# ---------------------------------------------------------------------------
# Migration readiness score (0-100)
# ---------------------------------------------------------------------------

def migration_readiness_score(safe_pct: float, n_az: int,
                               n_rejected: int) -> int:
    """
    Compute a 0-100 migration readiness score.

    Base  = safe_pct (already as a percentage, e.g. 79.4)
    Deductions (capped at 45 total):
      -10  per rejected/wrong-match pair
      -min(n_az * 0.1, 15)  active/$0 salary employees (capped at 15 pts)
      -2   if safe_pct < 80.0
    Score  = max(0, min(100, base - deductions))
    """
    base       = float(safe_pct)
    deductions = 0.0
    deductions += n_rejected * 10
    deductions += min(n_az * 0.1, 15.0)   # cap zero-salary penalty at 15 pts
    deductions += 2.0 if safe_pct < 80.0 else 0.0
    deductions  = min(deductions, 45.0)   # total deduction cap
    return max(0, min(100, int(round(base - deductions))))


# ---------------------------------------------------------------------------
# Branding text
# ---------------------------------------------------------------------------
FOOTER_CONFIDENTIAL = "CONFIDENTIAL - For Internal Use Only"
FOOTER_GENERATED_BY = "Generated by Data Whisperer Reconciliation Engine"
REPORT_TITLE        = "Reconciliation Audit Report"
REPORT_SUBTITLE     = "ADP to Workday Migration"
BRAND_NAME          = "DATA  WHISPERER"
