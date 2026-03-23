"""
report_theme.py - Shared design tokens for Data Whisperer audit reports.

Imported by:
  - build_recon_report.py  (ReportLab / PDF)
  - generate_report.py     (python-docx / Word -> PDF)

Design system
-------------
  Primary:    NAVY      #0A1628  (page backgrounds, table headers, header bars)
  Accent:     TEAL      #00C2CB  (section borders, rules, brand highlights)
  Critical:   RED       #DC2626  (active/$0 salary, identity blocks, CRITICAL findings)
  High:       ORANGE    #EA580C  (salary mismatches, wrong matches, HIGH findings)
  Medium:     AMBER     #D97706  (review-queue items, hire-date, MEDIUM findings)
  Low:        SLATE     #475569  (held records, low-priority, muted labels)
  Positive:   GREEN     #16A34A  (Safe / auto-approved, ok status, passing checks)
  Body text:  CHARCOAL  #1E293B
  Muted text: MUTED     #64748B
  Alt row bg: OFF_WHITE #F4F7FA
  Light rule: LIGHT_GRAY #E8EDF4

Severity thresholds (priority_score)
-------------------------------------
  CRITICAL  >= 70
  HIGH      >= 40
  MEDIUM    >= 20
  LOW        < 20
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Hex palette  (single source of truth for ALL report outputs)
# ---------------------------------------------------------------------------
HEX_NAVY       = "#0A1628"
HEX_TEAL       = "#00C2CB"
HEX_WHITE      = "#FFFFFF"
HEX_OFF_WHITE  = "#F4F7FA"
HEX_CHARCOAL   = "#1E293B"
HEX_MUTED      = "#64748B"
HEX_SLATE      = "#475569"
HEX_RED        = "#DC2626"
HEX_ORANGE     = "#EA580C"
HEX_AMBER      = "#D97706"
HEX_GREEN      = "#16A34A"
HEX_LIGHT_GRAY = "#E8EDF4"

# Bare hex strings without '#' (used by python-docx _set_cell_bg)
BARE_NAVY       = "0A1628"
BARE_TEAL       = "00C2CB"
BARE_WHITE      = "FFFFFF"
BARE_OFF_WHITE  = "F4F7FA"
BARE_CHARCOAL   = "1E293B"
BARE_MUTED      = "64748B"
BARE_SLATE      = "475569"
BARE_RED        = "DC2626"
BARE_ORANGE     = "EA580C"
BARE_AMBER      = "D97706"
BARE_GREEN      = "16A34A"
BARE_LIGHT_GRAY = "E8EDF4"

# ---------------------------------------------------------------------------
# Severity system
# ---------------------------------------------------------------------------
# Each entry: label, hex color, bare hex, priority_score threshold
SEVERITY_LEVELS: list[dict] = [
    {"label": "CRITICAL", "hex": HEX_RED,    "bare": BARE_RED,    "threshold": 70},
    {"label": "HIGH",     "hex": HEX_ORANGE, "bare": BARE_ORANGE, "threshold": 40},
    {"label": "MEDIUM",   "hex": HEX_AMBER,  "bare": BARE_AMBER,  "threshold": 20},
    {"label": "LOW",      "hex": HEX_SLATE,  "bare": BARE_SLATE,  "threshold":  0},
]


def severity_for_score(score: float) -> dict:
    """Return severity dict matching a numeric priority_score."""
    for s in SEVERITY_LEVELS:
        if score >= s["threshold"]:
            return s
    return SEVERITY_LEVELS[-1]  # LOW


def severity_for_color(hex_color: str) -> str:
    """Return severity label for a brand color hex (with or without #)."""
    h = hex_color.lstrip("#").upper()
    mapping = {
        BARE_RED.upper():    "CRITICAL",
        BARE_ORANGE.upper(): "HIGH",
        BARE_AMBER.upper():  "MEDIUM",
        BARE_SLATE.upper():  "LOW",
        BARE_GREEN.upper():  "OK",
        BARE_TEAL.upper():   "INFO",
    }
    return mapping.get(h, "")


# ---------------------------------------------------------------------------
# Action -> label mapping (recon engine action codes)
# ---------------------------------------------------------------------------
ACTION_LABELS: dict[str, str] = {
    "APPROVE":      "Safe",
    "REVIEW":       "Needs Review",
    "REJECT_MATCH": "Wrong Match",
    "HELD":         "Held",
}

# Fix-type pipe-separated fields -> human labels
FIX_TYPE_LABELS: dict[str, str] = {
    "salary":    "Salary",
    "payrate":   "Pay Rate",
    "status":    "Status",
    "hire_date": "Hire Date",
    "job_org":   "Job / Org",
}


def fmt_fix_types(raw: str) -> str:
    """Convert pipe-separated fix_types to comma-separated human labels."""
    if not raw or str(raw).strip() in ("", "nan", "None"):
        return "No Changes"
    parts = [FIX_TYPE_LABELS.get(p.strip(), p.strip().title())
             for p in str(raw).split("|") if p.strip()]
    return ", ".join(parts) if parts else "No Changes"


# ---------------------------------------------------------------------------
# Typography scale  (font sizes in points)
# ---------------------------------------------------------------------------
FONT_COVER_TITLE  = 36
FONT_COVER_SUB    = 16
FONT_PAGE_HEADER  = 11
FONT_SECTION_HEAD = 14
FONT_BODY         = 10
FONT_TABLE_HDR    = 9
FONT_TABLE_CELL   = 9
FONT_TABLE_SMALL  = 8
FONT_FOOTER       = 8
FONT_META         = 11
FONT_BADGE        = 8

# ---------------------------------------------------------------------------
# Layout constants (letter page, 0.6-inch margins)
# ---------------------------------------------------------------------------
PAGE_W     = 612
PAGE_H     = 792
MARGIN     = 43
TEXT_WIDTH = PAGE_W - MARGIN * 2   # 526 pt

# ---------------------------------------------------------------------------
# Shared footer / branding text
# ---------------------------------------------------------------------------
FOOTER_CONFIDENTIAL = "CONFIDENTIAL - For Internal Use Only"
FOOTER_GENERATED_BY = "Generated by Data Whisperer Reconciliation Engine"

# ---------------------------------------------------------------------------
# Match source -> human-readable label
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
# ReportLab color factory  (lazy import - only available when reportlab installed)
# ---------------------------------------------------------------------------

def rl_palette() -> dict:
    """Return brand palette as reportlab HexColor objects."""
    from reportlab.lib.colors import HexColor  # type: ignore
    return {
        "NAVY":       HexColor(HEX_NAVY),
        "TEAL":       HexColor(HEX_TEAL),
        "WHITE":      HexColor(HEX_WHITE),
        "OFF_WHITE":  HexColor(HEX_OFF_WHITE),
        "CHARCOAL":   HexColor(HEX_CHARCOAL),
        "MUTED":      HexColor(HEX_MUTED),
        "SLATE":      HexColor(HEX_SLATE),
        "RED":        HexColor(HEX_RED),
        "ORANGE":     HexColor(HEX_ORANGE),
        "AMBER":      HexColor(HEX_AMBER),
        "GREEN":      HexColor(HEX_GREEN),
        "LIGHT_GRAY": HexColor(HEX_LIGHT_GRAY),
    }


# ---------------------------------------------------------------------------
# python-docx color factory  (lazy import - only available when python-docx installed)
# ---------------------------------------------------------------------------

def docx_palette() -> dict:
    """Return brand palette as python-docx RGBColor objects."""
    from docx.shared import RGBColor  # type: ignore

    def _rgb(h: str) -> "RGBColor":
        h = h.lstrip("#")
        return RGBColor(int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))

    return {
        "NAVY":       _rgb(HEX_NAVY),
        "TEAL":       _rgb(HEX_TEAL),
        "WHITE":      _rgb(HEX_WHITE),
        "OFF_WHITE":  _rgb(HEX_OFF_WHITE),
        "CHARCOAL":   _rgb(HEX_CHARCOAL),
        "MUTED":      _rgb(HEX_MUTED),
        "SLATE":      _rgb(HEX_SLATE),
        "RED":        _rgb(HEX_RED),
        "ORANGE":     _rgb(HEX_ORANGE),
        "AMBER":      _rgb(HEX_AMBER),
        "GREEN":      _rgb(HEX_GREEN),
        "LIGHT_GRAY": _rgb(HEX_LIGHT_GRAY),
    }
