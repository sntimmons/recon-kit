"""
styles.py - ReportLab ParagraphStyle definitions for the Data Whisperer PDF.

All styles are defined once at module level and imported by components.py
and report.py.  Never create ad-hoc styles in drawing functions.
"""
from __future__ import annotations

from io import BytesIO

from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.platypus import Paragraph

from .constants import (
    FONT_REGULAR, FONT_BOLD, FONT_OBLIQUE,
    FONT_BODY, FONT_SMALL, FONT_META,
    COLOR_CHARCOAL, COLOR_WHITE, COLOR_MID_GRAY,
    COLOR_NAVY, COLOR_TEAL,
    COLOR_CRITICAL, COLOR_HIGH, COLOR_MEDIUM, COLOR_LOW,
    COLOR_BLOCKED, COLOR_PASS, COLOR_WARN, COLOR_FAIL,
    COLOR_APPROVED, COLOR_REVIEW, COLOR_REJECTED, COLOR_HELD,
)


def _ps(name: str,
        font: str = FONT_REGULAR,
        size: float = FONT_BODY,
        color=None,
        leading: float | None = None,
        align: int = TA_LEFT) -> ParagraphStyle:
    """ParagraphStyle factory - keeps constructor calls DRY."""
    return ParagraphStyle(
        name,
        fontName=font,
        fontSize=size,
        textColor=color or COLOR_CHARCOAL,
        leading=leading or max(12.0, size * 1.42),
        alignment=align,
        wordWrap="LTR",
    )


# ---------------------------------------------------------------------------
# Body / narrative
# ---------------------------------------------------------------------------
PS_BODY      = _ps("ps_body",      size=9,  color=COLOR_CHARCOAL, leading=13)
PS_BODY_MUT  = _ps("ps_body_mut",  size=9,  color=COLOR_MID_GRAY, leading=13)
PS_BODY_WHT  = _ps("ps_body_wht",  size=9,  color=COLOR_WHITE,    leading=13)
PS_BODY_CTR  = _ps("ps_body_ctr",  size=9,  color=COLOR_CHARCOAL, leading=13, align=TA_CENTER)
PS_BODY_B    = _ps("ps_body_b",    font=FONT_BOLD, size=9, color=COLOR_CHARCOAL, leading=13)

# ---------------------------------------------------------------------------
# Table header cells (navy background, white bold text)
# ---------------------------------------------------------------------------
PS_HDR       = _ps("ps_hdr",       font=FONT_BOLD, size=9,  color=COLOR_WHITE,    leading=13)
PS_HDR_SM    = _ps("ps_hdr_sm",    font=FONT_BOLD, size=8,  color=COLOR_WHITE,    leading=12)

# ---------------------------------------------------------------------------
# Table data cells
# ---------------------------------------------------------------------------
PS_CELL      = _ps("ps_cell",      size=9,  color=COLOR_CHARCOAL, leading=13)
PS_CELL_MUT  = _ps("ps_cell_mut",  size=9,  color=COLOR_MID_GRAY, leading=13)
PS_CELL_B    = _ps("ps_cell_b",    font=FONT_BOLD, size=9, color=COLOR_CHARCOAL, leading=13)
PS_CELL_SM   = _ps("ps_cell_sm",   size=8,  color=COLOR_CHARCOAL, leading=12)
PS_CELL_SMWT = _ps("ps_cell_smwt", font=FONT_BOLD, size=8, color=COLOR_WHITE,    leading=12)
PS_CELL_WHT  = _ps("ps_cell_wht",  font=FONT_BOLD, size=9, color=COLOR_WHITE,    leading=13)

# ---------------------------------------------------------------------------
# Severity-colored text
# ---------------------------------------------------------------------------
PS_CRITICAL  = _ps("ps_crit",  size=9, color=COLOR_CRITICAL, leading=13)
PS_HIGH      = _ps("ps_high",  size=9, color=COLOR_HIGH,     leading=13)
PS_MEDIUM    = _ps("ps_med",   size=9, color=COLOR_MEDIUM,   leading=13)
PS_LOW       = _ps("ps_low",   size=9, color=COLOR_LOW,      leading=13)
PS_BLOCKED   = _ps("ps_blk",   size=9, color=COLOR_BLOCKED,  leading=13)
PS_PASS      = _ps("ps_pass",  size=9, color=COLOR_PASS,     leading=13)
PS_WARN      = _ps("ps_warn",  size=9, color=COLOR_WARN,     leading=13)
PS_FAIL      = _ps("ps_fail",  size=9, color=COLOR_FAIL,     leading=13)
PS_NAVY      = _ps("ps_navy",  size=9, color=COLOR_NAVY,     leading=13)

# ---------------------------------------------------------------------------
# Captions / footnotes
# ---------------------------------------------------------------------------
PS_CAPTION   = _ps("ps_caption",   font=FONT_OBLIQUE, size=7.5,
                    color=COLOR_MID_GRAY, leading=10)
PS_CAPTION_C = _ps("ps_caption_c", font=FONT_OBLIQUE, size=7.5,
                    color=COLOR_MID_GRAY, leading=10, align=TA_CENTER)
PS_CAPTION_R = _ps("ps_caption_r", font=FONT_OBLIQUE, size=7.5,
                    color=COLOR_MID_GRAY, leading=10, align=TA_RIGHT)

# ---------------------------------------------------------------------------
# Measurement canvas (singleton - used by para_height())
# ---------------------------------------------------------------------------
_MC_BUF: BytesIO | None = None
_MC = None


def _mc():
    global _MC_BUF, _MC
    if _MC is None:
        _MC_BUF = BytesIO()
        _MC = rl_canvas.Canvas(_MC_BUF, pagesize=letter)
    return _MC


def para_height(text: str, style: ParagraphStyle, width: float) -> float:
    """Estimate the rendered height of a paragraph without drawing it."""
    p = Paragraph(str(text), style)
    _, h = p.wrapOn(_mc(), max(width, 1.0), 10_000)
    return float(h)
