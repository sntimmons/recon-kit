"""
components.py - Reusable drawing functions for the Data Whisperer PDF.

All functions use the y_top convention: y_top is the distance from the
PAGE TOP downward (i.e. 0 = top of page, PAGE_H = bottom of page).
ReportLab uses bottom-up coordinates internally; conversions are handled here.

Public drawing functions:
    draw_rect(c, x, y_top, w, h, fill, stroke, sw, radius)
    draw_hrule(c, y_top, x, w, color, lw)
    draw_text(c, x, y_base, text, font, size, color, align)
    draw_para(c, x, y_top, text, style, width)  -> float
    draw_table(c, x, y_top, col_widths, rows, ...)  -> float
    draw_page_header(c, page_num, total_pages, section_name, org_name)  -> float
    draw_footer(c, page_num, total_pages, org_name, run_id)
    draw_section_header(c, y_top, text, color)  -> float
    draw_severity_badge(c, x, y_center, sev_dict)
    draw_finding_block(c, y, finding, page_num, total_pages, org_name, run_id, overflow_cb)  -> (page_num, float)
    draw_sample_table(c, x, y_top, inner_w, sample_rows)  -> float
    draw_callout_box(c, x, y_top, w, text, border_color, bg_color, style)  -> float
    draw_stat_boxes(c, x, y_top, stats, box_w, box_h, gap)  -> float
    draw_gate_badge(c, x_right, y_top, label, color)
    draw_readiness_bar(c, x, y_top, score, w, h)  -> float
    draw_cover_page(c, run_id, summary, org_name, date_str)
    draw_bar_chart(c, x, y_top, w, h, categories, values, bar_color, max_label_w)  -> float
"""
from __future__ import annotations

from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import Paragraph

from .constants import (
    PAGE_W, PAGE_H, LM, RM, TW, TM, BM, FH,
    PAGE_HEADER_H, CONTENT_TOP, CONTENT_BOTTOM,
    COVER_NAVY_H,
    COLOR_NAVY, COLOR_WHITE, COLOR_LIGHT_GRAY, COLOR_MID_GRAY,
    COLOR_BORDER, COLOR_CHARCOAL, COLOR_TEAL,
    COLOR_CRITICAL, COLOR_HIGH, COLOR_MEDIUM, COLOR_LOW, COLOR_BLOCKED,
    COLOR_PASS, COLOR_WARN, COLOR_FAIL,
    COLOR_APPROVED, COLOR_REVIEW, COLOR_REJECTED, COLOR_HELD,
    COLOR_BLOCKED_TINT,
    FONT_REGULAR, FONT_BOLD, FONT_OBLIQUE,
    FONT_H1, FONT_H2, FONT_H3, FONT_BODY, FONT_SMALL, FONT_BADGE, FONT_META,
    SEVERITY_BLOCKED,
    FOOTER_CONFIDENTIAL, FOOTER_GENERATED_BY,
    BRAND_NAME, REPORT_TITLE, REPORT_SUBTITLE,
    get_gate_status, migration_readiness_score,
)
from .styles import (
    PS_BODY, PS_BODY_MUT, PS_BODY_WHT, PS_BODY_B,
    PS_HDR, PS_HDR_SM,
    PS_CELL, PS_CELL_MUT, PS_CELL_B, PS_CELL_SM, PS_CELL_SMWT, PS_CELL_WHT,
    PS_CRITICAL, PS_HIGH, PS_MEDIUM, PS_LOW, PS_BLOCKED,
    PS_PASS, PS_WARN, PS_FAIL, PS_NAVY,
    PS_CAPTION, PS_CAPTION_C,
    para_height,
)


# ===========================================================================
# LOW-LEVEL PRIMITIVES
# ===========================================================================

def draw_rect(c, x: float, y_top: float, w: float, h: float,
              fill=None, stroke=None, sw: float = 0.5,
              radius: float = 0.0) -> None:
    """Draw a rectangle. y_top = distance from page top."""
    rl_y = PAGE_H - y_top - h
    if fill:
        c.setFillColor(fill)
    if stroke:
        c.setStrokeColor(stroke)
        c.setLineWidth(sw)
    if radius > 0:
        if fill and stroke:
            c.roundRect(x, rl_y, w, h, radius, fill=1, stroke=1)
        elif fill:
            c.roundRect(x, rl_y, w, h, radius, fill=1, stroke=0)
        elif stroke:
            c.roundRect(x, rl_y, w, h, radius, fill=0, stroke=1)
    else:
        if fill and stroke:
            c.rect(x, rl_y, w, h, fill=1, stroke=1)
        elif fill:
            c.rect(x, rl_y, w, h, fill=1, stroke=0)
        elif stroke:
            c.rect(x, rl_y, w, h, fill=0, stroke=1)


def draw_hrule(c, y_top: float,
               x: float = LM, w: float = TW,
               color=COLOR_BORDER, lw: float = 0.5) -> None:
    """Draw a horizontal rule."""
    c.setStrokeColor(color)
    c.setLineWidth(lw)
    rl_y = PAGE_H - y_top
    c.line(x, rl_y, x + w, rl_y)


def draw_text(c, x: float, y_base: float, text: str,
              font: str = FONT_REGULAR, size: float = FONT_BODY,
              color=COLOR_CHARCOAL, align: str = "left") -> None:
    """
    Draw plain text.
    y_base = distance from PAGE TOP to the text baseline.
    """
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


def draw_para(c, x: float, y_top: float, text: str,
              style: ParagraphStyle, width: float) -> float:
    """
    Draw a paragraph. Returns new y_top (bottom edge of rendered text).
    """
    p = Paragraph(str(text), style)
    _, h = p.wrapOn(c, max(width, 1.0), 10_000)
    rl_y = PAGE_H - y_top - h
    p.drawOn(c, x, rl_y)
    return y_top + h


def draw_table(c, x: float, y_top: float,
               col_widths: list[float],
               rows: list[list],
               hdr_bg=None,
               alt_bg=COLOR_LIGHT_GRAY,
               row_bgs: list | None = None,
               pad: int = 6,
               font_size: int = 9,
               min_h: int = 22,
               stroke_color=COLOR_BORDER,
               stroke_w: float = 0.4) -> float:
    """
    Draw a data table. Every cell must be a Paragraph or will be wrapped.
    Column widths must sum to TW or a contiguous subset.
    Returns new y_top after last row.
    """
    y = y_top
    for r_i, row in enumerate(rows):
        is_hdr = (r_i == 0)

        # Determine row background
        if row_bgs and r_i < len(row_bgs) and row_bgs[r_i] is not None:
            bg = row_bgs[r_i]
        elif is_hdr and hdr_bg:
            bg = hdr_bg
        elif r_i % 2 == 1:
            bg = alt_bg
        else:
            bg = COLOR_WHITE

        # Measure all cells to find row height
        cell_items: list[tuple] = []
        row_h = float(min_h)
        for c_i, (cell, cw) in enumerate(zip(row, col_widths)):
            if isinstance(cell, Paragraph):
                p = cell
            else:
                if is_hdr:
                    st = PS_HDR if font_size >= 9 else PS_HDR_SM
                else:
                    from reportlab.lib.styles import ParagraphStyle as _PS
                    from reportlab.lib.enums import TA_LEFT as _TAL
                    st = _PS(
                        f"_tbl_{r_i}_{c_i}",
                        fontName=FONT_REGULAR,
                        fontSize=font_size,
                        textColor=COLOR_CHARCOAL,
                        leading=max(11, int(font_size * 1.4)),
                        alignment=_TAL,
                        wordWrap="LTR",
                    )
                p = Paragraph(str(cell) if cell is not None else "", st)
            inner_w = max(cw - pad * 2, 1.0)
            _, h = p.wrapOn(c, inner_w, 10_000)
            row_h = max(row_h, h + pad * 2)
            cell_items.append((p, cw))

        # Draw cells
        x_pos = x
        for p, cw in cell_items:
            draw_rect(c, x_pos, y, cw, row_h, fill=bg,
                      stroke=stroke_color, sw=stroke_w)
            inner_w = max(cw - pad * 2, 1.0)
            _, h = p.wrapOn(c, inner_w, 10_000)
            v_off = (row_h - h) / 2
            rl_y = PAGE_H - y - row_h + v_off
            p.drawOn(c, x_pos + pad, rl_y)
            x_pos += cw

        y += row_h
    return y


# ===========================================================================
# PAGE CHROME (header bar, footer, section headings)
# ===========================================================================

def draw_page_header(c, page_num: int, total_pages: int,
                     section_name: str, org_name: str) -> float:
    """
    Draw the running page header bar (navy, every non-cover page).
    Returns y_top of the content area below the bar.
    """
    bar_h = PAGE_HEADER_H
    draw_rect(c, 0, 0, PAGE_W, bar_h, fill=COLOR_NAVY)
    # Left: brand name
    draw_text(c, LM, bar_h - 10,
              BRAND_NAME, font=FONT_BOLD, size=8, color=COLOR_WHITE)
    # Right: section name
    draw_text(c, PAGE_W - RM, bar_h - 10,
              section_name, font=FONT_REGULAR, size=8,
              color=COLOR_MID_GRAY, align="right")
    # Thin border line below header
    draw_hrule(c, bar_h, x=0, w=PAGE_W, color=COLOR_BORDER, lw=0.5)
    return CONTENT_TOP


def draw_footer(c, page_num: int, total_pages: int,
                org_name: str, run_id: str) -> None:
    """
    Draw the two-line running footer (every non-cover page).
    Contains confidential notice, org, run ID, and page count.
    """
    footer_top = PAGE_H - BM - FH + 4
    draw_hrule(c, footer_top, x=0, w=PAGE_W, color=COLOR_BORDER, lw=0.4)
    left_text  = f"{FOOTER_CONFIDENTIAL}  |  {org_name}  |  Run {run_id}"
    right_text = f"Page {page_num} of {total_pages}"
    y_base = footer_top + FH / 2 + 4
    draw_text(c, LM, y_base, left_text,
              font=FONT_REGULAR, size=FONT_SMALL, color=COLOR_MID_GRAY)
    draw_text(c, PAGE_W - RM, y_base, right_text,
              font=FONT_REGULAR, size=FONT_SMALL,
              color=COLOR_MID_GRAY, align="right")
    draw_text(c, LM, y_base + 10, FOOTER_GENERATED_BY,
              font=FONT_REGULAR, size=FONT_SMALL - 0.5, color=COLOR_MID_GRAY)


def draw_section_header(c, y_top: float, text: str,
                        color=COLOR_NAVY) -> float:
    """
    Draw a section heading with a 3pt teal left-border accent.
    Returns new y_top below the heading.
    """
    h = 24
    draw_rect(c, LM, y_top, 3, h, fill=COLOR_TEAL)
    draw_text(c, LM + 10, y_top + h - 7,
              text, font=FONT_BOLD, size=FONT_H2, color=color)
    return y_top + h + 6


# ===========================================================================
# SEVERITY BADGE  (colored square with white letter)
# ===========================================================================

def draw_severity_badge(c, x: float, y_center: float,
                        sev_dict: dict, size: float = 18) -> None:
    """
    Draw a severity badge: colored rounded square with white letter.
    x = left edge of badge, y_center = vertical center of badge.
    """
    col    = sev_dict["color"]
    letter = sev_dict["letter"]
    y_top  = y_center - size / 2
    draw_rect(c, x, y_top, size, size, fill=col, radius=3.0)
    # Center letter horizontally and vertically
    draw_text(c, x + size / 2, y_top + size - 5,
              letter, font=FONT_BOLD, size=FONT_BADGE,
              color=COLOR_WHITE, align="center")


# ===========================================================================
# CALLOUT BOX  (colored left border, optional tinted background)
# ===========================================================================

def draw_callout_box(c, x: float, y_top: float, w: float,
                     text: str, border_color,
                     bg_color=None, style=None) -> float:
    """
    Draw a callout box with a 3pt left border.
    Returns new y_top below the box.
    """
    st    = style or PS_BODY
    inner = w - 3 - 12  # 3pt border + 12pt padding
    th    = para_height(text, st, inner)
    box_h = th + 14  # 7pt top + 7pt bottom padding

    if bg_color:
        draw_rect(c, x, y_top, w, box_h, fill=bg_color)
    draw_rect(c, x, y_top, 3, box_h, fill=border_color)
    draw_para(c, x + 12, y_top + 7, text, st, inner)
    return y_top + box_h + 6


# ===========================================================================
# SAMPLE TABLE  (8 records, navy header, alternating rows)
# ===========================================================================

def draw_sample_table(c, x: float, y_top: float, inner_w: float,
                      sample_rows: list[dict]) -> float:
    """
    Draw a sample-record table from a list of dicts.
    Each dict key becomes a column header.
    Returns new y_top below the table.
    """
    if not sample_rows or not isinstance(sample_rows[0], dict):
        return y_top

    keys     = [k for k in sample_rows[0].keys() if k != "row_number"][:5]
    n_cols   = len(keys) + 1   # +1 for row number (#)
    num_w    = 26
    rest_tot = inner_w - num_w
    col_w    = round(rest_tot / len(keys))
    last_w   = rest_tot - col_w * (len(keys) - 1)
    cw_list  = [num_w] + [col_w] * (len(keys) - 1) + [last_w]
    cw_list  = [max(c2, 18) for c2 in cw_list]

    hdr = ([Paragraph("<b>#</b>", PS_HDR_SM)] +
           [Paragraph(f"<b>{k.replace('_',' ').title()}</b>", PS_HDR_SM)
            for k in keys])
    tbl = [hdr]

    for idx, sr in enumerate(sample_rows):
        row = [Paragraph(str(idx + 1), PS_CELL_SM)]
        for k in keys:
            v = str(sr.get(k, ""))
            if len(v) > 36:
                v = v[:33] + "..."
            row.append(Paragraph(v, PS_CELL_SM))
        tbl.append(row)

    y = draw_table(c, x, y_top, cw_list, tbl,
                   hdr_bg=COLOR_NAVY, font_size=8, pad=4, min_h=16)
    return y + 4


# ===========================================================================
# FINDING BLOCK  (badge + WHAT / WHY / ACTION + sample table)
# ===========================================================================

def draw_finding_block(c, y: float, finding: dict,
                       page_num: int, total_pages: int,
                       org_name: str, run_id: str,
                       overflow_cb) -> tuple[int, float]:
    """
    Draw a single finding block (badge + 3 paragraphs + sample table).

    finding keys:
        label       str   - finding title
        sev         dict  - severity dict from SEVERITY_LEVELS (or SEVERITY_BLOCKED)
        count       int
        what        str   - WHAT WAS FOUND paragraph (may contain <b> tags)
        why         str   - WHY THIS MATTERS paragraph
        action      str   - RECOMMENDED ACTION paragraph
        sample      list  - list of dicts (optional)
        sample_src  str   - CSV filename for "full list in X.csv" caption (optional)

    overflow_cb is called with no args when a new page is needed;
    it must return (new_page_num, new_y).

    Returns (page_num, new_y).
    """
    label   = finding["label"]
    sev     = finding["sev"]
    count   = int(finding.get("count", 0))
    what    = finding.get("what", "")
    why     = finding.get("why", "")
    action  = finding.get("action", "")
    sample  = (finding.get("sample") or [])[:8]
    src_csv = finding.get("sample_src", "")

    inner_x = LM + 3 + 10   # past left border
    inner_w = TW - 13

    # Estimate block height for overflow check
    badge_h  = 26
    est_h = (badge_h
             + 14 + para_height(what,   PS_BODY, inner_w) + 6
             + 14 + para_height(why,    PS_BODY, inner_w) + 6
             + 14 + para_height(action, PS_BODY, inner_w) + 12
             + (len(sample) + 1) * 20 if sample else 0
             + 20)

    if y + min(est_h, 100) > CONTENT_BOTTOM:
        page_num, y = overflow_cb()

    # Left accent bar (3pt, severity color)
    bar_top = y
    # We'll know the bar height after drawing; pre-draw, then extend if needed

    # Row with badge + title + count
    badge_size = 18
    badge_cx   = LM + 10 + badge_size / 2    # center of badge
    draw_severity_badge(c, LM + 10, y + badge_size / 2 + 4, sev, size=badge_size)

    title_x = LM + 10 + badge_size + 8
    draw_text(c, title_x, y + badge_size - 1,
              label, font=FONT_BOLD, size=11, color=COLOR_NAVY)
    count_str = f"{count:,} records"
    draw_text(c, LM + TW, y + badge_size - 1,
              count_str, font=FONT_REGULAR, size=9,
              color=COLOR_MID_GRAY, align="right")
    y += badge_size + 10

    # WHAT WAS FOUND
    draw_text(c, inner_x, y + 10, "WHAT WAS FOUND",
              font=FONT_BOLD, size=8, color=sev["color"])
    y += 13
    y = draw_para(c, inner_x, y, what, PS_BODY, inner_w)
    y += 6

    # WHY THIS MATTERS
    draw_text(c, inner_x, y + 10, "WHY THIS MATTERS",
              font=FONT_BOLD, size=8, color=sev["color"])
    y += 13
    y = draw_para(c, inner_x, y, why, PS_BODY, inner_w)
    y += 6

    # RECOMMENDED ACTION
    draw_text(c, inner_x, y + 10, "RECOMMENDED ACTION",
              font=FONT_BOLD, size=8, color=sev["color"])
    y += 13
    y = draw_para(c, inner_x, y, action, PS_BODY, inner_w)
    y += 8

    # Sample records table
    if sample:
        y = draw_sample_table(c, inner_x, y, inner_w, sample)
        n_total = count
        n_shown = min(len(sample), 8)
        cap_text = f"{n_shown} of {n_total:,} shown"
        if src_csv:
            cap_text += f"  |  Full list in {src_csv}"
        draw_text(c, inner_x, y + 9, cap_text,
                  font=FONT_OBLIQUE, size=FONT_SMALL, color=COLOR_MID_GRAY)
        y += 14

    # Now draw the left accent bar covering the full block height
    block_h = y - bar_top + 4
    draw_rect(c, LM, bar_top - 2, 3, block_h, fill=sev["color"])

    y += 10
    return page_num, y


# ===========================================================================
# STAT BOXES  (4 summary cards on cover and exec summary)
# ===========================================================================

def draw_stat_boxes(c, x: float, y_top: float,
                    stats: list[dict],
                    box_w: float = 118, box_h: float = 70,
                    gap: float = 8) -> float:
    """
    Draw N stat boxes in a single row.

    Each stat dict:
        label  str   - label text below the number
        value  int   - large number shown on top
        color  obj   - left-border + number color

    Returns new y_top below boxes.
    """
    x_pos = x
    for st in stats:
        label = st["label"]
        value = st["value"]
        col   = st["color"]
        # Box background
        draw_rect(c, x_pos, y_top, box_w, box_h,
                  fill=COLOR_LIGHT_GRAY, stroke=COLOR_BORDER, sw=0.5)
        # Colored left border (4pt)
        draw_rect(c, x_pos, y_top, 4, box_h, fill=col)
        # Large number
        num_str = f"{value:,}"
        draw_text(c, x_pos + box_w / 2, y_top + box_h - 38,
                  num_str, font=FONT_BOLD, size=26, color=col, align="center")
        # Label
        draw_text(c, x_pos + box_w / 2, y_top + box_h - 16,
                  label, font=FONT_REGULAR, size=8,
                  color=COLOR_MID_GRAY, align="center")
        x_pos += box_w + gap

    return y_top + box_h + 12


# ===========================================================================
# GATE STATUS BADGE  (rounded rectangle in top-right of navy cover block)
# ===========================================================================

def draw_gate_badge(c, x_right: float, y_top: float,
                    label: str, color) -> None:
    """
    Draw a rounded gate-status badge (PASS / WARNING / BLOCKED).
    x_right = right edge of badge. y_top = top edge.
    """
    badge_w = 90
    badge_h = 44
    x = x_right - badge_w
    draw_rect(c, x, y_top, badge_w, badge_h,
              fill=color, radius=5.0)
    draw_text(c, x + badge_w / 2, y_top + 14,
              "Gate Status", font=FONT_REGULAR, size=8,
              color=COLOR_WHITE, align="center")
    draw_text(c, x + badge_w / 2, y_top + 33,
              label, font=FONT_BOLD, size=16,
              color=COLOR_WHITE, align="center")


# ===========================================================================
# MIGRATION READINESS PROGRESS BAR
# ===========================================================================

def draw_readiness_bar(c, x: float, y_top: float,
                       score: int, w: float = 220, h: float = 16) -> float:
    """
    Draw a horizontal progress bar for the migration readiness score (0-100).
    Bar color = green if >=80, amber if >=60, red otherwise.
    Returns new y_top below the bar + label.
    """
    if score >= 80:
        bar_color = COLOR_PASS
    elif score >= 60:
        bar_color = COLOR_WARN
    else:
        bar_color = COLOR_FAIL

    # Track (background)
    draw_rect(c, x, y_top, w, h, fill=COLOR_LIGHT_GRAY,
              stroke=COLOR_BORDER, sw=0.4)
    # Fill
    fill_w = max(4, round(w * score / 100))
    draw_rect(c, x, y_top, fill_w, h, fill=bar_color)
    # Score label inside bar
    draw_text(c, x + fill_w / 2, y_top + h - 4,
              f"{score}", font=FONT_BOLD, size=8,
              color=COLOR_WHITE, align="center")
    # "/ 100" label to the right
    draw_text(c, x + w + 6, y_top + h - 4,
              f"/ 100", font=FONT_REGULAR, size=8,
              color=COLOR_MID_GRAY)
    return y_top + h + 4


# ===========================================================================
# HORIZONTAL BAR CHART  (ReportLab Drawing + HorizontalBarChart)
# ===========================================================================

def draw_bar_chart(c, x: float, y_top: float,
                   w: float, h: float,
                   categories: list[str],
                   values: list[float],
                   bar_color=None,
                   max_label_w: float = 120) -> float:
    """
    Draw a horizontal bar chart using ReportLab Drawing API.
    Returns new y_top below the chart.
    """
    try:
        from reportlab.graphics.shapes import Drawing
        from reportlab.graphics.charts.barcharts import HorizontalBarChart
        from reportlab.graphics import renderPDF
        from reportlab.lib.colors import HexColor as _HexColor
    except ImportError:
        # Fallback: draw plain text rows if graphics unavailable
        draw_text(c, x, y_top + 12, "(chart not available)",
                  font=FONT_OBLIQUE, size=8, color=COLOR_MID_GRAY)
        return y_top + 20

    if not values or max(values) == 0:
        draw_text(c, x, y_top + 12, "No data",
                  font=FONT_OBLIQUE, size=8, color=COLOR_MID_GRAY)
        return y_top + 20

    col = bar_color or COLOR_NAVY
    chart_x    = max_label_w + 10
    chart_w    = w - chart_x - 10
    chart_h    = h - 20    # leave space for axes

    d  = Drawing(w, h)
    bc = HorizontalBarChart()
    bc.x           = chart_x
    bc.y           = 10
    bc.width       = chart_w
    bc.height      = chart_h
    bc.data        = [values]
    bc.bars[0].fillColor = col
    bc.bars[0].strokeColor = None

    bc.valueAxis.valueMin  = 0
    bc.valueAxis.valueMax  = max(values) * 1.15
    bc.valueAxis.visibleGrid  = True
    bc.valueAxis.labels.fontSize = 7
    bc.valueAxis.gridStrokeColor = _HexColor("#E0E0E0")

    bc.categoryAxis.categoryNames  = [str(cat)[:18] for cat in categories]
    bc.categoryAxis.labels.fontSize  = 7
    bc.categoryAxis.labels.dx        = -4
    bc.categoryAxis.labels.textAnchor = "end"
    bc.categoryAxis.visibleGrid = False
    bc.categoryAxis.reverseDirection = 1

    d.add(bc)

    # renderPDF.draw uses bottom-left origin
    rl_y = PAGE_H - y_top - h
    renderPDF.draw(d, c, x, rl_y)
    return y_top + h + 8


# ===========================================================================
# COVER PAGE
# ===========================================================================

def draw_cover_page(c, run_id: str, summary: dict,
                    org_name: str, date_str: str) -> None:
    """
    Draw the full cover page (Page 1).
    Top 42% = navy block.  Bottom 58% = white metadata + stat boxes.
    """
    cx = PAGE_W / 2

    # ---- NAVY TOP BLOCK ----
    draw_rect(c, 0, 0, PAGE_W, COVER_NAVY_H, fill=COLOR_NAVY)

    # Brand name top-left
    draw_text(c, LM, 22, BRAND_NAME,
              font=FONT_BOLD, size=11, color=COLOR_WHITE)

    # Gate status badge top-right
    n_safe    = int(summary.get("n_safe", 0))
    total     = int(summary.get("total_matched", 0))
    n_az      = int(summary.get("n_active_zero_salary", 0))
    n_wrong   = int(summary.get("n_wrong_match", 0))
    safe_pct  = float(summary.get("safe_pct", 0.0))
    gate_lbl, gate_col = get_gate_status(safe_pct, n_az, n_wrong)
    draw_gate_badge(c, PAGE_W - RM, 14, gate_lbl, gate_col)

    # Report title centered in navy block
    title_y = COVER_NAVY_H * 0.38
    draw_text(c, cx, title_y, REPORT_TITLE,
              font=FONT_BOLD, size=FONT_H1, color=COLOR_WHITE, align="center")
    draw_text(c, cx, title_y + 30, REPORT_SUBTITLE,
              font=FONT_REGULAR, size=FONT_H3, color=COLOR_WHITE, align="center")

    # Teal accent rule below subtitle
    rule_y = title_y + 48
    draw_hrule(c, rule_y, x=LM, w=TW, color=COLOR_TEAL, lw=2)

    # ---- WHITE SECTION ----
    # Metadata grid
    meta_y    = COVER_NAVY_H + 20
    uo_count  = int(summary.get("unmatched_old", 0))
    un_count  = int(summary.get("unmatched_new", 0))
    score     = migration_readiness_score(safe_pct, n_az, n_wrong, total)

    meta_pairs = [
        ("Organization",       org_name),
        ("Records Analyzed",   f"{total:,} matched pairs   ({uo_count + un_count:,} unmatched)"),
        ("Audit Date",         date_str),
        ("Run ID",             run_id),
        ("Migration Readiness Score", f"{score} / 100"),
    ]
    label_x = LM
    val_x   = LM + 160
    line_h  = 18

    for lbl, val in meta_pairs:
        draw_text(c, label_x, meta_y + 12, lbl,
                  font=FONT_BOLD, size=9, color=COLOR_MID_GRAY)
        if lbl == "Migration Readiness Score":
            draw_readiness_bar(c, val_x, meta_y, score, w=200, h=14)
        else:
            draw_text(c, val_x, meta_y + 12, val,
                      font=FONT_REGULAR, size=9, color=COLOR_CHARCOAL)
        meta_y += line_h

    draw_hrule(c, meta_y + 6, color=COLOR_BORDER)
    meta_y += 14

    # 4 Summary stat boxes
    n_review  = int(summary.get("n_review", 0))
    n_held    = int(summary.get("n_held", 0))
    n_manifest = int(summary.get("n_manifest", 0))

    stats = [
        {"label": "APPROVED",    "value": n_safe,      "color": COLOR_APPROVED},
        {"label": "NEEDS REVIEW","value": n_review,    "color": COLOR_REVIEW},
        {"label": "REJECTED",    "value": n_wrong,     "color": COLOR_REJECTED},
        {"label": "CORRECTIONS", "value": n_manifest,  "color": COLOR_MEDIUM},
    ]
    n_boxes = len(stats)
    box_gap = 8
    box_w   = (TW - box_gap * (n_boxes - 1)) / n_boxes
    box_h   = 68
    draw_stat_boxes(c, LM, meta_y, stats,
                    box_w=box_w, box_h=box_h, gap=box_gap)

    # Bottom confidential line
    conf_y = PAGE_H - 28
    draw_hrule(c, conf_y - 6, color=COLOR_BORDER)
    draw_text(c, cx, conf_y, FOOTER_CONFIDENTIAL,
              font=FONT_REGULAR, size=8, color=COLOR_MID_GRAY, align="center")
