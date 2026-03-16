"""
build_chro_approval.py - Build the CHRO pre-load approval document as a PDF.

Reads existing run artifacts only:
  - audit_trail.json
  - sanity_gate.json
  - wide_compare.csv
  - config/policy.yaml

The script writes:
  dashboard_runs/{run_id}/chro_approval_document.pdf
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import textwrap
from datetime import date
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "recon_kit_mpl"))
os.environ.setdefault("MPLBACKEND", "Agg")

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Rectangle

plt.rcParams["pdf.compression"] = 0

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT / "audit" / "summary"))

from config_loader import load_policy

PAGE_W = 612.0
PAGE_H = 792.0
MARGIN = 72.0
CONTENT_W = PAGE_W - (MARGIN * 2)
BLUE = "#1F4E79"
LIGHT_GREY = "#F2F2F2"
LIGHT_RED = "#FFE6E6"
GREY = "#808080"
BLACK = "#000000"


def _n(count: int, word: str) -> str:
    return f"{count:,} {word}" if count == 1 else f"{count:,} {word}s"


def _load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _load_wide_compare(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _wrap_lines(text: str, width_pt: float, font_size: float = 10.0) -> list[str]:
    if text is None:
        return [""]
    approx_char_width = max(4.5, font_size * 0.52)
    wrap_width = max(8, int(width_pt / approx_char_width))
    all_lines: list[str] = []
    for chunk in str(text).splitlines() or [""]:
        wrapped = textwrap.wrap(chunk, width=wrap_width, break_long_words=False, break_on_hyphens=False)
        all_lines.extend(wrapped if wrapped else [""])
    return all_lines or [""]


def _new_page() -> tuple[object, object]:
    fig = plt.figure(figsize=(8.5, 11), dpi=72)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, PAGE_W)
    ax.set_ylim(0, PAGE_H)
    ax.axis("off")
    return fig, ax


def _draw_text(ax, x: float, y: float, text: str, *, size: float = 10.0, weight: str = "normal",
               color: str = BLACK, ha: str = "left", va: str = "top") -> None:
    ax.text(x, y, text, fontsize=size, fontname="Helvetica", fontweight=weight,
            color=color, ha=ha, va=va)


def _draw_paragraph(ax, x: float, y_top: float, width_pt: float, text: str, *,
                    size: float = 10.0, leading: float = 14.0, weight: str = "normal") -> float:
    lines = _wrap_lines(text, width_pt, font_size=size)
    y = y_top
    for line in lines:
        _draw_text(ax, x, y, line, size=size, weight=weight)
        y -= leading
    return y


def _draw_bullets(ax, x: float, y_top: float, width_pt: float, items: list[str], *,
                  size: float = 10.0, leading: float = 14.0) -> float:
    y = y_top
    bullet_width = 14.0
    for item in items:
        _draw_text(ax, x, y, "-", size=size)
        lines = _wrap_lines(item, width_pt - bullet_width, font_size=size)
        line_y = y
        for idx, line in enumerate(lines):
            _draw_text(ax, x + bullet_width, line_y, line, size=size)
            line_y -= leading
        y = line_y - 2
    return y


def _draw_table(
    ax,
    x: float,
    y_top: float,
    col_widths: list[float],
    rows: list[list[str]],
    *,
    headers: list[str] | None = None,
    font_size: float = 10.0,
    leading: float = 14.0,
    header_fill: str = LIGHT_GREY,
    warning_fill: str | None = None,
) -> float:
    padding_l = 6.0
    padding_r = 6.0
    padding_t = 4.0
    padding_b = 4.0
    cursor_y = y_top
    all_rows = []
    if headers is not None:
        all_rows.append((headers, header_fill, True))
    for idx, row in enumerate(rows):
        fill = LIGHT_GREY if idx % 2 == 1 else "#FFFFFF"
        if warning_fill is not None:
            fill = warning_fill
        all_rows.append((row, fill, False))

    for row, fill, is_header in all_rows:
        wrapped_cells = []
        row_line_count = 1
        for col_idx, cell in enumerate(row):
            lines = _wrap_lines(str(cell), col_widths[col_idx] - padding_l - padding_r, font_size=font_size)
            wrapped_cells.append(lines)
            row_line_count = max(row_line_count, len(lines))
        row_h = padding_t + padding_b + (row_line_count * leading)
        cursor_x = x
        for col_idx, lines in enumerate(wrapped_cells):
            col_w = col_widths[col_idx]
            rect = Rectangle((cursor_x, cursor_y - row_h), col_w, row_h,
                             facecolor=fill, edgecolor=GREY, linewidth=0.5)
            ax.add_patch(rect)
            text_y = cursor_y - padding_t - 1
            for line in lines:
                _draw_text(ax, cursor_x + padding_l, text_y, line,
                           size=font_size, weight="bold" if is_header else "normal")
                text_y -= leading
            cursor_x += col_w
        cursor_y -= row_h
    return cursor_y


def _footer(ax, run_id: str, report_date: str, page_num: int, total_pages: int) -> None:
    footer = (
        f"CONFIDENTIAL - Data Whisperer Pre-Load Approval Document "
        f"Run {run_id} - Generated {report_date} - Page {page_num} of {total_pages}"
    )
    _draw_text(ax, PAGE_W / 2, 20, footer, size=8, ha="center", va="center")


def _count_fix_types(df: pd.DataFrame) -> dict[str, int]:
    if df.empty or "action" not in df.columns or "fix_types" not in df.columns:
        return {"job_org": 0, "hire_date": 0, "status": 0, "salary": 0}
    approve = df[df["action"] == "APPROVE"].copy()
    fix_text = approve["fix_types"].fillna("").astype(str)
    return {
        "job_org": int(fix_text.str.contains("job_org", na=False).sum()),
        "hire_date": int(fix_text.str.contains("hire_date", na=False).sum()),
        "status": int(fix_text.str.contains("status", na=False).sum()),
        "salary": int(fix_text.str.contains("salary", na=False).sum()),
    }


def _count_salary_variance(df: pd.DataFrame) -> int:
    if df.empty or "reason" not in df.columns:
        return 0
    return int(df["reason"].fillna("").astype(str).str.contains("salary_ratio_extreme", na=False).sum())


def _read_run_data(run_dir: Path) -> dict:
    audit_trail = _load_json(run_dir / "audit_trail.json", {})
    sanity_gate = _load_json(run_dir / "sanity_gate.json", {})
    wide_compare = _load_wide_compare(run_dir / "wide_compare.csv")
    policy = load_policy(ROOT / "config" / "policy.yaml")

    client_cfg = policy.get("client", {}) if isinstance(policy.get("client"), dict) else {}
    legacy_client_name = str(policy.get("client_name") or "").strip()
    client_name = str(client_cfg.get("name") or legacy_client_name or "Your Organization")
    chro_name = str(client_cfg.get("chro_name") or "")
    chro_title = str(client_cfg.get("chro_title") or "Chief Human Resources Officer")

    total_records = int(audit_trail.get("total_records_processed", 0) or 0)
    approve_count = int(audit_trail.get("approve_count", 0) or 0)
    review_count = int(audit_trail.get("review_count", 0) or 0)
    reject_count = int(audit_trail.get("reject_count", 0) or 0)
    corrections_staged = int(audit_trail.get("corrections_staged_count", 0) or 0)

    fix_counts = _count_fix_types(wide_compare)
    gate_metrics = sanity_gate.get("metrics", {}) if isinstance(sanity_gate.get("metrics"), dict) else {}
    gate_health = sanity_gate.get("health_checks", {}) if isinstance(sanity_gate.get("health_checks"), dict) else {}
    zero_salary_count = int(((gate_health.get("active_zero_salary") or {}).get("value", 0)) or 0)
    hire_date_wave_count = int(((gate_metrics.get("hire_date_wave") or {}).get("count", 0)) or 0)
    salary_variance_count = _count_salary_variance(wide_compare)

    return {
        "audit_trail": audit_trail,
        "sanity_gate": sanity_gate,
        "wide_compare": wide_compare,
        "client_name": client_name,
        "chro_name": chro_name,
        "chro_title": chro_title,
        "total_records": total_records,
        "approve_count": approve_count,
        "review_count": review_count,
        "reject_count": reject_count,
        "approve_pct": f"{((approve_count / total_records) * 100) if total_records else 0:.1f}",
        "review_pct": f"{((review_count / total_records) * 100) if total_records else 0:.1f}",
        "job_org_count": fix_counts["job_org"],
        "hire_date_count": fix_counts["hire_date"],
        "status_count": fix_counts["status"],
        "salary_count": fix_counts["salary"],
        "zero_salary_count": zero_salary_count,
        "hire_date_wave_count": hire_date_wave_count,
        "salary_variance_count": salary_variance_count,
        "gate_result": "PASS" if sanity_gate.get("passed", False) else "FAIL",
        "gate_reasons": sanity_gate.get("reasons", []) or [],
        "corrections_staged_count": corrections_staged,
    }


def _build_cover(ax, run_id: str, report_date: str, data: dict) -> None:
    _draw_text(ax, PAGE_W / 2, 710, "DATA WHISPERER", size=24, weight="bold", color=BLUE, ha="center")
    _draw_text(ax, PAGE_W / 2, 680, "Pre-Load Approval Document", size=18, weight="bold", color=BLUE, ha="center")

    y = 620
    for line in [
        f"Organization: {data['client_name']}",
        "Migration: ADP to Workday",
        f"Run ID: {run_id}",
        f"Report Date: {report_date}",
        "Prepared By: Data Whisperer Reconciliation Engine",
    ]:
        _draw_text(ax, MARGIN, y, line, size=12)
        y -= 28

    _draw_text(ax, PAGE_W / 2, 500, "CONFIDENTIAL - FOR EXECUTIVE REVIEW ONLY", size=11, weight="bold", ha="center")

    sig_x = MARGIN
    sig_y_top = 430
    sig_w = CONTENT_W
    ax.add_patch(Rectangle((sig_x, 120), sig_w, sig_y_top - 120, fill=False, edgecolor=BLUE, linewidth=1.2))
    _draw_text(ax, PAGE_W / 2, sig_y_top - 20, "CHIEF HUMAN RESOURCES OFFICER APPROVAL", size=14, weight="bold", ha="center")
    y = sig_y_top - 52
    y = _draw_bullets(
        ax,
        sig_x + 18,
        y,
        sig_w - 36,
        [
            "I have reviewed this reconciliation summary and authorize the correction load described in this document."
        ],
        size=11,
        leading=15,
    )
    y -= 10
    sig_rows = [
        ["Signature:", "_______________________________"],
        ["Name (Print):", data["chro_name"] if data["chro_name"] else "___________________________"],
        ["Title:", data["chro_title"] if data["chro_title"] else "__________________________________"],
        ["Date:", "___________________________________"],
    ]
    _draw_table(ax, sig_x, y, [180.0, 288.0], sig_rows, font_size=11)


def _build_summary_page(ax, data: dict) -> None:
    _draw_text(ax, MARGIN, 720, "What We Found", size=18, weight="bold", color=BLUE)
    p1 = (
        f"We compared {data['total_records']:,} employee records between your ADP system and Workday. "
        f"Of those, {_n(data['approve_count'], 'record')} ({data['approve_pct']}%) matched cleanly and "
        f"are ready to load. {_n(data['review_count'], 'record')} ({data['review_pct']}%) require human "
        f"review before they can be loaded. {_n(data['reject_count'], 'record')} "
        f"{'was' if data['reject_count'] == 1 else 'were'} flagged as possible wrong-person "
        f"{'match' if data['reject_count'] == 1 else 'matches'} and {'has' if data['reject_count'] == 1 else 'have'} been blocked entirely."
    )
    y = _draw_paragraph(ax, MARGIN, 680, CONTENT_W, p1, size=10, leading=14)
    y -= 8

    if data["gate_result"] == "FAIL":
        intro = (
            "Corrections are currently blocked because the sanity gate failed. The following counts "
            "show what will be applied once the gate issue is resolved and a clean run is completed:"
        )
    else:
        intro = (
            f"If you authorize this load, the following corrections will be applied to "
            f"{data['approve_count']:,} Workday records:"
        )
    y = _draw_paragraph(ax, MARGIN, y, CONTENT_W, intro, size=10, leading=14)
    y -= 4
    y = _draw_bullets(
        ax,
        MARGIN,
        y,
        CONTENT_W,
        [
            f"{_n(data['job_org_count'], 'job title or department correction')}",
            f"{_n(data['hire_date_count'], 'hire date correction')}",
            f"{_n(data['status_count'], 'employment status correction')}",
            f"{_n(data['salary_count'], 'salary correction')}",
            "No corrections will be applied to records in the review queue until a human reviewer clears them.",
        ],
        size=10,
        leading=14,
    )
    y -= 8

    p3 = (
        "The following records were automatically protected and will NOT receive any corrections "
        "regardless of authorization:"
    )
    y = _draw_paragraph(ax, MARGIN, y, CONTENT_W, p3, size=10, leading=14)
    y -= 4
    _draw_bullets(
        ax,
        MARGIN,
        y,
        CONTENT_W,
        [
            f"{_n(data['zero_salary_count'], 'active employee')} showing $0 salary in Workday (possible data entry error - requires investigation)",
            f"{_n(data['review_count'], 'record')} flagged for human review",
            f"{_n(data['reject_count'], 'record')} blocked as possible wrong-person {'match' if data['reject_count'] == 1 else 'matches'}",
        ],
        size=10,
        leading=14,
    )


def _risk_rows(data: dict) -> list[list[str]]:
    rows = [
        ["Active employees with $0 salary", f"{data['zero_salary_count']:,}", "Investigate before load"],
        ["Records requiring human review", f"{data['review_count']:,}", "Complete review queue first"],
        ["Possible wrong-person matches", f"{data['reject_count']:,}", "Manual investigation required"],
        ["Hire date wave detected", f"{data['hire_date_wave_count']:,}", "Verify bulk import date is correct"],
        ["Salary changes > 15% variance", f"{data['salary_variance_count']:,}", "Spot check recommended"],
    ]
    return [row for row in rows if row[1] != "0"]


def _build_risk_page(ax, data: dict) -> None:
    _draw_text(ax, MARGIN, 720, "Risks and Flags Identified", size=18, weight="bold", color=BLUE)
    rows = _risk_rows(data)
    y = 675
    if rows:
        y = _draw_table(
            ax,
            MARGIN,
            y,
            [250.0, 60.0, 158.0],
            rows,
            headers=["Risk Type", "Count", "Action Required"],
            font_size=10,
            leading=14,
        )
    else:
        y = _draw_paragraph(ax, MARGIN, y, CONTENT_W, "No material risk rows were identified for this run.", size=10, leading=14)
    y -= 22
    y = _draw_paragraph(
        ax,
        MARGIN,
        y,
        CONTENT_W,
        f"The sanity gate result for this run was {data['gate_result']}. If the gate result is FAIL, corrections have been blocked and this document should not be signed until the gate failure reason has been investigated and resolved.",
        size=10,
        leading=14,
    )

    if data["gate_result"] == "FAIL":
        y -= 18
        warning = (
            "WARNING: SANITY GATE FAILED\n"
            "Corrections are currently blocked. Do not sign this document until the gate failure has been reviewed and resolved.\n"
            f"Gate failure reason: {'; '.join(data['gate_reasons']) if data['gate_reasons'] else 'No reason provided'}"
        )
        _draw_table(
            ax,
            MARGIN,
            y,
            [468.0],
            [[warning]],
            font_size=10,
            leading=14,
            warning_fill=LIGHT_RED,
        )


def _build_authorization_page(ax, run_id: str, data: dict) -> None:
    audit_trail = data["audit_trail"]
    input_files = audit_trail.get("input_files", {}) if isinstance(audit_trail.get("input_files"), dict) else {}
    old_file = input_files.get("old_system", {}) if isinstance(input_files.get("old_system"), dict) else {}
    new_file = input_files.get("new_system", {}) if isinstance(input_files.get("new_system"), dict) else {}

    _draw_text(ax, MARGIN, 720, "Authorization Record", size=18, weight="bold", color=BLUE)
    rows = [
        ["Run ID:", run_id],
        ["Run completed:", str(audit_trail.get("run_complete_timestamp") or "")],
        ["Engine version:", str(audit_trail.get("engine_version") or "")],
        ["Old system file:", f"{old_file.get('filename', '')} (SHA-256: {str(old_file.get('sha256') or '')[:16]}...)"],
        ["New system file:", f"{new_file.get('filename', '')} (SHA-256: {str(new_file.get('sha256') or '')[:16]}...)"],
        ["Total records processed:", f"{data['total_records']:,}"],
        ["Records approved:", f"{data['approve_count']:,}"],
        ["Records for review:", f"{data['review_count']:,}"],
        ["Records blocked:", f"{data['reject_count']:,}"],
        ["Corrections staged:", f"{data['corrections_staged_count']:,}"],
        ["Gate result:", data["gate_result"]],
    ]
    y = _draw_table(ax, MARGIN, 665, [160.0, 308.0], rows, font_size=9.5, leading=13)
    y -= 26
    _draw_text(ax, MARGIN, y, "What This Signature Authorizes", size=13, weight="bold", color=BLUE)
    y -= 26
    y = _draw_paragraph(
        ax,
        MARGIN,
        y,
        CONTENT_W,
        f"By signing this document, the {data['chro_title']} confirms they have reviewed the reconciliation summary above and authorizes Data Whisperer to apply the staged corrections to {data['approve_count']:,} Workday employee records. This authorization does not apply to records in the review queue or to records that have been blocked. Those records require separate disposition before any corrections can be applied.",
        size=10,
        leading=14,
    )
    y -= 18
    _draw_text(ax, MARGIN, y, "Document Retention", size=13, weight="bold", color=BLUE)
    y -= 26
    _draw_paragraph(
        ax,
        MARGIN,
        y,
        CONTENT_W,
        "This signed document should be retained in accordance with your organization's HR records retention policy. The run ID above can be used to retrieve the full technical audit trail at any time.",
        size=10,
        leading=14,
    )


def build_document(run_id: str, run_dir: Path, out_path: Path) -> None:
    report_date = date.today().strftime("%B %d, %Y")
    data = _read_run_data(run_dir)
    total_pages = 4
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with PdfPages(out_path) as pdf:
        fig, ax = _new_page()
        _build_cover(ax, run_id, report_date, data)
        _footer(ax, run_id, report_date, 1, total_pages)
        pdf.savefig(fig)
        plt.close(fig)

        fig, ax = _new_page()
        _build_summary_page(ax, data)
        _footer(ax, run_id, report_date, 2, total_pages)
        pdf.savefig(fig)
        plt.close(fig)

        fig, ax = _new_page()
        _build_risk_page(ax, data)
        _footer(ax, run_id, report_date, 3, total_pages)
        pdf.savefig(fig)
        plt.close(fig)

        fig, ax = _new_page()
        _build_authorization_page(ax, run_id, data)
        _footer(ax, run_id, report_date, 4, total_pages)
        pdf.savefig(fig)
        plt.close(fig)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build CHRO approval PDF for a completed run.")
    parser.add_argument("--run-id", required=True, help="Run identifier")
    parser.add_argument("--run-dir", required=True, help="Run output directory")
    parser.add_argument("--out", required=True, help="Output PDF path")
    args = parser.parse_args(argv)

    build_document(args.run_id, Path(args.run_dir), Path(args.out))
    print(f"[build_chro_approval] saved: {args.out}")


if __name__ == "__main__":
    main()
