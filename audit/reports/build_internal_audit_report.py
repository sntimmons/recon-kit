"""
build_internal_audit_report.py - Build the Internal Data Audit PDF report.

Reads existing audit-mode artifacts only:
  - internal_audit_report.json
  - internal_audit_blanks.csv
  - config/policy.yaml

Generates:
  dashboard_runs/{run_id}/internal_audit_report.pdf
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import textwrap
from datetime import datetime
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
import sys

sys.path.insert(0, str(ROOT / "audit" / "summary"))
from config_loader import load_policy

PAGE_W = 8.5
PAGE_H = 11


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _footer(fig, run_id: str, page: int, total: int) -> None:
    fig.text(
        0.5,
        0.02,
        f"CONFIDENTIAL - Internal HR Data Audit Report - Run {run_id} - Page {page} of {total}",
        ha="center",
        va="center",
        fontsize=8,
    )


def _wrap(text: str, width: int = 90) -> str:
    return textwrap.fill(text, width=width)


def _organization_name() -> str:
    policy = load_policy(ROOT / "config" / "policy.yaml")
    client = policy.get("client", {})
    return str(client.get("name") or policy.get("client_name") or "Your Organization")


def _severity_meaning(severity: str) -> str:
    return {
        "CRITICAL": "Requires immediate attention",
        "HIGH": "Should be resolved before migration or reporting",
        "MEDIUM": "Data quality improvement recommended",
        "LOW": "Informational",
    }[severity]


def _severity_paragraphs(summary: dict) -> list[str]:
    counts = summary.get("severity_counts", {}) or {}
    mapping = {
        "CRITICAL": "critical issues were found that require immediate attention before this data can be used for migration or compliance reporting.",
        "HIGH": "high-severity issues were found that should be resolved before this data is used for migration or reporting.",
        "MEDIUM": "medium-severity issues were found and should be addressed as part of data quality cleanup.",
        "LOW": "low-severity informational findings were recorded for reference.",
    }
    paragraphs = []
    for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        count = int(counts.get(severity, 0) or 0)
        if count > 0:
            paragraphs.append(f"{count} {mapping[severity]}")
    return paragraphs


def _draw_table(ax, x: float, y_top: float, width: float, rows: list[list[str]], col_widths: list[float], row_h: float, header_fill: str = "#E9EFF6", alt_fill: str = "#F7F7F7", font_size: int = 9) -> float:
    y = y_top
    for r_idx, row in enumerate(rows):
        x_pos = x
        fill = header_fill if r_idx == 0 else (alt_fill if r_idx % 2 == 0 else "#FFFFFF")
        for c_idx, cell in enumerate(row):
            cell_w = col_widths[c_idx]
            ax.add_patch(Rectangle((x_pos, y - row_h), cell_w, row_h, facecolor=fill, edgecolor="#A0A0A0", linewidth=0.6))
            ax.text(
                x_pos + 0.01,
                y - 0.015,
                _wrap(str(cell), width=max(12, int(cell_w * 95))),
                ha="left",
                va="top",
                fontsize=font_size,
                fontweight="bold" if r_idx == 0 else "normal",
            )
            x_pos += cell_w
        y -= row_h
    return y


def _build_page_one(pdf: PdfPages, run_id: str, summary: dict) -> None:
    fig = plt.figure(figsize=(PAGE_W, PAGE_H))
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    fig.text(0.08, 0.93, "Internal HR Data Audit Report", ha="left", va="top", fontsize=20, weight="bold", color="#1F4E79")
    fig.text(0.08, 0.88, f"Organization: {_organization_name()}", ha="left", va="top", fontsize=11)
    fig.text(0.08, 0.85, f"File audited: {summary.get('source_filename', '')}", ha="left", va="top", fontsize=11)
    fig.text(0.08, 0.82, f"Records analyzed: {int(summary.get('total_rows', 0) or 0):,}", ha="left", va="top", fontsize=11)
    fig.text(0.08, 0.79, f"Audit date: {datetime.now().strftime('%B %d, %Y')}", ha="left", va="top", fontsize=11)

    counts = summary.get("severity_counts", {}) or {}
    rows = [["Severity", "Count", "What it means"]]
    for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        rows.append([severity, str(int(counts.get(severity, 0) or 0)), _severity_meaning(severity)])
    _draw_table(ax, 0.08, 0.72, 0.84, rows, [0.18, 0.12, 0.54], 0.07)

    y = 0.39
    for para in _severity_paragraphs(summary):
        ax.text(0.08, y, _wrap(para, width=88), ha="left", va="top", fontsize=11)
        y -= 0.09

    _footer(fig, run_id, 1, 3)
    pdf.savefig(fig)
    plt.close(fig)


def _build_page_two(pdf: PdfPages, run_id: str, summary: dict) -> None:
    fig = plt.figure(figsize=(PAGE_W, PAGE_H))
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    fig.text(0.08, 0.93, "Findings By Category", ha="left", va="top", fontsize=18, weight="bold", color="#1F4E79")
    findings = summary.get("findings_for_pdf", []) or []

    y = 0.88
    for finding in findings:
        if y < 0.18:
            ax.text(0.08, y, "Additional findings are available in internal_audit_report.csv.", ha="left", va="top", fontsize=10, style="italic")
            break

        ax.text(0.08, y, finding.get("check_name", ""), ha="left", va="top", fontsize=12, weight="bold", color="#1F4E79")
        ax.add_patch(Rectangle((0.72, y - 0.018), 0.16, 0.03, facecolor="#F3F6F9", edgecolor="#7F8C8D", linewidth=0.6))
        ax.text(0.80, y - 0.003, finding.get("severity", ""), ha="center", va="center", fontsize=9, weight="bold")
        y -= 0.03

        ax.text(0.08, y, f"Count of records flagged: {int(finding.get('count', 0) or 0):,}", ha="left", va="top", fontsize=10)
        y -= 0.028
        ax.text(0.08, y, _wrap(str(finding.get("description", "")), width=88), ha="left", va="top", fontsize=10)
        y -= 0.045

        samples = finding.get("sample_rows", [])[:5]
        if samples:
            headers = list(samples[0].keys())
            rows = [headers] + [[str(row.get(col, "")) for col in headers] for row in samples]
            if len(headers) == 1:
                widths = [0.84]
            else:
                widths = [0.14] + [0.70 / (len(headers) - 1)] * (len(headers) - 1)
            y = _draw_table(ax, 0.08, y, 0.84, rows, widths, 0.04, font_size=8)
            y -= 0.02
        else:
            y -= 0.02

    _footer(fig, run_id, 2, 3)
    pdf.savefig(fig)
    plt.close(fig)


def _quality_label(filled_pct: float) -> str:
    if filled_pct >= 95:
        return "GOOD"
    if filled_pct >= 80:
        return "WARN"
    return "LOW"


def _build_page_three(pdf: PdfPages, run_id: str, blanks_csv: Path) -> None:
    fig = plt.figure(figsize=(PAGE_W, PAGE_H))
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    fig.text(0.08, 0.93, "Data Completeness", ha="left", va="top", fontsize=18, weight="bold", color="#1F4E79")
    if blanks_csv.exists():
        blanks = pd.read_csv(blanks_csv)
    else:
        blanks = pd.DataFrame(columns=["field", "filled_pct", "blank_count", "severity"])

    rows = [["Column", "Filled %", "Blank Count", "Label"]]
    for _, row in blanks.iterrows():
        filled_pct = float(row.get("filled_pct", 0) or 0)
        rows.append([
            str(row.get("field", "")),
            f"{filled_pct:.2f}%",
            str(int(row.get("blank_count", 0) or 0)),
            _quality_label(filled_pct),
        ])

    _draw_table(ax, 0.08, 0.88, 0.84, rows[:20], [0.34, 0.16, 0.16, 0.18], 0.038, font_size=8)
    if len(rows) > 20:
        ax.text(0.08, 0.10, "Additional completeness rows are available in internal_audit_blanks.csv.", ha="left", va="top", fontsize=9, style="italic")

    _footer(fig, run_id, 3, 3)
    pdf.savefig(fig)
    plt.close(fig)


def build_pdf(run_id: str, run_dir: Path, out_path: Path) -> None:
    summary = _read_json(run_dir / "internal_audit_report.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with PdfPages(out_path) as pdf:
        _build_page_one(pdf, run_id, summary)
        _build_page_two(pdf, run_id, summary)
        _build_page_three(pdf, run_id, run_dir / "internal_audit_blanks.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Internal Data Audit PDF report")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    build_pdf(args.run_id, Path(args.run_dir), Path(args.out))


if __name__ == "__main__":
    main()
