"""
generate_report.py — Audit Report Generator for the Data Whisperer reconciliation pipeline.

Reads data from audit/audit.db, audit/summary/*.csv, and audit/exports/out/wide_compare.csv
to produce a professional .docx audit report summarising the reconciliation run.

Report sections
---------------
  1. Executive Summary
  2. Match Quality Analysis
  3. Data Quality Findings
  4. Field Change Analysis  (Salary / Status / Hire Date / Job & Org)
  5. Priority Review Queue
  6. Rejected Matches (REJECT_MATCH tier)
  7. Recommendations

Dependencies
------------
  python-docx >= 1.1.0   (pip install python-docx)
  pandas
  PyYAML

Run:
    python3 audit/reports/generate_report.py [--db PATH] [--out PATH] [--wide PATH]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    from docx import Document
    from docx.shared import Inches, Pt, RGBColor
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement
except ImportError:
    print(
        "[error] python-docx not installed.\n"
        "  Run: pip install python-docx",
        file=sys.stderr,
    )
    sys.exit(2)

_HERE        = Path(__file__).resolve().parent
_SUMMARY_DIR = _HERE.parent / "summary"
sys.path.insert(0, str(_SUMMARY_DIR))

from gating import classify_all, salary_delta, _parse_confidence, _norm
from sanity_checks import detect_wave_dates

ROOT         = _HERE.parents[2]
DB_PATH      = ROOT / "audit" / "audit.db"
WIDE_CSV     = ROOT / "audit" / "exports" / "out" / "wide_compare.csv"
REVIEW_CSV   = ROOT / "audit" / "summary" / "review_queue.csv"
SANITY_GATE  = ROOT / "audit" / "summary" / "sanity_gate.json"
OUT_PATH     = _HERE / "audit_report.docx"

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
_BLUE_DARK    = RGBColor(0x2E, 0x75, 0xB6)   # section headings
_BLUE_LIGHT   = RGBColor(0xBD, 0xD7, 0xEE)   # table header fill
_RED          = RGBColor(0xC0, 0x00, 0x00)   # critical warnings
_ORANGE       = RGBColor(0xE2, 0x6B, 0x0A)   # caution
_GREEN        = RGBColor(0x37, 0x86, 0x45)   # positive
_GREY_LIGHT   = RGBColor(0xF2, 0xF2, 0xF2)   # alternating rows
_WHITE        = RGBColor(0xFF, 0xFF, 0xFF)


# ---------------------------------------------------------------------------
# Document helpers
# ---------------------------------------------------------------------------

def _set_cell_bg(cell, hex_color: str) -> None:
    """Set cell background colour (XML shading element)."""
    tc   = cell._tc
    tcPr = tc.get_or_add_tcPr()
    shd  = OxmlElement("w:shd")
    shd.set(qn("w:val"),   "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"),  hex_color.upper())
    tcPr.append(shd)


def _add_heading(doc: Document, text: str, level: int = 1) -> None:
    p = doc.add_heading(text, level=level)
    if level == 1:
        for run in p.runs:
            run.font.color.rgb = _BLUE_DARK
            run.font.bold = True


def _kv_table(doc: Document, rows: list[tuple]) -> None:
    """Write a 2-column key-value table with alternating row colours."""
    tbl = doc.add_table(rows=len(rows), cols=2)
    tbl.style = "Table Grid"
    for i, (key, val) in enumerate(rows):
        cells = tbl.rows[i].cells
        cells[0].text = str(key)
        cells[1].text = str(val)
        # Bold key column
        for run in cells[0].paragraphs[0].runs:
            run.font.bold = True
        # Alternating background
        bg = "F2F2F2" if i % 2 == 0 else "FFFFFF"
        _set_cell_bg(cells[0], bg)
        _set_cell_bg(cells[1], bg)
    # Column widths
    tbl.columns[0].width = Inches(3.0)
    tbl.columns[1].width = Inches(2.5)


def _data_table(doc: Document, headers: list[str], rows_data: list[list]) -> None:
    """Write a header + data table."""
    n_cols = len(headers)
    tbl    = doc.add_table(rows=1 + len(rows_data), cols=n_cols)
    tbl.style = "Table Grid"

    # Header row
    hdr_cells = tbl.rows[0].cells
    for i, h in enumerate(headers):
        hdr_cells[i].text = h
        run = hdr_cells[i].paragraphs[0].runs[0] if hdr_cells[i].paragraphs[0].runs else hdr_cells[i].paragraphs[0].add_run(h)
        run.font.bold = True
        run.font.color.rgb = _WHITE
        _set_cell_bg(hdr_cells[i], "2E75B6")

    # Data rows
    for r_i, row_vals in enumerate(rows_data):
        cells = tbl.rows[r_i + 1].cells
        for c_i, val in enumerate(row_vals):
            cells[c_i].text = str(val) if val is not None else ""
            bg = "F2F2F2" if r_i % 2 == 0 else "FFFFFF"
            _set_cell_bg(cells[c_i], bg)


def _callout(doc: Document, text: str, level: str = "warning") -> None:
    """Add a callout paragraph (bold, coloured text)."""
    p    = doc.add_paragraph()
    run  = p.add_run(f"{'⚠' if level == 'warning' else '✓'}  {text}")
    run.font.bold = True
    run.font.color.rgb = _RED if level == "critical" else (_ORANGE if level == "warning" else _GREEN)


# ---------------------------------------------------------------------------
# Data loader
# ---------------------------------------------------------------------------

def _load_data(db_path: Path, wide_path: Path) -> pd.DataFrame:
    """Load matched pairs with gating results from wide_compare CSV or DB."""
    if wide_path.exists():
        df = pd.read_csv(str(wide_path))
        print(f"[generate_report] loaded {len(df):,} rows from {wide_path.name}")
        return df

    print(f"[generate_report] {wide_path.name} not found — computing from DB ...")
    con = sqlite3.connect(str(db_path))
    try:
        mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)
    finally:
        con.close()

    if "confidence" not in mp.columns:
        mp["confidence"] = None

    all_rows   = mp.to_dict(orient="records")
    wave_dates = detect_wave_dates(all_rows)

    out_rows = []
    for r in all_rows:
        result    = classify_all(r, wave_dates=wave_dates)
        fix_types = result["fix_types"]
        sal_d     = salary_delta(r)
        out_rows.append({
            "pair_id":           r.get("pair_id", ""),
            "match_source":      r.get("match_source", ""),
            "confidence":        r.get("confidence"),
            "action":            result["action"],
            "reason":            result["reason"],
            "fix_types":         "|".join(fix_types),
            "old_full_name_norm":r.get("old_full_name_norm", ""),
            "new_full_name_norm":r.get("new_full_name_norm", ""),
            "old_salary":        r.get("old_salary"),
            "new_salary":        r.get("new_salary"),
            "old_worker_status": r.get("old_worker_status", ""),
            "new_worker_status": r.get("new_worker_status", ""),
            "old_hire_date":     r.get("old_hire_date", ""),
            "new_hire_date":     r.get("new_hire_date", ""),
            "old_position":      r.get("old_position", ""),
            "new_position":      r.get("new_position", ""),
            "salary_delta":      sal_d,
        })
    print(f"[generate_report] computed gating for {len(out_rows):,} rows from DB")
    return pd.DataFrame(out_rows)


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _section_executive_summary(doc: Document, df: pd.DataFrame) -> None:
    _add_heading(doc, "1. Executive Summary", 1)

    total          = len(df)
    n_approve      = int((df["action"] == "APPROVE").sum())  if "action" in df.columns else 0
    n_review       = int((df["action"] == "REVIEW").sum())   if "action" in df.columns else 0
    n_reject       = int((df["action"] == "REJECT_MATCH").sum()) if "action" in df.columns else 0
    approve_pct    = f"{n_approve/total*100:.1f}%" if total > 0 else "—"
    review_pct     = f"{n_review/total*100:.1f}%"  if total > 0 else "—"
    reject_pct     = f"{n_reject/total*100:.1f}%"  if total > 0 else "—"

    n_with_changes = int((df["fix_types"].fillna("") != "").sum()) if "fix_types" in df.columns else 0
    n_clean        = total - n_with_changes

    doc.add_paragraph(
        f"This report summarises the results of the Data Whisperer reconciliation run "
        f"completed on {datetime.now().strftime('%B %d, %Y')}. A total of "
        f"{total:,} matched employee pairs were evaluated across all match sources."
    )

    _kv_table(doc, [
        ("Total matched pairs",   f"{total:,}"),
        ("Auto-approved (APPROVE)",   f"{n_approve:,}  ({approve_pct})"),
        ("Requires review (REVIEW)",  f"{n_review:,}  ({review_pct})"),
        ("Rejected — wrong person",   f"{n_reject:,}  ({reject_pct})"),
        ("Pairs with field changes",  f"{n_with_changes:,}"),
        ("Clean pairs (no changes)",  f"{n_clean:,}"),
        ("Report generated",         datetime.now().strftime("%Y-%m-%d %H:%M")),
    ])
    doc.add_paragraph()

    if n_reject > 0:
        _callout(doc,
            f"{n_reject:,} pairs were flagged as REJECT_MATCH (likely wrong-person pairings). "
            f"These must not receive any automated corrections.",
            level="critical"
        )

    active_zero = 0
    if "new_worker_status" in df.columns and "new_salary" in df.columns:
        active_mask = df["new_worker_status"].fillna("").str.strip().str.lower() == "active"
        new_sal_num = pd.to_numeric(
            df["new_salary"].astype(str).str.replace(",","").str.replace("$",""),
            errors="coerce"
        )
        active_zero = int((active_mask & (new_sal_num.isna() | (new_sal_num == 0))).sum())

    if active_zero > 0:
        _callout(doc,
            f"CRITICAL: {active_zero:,} active employees have $0 or missing salary in the "
            f"source data. Salary corrections for these records have been blocked.",
            level="critical"
        )


def _section_match_quality(doc: Document, df: pd.DataFrame) -> None:
    _add_heading(doc, "2. Match Quality Analysis", 1)

    if "match_source" not in df.columns:
        doc.add_paragraph("(match_source column not available)")
        return

    total = len(df)
    _add_heading(doc, "2.1 Match Source Breakdown", 2)
    src_counts = df["match_source"].value_counts()
    table_rows = []
    for src, cnt in src_counts.items():
        pct     = f"{cnt/total*100:.1f}%"
        src_str = str(src) if src else "(blank)"
        table_rows.append([src_str, f"{cnt:,}", pct])
    _data_table(doc,
        ["Match Source", "Count", "% of Total"],
        table_rows
    )
    doc.add_paragraph()

    if "confidence" in df.columns:
        _add_heading(doc, "2.2 Confidence Distribution", 2)
        conf_num    = pd.to_numeric(df["confidence"], errors="coerce")
        n_exact     = int((conf_num == 1.0).sum())
        n_high      = int(((conf_num >= 0.97) & (conf_num < 1.0)).sum())
        n_medium    = int(((conf_num >= 0.80) & (conf_num < 0.97)).sum())
        n_low       = int(((conf_num < 0.80) & conf_num.notna()).sum())
        n_missing   = int(conf_num.isna().sum())
        _data_table(doc,
            ["Confidence Band", "Count", "% of Total"],
            [
                ["Exact (1.00)",          f"{n_exact:,}",   f"{n_exact/total*100:.1f}%"],
                ["High (0.97–0.99)",      f"{n_high:,}",    f"{n_high/total*100:.1f}%"],
                ["Medium (0.80–0.96)",    f"{n_medium:,}",  f"{n_medium/total*100:.1f}%"],
                ["Low (< 0.80)",          f"{n_low:,}",     f"{n_low/total*100:.1f}%"],
                ["Missing / not scored",  f"{n_missing:,}", f"{n_missing/total*100:.1f}%"],
            ]
        )
        doc.add_paragraph()

        if n_low > 0:
            _callout(doc,
                f"{n_low:,} pairs have low confidence scores (< 0.80). "
                f"These require careful human review before any corrections are applied.",
                level="warning"
            )


def _section_data_quality(doc: Document, df: pd.DataFrame) -> None:
    _add_heading(doc, "3. Data Quality Findings", 1)

    # Active/$0 salary
    _add_heading(doc, "3.1 Active Employees with $0 Salary", 2)
    if "new_worker_status" in df.columns and "new_salary" in df.columns:
        active_mask = df["new_worker_status"].fillna("").str.strip().str.lower() == "active"
        new_sal_num = pd.to_numeric(
            df["new_salary"].astype(str).str.replace(",","").str.replace("$",""),
            errors="coerce"
        )
        az_df = df[active_mask & (new_sal_num.isna() | (new_sal_num == 0))].copy()
        if az_df.empty:
            p = doc.add_paragraph("No active employees with $0 or missing salary detected. ")
            run = p.runs[0]
            run.font.color.rgb = _GREEN
        else:
            _callout(doc,
                f"{len(az_df):,} active employees have $0 or missing salary. "
                "Salary corrections for these records have been blocked from the corrections pipeline.",
                level="critical"
            )
            sample_cols = [c for c in ["pair_id","old_full_name_norm","old_salary","new_salary"] if c in az_df.columns]
            if sample_cols:
                sample = az_df[sample_cols].head(10)
                _data_table(doc, sample_cols, sample.values.tolist())
    else:
        doc.add_paragraph("(Status/salary columns not available for this check)")
    doc.add_paragraph()

    # Wave dates
    _add_heading(doc, "3.2 Hire Date Wave Detection", 2)
    if "new_hire_date" in df.columns and "reason" in df.columns:
        wave_rows = df[df["reason"].fillna("").str.contains("hire_date_wave", na=False)]
        if wave_rows.empty:
            p = doc.add_paragraph("No hire date wave patterns detected. ")
            p.runs[0].font.color.rgb = _GREEN
        else:
            wave_date_vals = df["new_hire_date"].value_counts()
            wave_dates_top = wave_date_vals[wave_date_vals > len(df) * 0.01].head(10)
            _callout(doc,
                f"{len(wave_rows):,} records share hire dates that appear in ≥ 1% of all pairs "
                f"— indicative of a bulk import. These have been routed to REVIEW.",
                level="warning"
            )
            if not wave_dates_top.empty:
                _data_table(doc,
                    ["Hire Date", "Count", "% of Total"],
                    [[d, f"{c:,}", f"{c/len(df)*100:.1f}%"] for d, c in wave_dates_top.items()]
                )
    else:
        doc.add_paragraph("(Hire date / reason columns not available)")
    doc.add_paragraph()

    # REJECT_MATCH
    _add_heading(doc, "3.3 Rejected Match Pairings (REJECT_MATCH)", 2)
    if "action" in df.columns:
        rej_df = df[df["action"] == "REJECT_MATCH"]
        if rej_df.empty:
            p = doc.add_paragraph("No REJECT_MATCH records detected. ")
            p.runs[0].font.color.rgb = _GREEN
        else:
            _callout(doc,
                f"{len(rej_df):,} pairs were identified as likely wrong-person pairings "
                f"and flagged REJECT_MATCH. No automated corrections will be applied.",
                level="critical"
            )
            if "reason" in rej_df.columns:
                rej_reasons = rej_df["reason"].value_counts().head(5)
                _data_table(doc,
                    ["Rejection Reason", "Count"],
                    [[str(r), f"{c:,}"] for r, c in rej_reasons.items()]
                )
    doc.add_paragraph()


def _section_field_changes(doc: Document, df: pd.DataFrame) -> None:
    _add_heading(doc, "4. Field Change Analysis", 1)

    if "fix_types" not in df.columns:
        doc.add_paragraph("(fix_types column not available)")
        return

    total = len(df)

    # Summary table
    change_rows = []
    for ft, label in [
        ("salary",    "Salary"),
        ("payrate",   "Payrate"),
        ("status",    "Worker Status"),
        ("hire_date", "Hire Date"),
        ("job_org",   "Job / Organization"),
    ]:
        cnt = int(df["fix_types"].str.contains(ft, na=False).sum())
        change_rows.append([label, f"{cnt:,}", f"{cnt/total*100:.1f}%"])
    _data_table(doc, ["Change Type", "Count", "% of Total"], change_rows)
    doc.add_paragraph()

    # Salary details
    _add_heading(doc, "4.1 Salary Changes", 2)
    if "salary_delta" in df.columns:
        sal_df    = df[df["fix_types"].str.contains("salary", na=False)].copy()
        sal_delta = pd.to_numeric(sal_df["salary_delta"], errors="coerce").dropna()
        if len(sal_delta) > 0:
            n_increase = int((sal_delta > 0).sum())
            n_decrease = int((sal_delta < 0).sum())
            _kv_table(doc, [
                ("Salary changes total",  f"{len(sal_df):,}"),
                ("Increases",            f"{n_increase:,}"),
                ("Decreases",            f"{n_decrease:,}"),
                ("Mean delta",           f"${sal_delta.mean():,.2f}"),
                ("Median delta",         f"${sal_delta.median():,.2f}"),
                ("Largest increase",     f"${sal_delta.max():,.2f}"),
                ("Largest decrease",     f"${sal_delta.min():,.2f}"),
            ])
        else:
            doc.add_paragraph("No parseable salary deltas found.")
    else:
        doc.add_paragraph("(salary_delta column not available)")
    doc.add_paragraph()

    # Status changes
    _add_heading(doc, "4.2 Status Changes", 2)
    if "old_worker_status" in df.columns and "new_worker_status" in df.columns:
        st_df = df[df["fix_types"].str.contains("status", na=False)].copy()
        if st_df.empty:
            doc.add_paragraph("No status changes detected.")
        else:
            transitions = (
                st_df["old_worker_status"].fillna("blank").str.lower().str.strip()
                + " → "
                + st_df["new_worker_status"].fillna("blank").str.lower().str.strip()
            ).value_counts().head(10)
            _data_table(doc,
                ["Transition", "Count"],
                [[t, f"{c:,}"] for t, c in transitions.items()]
            )
    doc.add_paragraph()

    # Hire date changes
    _add_heading(doc, "4.3 Hire Date Changes", 2)
    hd_df = df[df["fix_types"].str.contains("hire_date", na=False)].copy() if "fix_types" in df.columns else pd.DataFrame()
    if hd_df.empty:
        doc.add_paragraph("No hire date changes detected.")
    else:
        # Pattern breakdown
        if "hire_date_pattern" in hd_df.columns:
            pat_counts = hd_df["hire_date_pattern"].fillna("").replace("", "none").value_counts().head(8)
            _data_table(doc,
                ["Pattern Applied", "Count"],
                [[str(p), f"{c:,}"] for p, c in pat_counts.items()]
            )
        else:
            doc.add_paragraph(f"Total hire date changes: {len(hd_df):,}")
    doc.add_paragraph()


def _section_review_queue(doc: Document, df: pd.DataFrame) -> None:
    _add_heading(doc, "5. Priority Review Queue", 1)

    if "action" not in df.columns:
        doc.add_paragraph("(action column not available)")
        return

    review_df = df[df["action"] == "REVIEW"].copy()
    total_review = len(review_df)

    doc.add_paragraph(
        f"A total of {total_review:,} matched pairs require human review before any "
        f"corrections can be applied. The table below shows the top priority items by "
        f"fix type."
    )

    if total_review == 0:
        _callout(doc, "No pairs require human review. All approved records are ready for corrections.", level="ok")
        return

    # Top 20 by priority score
    if "priority_score" in review_df.columns:
        top_review = (
            review_df
            .sort_values("priority_score", ascending=False)
            .head(20)
        )
        cols = [c for c in ["pair_id","match_source","fix_types","priority_score","reason"] if c in top_review.columns]
        if cols:
            _data_table(doc, cols, top_review[cols].values.tolist())
    else:
        # No priority score — show by fix_type
        if "fix_types" in review_df.columns:
            ft_counts = {}
            for _, row in review_df.iterrows():
                for ft in str(row["fix_types"]).split("|"):
                    if ft:
                        ft_counts[ft] = ft_counts.get(ft, 0) + 1
            _data_table(doc,
                ["Fix Type", "Review Count"],
                [[ft, f"{c:,}"] for ft, c in sorted(ft_counts.items(), key=lambda x: -x[1])]
            )
    doc.add_paragraph()


def _section_rejected_matches(doc: Document, df: pd.DataFrame) -> None:
    _add_heading(doc, "6. Rejected Match Pairings", 1)

    if "action" not in df.columns:
        doc.add_paragraph("(action column not available)")
        return

    rej_df = df[df["action"] == "REJECT_MATCH"].copy()

    if rej_df.empty:
        _callout(doc, "No REJECT_MATCH records detected. All pairings appear valid.", level="ok")
        return

    doc.add_paragraph(
        f"{len(rej_df):,} pairs were flagged as REJECT_MATCH — likely wrong-person pairings "
        f"where automated matching selected an incorrect employee. These records have been "
        f"completely excluded from all correction pipelines and require manual investigation."
    )

    if "match_source" in rej_df.columns:
        src_counts = rej_df["match_source"].value_counts()
        _data_table(doc,
            ["Match Source", "REJECT_MATCH Count"],
            [[str(s), f"{c:,}"] for s, c in src_counts.items()]
        )
    doc.add_paragraph()


def _section_recommendations(doc: Document, df: pd.DataFrame) -> None:
    _add_heading(doc, "7. Recommendations", 1)

    total   = len(df)
    n_rev   = int((df["action"] == "REVIEW").sum())   if "action" in df.columns else 0
    n_rej   = int((df["action"] == "REJECT_MATCH").sum()) if "action" in df.columns else 0
    approve_rate = (total - n_rev - n_rej) / total if total > 0 else 0.0

    rec_items = []

    if n_rej > 0:
        rec_items.append(
            f"Investigate {n_rej:,} REJECT_MATCH pairs. These likely represent wrong-person "
            f"pairings introduced by fuzzy matching on insufficient keys. Consider tightening "
            f"the minimum confidence threshold for dob_name sources in policy.yaml."
        )

    if n_rev > 0:
        rec_items.append(
            f"Clear the review queue of {n_rev:,} pairs before re-running the corrections "
            f"pipeline. The held_corrections.csv file contains all deferred corrections."
        )

    if approve_rate < 0.80:
        rec_items.append(
            f"Auto-approval rate is {approve_rate:.1%}, below the recommended 80% threshold. "
            f"Review confidence thresholds in policy.yaml or investigate data source quality."
        )

    if "fix_types" in df.columns:
        active_zero_count = 0
        if "new_worker_status" in df.columns and "new_salary" in df.columns:
            am = df["new_worker_status"].fillna("").str.strip().str.lower() == "active"
            sn = pd.to_numeric(df["new_salary"].astype(str).str.replace(",","").str.replace("$",""), errors="coerce")
            active_zero_count = int((am & (sn.isna() | (sn == 0))).sum())
        if active_zero_count > 0:
            rec_items.append(
                f"Resolve {active_zero_count:,} active employees with $0 salary before re-running. "
                f"These indicate a data extraction failure in the source system."
            )

    if not rec_items:
        rec_items.append(
            "All data quality checks passed. The corrections pipeline can proceed. "
            "Review and apply the corrections in audit/corrections/out/ to the target HRIS."
        )

    for item in rec_items:
        p = doc.add_paragraph(style="List Bullet")
        p.add_run(item)

    doc.add_paragraph()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Audit Report Generator.")
    parser.add_argument("--db",   default=None, metavar="PATH",
                        help=f"SQLite DB path (default: {DB_PATH}).")
    parser.add_argument("--wide", default=None, metavar="PATH",
                        help=f"wide_compare.csv path (default: {WIDE_CSV}).")
    parser.add_argument("--out",  default=None, metavar="PATH",
                        help=f"Output .docx path (default: {OUT_PATH}).")
    args = parser.parse_args(argv)

    db_path   = Path(args.db)   if args.db   else DB_PATH
    wide_path = Path(args.wide) if args.wide else WIDE_CSV
    out_path  = Path(args.out)  if args.out  else OUT_PATH

    if not db_path.exists() and not wide_path.exists():
        print(f"[error] Neither DB ({db_path.name}) nor wide_compare.csv found.", file=sys.stderr)
        sys.exit(2)

    df = _load_data(db_path, wide_path)

    # Ensure numeric columns
    for col in ["salary_delta", "confidence", "priority_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ---------------------------------------------------------------------------
    # Build document
    # ---------------------------------------------------------------------------
    doc = Document()

    # Title page
    title   = doc.add_heading("Data Whisperer — Reconciliation Audit Report", 0)
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    for run in title.runs:
        run.font.color.rgb = _BLUE_DARK

    subtitle   = doc.add_paragraph(
        f"Generated: {datetime.now().strftime('%B %d, %Y %H:%M')}  |  "
        f"Records: {len(df):,}  |  Source: {db_path.name}"
    )
    subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
    doc.add_paragraph()

    # Sections
    _section_executive_summary(doc, df)
    doc.add_page_break()
    _section_match_quality(doc, df)
    doc.add_page_break()
    _section_data_quality(doc, df)
    doc.add_page_break()
    _section_field_changes(doc, df)
    doc.add_page_break()
    _section_review_queue(doc, df)
    _section_rejected_matches(doc, df)
    doc.add_page_break()
    _section_recommendations(doc, df)

    # Save
    out_path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(out_path))
    print(f"\n[generate_report] saved: {out_path.relative_to(ROOT)}")
    print(f"  sections: Executive Summary, Match Quality, Data Quality,")
    print(f"            Field Changes, Review Queue, Rejected Matches, Recommendations")


if __name__ == "__main__":
    main()
