"""
report.py - Page renderers and main orchestrator for the Reconciliation Audit PDF.

Pages:
  1. Cover page              - _draw_cover()
  2. Executive Summary       - _draw_exec_summary()
  3+. Findings by Severity   - _draw_findings()
  N.  Rejected Matches       - _draw_rejected_matches() (special BLOCKED section)
  N+1. Action Summary        - _draw_action_summary()
  N+2. Match Quality         - _draw_match_quality()

Entry point: build_pdf(run_id, wide_path, out_path, ...)

Absolute rules enforced here:
  - No em dashes anywhere - plain hyphens only
  - pair_id is always 12-char hex, never integer
  - SSN/DOB columns stripped before any output
  - No Unicode subscript/superscript - XML tags only
"""
from __future__ import annotations

import re
import sys
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd

from reportlab.pdfgen import canvas as rl_canvas
from reportlab.platypus import Paragraph

from .constants import (
    PAGE_W, PAGE_H, LM, RM, TW, BM, FH,
    CONTENT_TOP, CONTENT_BOTTOM, PAGE_HEADER_H,
    COLOR_NAVY, COLOR_WHITE, COLOR_LIGHT_GRAY, COLOR_MID_GRAY,
    COLOR_BORDER, COLOR_CHARCOAL, COLOR_TEAL,
    COLOR_CRITICAL, COLOR_HIGH, COLOR_MEDIUM, COLOR_LOW,
    COLOR_BLOCKED, COLOR_PASS, COLOR_WARN, COLOR_FAIL,
    COLOR_APPROVED, COLOR_REVIEW, COLOR_REJECTED, COLOR_HELD,
    COLOR_BLOCKED_TINT,
    FONT_REGULAR, FONT_BOLD, FONT_OBLIQUE,
    FONT_H2, FONT_BODY, FONT_SMALL,
    ACTION_LABELS, ACTION_COLORS,
    MATCH_SOURCE_LABELS, FIX_TYPE_LABELS,
    SEVERITY_LEVELS, SEVERITY_BLOCKED,
    get_gate_status, migration_readiness_score,
    FOOTER_CONFIDENTIAL, FOOTER_GENERATED_BY,
)
from .styles import (
    PS_BODY, PS_BODY_MUT, PS_BODY_B,
    PS_CELL, PS_CELL_MUT, PS_CELL_B, PS_CELL_SM, PS_CELL_SMWT, PS_CELL_WHT,
    PS_HDR, PS_HDR_SM,
    PS_CRITICAL, PS_HIGH, PS_MEDIUM, PS_LOW, PS_BLOCKED,
    PS_PASS, PS_WARN, PS_FAIL,
    PS_CAPTION, PS_CAPTION_C,
    para_height,
)
from .components import (
    draw_rect, draw_hrule, draw_text, draw_para, draw_table,
    draw_page_header, draw_footer, draw_section_header,
    draw_severity_badge, draw_finding_block,
    draw_callout_box, draw_sample_table,
    draw_stat_boxes, draw_readiness_bar, draw_bar_chart,
    draw_cover_page,
)


# ===========================================================================
# DATA HELPERS
# ===========================================================================

def _fmt_sal(val: Any) -> str:
    """Format a salary value as $150,000 or empty string."""
    if val is None:
        return ""
    try:
        s = str(val).strip().replace(",", "").replace("$", "")
        f = float(s)
        if f != f:   # NaN
            return ""
        return f"${f:,.0f}"
    except (ValueError, TypeError):
        return str(val) if val else ""


def _name_of(row: dict) -> str:
    """Return display name from a wide_compare row dict."""
    first = str(row.get("old_first_name_norm") or "").strip()
    last  = str(row.get("old_last_name_norm")  or "").strip()
    if first or last:
        return f"{first} {last}".strip()
    full = str(row.get("old_full_name_norm") or "").strip()
    if full:
        return " ".join(w.capitalize() for w in full.split())
    return str(row.get("pair_id", "-"))


def _fmt_fix_types(raw: str) -> str:
    """Convert pipe-separated fix_types to comma-separated human labels."""
    if not raw or str(raw).strip() in ("", "nan", "None"):
        return "No Changes"
    parts = [FIX_TYPE_LABELS.get(p.strip(), p.strip().title())
             for p in str(raw).split("|") if p.strip()]
    return ", ".join(parts) if parts else "No Changes"


# ---------------------------------------------------------------------------
# Wrong-match reason translator
# ---------------------------------------------------------------------------

_REASON_MAP: dict[str, str] = {
    "worker_id_auto_approve":              "Exact Worker ID match - auto-approved",
    "pk_auto_approve":                     "Matched on name, DOB, and last-4 - auto-approved",
    "hire_date:off_by_one_day_pattern":    "Hire date off by one day - auto-approved as system convention",
    "hire_date:year_shift_systematic":     "Hire date off by one year (systematic pattern) - auto-approved",
    "active_to_terminated":                "Status changed from active to terminated - needs review",
    "hire_date_wave":                      "Hire date is a bulk import date - needs review",
    "name_change_detected":                "Last name differs between systems - needs review",
    "payrate_conversion:annual_to_hourly": "Pay rate auto-approved: annual -> hourly (divided by 2080)",
    "payrate_conversion:hourly_to_annual": "Pay rate auto-approved: hourly -> annual (x 2080)",
    "payrate_conversion:biweekly_to_annual": "Pay rate auto-approved: biweekly -> annual (x 26)",
    "payrate_conversion:annual_to_biweekly": "Pay rate auto-approved: annual -> biweekly (divided by 26)",
}


def _translate_reason(reason: str) -> str:
    """Translate an internal engine flag to plain English."""
    if not reason or str(reason).strip() in ("", "nan", "None"):
        return "No reason recorded"
    r = str(reason).strip()
    if "|" in r:
        return "; ".join(_translate_reason(p.strip())
                         for p in r.split("|") if p.strip())
    r_inner    = re.sub(r"^reject_match:", "", r,      flags=re.IGNORECASE).strip()
    r_noprefix = re.sub(r"^[a-z_]+:",     "", r_inner, flags=re.IGNORECASE).strip()

    m = re.match(r"dob_name_low_confidence\s*\(?([\d.]+)<([\d.]+)", r_inner)
    if m:
        sc = int(round(float(m.group(1)) * 100))
        th = int(round(float(m.group(2)) * 100))
        return f"Name+DOB confidence {sc}% below {th}% minimum"

    m = re.match(r"dob_name_low_confidence\s*\(?([\d.]+)", r_inner)
    if m:
        sc = int(round(float(m.group(1)) * 100))
        return f"Name+DOB confidence too low ({sc}%)"

    m = re.match(r"salary_ratio_extreme\s*\(?([\d.]+)", r_noprefix)
    if m:
        ratio = float(m.group(1))
        if ratio > 1.5:
            return "Salary more than doubled - wrong-person signal"
        if ratio < 0.5:
            return "Salary dropped by more than half - wrong-person signal"
        return "Salary changed significantly - needs review"

    m = re.match(r"below_threshold\s*\(?([\d.]+)<([\d.]+)", r_noprefix)
    if m:
        sc = int(round(float(m.group(1)) * 100))
        th = int(round(float(m.group(2)) * 100))
        return f"Confidence {sc}% below the {th}% minimum required"

    m = re.match(r"name_change_detected\s*\(([^)]+)\)", r_noprefix)
    if m:
        return f"Last name changed ({m.group(1).strip()}) - verify same person"

    for candidate in (r_inner, r_noprefix, r):
        if candidate in _REASON_MAP:
            return _REASON_MAP[candidate]
    for key, val in _REASON_MAP.items():
        if any(c.startswith(key) for c in (r_inner, r_noprefix, r)):
            return val

    display = r_noprefix or r_inner or r
    return (display.replace("hire_date:", "hire date: ")
                   .replace("_", " ").strip().capitalize())


# ===========================================================================
# SAMPLE ROW BUILDERS
# ===========================================================================

def _build_salary_sample(df: pd.DataFrame) -> list[dict]:
    if "fix_types" not in df.columns:
        return []
    sal_df = df[df["fix_types"].str.contains("salary", na=False)].copy()
    if sal_df.empty:
        return []
    if "salary_delta" in sal_df.columns:
        sal_df = sal_df.reindex(
            sal_df["salary_delta"].abs().sort_values(ascending=False).index
        )
    rows = []
    for _, r in sal_df.head(8).iterrows():
        rd = r.to_dict()
        try:
            delta_raw = rd.get("salary_delta")
            delta_str = (f"${float(delta_raw):+,.0f}"
                         if delta_raw is not None
                         and str(delta_raw) not in ("", "nan") else "")
        except (ValueError, TypeError):
            delta_str = ""
        rows.append({
            "Worker ID":  str(rd.get("old_worker_id", ""))[:12],
            "Name":       _name_of(rd)[:22],
            "Old Salary": _fmt_sal(rd.get("old_salary")),
            "New Salary": _fmt_sal(rd.get("new_salary")),
            "Delta":      delta_str,
        })
    return rows


def _build_status_sample(df: pd.DataFrame) -> list[dict]:
    if "fix_types" not in df.columns:
        return []
    sts_df = df[df["fix_types"].str.contains("status", na=False)].copy()
    rows = []
    for _, r in sts_df.head(8).iterrows():
        rd = r.to_dict()
        rows.append({
            "Worker ID":  str(rd.get("old_worker_id", ""))[:12],
            "Name":       _name_of(rd)[:22],
            "Old Status": str(rd.get("old_worker_status", "") or "")[:18],
            "New Status": str(rd.get("new_worker_status", "") or "")[:18],
        })
    return rows


def _build_wrong_sample(df: pd.DataFrame) -> list[dict]:
    if "action" not in df.columns:
        return []
    rej_df = df[df["action"] == "REJECT_MATCH"].copy()
    rows = []
    for _, r in rej_df.head(8).iterrows():
        rd      = r.to_dict()
        src     = str(rd.get("match_source", "") or "")
        conf_v  = rd.get("confidence")
        try:
            conf_str = (f"{float(conf_v):.2f}"
                        if conf_v is not None and str(conf_v) not in ("", "nan")
                        else "")
        except (ValueError, TypeError):
            conf_str = ""
        reason = _translate_reason(str(rd.get("reason", "") or ""))[:45]
        rows.append({
            "Pair ID":      str(rd.get("pair_id", ""))[:12],
            "Name":         _name_of(rd)[:22],
            "Confidence":   conf_str,
            "Block Reason": reason,
        })
    return rows


def _build_az_sample(az_df: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in az_df.head(8).iterrows():
        rd = r.to_dict()
        rows.append({
            "Worker ID":  str(rd.get("old_worker_id", ""))[:12],
            "Name":       _name_of(rd)[:22],
            "Old Salary": _fmt_sal(rd.get("old_salary")),
            "New Salary": _fmt_sal(rd.get("new_salary")) or "$0",
            "Status":     str(rd.get("new_worker_status", "") or "")[:12],
        })
    return rows


def _build_conf_bands(df: pd.DataFrame) -> dict:
    if "confidence" not in df.columns:
        return {}
    conf = pd.to_numeric(df["confidence"], errors="coerce")
    return {
        "exact":   int((conf == 1.0).sum()),
        "high":    int(((conf >= 0.97) & (conf < 1.0)).sum()),
        "medium":  int(((conf >= 0.80) & (conf < 0.97)).sum()),
        "low":     int((conf < 0.80).sum()),
        "missing": int(conf.isna().sum()),
    }


def _build_wave_dates(df: pd.DataFrame) -> dict:
    """Return hire dates shared by >= 1% of records (wave-date candidates)."""
    if "new_hire_date" not in df.columns:
        return {}
    hd_counts = df["new_hire_date"].value_counts()
    threshold = max(2, int(len(df) * 0.01))
    return {str(k): int(v)
            for k, v in hd_counts[hd_counts >= threshold].head(6).items()}


# ===========================================================================
# DATA LOADING
# ===========================================================================

def _active_zero_mask(df: pd.DataFrame) -> "pd.Series":
    """Boolean mask: active workers where new_salary is 0 or null."""
    if "new_worker_status" not in df.columns or "new_salary" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    active = (df["new_worker_status"].fillna("")
              .str.strip().str.lower().isin(["active", ""]))
    sal    = pd.to_numeric(
        df["new_salary"].astype(str).str.replace(",", "").str.replace("$", ""),
        errors="coerce",
    )
    return active & (sal.isna() | (sal == 0.0))


def load_summary(wide_path: Path,
                 held_path: Path | None,
                 uo_path:   Path | None,
                 un_path:   Path | None,
                 manifest_path: Path | None,
                 review_path:   Path | None) -> dict:
    """
    Load all data files and compute the summary dict consumed by all renderers.
    PII columns (SSN, DOB) are stripped immediately after loading.
    """
    if not wide_path.exists():
        print(f"[pdf_generator] error: {wide_path} not found", file=sys.stderr)
        sys.exit(2)

    all_df = pd.read_csv(str(wide_path))

    # Strip PII - must not appear in any output
    _pii = [c for c in ("old_last4_ssn", "new_last4_ssn",
                         "old_dob", "new_dob") if c in all_df.columns]
    if _pii:
        all_df = all_df.drop(columns=_pii)

    # Ensure numeric columns
    for nc in ["salary_delta", "payrate_delta", "priority_score", "confidence"]:
        if nc in all_df.columns:
            all_df[nc] = pd.to_numeric(all_df[nc], errors="coerce")

    total    = len(all_df)
    n_safe   = int((all_df["action"] == "APPROVE").sum())      if "action" in all_df.columns else 0
    n_review = int((all_df["action"] == "REVIEW").sum())       if "action" in all_df.columns else 0
    n_wrong  = int((all_df["action"] == "REJECT_MATCH").sum()) if "action" in all_df.columns else 0
    safe_pct = round(n_safe / total * 100, 1) if total else 0.0

    def _ft(ft: str) -> int:
        if "fix_types" not in all_df.columns:
            return 0
        return int(all_df["fix_types"].str.contains(ft, na=False).sum())

    n_salary = _ft("salary")
    n_status = _ft("status")
    n_hdate  = _ft("hire_date")
    n_joborg = _ft("job_org")

    # Fuzzy matches (confidence < 1.0)
    n_fuzzy = 0
    if "confidence" in all_df.columns:
        n_fuzzy = int((pd.to_numeric(all_df["confidence"], errors="coerce") < 1.0).sum())

    # Match source breakdown
    src_counts: dict = {}
    if "match_source" in all_df.columns:
        src_counts = {str(k): int(v)
                      for k, v in all_df["match_source"].value_counts().items()}

    # Active/$0 salary
    az_mask    = _active_zero_mask(all_df)
    active_zero_df = all_df[az_mask].copy()
    n_az       = len(active_zero_df)

    # Salary delta stats (excluding Active/$0 records)
    sal_d = all_df["salary_delta"].copy() if "salary_delta" in all_df.columns else pd.Series(dtype=float)
    if n_az > 0 and len(sal_d):
        sal_d = sal_d.drop(active_zero_df.index, errors="ignore")
    sal_nz   = sal_d[sal_d != 0].dropna() if len(sal_d) else pd.Series(dtype=float)
    sal_mean = round(float(sal_nz.mean()),   2) if len(sal_nz) else 0.0
    sal_med  = round(float(sal_nz.median()), 2) if len(sal_nz) else 0.0
    sal_max  = round(float(sal_nz.max()),    2) if len(sal_nz) else 0.0
    sal_min  = round(float(sal_nz.min()),    2) if len(sal_nz) else 0.0

    # Priority bands (from review_queue or all_df REVIEW rows)
    rev_df = pd.DataFrame()
    if review_path and review_path.exists():
        try:
            rev_df = pd.read_csv(str(review_path))
        except Exception:
            pass
    if rev_df.empty and "action" in all_df.columns:
        rev_df = all_df[all_df["action"] == "REVIEW"].copy()

    prio_ser = (pd.to_numeric(rev_df.get("priority_score", pd.Series(dtype=float)),
                              errors="coerce")
                if not rev_df.empty else pd.Series(dtype=float))
    prio_bands = {
        "critical": int((prio_ser >= 70).sum()),
        "high":     int(((prio_ser >= 40) & (prio_ser < 70)).sum()),
        "medium":   int(((prio_ser >= 20) & (prio_ser < 40)).sum()),
        "low":      int((prio_ser  < 20).sum()),
    }

    # Top 10 review rows
    top_rows: list[dict] = []
    if not rev_df.empty and "priority_score" in rev_df.columns:
        want = [c for c in ["old_worker_id", "old_first_name_norm",
                            "old_last_name_norm", "old_full_name_norm",
                            "action", "fix_types", "priority_score"]
                if c in rev_df.columns]
        top_rows = (rev_df[want]
                    .sort_values("priority_score", ascending=False)
                    .head(10)
                    .to_dict(orient="records"))

    # Held corrections count
    n_held = 0
    if held_path and held_path.exists():
        try:
            held_df = pd.read_csv(str(held_path))
            if not held_df.empty and "hold_reason" in held_df.columns:
                held_df = held_df[
                    held_df["hold_reason"].str.contains(
                        r"BLOCKED|REJECT_MATCH|active_zero_salary_blocked",
                        na=False
                    )
                ]
            n_held = len(held_df)
        except Exception:
            pass

    # Unmatched record counts
    uo_count = un_count = 0
    for path, attr in [(uo_path, "uo"), (un_path, "un")]:
        if path and path.exists():
            try:
                cnt = len(pd.read_csv(str(path)))
                if attr == "uo":
                    uo_count = cnt
                else:
                    un_count = cnt
            except Exception:
                pass

    # Corrections manifest count
    n_manifest = 0
    if manifest_path and manifest_path.exists():
        try:
            n_manifest = len(pd.read_csv(str(manifest_path)))
        except Exception:
            pass

    # Payrate conversion breakdown
    conv_counts: dict = {}
    if "conversion_type" in all_df.columns:
        ct = all_df["conversion_type"].fillna("").str.strip()
        conv_counts = {str(k): int(v)
                       for k, v in ct[ct != ""].value_counts().items()}

    return {
        # Counts
        "total_matched":        total,
        "n_safe":               n_safe,
        "n_review":             n_review,
        "n_wrong_match":        n_wrong,
        "n_held":               n_held,
        "n_manifest":           n_manifest,
        "safe_pct":             safe_pct,
        "unmatched_old":        uo_count,
        "unmatched_new":        un_count,
        # Mismatch breakdown
        "n_salary":             n_salary,
        "n_status":             n_status,
        "n_hire_date":          n_hdate,
        "n_job_org":            n_joborg,
        "n_fuzzy":              n_fuzzy,
        "n_active_zero_salary": n_az,
        # Salary stats
        "salary_delta_mean":    sal_mean,
        "salary_delta_median":  sal_med,
        "salary_delta_max":     sal_max,
        "salary_delta_min":     sal_min,
        # Distributions
        "match_source_counts":  src_counts,
        "priority_bands":       prio_bands,
        "top_review_rows":      top_rows,
        "confidence_bands":     _build_conf_bands(all_df),
        "wave_dates":           _build_wave_dates(all_df),
        "conversion_counts":    conv_counts,
        # Sample rows for findings
        "salary_sample":        _build_salary_sample(all_df),
        "status_sample":        _build_status_sample(all_df),
        "wrong_sample":         _build_wrong_sample(all_df),
        "active_zero_sample":   _build_az_sample(active_zero_df),
        # Source file name
        "wide_src":             wide_path.name,
    }


# ===========================================================================
# PAGE RENDERERS
# ===========================================================================

def _draw_exec_summary(c, y: float, page_num: int, total_pages: int,
                       summary: dict, org_name: str, run_id: str,
                       date_str: str) -> None:
    """Page 2: Executive Summary."""
    total     = int(summary.get("total_matched", 0))
    n_safe    = int(summary.get("n_safe", 0))
    n_review  = int(summary.get("n_review", 0))
    n_wrong   = int(summary.get("n_wrong_match", 0))
    n_held    = int(summary.get("n_held", 0))
    uo        = int(summary.get("unmatched_old", 0))
    un        = int(summary.get("unmatched_new", 0))
    safe_pct  = float(summary.get("safe_pct", 0.0))
    n_salary  = int(summary.get("n_salary", 0))
    n_status  = int(summary.get("n_status", 0))
    n_hdate   = int(summary.get("n_hire_date", 0))
    n_joborg  = int(summary.get("n_job_org", 0))
    n_fuzzy   = int(summary.get("n_fuzzy", 0))
    n_az      = int(summary.get("n_active_zero_salary", 0))
    wide_src  = str(summary.get("wide_src", "wide_compare.csv"))
    score     = migration_readiness_score(safe_pct, n_az, n_wrong, total)

    # ---- What We Found narrative ----
    y = draw_section_header(c, y, "EXECUTIVE SUMMARY")

    gate_lbl, _ = get_gate_status(safe_pct, n_az, n_wrong)
    narrative = (
        f"This reconciliation analyzed <b>{total:,}</b> matched record pairs from "
        f"<b>{wide_src}</b> on {date_str}. "
    )
    if safe_pct >= 90:
        narrative += (
            f"<b>{safe_pct:.1f}% of records ({n_safe:,}) are Safe</b> and "
            "approved for automated import. "
        )
    elif safe_pct >= 80:
        narrative += (
            f"<b>{safe_pct:.1f}% of records ({n_safe:,}) are Safe.</b> "
            "The dataset is approaching import readiness - review queue work is needed. "
        )
    else:
        narrative += (
            f"Only <b>{safe_pct:.1f}% of records ({n_safe:,}) are Safe.</b> "
            "Significant review work is required before this dataset is ready for import. "
        )
    if n_review > 0:
        narrative += (
            f"<b>{n_review:,} records need manual review</b> before corrections can execute. "
        )
    if n_wrong > 0:
        narrative += (
            f"<b>{n_wrong:,} records are blocked as wrong-person matches</b> and must be "
            "manually investigated before any corrections are applied to them. "
        )
    if n_az > 0:
        narrative += (
            f"<b>CRITICAL: {n_az:,} active employees show $0 salary</b> in the new system - "
            "these are blocked from corrections and must be resolved before import. "
        )
    if uo > 0 or un > 0:
        narrative += (
            f"<b>{uo:,} old-system and {un:,} new-system records could not be matched</b> - "
            "these require investigation. "
        )

    y = draw_para(c, LM, y, narrative, PS_BODY, TW)
    y += 8

    # Migration Readiness Score bar
    draw_text(c, LM, y + 10, "Migration Readiness Score",
              font=FONT_BOLD, size=9, color=COLOR_NAVY)
    y += 14
    y = draw_readiness_bar(c, LM, y, score, w=280, h=16)
    y += 8

    # ---- Audit Scope Table ----
    y = draw_section_header(c, y, "Audit Scope Checklist")

    def _res_cell(n: int, noun: str = "found") -> Paragraph:
        if n == 0:
            return Paragraph("PASS", PS_PASS)
        return Paragraph(f"{n:,} {noun}", PS_HIGH)

    scope_cw = [160, 226, 140]   # = 526pt
    scope_rows = [
        [Paragraph("<b>Check Category</b>",    PS_HDR),
         Paragraph("<b>What Was Checked</b>",  PS_HDR),
         Paragraph("<b>Result</b>",            PS_HDR)],
        [Paragraph("Match Quality",            PS_CELL),
         Paragraph("Confidence scores, match source distribution", PS_CELL),
         _res_cell(n_fuzzy, "low-confidence")],
        [Paragraph("Payrate Conversion",       PS_CELL),
         Paragraph("Unit conversion artefacts, unconverted rates", PS_CELL),
         _res_cell(int(summary.get("n_salary", 0)), "found")],
        [Paragraph("Salary Integrity",         PS_CELL),
         Paragraph("Active employees with $0 salary", PS_CELL),
         Paragraph(f"{n_az:,} CRITICAL", PS_CRITICAL) if n_az > 0
         else Paragraph("PASS", PS_PASS)],
        [Paragraph("Hire Date Analysis",       PS_CELL),
         Paragraph("Bulk import waves, off-by-one, year shifts", PS_CELL),
         _res_cell(n_hdate, "mismatches")],
        [Paragraph("Rejected Matches",         PS_CELL),
         Paragraph("Wrong-person pairings, blocked records",      PS_CELL),
         Paragraph(f"{n_wrong:,} BLOCKED", PS_BLOCKED) if n_wrong > 0
         else Paragraph("PASS", PS_PASS)],
        [Paragraph("Status Changes",           PS_CELL),
         Paragraph("Status field differences", PS_CELL),
         _res_cell(n_status, "changes")],
        [Paragraph("Salary Changes",           PS_CELL),
         Paragraph("Non-conversion salary differences", PS_CELL),
         _res_cell(n_salary, "changes")],
        [Paragraph("Data Completeness",        PS_CELL),
         Paragraph("Missing fields across all records", PS_CELL),
         Paragraph(f"{safe_pct:.1f}% complete", PS_PASS if safe_pct >= 90 else PS_HIGH)],
    ]
    y = draw_table(c, LM, y, scope_cw, scope_rows, hdr_bg=COLOR_NAVY, font_size=9)
    y += 12

    # ---- Files in this package ----
    if y + 100 > CONTENT_BOTTOM:
        return

    y = draw_section_header(c, y, "Files Included in This Download Package")
    files = [
        ("Reconciliation Audit Report (this document)", "PDF"),
        ("review_queue.csv",          "Records requiring human review"),
        ("corrections_manifest.csv",  "Safe corrections ready to load into target system"),
        ("held_corrections.csv",      "Blocked records requiring manual review before loading"),
        ("rejected_matches.csv",      "Wrong-person pairs blocked from corrections entirely"),
        ("recon_workbook.xlsx",       "Full Excel workbook - all sheets and raw data"),
    ]
    file_cw = [260, 266]
    file_rows = [[Paragraph("<b>File</b>", PS_HDR), Paragraph("<b>Contents</b>", PS_HDR)]]
    for fname, desc in files:
        file_rows.append([Paragraph(fname, PS_CELL_B), Paragraph(desc, PS_CELL)])
    y = draw_table(c, LM, y, file_cw, file_rows, hdr_bg=COLOR_NAVY, font_size=9)
    y += 10


def _draw_findings(c, y_start: float, page_num: int, total_pages: int,
                   summary: dict, org_name: str, run_id: str) -> tuple[int, float]:
    """
    Pages 3+: Findings by Severity.
    Returns (final_page_num, final_y).
    """
    y = y_start

    n_salary  = int(summary.get("n_salary", 0))
    n_status  = int(summary.get("n_status", 0))
    n_hdate   = int(summary.get("n_hire_date", 0))
    n_fuzzy   = int(summary.get("n_fuzzy", 0))
    n_az      = int(summary.get("n_active_zero_salary", 0))
    sal_mean  = float(summary.get("salary_delta_mean", 0) or 0)
    sal_med   = float(summary.get("salary_delta_median", 0) or 0)
    sal_max   = float(summary.get("salary_delta_max", 0) or 0)
    sal_min   = float(summary.get("salary_delta_min", 0) or 0)
    safe_pct  = float(summary.get("safe_pct", 0.0))

    def _overflow(title: str = "FINDINGS BY SEVERITY (continued)"):
        nonlocal page_num, y
        c.showPage()
        page_num += 1
        y = draw_page_header(c, page_num, total_pages, title, org_name)
        draw_footer(c, page_num, total_pages, org_name, run_id)
        return page_num, y

    findings: list[dict] = []

    # CRITICAL: Active/$0 salary
    if n_az > 0:
        findings.append({
            "label": "Active Workers with $0 Salary",
            "sev":   SEVERITY_LEVELS[0],   # CRITICAL
            "count": n_az,
            "what": (
                f"<b>{n_az:,} active workers have $0 or missing salary</b> in the new "
                "system data. These records cannot be safely imported - they would zero "
                "out an active employee's compensation in the target system."
            ),
            "why": (
                "An active employee with a $0 salary entry represents a mapping failure "
                "or data artifact, not a real salary of zero. If loaded into the target "
                "system, the employee would not receive pay - creating a direct payroll "
                "risk, employee complaints, and potential regulatory exposure."
            ),
            "action": (
                "Do not import these records until each employee's correct salary is "
                "confirmed and entered. See the CRITICAL_Zero_Salary sheet in the "
                "workbook for the full list. Resolve all entries before proceeding."
            ),
            "sample":     summary.get("active_zero_sample") or [],
            "sample_src": "CRITICAL_Zero_Salary (workbook sheet)",
        })

    # HIGH: Low-confidence matches
    if n_fuzzy > 0:
        findings.append({
            "label": "Low-Confidence Matches (< 1.00)",
            "sev":   SEVERITY_LEVELS[1],   # HIGH
            "count": n_fuzzy,
            "what": (
                f"<b>{n_fuzzy:,} matched pairs have confidence scores below 1.0,</b> "
                "meaning the engine used name, date-of-birth, or hire-date signals "
                "rather than an exact Worker ID match."
            ),
            "why": (
                "Fuzzy matches carry a higher risk of wrong-person pairings. While most "
                "will be correct, the lower the confidence score, the more likely the "
                "two records belong to different people. Applying corrections to a wrong "
                "match affects both employees."
            ),
            "action": (
                "Spot-check a representative sample of fuzzy matches before import. "
                "Records with confidence below 0.80 have been flagged for manual review. "
                "Check the Match_Confidence column in the All_Matches sheet."
            ),
            "sample":     [],
            "sample_src": "All_Matches (workbook sheet) - sort by confidence ascending",
        })

    # HIGH: Salary mismatches
    if n_salary > 0:
        sal_stmt = f"Mean change: ${sal_mean:+,.0f}. Median: ${sal_med:+,.0f}."
        if sal_max > 0:
            sal_stmt += f" Largest increase: ${sal_max:+,.0f}."
        if sal_min < 0:
            sal_stmt += f" Largest decrease: ${sal_min:+,.0f}."
        findings.append({
            "label": "Salary and Pay Rate Mismatches",
            "sev":   SEVERITY_LEVELS[1],   # HIGH
            "count": n_salary,
            "what": (
                f"<b>{n_salary:,} matched records have salary or pay rate differences</b> "
                f"between the old and new system. {sal_stmt}"
            ),
            "why": (
                "Salary differences require review to confirm which system holds the "
                "correct value. A mismatch may reflect a legitimate pay change during "
                "the migration window, a data entry error, or a mapping problem. "
                "Applying the wrong salary creates payroll risk."
            ),
            "action": (
                "Review the Salary_Mismatches sheet. For each record, confirm which "
                "system holds the correct current salary. Records marked Safe have been "
                "auto-approved - spot-check the largest deltas before final import."
            ),
            "sample":     summary.get("salary_sample") or [],
            "sample_src": "Salary_Mismatches (workbook sheet)",
        })

    # MEDIUM: Hire date mismatches
    if n_hdate > 0:
        wave_dates = summary.get("wave_dates") or {}
        n_wave = sum(wave_dates.values()) if wave_dates else 0
        hdate_what = (
            f"<b>{n_hdate:,} matched records have different hire dates</b> between the "
            "old and new system. "
        )
        if n_wave > 0:
            hdate_what += (
                f"<b>{n_wave:,} records share hire dates with 1% or more of all records</b> - "
                "these are likely bulk-import placeholder dates, not actual start dates."
            )
        findings.append({
            "label": "Hire Date Mismatches",
            "sev":   SEVERITY_LEVELS[2],   # MEDIUM
            "count": n_hdate,
            "what":  hdate_what,
            "why": (
                "Incorrect hire dates affect seniority calculations, benefits eligibility "
                "dates, and compliance reporting. A wave-date placeholder loaded as a "
                "real hire date will produce systematically wrong results for all "
                "affected employees."
            ),
            "action": (
                "Review the HireDate_Mismatches sheet. Records with 'wave_date' in the "
                "hire_date_pattern column are likely placeholder dates and need "
                "correction. For others, verify the correct date from offer letters or "
                "the source system."
            ),
            "sample":     [],
            "sample_src": "HireDate_Mismatches (workbook sheet)",
        })

    # LOW: Status changes
    if n_status > 0:
        findings.append({
            "label": "Employment Status Mismatches",
            "sev":   SEVERITY_LEVELS[3],   # LOW
            "count": n_status,
            "what": (
                f"<b>{n_status:,} matched records have different employment status</b> "
                "between the old and new system - for example, Active in one system "
                "and Inactive or Terminated in the other."
            ),
            "why": (
                "Importing the wrong status can activate terminated employees, deactivate "
                "current employees, or misroute payroll and benefits. Status changes must "
                "be verified against a source of truth before applying any correction."
            ),
            "action": (
                "Review every record in the Status_Mismatches sheet. For each, determine "
                "the employee's actual current status from payroll records or direct "
                "manager confirmation. Do not import status corrections without verification."
            ),
            "sample":     summary.get("status_sample") or [],
            "sample_src": "Status_Mismatches (workbook sheet)",
        })

    # LOW: Below 80% approval rate
    if safe_pct < 80.0:
        findings.append({
            "label": "Auto-Approval Rate Below 80% Threshold",
            "sev":   SEVERITY_LEVELS[3],   # LOW
            "count": int(summary.get("total_matched", 0)) - int(summary.get("n_safe", 0)),
            "what": (
                f"The auto-approval rate is <b>{safe_pct:.1f}%</b>, which is below the "
                "recommended 80% minimum threshold. "
                f"{int(summary.get('total_matched',0)) - int(summary.get('n_safe',0)):,} "
                "records were not auto-approved and require human review."
            ),
            "why": (
                "A low approval rate means more records than expected need human "
                "disposition before corrections can execute. This increases the manual "
                "workload and delays the migration timeline."
            ),
            "action": (
                "Check whether the source data extract is complete and whether any "
                "fields are missing or malformed. Review the sanity_gate.json file for "
                "specific threshold violations. Work through the Review_Queue sheet "
                "systematically starting from the highest priority scores."
            ),
            "sample":     [],
            "sample_src": "",
        })

    # Draw the severity-grouped header
    y = draw_section_header(c, y, "FINDINGS BY SEVERITY")
    y += 4

    # Severity group counts summary
    sev_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for f in findings:
        sev_counts[f["sev"]["label"]] = sev_counts.get(f["sev"]["label"], 0) + 1

    badge_x = LM
    for sev in SEVERITY_LEVELS:
        cnt = sev_counts.get(sev["label"], 0)
        draw_severity_badge(c, badge_x, y + 12, sev, size=16)
        draw_text(c, badge_x + 20, y + 14,
                  f"{sev['label']}  {cnt} finding{'s' if cnt != 1 else ''}",
                  font=FONT_REGULAR, size=8, color=COLOR_MID_GRAY)
        badge_x += 130
    y += 26

    draw_hrule(c, y, color=COLOR_BORDER)
    y += 10

    # Draw each finding block
    first = True
    for finding in findings:
        if not first:
            draw_hrule(c, y + 4, color=COLOR_BORDER, lw=0.3)
            y += 12
        first = False

        def _overflow_with_title(f=finding):
            return _overflow(f"FINDINGS BY SEVERITY - {f['sev']['label']}")

        page_num, y = draw_finding_block(
            c, y, finding,
            page_num, total_pages,
            org_name, run_id,
            _overflow_with_title,
        )

    if not findings:
        y = draw_para(c, LM, y,
                      "No significant findings requiring attention were identified "
                      "in this reconciliation run.", PS_BODY_MUT, TW)
        y += 10

    return page_num, y


def _draw_rejected_matches(c, y: float, page_num: int, total_pages: int,
                            summary: dict, org_name: str,
                            run_id: str) -> tuple[int, float]:
    """
    Dedicated REJECTED MATCHES section with special BLOCKED/purple treatment.
    This is a hard migration block - gets its own section after findings.
    Returns (page_num, y).
    """
    n_wrong = int(summary.get("n_wrong_match", 0))
    if n_wrong == 0:
        return page_num, y

    # Section header with purple accent
    draw_rect(c, LM, y, 3, 24, fill=COLOR_BLOCKED)
    draw_text(c, LM + 10, y + 17,
              "REJECTED MATCHES - BLOCKED FROM CORRECTIONS",
              font=FONT_BOLD, size=FONT_H2, color=COLOR_BLOCKED)
    y += 32

    # Opening callout (light purple background)
    callout_text = (
        f"<b>These {n_wrong:,} records are BLOCKED from corrections entirely.</b> "
        "Do not load any corrections for these employees until manual investigation "
        "is complete. Each wrong-match pair must be verified and either re-matched "
        "or excluded from the migration before any correction file is generated."
    )
    y = draw_callout_box(c, LM, y, TW, callout_text,
                         border_color=COLOR_BLOCKED,
                         bg_color=COLOR_BLOCKED_TINT,
                         style=PS_BODY)

    # Badge + finding block
    finding = {
        "label":  "Wrong-Person Matches (Identity Not Confirmed)",
        "sev":    SEVERITY_BLOCKED,
        "count":  n_wrong,
        "what": (
            f"<b>{n_wrong:,} matched pairs were rejected by the engine</b> because "
            "match confidence was too low to confirm the two records belong to the "
            "same person. These are marked REJECT_MATCH."
        ),
        "why": (
            "Applying corrections to a wrong match means one employee's data changes "
            "are applied to a different person. This is a data integrity failure and "
            "can cause payroll, benefits, and compliance errors affecting both "
            "employees. Wrong matches must be resolved manually before any corrections "
            "are applied to them."
        ),
        "action": (
            "For each pair shown below, verify whether the two records belong to the "
            "same person. If confirmed: re-run the matcher with the correct linking "
            "field. If different people: route each to the correct record and "
            "investigate how they were matched."
        ),
        "sample":     summary.get("wrong_sample") or [],
        "sample_src": "held_corrections.csv / Held_Corrections (workbook sheet)",
    }

    def _overflow():
        nonlocal page_num, y
        c.showPage()
        page_num += 1
        y = draw_page_header(c, page_num, total_pages,
                             "REJECTED MATCHES", org_name)
        draw_footer(c, page_num, total_pages, org_name, run_id)
        return page_num, y

    page_num, y = draw_finding_block(
        c, y, finding, page_num, total_pages, org_name, run_id, _overflow
    )

    return page_num, y


def _draw_action_summary(c, y: float, page_num: int, total_pages: int,
                          summary: dict, org_name: str, run_id: str) -> None:
    """Action Summary page: priority bands, corrections, top-10 queue."""
    total      = int(summary.get("total_matched", 0))
    n_safe     = int(summary.get("n_safe", 0))
    n_review   = int(summary.get("n_review", 0))
    n_wrong    = int(summary.get("n_wrong_match", 0))
    n_held     = int(summary.get("n_held", 0))
    n_manifest = int(summary.get("n_manifest", 0))
    top_rows   = summary.get("top_review_rows") or []
    prio_bands = summary.get("priority_bands") or {}

    n_critical = int(prio_bands.get("critical", 0))
    n_high     = int(prio_bands.get("high", 0))
    n_medium   = int(prio_bands.get("medium", 0))
    n_low      = int(prio_bands.get("low", 0))

    # ---- Priority Band table ----
    y = draw_section_header(c, y, "Review Queue Priority Breakdown")
    prio_cw  = [130, 130, 136, 130]   # = 526pt
    prio_rows = [
        [Paragraph("<b>Severity Band</b>", PS_HDR),
         Paragraph("<b>Score Range</b>",   PS_HDR),
         Paragraph("<b>Records</b>",       PS_HDR),
         Paragraph("<b>Required Action</b>", PS_HDR)],
        [Paragraph("CRITICAL", PS_CRITICAL),
         Paragraph("70 or above", PS_CELL),
         Paragraph(f"{n_critical:,}", PS_CRITICAL if n_critical > 0 else PS_CELL),
         Paragraph("Resolve before import", PS_CELL)],
        [Paragraph("HIGH", PS_HIGH),
         Paragraph("40 - 69", PS_CELL),
         Paragraph(f"{n_high:,}", PS_HIGH if n_high > 0 else PS_CELL),
         Paragraph("Review before import", PS_CELL)],
        [Paragraph("MEDIUM", PS_MEDIUM),
         Paragraph("20 - 39", PS_CELL),
         Paragraph(f"{n_medium:,}", PS_MEDIUM if n_medium > 0 else PS_CELL),
         Paragraph("Spot-check advised", PS_CELL)],
        [Paragraph("LOW", PS_LOW),
         Paragraph("0 - 19", PS_CELL),
         Paragraph(f"{n_low:,}", PS_CELL),
         Paragraph("Auto-approve candidate", PS_CELL)],
    ]
    y = draw_table(c, LM, y, prio_cw, prio_rows, hdr_bg=COLOR_NAVY, font_size=9)
    y += 12

    # ---- Record Disposition Summary ----
    y = draw_section_header(c, y, "Record Disposition Summary")
    act_cw = [200, 136, 190]   # = 526pt
    act_rows = [
        [Paragraph("<b>Disposition</b>", PS_HDR),
         Paragraph("<b>Count</b>",       PS_HDR),
         Paragraph("<b>Next Step</b>",   PS_HDR)],
        [Paragraph("Safe - corrections ready",            PS_CELL),
         Paragraph(f"{n_safe:,}",    PS_PASS),
         Paragraph("Apply via corrections_manifest.csv",  PS_CELL)],
        [Paragraph("Needs Review - manual required",       PS_CELL),
         Paragraph(f"{n_review:,}",  PS_WARN if n_review > 0 else PS_CELL),
         Paragraph("Work through Review_Queue sheet",      PS_CELL)],
        [Paragraph("Wrong Match - identity not confirmed", PS_CELL),
         Paragraph(f"{n_wrong:,}",   PS_BLOCKED if n_wrong > 0 else PS_CELL),
         Paragraph("Investigate in Held_Corrections",      PS_CELL)],
        [Paragraph("Held - blocked from auto-processing",  PS_CELL),
         Paragraph(f"{n_held:,}",    PS_CELL_MUT if n_held > 0 else PS_CELL),
         Paragraph("Manual decision required per record",  PS_CELL)],
        [Paragraph("<b>Total Matched</b>",                 PS_CELL_B),
         Paragraph(f"<b>{total:,}</b>",                    PS_CELL_B),
         Paragraph("",                                     PS_CELL)],
    ]
    y = draw_table(c, LM, y, act_cw, act_rows, hdr_bg=COLOR_NAVY, font_size=9)
    y += 12

    # ---- Corrections Manifest ----
    mf_text = (
        f"The corrections manifest contains <b>{n_manifest:,} corrections</b> from "
        f"{n_safe:,} Safe-approved records. These have passed all automated checks "
        "and are ready to apply to the target system. Review the "
        "Corrections_Manifest sheet in the workbook before applying."
    ) if n_manifest > 0 else (
        "No corrections have been generated yet. Run generate_corrections.py to "
        "produce the corrections manifest from Safe-approved records."
    )
    y = draw_para(c, LM, y, mf_text, PS_BODY, TW)
    y += 12

    # ---- Top 10 Review Records ----
    if top_rows and y + 120 < CONTENT_BOTTOM:
        y = draw_section_header(c, y, "Top Priority Records for Review")
        draw_text(c, LM, y + 10,
                  "Review these records first - highest priority scores.",
                  font=FONT_OBLIQUE, size=8, color=COLOR_MID_GRAY)
        y += 16

        top_cw = [76, 126, 86, 138, 100]   # = 526pt
        top_hdr = [
            Paragraph("<b>Worker ID</b>",   PS_HDR_SM),
            Paragraph("<b>Name</b>",        PS_HDR_SM),
            Paragraph("<b>Action</b>",      PS_HDR_SM),
            Paragraph("<b>Fix Types</b>",   PS_HDR_SM),
            Paragraph("<b>Priority</b>",    PS_HDR_SM),
        ]
        tbl = [top_hdr]
        for tr in top_rows[:10]:
            act   = str(tr.get("action", ""))
            ft    = str(tr.get("fix_types", ""))
            prio  = tr.get("priority_score")
            name  = str(tr.get("old_first_name_norm") or tr.get("old_full_name_norm") or "")
            ln    = str(tr.get("old_last_name_norm") or "")
            if ln and name:
                name = f"{name} {ln}"
            prio_style = (PS_CRITICAL if (prio or 0) >= 70 else
                          PS_HIGH     if (prio or 0) >= 40 else PS_CELL_SM)
            tbl.append([
                Paragraph(str(tr.get("old_worker_id") or ""),  PS_CELL_SM),
                Paragraph(name[:24],                           PS_CELL_SM),
                Paragraph(ACTION_LABELS.get(act, act),         PS_CELL_SM),
                Paragraph(_fmt_fix_types(ft)[:28],             PS_CELL_SM),
                Paragraph(str(int(prio)) if prio is not None else "-", prio_style),
            ])
        y = draw_table(c, LM, y, top_cw, tbl,
                       hdr_bg=COLOR_NAVY, font_size=8, pad=4, min_h=16)
        y += 10

    # ---- What Happens Next ----
    if y + 90 < CONTENT_BOTTOM:
        y = draw_section_header(c, y, "What Happens Next")
        steps: list[tuple[str, str, object]] = []
        step_n = 1
        n_az_local = int(summary.get("n_active_zero_salary", 0))
        if n_az_local > 0:
            steps.append((f"Step {step_n}",
                          f"Resolve {n_az_local:,} Active/$0 salary records. "
                          "See CRITICAL_Zero_Salary sheet.", COLOR_CRITICAL))
            step_n += 1
        if n_wrong > 0:
            steps.append((f"Step {step_n}",
                          f"Investigate {n_wrong:,} Wrong Match records in Held_Corrections. "
                          "Verify each employee's identity manually.", COLOR_BLOCKED))
            step_n += 1
        if n_review > 0:
            steps.append((f"Step {step_n}",
                          f"Work through Review_Queue ({n_review:,} records). "
                          "Start with Critical (>= 70), then High (40-69).", COLOR_HIGH))
            step_n += 1
        steps.append((f"Step {step_n}",
                      "Apply corrections_manifest.csv to the target system once the review "
                      "queue is cleared. Spot-check 5% before full import.", COLOR_PASS))
        step_n += 1
        steps.append((f"Step {step_n}",
                      "Re-run reconciliation after corrections are applied to confirm "
                      "remaining mismatches are within acceptable tolerance.", COLOR_MID_GRAY))

        rec_cw = [76, 450]   # = 526pt
        rec_rows = [[Paragraph("<b>Step</b>", PS_HDR), Paragraph("<b>Action</b>", PS_HDR)]]
        for lbl, txt, col in steps:
            from reportlab.lib.styles import ParagraphStyle as _PS
            from reportlab.lib.enums import TA_LEFT as _TAL
            lbl_st = _PS(f"_lbl_{lbl.replace(' ','')}",
                         fontName=FONT_BOLD, fontSize=9, textColor=col,
                         leading=13, alignment=_TAL, wordWrap="LTR")
            rec_rows.append([Paragraph(lbl, lbl_st), Paragraph(txt, PS_CELL)])
        y = draw_table(c, LM, y, rec_cw, rec_rows, hdr_bg=COLOR_NAVY, font_size=9)
        y += 10


def _draw_match_quality(c, y: float, page_num: int, total_pages: int,
                         summary: dict, org_name: str, run_id: str) -> None:
    """Match Quality and Patterns page."""
    total      = int(summary.get("total_matched", 0))
    n_fuzzy    = int(summary.get("n_fuzzy", 0))
    src_counts = summary.get("match_source_counts") or {}
    sal_mean   = float(summary.get("salary_delta_mean",   0) or 0)
    sal_med    = float(summary.get("salary_delta_median", 0) or 0)
    sal_max    = float(summary.get("salary_delta_max",    0) or 0)
    sal_min    = float(summary.get("salary_delta_min",    0) or 0)
    sal_rows   = int(summary.get("n_salary",  0))
    n_az       = int(summary.get("n_active_zero_salary", 0))
    n_salary   = int(summary.get("n_salary",  0))
    n_status   = int(summary.get("n_status",  0))
    n_hdate    = int(summary.get("n_hire_date", 0))
    n_joborg   = int(summary.get("n_job_org",   0))

    # ---- Match Source Breakdown ----
    y = draw_section_header(c, y, "Match Source Breakdown")
    if src_counts:
        src_cw = [180, 116, 100, 130]   # = 526pt
        src_rows = [
            [Paragraph("<b>Match Source</b>",   PS_HDR),
             Paragraph("<b>Count</b>",          PS_HDR),
             Paragraph("<b>% of Total</b>",     PS_HDR),
             Paragraph("<b>Confidence Type</b>", PS_HDR)]
        ]
        for src, cnt in sorted(src_counts.items(), key=lambda x: -x[1]):
            pct      = round(cnt / total * 100, 1) if total else 0
            is_exact = str(src).lower() in ("worker_id", "pk")
            src_lbl  = MATCH_SOURCE_LABELS.get(str(src).lower(), str(src))
            src_rows.append([
                Paragraph(src_lbl, PS_CELL),
                Paragraph(f"{cnt:,}", PS_CELL),
                Paragraph(f"{pct:.1f}%", PS_CELL),
                Paragraph("Exact", PS_PASS) if is_exact else Paragraph("Fuzzy / Name", PS_HIGH),
            ])
        y = draw_table(c, LM, y, src_cw, src_rows, hdr_bg=COLOR_NAVY, font_size=9)
        if n_fuzzy > 0:
            y += 4
            pct_str = f"{round(n_fuzzy/total*100,1):.1f}%" if total else "0.0%"
            y = draw_para(c, LM, y,
                          f"<b>{n_fuzzy:,} records ({pct_str}) matched with confidence "
                          f"below 1.0.</b> Spot-check these before import.",
                          PS_BODY, TW)
    else:
        y = draw_para(c, LM, y, "Match source data not available.", PS_BODY_MUT, TW)
    y += 12

    # ---- Match Confidence Distribution bar chart ----
    conf_bands = summary.get("confidence_bands") or {}
    if conf_bands and y + 130 < CONTENT_BOTTOM:
        y = draw_section_header(c, y, "Match Confidence Distribution")
        categories = ["Exact (1.00)", "High (0.97-0.99)",
                      "Medium (0.80-0.96)", "Low (<0.80)", "Missing"]
        values     = [conf_bands.get("exact", 0), conf_bands.get("high", 0),
                      conf_bands.get("medium", 0), conf_bands.get("low", 0),
                      conf_bands.get("missing", 0)]
        bar_colors = [COLOR_PASS, COLOR_PASS, COLOR_HIGH, COLOR_CRITICAL, COLOR_MID_GRAY]

        # Draw each category as its own bar (color-coded)
        bar_y = y + 4
        bar_h = 14
        bar_gap = 6
        max_val = max(values) if any(v > 0 for v in values) else 1
        for cat, val, bcol in zip(categories, values, bar_colors):
            fill_w = round((TW - 150) * val / max_val) if max_val else 4
            fill_w = max(4, fill_w)
            draw_text(c, LM, bar_y + bar_h - 3,
                      cat, font=FONT_REGULAR, size=8, color=COLOR_CHARCOAL)
            track_x = LM + 150
            draw_rect(c, track_x, bar_y, TW - 150, bar_h,
                      fill=COLOR_LIGHT_GRAY, stroke=COLOR_BORDER, sw=0.3)
            draw_rect(c, track_x, bar_y, fill_w, bar_h, fill=bcol)
            draw_text(c, track_x + fill_w + 6, bar_y + bar_h - 3,
                      f"{val:,}", font=FONT_BOLD, size=8, color=COLOR_CHARCOAL)
            bar_y += bar_h + bar_gap
        y = bar_y + 8

    # ---- Salary Delta Statistics ----
    if y + 120 < CONTENT_BOTTOM:
        y = draw_section_header(c, y, "Salary Change Statistics")
        if sal_rows > 0:
            if n_az > 0:
                y = draw_para(c, LM, y,
                              f"Note: {n_az:,} Active/$0 salary records excluded from these "
                              "statistics - they represent data quality issues, not real "
                              "salary changes.", PS_BODY_MUT, TW)
                y += 6
            sal_cw = [200, 146, 180]   # = 526pt
            sal_rows_data = [
                [Paragraph("<b>Statistic</b>",           PS_HDR),
                 Paragraph("<b>Value</b>",               PS_HDR),
                 Paragraph("<b>Notes</b>",               PS_HDR)],
                [Paragraph("Records with mismatch",      PS_CELL),
                 Paragraph(f"{sal_rows:,}",              PS_CELL),
                 Paragraph("Review all in Salary_Mismatches", PS_CELL_MUT)],
                [Paragraph("Mean salary change",         PS_CELL),
                 Paragraph(f"${sal_mean:+,.0f}",
                           PS_PASS if sal_mean > 0 else PS_FAIL if sal_mean < 0 else PS_CELL),
                 Paragraph("Average delta across all mismatches", PS_CELL_MUT)],
                [Paragraph("Median salary change",       PS_CELL),
                 Paragraph(f"${sal_med:+,.0f}",          PS_CELL),
                 Paragraph("More representative than mean",       PS_CELL_MUT)],
                [Paragraph("Largest increase",           PS_CELL),
                 Paragraph(f"${sal_max:+,.0f}",
                           PS_PASS if sal_max > 0 else PS_CELL),
                 Paragraph("Spot-check outliers",         PS_CELL_MUT)],
                [Paragraph("Largest decrease",           PS_CELL),
                 Paragraph(f"${sal_min:+,.0f}",
                           PS_FAIL if sal_min < 0 else PS_CELL),
                 Paragraph("Investigate large decreases", PS_CELL_MUT)],
            ]
            y = draw_table(c, LM, y, sal_cw, sal_rows_data, hdr_bg=COLOR_NAVY, font_size=9)
        else:
            y = draw_para(c, LM, y, "No salary mismatches detected.", PS_PASS, TW)
        y += 12

    # ---- Payrate Conversion Breakdown ----
    conv_counts = summary.get("conversion_counts") or {}
    if conv_counts and y + 100 < CONTENT_BOTTOM:
        y = draw_section_header(c, y, "Payrate Conversion Patterns")
        _CONV_LABELS = {
            "annual_to_hourly":   "Annual to hourly (old salary / 2080 = new payrate)",
            "hourly_to_annual":   "Hourly to annual (old payrate x 2080 = new salary)",
            "biweekly_to_annual": "Biweekly to annual (old payrate x 26 = new salary)",
            "annual_to_biweekly": "Annual to biweekly (old salary / 26 = new payrate)",
        }
        conv_cw = [310, 216]   # = 526pt
        conv_rows = [
            [Paragraph("<b>Conversion Pattern</b>", PS_HDR),
             Paragraph("<b>Records</b>",            PS_HDR)]
        ]
        for ct, cnt in sorted(conv_counts.items(), key=lambda x: -x[1]):
            lbl = _CONV_LABELS.get(ct, ct.replace("_", " ").title())
            conv_rows.append([Paragraph(lbl, PS_CELL), Paragraph(f"{cnt:,}", PS_CELL)])
        y = draw_table(c, LM, y, conv_cw, conv_rows, hdr_bg=COLOR_NAVY, font_size=9)
        y += 12

    # ---- Hire Date Wave Detection ----
    wave_dates = summary.get("wave_dates") or {}
    if wave_dates and y + 120 < CONTENT_BOTTOM:
        y = draw_section_header(c, y, "Hire Date Wave Detection")
        y = draw_para(c, LM, y,
                      "Hire dates shared by 1% or more of all records may be bulk-import "
                      "placeholder dates rather than actual start dates.",
                      PS_BODY_MUT, TW)
        y += 4
        total_rec = max(total, 1)
        wd_cw = [180, 100, 110, 136]   # = 526pt
        wd_rows = [
            [Paragraph("<b>Hire Date</b>",   PS_HDR),
             Paragraph("<b>Count</b>",       PS_HDR),
             Paragraph("<b>% of Total</b>",  PS_HDR),
             Paragraph("<b>Risk Level</b>",  PS_HDR)]
        ]
        for hd, cnt in sorted(wave_dates.items(), key=lambda x: -x[1]):
            pct        = round(cnt / total_rec * 100, 1)
            risk_style = PS_CRITICAL if pct >= 5 else PS_HIGH if pct >= 2 else PS_MEDIUM
            risk_lbl   = ("High - bulk import likely" if pct >= 5
                          else "Medium" if pct >= 2 else "Low")
            wd_rows.append([
                Paragraph(str(hd),       PS_CELL),
                Paragraph(f"{cnt:,}",    PS_CELL),
                Paragraph(f"{pct:.1f}%", PS_CELL),
                Paragraph(risk_lbl,      risk_style),
            ])
        y = draw_table(c, LM, y, wd_cw, wd_rows, hdr_bg=COLOR_NAVY, font_size=9)
        y += 12

    # ---- Mismatch Categories ----
    if y + 120 < CONTENT_BOTTOM:
        y = draw_section_header(c, y, "Mismatch Categories at a Glance")
        cat_cw = [200, 116, 210]   # = 526pt
        cat_rows = [
            [Paragraph("<b>Category</b>",     PS_HDR),
             Paragraph("<b>Records</b>",      PS_HDR),
             Paragraph("<b>Workbook Sheet</b>", PS_HDR)],
            [Paragraph("Salary / Pay Rate",   PS_CELL),
             Paragraph(f"{n_salary:,}", PS_HIGH   if n_salary > 0 else PS_PASS),
             Paragraph("Salary_Mismatches",   PS_CELL_MUT)],
            [Paragraph("Employment Status",   PS_CELL),
             Paragraph(f"{n_status:,}", PS_CRITICAL if n_status > 0 else PS_PASS),
             Paragraph("Status_Mismatches",   PS_CELL_MUT)],
            [Paragraph("Hire Date",           PS_CELL),
             Paragraph(f"{n_hdate:,}",  PS_HIGH   if n_hdate > 0 else PS_PASS),
             Paragraph("HireDate_Mismatches", PS_CELL_MUT)],
            [Paragraph("Job / Organization",  PS_CELL),
             Paragraph(f"{n_joborg:,}", PS_MEDIUM  if n_joborg > 0 else PS_PASS),
             Paragraph("JobOrg_Mismatches",   PS_CELL_MUT)],
            [Paragraph("Active / $0 Salary",  PS_CELL),
             Paragraph(f"{n_az:,}",     PS_CRITICAL if n_az > 0 else PS_PASS),
             Paragraph("CRITICAL_Zero_Salary", PS_CELL_MUT)],
        ]
        y = draw_table(c, LM, y, cat_cw, cat_rows, hdr_bg=COLOR_NAVY, font_size=9)
        y += 12

    # ---- Key Observations + Recommended Next Steps ----
    if y + 60 < CONTENT_BOTTOM:
        y = draw_section_header(c, y, "Key Observations")
        safe_pct = float(summary.get("safe_pct", 0))
        obs = []
        if n_az > 0:
            obs.append(
                f"<b>CRITICAL:</b> {n_az:,} active workers have $0 or missing salary. "
                "Must be resolved before any import - these employees will not receive pay."
            )
        if safe_pct >= 90:
            obs.append(
                f"{safe_pct:.1f}% of matched records are Safe - good shape for import "
                "once the review queue is cleared."
            )
        elif safe_pct < 70:
            obs.append(
                f"Only {safe_pct:.1f}% of records are Safe - significant review work is "
                "needed before this dataset is ready for import."
            )
        if n_status > 0:
            obs.append(
                f"{n_status:,} status mismatches require verification. Status changes "
                "carry the highest business risk in any reconciliation."
            )
        if n_fuzzy > 0 and total > 0:
            fpct = round(n_fuzzy / total * 100, 1)
            obs.append(
                f"{fpct:.1f}% of matches used fuzzy or non-ID matching. "
                "Spot-check a sample of these to verify identity before importing."
            )
        if not obs:
            obs.append("No significant pattern issues detected in this reconciliation run.")
        for ob in obs[:5]:
            y = draw_para(c, LM, y, f"- {ob}", PS_BODY, TW)
            y += 4


# ===========================================================================
# TWO-PASS RENDERER
# ===========================================================================

def _render(output, run_id: str, summary: dict, org_name: str,
            total_pages: int, date_str: str) -> int:
    """
    Render all pages. Returns actual total page count.
    output may be a filepath str or a BytesIO buffer.
    """
    from reportlab.pdfgen import canvas as _rl_canvas
    c = _rl_canvas.Canvas(output, pagesize=(PAGE_W, PAGE_H))

    page_num = 1

    # PAGE 1: Cover (no header bar, no running footer)
    draw_cover_page(c, run_id, summary, org_name, date_str)
    c.showPage()
    page_num += 1

    # PAGE 2: Executive Summary
    y = draw_page_header(c, page_num, total_pages, "EXECUTIVE SUMMARY", org_name)
    draw_footer(c, page_num, total_pages, org_name, run_id)
    _draw_exec_summary(c, y, page_num, total_pages, summary, org_name, run_id, date_str)
    c.showPage()
    page_num += 1

    # PAGES 3+: Findings by Severity
    y = draw_page_header(c, page_num, total_pages, "FINDINGS BY SEVERITY", org_name)
    draw_footer(c, page_num, total_pages, org_name, run_id)
    page_num, y = _draw_findings(c, y, page_num, total_pages, summary, org_name, run_id)
    c.showPage()
    page_num += 1

    # Rejected Matches (only if any exist)
    n_wrong = int(summary.get("n_wrong_match", 0))
    if n_wrong > 0:
        y = draw_page_header(c, page_num, total_pages, "REJECTED MATCHES", org_name)
        draw_footer(c, page_num, total_pages, org_name, run_id)
        page_num, y = _draw_rejected_matches(
            c, y, page_num, total_pages, summary, org_name, run_id
        )
        c.showPage()
        page_num += 1

    # Action Summary
    y = draw_page_header(c, page_num, total_pages, "ACTION SUMMARY", org_name)
    draw_footer(c, page_num, total_pages, org_name, run_id)
    _draw_action_summary(c, y, page_num, total_pages, summary, org_name, run_id)
    c.showPage()
    page_num += 1

    # Match Quality (last page - no showPage before save)
    y = draw_page_header(c, page_num, total_pages, "MATCH QUALITY AND PATTERNS", org_name)
    draw_footer(c, page_num, total_pages, org_name, run_id)
    _draw_match_quality(c, y, page_num, total_pages, summary, org_name, run_id)

    c.save()
    return page_num


# ===========================================================================
# PUBLIC ENTRY POINT
# ===========================================================================

def build_pdf(run_id: str,
              wide_path: Path,
              out_path: Path,
              held_path:     Path | None = None,
              uo_path:       Path | None = None,
              un_path:       Path | None = None,
              manifest_path: Path | None = None,
              review_path:   Path | None = None) -> int:
    """
    Load data, run two-pass render (page-count pass + final pass),
    and write PDF to out_path.  Returns actual page count.
    """
    summary = load_summary(wide_path, held_path, uo_path, un_path,
                           manifest_path, review_path)

    org_name = "Your Organization"
    try:
        # Try to load org name from policy.yaml
        _here = Path(__file__).resolve().parent.parent
        _root = _here.parents[1]
        sys.path.insert(0, str(_root / "audit" / "summary"))
        from config_loader import load_policy  # type: ignore
        _policy  = load_policy(_root / "config" / "policy.yaml")
        org_name = str(
            (_policy.get("client") or {}).get("name")
            or _policy.get("client_name")
            or "Your Organization"
        )
    except Exception:
        pass

    date_str = datetime.now().strftime("%B %d, %Y")

    # Pass 1: render to memory to count pages accurately
    buf1         = BytesIO()
    actual_pages = _render(buf1, run_id, summary, org_name, 99, date_str)

    # Pass 2: render to real file with correct total_pages value
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _render(str(out_path), run_id, summary, org_name, actual_pages, date_str)

    print(f"[pdf_generator] wrote: {out_path}  ({actual_pages} pages)")
    print(f"  Gate status: {get_gate_status(summary['safe_pct'], summary['n_active_zero_salary'], summary['n_wrong_match'])[0]}")
    print(f"  Migration readiness score: {migration_readiness_score(summary['safe_pct'], summary['n_active_zero_salary'], summary['n_wrong_match'], summary['total_matched'])}/100")
    return actual_pages
