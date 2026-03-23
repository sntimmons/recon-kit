"""
build_recon_report.py - Entry point for the Data Whisperer Reconciliation Audit PDF.

This module is a thin wrapper that delegates all logic to the pdf_generator package.
The pdf_generator package implements the full visual redesign per the design mandate:

  pdf_generator/
    __init__.py     - public interface
    constants.py    - brand palette, fonts, layout (single source of truth)
    styles.py       - ReportLab ParagraphStyle definitions
    components.py   - reusable drawing functions (cover, badges, tables, charts)
    report.py       - page renderers + two-pass orchestrator

PDF structure:
  Page 1:    Cover page         - navy block, gate badge, stat cards, readiness score
  Page 2:    Executive Summary  - narrative + audit scope table + file manifest
  Pages 3+:  Findings by Severity - CRITICAL / HIGH / MEDIUM / LOW finding blocks
  Page N:    Rejected Matches   - BLOCKED section with purple treatment (if any)
  Page N+1:  Action Summary     - priority bands, disposition, top-10 queue
  Page N+2:  Match Quality      - match source, confidence dist, salary stats, wave dates

Absolute rules (enforced throughout):
  - No em dashes anywhere - plain hyphens only
  - pair_id is always a 12-char hex string, never a sequential integer
  - SSN/DOB columns stripped before any output (old_last4_ssn, new_last4_ssn, etc.)
  - No Unicode subscript/superscript - XML tags only in ReportLab
  - PDF generation uses ReportLab only - do not switch libraries
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]

# Ensure pdf_generator package is importable
sys.path.insert(0, str(HERE))

# Set up RK_WORK_DIR-aware default paths (mirrors build_workbook.py conventions)
_rk_work = Path(os.environ["RK_WORK_DIR"]) if "RK_WORK_DIR" in os.environ else None

WIDE_CSV      = ROOT / "audit" / "exports" / "out" / "wide_compare.csv"
MANIFEST_CSV  = ((_rk_work / "audit" / "corrections" / "out" / "corrections_manifest.csv")
                 if _rk_work else
                 (ROOT / "audit" / "corrections" / "out" / "corrections_manifest.csv"))
HELD_CSV      = ((_rk_work / "audit" / "corrections" / "out" / "held_corrections.csv")
                 if _rk_work else
                 (ROOT / "audit" / "corrections" / "out" / "held_corrections.csv"))
REVIEW_CSV    = ((_rk_work / "review_queue.csv")
                 if _rk_work else
                 (HERE.parent / "summary" / "review_queue.csv"))
_RUN_OUTS     = (_rk_work / "outputs") if _rk_work else (ROOT / "outputs")
UO_CSV        = _RUN_OUTS / "unmatched_old.csv"
UN_CSV        = _RUN_OUTS / "unmatched_new.csv"
OUT_PATH      = HERE / "recon_report.pdf"

# Import the package's public interface
from pdf_generator import build_pdf   # noqa: E402


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Build the Data Whisperer Reconciliation Audit PDF report."
    )
    parser.add_argument("--run-id",   default="local",
                        help="Run ID string (default: local)")
    parser.add_argument("--wide",     default=None,
                        help=f"wide_compare.csv path (default: {WIDE_CSV})")
    parser.add_argument("--held",     default=None,
                        help=f"held_corrections.csv path (default: {HELD_CSV})")
    parser.add_argument("--uo",       default=None,
                        help=f"unmatched_old.csv path (default: {UO_CSV})")
    parser.add_argument("--un",       default=None,
                        help=f"unmatched_new.csv path (default: {UN_CSV})")
    parser.add_argument("--manifest", default=None,
                        help=f"corrections_manifest.csv path (default: {MANIFEST_CSV})")
    parser.add_argument("--review",   default=None,
                        help=f"review_queue.csv path (default: {REVIEW_CSV})")
    parser.add_argument("--out",      default=None,
                        help=f"Output PDF path (default: {OUT_PATH})")
    args = parser.parse_args(argv)

    build_pdf(
        run_id        = args.run_id,
        wide_path     = Path(args.wide)     if args.wide     else WIDE_CSV,
        out_path      = Path(args.out)      if args.out      else OUT_PATH,
        held_path     = Path(args.held)     if args.held     else HELD_CSV,
        uo_path       = Path(args.uo)       if args.uo       else UO_CSV,
        un_path       = Path(args.un)       if args.un       else UN_CSV,
        manifest_path = Path(args.manifest) if args.manifest else MANIFEST_CSV,
        review_path   = Path(args.review)   if args.review   else REVIEW_CSV,
    )


if __name__ == "__main__":
    main()
