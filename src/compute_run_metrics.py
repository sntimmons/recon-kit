"""compute_run_metrics.py — Compute high-level run metrics and write run_metrics.json.

Reads:
    match_report.json  (written by src/matcher.py)
    wide_compare.csv   (written by build_diy_exports - optional)

Writes:
    run_metrics.json

Metrics
-------
    match_rate            matched_pairs / total_input_records (old-side basis)
    fallback_usage_pct    non-id matches / total matches
    ambiguity_rate        ambiguous_total / total_input_records
    review_rate           REVIEW rows / total matches
    reject_rate           REJECT_MATCH rows / total matches
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT     = Path(__file__).resolve().parents[1]
_rk_work = Path(os.environ["RK_WORK_DIR"]) if "RK_WORK_DIR" in os.environ else None


def _default_match_report() -> Path:
    if _rk_work:
        return _rk_work / "outputs" / "match_report.json"
    return ROOT / "outputs" / "match_report.json"


def _default_wide_compare() -> Path:
    if _rk_work:
        return _rk_work / "wide_compare.csv"
    return ROOT / "audit" / "exports" / "out" / "wide_compare.csv"


def _default_out() -> Path:
    if _rk_work:
        return _rk_work / "run_metrics.json"
    return ROOT / "outputs" / "run_metrics.json"


def compute(match_report_path: Path, wide_compare_path: Path) -> dict:
    """Read source files and return computed metrics dict."""
    with open(str(match_report_path), encoding="utf-8") as f:
        report = json.load(f)

    matched_total   = int(report.get("matched_total", 0))
    ambiguous_total = int(report.get("ambiguous_total", 0))
    id_matches = (
        int(report.get("matched_by_worker_id", 0))
        + int(report.get("matched_by_recon_id", 0))
    )

    _uo = report.get("unmatched_old")
    _un = report.get("unmatched_new")
    old_total = matched_total + int(_uo) if _uo is not None else None
    new_total = matched_total + int(_un) if _un is not None else None
    if old_total is not None and new_total is not None:
        total_input = max(old_total, new_total)
    elif old_total is not None:
        total_input = old_total
    elif new_total is not None:
        total_input = new_total
    else:
        total_input = matched_total

    match_rate         = round(matched_total / total_input, 4)          if total_input    > 0 else 0.0
    fallback_usage_pct = round((matched_total - id_matches) / matched_total, 4) if matched_total > 0 else 0.0
    ambiguity_rate     = round(ambiguous_total / total_input, 4)        if total_input    > 0 else 0.0

    n_review = 0
    n_reject = 0
    if wide_compare_path.exists():
        try:
            import pandas as pd
            wc = pd.read_csv(str(wide_compare_path), dtype="string", usecols=["action"])
            n_review = int((wc["action"] == "REVIEW").sum())
            n_reject = int((wc["action"] == "REJECT_MATCH").sum())
        except Exception as exc:
            print(f"[warn] could not read wide_compare.csv for action counts: {exc}", file=sys.stderr)
    else:
        print(
            f"[warn] wide_compare.csv not found at {wide_compare_path}; "
            "review_rate and reject_rate will be 0",
            file=sys.stderr,
        )

    review_rate = round(n_review / matched_total, 4) if matched_total > 0 else 0.0
    reject_rate = round(n_reject / matched_total, 4) if matched_total > 0 else 0.0

    return {
        "match_rate":         match_rate,
        "fallback_usage_pct": fallback_usage_pct,
        "ambiguity_rate":     ambiguity_rate,
        "review_rate":        review_rate,
        "reject_rate":        reject_rate,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Compute run metrics and write run_metrics.json.")
    parser.add_argument("--match-report", default=None, metavar="PATH",
                        help="Path to match_report.json (default: auto-detect from RK_WORK_DIR or outputs/)")
    parser.add_argument("--wide-compare", default=None, metavar="PATH",
                        help="Path to wide_compare.csv (default: auto-detect; optional)")
    parser.add_argument("--out", default=None, metavar="PATH",
                        help="Output path for run_metrics.json (default: auto-detect from RK_WORK_DIR or outputs/)")
    args = parser.parse_args(argv)

    match_report_path = Path(args.match_report) if args.match_report else _default_match_report()
    wide_compare_path = Path(args.wide_compare) if args.wide_compare else _default_wide_compare()
    out_path          = Path(args.out)           if args.out           else _default_out()

    if not match_report_path.exists():
        print(f"[error] match_report.json not found: {match_report_path}", file=sys.stderr)
        sys.exit(2)

    metrics = compute(match_report_path, wide_compare_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    pct = lambda v: f"{v * 100:.0f}%"
    print("\n[run_metrics]")
    print(f"  Match Rate:       {pct(metrics['match_rate'])}")
    print(f"  Fallback Usage:   {pct(metrics['fallback_usage_pct'])}")
    print(f"  Ambiguity:        {pct(metrics['ambiguity_rate'])}")
    print(f"  Review Required:  {pct(metrics['review_rate'])}")
    print(f"  Rejected:         {pct(metrics['reject_rate'])}")
    print(f"  wrote: {out_path}")


if __name__ == "__main__":
    main()
