"""
run_sanity_gate.py - Run sanity checks and evaluate the sanity gate.

Writes (to --out directory, default: audit/summary/):
  sanity_results.json   - structured sanity check results from run_sanity_checks()
  sanity_gate.json      - gate evaluation: passed, reasons, blocked_outputs, metrics

Also writes the three sanity CSVs (salary_buckets, hire_date_diff, suspicious_defaults)
to the same output directory, because run_sanity_checks() is called internally.

Exit codes:
  0  - gate PASSED (or gate disabled in policy)
  2  - DB missing or required columns absent (propagated from sanity_checks)
  3  - gate FAILED (configurable via fail_exit_code in policy.yaml)

Run:
    venv/Scripts/python.exe audit/summary/run_sanity_gate.py [--db PATH] [--out PATH] [--min-approve-rate 0.75]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd

import os as _os_rk
_HERE = Path(__file__).resolve().parent   # audit/summary/
ROOT  = _HERE.parents[1]                  # repo root

sys.path.insert(0, str(_HERE))

from sanity_checks import run_sanity_checks, detect_wave_dates   # noqa: E402
from sanity_gate   import evaluate_sanity_gate                    # noqa: E402
from config_loader import load_policy                             # noqa: E402
from gating        import classify_all                            # noqa: E402

# Per-run isolation: --db CLI arg takes precedence; env var is the fallback.
_rk_work = Path(_os_rk.environ["RK_WORK_DIR"]) if "RK_WORK_DIR" in _os_rk.environ else None
DB_PATH  = (_rk_work / "audit" / "audit.db") if _rk_work else (ROOT / "audit" / "audit.db")
OUT_DIR  = _HERE


def _parse_salary(val) -> float | None:
    """Parse a salary value that may be a string with commas or $ signs."""
    if val is None:
        return None
    s = str(val).strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _compute_approve_rate(db_path: Path) -> tuple[int, int, float, int]:
    """
    Run classify_all on every matched pair.

    Returns (approve_count, total, approve_rate, active_zero_approved).

    active_zero_approved: count of Active workers with new_salary == 0
    that received an APPROVE action - meaning corrections WOULD be staged
    for them.  This is the critical-issues check for the new 3-part gate.
    Under current gating rules this is always 0 (salary_ratio == 0.0
    triggers Override 1 → REVIEW), but we compute it explicitly.
    """
    con = sqlite3.connect(str(db_path))
    try:
        mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)
    finally:
        con.close()

    if mp.empty:
        return 0, 0, 0.0, 0

    if "confidence" not in mp.columns:
        mp["confidence"] = None

    all_rows            = mp.to_dict(orient="records")
    wave_dates          = detect_wave_dates(all_rows)
    total               = len(all_rows)
    n_approve           = 0
    active_zero_approved = 0

    for r in all_rows:
        result = classify_all(r, wave_dates=wave_dates)
        if result["action"] == "APPROVE":
            n_approve += 1
            # Check: is this an active employee with $0 salary?
            status = str(r.get("new_worker_status", "") or "").strip().lower()
            sal    = _parse_salary(r.get("new_salary"))
            if status == "active" and (sal is None or sal == 0.0):
                active_zero_approved += 1

    rate = round(n_approve / total, 6) if total > 0 else 0.0
    return n_approve, total, rate, active_zero_approved


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run sanity checks and evaluate the sanity gate.",
    )
    parser.add_argument(
        "--db", default=None, metavar="PATH",
        help=f"SQLite database path (default: {DB_PATH}).",
    )
    parser.add_argument(
        "--out", default=None, metavar="PATH",
        help=f"Output directory for JSON and CSV files (default: {OUT_DIR}).",
    )
    parser.add_argument(
        "--min-approve-rate", default=None, type=float, metavar="RATE",
        help="Override sanity_gate.health_thresholds.min_approve_rate from policy.yaml (e.g. 0.75).",
    )
    args = parser.parse_args(argv)

    db_path = Path(args.db)  if args.db  else DB_PATH
    out_dir = Path(args.out) if args.out else OUT_DIR

    # run_sanity_checks exits 2 on missing DB / missing columns
    results = run_sanity_checks(db_path=db_path, out_dir=out_dir)

    # Fix 6 / 3-part gate: Compute approve_rate + active_zero_approved
    # (requires a full gating pass over all pairs).
    print("[run_sanity_gate] computing approve_rate + active_zero_approved (gating pass) ...")
    n_approve, total_pairs, approve_rate, active_zero_approved = _compute_approve_rate(db_path)
    if "health_metrics" not in results:
        results["health_metrics"] = {}
    results["health_metrics"]["approve_count"]         = n_approve
    results["health_metrics"]["approve_rate"]          = approve_rate
    results["health_metrics"]["active_zero_approved"]  = active_zero_approved
    print(f"  approve_rate:          {approve_rate:.4f}  ({n_approve:,}/{total_pairs:,} APPROVE)")
    print(f"  active_zero_approved:  {active_zero_approved:,}  (active/$0 with APPROVE action)")

    out_dir.mkdir(parents=True, exist_ok=True)

    # Write sanity_results.json
    results_path = out_dir / "sanity_results.json"
    with open(str(results_path), "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"  wrote: sanity_results.json")

    # Evaluate gate
    policy = load_policy()
    # Per-run override: --min-approve-rate takes precedence over policy.yaml
    if args.min_approve_rate is not None:
        rate_override = max(0.0, min(1.0, args.min_approve_rate))
        policy.setdefault("sanity_gate", {}).setdefault("health_thresholds", {})
        policy["sanity_gate"]["health_thresholds"]["min_approve_rate"] = rate_override
        print(f"[run_sanity_gate] min_approve_rate overridden to {rate_override:.4f} (from --min-approve-rate CLI arg)")
    gate   = evaluate_sanity_gate(results, policy)

    # Write sanity_gate.json
    gate_path = out_dir / "sanity_gate.json"
    with open(str(gate_path), "w", encoding="utf-8") as f:
        json.dump(gate, f, indent=2)
    print(f"  wrote: sanity_gate.json")

    # Print summary
    W      = 60
    status = "PASS" if gate["passed"] else "FAIL"
    print()
    print("=" * W)
    print(f"  SANITY GATE: {status}")
    print("=" * W)

    if gate["passed"]:
        print("  All thresholds within acceptable limits.")
    else:
        print("  Threshold violations:")
        for reason in gate["reasons"]:
            print(f"    - {reason}")
        blocked = [k for k, v in gate["blocked_outputs"].items() if v]
        if blocked:
            print(f"  Blocked outputs: {', '.join(blocked)}")

    if gate.get("health_checks"):
        print()
        print("  Health checks:")
        for check, info in gate["health_checks"].items():
            status_icon = "OK" if info["passed"] else "FAIL"
            print(f"    [{status_icon}] {check:<28}: {info['value']}  (threshold: {info['threshold']})")

    fail_code = policy.get("sanity_gate", {}).get("fail_exit_code", 3)
    sys.exit(0 if gate["passed"] else fail_code)


if __name__ == "__main__":
    main()
