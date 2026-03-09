"""
run_sanity_gate.py — Run sanity checks and evaluate the sanity gate.

Writes (to --out directory, default: audit/summary/):
  sanity_results.json   — structured sanity check results from run_sanity_checks()
  sanity_gate.json      — gate evaluation: passed, reasons, blocked_outputs, metrics

Also writes the three sanity CSVs (salary_buckets, hire_date_diff, suspicious_defaults)
to the same output directory, because run_sanity_checks() is called internally.

Exit codes:
  0  — gate PASSED (or gate disabled in policy)
  2  — DB missing or required columns absent (propagated from sanity_checks)
  3  — gate FAILED (configurable via fail_exit_code in policy.yaml)

Run:
    venv/Scripts/python.exe audit/summary/run_sanity_gate.py [--db PATH] [--out PATH]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent   # audit/summary/
ROOT  = _HERE.parents[1]                  # repo root

sys.path.insert(0, str(_HERE))

from sanity_checks import run_sanity_checks    # noqa: E402
from sanity_gate   import evaluate_sanity_gate  # noqa: E402
from config_loader import load_policy           # noqa: E402

DB_PATH = ROOT / "audit" / "audit.db"
OUT_DIR = _HERE


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
    args = parser.parse_args(argv)

    db_path = Path(args.db)  if args.db  else DB_PATH
    out_dir = Path(args.out) if args.out else OUT_DIR

    # run_sanity_checks exits 2 on missing DB / missing columns
    results = run_sanity_checks(db_path=db_path, out_dir=out_dir)

    out_dir.mkdir(parents=True, exist_ok=True)

    # Write sanity_results.json
    results_path = out_dir / "sanity_results.json"
    with open(str(results_path), "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"  wrote: sanity_results.json")

    # Evaluate gate
    policy = load_policy()
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

    fail_code = policy.get("sanity_gate", {}).get("fail_exit_code", 3)
    sys.exit(0 if gate["passed"] else fail_code)


if __name__ == "__main__":
    main()
