"""
smoke_check_sanity_gate.py — Verify sanity_gate.py logic and JSON output files.

Assertions
----------
1. Gate FAILS when thresholds are set to zero and suspicious data is present
   (uses in-memory fake results + tight policy — no DB required).
2. Gate PASSES when thresholds are set very high
   (uses in-memory fake results + loose policy — no DB required).
3. sanity_results.json and sanity_gate.json exist in audit/summary/ and each
   contains the expected boolean field (skipped if files do not exist yet —
   run `run_sanity_gate.py` first to generate them).

Does NOT modify config/policy.yaml.  Exits 0 on pass, 2 on fail.

Run:
    venv/Scripts/python.exe audit/summary/smoke_check_sanity_gate.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent   # audit/summary/
ROOT  = _HERE.parents[1]                  # repo root

sys.path.insert(0, str(_HERE))
from sanity_gate import evaluate_sanity_gate  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Fake results representing a dataset with many suspicious rows.
_FAKE_RESULTS = {
    "total_pairs": 1_000,
    "mismatch_counts": {"salary": 800, "hire_date": 700, "status": 100},
    "suspicious": {
        "hire_date_default_2026_02": {"count": 600, "rate": 0.60},
        "salary_suspicious_default": {"count": 200, "rate": 0.20},
    },
    "files_written": {},
}

# Policy with zero thresholds — any suspicious row triggers FAIL.
_TIGHT_POLICY = {
    "sanity_gate": {
        "enabled": True,
        "fail_exit_code": 3,
        "block_if_rate_greater_than": {
            "hire_date_default_2026_02": 0.0,
            "salary_suspicious_default": 0.0,
        },
        "block_if_count_greater_than": {
            "hire_date_default_2026_02": 0,
            "salary_suspicious_default": 0,
        },
        "block_corrections": True,
        "block_workbook":    False,
        "block_exports":     False,
    }
}

# Policy with very high thresholds — nothing triggers FAIL.
_LOOSE_POLICY = {
    "sanity_gate": {
        "enabled": True,
        "fail_exit_code": 3,
        "block_if_rate_greater_than": {
            "hire_date_default_2026_02": 1.0,
            "salary_suspicious_default": 1.0,
        },
        "block_if_count_greater_than": {
            "hire_date_default_2026_02": 999_999,
            "salary_suspicious_default": 999_999,
        },
        "block_corrections": True,
        "block_workbook":    False,
        "block_exports":     False,
    }
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(2)


def _pass(msg: str) -> None:
    print(f"  [PASS] {msg}")


def _skip(msg: str) -> None:
    print(f"  [SKIP] {msg}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("  SMOKE CHECK: Sanity Gate")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Assertion 1: Gate FAILS with tight (zero) thresholds + suspicious data
    # ------------------------------------------------------------------
    gate1 = evaluate_sanity_gate(_FAKE_RESULTS, _TIGHT_POLICY)

    if gate1["passed"]:
        _fail(
            "Assertion 1 FAILED: gate returned passed=True with zero thresholds "
            f"and suspicious data present. reasons={gate1['reasons']}"
        )
    if not gate1["reasons"]:
        _fail("Assertion 1 FAILED: gate returned passed=False but reasons list is empty.")
    if not gate1["blocked_outputs"].get("corrections"):
        _fail(
            "Assertion 1 FAILED: block_corrections=True in policy but "
            f"blocked_outputs.corrections={gate1['blocked_outputs'].get('corrections')}"
        )
    _pass(
        f"Assertion 1: gate correctly FAILS with tight thresholds "
        f"({len(gate1['reasons'])} violations, corrections blocked)"
    )

    # ------------------------------------------------------------------
    # Assertion 2: Gate PASSES with loose (very high) thresholds
    # ------------------------------------------------------------------
    gate2 = evaluate_sanity_gate(_FAKE_RESULTS, _LOOSE_POLICY)

    if not gate2["passed"]:
        _fail(
            f"Assertion 2 FAILED: gate returned passed=False with loose thresholds. "
            f"reasons={gate2['reasons']}"
        )
    if any(gate2["blocked_outputs"].values()):
        _fail(
            "Assertion 2 FAILED: gate passed but blocked_outputs is non-empty: "
            f"{gate2['blocked_outputs']}"
        )
    _pass(
        f"Assertion 2: gate correctly PASSES with loose thresholds "
        f"(no violations, nothing blocked)"
    )

    # ------------------------------------------------------------------
    # Assertion 3: JSON output files exist and contain 'passed' boolean
    # ------------------------------------------------------------------
    results_json = _HERE / "sanity_results.json"
    gate_json    = _HERE / "sanity_gate.json"

    if not results_json.exists() or not gate_json.exists():
        missing = []
        if not results_json.exists():
            missing.append("sanity_results.json")
        if not gate_json.exists():
            missing.append("sanity_gate.json")
        _skip(
            f"Assertion 3: {', '.join(missing)} not found — "
            "run `run_sanity_gate.py` first to generate them."
        )
    else:
        # Check sanity_results.json has total_pairs key
        with open(str(results_json), "r", encoding="utf-8") as f:
            r_data = json.load(f)
        if "total_pairs" not in r_data:
            _fail("Assertion 3 FAILED: sanity_results.json missing 'total_pairs' key.")

        # Check sanity_gate.json has 'passed' boolean
        with open(str(gate_json), "r", encoding="utf-8") as f:
            g_data = json.load(f)
        if "passed" not in g_data:
            _fail("Assertion 3 FAILED: sanity_gate.json missing 'passed' key.")
        if not isinstance(g_data["passed"], bool):
            _fail(
                f"Assertion 3 FAILED: sanity_gate.json 'passed' is not a bool: "
                f"{type(g_data['passed']).__name__}"
            )

        gate_status = "PASS" if g_data["passed"] else "FAIL"
        _pass(
            f"Assertion 3: both JSON files exist; "
            f"sanity_gate.json passed={g_data['passed']} ({gate_status}); "
            f"total_pairs={r_data.get('total_pairs', '?'):,}"
        )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()
