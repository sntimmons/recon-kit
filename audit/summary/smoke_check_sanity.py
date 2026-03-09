"""
smoke_check_sanity.py — Verifies sanity_checks.py outputs.

Assertions
----------
1. audit/audit.db exists.
2. sanity_checks.main() runs without error.
3. All three CSV outputs exist and are non-empty (size > 0 bytes).

Exits 0 on pass, 2 on fail.

Run:
    venv/Scripts/python.exe audit/summary/smoke_check_sanity.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_HERE   = Path(__file__).resolve().parent    # audit/summary/
ROOT    = _HERE.parents[1]
DB_PATH = ROOT / "audit" / "audit.db"

EXPECTED_OUTPUTS = [
    _HERE / "sanity_salary_buckets.csv",
    _HERE / "sanity_hire_date_diff.csv",
    _HERE / "sanity_suspicious_defaults.csv",
]


def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(2)


def _pass(msg: str) -> None:
    print(f"  [PASS] {msg}")


def main() -> None:
    print("=" * 60)
    print("  SMOKE CHECK: Sanity Checks")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Assertion 1: DB exists
    # ------------------------------------------------------------------
    if not DB_PATH.exists():
        _fail(f"Assertion 1 FAILED: DB not found: {DB_PATH}")
    _pass(f"Assertion 1: audit.db exists  ({DB_PATH.stat().st_size // 1024:,} KB)")

    # ------------------------------------------------------------------
    # Assertion 2: sanity_checks.main() runs without error
    # ------------------------------------------------------------------
    print("\n  Running sanity_checks.main() ...")
    sys.path.insert(0, str(_HERE))
    import sanity_checks
    try:
        sanity_checks.main(argv=[])
    except SystemExit as exc:
        if exc.code not in (None, 0):
            _fail(f"Assertion 2 FAILED: sanity_checks.main() exited with code {exc.code}")
    except Exception as exc:
        _fail(f"Assertion 2 FAILED: sanity_checks.main() raised {type(exc).__name__}: {exc}")
    print()
    _pass("Assertion 2: sanity_checks.main() ran without error")

    # ------------------------------------------------------------------
    # Assertion 3: All three CSV outputs exist and are non-empty
    # ------------------------------------------------------------------
    for path in EXPECTED_OUTPUTS:
        if not path.exists():
            _fail(f"Assertion 3 FAILED: output not found: {path.name}")
        size = path.stat().st_size
        if size == 0:
            _fail(f"Assertion 3 FAILED: output is empty (0 bytes): {path.name}")
        _pass(f"Assertion 3: {path.name:<40}  ({size:,} bytes)")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()
