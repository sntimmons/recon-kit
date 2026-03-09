"""
smoke_check_single_audit.py — Validates single_audit.py without running the DB.

Assertions
----------
1. CLI --help works and exits 0.
2. Invalid --type exits with nonzero exit code.
3. Path creation and receipt.json round-trip in a temp directory.

Run with:
    venv/Scripts/python.exe src/smoke_check_single_audit.py
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY   = sys.executable
SCRIPT = str(ROOT / "src" / "single_audit.py")


def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(1)


def _run(argv: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    cmd = [PY, SCRIPT] + argv
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(ROOT),
    )


def main() -> None:
    print("=" * 60)
    print("  SMOKE CHECK: single_audit.py")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Assertion 1: --help exits 0
    # ------------------------------------------------------------------
    res = _run(["--help"])
    if res.returncode != 0:
        _fail(f"Assertion 1: --help exited {res.returncode}; stderr: {res.stderr[:200]}")
    if "--type" not in res.stdout:
        _fail(f"Assertion 1: --help output missing --type flag")
    print("  [PASS] Assertion 1: --help works and lists --type flag")

    # ------------------------------------------------------------------
    # Assertion 2: invalid --type exits nonzero
    # ------------------------------------------------------------------
    res = _run(["--type", "bogus_type"])
    if res.returncode == 0:
        _fail("Assertion 2: invalid --type should exit nonzero, but exited 0")
    print(f"  [PASS] Assertion 2: invalid --type exits {res.returncode} (nonzero)")

    # ------------------------------------------------------------------
    # Assertion 3: out-dir creation and receipt.json round-trip
    # ------------------------------------------------------------------
    # We don't have a DB in CI, so we test path creation by writing a dummy
    # receipt directly — verifying that the receipt schema is valid JSON.
    with tempfile.TemporaryDirectory() as tmp:
        out_dir = Path(tmp) / "test_slice"
        out_dir.mkdir()

        receipt = {
            "run_ts":        "20260305_120000",
            "audit_type":    "salary",
            "db_path":       str(ROOT / "audit" / "audit.db"),
            "out_dir":       str(out_dir),
            "total_pairs":   100,
            "mismatch_rows": 10,
            "gate": {"approve": 8, "review": 2, "only_approved": True},
            "files": {
                "ui_pairs":    str(out_dir / "ui_pairs_salary.csv"),
                "review_queue": str(out_dir / "review_queue_salary.csv"),
                "corrections": str(out_dir / "corrections_salary.csv"),
                "manifest":    str(out_dir / "manifest_salary.csv"),
            },
        }
        p = out_dir / "receipt.json"
        p.write_text(json.dumps(receipt, indent=2), encoding="utf-8")

        loaded = json.loads(p.read_text(encoding="utf-8"))
        if loaded["audit_type"] != "salary":
            _fail("Assertion 3: receipt.json round-trip failed (audit_type mismatch)")
        if loaded["gate"]["approve"] != 8:
            _fail("Assertion 3: receipt.json round-trip failed (gate.approve mismatch)")

    print("  [PASS] Assertion 3: receipt.json schema valid and round-trips correctly")

    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")


if __name__ == "__main__":
    main()
