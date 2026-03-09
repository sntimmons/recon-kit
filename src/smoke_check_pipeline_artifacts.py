"""
smoke_check_pipeline_artifacts.py — Verify run_manager + step_receipts integration.

Assertions
----------
1. make_run_id() returns YYYY_MM_DD_HHMMSS format.
2. ensure_run_dirs() creates expected dirs including ui/ and meta/receipts/.
3. write_receipt() writes valid JSON with required fields.
4. Multiple receipts round-trip correctly (ok=True and ok=False).
5. write_run_manifest() includes receipts in key_output_files when present.

Cleans up the test run folder on completion.
Exits 0 on pass, 2 on fail.

Run:
    venv/Scripts/python.exe src/smoke_check_pipeline_artifacts.py
"""
from __future__ import annotations

import json
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent   # src/
ROOT  = _HERE.parent                      # repo root

sys.path.insert(0, str(_HERE))

from run_manager import (   # noqa: E402
    make_run_id,
    ensure_run_dirs,
    write_run_manifest,
)
from step_receipts import write_receipt, safe_stat, file_info  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(2)


def _pass(msg: str) -> None:
    print(f"  [PASS] {msg}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("  SMOKE CHECK: Pipeline Artifacts")
    print("=" * 60)

    run_id = make_run_id() + "_ARTTEST"

    # ------------------------------------------------------------------
    # Assertion 1: make_run_id format
    # ------------------------------------------------------------------
    base = run_id.replace("_ARTTEST", "")
    if not re.match(r"^\d{4}_\d{2}_\d{2}_\d{6}$", base):
        _fail(f"Assertion 1: make_run_id() format wrong: {base!r}")
    _pass(f"Assertion 1: make_run_id() format OK → {run_id}")

    # ------------------------------------------------------------------
    # Assertion 2: ensure_run_dirs creates expected dirs + ui + receipts
    # ------------------------------------------------------------------
    paths = ensure_run_dirs(run_id)

    required_keys = {"run", "inputs", "outputs", "audit", "summary",
                     "exports", "corrections", "ui", "logs", "meta"}
    missing_keys = required_keys - set(paths)
    if missing_keys:
        _fail(f"Assertion 2: ensure_run_dirs missing dict keys: {missing_keys}")

    for key, path in paths.items():
        if not path.is_dir():
            _fail(f"Assertion 2: directory not created: {key}={path}")

    receipts_dir = paths["meta"] / "receipts"
    if not receipts_dir.is_dir():
        _fail(f"Assertion 2: receipts dir not created: {receipts_dir}")

    _pass(f"Assertion 2: ensure_run_dirs created {len(paths)} dirs + meta/receipts/")

    # ------------------------------------------------------------------
    # Assertion 3: write_receipt writes valid JSON with required fields
    # ------------------------------------------------------------------
    receipt_path = write_receipt(paths, "smoke_test_step", {
        "inputs":        [{"path": "test/input.csv", "exists": False}],
        "outputs":       [{"path": "test/output.csv", "exists": False}],
        "warnings":      [],
        "elapsed_sec":   0.123,
    })

    if not receipt_path.exists():
        _fail("Assertion 3: write_receipt did not create file.")

    with open(str(receipt_path), "r", encoding="utf-8") as f:
        r = json.load(f)

    for field in ("step", "timestamp_utc", "ok"):
        if field not in r:
            _fail(f"Assertion 3: receipt missing required field '{field}'.")
    if r["step"] != "smoke_test_step":
        _fail(f"Assertion 3: step field wrong: {r['step']!r}")
    if r["ok"] is not True:
        _fail(f"Assertion 3: ok should default to True, got {r['ok']!r}")

    # Check timestamp_utc is ISO-8601 parseable
    try:
        datetime.fromisoformat(r["timestamp_utc"])
    except Exception as exc:
        _fail(f"Assertion 3: timestamp_utc not valid ISO-8601: {exc}")

    _pass(f"Assertion 3: write_receipt created valid JSON at {receipt_path.name}")

    # ------------------------------------------------------------------
    # Assertion 4: receipts with ok=False round-trip correctly
    # ------------------------------------------------------------------
    fail_path = write_receipt(paths, "smoke_failed_step", {
        "inputs":        [],
        "outputs":       [],
        "warnings":      ["exited with code 3"],
        "elapsed_sec":   0.5,
        "ok":            False,
        "skipped":       False,
    })

    with open(str(fail_path), "r", encoding="utf-8") as f:
        r2 = json.load(f)

    if r2.get("ok") is not False:
        _fail(f"Assertion 4: ok=False not preserved, got {r2.get('ok')!r}")
    if r2.get("warnings") != ["exited with code 3"]:
        _fail(f"Assertion 4: warnings not preserved: {r2.get('warnings')!r}")

    _pass("Assertion 4: receipt with ok=False round-trips correctly.")

    # ------------------------------------------------------------------
    # Assertion 5: manifest includes receipts in key_output_files
    # ------------------------------------------------------------------
    manifest_path = write_run_manifest(run_id, paths, extra={"smoke_test": True})

    with open(str(manifest_path), "r", encoding="utf-8") as f:
        manifest = json.load(f)

    key_files = manifest.get("key_output_files", [])
    receipt_files = [p for p in key_files if "receipts/" in p]
    if not receipt_files:
        _fail(
            f"Assertion 5: no receipt files found in key_output_files.\n"
            f"  key_output_files={key_files}"
        )
    _pass(f"Assertion 5: {len(receipt_files)} receipt(s) listed in manifest key_output_files.")

    # Bonus: verify safe_stat and file_info work on real and missing paths
    stat_real    = safe_stat(manifest_path)
    stat_missing = safe_stat(ROOT / "nonexistent_file_xyz.txt")
    if not stat_real["exists"]:
        _fail("safe_stat: manifest_path should exist.")
    if stat_missing["exists"]:
        _fail("safe_stat: nonexistent file should not exist.")

    fi = file_info(manifest_path)
    if "path" not in fi or "size_bytes" not in fi:
        _fail(f"file_info: missing expected keys: {list(fi.keys())}")

    _pass("safe_stat / file_info helpers work correctly.")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    shutil.rmtree(str(paths["run"]), ignore_errors=True)
    print(f"\n  [cleanup] removed test run folder: runs/{run_id}")

    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()
