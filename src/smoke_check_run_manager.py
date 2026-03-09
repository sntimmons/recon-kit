"""
smoke_check_run_manager.py — Verify run_manager.py logic.

Assertions
----------
1. make_run_id() returns YYYY_MM_DD_HHMMSS format.
2. ensure_run_dirs() creates all expected subdirectories.
3. copy_artifacts_to_run() copies a dummy sanity_*.csv into the run folder.
4. write_run_manifest() writes manifest.json with required fields.

Cleans up the test run folder on completion.
Exits 0 on pass, 2 on fail.

Run:
    venv/Scripts/python.exe src/smoke_check_run_manager.py
"""
from __future__ import annotations

import json
import re
import shutil
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent   # src/
ROOT  = _HERE.parent                      # repo root

sys.path.insert(0, str(_HERE))

from run_manager import (  # noqa: E402
    make_run_id,
    ensure_run_dirs,
    copy_artifacts_to_run,
    write_run_manifest,
)


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
    print("  SMOKE CHECK: Run Manager")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Assertion 1: make_run_id() format
    # ------------------------------------------------------------------
    run_id = make_run_id() + "_TEST"
    base   = run_id[:-5]  # strip _TEST
    if not re.match(r"^\d{4}_\d{2}_\d{2}_\d{6}$", base):
        _fail(f"Assertion 1: make_run_id() format wrong: {base!r}")
    _pass(f"Assertion 1: make_run_id() format OK → {run_id}")

    # ------------------------------------------------------------------
    # Assertion 2: ensure_run_dirs() creates expected dirs
    # ------------------------------------------------------------------
    paths = ensure_run_dirs(run_id)
    expected_keys = {
        "run", "inputs", "outputs", "audit", "summary",
        "exports", "corrections", "logs", "meta",
    }
    missing_keys = expected_keys - set(paths)
    if missing_keys:
        _fail(f"Assertion 2: ensure_run_dirs missing dict keys: {missing_keys}")
    for key, path in paths.items():
        if not path.is_dir():
            _fail(f"Assertion 2: directory not created: {key}={path}")
    _pass(f"Assertion 2: ensure_run_dirs created {len(paths)} directories OK")

    # ------------------------------------------------------------------
    # Assertion 3: copy_artifacts_to_run() handles missing gracefully
    #              and copies a dummy sanity_*.csv into the run folder
    # ------------------------------------------------------------------
    dummy_name = "sanity_smoke_check_dummy.csv"
    dummy_src  = ROOT / "audit" / "summary" / dummy_name
    dummy_src.parent.mkdir(parents=True, exist_ok=True)
    dummy_src.write_text("col1,col2\n1,2\n", encoding="utf-8")

    try:
        result = copy_artifacts_to_run(run_id, paths)
    finally:
        if dummy_src.exists():
            dummy_src.unlink()

    if not isinstance(result, dict):
        _fail(f"Assertion 3: copy_artifacts_to_run must return dict, got {type(result)}")
    for key in ("copied", "missing", "errors"):
        if key not in result:
            _fail(f"Assertion 3: result dict missing key '{key}'")
    if result["errors"]:
        _fail(f"Assertion 3: unexpected errors: {result['errors']}")

    dummy_dest = paths["summary"] / dummy_name
    if not dummy_dest.exists():
        _fail(
            f"Assertion 3: dummy file '{dummy_name}' not found in run summary dir.\n"
            f"  copied={result['copied']}"
        )
    _pass(
        f"Assertion 3: copy_artifacts_to_run OK "
        f"(copied={len(result['copied'])}, missing={len(result['missing'])})"
    )

    # ------------------------------------------------------------------
    # Assertion 4: write_run_manifest() writes valid JSON with expected fields
    # ------------------------------------------------------------------
    manifest_path = write_run_manifest(run_id, paths, extra={"smoke_test": True})

    if not manifest_path.exists():
        _fail("Assertion 4: manifest.json not written")

    with open(str(manifest_path), "r", encoding="utf-8") as f:
        manifest = json.load(f)

    for field in ("run_id", "created_at_local", "python_exe", "repo_root"):
        if field not in manifest:
            _fail(f"Assertion 4: manifest missing required field '{field}'")
    if manifest.get("run_id") != run_id:
        _fail(f"Assertion 4: run_id mismatch: {manifest.get('run_id')!r} != {run_id!r}")
    if not manifest.get("smoke_test"):
        _fail("Assertion 4: extra key 'smoke_test' not found in manifest")

    _pass(f"Assertion 4: manifest.json OK (fields: {list(manifest.keys())})")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    run_dir = paths["run"]
    shutil.rmtree(str(run_dir), ignore_errors=True)
    print(f"\n  [cleanup] removed test run folder: runs/{run_id}")

    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()
