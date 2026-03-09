"""
smoke_check_corrections.py — Verifies the Correction File Generator output.

Assertions
----------
1. All expected output files exist.
2. corrections_manifest.csv exists.
3. Manifest row count == sum of rows in each corrections_*.csv.
4. review_needed.csv has at least 1 row (expect 5 from pk matches).
5. Every pair_id in review_needed appears at most once.
6. review_needed pair_ids match rows classified as REVIEW by the gating engine.
7. Correction file headers match the required column schemas.
8. --dry-run exits 0, prints "DRY RUN", shows non-zero counts, writes no files.

Run:
    venv/Scripts/python.exe audit/corrections/smoke_check_corrections.py
"""
from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

_HERE        = Path(__file__).resolve().parent       # audit/corrections/
_SUMMARY_DIR = _HERE.parent / "summary"              # audit/summary/
sys.path.insert(0, str(_SUMMARY_DIR))

import gating
from gating import infer_fix_types, classify_all

ROOT    = _HERE.parents[1]
DB_PATH = ROOT / "audit" / "audit.db"
OUT_DIR = _HERE / "out"

EXPECTED_FILES = [
    "corrections_salary.csv",
    "corrections_status.csv",
    "corrections_hire_date.csv",
    "corrections_job_org.csv",
    "review_needed.csv",
    "corrections_manifest.csv",
]

CORRECTION_FILES = [
    "corrections_salary.csv",
    "corrections_status.csv",
    "corrections_hire_date.csv",
    "corrections_job_org.csv",
]

EXPECTED_HEADERS = {
    "corrections_salary.csv": [
        "worker_id", "effective_date", "compensation_amount",
        "currency", "reason", "pair_id", "match_source", "confidence", "summary",
    ],
    "corrections_status.csv": [
        "worker_id", "effective_date", "worker_status",
        "reason", "pair_id", "match_source", "confidence", "summary",
    ],
    "corrections_hire_date.csv": [
        "worker_id", "hire_date",
        "reason", "pair_id", "match_source", "confidence", "summary",
    ],
    "corrections_job_org.csv": [
        "worker_id", "effective_date", "position", "district",
        "location_state", "location",
        "reason", "pair_id", "match_source", "confidence", "summary",
    ],
    "review_needed.csv": [
        "worker_id", "pair_id", "match_source", "fix_types",
        "action", "reason", "confidence", "min_confidence", "summary",
    ],
    "corrections_manifest.csv": [
        "correction_type", "worker_id", "pair_id", "match_source",
        "fix_types", "action", "confidence", "summary", "output_file",
    ],
}


def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(1)


def _pass(msg: str) -> None:
    print(f"  [PASS] {msg}")


def main() -> None:
    print("=" * 60)
    print("  SMOKE CHECK: Correction File Generator")
    print("=" * 60)

    # Run the generator first
    print("\n  Running generate_corrections.main() ...")
    sys.path.insert(0, str(_HERE))
    import generate_corrections
    generate_corrections.main()
    print()

    # ------------------------------------------------------------------
    # Assertion 1: All expected output files exist
    # ------------------------------------------------------------------
    missing_files = [f for f in EXPECTED_FILES if not (OUT_DIR / f).exists()]
    if missing_files:
        _fail(f"Assertion 1 FAILED: missing output files: {missing_files}")
    _pass(f"Assertion 1: all {len(EXPECTED_FILES)} expected output files exist")

    # ------------------------------------------------------------------
    # Assertion 2: corrections_manifest.csv exists (redundant but explicit)
    # ------------------------------------------------------------------
    manifest_path = OUT_DIR / "corrections_manifest.csv"
    if not manifest_path.exists():
        _fail("Assertion 2 FAILED: corrections_manifest.csv missing")
    manifest = pd.read_csv(str(manifest_path))
    _pass(f"Assertion 2: corrections_manifest.csv exists ({len(manifest):,} rows)")

    # ------------------------------------------------------------------
    # Assertion 3: manifest row count == sum of correction file rows
    # ------------------------------------------------------------------
    correction_total = 0
    for fname in CORRECTION_FILES:
        df = pd.read_csv(str(OUT_DIR / fname))
        correction_total += len(df)

    if len(manifest) != correction_total:
        _fail(
            f"Assertion 3 FAILED: manifest has {len(manifest):,} rows "
            f"but sum of correction files is {correction_total:,}"
        )
    _pass(f"Assertion 3: manifest rows ({len(manifest):,}) == sum of correction files ({correction_total:,})")

    # ------------------------------------------------------------------
    # Assertion 4: review_needed has >= 1 row
    # ------------------------------------------------------------------
    review = pd.read_csv(str(OUT_DIR / "review_needed.csv"))
    if len(review) == 0:
        _fail("Assertion 4 FAILED: review_needed.csv is empty (expected >= 1 row)")
    _pass(f"Assertion 4: review_needed.csv has {len(review):,} rows")

    # ------------------------------------------------------------------
    # Assertion 5: each pair_id appears at most once in review_needed
    # ------------------------------------------------------------------
    dup_pairs = review[review["pair_id"].duplicated(keep=False)]
    if len(dup_pairs) > 0:
        _fail(f"Assertion 5 FAILED: {len(dup_pairs)} duplicate pair_ids in review_needed: "
              f"{dup_pairs['pair_id'].tolist()[:3]}")
    _pass(f"Assertion 5: all pair_ids in review_needed are unique ({len(review):,} pairs)")

    # ------------------------------------------------------------------
    # Assertion 6: review_needed pair_ids match gating engine REVIEW pairs
    # ------------------------------------------------------------------
    if not DB_PATH.exists():
        print("  [SKIP] Assertion 6: audit.db not found, skipping gating cross-check")
    else:
        con = sqlite3.connect(str(DB_PATH))
        try:
            mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)
        finally:
            con.close()

        if "confidence" not in mp.columns:
            mp = mp.copy()
            mp["confidence"] = None

        expected_review_pairs: set[str] = set()
        for r in mp.to_dict(orient="records"):
            result = classify_all(r)
            if result["fix_types"] and any(v["action"] == "REVIEW" for v in result["per_fix"].values()):
                expected_review_pairs.add(str(r.get("pair_id", "")))

        actual_review_pairs = set(review["pair_id"].astype(str).tolist())
        if actual_review_pairs != expected_review_pairs:
            only_in_file = actual_review_pairs - expected_review_pairs
            only_in_gate = expected_review_pairs - actual_review_pairs
            _fail(
                f"Assertion 6 FAILED: review_needed mismatch.\n"
                f"  Only in file (unexpected): {list(only_in_file)[:5]}\n"
                f"  Only from gating (missing): {list(only_in_gate)[:5]}"
            )
        _pass(f"Assertion 6: review_needed pair_ids match gating engine "
              f"({len(actual_review_pairs)} REVIEW pairs)")

    # ------------------------------------------------------------------
    # Assertion 7: Correction file headers match required schemas
    # ------------------------------------------------------------------
    header_failures: list[str] = []
    for fname, expected_cols in EXPECTED_HEADERS.items():
        df = pd.read_csv(str(OUT_DIR / fname), nrows=0)
        actual_cols = list(df.columns)
        if actual_cols != expected_cols:
            header_failures.append(
                f"{fname}: expected {expected_cols}, got {actual_cols}"
            )
    if header_failures:
        _fail(f"Assertion 7 FAILED: header mismatches:\n  " + "\n  ".join(header_failures))
    _pass(f"Assertion 7: all {len(EXPECTED_HEADERS)} file headers match required schemas")

    # ------------------------------------------------------------------
    # Assertion 8: --dry-run exits 0, prints "DRY RUN", non-zero counts,
    #              and writes NO files
    # ------------------------------------------------------------------
    print("\n  Running --dry-run assertion ...")
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_out = Path(tmpdir) / "dry_run_test_out"
        # temp_out must NOT exist before the run
        assert not temp_out.exists(), "temp dir already exists before dry-run"

        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT)

        proc = subprocess.run(
            [
                sys.executable,
                str(_HERE / "generate_corrections.py"),
                "--dry-run",
                "--out-dir", str(temp_out),
            ],
            capture_output=True,
            text=True,
            env=env,
        )

        if proc.returncode != 0:
            _fail(
                f"Assertion 8 FAILED: --dry-run exited with code {proc.returncode}\n"
                f"stdout: {proc.stdout[:500]}\nstderr: {proc.stderr[:300]}"
            )

        stdout = proc.stdout
        if "DRY RUN" not in stdout:
            _fail(
                f"Assertion 8 FAILED: 'DRY RUN' not found in dry-run output.\n"
                f"Output: {stdout[:500]}"
            )

        # Check that at least one "N rows" count is non-zero
        counts = re.findall(r":\s+([\d,]+)\s+rows", stdout)
        count_vals = [int(c.replace(",", "")) for c in counts]
        if not count_vals or not any(v > 0 for v in count_vals):
            _fail(
                f"Assertion 8 FAILED: no non-zero row counts found in dry-run output.\n"
                f"Output: {stdout[:500]}"
            )

        if temp_out.exists():
            created = list(temp_out.iterdir()) if temp_out.is_dir() else []
            _fail(
                f"Assertion 8 FAILED: --dry-run created output directory with "
                f"{len(created)} file(s): {[f.name for f in created[:5]]}"
            )

    _pass(
        "Assertion 8: --dry-run exits 0, prints 'DRY RUN', "
        "shows non-zero counts, writes no files"
    )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n  File size summary:")
    for fname in EXPECTED_FILES:
        df = pd.read_csv(str(OUT_DIR / fname))
        print(f"    {fname:<35}  {len(df):>8,} rows")

    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")


if __name__ == "__main__":
    main()
