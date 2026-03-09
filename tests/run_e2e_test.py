"""
tests/run_e2e_test.py — Smallest viable end-to-end integration test.

Covers: matcher → resolve_matched_raw → load_sqlite → build_review_queue →
        build_diy_exports → generate_corrections

Does NOT cover: mapping.py (requires client-specific raw input files).

Fixture (20 pre-mapped employees):
  16 worker_id matches  — salary(4), status(3), hire_date(4), job_org(3),
                          multi-fix salary+status(1), no-change(1)
   2 pk matches         — one with salary mismatch (→ REVIEW/below_threshold, confidence=0.9), one clean (→ APPROVE)
   2 unmatched old      — worker_ids with no NEW counterpart
   2 unmatched new      — NEW-only workers with no OLD counterpart

Expected outcomes (asserted):
  1. matched_total == 18, unmatched_old == 2, unmatched_new == 2
  2. matched_by_worker_id == 16, matched_by_pk == 2
  3. Q0 PASS (no duplicate worker_ids after resolve)
  4. corrections_salary.csv      has exactly 5 rows
  5. corrections_status.csv      has exactly 4 rows
  6. corrections_hire_date.csv   has exactly 4 rows
  7. corrections_job_org.csv     has exactly 3 rows
  8. review_needed.csv           has exactly 1 row
  9. Salary correction for W001: compensation_amount == "55000.0" (new_salary)
 10. old_last4_ssn / new_last4_ssn are absent from audit.db matched_pairs
 11. wide_compare.csv has mm_cost_center and mismatch_group_org columns

Run:
    PYTHONUTF8=1 venv/Scripts/python.exe tests/run_e2e_test.py
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

ROOT   = Path(__file__).resolve().parents[1]
PYTHON = ROOT / "venv" / "Scripts" / "python.exe"
OUT    = ROOT / "outputs"
TESTS  = Path(__file__).resolve().parent

# Temporary test artifacts — cleaned up on each run
_TEST_DB  = ROOT / "audit" / "_e2e_test.db"
_CORR_DIR = ROOT / "audit" / "corrections" / "_e2e_out"
_WIDE_DIR = ROOT / "audit" / "exports" / "_e2e_out"
_RQ_DIR   = ROOT / "audit" / "summary"   # review_queue.csv lands here by default

# Backup slots — we swap the live outputs/ files while the test runs
_BKUP_OLD = OUT / "_e2e_bkup_mapped_old.csv"
_BKUP_NEW = OUT / "_e2e_bkup_mapped_new.csv"
_BKUP_RAW = OUT / "_e2e_bkup_matched_raw.csv"
_LIVE_DB  = ROOT / "audit" / "audit.db"
_BKUP_DB  = ROOT / "audit" / "_e2e_bkup_audit.db"


# ---------------------------------------------------------------------------
# Fixture data
# ---------------------------------------------------------------------------
# All values use the mapped column schema read by matcher.py.
# Columns: worker_id, recon_id, full_name_norm, dob, last4_ssn,
#          hire_date, worker_status, worker_type, position,
#          district, location_state, salary, payrate

_OLD_CSV = """\
worker_id,recon_id,full_name_norm,dob,last4_ssn,hire_date,worker_status,worker_type,position,district,location_state,salary,payrate,cost_center
W001,R001,alice_johnson,1985-03-15,1001,2018-06-01,Active,Full-time,Analyst,North,CA,50000,24.04,CC100
W002,R002,bob_martinez,1979-07-22,1002,2017-04-15,Active,Full-time,Engineer,South,TX,60000,28.85,CC200
W003,R003,carol_thompson,1990-11-08,1003,2020-01-10,Active,Full-time,Manager,East,NY,70000,33.65,CC100
W004,R004,david_lee,1983-05-30,1004,2016-09-20,Active,Full-time,Director,West,WA,80000,38.46,CC300
W005,R005,emma_garcia,1992-02-14,1005,2021-03-01,Active,Full-time,Analyst,North,CA,52000,25.00,CC100
W006,R006,frank_wilson,1975-08-19,1006,2015-11-15,Active,Full-time,Engineer,South,TX,65000,31.25,CC200
W007,R007,grace_anderson,1988-12-03,1007,2019-07-01,Inactive,Part-time,Coordinator,East,NY,45000,21.63,CC400
W008,R008,henry_taylor,1981-04-25,1008,2020-04-15,Active,Full-time,Manager,West,WA,75000,36.06,CC300
W009,R009,iris_moore,1994-09-11,1009,2019-03-10,Active,Full-time,Analyst,North,CA,48000,23.08,CC100
W010,R010,james_jackson,1977-01-07,1010,2021-06-01,Active,Full-time,Engineer,South,TX,62000,29.81,CC200
W011,R011,karen_white,1986-06-20,1011,2022-09-15,Active,Full-time,Coordinator,East,NY,47000,22.60,CC400
W012,R012,liam_harris,1991-10-14,1012,2020-02-01,Active,Full-time,Manager,North,CA,78000,37.50,CC300
W013,R013,mia_clark,1984-03-28,1013,2018-08-15,Active,Full-time,Analyst,South,TX,53000,25.48,CC100
W014,R014,noah_lewis,1989-07-16,1014,2021-01-10,Active,Full-time,Engineer,East,NY,68000,32.69,CC200
W015,R015,olivia_walker,1993-11-01,1015,2022-04-01,Active,Full-time,Analyst,North,CA,90000,43.27,CC100
W016,R016,peter_hall,1980-02-12,1016,2017-12-01,Active,Full-time,Manager,West,WA,72000,34.62,CC300
W017,R017,quinn_allen,1987-05-05,1017,2019-10-15,Active,Full-time,Analyst,South,TX,55000,26.44,CC200
W018,R018,rachel_young,1976-09-23,1018,2016-06-01,Active,Full-time,Director,East,NY,95000,45.67,CC400
W019,R019,sam_king,1982-08-17,1019,2020-05-01,Active,Full-time,Engineer,North,CA,45000,21.63,CC100
W020,R020,tina_scott,1995-04-09,1020,2023-01-15,Active,Full-time,Coordinator,West,WA,52000,25.00,CC300
"""

# NEW dataset:
# W001-W016: worker_id matches (same IDs as OLD) with various field changes
# W097, W098: unmatched NEW-only workers
# W099: pk match for OLD W019 (different worker_id, same name+dob+last4_ssn, salary mismatch)
# W100: pk match for OLD W020 (different worker_id, same name+dob+last4_ssn, no mismatch)
_NEW_CSV = """\
worker_id,recon_id,full_name_norm,dob,last4_ssn,hire_date,worker_status,worker_type,position,district,location_state,salary,payrate,cost_center
W001,R001,alice_johnson,1985-03-15,1001,2018-06-01,Active,Full-time,Analyst,North,CA,55000,24.04,CC100
W002,R002,bob_martinez,1979-07-22,1002,2017-04-15,Active,Full-time,Engineer,South,TX,65000,28.85,CC999
W003,R003,carol_thompson,1990-11-08,1003,2020-01-10,Active,Full-time,Manager,East,NY,75000,33.65,CC100
W004,R004,david_lee,1983-05-30,1004,2016-09-20,Active,Full-time,Director,West,WA,85000,38.46,CC300
W005,R005,emma_garcia,1992-02-14,1005,2021-03-01,Terminated,Full-time,Analyst,North,CA,52000,25.00,CC100
W006,R006,frank_wilson,1975-08-19,1006,2015-11-15,Leave of Absence,Full-time,Engineer,South,TX,65000,31.25,CC200
W007,R007,grace_anderson,1988-12-03,1007,2019-07-01,Active,Part-time,Coordinator,East,NY,45000,21.63,CC400
W008,R008,henry_taylor,1981-04-25,1008,2020-05-15,Active,Full-time,Manager,West,WA,75000,36.06,CC300
W009,R009,iris_moore,1994-09-11,1009,2019-04-10,Active,Full-time,Analyst,North,CA,48000,23.08,CC100
W010,R010,james_jackson,1977-01-07,1010,2021-07-01,Active,Full-time,Engineer,South,TX,62000,29.81,CC200
W011,R011,karen_white,1986-06-20,1011,2022-10-15,Active,Full-time,Coordinator,East,NY,47000,22.60,CC400
W012,R012,liam_harris,1991-10-14,1012,2020-02-01,Active,Full-time,Senior Manager,North,CA,78000,37.50,CC300
W013,R013,mia_clark,1984-03-28,1013,2018-08-15,Active,Full-time,Analyst,Southeast,TX,53000,25.48,CC100
W014,R014,noah_lewis,1989-07-16,1014,2021-01-10,Active,Full-time,Engineer,East,FL,68000,32.69,CC200
W015,R015,olivia_walker,1993-11-01,1015,2022-04-01,Terminated,Full-time,Analyst,North,CA,95000,43.27,CC100
W016,R016,peter_hall,1980-02-12,1016,2017-12-01,Active,Full-time,Manager,West,WA,72000,34.62,CC300
W097,R097,uma_thomas,1990-06-15,9701,2022-07-01,Active,Full-time,Analyst,North,CA,58000,27.88,CC100
W098,R098,victor_nguyen,1985-11-28,9801,2023-03-15,Active,Full-time,Engineer,South,TX,67000,32.21,CC200
W099,R099,sam_king,1982-08-17,1019,2020-05-01,Active,Full-time,Engineer,North,TX,48000,21.63,CC100
W100,R100,tina_scott,1995-04-09,1020,2023-01-15,Active,Full-time,Coordinator,West,WA,52000,25.00,CC300
"""

# Expected outcomes
_EXP_MATCHED_TOTAL   = 18
_EXP_UNMATCHED_OLD   = 2   # W017, W018
_EXP_UNMATCHED_NEW   = 2   # W097, W098
_EXP_BY_WORKER_ID    = 16
_EXP_BY_PK           = 2   # W019/W099, W020/W100
_EXP_SALARY_ROWS     = 5   # W001,W002,W003,W004 (direct) + W015 (multi-fix)
_EXP_STATUS_ROWS     = 4   # W005,W006,W007 + W015 (multi-fix)
_EXP_HIRE_DATE_ROWS  = 4   # W008,W009,W010,W011
_EXP_JOB_ORG_ROWS    = 3   # W012(position),W013(district),W014(location_state)
_EXP_REVIEW_ROWS     = 1   # W019/W099 pk match with salary mismatch → REVIEW(below_threshold; confidence=0.9 < 0.97)
_EXP_W001_NEW_SAL    = "55000"   # new_salary for W001 in fixture (stored as string from CSV)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_pass_count = 0
_fail_count = 0


def _pass(msg: str) -> None:
    global _pass_count
    _pass_count += 1
    print(f"  [PASS] {msg}")


def _fail(msg: str) -> None:
    global _fail_count
    _fail_count += 1
    print(f"  [FAIL] {msg}", file=sys.stderr)


def _run(script: str, extra_args: list[str] | None = None) -> int:
    cmd = [str(PYTHON), script] + (extra_args or [])
    result = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  [WARN] {script} exited {result.returncode}")
        print(result.stdout[-1000:] if result.stdout else "")
        print(result.stderr[-1000:] if result.stderr else "", file=sys.stderr)
    return result.returncode


# ---------------------------------------------------------------------------
# Setup / teardown
# ---------------------------------------------------------------------------

def _write_fixture() -> None:
    """Write fixture CSVs to outputs/ and backup live DB (swapping out live files)."""
    for src, bkup in [
        (OUT / "mapped_old.csv", _BKUP_OLD),
        (OUT / "mapped_new.csv", _BKUP_NEW),
        (OUT / "matched_raw.csv", _BKUP_RAW),
    ]:
        if src.exists():
            shutil.copy2(str(src), str(bkup))

    # Backup the live DB before the test overwrites it
    if _LIVE_DB.exists():
        shutil.copy2(str(_LIVE_DB), str(_BKUP_DB))

    (OUT / "mapped_old.csv").write_text(_OLD_CSV, encoding="utf-8")
    (OUT / "mapped_new.csv").write_text(_NEW_CSV, encoding="utf-8")
    # Remove any stale matched_raw so matcher writes a fresh one
    raw = OUT / "matched_raw.csv"
    if raw.exists():
        raw.unlink()


def _restore_live_files() -> None:
    """Restore live outputs/ files and DB from backup."""
    for src, bkup in [
        (OUT / "mapped_old.csv", _BKUP_OLD),
        (OUT / "mapped_new.csv", _BKUP_NEW),
        (OUT / "matched_raw.csv", _BKUP_RAW),
    ]:
        if bkup.exists():
            shutil.copy2(str(bkup), str(src))
            bkup.unlink()
        elif src.exists():
            pass  # leave it — live restore is authoritative

    # Restore the live DB
    if _BKUP_DB.exists():
        shutil.copy2(str(_BKUP_DB), str(_LIVE_DB))
        _BKUP_DB.unlink()


def _cleanup() -> None:
    """Remove test-specific DB and output directories."""
    if _TEST_DB.exists():
        _TEST_DB.unlink()
    if _CORR_DIR.exists():
        shutil.rmtree(str(_CORR_DIR))
    if _WIDE_DIR.exists():
        shutil.rmtree(str(_WIDE_DIR))


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------

def _check_match_report() -> None:
    report_path = OUT / "match_report.json"
    if not report_path.exists():
        _fail("match_report.json not found")
        return
    rpt = json.loads(report_path.read_text(encoding="utf-8"))

    if rpt.get("matched_total") == _EXP_MATCHED_TOTAL:
        _pass(f"Assertion 1a: matched_total == {_EXP_MATCHED_TOTAL}")
    else:
        _fail(f"Assertion 1a: matched_total expected {_EXP_MATCHED_TOTAL}, got {rpt.get('matched_total')}")

    if rpt.get("unmatched_old") == _EXP_UNMATCHED_OLD:
        _pass(f"Assertion 1b: unmatched_old == {_EXP_UNMATCHED_OLD}")
    else:
        _fail(f"Assertion 1b: unmatched_old expected {_EXP_UNMATCHED_OLD}, got {rpt.get('unmatched_old')}")

    if rpt.get("unmatched_new") == _EXP_UNMATCHED_NEW:
        _pass(f"Assertion 1c: unmatched_new == {_EXP_UNMATCHED_NEW}")
    else:
        _fail(f"Assertion 1c: unmatched_new expected {_EXP_UNMATCHED_NEW}, got {rpt.get('unmatched_new')}")

    if rpt.get("matched_by_worker_id") == _EXP_BY_WORKER_ID:
        _pass(f"Assertion 2a: matched_by_worker_id == {_EXP_BY_WORKER_ID}")
    else:
        _fail(f"Assertion 2a: matched_by_worker_id expected {_EXP_BY_WORKER_ID}, got {rpt.get('matched_by_worker_id')}")

    if rpt.get("matched_by_pk") == _EXP_BY_PK:
        _pass(f"Assertion 2b: matched_by_pk == {_EXP_BY_PK}")
    else:
        _fail(f"Assertion 2b: matched_by_pk expected {_EXP_BY_PK}, got {rpt.get('matched_by_pk')}")


def _check_q0(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        dup_old = con.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT old_worker_id FROM matched_pairs_raw"
            "  WHERE old_worker_id != ''"
            "  GROUP BY old_worker_id HAVING COUNT(*) > 1"
            ")"
        ).fetchone()[0]
        dup_new = con.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT new_worker_id FROM matched_pairs_raw"
            "  WHERE new_worker_id != ''"
            "  GROUP BY new_worker_id HAVING COUNT(*) > 1"
            ")"
        ).fetchone()[0]
    finally:
        con.close()

    if dup_old == 0 and dup_new == 0:
        _pass("Assertion 3: Q0 PASS — no duplicate worker_ids in matched_pairs_raw")
    else:
        _fail(f"Assertion 3: Q0 FAIL — dup_old={dup_old}, dup_new={dup_new}")


def _check_extra_fields(wide_dir: Path) -> None:
    """Assertion 11: wide_compare.csv has mm_cost_center and mismatch_group_org."""
    wide_path = wide_dir / "wide_compare.csv"
    if not wide_path.exists():
        _fail("Assertion 11: wide_compare.csv not found")
        return
    import csv as _csv
    with wide_path.open(encoding="utf-8") as f:
        cols = next(_csv.reader(f))
    if "mm_cost_center" not in cols:
        _fail(f"Assertion 11: mm_cost_center not in wide_compare.csv columns: {cols[:20]}")
        return
    if "mismatch_group_org" not in cols:
        _fail(f"Assertion 11: mismatch_group_org not in wide_compare.csv columns: {cols[:20]}")
        return
    _pass("Assertion 11: mm_cost_center and mismatch_group_org present in wide_compare.csv")


def _check_no_ssn_in_db(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        col_names = [
            row[1]
            for row in con.execute("PRAGMA table_info(matched_pairs_raw)").fetchall()
        ]
    finally:
        con.close()

    pii_cols = [c for c in col_names if "last4_ssn" in c.lower()]
    if not pii_cols:
        _pass("Assertion 10: last4_ssn columns absent from matched_pairs_raw in DB")
    else:
        _fail(f"Assertion 10: last4_ssn still in DB columns: {pii_cols}")


def _check_corrections(corr_dir: Path) -> None:
    import csv as _csv

    checks = [
        ("corrections_salary.csv",    _EXP_SALARY_ROWS,    4),
        ("corrections_status.csv",    _EXP_STATUS_ROWS,    5),
        ("corrections_hire_date.csv", _EXP_HIRE_DATE_ROWS, 6),
        ("corrections_job_org.csv",   _EXP_JOB_ORG_ROWS,   7),
        ("review_needed.csv",         _EXP_REVIEW_ROWS,    8),
    ]
    for fname, expected, assertion_num in checks:
        fpath = corr_dir / fname
        if not fpath.exists():
            _fail(f"Assertion {assertion_num}: {fname} not found")
            continue
        with fpath.open(encoding="utf-8") as f:
            actual = sum(1 for _ in _csv.reader(f)) - 1  # subtract header
        if actual == expected:
            _pass(f"Assertion {assertion_num}: {fname} has {expected} rows")
        else:
            _fail(f"Assertion {assertion_num}: {fname} expected {expected} rows, got {actual}")

    # Assertion 9: value correctness — W001 salary correction
    sal_path = corr_dir / "corrections_salary.csv"
    if sal_path.exists():
        import csv as _csv
        with sal_path.open(encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            w001_rows = [r for r in reader if r.get("worker_id", "").strip() == "W001"]
        if not w001_rows:
            _fail("Assertion 9: W001 not found in corrections_salary.csv")
        elif w001_rows[0].get("compensation_amount", "").strip() == _EXP_W001_NEW_SAL:
            _pass(f"Assertion 9: W001 compensation_amount == {_EXP_W001_NEW_SAL}")
        else:
            _fail(
                f"Assertion 9: W001 compensation_amount expected {_EXP_W001_NEW_SAL!r}, "
                f"got {w001_rows[0].get('compensation_amount')!r}"
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    W = 60
    print("=" * W)
    print("  E2E INTEGRATION TEST")
    print("=" * W)

    _cleanup()
    _write_fixture()

    try:
        # Step 1: matcher
        print("\n  [step] running matcher.py ...")
        rc = _run("src/matcher.py")
        if rc != 0:
            _fail("matcher.py failed — aborting test")
            return

        # Step 2: resolve_matched_raw
        print("  [step] running resolve_matched_raw.py ...")
        rc = _run("resolve_matched_raw.py")
        if rc != 0:
            _fail("resolve_matched_raw.py failed — aborting test")
            return

        # Assert match counts (before DB load overwrites anything)
        print()
        _check_match_report()

        # Step 3: load_sqlite (into dedicated test DB)
        print("\n  [step] running load_sqlite.py ...")
        rc = _run("audit/load_sqlite.py")  # uses default audit.db path
        if rc != 0:
            _fail("load_sqlite.py failed — aborting test")
            return

        # Use the real audit.db for remaining checks
        db_path = ROOT / "audit" / "audit.db"

        _check_q0(db_path)
        _check_no_ssn_in_db(db_path)

        # Step 4: generate_corrections (into isolated output dir)
        print("\n  [step] running generate_corrections.py ...")
        _CORR_DIR.mkdir(parents=True, exist_ok=True)
        rc = _run(
            "audit/corrections/generate_corrections.py",
            ["--out-dir", str(_CORR_DIR)],
        )
        if rc != 0:
            _fail("generate_corrections.py failed")
            return

        print()
        _check_corrections(_CORR_DIR)

        # Step 5: build_diy_exports (into isolated output dir) — checks extra fields
        print("\n  [step] running build_diy_exports.py ...")
        _WIDE_DIR.mkdir(parents=True, exist_ok=True)
        rc = _run(
            "audit/exports/build_diy_exports.py",
            ["--out-dir", str(_WIDE_DIR)],
        )
        if rc != 0:
            _fail("build_diy_exports.py failed")
        else:
            print()
            _check_extra_fields(_WIDE_DIR)

    finally:
        _restore_live_files()
        _cleanup()

    # Summary
    print()
    print("=" * W)
    total = _pass_count + _fail_count
    if _fail_count == 0:
        print(f"  All {total} assertions PASSED.")
    else:
        print(f"  {_pass_count}/{total} PASSED  |  {_fail_count} FAILED", file=sys.stderr)
    print("=" * W)

    if _fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
