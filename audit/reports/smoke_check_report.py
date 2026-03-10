"""
smoke_check_report.py — Smoke test for generate_report.py

Creates a minimal in-memory SQLite database with synthetic matched_pairs data,
runs generate_report.main(), and asserts that a non-empty .docx is produced.

Run:
    venv/Scripts/python.exe audit/reports/smoke_check_report.py

Exit codes:
    0  — all checks passed
    1  — one or more checks failed
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Synthetic data
# ---------------------------------------------------------------------------
_PAIRS = [
    # (pair_id, match_source, confidence,
    #  old_worker_id, new_worker_id,
    #  old_full_name_norm, new_full_name_norm,
    #  old_salary, new_salary,
    #  old_payrate, new_payrate,
    #  old_worker_status, new_worker_status,
    #  old_hire_date, new_hire_date,
    #  old_position, new_position,
    #  old_district, new_district,
    #  old_dob, new_dob)
    (1, "worker_id",  1.0, "W001", "W001", "Alice Smith",  "Alice Smith",
     75000, 76000, 36.06, 36.54, "Active",    "Active",
     "2019-06-01", "2019-06-01", "Analyst",   "Analyst",
     "NYC",        "NYC",        "1990-01-01", "1990-01-01"),
    (2, "worker_id",  1.0, "W002", "W002", "Bob Jones",    "Bob Jones",
     52000, 52000, 25.0,  25.0,  "Active",    "Terminated",
     "2020-03-15", "2020-03-15", "Specialist","Specialist",
     "LA",         "LA",         "1985-06-15", "1985-06-15"),
    (3, "dob_name",   0.93, "W003", "W003", "Carol White",  "Carol White",
     40000, 40000, 19.23, 19.23, "Active",    "Active",
     "2026-02-01", "2026-02-01", "Clerk",     "Clerk",
     "CHI",        "CHI",        "1992-09-20", "1992-09-20"),
    (4, "worker_id",  1.0, "W004", "W004", "Dave Brown",   "Dave Brown",
     0,     0,     0.0,   0.0,   "Active",    "Active",
     "2021-01-10", "2021-01-10", "Driver",    "Driver",
     "DAL",        "DAL",        "1988-03-03", "1988-03-03"),
    (5, "dob_name",   0.60, "W005", "X999", "Eve Davis",    "Frank Garcia",
     95000, 250000, 45.67, 120.19, "Active",  "Active",
     "2018-08-08", "2016-04-01", "Manager",  "Director",
     "SEA",        "SEA",        "1983-11-11", "1975-05-22"),
]

_CREATE = """
CREATE TABLE matched_pairs (
    pair_id            INTEGER PRIMARY KEY,
    match_source       TEXT,
    confidence         REAL,
    old_worker_id      TEXT,
    new_worker_id      TEXT,
    old_full_name_norm TEXT,
    new_full_name_norm TEXT,
    old_salary         REAL,
    new_salary         REAL,
    old_payrate        REAL,
    new_payrate        REAL,
    old_worker_status  TEXT,
    new_worker_status  TEXT,
    old_hire_date      TEXT,
    new_hire_date      TEXT,
    old_position       TEXT,
    new_position       TEXT,
    old_district       TEXT,
    new_district       TEXT,
    old_dob            TEXT,
    new_dob            TEXT
)
"""

_INSERT = """
INSERT INTO matched_pairs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def _build_db(path: str) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute(_CREATE)
        con.executemany(_INSERT, _PAIRS)
        con.commit()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------
_CHECKS: list[tuple[str, bool]] = []


def _check(label: str, expr: bool) -> None:
    _CHECKS.append((label, expr))
    status = "PASS" if expr else "FAIL"
    print(f"  [{status}] {label}")


def run() -> int:
    print("smoke_check_report: building synthetic DB ...")
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        db_path  = tmp_path / "test_audit.db"
        out_path = tmp_path / "audit_report.docx"

        _build_db(str(db_path))

        # Import and run the generator
        import audit.reports.generate_report as rg  # noqa: E402
        try:
            rg.main([
                "--db",  str(db_path),
                "--out", str(out_path),
            ])
        except SystemExit as exc:
            if exc.code != 0:
                _check("main() exited with code 0", False)
                return 1

        _check("audit_report.docx created", out_path.exists())
        if out_path.exists():
            size = out_path.stat().st_size
            _check("audit_report.docx is non-empty (> 5 KB)", size > 5_000)
            print(f"       file size: {size:,} bytes")

        # Verify it's a valid ZIP/DOCX (starts with PK magic bytes)
        if out_path.exists():
            with out_path.open("rb") as f:
                magic = f.read(2)
            _check("audit_report.docx has valid DOCX magic bytes (PK)", magic == b"PK")

    failed = sum(1 for _, ok in _CHECKS if not ok)
    total  = len(_CHECKS)
    print()
    print(f"  {total - failed}/{total} checks passed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(run())
