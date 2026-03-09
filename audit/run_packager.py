# audit/run_packager.py
"""Package a completed audit run into a timestamped folder."""
from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

AUDIT_DIR = Path(__file__).parent
PROJECT_ROOT = AUDIT_DIR.parent
RUNS_DIR = AUDIT_DIR / "audit_runs"
ARCHIVE_DIR = AUDIT_DIR / "audit_archive"

RETENTION_COUNT = 20

# Static source files (always hashed). Finalized source is dynamic — added at runtime.
_SOURCE_FILES = [
    PROJECT_ROOT / "outputs" / "matched_raw.csv",
]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _count(con: sqlite3.Connection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _check_salary_quality(con: sqlite3.Connection) -> list[dict]:
    """Return salary quality stats for old_salary and new_salary.

    bad_char_count: values containing '$' or ',' or that fail float conversion.
    blank_count:    empty strings (NULL already cleaned to '' by load_sqlite).
    """
    rows = con.execute("SELECT old_salary, new_salary FROM matched_pairs_raw").fetchall()
    total = len(rows)

    result = []
    for side, idx in [("old", 0), ("new", 1)]:
        bad_char = 0
        blank = 0
        for row in rows:
            val = (row[idx] or "").strip()
            if not val:
                blank += 1
            elif "$" in val or "," in val:
                bad_char += 1
            else:
                try:
                    float(val)
                except ValueError:
                    bad_char += 1
        result.append({
            "field_side": side,
            "bad_char_count": bad_char,
            "blank_count": blank,
            "total_rows_checked": total,
        })
    return result


def _folder_size_bytes(folder: Path) -> int:
    return sum(f.stat().st_size for f in folder.rglob("*") if f.is_file())


def _apply_retention(current_run_dir: Path) -> None:
    """Archive oldest run folders when total count exceeds RETENTION_COUNT.

    The current (just-created) run is never touched.
    A folder is only deleted after its zip is confirmed to exist.
    """
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    # All completed run folders except the one just created
    other_runs = sorted(
        [
            d for d in RUNS_DIR.iterdir()
            if d.is_dir() and d.name.startswith("run_") and d != current_run_dir
        ],
        key=lambda d: d.name,  # lexicographic == chronological for run_YYYYMMDD_HHMMSS
    )

    # +1 to account for current_run_dir, which is always kept
    overflow = (len(other_runs) + 1) - RETENTION_COUNT
    if overflow <= 0:
        return

    to_archive = other_runs[:overflow]
    total_freed = 0
    archived = 0

    for folder in to_archive:
        folder_bytes = _folder_size_bytes(folder)
        zip_base = str(ARCHIVE_DIR / folder.name)

        # Create zip first — only delete original on success
        shutil.make_archive(zip_base, "zip", RUNS_DIR, folder.name)
        zip_path = Path(zip_base + ".zip")

        if zip_path.exists():
            shutil.rmtree(folder)
            total_freed += folder_bytes
            archived += 1

    freed_mb = round(total_freed / (1024 * 1024), 2)
    print(f"\n[retention] archived {archived} run(s)  |  freed {freed_mb} MB")


def package_run(
    db_path: str,
    output_csv_paths: list[str],
    finalized_csv_path: "Path | None" = None,
) -> Path:
    """Package audit outputs into a timestamped run folder.

    Parameters
    ----------
    db_path:
        Path to the SQLite database used for this run.
    output_csv_paths:
        List of CSV file paths produced by run_audit.py.
    finalized_csv_path:
        The finalized CSV actually loaded into audit.db (None if none was found).

    Returns
    -------
    Path to the created run folder.
    """
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_dir = RUNS_DIR / f"run_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    reports_dir = run_dir / "reports"
    reports_dir.mkdir(exist_ok=True)

    db = Path(db_path)

    # --- copy audit.db ---
    shutil.copy2(db, run_dir / "audit.db")

    # --- copy CSVs into reports/ ---
    copied_csvs = 0
    for csv_path in output_csv_paths:
        p = Path(csv_path)
        if p.exists():
            shutil.copy2(p, reports_dir / p.name)
            copied_csvs += 1

    # --- query DB for summary metrics ---
    con = sqlite3.connect(db)

    total_matched = _count(con, "SELECT COUNT(*) FROM matched_pairs_raw")

    salary_mismatch = _count(
        con,
        """
        SELECT COUNT(*) FROM matched_pairs_raw
        WHERE NULLIF(old_salary, '') IS NOT NULL
          AND NULLIF(new_salary, '') IS NOT NULL
          AND old_salary <> new_salary
        """,
    )

    extreme_salary = _count(
        con,
        """
        SELECT COUNT(*) FROM matched_pairs_raw
        WHERE NULLIF(old_salary, '') IS NOT NULL
          AND NULLIF(new_salary, '') IS NOT NULL
          AND ABS(
              (CAST(new_salary AS REAL) - CAST(old_salary AS REAL))
              / NULLIF(CAST(old_salary AS REAL), 0)
          ) >= 0.50
        """,
    )

    hire_date_mismatch = _count(
        con,
        """
        SELECT COUNT(*) FROM matched_pairs_raw
        WHERE NULLIF(old_hire_date, '') IS NOT NULL
          AND NULLIF(new_hire_date, '') IS NOT NULL
          AND old_hire_date <> new_hire_date
        """,
    )

    status_mismatch = _count(
        con,
        """
        SELECT COUNT(*) FROM matched_pairs_raw
        WHERE NULLIF(old_worker_status, '') IS NOT NULL
          AND NULLIF(new_worker_status, '') IS NOT NULL
          AND old_worker_status <> new_worker_status
        """,
    )

    finalized_count = 0
    identity_confidence_score = None
    if _table_exists(con, "finalized_pairs"):
        finalized_count = _count(con, "SELECT COUNT(*) FROM finalized_pairs")
        if finalized_count > 0:
            try:
                row = con.execute(
                    """
                    SELECT AVG(CAST(score AS REAL))
                    FROM finalized_pairs
                    WHERE NULLIF(score, '') IS NOT NULL
                    """
                ).fetchone()
                if row and row[0] is not None:
                    identity_confidence_score = round(float(row[0]), 4)
            except Exception:
                pass

    # --- salary data quality check ---
    salary_quality = _check_salary_quality(con)

    con.close()

    db_size_mb = round(db.stat().st_size / (1024 * 1024), 4) if db.exists() else None

    # --- audit_q0_salary_quality.csv → reports/ ---
    sq_headers = ["field_side", "bad_char_count", "blank_count", "total_rows_checked"]
    sq_path = reports_dir / "audit_q0_salary_quality.csv"
    with open(sq_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(sq_headers)
        for row in salary_quality:
            writer.writerow([row[h] for h in sq_headers])

    # --- inputs_manifest.json ---
    source_files = []
    for sf in _SOURCE_FILES:
        entry: dict = {"filename": sf.name, "path": str(sf)}
        if sf.exists():
            entry["sha256"] = _sha256(sf)
            entry["size_bytes"] = sf.stat().st_size
        else:
            entry["sha256"] = None
            entry["size_bytes"] = None
            entry["missing"] = True
        source_files.append(entry)

    # Add the actual finalized source (dynamic — selected by _pick_finalized_csv)
    if finalized_csv_path is not None:
        fin_entry: dict = {
            "filename": finalized_csv_path.name,
            "path": str(finalized_csv_path),
        }
        if finalized_csv_path.exists():
            fin_entry["sha256"] = _sha256(finalized_csv_path)
            fin_entry["size_bytes"] = finalized_csv_path.stat().st_size
        else:
            fin_entry["sha256"] = None
            fin_entry["size_bytes"] = None
            fin_entry["missing"] = True
        source_files.append(fin_entry)

    finalized_used_str = str(finalized_csv_path) if finalized_csv_path else None
    finalized_sha256 = (
        _sha256(finalized_csv_path)
        if finalized_csv_path and finalized_csv_path.exists()
        else None
    )

    manifest: dict = {
        "run_timestamp": ts,
        "matched_pairs_row_count": total_matched,
        "finalized_pairs_row_count": finalized_count,
        "finalized_source_file_used": finalized_used_str,
        "finalized_source_file_sha256": finalized_sha256,
        "sqlite_db_size_mb": db_size_mb,
        "source_files": source_files,
    }
    (run_dir / "inputs_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )

    # --- audit_summary.json ---
    sq_by_side = {r["field_side"]: r for r in salary_quality}
    summary: dict = {
        "run_timestamp": ts,
        "total_matched_pairs": total_matched,
        "salary_mismatch_count": salary_mismatch,
        "extreme_salary_count": extreme_salary,
        "hire_date_mismatch_count": hire_date_mismatch,
        "status_mismatch_count": status_mismatch,
        "salary_quality": {
            "old_bad_char_count": sq_by_side.get("old", {}).get("bad_char_count", 0),
            "old_blank_count": sq_by_side.get("old", {}).get("blank_count", 0),
            "new_bad_char_count": sq_by_side.get("new", {}).get("bad_char_count", 0),
            "new_blank_count": sq_by_side.get("new", {}).get("blank_count", 0),
            "total_rows_checked": sq_by_side.get("old", {}).get("total_rows_checked", 0),
        },
    }
    if identity_confidence_score is not None:
        summary["identity_confidence_score"] = identity_confidence_score
    (run_dir / "audit_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )

    print(f"\n[packager] run folder created: {run_dir}")
    print(f"  audit.db          {db_size_mb} MB")
    print(f"  reports/          {copied_csvs} CSV files")
    print(f"  inputs_manifest.json")
    print(f"  audit_summary.json")

    _apply_retention(run_dir)

    return run_dir
