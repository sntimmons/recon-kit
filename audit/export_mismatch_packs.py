# audit/export_mismatch_packs.py
"""Export normalized mismatch CSVs for non-technical review."""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

AUDIT_DIR = Path(__file__).parent
DB_PATH = AUDIT_DIR / "audit.db"

_HEADERS = [
    "old_worker_id",
    "new_worker_id",
    "full_name_old",
    "full_name_new",
    "match_source",
    "match_tier",
    "field_name",
    "old_value",
    "new_value",
    "delta_amount",
    "delta_pct",
]

_TIER_CASE = """\
    CASE match_source
        WHEN 'worker_id'              THEN 'Tier 1 - Worker ID'
        WHEN 'last4_dob'              THEN 'Tier 2 - Last4+DOB'
        WHEN 'last4_birthyear_lname3' THEN 'Tier 3 - Last4+BirthYear+LName3'
        WHEN 'fallback'               THEN 'Tier 4 - Fallback'
        ELSE                               'Unknown'
    END"""

_PACKS: list[tuple[str, str]] = [
    (
        "mismatch_pay.csv",
        f"""
        SELECT
            old_worker_id,
            new_worker_id,
            old_full_name_norm          AS full_name_old,
            new_full_name_norm          AS full_name_new,
            match_source,
            {_TIER_CASE}                AS match_tier,
            'salary'                    AS field_name,
            old_salary                  AS old_value,
            new_salary                  AS new_value,
            ROUND(
                CAST(new_salary AS REAL) - CAST(old_salary AS REAL), 2
            )                           AS delta_amount,
            ROUND(
                (CAST(new_salary AS REAL) - CAST(old_salary AS REAL))
                / NULLIF(CAST(old_salary AS REAL), 0) * 100, 2
            )                           AS delta_pct
        FROM matched_pairs_raw
        WHERE NULLIF(old_salary, '') IS NOT NULL
          AND NULLIF(new_salary, '') IS NOT NULL
          AND old_salary <> new_salary
        ORDER BY ABS(CAST(new_salary AS REAL) - CAST(old_salary AS REAL)) DESC
        """,
    ),
    (
        "mismatch_hire_date.csv",
        f"""
        SELECT
            old_worker_id,
            new_worker_id,
            old_full_name_norm          AS full_name_old,
            new_full_name_norm          AS full_name_new,
            match_source,
            {_TIER_CASE}                AS match_tier,
            'hire_date'                 AS field_name,
            old_hire_date               AS old_value,
            new_hire_date               AS new_value,
            NULL                        AS delta_amount,
            NULL                        AS delta_pct
        FROM matched_pairs_raw
        WHERE NULLIF(old_hire_date, '') IS NOT NULL
          AND NULLIF(new_hire_date, '') IS NOT NULL
          AND old_hire_date <> new_hire_date
        ORDER BY old_worker_id
        """,
    ),
    (
        "mismatch_status.csv",
        f"""
        SELECT
            old_worker_id,
            new_worker_id,
            old_full_name_norm          AS full_name_old,
            new_full_name_norm          AS full_name_new,
            match_source,
            {_TIER_CASE}                AS match_tier,
            'worker_status'             AS field_name,
            old_worker_status           AS old_value,
            new_worker_status           AS new_value,
            NULL                        AS delta_amount,
            NULL                        AS delta_pct
        FROM matched_pairs_raw
        WHERE NULLIF(old_worker_status, '') IS NOT NULL
          AND NULLIF(new_worker_status, '') IS NOT NULL
          AND old_worker_status <> new_worker_status
        ORDER BY old_worker_id
        """,
    ),
    (
        "mismatch_position.csv",
        f"""
        SELECT
            old_worker_id,
            new_worker_id,
            old_full_name_norm          AS full_name_old,
            new_full_name_norm          AS full_name_new,
            match_source,
            {_TIER_CASE}                AS match_tier,
            'position'                  AS field_name,
            old_position                AS old_value,
            new_position                AS new_value,
            NULL                        AS delta_amount,
            NULL                        AS delta_pct
        FROM matched_pairs_raw
        WHERE NULLIF(old_position, '') IS NOT NULL
          AND NULLIF(new_position, '') IS NOT NULL
          AND old_position <> new_position
        ORDER BY old_worker_id
        """,
    ),
    (
        "mismatch_salary_extreme.csv",
        f"""
        SELECT
            old_worker_id,
            new_worker_id,
            old_full_name_norm          AS full_name_old,
            new_full_name_norm          AS full_name_new,
            match_source,
            {_TIER_CASE}                AS match_tier,
            'salary'                    AS field_name,
            old_salary                  AS old_value,
            new_salary                  AS new_value,
            ROUND(
                CAST(new_salary AS REAL) - CAST(old_salary AS REAL), 2
            )                           AS delta_amount,
            ROUND(
                (CAST(new_salary AS REAL) - CAST(old_salary AS REAL))
                / NULLIF(CAST(old_salary AS REAL), 0) * 100, 2
            )                           AS delta_pct
        FROM matched_pairs_raw
        WHERE NULLIF(old_salary, '') IS NOT NULL
          AND NULLIF(new_salary, '') IS NOT NULL
          AND ABS(
              (CAST(new_salary AS REAL) - CAST(old_salary AS REAL))
              / NULLIF(CAST(old_salary AS REAL), 0)
          ) >= 0.50
        ORDER BY ABS(
            (CAST(new_salary AS REAL) - CAST(old_salary AS REAL))
            / NULLIF(CAST(old_salary AS REAL), 0)
        ) DESC
        """,
    ),
]


def export_all(reports_dir: Path, db_path: Path = DB_PATH) -> None:
    """Write all five mismatch pack CSVs into reports_dir."""
    reports_dir.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    for filename, sql in _PACKS:
        rows = con.execute(sql).fetchall()
        out_path = reports_dir / filename
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(_HEADERS)
            for row in rows:
                writer.writerow([row[h] for h in _HEADERS])
        print(f"  {filename:<40} {len(rows):>6} rows")

    con.close()


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {DB_PATH}\n"
            "Run  venv/Scripts/python.exe audit/load_sqlite.py  first."
        )

    # Prefer the latest run folder's reports/ when run standalone.
    runs_dir = AUDIT_DIR / "audit_runs"
    reports_dir: Path | None = None
    if runs_dir.exists():
        runs = sorted(
            [d for d in runs_dir.iterdir() if d.is_dir() and d.name.startswith("run_")],
            key=lambda d: d.name,
            reverse=True,
        )
        if runs:
            reports_dir = runs[0] / "reports"

    if reports_dir is None:
        reports_dir = AUDIT_DIR

    print(f"\n[mismatch-packs] writing to {reports_dir}")
    export_all(reports_dir)
    print("[mismatch-packs] done")


if __name__ == "__main__":
    main()
