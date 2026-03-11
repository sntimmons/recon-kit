"""
audit/schema_validator.py - Validate matched_pairs view schema before gating.

Exits with code 2 and a clear error if any required column is missing.
Exits with code 0 when all required columns are present.

Run:
    venv/Scripts/python.exe audit/schema_validator.py [--db PATH]

Import:
    from audit.schema_validator import validate_schema
    validate_schema(db_path)          # raises SystemExit(2) on failure
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT    = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "audit" / "audit.db"

# Columns that every downstream step depends on.
# confidence is required because gating.classify_row reads it directly.
REQUIRED_COLS: list[str] = [
    "pair_id",
    "match_source",
    "confidence",
    "old_worker_id",
    "new_worker_id",
    "old_salary",
    "new_salary",
    "old_worker_status",
    "new_worker_status",
    "old_hire_date",
    "new_hire_date",
]


def validate_schema(db_path: Path) -> None:
    """
    Check matched_pairs view has all required columns.

    Prints a summary and exits with code 2 if any are missing.
    Exits normally (returns) if all required columns are present.
    """
    if not db_path.exists():
        print(f"[schema] ERROR: database not found: {db_path}", file=sys.stderr)
        sys.exit(2)

    try:
        con = sqlite3.connect(str(db_path))
        try:
            cur = con.execute("SELECT * FROM matched_pairs LIMIT 0")
            actual_cols = {desc[0] for desc in cur.description}
        except sqlite3.OperationalError as exc:
            print(f"[schema] ERROR: cannot query matched_pairs: {exc}", file=sys.stderr)
            sys.exit(2)
        finally:
            con.close()
    except Exception as exc:
        print(f"[schema] ERROR: cannot open database: {exc}", file=sys.stderr)
        sys.exit(2)

    missing = [c for c in REQUIRED_COLS if c not in actual_cols]
    if missing:
        print(
            f"[schema] ERROR: matched_pairs is missing required columns: {missing}",
            file=sys.stderr,
        )
        print(
            "[schema] Run the full pipeline (mapping → matcher → resolve → load_sqlite) "
            "to regenerate the database with all required columns.",
            file=sys.stderr,
        )
        sys.exit(2)

    print(
        f"[schema] OK: matched_pairs has all {len(REQUIRED_COLS)} required columns "
        f"({len(actual_cols)} total)."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate matched_pairs view schema before pipeline gating steps."
    )
    parser.add_argument(
        "--db", default=None, metavar="PATH",
        help=f"SQLite database path (default: {DB_PATH}).",
    )
    args = parser.parse_args()
    db_path = Path(args.db) if args.db else DB_PATH
    validate_schema(db_path)


if __name__ == "__main__":
    main()
