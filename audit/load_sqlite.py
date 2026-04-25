from __future__ import annotations

import csv
import os
import sqlite3
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]

# Per-run isolation: when RK_WORK_DIR is set by api_server.py, read/write
# all I/O within that run-specific directory instead of global repo paths.
_rk_work    = Path(os.environ["RK_WORK_DIR"]) if "RK_WORK_DIR" in os.environ else None

DB_PATH     = (_rk_work / "audit" / "audit.db")         if _rk_work else (ROOT / "audit" / "audit.db")
SCHEMA_PATH = ROOT / "audit" / "schema.sql"             # schema is static - always from repo
MAPPED_OLD  = (_rk_work / "outputs" / "mapped_old.csv")  if _rk_work else (ROOT / "outputs" / "mapped_old.csv")
MAPPED_NEW  = (_rk_work / "outputs" / "mapped_new.csv")  if _rk_work else (ROOT / "outputs" / "mapped_new.csv")
MATCHED_RAW = (_rk_work / "outputs" / "matched_raw.csv") if _rk_work else (ROOT / "outputs" / "matched_raw.csv")

# If you ever generate a curated/finalized file, we'll prefer it.
FINALIZED_CANDIDATES_DEFAULT = ROOT / "outputs" / "finalized_matches_candidates.csv"


def _pick_finalized_csv() -> Optional[Path]:
    """
    Backwards-compatible helper expected by run_audit.py.

    Returns a path to a "finalized matches" CSV if one exists, otherwise None.
    """
    candidates = [
        FINALIZED_CANDIDATES_DEFAULT,
        ROOT / "audit" / "finalized_matches_candidates.csv",
        ROOT / "finalized_matches_candidates.csv",
    ]
    for p in candidates:
        if p.exists() and p.stat().st_size > 0:
            return p
    return None


# PII columns to strip from matched_pairs_raw when loading into SQLite.
# matcher.py already omits these when writing matched_raw.csv (confidence is
# computed before the file is written), so this is a defensive fallback for
# any pre-existing CSVs that still contain the columns.
_STRIP_COLS: frozenset[str] = frozenset({"old_last4_ssn", "new_last4_ssn"})


def _safe_col(name: str) -> str:
    """Sanitize a column name for safe use as a quoted SQL identifier.

    Strips characters that would break a double-quoted SQL identifier
    (double-quotes, semicolons, newlines).  Whitespace is stripped from ends.
    """
    return (
        str(name)
        .replace('"', "")
        .replace(";", "")
        .replace("\n", "")
        .replace("\r", "")
        .strip()
    )


def _read_csv_headers(path: Path) -> list[str]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        return next(reader)


def _ensure_schema(con: sqlite3.Connection) -> None:
    if SCHEMA_PATH.exists():
        con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    else:
        # Minimal schema fallback (keeps pipeline running even if schema.sql is missing)
        con.executescript(
            """
            DROP TABLE IF EXISTS matched_pairs_raw;
            DROP TABLE IF EXISTS mapped_old;
            DROP TABLE IF EXISTS mapped_new;

            CREATE TABLE matched_pairs_raw (
              pair_id TEXT PRIMARY KEY,
              match_source TEXT,
              old_worker_id TEXT,
              new_worker_id TEXT
            );

            CREATE TABLE mapped_old (
              worker_id TEXT,
              recon_id TEXT,
              full_name_norm TEXT,
              dob TEXT,
              last4_ssn TEXT,
              hire_date TEXT,
              worker_status TEXT,
              worker_type TEXT,
              position TEXT,
              district TEXT,
              location_state TEXT,
              salary TEXT,
              payrate TEXT
            );

            CREATE TABLE mapped_new (
              worker_id TEXT,
              recon_id TEXT,
              full_name_norm TEXT,
              dob TEXT,
              last4_ssn TEXT,
              hire_date TEXT,
              worker_status TEXT,
              worker_type TEXT,
              position TEXT,
              district TEXT,
              location_state TEXT,
              salary TEXT,
              payrate TEXT
            );
            """
        )


def _load_csv_to_table(con: sqlite3.Connection, csv_path: Path, table: str) -> None:
    raw_headers = _read_csv_headers(csv_path)

    # Sanitize header names for safe use as SQL identifiers.
    # For matched_pairs_raw, also strip PII columns that are no longer needed
    # after matching (last4_ssn served its purpose in matcher.py).
    if table == "matched_pairs_raw":
        headers = [_safe_col(h) for h in raw_headers if h not in _STRIP_COLS]
        _stripped = [h for h in raw_headers if h in _STRIP_COLS]
        if _stripped:
            print(f"[load] stripped PII columns from {table}: {_stripped}")
    else:
        headers = [_safe_col(h) for h in raw_headers]

    con.execute(f"DROP TABLE IF EXISTS {table};")
    cols_sql = ", ".join([f'"{h}" TEXT' for h in headers])
    con.execute(f'CREATE TABLE {table} ({cols_sql});')

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Use raw (unsanitized) headers to read values from CSV, but map to
        # sanitized header names for INSERT.  Rows skip stripped PII columns.
        orig_to_safe = {
            raw: safe
            for raw, safe in zip(raw_headers, [_safe_col(h) for h in raw_headers])
            if safe in headers
        }
        rows = [
            tuple((r.get(raw, None) or "").strip() for raw in orig_to_safe)
            for r in reader
        ]

    if rows:
        placeholders = ", ".join(["?"] * len(headers))
        col_list = ", ".join([f'"{h}"' for h in headers])
        con.executemany(
            f'INSERT INTO {table} ({col_list}) VALUES ({placeholders});',
            rows,
        )


def _create_indexes(con: sqlite3.Connection) -> None:
    """
    Recreate the three working indexes on matched_pairs_raw.

    _load_csv_to_table drops and recreates matched_pairs_raw, which also
    drops any indexes that schema.sql created. We rebuild them here, after
    the table is populated, so they actually exist at query time.

    Indexes are only created when the target column is actually present in the
    table.  When the matcher finds zero matches it writes a minimal CSV
    (match_source + confidence only), so the worker-id columns are absent and
    attempting to index them would raise OperationalError.
    """
    existing_cols: set[str] = {
        row[1]
        for row in con.execute("PRAGMA table_info(matched_pairs_raw)").fetchall()
    }
    candidates = [
        ("idx_matched_old_worker",    "old_worker_id"),
        ("idx_matched_new_worker",    "new_worker_id"),
        ("idx_matched_match_source",  "match_source"),
    ]
    for idx_name, col_name in candidates:
        if col_name in existing_cols:
            con.execute(
                f"CREATE INDEX IF NOT EXISTS {idx_name} "
                f"ON matched_pairs_raw({col_name});"
            )


def _create_views(con: sqlite3.Connection) -> None:
    """
    Create the matched_pairs view.

    Requirements:
    - Always expose a usable pair_id column to audits.
    - If matched_pairs_raw already has pair_id (post-resolve), use it.
    - If not (debug / pre-resolve), synthesize pair_id from rowid.
    - Never create a duplicate pair_id column in the view.
    - Include ALL columns from matched_pairs_raw (future-proof).
    """
    col_names = [
        row[1]
        for row in con.execute("PRAGMA table_info(matched_pairs_raw)").fetchall()
    ]
    has_pair_id = "pair_id" in col_names

    # Build a column list that includes everything, without duplicating pair_id.
    # Quote all column names to prevent SQL injection via unusual identifiers.
    other_cols = [c for c in col_names if c != "pair_id"]
    other_cols_sql = ",\n          ".join([f'"{c}"' for c in other_cols]) if other_cols else ""

    con.execute("DROP VIEW IF EXISTS matched_pairs;")

    if has_pair_id:
        # pair_id exists in table: select it first, then everything else (excluding pair_id).
        select_sql = f"""
        CREATE VIEW matched_pairs AS
        SELECT
          pair_id
          {"," if other_cols_sql else ""}
          {other_cols_sql}
        FROM matched_pairs_raw;
        """
        src = "table column"
    else:
        # pair_id missing: synthesize from rowid, then include all columns.
        select_sql = f"""
        CREATE VIEW matched_pairs AS
        SELECT
          CAST(rowid AS TEXT) AS pair_id
          {"," if other_cols_sql else ""}
          {other_cols_sql}
        FROM matched_pairs_raw;
        """
        src = "rowid fallback (resolve not yet run)"

    con.execute(select_sql)
    print(f"[load] matched_pairs view: pair_id from {src}")


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(str(DB_PATH))
    try:
        _ensure_schema(con)

        # Load mapped files + matched_raw
        if not MAPPED_OLD.exists():
            raise FileNotFoundError(f"Missing {MAPPED_OLD}")
        if not MAPPED_NEW.exists():
            raise FileNotFoundError(f"Missing {MAPPED_NEW}")
        if not MATCHED_RAW.exists():
            raise FileNotFoundError(f"Missing {MATCHED_RAW}")

        _load_csv_to_table(con, MAPPED_OLD, "mapped_old")
        _load_csv_to_table(con, MAPPED_NEW, "mapped_new")
        _load_csv_to_table(con, MATCHED_RAW, "matched_pairs_raw")

        # Indexes must be created AFTER _load_csv_to_table because that function
        # drops and recreates the table, which also drops schema.sql's indexes.
        _create_indexes(con)

        _create_views(con)

        con.commit()
        print(
            f"[load] schema applied: {SCHEMA_PATH.name if SCHEMA_PATH.exists() else '(fallback schema)'}"
        )
        print(
            "[load] matched_pairs_raw : "
            f"{con.execute('select count(*) from matched_pairs_raw').fetchone()[0]} rows inserted"
        )
        print(f"[load] database          : {DB_PATH}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
