# audit/stress_run.py
"""Benchmark the audit SQL pipeline at 1x, 2x, 4x, 8x, 16x, 32x scale."""
from __future__ import annotations

import csv
import re
import sqlite3
import time
from pathlib import Path

import pandas as pd

_CHUNK = 5000  # rows per executemany batch

AUDIT_DIR = Path(__file__).parent
SOURCE_DB = AUDIT_DIR / "audit.db"
SQL_PATH = AUDIT_DIR / "run_audit.sql"
RESULTS_CSV = AUDIT_DIR / "stress_benchmark_results.csv"

SCALES = [
    ("1x",  None,                                     AUDIT_DIR / "audit_stress_1x.db"),
    ("2x",  AUDIT_DIR / "matched_pairs_raw_2x.csv",  AUDIT_DIR / "audit_stress_2x.db"),
    ("4x",  AUDIT_DIR / "matched_pairs_raw_4x.csv",  AUDIT_DIR / "audit_stress_4x.db"),
    ("8x",  AUDIT_DIR / "matched_pairs_raw_8x.csv",  AUDIT_DIR / "audit_stress_8x.db"),
    ("16x", AUDIT_DIR / "matched_pairs_raw_16x.csv", AUDIT_DIR / "audit_stress_16x.db"),
    ("32x", AUDIT_DIR / "matched_pairs_raw_32x.csv", AUDIT_DIR / "audit_stress_32x.db"),
]


def _split_statements(sql: str) -> list[str]:
    stripped = re.sub(r"--[^\n]*", "", sql)
    parts = [s.strip() for s in stripped.split(";")]
    return [s for s in parts if s]


_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_matched_old_worker   ON matched_pairs_raw(old_worker_id)",
    "CREATE INDEX IF NOT EXISTS idx_matched_new_worker   ON matched_pairs_raw(new_worker_id)",
    "CREATE INDEX IF NOT EXISTS idx_matched_match_source ON matched_pairs_raw(match_source)",
]


def _create_indexes(con: sqlite3.Connection) -> None:
    for ddl in _INDEXES:
        con.execute(ddl)
    con.commit()
    print("  Indexes verified/created successfully.")


def _load_csv_to_sqlite(csv_path: Path, con: sqlite3.Connection) -> int:
    """Stream a CSV directly into SQLite without holding the full DataFrame in RAM."""
    rows_loaded = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        cols = ", ".join(f'"{h}"' for h in headers)
        placeholders = ", ".join("?" * len(headers))
        con.execute(f"DROP TABLE IF EXISTS matched_pairs_raw")
        con.execute(f"CREATE TABLE matched_pairs_raw ({cols})")
        batch: list[tuple] = []
        for row in reader:
            batch.append(tuple(row))
            if len(batch) >= _CHUNK:
                con.executemany(
                    f"INSERT INTO matched_pairs_raw VALUES ({placeholders})", batch
                )
                rows_loaded += len(batch)
                batch = []
        if batch:
            con.executemany(
                f"INSERT INTO matched_pairs_raw VALUES ({placeholders})", batch
            )
            rows_loaded += len(batch)
    con.commit()
    return rows_loaded


def run_scale(
    scale: str,
    csv_path: Path | None,
    db_path: Path,
    statements: list[str],
) -> dict:
    t0 = time.perf_counter()

    db_path.unlink(missing_ok=True)
    con = sqlite3.connect(db_path)

    if csv_path is None:
        # 1x: copy from existing audit.db via pandas (small enough to be safe)
        src = sqlite3.connect(SOURCE_DB)
        df = pd.read_sql("SELECT * FROM matched_pairs_raw", src)
        src.close()
        for col in df.columns:
            df[col] = df[col].where(df[col].notna(), "")
        df.to_sql("matched_pairs_raw", con, index=False, if_exists="replace", chunksize=_CHUNK)
        rows_loaded = len(df)
        del df
    else:
        rows_loaded = _load_csv_to_sqlite(csv_path, con)

    _create_indexes(con)
    load_seconds = round(time.perf_counter() - t0, 3)
    rows_per_second_loaded = round(rows_loaded / load_seconds) if load_seconds > 0 else 0

    result: dict = {
        "scale": scale,
        "rows_loaded": rows_loaded,
        "load_seconds": load_seconds,
        "rows_per_second_loaded": rows_per_second_loaded,
    }

    for i, stmt in enumerate(statements, start=1):
        t1 = time.perf_counter()
        cur = con.execute(stmt)
        rows = cur.fetchall()
        result[f"Q{i}_seconds"] = round(time.perf_counter() - t1, 3)
        result[f"Q{i}_rows"] = len(rows)

    con.close()

    # Worst query: highest Q{i}_seconds
    worst_q, worst_s = max(
        ((f"Q{i}", result[f"Q{i}_seconds"]) for i in range(1, len(statements) + 1)),
        key=lambda x: x[1],
    )
    result["worst_query_at_scale"] = worst_q
    result["worst_query_seconds"] = worst_s

    return result


def _print_summary(all_results: list[dict], num_q: int) -> None:
    col_w = 10
    headers = (
        ["scale", "rows", "load_s", "rows/s"]
        + [f"Q{i}_s" for i in range(1, num_q + 1)]
        + ["worst_Q", "worst_s"]
    )
    sep = "  "
    print("\n" + "=" * 62)
    print("  SUMMARY")
    print("=" * 62)
    print("  " + sep.join(h.ljust(col_w) for h in headers))
    print("  " + "-" * (col_w * len(headers) + len(sep) * len(headers)))
    for r in all_results:
        vals = [
            r["scale"],
            str(r["rows_loaded"]),
            str(r["load_seconds"]),
            str(r["rows_per_second_loaded"]),
        ]
        vals += [str(r[f"Q{i}_seconds"]) for i in range(1, num_q + 1)]
        vals += [r["worst_query_at_scale"], str(r["worst_query_seconds"])]
        print("  " + sep.join(v.ljust(col_w) for v in vals))


def main() -> None:
    if not SQL_PATH.exists():
        raise FileNotFoundError(f"SQL file not found: {SQL_PATH}")
    if not SOURCE_DB.exists():
        raise FileNotFoundError(
            f"Source database not found: {SOURCE_DB}\n"
            "Run  python audit/load_sqlite.py  first."
        )

    sql_text = SQL_PATH.read_text(encoding="utf-8")
    statements = _split_statements(sql_text)
    num_q = len(statements)

    print(f"Queries: {num_q}  |  Scales: {len(SCALES)}")

    all_results: list[dict] = []

    for scale, csv_path, db_path in SCALES:
        src_label = csv_path.name if csv_path else f"{SOURCE_DB.name} (direct)"
        print(f"\n[{scale}] source: {src_label}")

        result = run_scale(scale, csv_path, db_path, statements)
        all_results.append(result)

        print(f"  rows_loaded          : {result['rows_loaded']}")
        print(f"  load_seconds         : {result['load_seconds']}")
        print(f"  rows_per_second      : {result['rows_per_second_loaded']}")
        for i in range(1, num_q + 1):
            print(f"  Q{i:>2}: {result[f'Q{i}_seconds']:>6}s  ({result[f'Q{i}_rows']} rows)")
        print(f"  worst_query          : {result['worst_query_at_scale']}  ({result['worst_query_seconds']}s)")

    fieldnames = list(all_results[0].keys())
    with open(RESULTS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    _print_summary(all_results, num_q)
    print(f"\n[done] results written to {RESULTS_CSV}")


if __name__ == "__main__":
    main()
