from __future__ import annotations

import sys
from pathlib import Path

# Ensure load_sqlite is importable regardless of the working directory from
# which this script is invoked (e.g. "python audit/run_audit.py" from repo root).
sys.path.insert(0, str(Path(__file__).parent))

import sqlite3
import pandas as pd

from load_sqlite import DB_PATH, _pick_finalized_csv

import os as _os_rk
ROOT = Path(__file__).resolve().parents[1]

# Per-run isolation: write audit CSV outputs into the per-run audit/ subdir.
_rk_work  = Path(_os_rk.environ["RK_WORK_DIR"]) if "RK_WORK_DIR" in _os_rk.environ else None
AUDIT_DIR = (_rk_work / "audit") if _rk_work else (ROOT / "audit")

# Minimum columns required in a finalized CSV for it to be usable as the
# audit dataset in Q2-Q5.  If any are absent the file is treated as a
# reviewer-only artefact and the SQLite matched_pairs view is used instead.
_REQUIRES_FOR_AUDIT: frozenset[str] = frozenset(
    {
        "old_salary",
        "new_salary",
        "old_payrate",
        "new_payrate",
        "old_worker_status",
        "new_worker_status",
        "old_hire_date",
        "new_hire_date",
        "old_position",
        "new_position",
        "old_district",
        "new_district",
    }
)


def _check_finalized_schema(path: Path) -> tuple[bool, set[str]]:
    """
    Return (is_audit_ready, missing_columns) for the finalized CSV at *path*.
    Reads only the header row (no full load).
    """
    try:
        header = pd.read_csv(path, nrows=0).columns
    except Exception as exc:
        print(f"  [finalized] could not read {path.name}: {exc}")
        return False, _REQUIRES_FOR_AUDIT.copy()
    missing = _REQUIRES_FOR_AUDIT - set(header)
    return len(missing) == 0, missing


def _write(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"  wrote: {out_path}")


def main() -> None:
    con = sqlite3.connect(str(DB_PATH))

    try:
        print("\n==============================================================")
        print("  Q0: preflight duplicate checks")
        print("==============================================================")

        # --- Q0a: duplicates inside mapped tables (worker_id should be unique) ---
        q_old_dup = """
        SELECT worker_id, COUNT(*) c
        FROM mapped_old
        WHERE worker_id IS NOT NULL AND TRIM(worker_id) <> ''
        GROUP BY worker_id
        HAVING c > 1
        """
        q_new_dup = """
        SELECT worker_id, COUNT(*) c
        FROM mapped_new
        WHERE worker_id IS NOT NULL AND TRIM(worker_id) <> ''
        GROUP BY worker_id
        HAVING c > 1
        """

        old_dups = pd.read_sql_query(q_old_dup, con)
        new_dups = pd.read_sql_query(q_new_dup, con)

        print(f"  [Q0] mapped_old  worker_id duplicates : {len(old_dups)}")
        print(f"  [Q0] mapped_new  worker_id duplicates : {len(new_dups)}")

        # --- Q0b: duplicates inside matched_pairs_raw (resolve must have enforced 1-to-1) ---
        q_matched_old_dup = """
        SELECT old_worker_id, COUNT(*) c
        FROM matched_pairs_raw
        WHERE old_worker_id IS NOT NULL AND TRIM(old_worker_id) <> ''
        GROUP BY old_worker_id
        HAVING c > 1
        """
        q_matched_new_dup = """
        SELECT new_worker_id, COUNT(*) c
        FROM matched_pairs_raw
        WHERE new_worker_id IS NOT NULL AND TRIM(new_worker_id) <> ''
        GROUP BY new_worker_id
        HAVING c > 1
        """

        matched_old_dups = pd.read_sql_query(q_matched_old_dup, con)
        matched_new_dups = pd.read_sql_query(q_matched_new_dup, con)

        _write(matched_old_dups, AUDIT_DIR / "audit_q0_duplicate_old_worker_id.csv")
        _write(matched_new_dups, AUDIT_DIR / "audit_q0_duplicate_new_worker_id.csv")

        print(f"  [Q0] matched_pairs_raw old_worker_id duplicates: {len(matched_old_dups)}")
        print(f"  [Q0] matched_pairs_raw new_worker_id duplicates: {len(matched_new_dups)}")

        if len(matched_old_dups) > 0 or len(matched_new_dups) > 0:
            raise SystemExit(
                f"\n[Q0] FAIL: matched_pairs_raw has {len(matched_old_dups)} duplicate "
                f"old_worker_id and {len(matched_new_dups)} duplicate new_worker_id rows. "
                "Re-run resolve_matched_raw.py then audit/load_sqlite.py before retrying."
            )

        print("  [Q0] PASS: matched_pairs_raw is 1-to-1 on worker_id")

        print("\n==============================================================")
        print("  Q1: audit_q1_match_source_summary.csv")
        print("==============================================================")

        q1 = pd.read_sql_query(
            """
            SELECT COALESCE(NULLIF(TRIM(match_source), ''), 'unknown') AS match_source,
                   COUNT(*) AS pair_count
            FROM matched_pairs_raw
            GROUP BY COALESCE(NULLIF(TRIM(match_source), ''), 'unknown')
            ORDER BY pair_count DESC
            """,
            con,
        )
        _write(q1, AUDIT_DIR / "audit_q1_match_source_summary.csv")

        # Q2-Q5 always run against the SQLite matched_pairs view so that all
        # matched rows are included regardless of any finalized review file.
        mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)

        # Q2 pay mismatches (salary OR payrate mismatch)
        print("\n==============================================================")
        print("  Q2: audit_q2_pay_mismatches.csv")
        print("==============================================================")
        q2 = mp.copy()

        def norm_num(x):
            if x is None:
                return None
            s = str(x).strip()
            if s == "":
                return None
            try:
                return float(s)
            except Exception:
                return None

        q2["old_salary_num"] = q2["old_salary"].map(norm_num)
        q2["new_salary_num"] = q2["new_salary"].map(norm_num)
        q2["old_payrate_num"] = q2["old_payrate"].map(norm_num)
        q2["new_payrate_num"] = q2["new_payrate"].map(norm_num)

        pay_mismatch = q2[
            (
                q2["old_salary_num"].notna()
                & q2["new_salary_num"].notna()
                & (q2["old_salary_num"] != q2["new_salary_num"])
            )
            | (
                q2["old_payrate_num"].notna()
                & q2["new_payrate_num"].notna()
                & (q2["old_payrate_num"] != q2["new_payrate_num"])
            )
        ][
            [
                "pair_id",
                "match_source",
                "old_worker_id",
                "new_worker_id",
                "old_full_name_norm",
                "old_salary",
                "new_salary",
                "old_payrate",
                "new_payrate",
            ]
        ]
        _write(pay_mismatch, AUDIT_DIR / "audit_q2_pay_mismatches.csv")

        # Q3 status mismatches
        print("\n==============================================================")
        print("  Q3: audit_q3_status_mismatches.csv")
        print("==============================================================")
        status_mismatch = mp[
            (mp["old_worker_status"].fillna("").str.strip() != mp["new_worker_status"].fillna("").str.strip())
            | (mp["old_worker_type"].fillna("").str.strip() != mp["new_worker_type"].fillna("").str.strip())
        ][
            [
                "pair_id",
                "match_source",
                "old_worker_id",
                "new_worker_id",
                "old_full_name_norm",
                "old_worker_status",
                "new_worker_status",
                "old_worker_type",
                "new_worker_type",
            ]
        ]
        _write(status_mismatch, AUDIT_DIR / "audit_q3_status_mismatches.csv")

        # Q4 job/org mismatches (position/district/state)
        print("\n==============================================================")
        print("  Q4: audit_q4_job_org_mismatches.csv")
        print("==============================================================")
        job_org = mp[
            (mp["old_position"].fillna("").str.strip() != mp["new_position"].fillna("").str.strip())
            | (mp["old_district"].fillna("").str.strip() != mp["new_district"].fillna("").str.strip())
            | (mp["old_location_state"].fillna("").str.strip() != mp["new_location_state"].fillna("").str.strip())
        ][
            [
                "pair_id",
                "match_source",
                "old_worker_id",
                "new_worker_id",
                "old_full_name_norm",
                "old_position",
                "new_position",
                "old_district",
                "new_district",
                "old_location_state",
                "new_location_state",
            ]
        ]
        _write(job_org, AUDIT_DIR / "audit_q4_job_org_mismatches.csv")

        # Q5 hire date mismatches
        print("\n==============================================================")
        print("  Q5: audit_q5_hire_date_mismatches.csv")
        print("==============================================================")
        hire_mismatch = mp[
            mp["old_hire_date"].fillna("").str.strip() != mp["new_hire_date"].fillna("").str.strip()
        ][
            ["pair_id", "match_source", "old_worker_id", "new_worker_id", "old_full_name_norm", "old_hire_date", "new_hire_date"]
        ]
        _write(hire_mismatch, AUDIT_DIR / "audit_q5_hire_date_mismatches.csv")

        # -----------------------------------------------------------------------
        # Finalized CSV schema guard
        # Detect any finalized review CSV and check whether it has the columns
        # required for audit Q2-Q5.  If it does not, print a warning and skip
        # it; the SQLite matched_pairs view (used above) is the authoritative
        # source regardless.  This prevents a small reviewer-only file from
        # silently producing empty audit reports.
        # -----------------------------------------------------------------------
        finalized = _pick_finalized_csv()
        if finalized:
            audit_ready, missing = _check_finalized_schema(finalized)
            if audit_ready:
                print(
                    f"\n[finalized] {finalized.name} detected and schema-complete "
                    f"({len(_REQUIRES_FOR_AUDIT)} required columns present). "
                    "Audits Q2-Q5 use SQLite matched_pairs (all pairs); "
                    "finalized file available for downstream review tooling."
                )
            else:
                print(
                    f"\n[finalized] {finalized.name} detected but IGNORED for audits — "
                    f"missing required columns: {sorted(missing)}. "
                    "Audits Q2-Q5 already use SQLite matched_pairs (unaffected)."
                )

        print("\n[done] audit complete — results in audit/")

    finally:
        con.close()


if __name__ == "__main__":
    main()
