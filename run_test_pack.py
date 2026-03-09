# run_test_pack.py
"""Phase 3 test harness.

For each pack directory under test_packs/:
  1. map_file (old + new)
  2. match_files  → matched_raw.csv + unmatched_*_raw.csv
  3. build_review_candidates → review_candidates.csv
  4. finalize (only if review_candidates.csv has at least one non-blank decision value)
  5. resolve_matched_raw.resolve() → 1-to-1 matched_raw.csv
  6. Copy matched_raw.csv → outputs/matched_raw.csv (main project location)
  7. Subprocess: audit/load_sqlite.py
  8. Subprocess: audit/run_audit.py  (creates a run_<ts>/ folder)
  9. Rename that run folder → pack_<packname>_<ts>/
 10. Collect metrics → phase3_scorecard.csv

The main outputs/matched_raw.csv and audit/audit.db are mutated during each pack run
and restored to the pre-test state at the end.

Usage
-----
  venv/Scripts/python.exe run_test_pack.py              # all packs
  venv/Scripts/python.exe run_test_pack.py accent        # single pack by name
  venv/Scripts/python.exe run_test_pack.py accent suffix  # multiple
"""
from __future__ import annotations

import csv
import difflib
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO       = Path(__file__).parent
PACKS_DIR  = REPO / "test_packs"
OUTPUTS    = REPO / "outputs"
AUDIT_DIR  = REPO / "audit"
RUNS_DIR   = AUDIT_DIR / "audit_runs"
ALIASES    = str(REPO / "config" / "column_aliases.yml")
VENV_PY    = sys.executable

# Backup path for the real matched_raw.csv (restored at the end of the run).
_BACKUP = OUTPUTS / "_matched_raw_backup.csv"

# ---------------------------------------------------------------------------
# Python path: expose both REPO (for src.*) and REPO/src (for bare imports)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

from mapping               import map_file                        # noqa: E402
from matcher               import match_files                     # noqa: E402
from build_review_candidates import build_review_candidates       # noqa: E402
from finalize              import finalize                        # noqa: E402
import pandas as pd                                               # noqa: E402
from resolve_matched_raw   import resolve                         # noqa: E402

# ---------------------------------------------------------------------------
# Scorecard schema
# ---------------------------------------------------------------------------
SCORECARD_COLS = [
    "pack_name",
    "old_rows",
    "new_rows",
    "tier1_worker_id",
    "tier2_last4_dob",
    "tier3_last4_year_lname3",
    "tier4_fallback",
    "dup_wid_old_excluded",
    "dup_wid_new_excluded",
    "matched_raw_before_resolve",
    "resolved_pairs",
    "conflicts_dropped",
    "q0_dup_old",
    "q0_dup_new",
    "review_candidates_rows",
    "finalized_rows",
    "suspicious_match_count",   # Tier-2 pairs where last4+dob agree but names diverge
    "mismatch_pay",
    "mismatch_hire_date",
    "mismatch_status",
    "mismatch_position",
    "mismatch_salary_extreme",
    "wall_secs",                # end-to-end wall-clock time for the full pack run
    "run_folder",
    "status",
]

# ---------------------------------------------------------------------------
# Suspicious-match detection
# ---------------------------------------------------------------------------
_NAME_SIM_THRESHOLD = 0.60   # pairs below this are flagged


def _name_sim(a: str, b: str) -> float:
    a = (a or "").strip().lower()
    b = (b or "").strip().lower()
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def _detect_suspicious_matches(matched_raw: Path, out_dir: Path) -> int:
    """Scan matched_raw.csv for pairs where identity keys match but names diverge.

    A pair is suspicious when:
      - last4_ssn_old == last4_ssn_new  (same SSN4)
      - dob_old == dob_new              (same DOB)
      - name_similarity(full_name_norm_old, full_name_norm_new) < threshold

    Writes suspicious pairs to out_dir/suspicious_matches.csv.
    Returns count of suspicious pairs found.
    """
    if not matched_raw.exists():
        return 0

    try:
        df = pd.read_csv(matched_raw, dtype=str).fillna("")
    except Exception:
        return 0

    required = {"last4_ssn_old", "last4_ssn_new", "dob_old", "dob_new",
                "full_name_norm_old", "full_name_norm_new"}
    if not required.issubset(df.columns):
        return 0

    # Only check rows where both last4+dob are non-blank
    mask = (
        (df["last4_ssn_old"] != "") & (df["last4_ssn_new"] != "")
        & (df["dob_old"] != "") & (df["dob_new"] != "")
        & (df["last4_ssn_old"] == df["last4_ssn_new"])
        & (df["dob_old"] == df["dob_new"])
    )
    candidates = df[mask].copy()
    if candidates.empty:
        return 0

    candidates["_name_sim"] = candidates.apply(
        lambda r: _name_sim(r["full_name_norm_old"], r["full_name_norm_new"]), axis=1
    )
    suspicious = candidates[candidates["_name_sim"] < _NAME_SIM_THRESHOLD].copy()

    if suspicious.empty:
        return 0

    out_path = out_dir / "suspicious_matches.csv"
    display_cols = [c for c in [
        "full_name_norm_old", "full_name_norm_new", "_name_sim",
        "last4_ssn_old", "dob_old", "worker_id",
    ] if c in suspicious.columns]
    suspicious[display_cols].rename(columns={"_name_sim": "name_similarity"}).to_csv(
        out_path, index=False
    )
    print(f"  [suspicious] {len(suspicious)} pair(s) -> {out_path.name}")
    return len(suspicious)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _empty_row(pack_name: str) -> dict:
    return {k: "" for k in SCORECARD_COLS} | {"pack_name": pack_name, "status": "FAIL"}


def _runs_before() -> set[str]:
    if not RUNS_DIR.exists():
        return set()
    return {d.name for d in RUNS_DIR.iterdir() if d.is_dir() and d.name.startswith("run_")}


def _newest_new_run(before: set[str]) -> str | None:
    after = _runs_before()
    new = after - before
    return sorted(new)[-1] if new else None


def _count_csv(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return len(pd.read_csv(path, dtype=str))
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Per-pack pipeline
# ---------------------------------------------------------------------------

def run_pack(pack_dir: Path) -> dict:
    _t0  = time.time()
    name = pack_dir.name
    print(f"\n{'=' * 62}")
    print(f"  PACK: {name}")
    print(f"{'=' * 62}")

    out = pack_dir / "outputs"
    out.mkdir(exist_ok=True)

    row = _empty_row(name)

    try:
        # ── 1. Map ──────────────────────────────────────────────────────────
        map_file(str(pack_dir / "old.csv"), str(out / "mapped_old.csv"), "old", aliases_path=ALIASES)
        map_file(str(pack_dir / "new.csv"), str(out / "mapped_new.csv"), "new", aliases_path=ALIASES)

        # ── 2. Match ─────────────────────────────────────────────────────────
        report = match_files(
            old_csv              = str(out / "mapped_old.csv"),
            new_csv              = str(out / "mapped_new.csv"),
            out_dir              = str(out),
            matched_name         = "matched_raw.csv",
            unmatched_new_name   = "unmatched_new_raw.csv",
            unmatched_old_name   = "unmatched_old_raw.csv",
            report_name          = "match_report_raw.json",
        )
        row["old_rows"]                  = report.old_rows
        row["new_rows"]                  = report.new_rows
        row["tier1_worker_id"]           = report.matched_by_worker_id
        row["tier2_last4_dob"]           = report.matched_by_last4_dob
        row["tier3_last4_year_lname3"]   = report.matched_by_last4_birthyear_lname3
        row["tier4_fallback"]            = report.matched_by_fallback
        row["dup_wid_old_excluded"]      = report.duplicate_worker_ids_old_excluded
        row["dup_wid_new_excluded"]      = report.duplicate_worker_ids_new_excluded
        row["matched_raw_before_resolve"]= report.matched_rows

        # ── 3. Build review candidates ───────────────────────────────────────
        rev_csv = out / "review_candidates.csv"
        build_review_candidates(
            old_csv = str(out / "unmatched_old_raw.csv"),
            new_csv = str(out / "unmatched_new_raw.csv"),
            out_csv = str(rev_csv),
        )
        row["review_candidates_rows"] = _count_csv(rev_csv)

        # ── 4. Finalize (only if a decision value exists) ────────────────────
        finalized_rows = 0
        if rev_csv.exists():
            rev_df = pd.read_csv(rev_csv, dtype=str).fillna("")
            has_decisions = (
                "decision" in rev_df.columns
                and rev_df["decision"].str.strip().ne("").any()
            )
            if has_decisions:
                fin_csv = out / "finalized_matches.csv"
                finalize(
                    review_csv       = str(rev_csv),
                    out_matches_csv  = str(fin_csv),
                    out_report_json  = str(out / "finalized_report.json"),
                    out_ambiguous_csv= str(out / "ambiguous_identity_groups.csv"),
                )
                finalized_rows = _count_csv(fin_csv)
        row["finalized_rows"] = finalized_rows

        # ── 5. Resolve matched_raw ───────────────────────────────────────────
        matched_raw = out / "matched_raw.csv"
        winners = resolve(input_path=matched_raw, output_path=matched_raw)
        row["resolved_pairs"]    = len(winners)
        row["conflicts_dropped"] = int(row["matched_raw_before_resolve"]) - len(winners)

        # ── 5b. Suspicious-match detection (runs on resolved matched_raw) ────
        row["suspicious_match_count"] = _detect_suspicious_matches(matched_raw, out)

        # ── 6. Swap into main outputs/ for audit pipeline ───────────────────
        shutil.copy2(matched_raw, OUTPUTS / "matched_raw.csv")

        # ── 7 + 8. load_sqlite → run_audit (subprocesses) ───────────────────
        runs_snap = _runs_before()

        res_load = subprocess.run(
            [str(VENV_PY), "audit/load_sqlite.py"],
            cwd=str(REPO), capture_output=True, text=True,
        )
        if res_load.returncode != 0:
            raise RuntimeError(f"load_sqlite failed:\n{res_load.stderr[-600:]}")

        res_audit = subprocess.run(
            [str(VENV_PY), "audit/run_audit.py"],
            cwd=str(REPO), capture_output=True, text=True,
        )
        audit_out = res_audit.stdout + res_audit.stderr

        # ── 9. Rename run folder ─────────────────────────────────────────────
        raw_run = _newest_new_run(runs_snap)
        if raw_run:
            ts_part  = raw_run[4:]                               # strip leading "run_"
            new_name = f"pack_{name}_{ts_part}"
            (RUNS_DIR / raw_run).rename(RUNS_DIR / new_name)
            row["run_folder"] = new_name
            run_path = RUNS_DIR / new_name
        else:
            run_path = None

        # ── 10. Collect audit metrics ────────────────────────────────────────
        if run_path:
            q0_old_path = run_path / "reports" / "audit_q0_duplicate_old_worker_id.csv"
            q0_new_path = run_path / "reports" / "audit_q0_duplicate_new_worker_id.csv"
            row["q0_dup_old"] = _count_csv(q0_old_path)
            row["q0_dup_new"] = _count_csv(q0_new_path)

            for key, fname in [
                ("mismatch_pay",            "mismatch_pay.csv"),
                ("mismatch_hire_date",      "mismatch_hire_date.csv"),
                ("mismatch_status",         "mismatch_status.csv"),
                ("mismatch_position",       "mismatch_position.csv"),
                ("mismatch_salary_extreme", "mismatch_salary_extreme.csv"),
            ]:
                row[key] = _count_csv(run_path / "reports" / fname)

        # Determine final status
        q0_fail = (int(row.get("q0_dup_old") or 0) + int(row.get("q0_dup_new") or 0)) > 0
        audit_fail = res_audit.returncode != 0 and "Q0" in audit_out
        row["status"] = "Q0_FAIL" if (q0_fail or audit_fail) else "PASS"

    except Exception as exc:
        row["status"] = f"ERROR: {exc}"
        print(f"  [ERROR] {exc}")

    row["wall_secs"] = round(time.time() - _t0, 1)
    _print_row_summary(row)
    return row


def _print_row_summary(row: dict) -> None:
    print(
        f"  status={row['status']}  "
        f"matched={row.get('resolved_pairs','')}  "
        f"t1={row.get('tier1_worker_id','')}  "
        f"t2={row.get('tier2_last4_dob','')}  "
        f"t4={row.get('tier4_fallback','')}  "
        f"q0_old={row.get('q0_dup_old','')}  "
        f"q0_new={row.get('q0_dup_new','')}  "
        f"suspicious={row.get('suspicious_match_count','')}  "
        f"pay_mm={row.get('mismatch_pay','')}  "
        f"secs={row.get('wall_secs','')}"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not PACKS_DIR.exists():
        raise RuntimeError(
            f"test_packs/ not found. Run:  venv/Scripts/python.exe generate_test_packs.py"
        )

    # Filter to requested packs (or run all)
    requested = set(sys.argv[1:])
    all_packs = sorted(
        d for d in PACKS_DIR.iterdir()
        if d.is_dir()
        and (d / "old.csv").exists()
        and (d / "new.csv").exists()
    )
    packs = [p for p in all_packs if not requested or p.name in requested]

    if not packs:
        raise RuntimeError(
            f"No matching packs found. Available: {[p.name for p in all_packs]}"
        )

    print(f"[harness] Running {len(packs)} pack(s): {[p.name for p in packs]}")

    # Back up real matched_raw.csv so we can restore after the run
    real_matched = OUTPUTS / "matched_raw.csv"
    if real_matched.exists():
        shutil.copy2(real_matched, _BACKUP)

    scorecard: list[dict] = []
    try:
        for pack_dir in packs:
            scorecard.append(run_pack(pack_dir))
    finally:
        # Restore real matched_raw.csv regardless of success/failure
        if _BACKUP.exists():
            shutil.copy2(_BACKUP, real_matched)
            _BACKUP.unlink()
            print("\n[harness] restored outputs/matched_raw.csv from backup")

    # Write scorecard — merge with any existing rows for other packs
    scorecard_path = REPO / "phase3_scorecard.csv"
    run_names = {r["pack_name"] for r in scorecard}
    merged: list[dict] = []
    if scorecard_path.exists():
        try:
            import pandas as _pd
            existing = _pd.read_csv(scorecard_path, dtype=str).fillna("").to_dict("records")
            merged = [r for r in existing if r.get("pack_name") not in run_names]
        except Exception:
            pass
    merged.extend(scorecard)

    all_cols = SCORECARD_COLS  # use canonical col order
    with open(scorecard_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(merged)

    # Print summary table (only the packs just run)
    print(f"\n{'=' * 70}")
    print(f"  PHASE 3 SCORECARD  ({len(scorecard)} packs)")
    print(f"{'=' * 70}")
    print(f"  {'pack':<28}  {'status':<12}  {'matched':>8}  {'t2':>4}  {'t4':>4}  {'susp':>5}  {'q0':>4}  {'secs':>6}")
    print(f"  {'-'*28}  {'-'*12}  {'-'*8}  {'-'*4}  {'-'*4}  {'-'*5}  {'-'*4}  {'-'*6}")
    for r in scorecard:
        q0 = int(r.get('q0_dup_old') or 0) + int(r.get('q0_dup_new') or 0)
        print(
            f"  {r['pack_name']:<28}  {r['status']:<12}  "
            f"{str(r.get('resolved_pairs','?')):>8}  "
            f"{str(r.get('tier2_last4_dob','?')):>4}  "
            f"{str(r.get('tier4_fallback','?')):>4}  "
            f"{str(r.get('suspicious_match_count','?')):>5}  "
            f"{str(q0):>4}  "
            f"{str(r.get('wall_secs','?')):>6}"
        )
    print(f"\n[done] {scorecard_path}")


if __name__ == "__main__":
    main()
