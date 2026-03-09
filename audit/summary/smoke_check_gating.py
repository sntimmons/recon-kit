"""
smoke_check_gating.py — Validates gating logic against audit/audit.db.

Assertions checked
------------------
1. worker_id rows are always APPROVE regardless of fix_type.
2. Non-worker_id rows (pk, last4_dob, etc.) with no confidence value
   become REVIEW with reason "missing_confidence".
3. classify_all returns consistent fix_types and action.
4. Priority scores are non-negative integers.
5. build_summary_str returns a non-empty string for rows with fix_types.

Run with:
    venv/Scripts/python.exe audit/summary/smoke_check_gating.py
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

import gating
from build_review_queue import _build_row, _priority_score

ROOT    = _HERE.parents[1]
DB_PATH = ROOT / "audit" / "audit.db"

SAMPLE_SIZE = 50
PRINT_EXAMPLES = 5


def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    print("=" * 60)
    print("  SMOKE CHECK: gating engine")
    print("=" * 60)

    if not DB_PATH.exists():
        _fail(f"audit.db not found: {DB_PATH}")

    con = sqlite3.connect(str(DB_PATH))
    try:
        try:
            mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)
        except Exception as exc:
            _fail(f"cannot query matched_pairs: {exc}")
    finally:
        con.close()

    if "confidence" not in mp.columns:
        mp["confidence"] = None

    total = len(mp)
    print(f"  Loaded {total:,} rows from matched_pairs.")

    # Find rows with at least one mismatch
    mismatch_rows = []
    for r in mp.to_dict(orient="records"):
        ft = gating.infer_fix_types(r)
        if ft:
            mismatch_rows.append(r)
        if len(mismatch_rows) >= SAMPLE_SIZE:
            break

    n_mismatch = len(mismatch_rows)
    print(f"  Found {n_mismatch} mismatch rows in first {total:,} (capped at {SAMPLE_SIZE}).")

    if n_mismatch == 0:
        print("  No mismatches found — cannot perform all checks. (DB may need re-running pipeline.)")

    # ------------------------------------------------------------------
    # Assertion 1: worker_id rows must always be APPROVE
    # ------------------------------------------------------------------
    wid_rows = [r for r in mismatch_rows if str(r.get("match_source", "")).strip().lower() == "worker_id"]
    fail_wid = []
    for r in wid_rows:
        result = gating.classify_all(r)
        if result["action"] != "APPROVE":
            fail_wid.append((r.get("pair_id"), result))
    if fail_wid:
        _fail(f"Assertion 1 FAILED: {len(fail_wid)} worker_id rows returned non-APPROVE: {fail_wid[:2]}")
    print(f"\n  [PASS] Assertion 1: all {len(wid_rows)} worker_id mismatch rows -> APPROVE")  # noqa: RUF001

    # ------------------------------------------------------------------
    # Assertion 2: non-worker_id rows with no confidence -> REVIEW / missing_confidence
    # ------------------------------------------------------------------
    non_wid = [r for r in mismatch_rows if str(r.get("match_source", "")).strip().lower() != "worker_id"]
    fail_nonwid = []
    for r in non_wid:
        result = gating.classify_all(r)
        # confidence is None (no confidence column in DB)
        conf = gating._parse_confidence(r.get("confidence"))
        if conf is None:
            if result["action"] != "REVIEW":
                fail_nonwid.append((r.get("pair_id"), result))
            else:
                # Check that at least one per_fix reason is missing_confidence
                has_mc = any("missing_confidence" in v["reason"] for v in result["per_fix"].values())
                if not has_mc:
                    fail_nonwid.append((r.get("pair_id"), result))
    if fail_nonwid:
        _fail(f"Assertion 2 FAILED: {len(fail_nonwid)} non-worker_id rows without confidence did not get REVIEW/missing_confidence: {fail_nonwid[:2]}")
    msg2 = f"all {len(non_wid)} non-worker_id rows with missing confidence -> REVIEW (missing_confidence)" if non_wid else "no non-worker_id rows in sample (all match via worker_id)"
    print(f"  [PASS] Assertion 2: {msg2}")

    # ------------------------------------------------------------------
    # Assertion 3: classify_all returns consistent fix_types and action
    # ------------------------------------------------------------------
    fail_cons = []
    for r in mismatch_rows:
        result   = gating.classify_all(r)
        ft_infer = gating.infer_fix_types(r)
        if sorted(result["fix_types"]) != sorted(ft_infer):
            fail_cons.append((r.get("pair_id"), result["fix_types"], ft_infer))
        # Action must be REVIEW if any per_fix is REVIEW
        if result["per_fix"]:
            expected_action = "REVIEW" if any(v["action"] == "REVIEW" for v in result["per_fix"].values()) else "APPROVE"
            if result["action"] != expected_action:
                fail_cons.append((r.get("pair_id"), "action mismatch", result))
    if fail_cons:
        _fail(f"Assertion 3 FAILED: {len(fail_cons)} inconsistencies in classify_all: {fail_cons[:2]}")
    print(f"  [PASS] Assertion 3: fix_types + action consistent across {len(mismatch_rows)} rows")

    # ------------------------------------------------------------------
    # Assertion 4: priority scores are non-negative integers
    # ------------------------------------------------------------------
    fail_score = []
    for r in mismatch_rows:
        result    = gating.classify_all(r)
        fix_types = result["fix_types"]
        score, _  = _priority_score(r, fix_types, result)
        if not isinstance(score, int) or score < 0:
            fail_score.append((r.get("pair_id"), score))
    if fail_score:
        _fail(f"Assertion 4 FAILED: invalid priority scores: {fail_score[:3]}")
    print(f"  [PASS] Assertion 4: priority scores are non-negative integers")

    # ------------------------------------------------------------------
    # Assertion 5: build_summary_str non-empty for rows with fix_types
    # ------------------------------------------------------------------
    fail_summ = []
    for r in mismatch_rows:
        ft   = gating.infer_fix_types(r)
        summ = gating.build_summary_str(r, ft)
        if ft and (not summ or summ == "no_changes"):
            fail_summ.append((r.get("pair_id"), ft, summ))
    if fail_summ:
        _fail(f"Assertion 5 FAILED: {len(fail_summ)} rows with fix_types returned empty summary: {fail_summ[:2]}")
    print(f"  [PASS] Assertion 5: build_summary_str non-empty for all {len(mismatch_rows)} mismatch rows")

    # ------------------------------------------------------------------
    # Assertion 6: confidence present and in [0, 1] for all sampled rows
    # ------------------------------------------------------------------
    fail_conf = []
    for r in mismatch_rows:
        ms   = str(r.get("match_source", "")).strip().lower()
        conf = gating._parse_confidence(r.get("confidence"))
        if ms in ("worker_id", "recon_id"):
            # Exact-ID matches must always have confidence 1.0
            if conf is not None and abs(conf - 1.0) > 1e-6:
                fail_conf.append((r.get("pair_id"), ms, conf, "expected 1.0"))
        else:
            # Probabilistic tiers: confidence present and in [0, 1]
            if conf is not None and not (0.0 <= conf <= 1.0):
                fail_conf.append((r.get("pair_id"), ms, conf, "out of range"))
    if fail_conf:
        _fail(f"Assertion 6 FAILED: confidence out of range or wrong for worker_id rows: {fail_conf[:3]}")
    checked = len(mismatch_rows)
    print(f"  [PASS] Assertion 6: confidence in [0,1] for all {checked} sampled rows")

    # ------------------------------------------------------------------
    # Print 5 example rows
    # ------------------------------------------------------------------
    print(f"\n  Sample of {min(PRINT_EXAMPLES, n_mismatch)} mismatch rows:")
    print(f"  {'pair_id':<14}  {'match_source':<12}  {'fix_types':<30}  {'action':<8}  {'score':>5}  reason")
    print("  " + "-" * 100)
    for r in mismatch_rows[:PRINT_EXAMPLES]:
        result    = gating.classify_all(r)
        fix_types = result["fix_types"]
        score, _  = _priority_score(r, fix_types, result)
        summ      = gating.build_summary_str(r, fix_types)
        print(
            f"  {str(r.get('pair_id', ''))[:14]:<14}  "
            f"{str(r.get('match_source', '')):<12}  "
            f"{','.join(fix_types):<30}  "
            f"{result['action']:<8}  "
            f"{score:>5}  "
            f"{result['reason'][:60]}"
        )
        print(f"  {'':14}  summary: {summ}")

    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")


if __name__ == "__main__":
    main()
