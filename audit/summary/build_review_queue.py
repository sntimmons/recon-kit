"""
build_review_queue.py — Production-grade review queue builder.

Reads matched_pairs from audit/audit.db, applies the gating engine to every row,
and writes audit/summary/review_queue.csv containing only rows with at least one
detected mismatch, sorted by priority_score desc.

Priority scoring
----------------
  +50  status changed
  +30  abs(salary_delta) >= 5000
  +15  abs(salary_delta) >= 1000          (cumulative with +30 for >= 5000)
  +20  hire_date changed
  +10  position changed
  +8   district changed
  +6   location_state changed
  +10  match_source != worker_id
  +10  confidence is missing (and source is not auto-approve)
  +5   multiple fix_types detected (2+)

Output columns
--------------
  pair_id, match_source, old_worker_id, new_worker_id, old_full_name_norm,
  fix_types, confidence, action, reason, priority_score, summary,
  salary_delta,
  old_salary, new_salary, old_payrate, new_payrate,
  old_worker_status, new_worker_status,
  old_hire_date, new_hire_date,
  old_position, new_position,
  old_district, new_district,
  old_location_state, new_location_state
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

# Allow sibling imports regardless of working directory.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

import gating
from confidence_policy import is_auto_approve_source
from sanity_checks import detect_wave_dates
from explanation import generate_explanation

ROOT    = _HERE.parents[1]
DB_PATH = ROOT / "audit" / "audit.db"
OUT_CSV = _HERE / "review_queue.csv"

_REQUIRED_COLS = [
    "pair_id", "match_source", "old_worker_id", "new_worker_id",
    "old_salary", "new_salary", "old_payrate", "new_payrate",
    "old_worker_status", "new_worker_status",
    "old_hire_date", "new_hire_date",
    "old_position", "new_position",
    "old_district", "new_district",
    "old_location_state", "new_location_state",
]


def _priority_score(row: dict, fix_types: list[str], result: dict) -> tuple[int, list[str]]:
    """
    Compute priority score and collect reason labels.
    Returns (score, list_of_labels).
    """
    score = 0
    labels: list[str] = []

    if "status" in fix_types:
        score += 50
        labels.append("status+50")

    d_sal = gating.salary_delta(row)
    if d_sal is not None:
        abs_d = abs(d_sal)
        if abs_d >= 5000:
            score += 30
            labels.append("sal>=5k+30")
        if abs_d >= 1000:
            score += 15
            labels.append("sal>=1k+15")

    if "hire_date" in fix_types:
        score += 20
        labels.append("hire_date+20")

    if "job_org" in fix_types:
        if gating._str_changed(row.get("old_position"), row.get("new_position")):
            score += 10
            labels.append("position+10")
        if gating._str_changed(row.get("old_district"), row.get("new_district")):
            score += 8
            labels.append("district+8")
        if gating._str_changed(row.get("old_location_state"), row.get("new_location_state")):
            score += 6
            labels.append("location+6")

    src = str(row.get("match_source", "")).strip().lower()
    if src != "worker_id":
        score += 10
        labels.append("non_wid+10")

    # Missing confidence (only relevant for non-auto-approve sources)
    if not is_auto_approve_source(src):
        conf_val = gating._parse_confidence(row.get("confidence"))
        if conf_val is None:
            score += 10
            labels.append("no_conf+10")

    if len(fix_types) >= 2:
        score += 5
        labels.append("multi_fix+5")

    return score, labels


def _build_row(row: dict, wave_dates: "frozenset[str] | None" = None) -> dict | None:
    """
    Process one matched-pair row.  Returns None if action != REVIEW.
    """
    result    = gating.classify_all(row, wave_dates=wave_dates)
    fix_types = result["fix_types"]
    if result["action"] != "REVIEW":
        return None

    score, labels = _priority_score(row, fix_types, result)

    d_sal  = gating.salary_delta(row)
    conf   = gating._parse_confidence(row.get("confidence"))

    return {
        "pair_id":            row.get("pair_id", ""),
        "match_source":       row.get("match_source", ""),
        "old_worker_id":      row.get("old_worker_id", ""),
        "new_worker_id":      row.get("new_worker_id", ""),
        "old_full_name_norm": row.get("old_full_name_norm", ""),
        "fix_types":          "|".join(fix_types),
        "confidence":         "" if conf is None else round(conf, 4),
        "action":             result["action"],
        "reason":             result["reason"],
        "priority_score":     score,
        "priority_labels":    "|".join(labels),
        "summary":            gating.build_summary_str(row, fix_types),
        "match_explanation":  generate_explanation(row, result),
        "salary_delta":       "" if d_sal is None else round(d_sal, 2),
        "old_salary":         row.get("old_salary", ""),
        "new_salary":         row.get("new_salary", ""),
        "old_payrate":        row.get("old_payrate", ""),
        "new_payrate":        row.get("new_payrate", ""),
        "old_worker_status":  row.get("old_worker_status", ""),
        "new_worker_status":  row.get("new_worker_status", ""),
        "old_hire_date":      row.get("old_hire_date", ""),
        "new_hire_date":      row.get("new_hire_date", ""),
        "old_position":       row.get("old_position", ""),
        "new_position":       row.get("new_position", ""),
        "old_district":       row.get("old_district", ""),
        "new_district":       row.get("new_district", ""),
        "old_location_state": row.get("old_location_state", ""),
        "new_location_state": row.get("new_location_state", ""),
    }


def main() -> None:
    if not DB_PATH.exists():
        print(f"[error] audit.db not found: {DB_PATH}", file=sys.stderr)
        sys.exit(2)

    con = sqlite3.connect(str(DB_PATH))
    try:
        try:
            mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)
        except Exception as exc:
            print(f"[error] cannot query matched_pairs: {exc}", file=sys.stderr)
            sys.exit(2)
    finally:
        con.close()

    cols = set(mp.columns)
    missing = [c for c in _REQUIRED_COLS if c not in cols]
    if missing:
        print(f"[error] matched_pairs missing columns: {sorted(missing)}", file=sys.stderr)
        sys.exit(2)

    total_in = len(mp)
    print(f"[build_review_queue] {total_in:,} matched pairs loaded.")

    # Add confidence column if absent (will be blank for all rows — handled gracefully)
    if "confidence" not in mp.columns:
        mp["confidence"] = None

    # Detect wave dates before per-row loop so classify_all can flag individual records
    all_rows   = mp.to_dict(orient="records")
    wave_dates = detect_wave_dates(all_rows)
    if wave_dates:
        print(f"  wave dates detected: {sorted(wave_dates)}")

    out_rows: list[dict] = []
    for r in all_rows:
        built = _build_row(r, wave_dates=wave_dates)
        if built is not None:
            out_rows.append(built)

    if not out_rows:
        print("[build_review_queue] no mismatches found — empty review queue.")
        pd.DataFrame(columns=[
            "pair_id", "match_source", "old_worker_id", "new_worker_id",
            "old_full_name_norm", "fix_types", "confidence", "action", "reason",
            "priority_score", "priority_labels", "summary", "match_explanation",
            "salary_delta",
            "old_salary", "new_salary", "old_payrate", "new_payrate",
            "old_worker_status", "new_worker_status",
            "old_hire_date", "new_hire_date",
            "old_position", "new_position",
            "old_district", "new_district",
            "old_location_state", "new_location_state",
        ]).to_csv(str(OUT_CSV), index=False)
        print(f"  wrote: {OUT_CSV.relative_to(ROOT)}  (0 rows)")
        return

    queue = pd.DataFrame(out_rows)

    # Sort: priority_score desc, abs(salary_delta) desc, pair_id asc
    queue["_abs_sal"] = pd.to_numeric(queue["salary_delta"], errors="coerce").abs().fillna(0)
    queue = queue.sort_values(
        by=["priority_score", "_abs_sal", "pair_id"],
        ascending=[False, False, True],
    ).drop(columns=["_abs_sal"])

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    queue.to_csv(str(OUT_CSV), index=False)

    # Print action summary by fix_type
    n_approve = int((queue["action"] == "APPROVE").sum())
    n_review  = int((queue["action"] == "REVIEW").sum())
    print(f"[build_review_queue] {len(queue):,} rows with mismatches  "
          f"({n_approve:,} APPROVE / {n_review:,} REVIEW)")

    # Per fix_type breakdown
    fix_type_counts: dict[str, dict[str, int]] = {}
    for _, row in queue.iterrows():
        for ft in str(row["fix_types"]).split("|"):
            if ft not in fix_type_counts:
                fix_type_counts[ft] = {"APPROVE": 0, "REVIEW": 0}
            fix_type_counts[ft][row["action"]] += 1

    print("  breakdown by fix_type:")
    for ft, counts in sorted(fix_type_counts.items()):
        total_ft = counts["APPROVE"] + counts["REVIEW"]
        print(f"    {ft:<12}  total={total_ft:>7,}  "
              f"APPROVE={counts['APPROVE']:>7,}  "
              f"REVIEW={counts['REVIEW']:>7,}")

    print(f"  wrote: {OUT_CSV.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
