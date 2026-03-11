"""
build_ui_pairs.py - Build a UI-ready consolidated dataset of all matched pairs.

Reads the matched_pairs view from audit/audit.db, applies gating logic to
every row, and writes one CSV with 1 row per pair_id containing identifiers,
gating decisions, mismatch booleans, and side-by-side field comparisons.

Schema is governed by audit/ui/contract_v1.json.
Required columns are stable; extra fields from config/policy.yaml are appended
after the required block if they exist in matched_pairs.

Output
------
  audit/ui/ui_pairs.csv

Run
---
  venv/Scripts/python.exe audit/ui/build_ui_pairs.py [--db PATH] [--out PATH]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd

_HERE        = Path(__file__).resolve().parent    # audit/ui/
_SUMMARY_DIR = _HERE.parent / "summary"           # audit/summary/
sys.path.insert(0, str(_SUMMARY_DIR))

from gating import (                              # noqa: E402
    classify_all,
    salary_delta,
    payrate_delta,
    build_summary_str,
    _parse_confidence,
    _norm,
    _str_changed,
)
from confidence_policy import is_auto_approve_source  # noqa: E402
from config_loader import load_policy, load_extra_fields, load_pii_config, load_audit_config  # noqa: E402

ROOT    = _HERE.parents[1]
DB_PATH = ROOT / "audit" / "audit.db"
OUT_CSV = _HERE / "ui_pairs.csv"

# Contract version written into every row so consumers can detect schema changes.
_CONTRACT_VERSION = "v1"

_REQUIRED_COLS = [
    "pair_id", "match_source", "old_worker_id", "new_worker_id",
    "old_salary", "new_salary", "old_payrate", "new_payrate",
    "old_worker_status", "new_worker_status",
    "old_hire_date", "new_hire_date",
    "old_position", "new_position",
    "old_district", "new_district",
    "old_location_state", "new_location_state",
]

# Stable required output columns - ui_contract_version is ALWAYS first.
# Do not reorder these; extra_fields columns are appended after this list.
_OUTPUT_COLS = [
    "ui_contract_version",
    # Identifiers
    "pair_id", "match_source", "old_worker_id", "new_worker_id",
    # Gating
    "fix_types", "action", "reason", "confidence", "min_confidence",
    "priority_score", "summary",
    # Mismatch booleans
    "has_salary_mismatch", "has_payrate_mismatch", "has_status_mismatch",
    "has_hire_date_mismatch", "has_job_org_mismatch",
    # Computed helpers
    "salary_delta", "payrate_delta",
    # Side-by-side fields
    "old_salary", "new_salary",
    "old_payrate", "new_payrate",
    "old_worker_status", "new_worker_status",
    "old_hire_date", "new_hire_date",
    "old_position", "new_position",
    "old_district", "new_district",
    "old_location_state", "new_location_state",
    "old_location", "new_location",
    "old_worker_type", "new_worker_type",
]


def _priority_score(row: dict, fix_types: list[str], sal_d) -> int:
    """Compute priority score - same rules as build_review_queue / build_diy_exports."""
    score = 0
    if "status" in fix_types:
        score += 50
    if sal_d is not None:
        if abs(sal_d) >= 5000:
            score += 30
        if abs(sal_d) >= 1000:
            score += 15
    if "hire_date" in fix_types:
        score += 20
    if "job_org" in fix_types:
        if _str_changed(row.get("old_position"),       row.get("new_position")):
            score += 10
        if _str_changed(row.get("old_district"),       row.get("new_district")):
            score += 8
        if _str_changed(row.get("old_location_state"), row.get("new_location_state")):
            score += 6
    src = _norm(row.get("match_source", ""))
    if src != "worker_id":
        score += 10
    if not is_auto_approve_source(src) and _parse_confidence(row.get("confidence")) is None:
        score += 10
    if len(fix_types) >= 2:
        score += 5
    return score


def _opt(row: dict, key: str, has_col: bool) -> str:
    if not has_col:
        return ""
    v = row.get(key)
    return "" if v is None else str(v)


def _build_row(
    row: dict,
    has_location: bool,
    has_worker_type: bool,
    available_extra: list[str],
    extra_groups: dict[str, list[str]] | None = None,
) -> dict:
    """Build one output row for a matched pair."""
    result    = classify_all(row)
    fix_types = result["fix_types"]
    action    = result["action"]
    reason    = result["reason"]

    sal_d  = salary_delta(row)
    pay_d  = payrate_delta(row)
    conf   = _parse_confidence(row.get("confidence"))
    prio   = _priority_score(row, fix_types, sal_d)
    summary = build_summary_str(row, fix_types) if fix_types else "no_changes"

    # Per-fix min_confidence: take the lowest threshold across all fix_types
    per_fix = result.get("per_fix", {})
    min_confs = [v.get("min_confidence") for v in per_fix.values() if v.get("min_confidence") is not None]
    min_conf = min(min_confs) if min_confs else None

    out = {
        # Contract version - always first
        "ui_contract_version":    _CONTRACT_VERSION,
        # Identifiers
        "pair_id":                row.get("pair_id", ""),
        "match_source":           row.get("match_source", ""),
        "old_worker_id":          row.get("old_worker_id", ""),
        "new_worker_id":          row.get("new_worker_id", ""),
        # Gating
        "fix_types":              "|".join(fix_types) if fix_types else "",
        "action":                 action,
        "reason":                 reason,
        "confidence":             "" if conf is None else round(conf, 4),
        "min_confidence":         "" if min_conf is None else round(min_conf, 4),
        "priority_score":         prio,
        "summary":                summary,
        # Mismatch booleans
        "has_salary_mismatch":    "salary"    in fix_types,
        "has_payrate_mismatch":   "payrate"   in fix_types,
        "has_status_mismatch":    "status"    in fix_types,
        "has_hire_date_mismatch": "hire_date" in fix_types,
        "has_job_org_mismatch":   "job_org"   in fix_types,
        # Computed helpers
        "salary_delta":           "" if sal_d is None else round(sal_d, 2),
        "payrate_delta":          "" if pay_d is None else round(pay_d, 4),
        # Side-by-side fields
        "old_salary":             row.get("old_salary", ""),
        "new_salary":             row.get("new_salary", ""),
        "old_payrate":            row.get("old_payrate", ""),
        "new_payrate":            row.get("new_payrate", ""),
        "old_worker_status":      row.get("old_worker_status", ""),
        "new_worker_status":      row.get("new_worker_status", ""),
        "old_hire_date":          row.get("old_hire_date", ""),
        "new_hire_date":          row.get("new_hire_date", ""),
        "old_position":           row.get("old_position", ""),
        "new_position":           row.get("new_position", ""),
        "old_district":           row.get("old_district", ""),
        "new_district":           row.get("new_district", ""),
        "old_location_state":     row.get("old_location_state", ""),
        "new_location_state":     row.get("new_location_state", ""),
        "old_location":           _opt(row, "old_location",    has_location),
        "new_location":           _opt(row, "new_location",    has_location),
        "old_worker_type":        _opt(row, "old_worker_type", has_worker_type),
        "new_worker_type":        _opt(row, "new_worker_type", has_worker_type),
    }

    # Extra fields - appended after stable required block
    # Build field mismatch map for group computation below.
    field_mm: dict[str, bool] = {}
    for field in available_extra:
        old_val = row.get(f"old_{field}")
        new_val = row.get(f"new_{field}")
        out[f"old_{field}"] = "" if old_val is None else str(old_val)
        out[f"new_{field}"] = "" if new_val is None else str(new_val)
        mm = _norm(old_val) != _norm(new_val)
        out[f"mm_{field}"]  = mm
        field_mm[field] = mm

    # Group mismatch booleans - True if any field in the group has a mismatch.
    if extra_groups:
        for group_name, group_fields in extra_groups.items():
            out[f"mismatch_group_{group_name}"] = any(
                field_mm.get(f, False) for f in group_fields
            )

    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build UI-ready consolidated pairs CSV.")
    parser.add_argument("--db",  default=None, metavar="PATH",
                        help=f"SQLite DB path (default: {DB_PATH}).")
    parser.add_argument("--out", default=None, metavar="PATH",
                        help=f"Output CSV path (default: {OUT_CSV}).")
    args = parser.parse_args(argv)

    db_path  = Path(args.db)  if args.db  else DB_PATH
    out_path = Path(args.out) if args.out else OUT_CSV

    if not db_path.exists():
        print(f"[error] DB not found: {db_path}", file=sys.stderr)
        sys.exit(2)

    # Load policy configs
    policy           = load_policy()
    audit_cfg        = load_audit_config(policy)
    configured_extra = load_extra_fields(policy)
    pii_cfg          = load_pii_config(policy)
    extra_groups     = audit_cfg.get("groups", {})

    con = sqlite3.connect(str(db_path))
    try:
        try:
            mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)
        except Exception as exc:
            print(f"[error] cannot query matched_pairs: {exc}", file=sys.stderr)
            sys.exit(2)
    finally:
        con.close()

    db_cols = set(mp.columns)
    missing = [c for c in _REQUIRED_COLS if c not in db_cols]
    if missing:
        print(f"[error] matched_pairs missing required columns: {sorted(missing)}", file=sys.stderr)
        sys.exit(2)

    if "confidence" not in mp.columns:
        mp["confidence"] = None

    # PII guard: suppress DOB columns from downstream output if configured.
    if not pii_cfg.get("include_dob_in_ui", True):
        dob_cols = [c for c in ["old_dob", "new_dob"] if c in mp.columns]
        if dob_cols:
            mp = mp.drop(columns=dob_cols)
            print(f"[build_ui_pairs] [pii] suppressed DOB columns: {dob_cols}")

    has_location    = "old_location"    in db_cols
    has_worker_type = "old_worker_type" in db_cols

    # Determine which extra fields are actually available in the DB.
    # Skip any whose columns already appear in the stable _OUTPUT_COLS.
    _existing = set(_OUTPUT_COLS)
    available_extra: list[str] = []
    for field in configured_extra:
        old_col = f"old_{field}"
        new_col = f"new_{field}"
        if old_col in _existing or new_col in _existing:
            # Already in stable schema - no duplication needed
            continue
        if old_col in db_cols or new_col in db_cols:
            available_extra.append(field)
        else:
            print(f"[warn] extra_field_missing: {field}")

    total_in = len(mp)
    print(f"[build_ui_pairs] {total_in:,} matched pairs loaded from {db_path.name}.")
    if available_extra:
        print(f"  extra fields active  : {available_extra}")
    if extra_groups:
        print(f"  extra groups active  : {list(extra_groups)}")

    out_rows: list[dict] = []
    for r in mp.to_dict(orient="records"):
        out_rows.append(_build_row(r, has_location, has_worker_type, available_extra, extra_groups))

    # Build final column list: stable required cols + group booleans + extra triplets (old/new/mm)
    extra_cols: list[str] = []
    for group_name in extra_groups:
        extra_cols.append(f"mismatch_group_{group_name}")
    for field in available_extra:
        extra_cols.extend([f"old_{field}", f"new_{field}", f"mm_{field}"])
    final_cols = _OUTPUT_COLS + extra_cols

    out_df = (
        pd.DataFrame(out_rows, columns=final_cols)
        if out_rows
        else pd.DataFrame(columns=final_cols)
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(str(out_path), index=False)

    n_approve  = int((out_df["action"] == "APPROVE").sum())
    n_review   = int((out_df["action"] == "REVIEW").sum())
    n_mismatch = int(
        (
            out_df["has_salary_mismatch"]
            | out_df["has_payrate_mismatch"]
            | out_df["has_status_mismatch"]
            | out_df["has_hire_date_mismatch"]
            | out_df["has_job_org_mismatch"]
        ).sum()
    )

    print(f"[build_ui_pairs] {len(out_df):,} rows written.")
    print(f"  contract version : {_CONTRACT_VERSION}")
    print(f"  APPROVE          : {n_approve:,}")
    print(f"  REVIEW           : {n_review:,}")
    print(f"  mismatches       : {n_mismatch:,}")
    print(f"  extra fields     : {available_extra or 'none'}")
    print(f"  wrote            : {out_path}")


if __name__ == "__main__":
    main()
