"""
build_diy_exports.py - DIY XLOOKUP export generator.

Writes two CSVs to audit/exports/out/ for use in Excel XLOOKUP formulas
and wide side-by-side comparison with gating decisions.

Outputs
-------
  xlookup_keys.csv  - key fields for XLOOKUP matching
  wide_compare.csv  - all matched_pairs with gating columns appended

Extra fields from config/policy.yaml are appended to wide_compare as
old_<field>, new_<field>, mm_<field> triplets when available in matched_pairs.
Stable columns are always written first and in the same order.

Run:
    venv/Scripts/python.exe audit/exports/build_diy_exports.py [--db PATH] [--out-dir PATH]
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

import pandas as pd

_HERE        = Path(__file__).resolve().parent    # audit/exports/
_SUMMARY_DIR = _HERE.parent / "summary"           # audit/summary/
sys.path.insert(0, str(_SUMMARY_DIR))

from gating import (
    classify_all,
    salary_delta,
    payrate_delta,
    build_summary_str,
    _parse_confidence,
    _norm,
)
from config_loader import load_policy, load_extra_fields, load_pii_config, load_audit_config
from explanation import generate_explanation
from sanity_checks import detect_wave_dates

ROOT    = _HERE.parents[1]
_rk_work = Path(os.environ["RK_WORK_DIR"]) if "RK_WORK_DIR" in os.environ else None
DB_PATH = (_rk_work / "audit" / "audit.db") if _rk_work else (ROOT / "audit" / "audit.db")
OUT_DIR = (_rk_work / "exports") if _rk_work else (_HERE / "out")

_REQUIRED_DB_COLS = [
    "pair_id", "match_source",
    "old_worker_id", "new_worker_id",
]

_XLOOKUP_COLS = [
    "pair_id", "match_source", "confidence", "match_key",
    "old_worker_id", "new_worker_id",
    "old_recon_id", "new_recon_id",
    "old_full_name_norm", "new_full_name_norm",
]

# Stable wide_compare columns - do not reorder.
# Extra field triplets (old_/new_/mm_) are appended after this list.
_WIDE_COLS = [
    # Keys / gating
    "pair_id", "match_source", "confidence",
    "action", "reason", "fix_types", "summary", "match_explanation", "priority_score",
    # Side-by-side fields
    "old_full_name_norm", "new_full_name_norm",
    # Name components (first/last/middle/suffix + name change flag)
    "old_first_name_norm", "new_first_name_norm",
    "old_last_name_norm",  "new_last_name_norm",
    "old_middle_name",     "new_middle_name",
    "old_suffix",          "new_suffix",
    "name_change_detected",
    "old_worker_status", "new_worker_status",
    "old_worker_type", "new_worker_type",
    "old_hire_date", "new_hire_date",
    "old_position", "new_position",
    "old_district", "new_district",
    "old_location_state", "new_location_state",
    "old_location", "new_location",
    "old_salary", "new_salary",
    "old_payrate", "new_payrate",
    # Computed helpers
    "salary_delta", "salary_ratio", "payrate_delta",
    "conversion_type",
    "status_changed", "hire_date_changed", "job_org_changed",
    "hire_date_pattern",
    "needs_review", "suggested_action",
    # Compensation band validation (optional - populated when bands file provided)
    "comp_band_status", "comp_band_min", "comp_band_mid", "comp_band_max", "comp_band_match",
]

# Set of column names already in the stable schema - used to prevent duplication
_WIDE_EXISTING = set(_WIDE_COLS)


def _str_eq(a, b) -> bool:
    return _norm(a) == _norm(b)


def _salary_ratio(old_sal, new_sal):
    """Compute new_salary / old_salary; None if not computable."""
    try:
        o = float(str(old_sal or "").replace(",", "").replace("$", ""))
        n = float(str(new_sal or "").replace(",", "").replace("$", ""))
        if o == 0:
            return None
        return round(n / o, 6)
    except Exception:
        return None


def _priority_score(row: dict, fix_types: list[str], sal_d, result: dict) -> int:
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
        if not _str_eq(row.get("old_position"), row.get("new_position")):
            score += 10
        if not _str_eq(row.get("old_district"), row.get("new_district")):
            score += 8
        if not _str_eq(row.get("old_location_state"), row.get("new_location_state")):
            score += 6
    if _norm(row.get("match_source", "")) != "worker_id":
        score += 10
    if _parse_confidence(row.get("confidence")) is None:
        score += 10
    if len(fix_types) > 1:
        score += 5
    return score


def _opt(row: dict, key: str, has_col: bool) -> str:
    """Return field value or blank string if column not present."""
    if not has_col:
        return ""
    v = row.get(key)
    return "" if v is None else str(v)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="DIY XLOOKUP export generator.")
    parser.add_argument(
        "--db", default=None, metavar="PATH",
        help=f"SQLite database path (default: {DB_PATH}).",
    )
    parser.add_argument(
        "--out-dir", default=None, metavar="PATH",
        help=f"Output directory (default: {OUT_DIR}).",
    )
    args = parser.parse_args(argv)

    db_path = Path(args.db) if args.db else DB_PATH
    out_dir = Path(args.out_dir) if args.out_dir else OUT_DIR

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
    missing = [c for c in _REQUIRED_DB_COLS if c not in db_cols]
    if missing:
        print(f"[error] matched_pairs missing required columns: {sorted(missing)}", file=sys.stderr)
        sys.exit(2)

    if "confidence" not in mp.columns:
        mp = mp.copy()
        mp["confidence"] = None

    # PII guard: suppress DOB columns from downstream outputs if configured.
    if not pii_cfg.get("include_dob_in_exports", True):
        dob_cols = [c for c in ["old_dob", "new_dob"] if c in mp.columns]
        if dob_cols:
            mp = mp.drop(columns=dob_cols)
            print(f"[build_diy_exports] [pii] suppressed DOB columns: {dob_cols}")

    has_recon_id    = "old_recon_id" in db_cols
    has_location    = "old_location" in db_cols
    has_worker_type = "old_worker_type" in db_cols

    # Determine which extra fields are actually available in the DB.
    # Skip any whose old_/new_ columns already exist in the stable _WIDE_COLS.
    available_extra: list[str] = []
    for field in configured_extra:
        old_col = f"old_{field}"
        new_col = f"new_{field}"
        if old_col in _WIDE_EXISTING or new_col in _WIDE_EXISTING:
            continue  # already covered by the stable schema
        if old_col in db_cols or new_col in db_cols:
            available_extra.append(field)
        else:
            print(f"[warn] extra_field_missing: {field}")

    total_in = len(mp)
    print(f"[build_diy_exports] {total_in:,} matched pairs loaded.")
    print(f"  has_recon_id    : {has_recon_id}")
    print(f"  has_location    : {has_location}")
    print(f"  has_worker_type : {has_worker_type}")
    if available_extra:
        print(f"  extra fields    : {available_extra}")

    # Detect wave dates before per-row loop so classify_all can flag individual records
    all_rows   = mp.to_dict(orient="records")
    wave_dates = detect_wave_dates(all_rows)
    if wave_dates:
        print(f"  wave dates detected: {sorted(wave_dates)}")

    xlookup_rows: list[dict] = []
    wide_rows:    list[dict] = []
    n_approve = 0
    n_review  = 0

    for r in all_rows:
        result    = classify_all(r, wave_dates=wave_dates)
        fix_types = result["fix_types"]
        action    = result["action"]
        reason    = result["reason"]
        summary   = build_summary_str(r, fix_types) if fix_types else "no_changes"

        sal_d   = salary_delta(r)
        pay_d   = payrate_delta(r)
        sal_rat = _salary_ratio(r.get("old_salary"), r.get("new_salary")) if sal_d is not None else None
        prio    = _priority_score(r, fix_types, sal_d, result)

        ms        = _norm(r.get("match_source", ""))
        match_key = str(r.get("old_worker_id", "")) if ms == "worker_id" else ""

        xlookup_rows.append({
            "pair_id":            r.get("pair_id", ""),
            "match_source":       r.get("match_source", ""),
            "confidence":         r.get("confidence", ""),
            "match_key":          match_key,
            "old_worker_id":      r.get("old_worker_id", ""),
            "new_worker_id":      r.get("new_worker_id", ""),
            "old_recon_id":       _opt(r, "old_recon_id", has_recon_id),
            "new_recon_id":       _opt(r, "new_recon_id", has_recon_id),
            "old_full_name_norm": r.get("old_full_name_norm", ""),
            "new_full_name_norm": r.get("new_full_name_norm", ""),
        })

        wide_row = {
            "pair_id":            r.get("pair_id", ""),
            "match_source":       r.get("match_source", ""),
            "confidence":         r.get("confidence"),
            "action":             action,
            "reason":             reason,
            "fix_types":          "|".join(fix_types),
            "summary":            summary,
            "match_explanation":  generate_explanation(r, result),
            "priority_score":     prio,
            "old_full_name_norm": r.get("old_full_name_norm", ""),
            "new_full_name_norm": r.get("new_full_name_norm", ""),
            "old_first_name_norm": r.get("old_first_name_norm", ""),
            "new_first_name_norm": r.get("new_first_name_norm", ""),
            "old_last_name_norm":  r.get("old_last_name_norm", ""),
            "new_last_name_norm":  r.get("new_last_name_norm", ""),
            "old_middle_name":     r.get("old_middle_name", ""),
            "new_middle_name":     r.get("new_middle_name", ""),
            "old_suffix":          r.get("old_suffix", ""),
            "new_suffix":          r.get("new_suffix", ""),
            "name_change_detected": r.get("name_change_detected", ""),
            "old_worker_status":  r.get("old_worker_status", ""),
            "new_worker_status":  r.get("new_worker_status", ""),
            "old_worker_type":    _opt(r, "old_worker_type", has_worker_type),
            "new_worker_type":    _opt(r, "new_worker_type", has_worker_type),
            "old_hire_date":      r.get("old_hire_date", ""),
            "new_hire_date":      r.get("new_hire_date", ""),
            "old_position":       r.get("old_position", ""),
            "new_position":       r.get("new_position", ""),
            "old_district":       r.get("old_district", ""),
            "new_district":       r.get("new_district", ""),
            "old_location_state": r.get("old_location_state", ""),
            "new_location_state": r.get("new_location_state", ""),
            "old_location":       _opt(r, "old_location", has_location),
            "new_location":       _opt(r, "new_location", has_location),
            "old_salary":         r.get("old_salary"),
            "new_salary":         r.get("new_salary"),
            "old_payrate":        r.get("old_payrate"),
            "new_payrate":        r.get("new_payrate"),
            "salary_delta":       sal_d,
            "salary_ratio":       sal_rat,
            "payrate_delta":      pay_d,
            "conversion_type":    result.get("conversion_type") or "",
            "status_changed":     not _str_eq(r.get("old_worker_status"), r.get("new_worker_status")),
            "hire_date_changed":  not _str_eq(r.get("old_hire_date"), r.get("new_hire_date")),
            "job_org_changed":    "job_org" in fix_types,
            # Fix 4: hire_date_pattern - populated when a systematic pattern was detected
            # (off_by_one_day_pattern or systematic_year_shift_pattern).
            "hire_date_pattern":  result.get("per_fix", {}).get("hire_date", {}).get("reason", "")
                                  if "hire_date" in fix_types else "",
            "needs_review":       action == "REVIEW",
            "suggested_action":   action,
            # Comp band - populated by comp_band_validator.py if bands file provided
            "comp_band_status":   "",
            "comp_band_min":      "",
            "comp_band_mid":      "",
            "comp_band_max":      "",
            "comp_band_match":    "",
        }

        # Append extra field triplets + compute per-field mismatch for groups.
        field_mm: dict[str, bool] = {}
        for field in available_extra:
            old_val = r.get(f"old_{field}")
            new_val = r.get(f"new_{field}")
            wide_row[f"old_{field}"] = "" if old_val is None else str(old_val)
            wide_row[f"new_{field}"] = "" if new_val is None else str(new_val)
            mm = _norm(old_val) != _norm(new_val)
            wide_row[f"mm_{field}"]  = mm
            field_mm[field] = mm

        # Group mismatch booleans - True if any field in the group has a mismatch.
        for group_name, group_fields in extra_groups.items():
            wide_row[f"mismatch_group_{group_name}"] = any(
                field_mm.get(f, False) for f in group_fields
            )

        wide_rows.append(wide_row)

        if action == "APPROVE":
            n_approve += 1
        else:
            n_review += 1

    # Build final column lists: stable + group booleans + extra triplets
    extra_cols: list[str] = []
    for group_name in extra_groups:
        extra_cols.append(f"mismatch_group_{group_name}")
    for field in available_extra:
        extra_cols.extend([f"old_{field}", f"new_{field}", f"mm_{field}"])
    final_wide_cols = _WIDE_COLS + extra_cols

    # Write outputs
    out_dir.mkdir(parents=True, exist_ok=True)

    xl_df = (
        pd.DataFrame(xlookup_rows, columns=_XLOOKUP_COLS)
        if xlookup_rows
        else pd.DataFrame(columns=_XLOOKUP_COLS)
    )
    xl_df.to_csv(str(out_dir / "xlookup_keys.csv"), index=False)
    print(f"\n  wrote: xlookup_keys.csv   ({len(xl_df):,} rows)")

    wide_df = (
        pd.DataFrame(wide_rows, columns=final_wide_cols)
        if wide_rows
        else pd.DataFrame(columns=final_wide_cols)
    )
    wide_df.to_csv(str(out_dir / "wide_compare.csv"), index=False)
    print(f"  wrote: wide_compare.csv   ({len(wide_df):,} rows)")
    if extra_cols:
        print(f"  extra columns appended   : {extra_cols}")

    print(f"\n[build_diy_exports] complete.")
    print(f"  total rows : {total_in:,}")
    print(f"  APPROVE    : {n_approve:,}")
    print(f"  REVIEW     : {n_review:,}")


if __name__ == "__main__":
    main()
