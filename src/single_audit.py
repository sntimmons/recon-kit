"""
single_audit.py — Run a single correction-type audit slice from an existing DB.

Reads matched_pairs from audit.db, filters to rows with a mismatch of the
requested type, applies gating, and writes a focused set of output files
without requiring the full pipeline to re-run.

Usage
-----
    venv/Scripts/python.exe src/single_audit.py --type salary
    venv/Scripts/python.exe src/single_audit.py --type status
    venv/Scripts/python.exe src/single_audit.py --type hire_date
    venv/Scripts/python.exe src/single_audit.py --type job_org

Flags
-----
    --type           salary|status|hire_date|job_org  (required)
    --db             PATH  (default: audit/audit.db)
    --out-dir        PATH  (default: audit/single_audit/<type>_<timestamp>/)
    --rebuild-db     re-run load_sqlite + run_audit before slicing
    --only-approved  output corrections only for APPROVE rows (default: True)
    --no-only-approved  include REVIEW rows in corrections CSV too

Outputs (in --out-dir)
-----------------------
    ui_pairs_<type>.csv      — 1 row/pair, same gating cols as ui_pairs.csv
    review_queue_<type>.csv  — REVIEW rows only, with gating context
    corrections_<type>.csv   — Workday-ready correction rows (APPROVE by default)
    manifest_<type>.csv      — file summary with row counts
    receipt.json             — run metadata (inputs, counts, gate summary)
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT         = Path(__file__).resolve().parents[1]
_SUMMARY_DIR = ROOT / "audit" / "summary"
sys.path.insert(0, str(_SUMMARY_DIR))

from gating import (  # noqa: E402
    classify_all,
    classify_row,
    infer_fix_types,
    salary_delta,
    payrate_delta,
    build_summary_str,
    _norm,
    _parse_confidence,
)
from build_review_queue import _priority_score  # noqa: E402

DB_PATH = ROOT / "audit" / "audit.db"

_TYPES = {"salary", "status", "hire_date", "job_org"}

# Per-type correction output columns (mirrors generate_corrections.py schema).
_CORRECTION_COLS: dict[str, list[str]] = {
    "salary": [
        "worker_id", "effective_date", "compensation_amount", "currency",
        "reason", "pair_id", "match_source", "confidence", "summary",
    ],
    "status": [
        "worker_id", "effective_date", "worker_status",
        "reason", "pair_id", "match_source", "confidence", "summary",
    ],
    "hire_date": [
        "worker_id", "hire_date",
        "reason", "pair_id", "match_source", "confidence", "summary",
    ],
    "job_org": [
        "worker_id", "effective_date", "position", "district",
        "location_state", "location",
        "reason", "pair_id", "match_source", "confidence", "summary",
    ],
}


# ---------------------------------------------------------------------------
# Mismatch detection
# ---------------------------------------------------------------------------

def _has_mismatch(row: dict, audit_type: str) -> bool:
    """Return True if the row has a mismatch of the given type."""
    if audit_type == "salary":
        d = salary_delta(row)
        return d is not None and d != 0.0
    elif audit_type == "status":
        return _norm(row.get("old_worker_status")) != _norm(row.get("new_worker_status"))
    elif audit_type == "hire_date":
        return _norm(row.get("old_hire_date")) != _norm(row.get("new_hire_date"))
    elif audit_type == "job_org":
        return (
            _norm(row.get("old_position"))       != _norm(row.get("new_position"))
            or _norm(row.get("old_district"))    != _norm(row.get("new_district"))
            or _norm(row.get("old_location_state")) != _norm(row.get("new_location_state"))
        )
    return False


# ---------------------------------------------------------------------------
# Output row builders
# ---------------------------------------------------------------------------

def _build_ui_row(row: dict, audit_type: str) -> dict:
    """Build a ui_pairs-style output row for a single matched pair."""
    result    = classify_all(row)
    fix_types = result["fix_types"]
    action    = result["action"]
    reason    = result["reason"]
    sal_d     = salary_delta(row)
    pay_d     = payrate_delta(row)
    conf      = _parse_confidence(row.get("confidence"))
    prio      = _priority_score(row, fix_types, result)
    summary   = build_summary_str(row, fix_types) if fix_types else "no_changes"
    per_fix   = result.get("per_fix", {})
    min_confs = [v.get("min_confidence") for v in per_fix.values() if v.get("min_confidence") is not None]
    min_conf  = min(min_confs) if min_confs else None

    return {
        "pair_id":            row.get("pair_id", ""),
        "match_source":       row.get("match_source", ""),
        "old_worker_id":      row.get("old_worker_id", ""),
        "new_worker_id":      row.get("new_worker_id", ""),
        "audit_type":         audit_type,
        "fix_types":          "|".join(fix_types),
        "action":             action,
        "reason":             reason,
        "confidence":         "" if conf is None else round(conf, 4),
        "min_confidence":     "" if min_conf is None else round(min_conf, 4),
        "priority_score":     prio,
        "summary":            summary,
        "salary_delta":       "" if sal_d is None else round(sal_d, 2),
        "payrate_delta":      "" if pay_d is None else round(pay_d, 4),
        "old_salary":         row.get("old_salary", ""),
        "new_salary":         row.get("new_salary", ""),
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


def _build_correction_row(row: dict, audit_type: str, gate: dict) -> dict | None:
    """Build a Workday-ready correction row for the given audit type."""
    worker_id    = str(row.get("new_worker_id") or row.get("old_worker_id") or "").strip()
    pair_id      = str(row.get("pair_id", ""))
    match_source = str(row.get("match_source", ""))
    confidence   = str(row.get("confidence") or "")
    reason       = gate.get("reason", "")
    summary      = build_summary_str(row, [audit_type])

    if audit_type == "salary":
        return {
            "worker_id":           worker_id,
            "effective_date":      "",
            "compensation_amount": str(row.get("new_salary") or "").strip(),
            "currency":            "USD",
            "reason":              reason,
            "pair_id":             pair_id,
            "match_source":        match_source,
            "confidence":          confidence,
            "summary":             summary,
        }
    elif audit_type == "status":
        return {
            "worker_id":     worker_id,
            "effective_date": "",
            "worker_status": str(row.get("new_worker_status") or "").strip(),
            "reason":        reason,
            "pair_id":       pair_id,
            "match_source":  match_source,
            "confidence":    confidence,
            "summary":       summary,
        }
    elif audit_type == "hire_date":
        return {
            "worker_id":    worker_id,
            "hire_date":    str(row.get("new_hire_date") or "").strip(),
            "reason":       reason,
            "pair_id":      pair_id,
            "match_source": match_source,
            "confidence":   confidence,
            "summary":      summary,
        }
    elif audit_type == "job_org":
        return {
            "worker_id":      worker_id,
            "effective_date": "",
            "position":       str(row.get("new_position") or "").strip(),
            "district":       str(row.get("new_district") or "").strip(),
            "location_state": str(row.get("new_location_state") or "").strip(),
            "location":       str(row.get("new_location") or "").strip(),
            "reason":         reason,
            "pair_id":        pair_id,
            "match_source":   match_source,
            "confidence":     confidence,
            "summary":        summary,
        }
    return None


# ---------------------------------------------------------------------------
# DB rebuild helpers
# ---------------------------------------------------------------------------

def _rebuild_db(py: str, root: Path) -> None:
    """Re-run load_sqlite and run_audit to refresh the database."""
    for script in ["audit/load_sqlite.py", "audit/run_audit.py"]:
        print(f"[single_audit] running {script} ...")
        rc = subprocess.run(
            [py, str(root / script)],
            cwd=str(root),
        ).returncode
        if rc != 0:
            print(f"[single_audit] ERROR: {script} exited with code {rc}", file=sys.stderr)
            sys.exit(rc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run a single correction-type audit slice from an existing DB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--type", required=True, metavar="TYPE",
        choices=sorted(_TYPES),
        help="Correction type: salary, status, hire_date, or job_org.",
    )
    parser.add_argument(
        "--db", default=None, metavar="PATH",
        help=f"SQLite database path (default: {DB_PATH}).",
    )
    parser.add_argument(
        "--out-dir", default=None, metavar="PATH",
        help="Output directory (default: audit/single_audit/<type>_<timestamp>/).",
    )
    parser.add_argument(
        "--rebuild-db", action="store_true", default=False,
        help="Re-run load_sqlite + run_audit before slicing.",
    )
    parser.add_argument(
        "--only-approved", action=argparse.BooleanOptionalAction, default=True,
        help="Include only APPROVE rows in corrections CSV (default: True).",
    )

    args   = parser.parse_args(argv)
    atype  = args.type
    db_path = Path(args.db) if args.db else DB_PATH

    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_out = ROOT / "audit" / "single_audit" / f"{atype}_{ts}"
    out_dir = Path(args.out_dir) if args.out_dir else default_out
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[single_audit] type={atype}  db={db_path.name}  out={out_dir}")

    # ------------------------------------------------------------------
    # Optional DB rebuild
    # ------------------------------------------------------------------
    if args.rebuild_db:
        _rebuild_db(sys.executable, ROOT)

    # ------------------------------------------------------------------
    # Validate DB
    # ------------------------------------------------------------------
    if not db_path.exists():
        print(
            f"[single_audit] ERROR: database not found: {db_path}\n"
            "Re-run the pipeline or use --rebuild-db to refresh it.",
            file=sys.stderr,
        )
        sys.exit(2)

    # ------------------------------------------------------------------
    # Load matched_pairs
    # ------------------------------------------------------------------
    con = sqlite3.connect(str(db_path))
    try:
        try:
            mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)
        except Exception as exc:
            print(f"[single_audit] ERROR: cannot query matched_pairs: {exc}", file=sys.stderr)
            sys.exit(2)
    finally:
        con.close()

    if "confidence" not in mp.columns:
        mp["confidence"] = None

    total_pairs = len(mp)
    print(f"[single_audit] loaded {total_pairs:,} pairs from {db_path.name}")

    # ------------------------------------------------------------------
    # Filter to rows with the requested mismatch type
    # ------------------------------------------------------------------
    records = mp.to_dict(orient="records")
    mismatch_rows = [r for r in records if _has_mismatch(r, atype)]
    n_mismatch = len(mismatch_rows)
    print(f"[single_audit] {n_mismatch:,} rows have {atype!r} mismatch")

    # ------------------------------------------------------------------
    # Apply gating and build output rows
    # ------------------------------------------------------------------
    ui_rows:          list[dict] = []
    review_rows:      list[dict] = []
    correction_rows:  list[dict] = []
    review_needed:    list[dict] = []

    n_approve = 0
    n_review  = 0

    for r in mismatch_rows:
        gate   = classify_row(r, atype)
        action = gate["action"]

        ui_row = _build_ui_row(r, atype)
        ui_rows.append(ui_row)

        if action == "APPROVE":
            n_approve += 1
            corr = _build_correction_row(r, atype, gate)
            if corr is not None:
                correction_rows.append(corr)
        else:
            n_review += 1
            review_rows.append(ui_row)
            if not args.only_approved:
                corr = _build_correction_row(r, atype, gate)
                if corr is not None:
                    correction_rows.append(corr)
            else:
                review_needed.append({
                    "pair_id":      r.get("pair_id", ""),
                    "old_worker_id": r.get("old_worker_id", ""),
                    "match_source": r.get("match_source", ""),
                    "reason":       gate.get("reason", ""),
                    "confidence":   r.get("confidence", ""),
                })

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------
    def _write_csv(rows: list[dict], cols: list[str], name: str) -> Path:
        path = out_dir / name
        df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
        df.to_csv(str(path), index=False)
        print(f"  wrote: {name} ({len(df):,} rows)")
        return path

    ui_cols = list(_build_ui_row(mismatch_rows[0], atype).keys()) if mismatch_rows else [
        "pair_id", "match_source", "old_worker_id", "new_worker_id", "audit_type",
        "fix_types", "action", "reason", "confidence", "min_confidence",
        "priority_score", "summary",
    ]
    review_cols = ui_cols

    print(f"[single_audit] writing outputs to {out_dir} ...")
    p_ui      = _write_csv(ui_rows, ui_cols, f"ui_pairs_{atype}.csv")
    p_review  = _write_csv(review_rows, review_cols, f"review_queue_{atype}.csv")
    p_corr    = _write_csv(correction_rows, _CORRECTION_COLS[atype], f"corrections_{atype}.csv")

    # Manifest
    manifest_rows = [
        {"file": p_ui.name,     "rows": len(ui_rows),         "description": f"All {atype} mismatch pairs with gating"},
        {"file": p_review.name, "rows": len(review_rows),      "description": f"REVIEW pairs (excluded from corrections)"},
        {"file": p_corr.name,   "rows": len(correction_rows),  "description": f"Workday-ready {atype} corrections"},
    ]
    p_manifest = _write_csv(manifest_rows, ["file", "rows", "description"], f"manifest_{atype}.csv")

    # Receipt
    receipt = {
        "run_ts":        ts,
        "audit_type":    atype,
        "db_path":       str(db_path),
        "out_dir":       str(out_dir),
        "total_pairs":   total_pairs,
        "mismatch_rows": n_mismatch,
        "gate": {
            "approve":      n_approve,
            "review":       n_review,
            "only_approved": args.only_approved,
        },
        "files": {
            "ui_pairs":      str(p_ui),
            "review_queue":  str(p_review),
            "corrections":   str(p_corr),
            "manifest":      str(p_manifest),
        },
    }
    p_receipt = out_dir / "receipt.json"
    p_receipt.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print(f"  wrote: receipt.json")

    print(f"\n[single_audit] complete.")
    print(f"  total pairs     : {total_pairs:,}")
    print(f"  {atype} mismatches : {n_mismatch:,}")
    print(f"  APPROVE         : {n_approve:,}  → corrections_{atype}.csv ({len(correction_rows):,} rows)")
    print(f"  REVIEW          : {n_review:,}  → review_queue_{atype}.csv")
    print(f"  out dir         : {out_dir}")


if __name__ == "__main__":
    main()
