"""
audit/summary/build_audit_trail.py - Immutable audit trail log generator.

Creates audit_trail.json in the run output directory capturing:
  - run_id, timestamp, source file metadata, record counts
  - Per-row action decisions: pair_id, action, reason, fix_types, timestamp
  - Sanity gate result with all three sub-checks and their values
  - Any manual overrides applied (from wide_compare.csv decisions)

The file is written once and marked read-only after creation.
It should NEVER be modified after the run completes.

Usage:
  python audit/summary/build_audit_trail.py \\
         --run-id  <run_id> \\
         --wide    <wide_compare.csv> \\
         --gate    <sanity_gate.json>  \\
         [--old    <old_input_path>]   \\
         [--new    <new_input_path>]   \\
         --out     <audit_trail.json>
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utcnow() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _file_meta(path: Path | None) -> dict:
    """Return size, name for a source file (None-safe)."""
    if path is None or not path.exists():
        return {"path": str(path) if path else "", "size_bytes": None, "exists": False}
    st = path.stat()
    return {
        "path":        str(path),
        "name":        path.name,
        "size_bytes":  st.st_size,
        "exists":      True,
    }


def _parse_wide(wide_path: Path) -> tuple[list[dict], dict]:
    """
    Parse wide_compare.csv returning:
      (action_decisions, counts)

    action_decisions: list of {pair_id, action, reason, fix_types, conversion_type}
    counts: {total, approve, review, reject_match}
    """
    decisions: list[dict] = []
    counts = {"total": 0, "approve": 0, "review": 0, "reject_match": 0}

    if not wide_path.exists():
        return decisions, counts

    ts = _utcnow()
    with wide_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            counts["total"] += 1
            action = str(row.get("action") or "").strip().upper()
            if action == "APPROVE":
                counts["approve"] += 1
            elif action == "REVIEW":
                counts["review"] += 1
            elif action == "REJECT_MATCH":
                counts["reject_match"] += 1

            decisions.append({
                "pair_id":         str(row.get("pair_id") or ""),
                "action":          action,
                "reason":          str(row.get("reason") or ""),
                "fix_types":       str(row.get("fix_types") or ""),
                "match_source":    str(row.get("match_source") or ""),
                "confidence":      str(row.get("confidence") or ""),
                "conversion_type": str(row.get("conversion_type") or "") or None,
                "comp_band_status": str(row.get("comp_band_status") or "") or None,
                "name_change_detected": str(row.get("name_change_detected") or "") or None,
                "recorded_at":     ts,
            })

    return decisions, counts


def _parse_gate(gate_path: Path) -> dict:
    """Parse sanity_gate.json into a structured gate section."""
    if not gate_path.exists():
        return {"available": False}

    try:
        data = json.loads(gate_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"available": False, "error": str(exc)}

    hc = data.get("health_checks", {})
    sub_checks: dict[str, dict] = {}

    for key, info in hc.items():
        sub_checks[key] = {
            "value":     info.get("value"),
            "threshold": info.get("threshold"),
            "passed":    info.get("passed"),
        }

    return {
        "available":   True,
        "passed":      data.get("passed"),
        "reasons":     data.get("reasons", []),
        "sub_checks":  sub_checks,
        "evaluated_at": data.get("evaluated_at", _utcnow()),
    }


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_audit_trail(
    run_id:    str,
    wide_path: Path,
    gate_path: Path,
    old_path:  Path | None,
    new_path:  Path | None,
    out_path:  Path,
) -> None:
    """Build and write the immutable audit_trail.json."""
    created_at = _utcnow()

    decisions, counts = _parse_wide(wide_path)
    gate_section      = _parse_gate(gate_path)

    trail = {
        "_schema_version": _VERSION,
        "_note": (
            "This file is an immutable compliance record. "
            "Do not modify after run completion."
        ),
        "run_id":        run_id,
        "created_at":    created_at,
        "source_files":  {
            "old": _file_meta(old_path),
            "new": _file_meta(new_path),
        },
        "record_counts": counts,
        "sanity_gate":   gate_section,
        "action_decisions": decisions,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(trail, indent=2, ensure_ascii=False), encoding="utf-8")

    # Mark file read-only to signal immutability
    try:
        current_mode = os.stat(out_path).st_mode
        read_only    = current_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
        os.chmod(out_path, read_only)
    except Exception:
        pass  # non-fatal if chmod fails (e.g. Windows)

    size_kb = out_path.stat().st_size / 1024
    print(
        f"[audit_trail] wrote {out_path.name}  "
        f"({counts['total']:,} decisions, {size_kb:.1f} KB, read-only)"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build immutable audit trail log.")
    parser.add_argument("--run-id", required=True, help="Run identifier")
    parser.add_argument("--wide",   required=True, help="Path to wide_compare.csv")
    parser.add_argument("--gate",   default=None,  help="Path to sanity_gate.json")
    parser.add_argument("--old",    default=None,  help="Path to old input file (metadata only)")
    parser.add_argument("--new",    default=None,  help="Path to new input file (metadata only)")
    parser.add_argument("--out",    required=True, help="Output path for audit_trail.json")
    args = parser.parse_args(argv)

    build_audit_trail(
        run_id    = args.run_id,
        wide_path = Path(args.wide),
        gate_path = Path(args.gate) if args.gate else Path("__nonexistent__"),
        old_path  = Path(args.old)  if args.old  else None,
        new_path  = Path(args.new)  if args.new  else None,
        out_path  = Path(args.out),
    )


if __name__ == "__main__":
    main()
