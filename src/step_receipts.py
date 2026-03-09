"""
step_receipts.py — Write per-step JSON receipts into the run folder.

Public API
----------
write_receipt(run_dirs: dict, step_name: str, payload: dict) -> Path
    Writes runs/<run_id>/meta/receipts/<step_name>.json.

safe_stat(path) -> dict
    Returns {exists, size_bytes, mtime_utc} for a path.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path


def safe_stat(path) -> dict:
    """Return {exists, size_bytes, mtime_utc} for a file or directory path."""
    p = Path(path)
    if not p.exists():
        return {"exists": False, "size_bytes": None, "mtime_utc": None}
    try:
        st = p.stat()
        return {
            "exists":     True,
            "size_bytes": st.st_size,
            "mtime_utc":  datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
        }
    except Exception:
        return {"exists": True, "size_bytes": None, "mtime_utc": None}


def _csv_row_count(path) -> int | None:
    """Return number of data rows in a CSV (lines - 1 header). None if unreadable."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        with open(str(p), "r", encoding="utf-8", errors="replace") as f:
            lines = sum(1 for _ in f)
        return max(0, lines - 1)
    except Exception:
        return None


def file_info(path) -> dict:
    """Return safe_stat plus row_count for CSV files."""
    p = Path(path)
    info = safe_stat(p)
    if info["exists"] and str(p).lower().endswith(".csv"):
        info["row_count"] = _csv_row_count(p)
    info["path"] = str(p)
    return info


def write_receipt(run_dirs: dict, step_name: str, payload: dict) -> Path:
    """
    Write a step receipt JSON to runs/<run_id>/meta/receipts/<step_name>.json.

    Common fields added automatically:
        step            : step_name
        timestamp_utc   : ISO-8601 UTC timestamp
        ok              : True unless payload already sets it False

    Parameters
    ----------
    run_dirs  : dict returned by ensure_run_dirs()
    step_name : one of the canonical step names
    payload   : dict with inputs, outputs, warnings, elapsed_sec, etc.

    Returns the path written.
    """
    receipts_dir: Path = run_dirs["meta"] / "receipts"
    receipts_dir.mkdir(parents=True, exist_ok=True)

    receipt: dict = {
        "step":          step_name,
        "timestamp_utc": datetime.now(tz=timezone.utc).isoformat(),
        "ok":            payload.get("ok", True),
    }
    receipt.update(payload)

    out_path = receipts_dir / f"{step_name}.json"
    with open(str(out_path), "w", encoding="utf-8") as f:
        json.dump(receipt, f, indent=2, default=str)

    return out_path
