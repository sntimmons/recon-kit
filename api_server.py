"""
api_server.py - Local API server for the Recon-Kit dashboard.

Run with:
    venv/Scripts/python.exe api_server.py

Listens on http://localhost:5001
Serves the static site from site/ and provides two API endpoints:
  POST /api/run/recon   - cross-system reconciliation (old + new CSV)
  POST /api/run/audit   - internal data audit (single CSV)
  GET  /api/status/<run_id>
  GET  /api/download/<run_id>/<filename>
"""

from __future__ import annotations

import json
import logging
import hashlib
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import csv
import zipfile
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory, abort
from werkzeug.exceptions import HTTPException

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
HERE        = Path(__file__).resolve().parent
SITE_DIR    = HERE / "site"
DATA_DIR    = Path(os.environ.get("RK_DATA_DIR", str(HERE)))
RUNS_DIR    = Path(os.environ.get("RK_RUNS_DIR", str(DATA_DIR / "dashboard_runs")))
JOBS_DB     = Path(os.environ.get("RK_JOBS_DB", str(DATA_DIR / "jobs.sqlite3")))
PYTHON      = sys.executable

DATA_DIR.mkdir(parents=True, exist_ok=True)
RUNS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Concurrency guard: only one full recon pipeline at a time.
# Concurrent internal-audit runs are fine (they write to isolated run dirs).
# ---------------------------------------------------------------------------
_RECON_SEMAPHORE = threading.BoundedSemaphore(1)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = Flask(__name__, static_folder=str(SITE_DIR), static_url_path="")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ValidationError(Exception):
    """User-facing validation error for uploaded files."""


def _warn_if_policy_unavailable() -> None:
    policy_path = HERE / "config" / "policy.yaml"
    try:
        from audit.summary.config_loader import load_policy
        load_policy(policy_path)
        if not policy_path.exists():
            logger.warning("policy.yaml could not be loaded at startup - using internal defaults")
    except Exception as exc:
        logger.warning(
            "policy.yaml could not be loaded at startup - using internal defaults (%s)",
            type(exc).__name__,
        )


_warn_if_policy_unavailable()

_job_store_lock = threading.Lock()


def _db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(JOBS_DB), timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def _ensure_job_store() -> None:
    JOBS_DB.parent.mkdir(parents=True, exist_ok=True)
    with _job_store_lock:
        with _db_connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                  run_id TEXT PRIMARY KEY,
                  job_type TEXT NOT NULL,
                  status TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  started REAL NOT NULL,
                  run_dir TEXT NOT NULL,
                  input_filenames_json TEXT NOT NULL DEFAULT '[]',
                  output_files_json TEXT NOT NULL DEFAULT '[]',
                  steps_json TEXT NOT NULL DEFAULT '[]',
                  stats_json TEXT NOT NULL DEFAULT '{}',
                  log_json TEXT NOT NULL DEFAULT '[]',
                  error_message TEXT,
                  gate_status TEXT,
                  gate_message TEXT
                );
                """
            )
            con.commit()


def _json_loads(value: str | None, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _job_status_for_api(status: str) -> str:
    return {
        "queued": "queued",
        "running": "running",
        "completed": "done",
        "failed": "error",
    }.get(status, status)


def _input_filenames_from_manifest(input_manifest: dict | None = None) -> list[str]:
    if not input_manifest:
        return []
    names: list[str] = []
    for value in input_manifest.values():
        if isinstance(value, dict):
            name = str(value.get("filename") or "").strip()
            if name:
                names.append(name)
    return names


def _create_job_record(
    run_id: str,
    *,
    job_type: str,
    run_dir: Path,
    input_filenames: list[str] | None = None,
) -> None:
    now = _utcnow_iso()
    with _job_store_lock:
        with _db_connect() as con:
            con.execute(
                """
                INSERT INTO jobs (
                  run_id, job_type, status, created_at, updated_at, started, run_dir,
                  input_filenames_json, output_files_json, steps_json, stats_json, log_json,
                  error_message, gate_status, gate_message
                ) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, '[]', '[]', '{}', '[]', NULL, NULL, NULL)
                """,
                (
                    run_id,
                    job_type,
                    now,
                    now,
                    time.time(),
                    str(run_dir),
                    json.dumps(input_filenames or []),
                ),
            )
            con.commit()


def _update_job_record(run_id: str, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = _utcnow_iso()
    assignments = ", ".join(f"{name} = ?" for name in fields)
    values = list(fields.values()) + [run_id]
    with _job_store_lock:
        with _db_connect() as con:
            cur = con.execute(f"UPDATE jobs SET {assignments} WHERE run_id = ?", values)
            con.commit()
            if cur.rowcount == 0:
                raise KeyError(run_id)


def _get_job_row(run_id: str) -> sqlite3.Row | None:
    with _job_store_lock:
        with _db_connect() as con:
            row = con.execute("SELECT * FROM jobs WHERE run_id = ?", (run_id,)).fetchone()
    return row


def _hydrate_job(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    run_dir = Path(row["run_dir"])
    outputs = _json_loads(row["output_files_json"], [])
    if run_dir.exists():
        live_outputs = _collect_outputs(run_dir)
        if live_outputs != outputs:
            outputs = live_outputs
            try:
                _update_job_record(run_id=row["run_id"], output_files_json=json.dumps(outputs))
            except Exception:
                pass
    return {
        "run_id": row["run_id"],
        "mode": row["job_type"],
        "job_type": row["job_type"],
        "status": _job_status_for_api(row["status"]),
        "job_status": row["status"],
        "steps": _json_loads(row["steps_json"], []),
        "outputs": outputs,
        "stats": _json_loads(row["stats_json"], {}),
        "error": row["error_message"],
        "started": row["started"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "input_filenames": _json_loads(row["input_filenames_json"], []),
        "gate_status": row["gate_status"],
        "gate_message": row["gate_message"],
        "run_dir": row["run_dir"],
        "log": _json_loads(row["log_json"], []),
    }


def _get_job(run_id: str) -> dict | None:
    return _hydrate_job(_get_job_row(run_id))


def _set_job_status(
    run_id: str,
    status: str,
    *,
    error: str | None = None,
    clear_error: bool = False,
) -> None:
    fields: dict[str, str | None] = {"status": status}
    if clear_error or error is not None:
        fields["error_message"] = error
    _update_job_record(run_id, **fields)


def _append_job_log(run_id: str, lines: list[str]) -> None:
    if not lines:
        return
    row = _get_job_row(run_id)
    if row is None:
        return
    log_lines = _json_loads(row["log_json"], [])
    log_lines.extend(lines)
    _update_job_record(run_id, log_json=json.dumps(log_lines))


def _set_step(run_id: str, step: str, status: str = "running"):
    job = _get_job(run_id)
    if not job:
        return
    steps = list(job.get("steps") or [])
    steps.append({"step": step, "status": status, "ts": time.time()})
    _update_job_record(run_id, steps_json=json.dumps(steps))


def _finish_step(run_id: str, step: str, status: str = "done"):
    job = _get_job(run_id)
    if not job:
        return
    steps = list(job.get("steps") or [])
    for entry in reversed(steps):
        if entry.get("step") == step and entry.get("status") == "running":
            entry["status"] = status
            entry["ts_end"] = time.time()
            break
    _update_job_record(run_id, steps_json=json.dumps(steps))


_ensure_job_store()


# ---------------------------------------------------------------------------
# Static site
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return send_from_directory(SITE_DIR, "index.html")


@app.route("/dashboard")
def dashboard():
    return send_from_directory(SITE_DIR, "dashboard.html")


@app.route("/<path:filename>")
def static_files(filename):
    # Serve any file from site/
    target = SITE_DIR / filename
    if target.is_file():
        return send_from_directory(SITE_DIR, filename)
    # Try adding .html
    if (SITE_DIR / (filename + ".html")).is_file():
        return send_from_directory(SITE_DIR, filename + ".html")
    abort(404)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_run_id() -> str:
    return uuid.uuid4().hex[:12]


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_policy() -> dict:
    try:
        from audit.summary.config_loader import load_policy
        return load_policy()
    except Exception as exc:
        logger.warning("policy.yaml unreadable - using internal defaults (%s)", type(exc).__name__)
        return {"retention": {"run_output_hours": 72}}


def _retention_hours(policy: dict | None = None) -> float:
    policy = policy or _load_policy()
    try:
        return float(policy.get("retention", {}).get("run_output_hours", 72) or 72)
    except Exception:
        return 72.0


def _is_audit_trail_only(run_dir: Path) -> bool:
    files = [p for p in run_dir.rglob("*") if p.is_file()]
    if not files:
        return False
    if len(files) != 1:
        return False
    return files[0].name == "audit_trail.json"


def _purge_run_dir_preserving_audit_trail(run_dir: Path) -> bool:
    trail = run_dir / "audit_trail.json"
    if trail.exists():
        removed_any = False
        for child in list(run_dir.iterdir()):
            if child == trail:
                continue
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
                removed_any = True
            except Exception:
                continue
        return removed_any
    try:
        shutil.rmtree(run_dir)
        return True
    except Exception:
        return False


def _cleanup_expired_runs(policy: dict | None = None) -> None:
    retention_hours = _retention_hours(policy)
    now_ts = time.time()
    for run_dir in RUNS_DIR.iterdir():
        if not run_dir.is_dir():
            continue
        try:
            created_ts = run_dir.stat().st_ctime
            child_times = []
            for child in run_dir.rglob("*"):
                try:
                    child_times.append(child.stat().st_mtime)
                except Exception:
                    continue
            if child_times:
                created_ts = min([created_ts, *child_times])
        except Exception:
            continue
        if not created_ts:
            continue
        age_hours = (now_ts - created_ts) / 3600.0
        if age_hours < retention_hours:
            continue
        if _is_audit_trail_only(run_dir):
            continue
        if _purge_run_dir_preserving_audit_trail(run_dir):
            try:
                with _job_store_lock:
                    with _db_connect() as con:
                        con.execute("DELETE FROM jobs WHERE run_id = ?", (run_dir.name,))
                        con.commit()
            except Exception:
                pass
            logger.info(
                "Run output %s deleted after %.1f hours (retention policy)",
                run_dir.name,
                age_hours,
            )


def _hash_file_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_upload(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _write_input_manifest(run_dir: Path, manifest: dict) -> Path:
    manifest_path = run_dir / "input_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path


def _delete_uploaded_source_files(run_id: str, paths: list[Path]) -> None:
    for path in paths:
        try:
            if path.exists():
                path.unlink()
        except Exception:
            continue
    logger.info("Uploaded source files deleted after processing - run %s", run_id)


def _sanitize_log_lines(output: str) -> list[str]:
    lines: list[str] = []
    skipping_traceback = False
    for raw_line in output.splitlines():
        line = raw_line.rstrip()
        if line.startswith("Traceback (most recent call last):"):
            if not skipping_traceback:
                lines.append("[internal traceback omitted]")
            skipping_traceback = True
            continue
        if skipping_traceback:
            if line.startswith("  File ") or line.startswith("    "):
                continue
            if ":" in line:
                skipping_traceback = False
                continue
            continue
        lines.append(line)
    return lines


def _validate_uploaded_source(path: Path, *, sheet_name: int | str = 0) -> None:
    from src.validator import validate_uploaded_file

    result = validate_uploaded_file(path, sheet_name=sheet_name)
    if not result.get("ok", False):
        raise ValidationError(result.get("error") or "The uploaded file could not be validated.")


def _extract_error(output: str) -> str:
    """Pull the most informative error line from subprocess stdout/stderr.

    Scans in reverse for lines containing common error keywords.
    Returns a ': <line>' suffix ready to append to a RuntimeError message,
    or empty string if nothing useful is found.
    """
    if not output:
        return ""
    lines = [ln.strip() for ln in output.splitlines() if ln.strip()]
    keywords = (
        "error:", "exception:", "traceback", "keyerror", "valueerror",
        "filenotfounderror", "missing", "column", "not found", "failed",
        "typeerror", "assertionerror",
    )
    for ln in reversed(lines):
        if any(kw in ln.lower() for kw in keywords):
            return ": " + ln[:300]
    return ": " + lines[-1][:300] if lines else ""


def _make_run_env(run_dir: Path) -> dict[str, str]:
    """Build an os.environ copy with per-run path overrides injected."""
    env = os.environ.copy()
    env["PYTHONUTF8"]  = "1"
    env["RK_WORK_DIR"] = str(run_dir)   # all pipeline scripts respect this
    return env


def _run_cmd(
    cmd: list[str],
    cwd: Path,
    run_id: str,
    env: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Run a subprocess, stream stdout/stderr into job log, return (rc, output)."""
    _env = os.environ.copy()
    _env["PYTHONUTF8"] = "1"
    if env:
        _env.update(env)
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=_env,
    )
    lines = []
    for line in proc.stdout:
        lines.append(line.rstrip())
    proc.wait()
    safe_lines = _sanitize_log_lines("\n".join(lines))
    _append_job_log(run_id, safe_lines)
    return proc.returncode, "\n".join(lines)


def _command_error_message(output: str, default: str) -> str:
    safe_lines = [line.strip() for line in _sanitize_log_lines(output) if line.strip()]
    if safe_lines:
        return safe_lines[-1]
    return default


# ---------------------------------------------------------------------------
# Download gating — shared between _collect_outputs and api_download.
# Any path that matches these rules is blocked from both the UI listing and
# the direct-download endpoint so they stay in sync automatically.
# ---------------------------------------------------------------------------
_DL_SKIP_NAMES: frozenset[str] = frozenset({
    "audit.db",
    "input_manifest.json",
    "matched_raw.csv",
    # Identity-review files that contain SSN last4 / DOB — internal use only.
    "needs_review_last4_conflicts.csv",
    "review_last4_pairs.csv",
    "review_candidates.csv",
})

_DL_SKIP_PREFIXES: tuple[str, ...] = (
    "old_input",
    "new_input",
    "audit_input",
    "audit_q",
)

_DL_SKIP_PARTS: frozenset[str] = frozenset({"inputs"})

_DL_SKIP_REL_PREFIXES: tuple[str, ...] = (
    "outputs/",
    "audit/summary/sanity_",
)


def _is_blocked_path(rel: str) -> bool:
    """Return True if a run-relative path should be blocked from download."""
    parts = rel.split("/")
    basename = parts[-1]
    if basename in _DL_SKIP_NAMES:
        return True
    if any(p in _DL_SKIP_PARTS for p in parts[:-1]):
        return True
    if any(basename.startswith(pfx) for pfx in _DL_SKIP_PREFIXES):
        return True
    if any(rel.startswith(pfx) for pfx in _DL_SKIP_REL_PREFIXES):
        return True
    return False


def _collect_outputs(run_dir: Path) -> list[dict]:
    """Scan a run directory for downloadable output files.

    Keep the well-known files first, but also pick up newly-added run outputs
    automatically so the dashboard does not need a backend allowlist update
    every time we add a new artifact.
    """
    wanted = [
        "Recon Audit Report.pdf",
        "details/Full Excel Workbook.xlsx",
        "Full Summary.csv",
        "Salary Mismatches.csv",
        "Job and Org Mismatches.csv",
        "Hire Date Mismatches.csv",
        "Status Mismatches.csv",
        "Employees Requiring Review.csv",
        "Rejected Matches.csv",
        "CHRO Summary Report.pdf",
        "details/Employees Missing from New System.csv",
        "details/Employees Missing from Old System.csv",
        "recon_outputs.zip",  # placeholder - real zip is recon_outputs_{run_id}.zip
        "support/Excel Lookup Keys for Validation.csv",
        "Clean Employee Data - Old System.csv",
        "Clean Employee Data - New System.csv",
        "recon_workbook.xlsx",
        "recon_report.pdf",
        "audit_report.docx",
        "audit_report.pdf",
        "chro_approval_document.pdf",
        "audit_trail.json",
        "wide_compare.csv",
        "unmatched_old.csv",
        "unmatched_new.csv",
        "review_queue.csv",
        "review_queue_summary.csv",
        "corrections_salary.csv",
        "corrections_status.csv",
        "corrections_hire_date.csv",
        "corrections_job_org.csv",
        "corrections_manifest.csv",
        "held_corrections.csv",
        "audit_report.csv",
        "sanity_results.json",
        "sanity_gate.json",
        "internal_audit_data.csv",
        "internal_audit_workbook.xlsx",
        "clean_data_ready_for_review.csv",
        "review_required_rows.csv",
        "correction_salary.csv",
        "correction_status.csv",
        "correction_dates.csv",
        "internal_audit_outputs.zip",
        "internal_audit_report.pdf",
        "internal_audit_duplicates.csv",
        "internal_audit_completeness.csv",
        "internal_audit_suspicious.csv",
        "internal_audit_distributions.csv",
        "fix_duplicates_full.csv",
        "fix_salary_full.csv",
        "fix_identity_full.csv",
        "fix_dates_full.csv",
        "fix_status_full.csv",
        "fix_data_quality_full.csv",
    ]
    found: list[dict] = []
    seen: set[str] = set()

    def _add_file(path: Path) -> None:
        if not path.exists() or not path.is_file():
            return
        if path.stat().st_size <= 0:
            return
        rel = path.relative_to(run_dir).as_posix()
        if rel in seen:
            return
        if _is_blocked_path(rel):
            return
        seen.add(rel)
        found.append({"name": rel, "size": path.stat().st_size})

    for name in wanted:
        _add_file(run_dir / name)

    for path in sorted(run_dir.rglob("*")):
        _add_file(path)

    return found


def _csv_row_count(path: Path) -> int:
    if not path.exists() or not path.is_file():
        return 0
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return max(sum(1 for _ in handle) - 1, 0)
    except Exception:
        return 0


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists() or not path.is_file():
        return [], []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = list(reader.fieldnames or [])
            rows = [{key: (value if value is not None else "") for key, value in row.items()} for row in reader]
        return fieldnames, rows
    except Exception:
        return [], []


def _write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def _copy_file(src: Path, dest: Path) -> None:
    if not src.exists() or not src.is_file():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)


def _move_file(src: Path, dest: Path) -> None:
    if not src.exists() or not src.is_file():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    if src.resolve() == dest.resolve():
        return
    if dest.exists():
        dest.unlink()
    shutil.move(str(src), str(dest))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _friendly_issue_types(fix_types_raw: str) -> str:
    labels = []
    seen = set()
    mapping = {
        "salary": "Salary",
        "payrate": "Salary",
        "status": "Employee Status",
        "hire_date": "Hire Date",
        "job_org": "Job and Organization",
        "identity": "Identity",
    }
    for part in str(fix_types_raw or "").split("|"):
        key = part.strip().lower()
        if not key:
            continue
        label = mapping.get(key, key.replace("_", " ").title())
        if label not in seen:
            seen.add(label)
            labels.append(label)
    return ", ".join(labels)


def _review_severity(priority_score_raw: str) -> str:
    try:
        score = int(float(str(priority_score_raw or "0")))
    except Exception:
        score = 0
    if score >= 90:
        return "Critical"
    if score >= 60:
        return "High"
    if score >= 30:
        return "Medium"
    return "Low"


def _review_department(row: dict[str, str]) -> str:
    return (
        row.get("old_district")
        or row.get("new_district")
        or row.get("old_department")
        or row.get("new_department")
        or "Unassigned"
    )


def _friendly_review_row(row: dict[str, str], wide_row: dict[str, str] | None = None) -> dict[str, str]:
    friendly = dict(row)
    issue_type = _friendly_issue_types(row.get("fix_types", ""))
    department = _review_department(row)
    friendly["employee_name"] = row.get("old_full_name_norm") or (wide_row or {}).get("new_full_name_norm", "")
    friendly["issue_type"] = issue_type or "Review Required"
    friendly["severity"] = _review_severity(row.get("priority_score", "0"))
    friendly["department"] = department
    friendly["what_changed"] = row.get("summary", "")
    friendly["recommended_action"] = row.get("match_explanation", "") or row.get("reason", "")
    if wide_row and str(wide_row.get("match_source", "")).strip().lower() != "worker_id":
        friendly["issue_type"] = (
            friendly["issue_type"] + ", Identity"
            if friendly["issue_type"] and "Identity" not in friendly["issue_type"]
            else "Identity"
        )
    return friendly


def _has_fix_type(row: dict[str, str], target: str) -> bool:
    parts = {part.strip().lower() for part in str(row.get("fix_types", "")).split("|") if part.strip()}
    if target == "salary":
        return "salary" in parts or "payrate" in parts
    return target in parts


def _build_identity_rows(
    review_rows: list[dict[str, str]],
    wide_lookup: dict[str, dict[str, str]],
    run_dir: Path,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str]] = set()

    for row in review_rows:
        wide_row = wide_lookup.get(row.get("pair_id", ""))
        if not wide_row:
            continue
        match_source = str(wide_row.get("match_source", "")).strip().lower()
        name_change = str(wide_row.get("name_change_detected", "")).strip().lower() == "true"
        if match_source == "worker_id" and not name_change:
            continue
        friendly = _friendly_review_row(row, wide_row)
        friendly["issue_type"] = "Identity"
        key = (
            friendly.get("old_worker_id", ""),
            friendly.get("new_worker_id", ""),
            friendly.get("employee_name", ""),
        )
        if key not in seen_keys:
            seen_keys.add(key)
            rows.append(friendly)

    for rel in (
        "outputs/conflicts_old_worker_id_resolution.csv",
        "outputs/conflicts_new_worker_id_resolution.csv",
    ):
        _, conflict_rows = _read_csv_rows(run_dir / rel)
        for row in conflict_rows:
            friendly = dict(row)
            friendly["employee_name"] = row.get("old_full_name_norm") or row.get("new_full_name_norm", "")
            friendly["issue_type"] = "Identity"
            friendly["severity"] = "High"
            friendly["department"] = row.get("old_district") or row.get("new_district") or "Unassigned"
            friendly["what_changed"] = "Worker ID conflict requires identity review."
            friendly["recommended_action"] = "Confirm the correct employee identity before loading corrections."
            key = (
                friendly.get("old_worker_id", ""),
                friendly.get("new_worker_id", ""),
                friendly.get("employee_name", ""),
            )
            if key not in seen_keys:
                seen_keys.add(key)
                rows.append(friendly)

    return rows


def _combine_audit_details(run_dir: Path) -> None:
    audit_dir = run_dir / "audit"
    if not audit_dir.exists():
        return
    source_files = sorted(p for p in audit_dir.glob("audit_q*.csv") if p.is_file())
    if not source_files:
        return

    combined_rows: list[dict[str, str]] = []
    combined_fields = ["audit_topic", "source_file"]
    field_set = set(combined_fields)
    topic_map = {
        "q0": "Duplicate Worker IDs",
        "q1": "Match Source Summary",
        "q2": "Salary Issues",
        "q3": "Status Issues",
        "q4": "Job and Organization Issues",
        "q5": "Hire Date Issues",
        "q16": "Hire Date Wave Analysis",
        "q17": "Salary Anomaly Analysis",
    }

    for path in source_files:
        fieldnames, rows = _read_csv_rows(path)
        stem = path.stem.lower()
        token = next((part for part in stem.split("_") if part.startswith("q")), "details")
        topic = topic_map.get(token, path.stem.replace("_", " ").title())
        for name in fieldnames:
            if name not in field_set:
                field_set.add(name)
                combined_fields.append(name)
        for row in rows:
            combined = {"audit_topic": topic, "source_file": path.name}
            combined.update(row)
            combined_rows.append(combined)

    _write_csv_rows(audit_dir / "System Audit Details.csv", combined_fields, combined_rows)


def _package_recon_outputs(run_id: str, run_dir: Path, stats: dict) -> None:
    details_dir = run_dir / "details"
    support_dir = run_dir / "support"
    logs_dir = run_dir / "logs"
    audit_dir = run_dir / "audit"
    details_dir.mkdir(parents=True, exist_ok=True)
    support_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    wide_fields, wide_rows = _read_csv_rows(run_dir / "wide_compare.csv")
    wide_lookup = {row.get("pair_id", ""): row for row in wide_rows if row.get("pair_id")}
    review_fields, review_rows = _read_csv_rows(run_dir / "review_queue.csv")

    friendly_review_rows = [_friendly_review_row(row, wide_lookup.get(row.get("pair_id", ""))) for row in review_rows]
    review_fieldnames = [
        "employee_name",
        "issue_type",
        "severity",
        "department",
        "what_changed",
        "recommended_action",
    ]
    for name in review_fields:
        if name not in review_fieldnames:
            review_fieldnames.append(name)
    _write_csv_rows(run_dir / "Employees Requiring Review.csv", review_fieldnames, friendly_review_rows)

    salary_rows = [row for row in friendly_review_rows if _has_fix_type(row, "salary")]
    status_rows = [row for row in friendly_review_rows if _has_fix_type(row, "status")]
    hire_date_rows = [row for row in friendly_review_rows if _has_fix_type(row, "hire_date")]
    job_org_rows = [row for row in friendly_review_rows if _has_fix_type(row, "job_org")]
    identity_rows = _build_identity_rows(review_rows, wide_lookup, run_dir)
    identity_fieldnames = list(review_fieldnames)
    for row in identity_rows:
        for name in row:
            if name not in identity_fieldnames:
                identity_fieldnames.append(name)

    _write_csv_rows(run_dir / "Salary Issues to Fix.csv", review_fieldnames, salary_rows)
    _write_csv_rows(run_dir / "Employee Status Issues.csv", review_fieldnames, status_rows)
    _write_csv_rows(run_dir / "Hire Date Issues.csv", review_fieldnames, hire_date_rows)
    _write_csv_rows(run_dir / "Job and Organization Issues to Review.csv", review_fieldnames, job_org_rows)
    _write_csv_rows(run_dir / "Identity Conflicts to Review.csv", identity_fieldnames, identity_rows)

    # Aliases for mismatch-focused CSV names
    def _copy_if_exists(src: Path, dest: Path) -> None:
        if src.exists() and src.stat().st_size > 0:
            _copy_file(src, dest)

    _copy_if_exists(run_dir / "Salary Issues to Fix.csv", run_dir / "Salary Mismatches.csv")
    _copy_if_exists(run_dir / "Job and Organization Issues to Review.csv", run_dir / "Job and Org Mismatches.csv")
    _copy_if_exists(run_dir / "Hire Date Issues.csv", run_dir / "Hire Date Mismatches.csv")
    _copy_if_exists(run_dir / "Employee Status Issues.csv", run_dir / "Status Mismatches.csv")

    # Rejected matches (if any) from wide_compare
    rejected_rows = [row for row in wide_rows if str(row.get("action", "")).strip().upper() == "REJECT_MATCH"]
    if rejected_rows:
        rej_fields = ["pair_id", "old_worker_id", "new_worker_id", "match_source", "reason", "summary"]
        for name in wide_fields:
            if name not in rej_fields:
                rej_fields.append(name)
        _write_csv_rows(run_dir / "Rejected Matches.csv", rej_fields, rejected_rows)

    summary_row = {
        "run_id": run_id,
        "total_matched_pairs": stats.get("total_pairs", stats.get("total_matched", len(wide_rows))),
        "auto_approved_pairs": stats.get("approve_count", max(len(wide_rows) - len(review_rows), 0)),
        "review_required_pairs": len(review_rows),
        "unmatched_from_old_system": _csv_row_count(run_dir / "unmatched_old.csv"),
        "unmatched_from_new_system": _csv_row_count(run_dir / "unmatched_new.csv"),
        "total_unmatched": _csv_row_count(run_dir / "unmatched_old.csv") + _csv_row_count(run_dir / "unmatched_new.csv"),
        "salary_issues": len(salary_rows),
        "employee_status_issues": len(status_rows),
        "hire_date_issues": len(hire_date_rows),
        "job_and_organization_issues": len(job_org_rows),
        "identity_conflicts": len(identity_rows),
        "gate_passed": "Yes" if stats.get("gate_passed", True) else "No",
        "gate_reason": " | ".join(stats.get("gate_reasons", [])),
    }
    _write_csv_rows(run_dir / "Full Summary.csv", list(summary_row.keys()), [summary_row])

    preferred_pdf = run_dir / "recon_report.pdf"
    fallback_pdf = run_dir / "audit_report.pdf"
    report_source = preferred_pdf if preferred_pdf.exists() else fallback_pdf
    _copy_file(report_source, run_dir / "Recon Audit Report.pdf")
    _move_file(run_dir / "recon_report.pdf", details_dir / "Technical Recon Audit Report.pdf")
    _copy_file(run_dir / "outputs" / "mapped_old.csv", run_dir / "Clean Employee Data - Old System.csv")
    _copy_file(run_dir / "outputs" / "mapped_new.csv", run_dir / "Clean Employee Data - New System.csv")

    _move_file(run_dir / "input_manifest.json", logs_dir / "Input Manifest.json")

    if (run_dir / "review_queue.csv").exists():
        _move_file(run_dir / "review_queue.csv", details_dir / "Technical Review Queue.csv")
    for path in run_dir.glob("review_queue_*.csv"):
        if path.name != "review_queue.csv":
            path.unlink(missing_ok=True)
    (run_dir / "review_queue_summary.csv").unlink(missing_ok=True)

    _move_file(run_dir / "wide_compare.csv", details_dir / "Full Employee Comparison (Side by Side).csv")
    _move_file(run_dir / "unmatched_old.csv", details_dir / "Employees Missing from New System.csv")
    _move_file(run_dir / "unmatched_new.csv", details_dir / "Employees Missing from Old System.csv")
    _move_file(run_dir / "recon_workbook.xlsx", details_dir / "Full Excel Workbook.xlsx")
    _move_file(run_dir / "audit_report.docx", details_dir / "Legacy Audit Report.docx")
    _move_file(run_dir / "audit_report.pdf", details_dir / "Legacy Audit Report.pdf")

    _move_file(run_dir / "xlookup_keys.csv", support_dir / "Excel Lookup Keys for Validation.csv")
    if (run_dir / "chro_approval_document.pdf").exists():
        _copy_file(run_dir / "chro_approval_document.pdf", run_dir / "CHRO Summary Report.pdf")
        _move_file(run_dir / "chro_approval_document.pdf", support_dir / "CHRO Approval Document.pdf")
    _move_file(run_dir / "corrections_manifest.csv", support_dir / "Corrections Manifest.csv")
    _move_file(run_dir / "held_corrections.csv", support_dir / "Held Corrections.csv")
    _move_file(run_dir / "corrections_salary.csv", support_dir / "Salary Corrections for Loading.csv")
    _move_file(run_dir / "corrections_status.csv", support_dir / "Status Corrections for Loading.csv")
    _move_file(run_dir / "corrections_hire_date.csv", support_dir / "Hire Date Corrections for Loading.csv")
    _move_file(run_dir / "corrections_job_org.csv", support_dir / "Job and Organization Corrections for Loading.csv")

    mapping_summary = {}
    old_map = run_dir / "outputs" / "mapping_report_mapped_old.json"
    new_map = run_dir / "outputs" / "mapping_report_mapped_new.json"
    if old_map.exists():
        mapping_summary["old_system"] = json.loads(old_map.read_text(encoding="utf-8"))
    if new_map.exists():
        mapping_summary["new_system"] = json.loads(new_map.read_text(encoding="utf-8"))
    if mapping_summary:
        _write_json(logs_dir / "Data Mapping Summary.json", mapping_summary)

    match_report = run_dir / "outputs" / "match_report.json"
    if match_report.exists():
        _write_json(logs_dir / "Matching Summary.json", json.loads(match_report.read_text(encoding="utf-8")))

    for src, dest_name in (
        (run_dir / "sanity_results.json", "Data Quality Check Results.json"),
        (run_dir / "sanity_gate.json", "Data Safety Check Results.json"),
        (run_dir / "audit_trail.json", "Audit Trail.json"),
    ):
        if src.exists():
            _move_file(src, logs_dir / dest_name)

    _move_file(run_dir / "outputs" / "skipped_missing_entity_keys.csv", logs_dir / "Skipped Rows Missing Entity Keys.csv")
    _move_file(run_dir / "audit" / "summary" / "sanity_salary_buckets.csv", logs_dir / "Salary Quality Detail.csv")
    _move_file(run_dir / "audit" / "summary" / "sanity_hire_date_diff.csv", logs_dir / "Hire Date Check Detail.csv")
    _move_file(run_dir / "audit" / "summary" / "sanity_suspicious_defaults.csv", logs_dir / "Suspicious Default Values Detail.csv")

    _combine_audit_details(run_dir)

    # Complete package ZIP
    zip_items = [
        "Recon Audit Report.pdf",
        "CHRO Summary Report.pdf",
        "details/Full Excel Workbook.xlsx",
        "Full Summary.csv",
        "Salary Mismatches.csv",
        "Job and Org Mismatches.csv",
        "Hire Date Mismatches.csv",
        "Status Mismatches.csv",
        "Employees Requiring Review.csv",
        "Rejected Matches.csv",
        "details/Employees Missing from New System.csv",
        "details/Employees Missing from Old System.csv",
    ]
    zip_path = run_dir / f"recon_outputs_{run_id}.zip"
    added = 0
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel in zip_items:
            p = run_dir / rel
            if p.exists() and p.is_file() and p.stat().st_size > 0:
                zf.write(p, arcname=rel)
                added += 1
    if added == 0 and zip_path.exists():
        zip_path.unlink(missing_ok=True)


def _parse_run_stats(run_dir: Path) -> dict:
    """Extract key stats from the completed run folder."""
    stats = {}

    # Try audit manifest / summary JSON first
    for fname in ["run_manifest.json", "sanity_results.json", "sanity_gate.json"]:
        p = run_dir / fname
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                stats.update(data)
            except Exception:
                pass

    # Try matched_raw.csv row count
    matched = run_dir / "matched_raw.csv"
    if not matched.exists():
        matched = HERE / "outputs" / "matched_raw.csv"
    if matched.exists():
        try:
            import csv as _csv
            with matched.open(encoding="utf-8") as f:
                reader = _csv.reader(f)
                next(reader)  # header
                rows = sum(1 for _ in reader)
            stats["total_matched"] = rows
        except Exception:
            pass

    # Try review_queue
    rq = run_dir / "review_queue.csv"
    if rq.exists():
        try:
            import csv as _csv
            with rq.open(encoding="utf-8") as f:
                reader = _csv.reader(f)
                next(reader)
                rows = sum(1 for _ in reader)
            stats["review_count"] = rows
        except Exception:
            pass

    # Gate status + health metrics from sanity_gate.json
    gate_file = run_dir / "sanity_gate.json"
    if gate_file.exists():
        try:
            gate = json.loads(gate_file.read_text(encoding="utf-8"))
            stats["gate_passed"] = gate.get("passed", True)
            stats["gate_reasons"] = gate.get("reasons", [])
            # Extract det_match_rate for the DET. MATCH RATE dashboard card
            hc     = gate.get("health_checks", {})
            dr_val = hc.get("det_rate", {}).get("value")
            if dr_val is not None:
                stats["det_match_rate"] = f"{float(dr_val) * 100:.1f}%"
                stats["det_match_rate_raw"] = float(dr_val)
            # approve_rate from health checks
            ar_val = hc.get("approve_rate", {}).get("value")
            if ar_val is not None and ar_val != "not_computed":
                try:
                    stats["approve_rate_pct"] = f"{float(ar_val) * 100:.1f}%"
                except (ValueError, TypeError):
                    pass
            # active_zero check
            az = hc.get("active_zero_salary", {})
            if az:
                stats["active_zero_approved"] = az.get("value_approved", az.get("value", 0))
        except Exception:
            pass

    # Approve count for AUTO-APPROVED dashboard card (sanity_results.json health_metrics)
    results_file = run_dir / "sanity_results.json"
    if results_file.exists():
        try:
            rdata = json.loads(results_file.read_text(encoding="utf-8"))
            hm    = rdata.get("health_metrics", {})
            if "approve_count" in hm:
                stats["approve_count"] = int(hm["approve_count"])
        except Exception:
            pass

    # Count REJECT_MATCH rows from wide_compare.csv
    wide = run_dir / "wide_compare.csv"
    if wide.exists():
        try:
            import csv as _csv
            n_reject = 0
            with wide.open(encoding="utf-8") as f:
                reader = _csv.DictReader(f)
                for row in reader:
                    if row.get("action") == "REJECT_MATCH":
                        n_reject += 1
            stats["reject_count"] = n_reject
        except Exception:
            pass

    # Per-fix correction counts from individual correction CSVs
    fix_counts: dict[str, int] = {}
    for fix_name in ("salary", "status", "hire_date", "job_org"):
        p = run_dir / f"corrections_{fix_name}.csv"
        if p.exists() and p.stat().st_size > 0:
            try:
                import csv as _csv
                with p.open(encoding="utf-8") as f:
                    reader = _csv.reader(f)
                    next(reader)
                    n = sum(1 for _ in reader)
                if n > 0:
                    fix_counts[fix_name] = n
            except Exception:
                pass
    if fix_counts:
        stats["fix_counts"] = fix_counts
        stats["total_corrections"] = sum(fix_counts.values())

    return stats


# ---------------------------------------------------------------------------
# Recon pipeline runner (background thread)
# ---------------------------------------------------------------------------
def _run_recon_pipeline(run_id: str, run_dir: Path, old_path: Path, new_path: Path, options: dict):
    """Run the full cross-system reconciliation pipeline.

    Per-run isolation strategy
    --------------------------
    Every subprocess inherits RK_WORK_DIR=run_dir so that all pipeline
    scripts write their outputs into the per-run tree rather than global
    paths.  The in-run directory layout mirrors the repo structure:

        run_dir/
          inputs/                    ← uploaded CSVs
          outputs/                   ← mapped_*.csv, matched_raw.csv
          audit/
            audit.db                 ← per-run SQLite database
            summary/                 ← sanity JSON, review_queue.csv
            corrections/out/         ← correction CSVs, held_corrections.csv
          wide_compare.csv           ← from build_diy_exports
          recon_workbook.xlsx        ← from build_workbook
          audit_report.docx          ← from generate_report
    """
    run_env = _make_run_env(run_dir)

    # Derive per-run sub-paths (scripts also compute these from RK_WORK_DIR)
    run_inputs  = run_dir / "inputs"
    run_outputs = run_dir / "outputs"
    run_audit   = run_dir / "audit"
    run_summary = run_audit / "summary"
    run_corr    = run_audit / "corrections" / "out"
    run_db      = run_audit / "audit.db"
    uploaded_paths = [Path(p) for p in options.get("uploaded_paths", [])]
    input_manifest_path = options.get("input_manifest_path")
    run_start_timestamp = options.get("run_start_timestamp", _utcnow_iso())

    try:
        _RECON_SEMAPHORE.acquire()
        _set_job_status(run_id, "running", clear_error=True)

        # Create the per-run directory structure up front
        for d in (run_inputs, run_outputs, run_summary, run_corr):
            d.mkdir(parents=True, exist_ok=True)

        # Resolve file-type options
        old_ext    = options.get("old_ext", ".csv")
        new_ext    = options.get("new_ext", ".csv")
        sheet_name = options.get("sheet_name", 0)

        # 1. Copy input files into per-run inputs/ (preserve original extension)
        _set_step(run_id, "upload")
        shutil.copy2(old_path, run_inputs / f"old{old_ext}")
        shutil.copy2(new_path, run_inputs / f"new{new_ext}")
        _finish_step(run_id, "upload")

        # 2. Mapping - use absolute run_dir paths so no global outputs/ is touched
        _set_step(run_id, "mapping")
        old_in  = run_inputs  / f"old{old_ext}"
        new_in  = run_inputs  / f"new{new_ext}"
        old_out = run_outputs / "mapped_old.csv"
        new_out = run_outputs / "mapped_new.csv"
        rc, output = _run_cmd(
            [str(PYTHON), "-c",
             f"from src.mapping import map_file; "
             f"map_file(r'{old_in}', r'{old_out}', 'old', sheet_name={sheet_name!r}); "
             f"map_file(r'{new_in}', r'{new_out}', 'new', sheet_name={sheet_name!r})"],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "mapping", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError(_command_error_message(output, "Mapping step failed"))

        # 3. Matching - RK_WORK_DIR tells matcher.py where to write matched_raw.csv
        _set_step(run_id, "matching")
        rc, output = _run_cmd([str(PYTHON), "src/matcher.py"], HERE, run_id, env=run_env)
        _finish_step(run_id, "matching", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError(_command_error_message(output, "Matcher step failed"))

        # 4. Resolve conflicts - RK_WORK_DIR tells resolve where matched_raw.csv lives
        _set_step(run_id, "resolve")
        rc, output = _run_cmd([str(PYTHON), "resolve_matched_raw.py"], HERE, run_id, env=run_env)
        _finish_step(run_id, "resolve", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError(_command_error_message(output, "Resolve step failed"))

        # 5. Load SQLite - RK_WORK_DIR tells load_sqlite where to place audit.db
        _set_step(run_id, "load_db")
        rc, output = _run_cmd([str(PYTHON), "audit/load_sqlite.py"], HERE, run_id, env=run_env)
        _finish_step(run_id, "load_db", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError(_command_error_message(output, "DB load step failed"))

        # 6. Run audit queries - RK_WORK_DIR tells run_audit.py which DB to use
        _set_step(run_id, "audit")
        rc, output = _run_cmd([str(PYTHON), "audit/run_audit.py"], HERE, run_id, env=run_env)
        _finish_step(run_id, "audit", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError(_command_error_message(output, "Audit step failed"))

        # 7. Optional: sanity gate - write JSON to per-run summary dir
        _gate_blocked = False
        if options.get("sanity_gate", True):
            _set_step(run_id, "sanity_gate")
            gate_cmd = [str(PYTHON), "audit/summary/run_sanity_gate.py",
                        "--db",  str(run_db),
                        "--out", str(run_summary),
                        "--min-approve-rate", str(options.get("min_approve_rate", 0.75))]
            rc, _ = _run_cmd(gate_cmd, HERE, run_id, env=run_env)
            _finish_step(run_id, "sanity_gate", "done" if rc == 0 else "warn")
            # Read gate result to decide whether corrections are blocked
            _gate_json = run_summary / "sanity_gate.json"
            if _gate_json.exists():
                try:
                    import json as _json
                    _gd = _json.loads(_gate_json.read_text())
                    _gate_blocked = not _gd.get("passed", True)
                except Exception as e:
                    logger.warning(
                        "sanity_gate.json unreadable - treating as gate FAIL: %s",
                        type(e).__name__,
                    )
                    _gate_blocked = True

        # 8. Optional: generate corrections - skipped when gate fails
        if options.get("corrections", True) and not _gate_blocked:
            _set_step(run_id, "corrections")
            rc, _ = _run_cmd(
                [str(PYTHON), "audit/corrections/generate_corrections.py",
                 "--db",      str(run_db),
                 "--out-dir", str(run_corr)],
                HERE, run_id, env=run_env,
            )
            _finish_step(run_id, "corrections", "done" if rc == 0 else "warn")
        elif _gate_blocked:
            _finish_step(run_id, "corrections", "blocked")

        # 9. DIY exports - writes wide_compare.csv directly to run_dir
        _set_step(run_id, "exports")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/exports/build_diy_exports.py",
             "--db",     str(run_db),
             "--out-dir", str(run_dir)],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "exports", "done" if rc == 0 else "warn")

        # 9.5 Optional: compensation band validation
        bands_path_str = options.get("bands_path")
        if bands_path_str and Path(bands_path_str).exists():
            _set_step(run_id, "comp_bands")
            rc, _ = _run_cmd(
                [str(PYTHON), "audit/summary/comp_band_validator.py",
                 "--wide",  str(run_dir / "wide_compare.csv"),
                 "--bands", bands_path_str],
                HERE, run_id, env=run_env,
            )
            _finish_step(run_id, "comp_bands", "done" if rc == 0 else "warn")

        # 10. Optional: Excel workbook - reads wide_compare.csv from run_dir
        if options.get("workbook", True):
            _set_step(run_id, "workbook")
            _wb_cmd = [str(PYTHON), "audit/summary/build_workbook.py",
                       "--out",      str(run_dir / "recon_workbook.xlsx"),
                       "--wide",     str(run_dir / "wide_compare.csv"),
                       "--db",       str(run_db),
                       "--manifest", str(run_corr / "corrections_manifest.csv")]
            if _gate_blocked:
                _wb_cmd.append("--gate-blocked")
            rc, _ = _run_cmd(_wb_cmd, HERE, run_id, env=run_env)
            _finish_step(run_id, "workbook", "done" if rc == 0 else "warn")

        # 11. Review queue - pass per-run DB and output path
        _set_step(run_id, "review_queue")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/summary/build_review_queue.py",
             "--db",  str(run_db),
             "--out", str(run_dir / "review_queue.csv")],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "review_queue", "done" if rc == 0 else "warn")

        # 11.5 Split review queue by department
        _set_step(run_id, "split_rq")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/summary/split_review_queue.py",
             "--rq",  str(run_dir / "review_queue.csv"),
             "--out", str(run_dir)],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "split_rq", "done" if rc == 0 else "warn")

        # 12. Audit report (.docx) - fully isolated, writes directly to run_dir
        _set_step(run_id, "audit_report")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/reports/generate_report.py",
             "--db",   str(run_db),
             "--wide", str(run_dir / "wide_compare.csv"),
             "--out",  str(run_dir / "audit_report.docx")],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "audit_report", "done" if rc == 0 else "warn")

        # 12.5 PDF version of audit report
        _set_step(run_id, "audit_pdf")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/reports/generate_pdf.py",
             "--docx", str(run_dir / "audit_report.docx"),
             "--out",  str(run_dir / "audit_report.pdf")],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "audit_pdf", "done" if rc == 0 else "warn")  # non-fatal

        # 13. Promote sub-dir outputs to run_dir root for easy download
        _promote_run_outputs(run_dir)

        # 13.5 Reconciliation Audit PDF (ReportLab - visual redesign)
        _set_step(run_id, "recon_pdf")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/reports/build_recon_report.py",
             "--run-id",  run_id,
             "--wide",    str(run_dir / "wide_compare.csv"),
             "--held",    str(run_dir / "held_corrections.csv"),
             "--uo",      str(run_dir / "unmatched_old.csv"),
             "--un",      str(run_dir / "unmatched_new.csv"),
             "--manifest",str(run_dir / "corrections_manifest.csv"),
             "--review",  str(run_dir / "review_queue.csv"),
             "--out",     str(run_dir / "recon_report.pdf")],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "recon_pdf", "done" if rc == 0 else "warn")  # non-fatal

        # 14. Build immutable audit trail log
        _set_step(run_id, "audit_trail")
        run_complete_timestamp = _utcnow_iso()
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/summary/build_audit_trail.py",
             "--run-id", run_id,
             "--wide",   str(run_dir / "wide_compare.csv"),
             "--gate",   str(run_dir / "sanity_gate.json"),
             "--old",    str(old_path),
             "--new",    str(new_path),
             "--run-start-ts", run_start_timestamp,
             "--run-complete-ts", run_complete_timestamp,
             "--inputs-manifest", str(input_manifest_path) if input_manifest_path else "",
             "--out",    str(run_dir / "audit_trail.json"),
            ],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "audit_trail", "done" if rc == 0 else "warn")

        # 15. CHRO approval document - requires audit_trail.json and gate outputs
        _set_step(run_id, "chro_approval")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/reports/build_chro_approval.py",
             "--run-id", run_id,
             "--run-dir", str(run_dir),
             "--out", str(run_dir / "chro_approval_document.pdf")],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "chro_approval", "done" if rc == 0 else "warn")

        # Parse stats
        stats = _parse_run_stats(run_dir)
        _package_recon_outputs(run_id, run_dir, stats)
        outputs = _collect_outputs(run_dir)
        _update_job_record(
            run_id,
            status="completed",
            output_files_json=json.dumps(outputs),
            stats_json=json.dumps(stats),
            error_message=None,
        )

    except Exception as exc:
        logger.exception("Pipeline step failed: %s - %s", type(exc).__name__, str(exc))
        _set_job_status(
            run_id,
            "failed",
            error=str(exc) or "Processing failed. Please try again or contact support.",
        )

    finally:
        _delete_uploaded_source_files(run_id, uploaded_paths)
        _RECON_SEMAPHORE.release()


def _promote_run_outputs(run_dir: Path) -> None:
    """Promote per-run sub-directory outputs to run_dir root for easy download.

    All pipeline steps now write into run_dir sub-directories (outputs/,
    audit/summary/, audit/corrections/out/) via RK_WORK_DIR isolation.
    This function copies the user-facing files to the flat run_dir root so
    the download list can find them without knowing the internal layout.
    """
    run_audit   = run_dir / "audit"
    run_summary = run_audit / "summary"
    run_corr    = run_audit / "corrections" / "out"
    run_outputs = run_dir / "outputs"

    flat_map = {
        # sanity outputs
        run_summary / "sanity_results.json":           run_dir / "sanity_results.json",
        run_summary / "sanity_gate.json":              run_dir / "sanity_gate.json",
        # corrections (build_review_queue writes directly to run_dir/review_queue.csv
        # via --out arg, so it's already there - entries below are safety copies)
        run_corr    / "corrections_salary.csv":        run_dir / "corrections_salary.csv",
        run_corr    / "corrections_status.csv":        run_dir / "corrections_status.csv",
        run_corr    / "corrections_hire_date.csv":     run_dir / "corrections_hire_date.csv",
        run_corr    / "corrections_job_org.csv":       run_dir / "corrections_job_org.csv",
        run_corr    / "corrections_manifest.csv":      run_dir / "corrections_manifest.csv",
        run_corr    / "held_corrections.csv":          run_dir / "held_corrections.csv",
        # matched_raw (informational - not required for downloads but useful)
        run_outputs / "matched_raw.csv":               run_dir / "matched_raw.csv",
        # unmatched records (records with no counterpart in the other system)
        run_outputs / "unmatched_old.csv":             run_dir / "unmatched_old.csv",
        run_outputs / "unmatched_new.csv":             run_dir / "unmatched_new.csv",
    }
    for src, dst in flat_map.items():
        if src.exists() and not dst.exists():
            try:
                shutil.copy2(src, dst)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Internal audit runner (background thread)
# ---------------------------------------------------------------------------
def _run_internal_audit(run_id: str, run_dir: Path, file_path: Path, options: dict | None = None):
    """Run a single-file internal data quality audit."""
    options = options or {}
    uploaded_paths = [file_path]
    try:
        _set_job_status(run_id, "running", clear_error=True)

        _set_step(run_id, "upload")
        _finish_step(run_id, "upload")

        _set_step(run_id, "audit")
        audit_cmd = [str(PYTHON), "audit/internal_audit.py",
                     "--file", str(file_path),
                     "--out-dir", str(run_dir),
                     "--source-name", str(options.get("source_name", "")),
                     "--sheet-name", str(options.get("sheet_name", 0))]
        if options.get("override_gate", False):
            audit_cmd.append("--override-gate")
        rc, output = _run_cmd(
            audit_cmd,
            HERE, run_id
        )
        _finish_step(run_id, "audit", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError(_command_error_message(output, "Internal audit failed"))

        _set_step(run_id, "audit_report")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/reports/build_internal_audit_report.py",
             "--run-id", run_id,
             "--run-dir", str(run_dir),
             "--out", str(run_dir / "internal_audit_report.pdf")],
            HERE, run_id,
        )
        _finish_step(run_id, "audit_report", "done" if rc == 0 else "warn")

        _set_step(run_id, "audit_workbook")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/reports/build_internal_audit_workbook.py",
             "--file", str(file_path),
             "--run-dir", str(run_dir),
             "--out", str(run_dir / "internal_audit_workbook.xlsx"),
             "--sheet-name", str(options.get("sheet_name", 0))],
            HERE, run_id,
        )
        _finish_step(run_id, "audit_workbook", "done" if rc == 0 else "warn")

        _set_step(run_id, "audit_exports")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/reports/build_internal_audit_exports.py",
             "--file", str(file_path),
             "--run-dir", str(run_dir),
             "--workbook", str(run_dir / "internal_audit_workbook.xlsx"),
             "--sheet-name", str(options.get("sheet_name", 0))],
            HERE, run_id,
        )
        _finish_step(run_id, "audit_exports", "done" if rc == 0 else "warn")

        stats = {}
        report = run_dir / "internal_audit_report.json"
        if report.exists():
            try:
                stats = json.loads(report.read_text(encoding="utf-8"))
            except Exception:
                pass

        outputs = _collect_outputs(run_dir)
        _update_job_record(
            run_id,
            status="completed",
            output_files_json=json.dumps(outputs),
            stats_json=json.dumps(stats),
            gate_status=stats.get("gate_status"),
            gate_message=stats.get("gate_message"),
            error_message=None,
        )

    except Exception as exc:
        logger.exception("Pipeline step failed: %s - %s", type(exc).__name__, str(exc))
        _set_job_status(
            run_id,
            "failed",
            error=str(exc) or "Processing failed. Please try again or contact support.",
        )
    finally:
        _delete_uploaded_source_files(run_id, uploaded_paths)


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------
def _safe_ext(filename: str) -> str:
    """Return .xlsx (or .xls/.xlsm) or .csv based on the uploaded filename."""
    if filename:
        ext = os.path.splitext(filename)[1].lower()
        if ext in (".xlsx", ".xls", ".xlsm", ".xlsb"):
            return ext
    return ".csv"


@app.post("/api/run/recon")
def api_run_recon():
    run_dir: Path | None = None
    try:
        _cleanup_expired_runs(_load_policy())

        old_file = request.files.get("old_file")
        new_file = request.files.get("new_file")
        if not old_file or not new_file:
            return jsonify({"error": "Both old_file and new_file are required"}), 400

        old_ext = _safe_ext(old_file.filename)
        new_ext = _safe_ext(new_file.filename)

        raw_sn = request.form.get("sheet_name", "0").strip()
        sheet_name: int | str = int(raw_sn) if raw_sn.lstrip("-").isdigit() else raw_sn

        raw_mar = request.form.get("min_approve_rate", "0.75").strip()
        try:
            min_approve_rate: float = float(raw_mar)
            if min_approve_rate > 1.0:
                min_approve_rate = min_approve_rate / 100.0
            min_approve_rate = max(0.0, min(1.0, min_approve_rate))
        except (ValueError, TypeError):
            min_approve_rate = 0.75

        bands_file = request.files.get("bands_file")

        options = {
            "sanity_gate":      request.form.get("sanity_gate", "true").lower() == "true",
            "corrections":      request.form.get("corrections", "true").lower() == "true",
            "workbook":         request.form.get("workbook",    "true").lower() == "true",
            "old_ext":          old_ext,
            "new_ext":          new_ext,
            "sheet_name":       sheet_name,
            "min_approve_rate": min_approve_rate,
            "has_bands":        bands_file is not None,
            "run_start_timestamp": _utcnow_iso(),
        }

        run_id  = _make_run_id()
        run_dir = RUNS_DIR / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        old_path = run_dir / f"old_input{old_ext}"
        new_path = run_dir / f"new_input{new_ext}"
        old_bytes = old_file.read()
        new_bytes = new_file.read()
        _write_upload(old_path, old_bytes)
        _write_upload(new_path, new_bytes)

        _validate_uploaded_source(old_path, sheet_name=sheet_name)
        _validate_uploaded_source(new_path, sheet_name=sheet_name)

        input_manifest = {
            "old_system": {
                "filename": old_file.filename or old_path.name,
                "sha256": _hash_file_bytes(old_bytes),
            },
            "new_system": {
                "filename": new_file.filename or new_path.name,
                "sha256": _hash_file_bytes(new_bytes),
            },
        }

        uploaded_paths = [old_path, new_path]

        if bands_file:
            bands_ext  = _safe_ext(bands_file.filename) or ".csv"
            bands_path = run_dir / f"compensation_bands{bands_ext}"
            bands_bytes = bands_file.read()
            _write_upload(bands_path, bands_bytes)
            input_manifest["compensation_bands"] = {
                "filename": bands_file.filename or bands_path.name,
                "sha256": _hash_file_bytes(bands_bytes),
            }
            options["bands_path"] = str(bands_path)
            uploaded_paths.append(bands_path)

        manifest_path = _write_input_manifest(run_dir, input_manifest)
        options["input_manifest_path"] = str(manifest_path)
        options["uploaded_paths"] = [str(p) for p in uploaded_paths]
        _create_job_record(
            run_id,
            job_type="recon",
            run_dir=run_dir,
            input_filenames=_input_filenames_from_manifest(input_manifest),
        )

        t = threading.Thread(
            target=_run_recon_pipeline,
            args=(run_id, run_dir, old_path, new_path, options),
            daemon=True,
        )
        t.start()

        return jsonify({"run_id": run_id, "mode": "recon"})
    except ValidationError as exc:
        if run_dir and run_dir.exists():
            shutil.rmtree(run_dir, ignore_errors=True)
        logger.info("Upload validation failed: %s", str(exc))
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        if run_dir and run_dir.exists():
            shutil.rmtree(run_dir, ignore_errors=True)
        logger.exception("Pipeline step failed: %s - %s", type(exc).__name__, str(exc))
        return jsonify({"error": "Processing failed. Please try again or contact support."}), 500


@app.post("/api/run/audit")
def api_run_audit():
    run_dir: Path | None = None
    try:
        _cleanup_expired_runs(_load_policy())

        audit_file = request.files.get("audit_file")
        if not audit_file:
            return jsonify({"error": "audit_file is required"}), 400

        audit_ext = _safe_ext(audit_file.filename)
        if audit_ext not in (".csv", ".xlsx", ".xls", ".xlsm", ".xlsb"):
            return jsonify({"error": "The uploaded file must be a CSV or Excel workbook."}), 400

        raw_sn = request.form.get("sheet_name", "0").strip()
        sheet_name: int | str = int(raw_sn) if raw_sn.lstrip("-").isdigit() else raw_sn
        override_gate = request.form.get("override_gate", "false").lower() == "true"

        run_id  = _make_run_id()
        run_dir = RUNS_DIR / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        file_path = run_dir / f"audit_input{audit_ext}"
        audit_bytes = audit_file.read()
        if not audit_bytes:
            shutil.rmtree(run_dir, ignore_errors=True)
            return jsonify({"error": "The uploaded file appears to be empty. Please check the file and try again."}), 400
        _write_upload(file_path, audit_bytes)

        from src.validator import validate_internal_audit_file

        result = validate_internal_audit_file(file_path, sheet_name=sheet_name)
        if not result.get("ok", False):
            shutil.rmtree(run_dir, ignore_errors=True)
            logger.info("Upload validation failed: %s", result.get("error"))
            return jsonify({"error": result.get("error") or "The uploaded file could not be validated."}), 400
        _create_job_record(
            run_id,
            job_type="audit",
            run_dir=run_dir,
            input_filenames=[audit_file.filename or file_path.name],
        )

        t = threading.Thread(
            target=_run_internal_audit,
            args=(run_id, run_dir, file_path, {"source_name": audit_file.filename or file_path.name, "sheet_name": sheet_name, "override_gate": override_gate}),
            daemon=True,
        )
        t.start()

        return jsonify({"run_id": run_id, "mode": "audit"})
    except Exception as exc:
        if run_dir and run_dir.exists():
            shutil.rmtree(run_dir, ignore_errors=True)
        logger.exception("Pipeline step failed: %s - %s", type(exc).__name__, str(exc))
        return jsonify({"error": "Processing failed. Please try again or contact support."}), 500


@app.get("/api/status/<run_id>")
def api_status(run_id: str):
    try:
        job = _get_job(run_id)
        if not job:
            return jsonify({"error": "Unknown run_id"}), 404
        out = {k: v for k, v in job.items() if k != "log"}
        return jsonify(out)
    except Exception as exc:
        logger.exception("Pipeline step failed: %s - %s", type(exc).__name__, str(exc))
        return jsonify({"error": "Processing failed. Please try again or contact support."}), 500


@app.get("/api/log/<run_id>")
def api_log(run_id: str):
    try:
        job = _get_job(run_id)
        if not job:
            return jsonify({"error": "Unknown run_id"}), 404
        return jsonify({"log": job.get("log", [])})
    except Exception as exc:
        logger.exception("Pipeline step failed: %s - %s", type(exc).__name__, str(exc))
        return jsonify({"error": "Processing failed. Please try again or contact support."}), 500


@app.get("/api/download/<run_id>/<path:filename>")
def api_download(run_id: str, filename: str):
    try:
        run_dir = RUNS_DIR / run_id
        if not run_dir.exists():
            abort(404)
        target = (run_dir / filename).resolve()
        base   = run_dir.resolve()
    except HTTPException:
        raise
    except Exception:
        abort(403)
    try:
        if not str(target).startswith(str(base) + os.sep) and str(target) != str(base):
            abort(403)
        rel = target.relative_to(base).as_posix()
        if _is_blocked_path(rel):
            abort(403)
        if not target.exists():
            abort(404)
        if target.stat().st_size == 0:
            abort(404)

        _MIME = {
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".csv":  "text/csv",
            ".json": "application/json",
            ".pdf":  "application/pdf",
        }
        ext      = os.path.splitext(filename)[1].lower()
        mimetype = _MIME.get(ext)
        rel_dir  = str(target.parent)
        basename = target.name
        return send_from_directory(
            rel_dir,
            basename,
            as_attachment=True,
            mimetype=mimetype,
            download_name=basename,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Pipeline step failed: %s - %s", type(exc).__name__, str(exc))
        return jsonify({"error": "Processing failed. Please try again or contact support."}), 500


@app.get("/api/ping")
def api_ping():
    try:
        return jsonify({"ok": True, "server": "recon-kit-api"})
    except Exception as exc:
        logger.exception("Pipeline step failed: %s - %s", type(exc).__name__, str(exc))
        return jsonify({"error": "Processing failed. Please try again or contact support."}), 500


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("RK_PORT", 5001))
    print(f"[recon-kit] API server starting on http://localhost:{port}")
    print(f"[recon-kit] Serving static site from: {SITE_DIR}")
    print(f"[recon-kit] Dashboard: http://localhost:{port}/dashboard.html")
    print(f"[recon-kit] Press Ctrl+C to stop.")
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
