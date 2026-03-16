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
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory, abort
from werkzeug.exceptions import HTTPException

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
HERE        = Path(__file__).resolve().parent
SITE_DIR    = HERE / "site"
RUNS_DIR    = HERE / "dashboard_runs"
PYTHON      = sys.executable

RUNS_DIR.mkdir(exist_ok=True)

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

# In-memory job registry  { run_id: { status, steps, result, error } }
_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Static site
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return send_from_directory(SITE_DIR, "index.html")


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


def _set_step(run_id: str, step: str, status: str = "running"):
    with _jobs_lock:
        _jobs[run_id]["steps"].append({"step": step, "status": status, "ts": time.time()})


def _finish_step(run_id: str, step: str, status: str = "done"):
    with _jobs_lock:
        for s in reversed(_jobs[run_id]["steps"]):
            if s["step"] == step and s["status"] == "running":
                s["status"] = status
                s["ts_end"] = time.time()
                break


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
    with _jobs_lock:
        _jobs[run_id].setdefault("log", []).extend(safe_lines)
    return proc.returncode, "\n".join(lines)


def _collect_outputs(run_dir: Path) -> list[dict]:
    """Scan a run directory for downloadable output files."""
    wanted = [
        "recon_summary.xlsx",
        "recon_workbook.xlsx",
        "audit_report.docx",
        "audit_report.pdf",
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
        "internal_audit_report.csv",
        "internal_audit_duplicates.csv",
        "internal_audit_blanks.csv",
        "internal_audit_suspicious.csv",
    ]
    found = []
    for name in wanted:
        p = run_dir / name
        if p.exists() and p.stat().st_size > 0:
            found.append({"name": name, "size": p.stat().st_size})
    # Also pick up anything in corrections subfolder
    corr_dir = run_dir / "corrections"
    if corr_dir.exists():
        for p in corr_dir.iterdir():
            if p.suffix in (".csv", ".xlsx", ".json") and p.name not in [f["name"] for f in found]:
                found.append({"name": "corrections/" + p.name, "size": p.stat().st_size})

    # Dynamically pick up per-department review queue CSVs
    existing_names = {f["name"] for f in found}
    for p in run_dir.glob("review_queue_*.csv"):
        if p.name not in existing_names and p.exists() and p.stat().st_size > 0:
            found.append({"name": p.name, "size": p.stat().st_size})

    return found


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
        with _jobs_lock:
            _jobs[run_id]["status"] = "running"

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
        rc, _ = _run_cmd(
            [str(PYTHON), "-c",
             f"from src.mapping import map_file; "
             f"map_file(r'{old_in}', r'{old_out}', 'old', sheet_name={sheet_name!r}); "
             f"map_file(r'{new_in}', r'{new_out}', 'new', sheet_name={sheet_name!r})"],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "mapping", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError("Mapping step failed")

        # 3. Matching - RK_WORK_DIR tells matcher.py where to write matched_raw.csv
        _set_step(run_id, "matching")
        rc, _ = _run_cmd([str(PYTHON), "src/matcher.py"], HERE, run_id, env=run_env)
        _finish_step(run_id, "matching", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError("Matcher step failed")

        # 4. Resolve conflicts - RK_WORK_DIR tells resolve where matched_raw.csv lives
        _set_step(run_id, "resolve")
        rc, _ = _run_cmd([str(PYTHON), "resolve_matched_raw.py"], HERE, run_id, env=run_env)
        _finish_step(run_id, "resolve", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError("Resolve step failed")

        # 5. Load SQLite - RK_WORK_DIR tells load_sqlite where to place audit.db
        _set_step(run_id, "load_db")
        rc, _ = _run_cmd([str(PYTHON), "audit/load_sqlite.py"], HERE, run_id, env=run_env)
        _finish_step(run_id, "load_db", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError("DB load step failed")

        # 6. Run audit queries - RK_WORK_DIR tells run_audit.py which DB to use
        _set_step(run_id, "audit")
        rc, _ = _run_cmd([str(PYTHON), "audit/run_audit.py"], HERE, run_id, env=run_env)
        _finish_step(run_id, "audit", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError("Audit step failed")

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

        # Parse stats
        stats = _parse_run_stats(run_dir)

        with _jobs_lock:
            _jobs[run_id]["status"] = "done"
            _jobs[run_id]["outputs"] = _collect_outputs(run_dir)
            _jobs[run_id]["stats"] = stats

    except Exception as exc:
        logger.exception("Pipeline step failed: %s - %s", type(exc).__name__, str(exc))
        with _jobs_lock:
            _jobs[run_id]["status"] = "error"
            _jobs[run_id]["error"] = "Processing failed. Please try again or contact support."

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
def _run_internal_audit(run_id: str, run_dir: Path, file_path: Path):
    """Run a single-file internal data quality audit."""
    uploaded_paths = [file_path]
    try:
        with _jobs_lock:
            _jobs[run_id]["status"] = "running"

        _set_step(run_id, "upload")
        _finish_step(run_id, "upload")

        _set_step(run_id, "audit")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/internal_audit.py",
             "--file", str(file_path),
             "--out-dir", str(run_dir)],
            HERE, run_id
        )
        _finish_step(run_id, "audit", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError("Internal audit failed")

        stats = {}
        report = run_dir / "internal_audit_report.json"
        if report.exists():
            try:
                stats = json.loads(report.read_text(encoding="utf-8"))
            except Exception:
                pass

        with _jobs_lock:
            _jobs[run_id]["status"] = "done"
            _jobs[run_id]["outputs"] = _collect_outputs(run_dir)
            _jobs[run_id]["stats"] = stats

    except Exception as exc:
        logger.exception("Pipeline step failed: %s - %s", type(exc).__name__, str(exc))
        with _jobs_lock:
            _jobs[run_id]["status"] = "error"
            _jobs[run_id]["error"] = "Processing failed. Please try again or contact support."
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

        with _jobs_lock:
            _jobs[run_id] = {
                "run_id": run_id,
                "mode": "recon",
                "status": "queued",
                "steps": [],
                "log": [],
                "outputs": [],
                "stats": {},
                "error": None,
                "started": time.time(),
            }

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

        run_id  = _make_run_id()
        run_dir = RUNS_DIR / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        file_path = run_dir / "audit_input.csv"
        audit_bytes = audit_file.read()
        _write_upload(file_path, audit_bytes)

        with _jobs_lock:
            _jobs[run_id] = {
                "run_id": run_id,
                "mode": "audit",
                "status": "queued",
                "steps": [],
                "log": [],
                "outputs": [],
                "stats": {},
                "error": None,
                "started": time.time(),
            }

        t = threading.Thread(
            target=_run_internal_audit,
            args=(run_id, run_dir, file_path),
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
        with _jobs_lock:
            job = _jobs.get(run_id)
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
        with _jobs_lock:
            job = _jobs.get(run_id)
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
