"""
api_server.py — Local API server for the Recon-Kit dashboard.

Run with:
    venv/Scripts/python.exe api_server.py

Listens on http://localhost:5001
Serves the static site from site/ and provides two API endpoints:
  POST /api/run/recon   — cross-system reconciliation (old + new CSV)
  POST /api/run/audit   — internal data audit (single CSV)
  GET  /api/status/<run_id>
  GET  /api/download/<run_id>/<filename>
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory, abort

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
        with _jobs_lock:
            _jobs[run_id].setdefault("log", []).append(line.rstrip())
    proc.wait()
    return proc.returncode, "\n".join(lines)


def _collect_outputs(run_dir: Path) -> list[dict]:
    """Scan a run directory for downloadable output files."""
    wanted = [
        "recon_summary.xlsx",
        "recon_workbook.xlsx",
        "audit_report.docx",
        "wide_compare.csv",
        "review_queue.csv",
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
        if p.exists():
            found.append({"name": name, "size": p.stat().st_size})
    # Also pick up anything in corrections subfolder
    corr_dir = run_dir / "corrections"
    if corr_dir.exists():
        for p in corr_dir.iterdir():
            if p.suffix in (".csv", ".xlsx", ".json") and p.name not in [f["name"] for f in found]:
                found.append({"name": "corrections/" + p.name, "size": p.stat().st_size})
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

    # Gate status from sanity_gate.json
    gate_file = run_dir / "sanity_gate.json"
    if gate_file.exists():
        try:
            gate = json.loads(gate_file.read_text(encoding="utf-8"))
            stats["gate_passed"] = gate.get("passed", True)
        except Exception:
            pass

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

    try:
        _RECON_SEMAPHORE.acquire()
        with _jobs_lock:
            _jobs[run_id]["status"] = "running"

        # Create the per-run directory structure up front
        for d in (run_inputs, run_outputs, run_summary, run_corr):
            d.mkdir(parents=True, exist_ok=True)

        # 1. Copy input files into per-run inputs/
        _set_step(run_id, "upload")
        shutil.copy2(old_path, run_inputs / "old.csv")
        shutil.copy2(new_path, run_inputs / "new.csv")
        _finish_step(run_id, "upload")

        # 2. Mapping — use absolute run_dir paths so no global outputs/ is touched
        _set_step(run_id, "mapping")
        old_in  = run_inputs  / "old.csv"
        new_in  = run_inputs  / "new.csv"
        old_out = run_outputs / "mapped_old.csv"
        new_out = run_outputs / "mapped_new.csv"
        rc, _ = _run_cmd(
            [str(PYTHON), "-c",
             f"from src.mapping import map_file; "
             f"map_file(r'{old_in}', r'{old_out}', 'old'); "
             f"map_file(r'{new_in}', r'{new_out}', 'new')"],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "mapping", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError("Mapping step failed")

        # 3. Matching — RK_WORK_DIR tells matcher.py where to write matched_raw.csv
        _set_step(run_id, "matching")
        rc, _ = _run_cmd([str(PYTHON), "src/matcher.py"], HERE, run_id, env=run_env)
        _finish_step(run_id, "matching", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError("Matcher step failed")

        # 4. Resolve conflicts — RK_WORK_DIR tells resolve where matched_raw.csv lives
        _set_step(run_id, "resolve")
        rc, _ = _run_cmd([str(PYTHON), "resolve_matched_raw.py"], HERE, run_id, env=run_env)
        _finish_step(run_id, "resolve", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError("Resolve step failed")

        # 5. Load SQLite — RK_WORK_DIR tells load_sqlite where to place audit.db
        _set_step(run_id, "load_db")
        rc, _ = _run_cmd([str(PYTHON), "audit/load_sqlite.py"], HERE, run_id, env=run_env)
        _finish_step(run_id, "load_db", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError("DB load step failed")

        # 6. Run audit queries — RK_WORK_DIR tells run_audit.py which DB to use
        _set_step(run_id, "audit")
        rc, _ = _run_cmd([str(PYTHON), "audit/run_audit.py"], HERE, run_id, env=run_env)
        _finish_step(run_id, "audit", "done" if rc == 0 else "error")
        if rc != 0:
            raise RuntimeError("Audit step failed")

        # 7. Optional: sanity gate — write JSON to per-run summary dir
        if options.get("sanity_gate", True):
            _set_step(run_id, "sanity_gate")
            rc, _ = _run_cmd(
                [str(PYTHON), "audit/summary/run_sanity_gate.py",
                 "--db",  str(run_db),
                 "--out", str(run_summary)],
                HERE, run_id, env=run_env,
            )
            _finish_step(run_id, "sanity_gate", "done" if rc == 0 else "warn")

        # 8. Optional: generate corrections — pass per-run DB and output dir
        if options.get("corrections", True):
            _set_step(run_id, "corrections")
            rc, _ = _run_cmd(
                [str(PYTHON), "audit/corrections/generate_corrections.py",
                 "--db",      str(run_db),
                 "--out-dir", str(run_corr)],
                HERE, run_id, env=run_env,
            )
            _finish_step(run_id, "corrections", "done" if rc == 0 else "warn")

        # 9. DIY exports — writes wide_compare.csv directly to run_dir
        _set_step(run_id, "exports")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/exports/build_diy_exports.py",
             "--db",     str(run_db),
             "--out-dir", str(run_dir)],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "exports", "done" if rc == 0 else "warn")

        # 10. Optional: Excel workbook — reads wide_compare.csv from run_dir
        if options.get("workbook", True):
            _set_step(run_id, "workbook")
            rc, _ = _run_cmd(
                [str(PYTHON), "audit/summary/build_workbook.py",
                 "--out",  str(run_dir / "recon_workbook.xlsx"),
                 "--wide", str(run_dir / "wide_compare.csv"),
                 "--db",   str(run_db)],
                HERE, run_id, env=run_env,
            )
            _finish_step(run_id, "workbook", "done" if rc == 0 else "warn")

        # 11. Review queue — pass per-run DB and output path
        _set_step(run_id, "review_queue")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/summary/build_review_queue.py",
             "--db",  str(run_db),
             "--out", str(run_dir / "review_queue.csv")],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "review_queue", "done" if rc == 0 else "warn")

        # 12. Audit report (.docx) — fully isolated, writes directly to run_dir
        _set_step(run_id, "audit_report")
        rc, _ = _run_cmd(
            [str(PYTHON), "audit/reports/generate_report.py",
             "--db",   str(run_db),
             "--wide", str(run_dir / "wide_compare.csv"),
             "--out",  str(run_dir / "audit_report.docx")],
            HERE, run_id, env=run_env,
        )
        _finish_step(run_id, "audit_report", "done" if rc == 0 else "warn")

        # 13. Promote sub-dir outputs to run_dir root for easy download
        _promote_run_outputs(run_dir)

        # Parse stats
        stats = _parse_run_stats(run_dir)

        with _jobs_lock:
            _jobs[run_id]["status"] = "done"
            _jobs[run_id]["outputs"] = _collect_outputs(run_dir)
            _jobs[run_id]["stats"] = stats

    except Exception as exc:
        with _jobs_lock:
            _jobs[run_id]["status"] = "error"
            _jobs[run_id]["error"] = str(exc)

    finally:
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
        # via --out arg, so it's already there — entries below are safety copies)
        run_corr    / "corrections_salary.csv":        run_dir / "corrections_salary.csv",
        run_corr    / "corrections_status.csv":        run_dir / "corrections_status.csv",
        run_corr    / "corrections_hire_date.csv":     run_dir / "corrections_hire_date.csv",
        run_corr    / "corrections_job_org.csv":       run_dir / "corrections_job_org.csv",
        run_corr    / "corrections_manifest.csv":      run_dir / "corrections_manifest.csv",
        run_corr    / "held_corrections.csv":          run_dir / "held_corrections.csv",
        # matched_raw (informational — not required for downloads but useful)
        run_outputs / "matched_raw.csv":               run_dir / "matched_raw.csv",
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
        with _jobs_lock:
            _jobs[run_id]["status"] = "error"
            _jobs[run_id]["error"] = str(exc)


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------
@app.post("/api/run/recon")
def api_run_recon():
    old_file = request.files.get("old_file")
    new_file = request.files.get("new_file")
    if not old_file or not new_file:
        return jsonify({"error": "Both old_file and new_file are required"}), 400

    options = {
        "sanity_gate": request.form.get("sanity_gate", "true").lower() == "true",
        "corrections": request.form.get("corrections", "true").lower() == "true",
        "workbook":    request.form.get("workbook",    "true").lower() == "true",
    }

    run_id  = _make_run_id()
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    old_path = run_dir / "old_input.csv"
    new_path = run_dir / "new_input.csv"
    old_file.save(str(old_path))
    new_file.save(str(new_path))

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


@app.post("/api/run/audit")
def api_run_audit():
    audit_file = request.files.get("audit_file")
    if not audit_file:
        return jsonify({"error": "audit_file is required"}), 400

    run_id  = _make_run_id()
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    file_path = run_dir / "audit_input.csv"
    audit_file.save(str(file_path))

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


@app.get("/api/status/<run_id>")
def api_status(run_id: str):
    with _jobs_lock:
        job = _jobs.get(run_id)
    if not job:
        return jsonify({"error": "Unknown run_id"}), 404
    # Return a safe copy (don't include full log by default)
    out = {k: v for k, v in job.items() if k != "log"}
    return jsonify(out)


@app.get("/api/log/<run_id>")
def api_log(run_id: str):
    with _jobs_lock:
        job = _jobs.get(run_id)
    if not job:
        return jsonify({"error": "Unknown run_id"}), 404
    return jsonify({"log": job.get("log", [])})


@app.get("/api/download/<run_id>/<path:filename>")
def api_download(run_id: str, filename: str):
    run_dir = RUNS_DIR / run_id
    if not run_dir.exists():
        abort(404)
    # Security: only allow files within run_dir
    target = (run_dir / filename).resolve()
    if not str(target).startswith(str(run_dir.resolve())):
        abort(403)
    if not target.exists():
        abort(404)
    return send_from_directory(str(run_dir), filename, as_attachment=True)


@app.get("/api/ping")
def api_ping():
    return jsonify({"ok": True, "server": "recon-kit-api"})


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
