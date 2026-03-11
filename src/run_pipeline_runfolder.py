"""
run_pipeline_runfolder.py - Run the full pipeline and collect outputs into a
timestamped run folder.

Run folder structure:
    runs/
      YYYY-MM-DD_HHMMSS/
        audit.db
        audit/           ← audit_q*.csv from run_audit.py
        summary/         ← sanity CSVs, JSONs, review_queue, workbook, report
        exports/         ← xlookup_keys.csv, wide_compare.csv
        corrections/     ← correction CSVs + manifest
        logs/
          pipeline.log   ← tee of all subprocess output

All subprocess stdout+stderr is streamed to console AND captured to pipeline.log.

Run:
    venv/Scripts/python.exe src/run_pipeline_runfolder.py
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / "runs"


# ---------------------------------------------------------------------------
# Streaming subprocess runner with log tee
# ---------------------------------------------------------------------------

def _stream(cmd: list[str], cwd: Path, env: dict, log, optional: bool = False) -> int:
    """
    Run cmd, streaming stdout+stderr line-by-line to both console and log.
    Returns the process exit code.
    """
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    kind = "RUN (optional)" if optional else "RUN"
    header = f"\n[{ts}] {kind}: {' '.join(cmd)}\n"
    sys.stdout.write(header)
    sys.stdout.flush()
    log.write(header)
    log.flush()

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            log.write(line)
            log.flush()
        proc.wait()
        return proc.returncode

    except Exception as exc:
        msg = f"[error] failed to launch: {exc}\n"
        sys.stdout.write(msg)
        log.write(msg)
        return -1


def _run(cmd: list[str], cwd: Path, env: dict, log, label: str = "") -> None:
    """Required step - raises SystemExit on non-zero exit code."""
    rc = _stream(cmd, cwd, env, log, optional=False)
    if rc != 0:
        msg = f"[error] Required step '{label or cmd[-1]}' exited {rc}. Aborting.\n"
        sys.stdout.write(msg)
        log.write(msg)
        raise SystemExit(rc)


def _run_opt(cmd: list[str], cwd: Path, env: dict, log, label: str = "") -> int:
    """Optional step - logs warning on non-zero exit, never raises. Returns exit code."""
    rc = _stream(cmd, cwd, env, log, optional=True)
    if rc not in (0,):
        msg = f"[warning] {label or cmd[-1]} exited {rc} - pipeline continues.\n"
        sys.stdout.write(msg)
        log.write(msg)
    return rc


def _copy_glob(src_dir: Path, dest_dir: Path, pattern: str) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    for f in src_dir.glob(pattern):
        shutil.copy2(str(f), str(dest_dir / f.name))


def _copy_if_exists(src: Path, dest: Path) -> None:
    if src.exists():
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(dest))


def _read_gate_json(gate_json: Path) -> tuple[bool, dict]:
    """Read sanity_gate.json; return (passed, blocked_outputs)."""
    if not gate_json.exists():
        return True, {"corrections": False, "workbook": False, "exports": False}
    try:
        with open(str(gate_json), "r", encoding="utf-8") as f:
            data = json.load(f)
        return (
            bool(data.get("passed", True)),
            data.get("blocked_outputs", {"corrections": False, "workbook": False, "exports": False}),
        )
    except Exception:
        return True, {"corrections": False, "workbook": False, "exports": False}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ts      = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_dir = RUNS / ts

    # Sub-directories
    run_audit_dir   = run_dir / "audit"
    run_summary_dir = run_dir / "summary"
    run_exports_dir = run_dir / "exports"
    run_corr_dir    = run_dir / "corrections"
    run_logs_dir    = run_dir / "logs"

    for d in (run_audit_dir, run_summary_dir, run_exports_dir,
              run_corr_dir, run_logs_dir):
        d.mkdir(parents=True, exist_ok=True)

    log_path = run_logs_dir / "pipeline.log"

    with open(str(log_path), "w", encoding="utf-8", buffering=1) as log:
        _main_inner(
            ts, run_dir, run_audit_dir, run_summary_dir,
            run_exports_dir, run_corr_dir, log,
        )

    print(f"\n[pipeline] log written to: {log_path.relative_to(ROOT)}")


def _main_inner(
    ts: str,
    run_dir: Path,
    run_audit_dir: Path,
    run_summary_dir: Path,
    run_exports_dir: Path,
    run_corr_dir: Path,
    log,
) -> None:
    root = ROOT
    py = sys.executable

    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)

    banner = (
        f"\n{'=' * 60}\n"
        f"  RECON PIPELINE (run-folder mode)\n"
        f"{'=' * 60}\n"
        f"  run folder : {run_dir}\n"
        f"  python     : {py}\n"
    )
    sys.stdout.write(banner)
    log.write(banner)

    # -------------------------------------------------------------------
    # Required steps (abort on failure)
    # -------------------------------------------------------------------
    _run(
        [py, "-c",
         "from src.mapping import map_file; "
         "map_file('inputs/old.csv','outputs/mapped_old.csv','old'); "
         "map_file('inputs/new.csv','outputs/mapped_new.csv','new')"],
        cwd=root, env=env, log=log, label="mapping",
    )
    _run([py, "-m", "src.matcher"], cwd=root, env=env, log=log, label="matcher")
    _run(
        [py, "-c",
         "from pathlib import Path; from resolve_matched_raw import resolve; "
         "resolve(input_path=Path('outputs/matched_raw.csv'), "
         "output_path=Path('outputs/matched_raw.csv'))"],
        cwd=root, env=env, log=log, label="resolve_matched_raw",
    )
    _run([py, str(root / "audit" / "load_sqlite.py")],
         cwd=root, env=env, log=log, label="load_sqlite")
    _run([py, str(root / "audit" / "run_audit.py")],
         cwd=root, env=env, log=log, label="run_audit")

    # Copy DB and audit CSVs into run folder
    db_src = root / "audit" / "audit.db"
    if db_src.exists():
        shutil.copy2(str(db_src), str(run_dir / "audit.db"))
        msg = f"  copied: audit.db → {run_dir.name}/audit.db\n"
        sys.stdout.write(msg)
        log.write(msg)

    _copy_glob(root / "audit", run_audit_dir, "audit_q*.csv")
    _copy_glob(root / "audit", run_audit_dir, "mismatch_*.csv")
    msg = f"  copied: audit/ CSVs → {run_dir.name}/audit/\n"
    sys.stdout.write(msg)
    log.write(msg)

    # -------------------------------------------------------------------
    # Optional steps - non-fatal
    # -------------------------------------------------------------------

    # Console-only; no output files to copy
    _run_opt(
        [py, str(root / "audit" / "reconciliation_summary.py")],
        cwd=root, env=env, log=log, label="reconciliation_summary",
    )

    # build_review_queue writes to audit/summary/review_queue.csv (hardcoded)
    _run_opt(
        [py, str(root / "audit" / "summary" / "build_review_queue.py")],
        cwd=root, env=env, log=log, label="build_review_queue",
    )

    # run_sanity_gate: writes CSVs + JSONs directly to run_summary_dir
    _run_opt(
        [py, str(root / "audit" / "summary" / "run_sanity_gate.py"),
         "--db", str(root / "audit" / "audit.db"),
         "--out", str(run_summary_dir)],
        cwd=root, env=env, log=log, label="run_sanity_gate",
    )

    # Read gate result to determine blocking
    gate_passed, blocked = _read_gate_json(run_summary_dir / "sanity_gate.json")

    if not gate_passed:
        msg = f"\n[WARNING] Sanity gate FAILED - see {run_dir.name}/summary/sanity_gate.json\n"
        sys.stdout.write(msg)
        log.write(msg)

    # DIY exports - write directly to run_exports_dir
    if blocked.get("exports"):
        msg = "[pipeline] Skipping build_diy_exports - blocked by sanity gate.\n"
        sys.stdout.write(msg)
        log.write(msg)
    else:
        _run_opt(
            [py, str(root / "audit" / "exports" / "build_diy_exports.py"),
             "--out-dir", str(run_exports_dir)],
            cwd=root, env=env, log=log, label="build_diy_exports",
        )

    # Corrections - write directly to run_corr_dir
    if blocked.get("corrections"):
        msg = "[pipeline] Skipping generate_corrections - blocked by sanity gate.\n"
        sys.stdout.write(msg)
        log.write(msg)
    else:
        _run_opt(
            [py, str(root / "audit" / "corrections" / "generate_corrections.py"),
             "--out-dir", str(run_corr_dir)],
            cwd=root, env=env, log=log, label="generate_corrections",
        )

    # build_report writes to audit/summary/ (hardcoded); copy outputs after
    _run_opt(
        [py, str(root / "audit" / "summary" / "build_report.py")],
        cwd=root, env=env, log=log, label="build_report",
    )
    for rpt in (root / "audit" / "summary").glob("recon_summary.*"):
        _copy_if_exists(rpt, run_summary_dir / rpt.name)
    charts_src = root / "audit" / "summary" / "charts"
    if charts_src.exists():
        charts_dest = run_summary_dir / "charts"
        charts_dest.mkdir(parents=True, exist_ok=True)
        _copy_glob(charts_src, charts_dest, "*.png")

    # Workbook - write directly to run_summary_dir
    if blocked.get("workbook"):
        msg = "[pipeline] Skipping build_workbook - blocked by sanity gate.\n"
        sys.stdout.write(msg)
        log.write(msg)
    else:
        wb_out = run_summary_dir / "recon_workbook.xlsx"
        # Pass wide_compare path from run_exports_dir if it exists; otherwise omit
        wide_arg = str(run_exports_dir / "wide_compare.csv")
        _run_opt(
            [py, str(root / "audit" / "summary" / "build_workbook.py"),
             "--out", str(wb_out),
             "--wide", wide_arg],
            cwd=root, env=env, log=log, label="build_workbook",
        )

    # Copy review_queue.csv (written by build_review_queue to default location)
    _copy_if_exists(
        root / "audit" / "summary" / "review_queue.csv",
        run_summary_dir / "review_queue.csv",
    )

    # -------------------------------------------------------------------
    # Final summary
    # -------------------------------------------------------------------
    gate_status  = "PASS" if gate_passed else "FAIL"
    blocked_list = [k for k, v in blocked.items() if v]
    blocked_str  = f" (blocked: {', '.join(blocked_list)})" if blocked_list else ""

    footer = (
        f"\n{'=' * 60}\n"
        f"  DONE\n"
        f"  run folder  : {run_dir}\n"
        f"  sanity_gate : {gate_status}{blocked_str}\n"
        f"{'=' * 60}\n"
    )
    sys.stdout.write(footer)
    log.write(footer)


if __name__ == "__main__":
    main()
