# src/run_pipeline.py
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime


ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Log tee — mirrors Python-level writes to both console and a log file
# ---------------------------------------------------------------------------

class _Tee:
    """Write to both the real stdout and a log file handle."""

    def __init__(self, stream, log_fh):
        self._s = stream
        self._l = log_fh

    def write(self, data: str) -> int:
        self._s.write(data)
        self._l.write(data)
        return len(data)

    def flush(self) -> None:
        self._s.flush()
        try:
            self._l.flush()
        except Exception:
            pass

    def fileno(self) -> int:
        return self._s.fileno()


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------

def _stream_proc(cmd: list[str], cwd: Path, env: dict) -> int:
    """Run cmd, streaming output line-by-line to sys.stdout. Returns exit code."""
    try:
        proc = subprocess.Popen(
            cmd, cwd=str(cwd), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
        proc.wait()
        return proc.returncode
    except Exception as exc:
        sys.stdout.write(f"[error] failed to launch {cmd[-1]}: {exc}\n")
        return -1


def run_cmd(cmd: list[str], cwd: Path, env: dict[str, str]) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{ts}] RUN: {' '.join(cmd)}")
    rc = _stream_proc(cmd, cwd, env)
    if rc != 0:
        raise subprocess.CalledProcessError(rc, cmd)


def run_cmd_optional(cmd: list[str], cwd: Path, env: dict[str, str], label: str) -> int:
    """Run a command but do not fail the pipeline if it errors. Returns exit code."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{ts}] RUN (optional): {' '.join(cmd)}")
    rc = _stream_proc(cmd, cwd, env)
    if rc != 0:
        print(f"[warning] {label} exited with code {rc} — pipeline continues.")
    return rc


def _read_gate_json(gate_json: Path) -> tuple[bool, dict]:
    """Read sanity_gate.json; return (passed, blocked_outputs). Defaults to PASS on error."""
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
# Receipt helpers
# ---------------------------------------------------------------------------

def _rc_info(path) -> dict:
    """Lightweight file info for receipts (avoids importing step_receipts at module level)."""
    from step_receipts import file_info  # noqa: PLC0415
    return file_info(path)


def _write_receipt(run_dirs: dict, step: str, payload: dict) -> None:
    """Write a step receipt; swallow errors so receipts never block the pipeline."""
    if run_dirs is None:
        return
    try:
        from step_receipts import write_receipt  # noqa: PLC0415
        write_receipt(run_dirs, step, payload)
    except Exception as exc:
        print(f"[warning] receipt write failed for {step}: {exc}")


def _csv_rows(path) -> int | None:
    """Return CSV row count (lines minus header), or None if unreadable."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        with open(str(p), "r", encoding="utf-8", errors="replace") as f:
            n = sum(1 for _ in f)
        return max(0, n - 1)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    from run_manager import make_run_id, ensure_run_dirs, copy_artifacts_to_run, write_run_manifest

    run_id    = make_run_id()
    run_paths = ensure_run_dirs(run_id)
    log_path  = run_paths["logs"] / "pipeline.log"

    root = ROOT
    py = sys.executable

    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)

    _log_fh      = open(str(log_path), "w", encoding="utf-8", buffering=1)
    _orig_stdout = sys.stdout
    sys.stdout   = _Tee(_orig_stdout, _log_fh)

    print("\n============================================================")
    print("RECON PIPELINE")
    print("============================================================")
    print(f"root:   {root}")
    print(f"python: {py}")
    print(f"run id: {run_id}")

    pipeline_ok = True
    gate_passed = True   # default; updated after sanity gate runs
    blocked: dict = {}

    try:
        # ==============================================================
        # REQUIRED STEPS — abort on failure
        # ==============================================================

        # --- mapping ---------------------------------------------------
        t0 = time.monotonic()
        run_cmd(
            [
                py, "-c",
                "from src.mapping import map_file; "
                "map_file('inputs/old.csv','outputs/mapped_old.csv','old'); "
                "map_file('inputs/new.csv','outputs/mapped_new.csv','new')",
            ],
            cwd=root, env=env,
        )
        _write_receipt(run_paths, "mapping", {
            "inputs":  [
                _rc_info(root / "inputs" / "old.csv"),
                _rc_info(root / "inputs" / "new.csv"),
            ],
            "outputs": [
                _rc_info(root / "outputs" / "mapped_old.csv"),
                _rc_info(root / "outputs" / "mapped_new.csv"),
            ],
            "warnings":      [],
            "elapsed_sec":   round(time.monotonic() - t0, 3),
        })

        # --- matcher ---------------------------------------------------
        t0 = time.monotonic()
        run_cmd([py, "-m", "src.matcher"], cwd=root, env=env)
        _write_receipt(run_paths, "matcher", {
            "inputs":  [_rc_info(root / "outputs" / "mapped_old.csv"),
                        _rc_info(root / "outputs" / "mapped_new.csv")],
            "outputs": [_rc_info(root / "outputs" / "matched_raw.csv")],
            "warnings":      [],
            "elapsed_sec":   round(time.monotonic() - t0, 3),
        })

        # --- resolve ---------------------------------------------------
        t0 = time.monotonic()
        run_cmd(
            [
                py, "-c",
                "from pathlib import Path; from resolve_matched_raw import resolve; "
                "resolve(input_path=Path('outputs/matched_raw.csv'), "
                "output_path=Path('outputs/matched_raw.csv'))",
            ],
            cwd=root, env=env,
        )
        _write_receipt(run_paths, "resolve", {
            "inputs":  [_rc_info(root / "outputs" / "matched_raw.csv")],
            "outputs": [_rc_info(root / "outputs" / "matched_raw.csv")],
            "warnings":      [],
            "elapsed_sec":   round(time.monotonic() - t0, 3),
        })

        # --- load_sqlite -----------------------------------------------
        t0 = time.monotonic()
        run_cmd([py, str(root / "audit" / "load_sqlite.py")], cwd=root, env=env)
        _write_receipt(run_paths, "load_sqlite", {
            "inputs":  [_rc_info(root / "outputs" / "matched_raw.csv")],
            "outputs": [_rc_info(root / "audit" / "audit.db")],
            "warnings":      [],
            "elapsed_sec":   round(time.monotonic() - t0, 3),
        })

        # --- schema_validator ------------------------------------------
        # Fast pre-flight check: ensures matched_pairs has all required columns
        # (including 'confidence') before any gating step runs.
        t0 = time.monotonic()
        run_cmd([py, str(root / "audit" / "schema_validator.py")], cwd=root, env=env)
        _write_receipt(run_paths, "schema_validator", {
            "inputs":  [_rc_info(root / "audit" / "audit.db")],
            "outputs": [],
            "warnings":      [],
            "elapsed_sec":   round(time.monotonic() - t0, 3),
        })

        # --- run_audit -------------------------------------------------
        t0 = time.monotonic()
        run_cmd([py, str(root / "audit" / "run_audit.py")], cwd=root, env=env)
        audit_csvs = [_rc_info(f) for f in sorted((root / "audit").glob("audit_q*.csv"))]
        _write_receipt(run_paths, "run_audit", {
            "inputs":  [_rc_info(root / "audit" / "audit.db")],
            "outputs": audit_csvs,
            "warnings":      [],
            "elapsed_sec":   round(time.monotonic() - t0, 3),
        })

        # ==============================================================
        # OPTIONAL STEPS — failures are non-fatal
        # ==============================================================

        # --- reconciliation_summary ------------------------------------
        t0 = time.monotonic()
        rc = run_cmd_optional(
            [py, str(root / "audit" / "reconciliation_summary.py")],
            cwd=root, env=env, label="reconciliation_summary.py",
        )
        _write_receipt(run_paths, "reconciliation_summary", {
            "inputs":        [_rc_info(root / "audit" / "audit.db")],
            "outputs":       [],
            "warnings":      [] if rc == 0 else [f"exited with code {rc}"],
            "elapsed_sec":   round(time.monotonic() - t0, 3),
            "ok":            rc == 0,
        })

        # --- build_review_queue ----------------------------------------
        t0 = time.monotonic()
        rc = run_cmd_optional(
            [py, str(root / "audit" / "summary" / "build_review_queue.py")],
            cwd=root, env=env, label="build_review_queue.py",
        )
        _write_receipt(run_paths, "build_review_queue", {
            "inputs":  [_rc_info(root / "audit" / "audit.db")],
            "outputs": [_rc_info(root / "audit" / "summary" / "review_queue.csv")],
            "warnings":      [] if rc == 0 else [f"exited with code {rc}"],
            "elapsed_sec":   round(time.monotonic() - t0, 3),
            "ok":            rc == 0,
        })

        # --- build_ui_pairs (first pass — before sanity gate) ----------
        t0 = time.monotonic()
        rc = run_cmd_optional(
            [py, str(root / "audit" / "ui" / "build_ui_pairs.py")],
            cwd=root, env=env, label="build_ui_pairs.py",
        )
        _write_receipt(run_paths, "build_ui_pairs_pre_gate", {
            "inputs":  [_rc_info(root / "audit" / "audit.db")],
            "outputs": [_rc_info(root / "audit" / "ui" / "ui_pairs.csv")],
            "warnings":      [] if rc == 0 else [f"exited with code {rc}"],
            "elapsed_sec":   round(time.monotonic() - t0, 3),
            "ok":            rc == 0,
        })

        # --- run_sanity_gate -------------------------------------------
        # Exits 3 if gate fails; run_cmd_optional captures that.
        t0 = time.monotonic()
        rc_gate = run_cmd_optional(
            [py, str(root / "audit" / "summary" / "run_sanity_gate.py")],
            cwd=root, env=env, label="run_sanity_gate.py",
        )

        gate_json_path = root / "audit" / "summary" / "sanity_gate.json"
        gate_passed, blocked = _read_gate_json(gate_json_path)
        gate_status = "PASS" if gate_passed else "FAIL"

        # Triage: when gate fails, we do NOT block exports/workbook/report —
        # only corrections are blocked. Override blocked to reflect triage policy.
        if not gate_passed:
            blocked = {
                "corrections": True,
                "workbook":    False,
                "exports":     False,
            }

        blocked_steps = [k for k, v in blocked.items() if v]

        _write_receipt(run_paths, "run_sanity_gate", {
            "inputs":       [_rc_info(root / "audit" / "audit.db"),
                             _rc_info(root / "audit" / "summary" / "sanity_results.json")],
            "outputs":      [_rc_info(gate_json_path),
                             _rc_info(root / "audit" / "summary" / "sanity_results.json")],
            "gate_status":  gate_status,
            "blocked_steps": blocked_steps,
            "warnings":      [] if rc_gate == 0 else [f"gate exited {rc_gate}; gate_status={gate_status}"],
            "elapsed_sec":   round(time.monotonic() - t0, 3),
            "ok":            gate_passed,
        })

        if not gate_passed:
            print(f"\n[pipeline] WARNING: Sanity gate FAILED — triage mode active.")
            print(f"[pipeline]   blocked : corrections")
            print(f"[pipeline]   running : exports, workbook, report, root-cause")

        # --- root_cause_hire_date (only when gate fails) ---------------
        if not gate_passed:
            t0 = time.monotonic()
            rc = run_cmd_optional(
                [py, str(root / "audit" / "summary" / "root_cause_hire_date.py")],
                cwd=root, env=env, label="root_cause_hire_date.py",
            )
            _write_receipt(run_paths, "root_cause_hire_date", {
                "inputs":  [_rc_info(root / "audit" / "audit.db")],
                "outputs": [
                    _rc_info(root / "audit" / "summary" / "root_cause_hire_date_defaults.csv"),
                    _rc_info(root / "audit" / "summary" / "root_cause_hire_date_samples.csv"),
                ],
                "warnings":      [] if rc == 0 else [f"exited with code {rc}"],
                "elapsed_sec":   round(time.monotonic() - t0, 3),
                "ok":            rc == 0,
            })

        # --- build_diy_exports -----------------------------------------
        if blocked.get("exports"):
            print("\n[pipeline] Skipping build_diy_exports — blocked by sanity gate.")
        else:
            t0 = time.monotonic()
            rc = run_cmd_optional(
                [py, str(root / "audit" / "exports" / "build_diy_exports.py")],
                cwd=root, env=env, label="build_diy_exports.py",
            )
            _write_receipt(run_paths, "build_diy_exports", {
                "inputs":  [_rc_info(root / "audit" / "audit.db")],
                "outputs": [
                    _rc_info(root / "audit" / "exports" / "xlookup_keys.csv"),
                    _rc_info(root / "audit" / "exports" / "wide_compare.csv"),
                ],
                "warnings":      [] if rc == 0 else [f"exited with code {rc}"],
                "elapsed_sec":   round(time.monotonic() - t0, 3),
                "ok":            rc == 0,
            })

        # --- generate_corrections (BLOCKED in triage mode) -------------
        if blocked.get("corrections"):
            print("\n[pipeline] Skipping generate_corrections — blocked by sanity gate (triage mode).")
            _write_receipt(run_paths, "generate_corrections", {
                "inputs":      [],
                "outputs":     [],
                "warnings":    ["skipped — gate failed, corrections blocked in triage mode"],
                "elapsed_sec": 0.0,
                "ok":          False,
                "skipped":     True,
            })
        else:
            t0 = time.monotonic()
            rc = run_cmd_optional(
                [py, str(root / "audit" / "corrections" / "generate_corrections.py")],
                cwd=root, env=env, label="generate_corrections.py",
            )
            _write_receipt(run_paths, "generate_corrections", {
                "inputs":  [_rc_info(root / "audit" / "audit.db")],
                "outputs": [
                    _rc_info(root / "audit" / "corrections" / "out" / "corrections_manifest.csv"),
                ],
                "warnings":      [] if rc == 0 else [f"exited with code {rc}"],
                "elapsed_sec":   round(time.monotonic() - t0, 3),
                "ok":            rc == 0,
            })

        # --- build_report ----------------------------------------------
        t0 = time.monotonic()
        rc = run_cmd_optional(
            [py, str(root / "audit" / "summary" / "build_report.py")],
            cwd=root, env=env, label="build_report.py",
        )
        _write_receipt(run_paths, "build_report", {
            "inputs":  [_rc_info(root / "audit" / "audit.db")],
            "outputs": [
                _rc_info(root / "audit" / "summary" / "recon_summary.md"),
                _rc_info(root / "audit" / "summary" / "recon_summary.html"),
            ],
            "warnings":      [] if rc == 0 else [f"exited with code {rc}"],
            "elapsed_sec":   round(time.monotonic() - t0, 3),
            "ok":            rc == 0,
        })

        # --- build_workbook --------------------------------------------
        if blocked.get("workbook"):
            print("\n[pipeline] Skipping build_workbook — blocked by sanity gate.")
        else:
            t0 = time.monotonic()
            rc = run_cmd_optional(
                [py, str(root / "audit" / "summary" / "build_workbook.py")],
                cwd=root, env=env, label="build_workbook.py",
            )
            _write_receipt(run_paths, "build_workbook", {
                "inputs":  [_rc_info(root / "audit" / "audit.db")],
                "outputs": [_rc_info(root / "audit" / "summary" / "recon_workbook.xlsx")],
                "warnings":      [] if rc == 0 else [f"exited with code {rc}"],
                "elapsed_sec":   round(time.monotonic() - t0, 3),
                "ok":            rc == 0,
            })

        # --- build_ui_pairs (second pass — after exports + gate) -------
        t0 = time.monotonic()
        rc = run_cmd_optional(
            [py, str(root / "audit" / "ui" / "build_ui_pairs.py")],
            cwd=root, env=env, label="build_ui_pairs.py (post-gate)",
        )
        _write_receipt(run_paths, "build_ui_pairs", {
            "inputs":  [_rc_info(root / "audit" / "audit.db")],
            "outputs": [_rc_info(root / "audit" / "ui" / "ui_pairs.csv")],
            "warnings":      [] if rc == 0 else [f"exited with code {rc}"],
            "elapsed_sec":   round(time.monotonic() - t0, 3),
            "ok":            rc == 0,
        })

        # --- Final banner ----------------------------------------------
        blocked_list = [k for k, v in blocked.items() if v]
        blocked_str  = f" (blocked: {', '.join(blocked_list)})" if blocked_list else ""

        print("\n============================================================")
        print("DONE")
        print(f"[pipeline] sanity_gate: {gate_status}{blocked_str}")
        print("============================================================")

    except BaseException:
        pipeline_ok = False
        raise

    finally:
        sys.stdout = _orig_stdout

        artifact_result = copy_artifacts_to_run(run_id, run_paths)
        write_run_manifest(run_id, run_paths, extra={
            "artifact_copy": artifact_result,
            "pipeline_ok":   pipeline_ok,
            "gate_status":   "PASS" if gate_passed else "FAIL",
        })

        n_copied  = len(artifact_result["copied"])
        n_missing = len(artifact_result["missing"])
        n_errors  = len(artifact_result["errors"])

        summary_lines = [
            "",
            f"  Run folder : {run_paths['run']}",
            f"  Artifacts  : {n_copied} copied, {n_missing} missing, {n_errors} errors",
            f"  Log        : {log_path}",
        ]
        if artifact_result["errors"]:
            for e in artifact_result["errors"]:
                summary_lines.append(f"  [error] {e}")
        summary_text = "\n".join(summary_lines) + "\n"

        sys.stdout.write(summary_text)
        sys.stdout.flush()
        try:
            _log_fh.write(summary_text)
        except Exception:
            pass
        _log_fh.close()


if __name__ == "__main__":
    main()
