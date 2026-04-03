# src/run_pipeline.py
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime


ROOT = Path(__file__).resolve().parents[1]

FOCUS_OPTIONS = {
    "full",
    "identity",
    "compensation",
    "status",
    "dates",
    "job_org",
    "corrections",
}

FOCUS_AUDIT_QS = {
    "full": {"q0_old", "q0_new", "q1", "q2", "q3", "q4", "q5"},
    "identity": {"q0_old", "q0_new", "q1"},
    "compensation": {"q1", "q2"},
    "status": {"q1", "q3"},
    "dates": {"q1", "q5"},
    "job_org": {"q1", "q4"},
    "corrections": {"q1"},
}

ISSUE_TEMPLATES = [
    {
        "issue": "Duplicate old worker IDs",
        "file": "audit/audit_q0_duplicate_old_worker_id.csv",
        "severity": "Critical",
        "why": "Duplicate old worker IDs risk bad reconciliation and payroll errors.",
        "what": "De-duplicate old records; choose canonical row; and sync changes.",
    },
    {
        "issue": "Duplicate new worker IDs",
        "file": "audit/audit_q0_duplicate_new_worker_id.csv",
        "severity": "Critical",
        "why": "Duplicate new worker IDs risk wrong downstream worker mapping.",
        "what": "De-duplicate new records and ensure one worker ID per person.",
    },
    {
        "issue": "Pay mismatches",
        "file": "audit/audit_q2_pay_mismatches.csv",
        "severity": "High",
        "why": "Pay mismatches indicate payroll values are out of sync and high-risk.",
        "what": "Validate and correct salary/pay values in source and target records.",
    },
    {
        "issue": "Status mismatches",
        "file": "audit/audit_q3_status_mismatches.csv",
        "severity": "High",
        "why": "Status mismatches may cause improper active/inactive employer actions.",
        "what": "Align worker status between systems for highlighted rows.",
    },
    {
        "issue": "Job org mismatches",
        "file": "audit/audit_q4_job_org_mismatches.csv",
        "severity": "Medium",
        "why": "Job/org mismatches impact cost accounting and reporting structures.",
        "what": "Update job and organizational fields in authoritative record.",
    },
    {
        "issue": "Hire date mismatches",
        "file": "audit/audit_q5_hire_date_mismatches.csv",
        "severity": "Medium",
        "why": "Hire date mismatches affect tenure, benefits and compliance reporting.",
        "what": "Confirm and correct hire dates from HR master data source.",
    },
    {
        "issue": "Corrections manifest",
        "file": "audit/corrections/out/corrections_manifest.csv",
        "severity": "Medium",
        "why": "Corrections manifest shows aggregated correction candidates.",
        "what": "Review and approve correction actions; deploy approved corrections.",
    },
    {
        "issue": "Records requiring review",
        "file": "audit/summary/review_queue.csv",
        "severity": "High",
        "why": "These records require manual review before automatic acceptance.",
        "what": "Open review queue and resolve each candidate per policy.",
    },
]


# ---------------------------------------------------------------------------
# Log tee - mirrors Python-level writes to both console and a log file
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
        print(f"[warning] {label} exited with code {rc} - pipeline continues.")
    return rc


def _read_gate_json(gate_json: Path) -> tuple[bool, dict]:
    """Read sanity_gate.json; return (passed, blocked_outputs). Fail closed on error."""
    if not gate_json.exists():
        return False, {"corrections": True, "workbook": True, "exports": True}
    try:
        with open(str(gate_json), "r", encoding="utf-8") as f:
            data = json.load(f)
        return (
            bool(data.get("passed", True)),
            data.get("blocked_outputs", {"corrections": False, "workbook": False, "exports": False}),
        )
    except Exception:
        return False, {"corrections": True, "workbook": True, "exports": True}


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


def _write_audit_action_plan(root: Path, run_path: Path, focus: str) -> None:
    """Create audit_action_plan.csv in run root based on the focus area."""
    out_path = run_path / "audit_action_plan.csv"
    rows = []

    focus_issues = {
        "full": {
            "Duplicate old worker IDs",
            "Duplicate new worker IDs",
            "Pay mismatches",
            "Status mismatches",
            "Job org mismatches",
            "Hire date mismatches",
            "Corrections manifest",
            "Records requiring review",
        },
        "identity": {"Duplicate old worker IDs", "Duplicate new worker IDs", "Records requiring review"},
        "compensation": {"Pay mismatches", "Corrections manifest", "Records requiring review"},
        "status": {"Status mismatches", "Corrections manifest", "Records requiring review"},
        "dates": {"Hire date mismatches", "Corrections manifest", "Records requiring review"},
        "job_org": {"Job org mismatches", "Corrections manifest", "Records requiring review"},
        "corrections": {"Corrections manifest", "Records requiring review"},
    }.get(focus, set())

    for template in ISSUE_TEMPLATES:
        if template["issue"] not in focus_issues:
            continue

        file_path = root / template["file"]
        count = _csv_rows(file_path) or 0

        rows.append(
            (
                template["issue"],
                count,
                template["severity"],
                template["why"],
                template["what"],
                str(file_path),
            )
        )

    if not rows:
        # At least include note so the file is not empty.
        rows.append(("No relevant audit actions", "N/A", "Info", "No rows for this focus area.", "", ""))

    with open(str(out_path), "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Issue", "Count", "Severity", "Why it matters", "What to do", "Where to look"])
        for row in rows:
            writer.writerow(row)


def _write_run_summary_with_focus(root: Path, run_path: Path, focus: str) -> None:
    """Write run_summary.csv including focus scope and evaluated/missing info."""
    path = run_path / "run_summary.csv"
    summary_rows = []

    summary_rows.extend([
        ("Run Type", "Focused Run (MVP)", "Type of this run", ""),
        ("Focus Area", focus, "Selected focus area", ""),
        ("Scope Note", f"Focused Run: {focus}. Only this category was evaluated. Other issue types were not analyzed.", "", ""),
    ])

    matched_count = _csv_rows(root / "outputs" / "matched_raw.csv") or 0
    review_count = _csv_rows(root / "audit" / "summary" / "review_queue.csv") or 0
    unmatched_old_count = _csv_rows(root / "outputs" / "unmatched_old.csv") or 0
    unmatched_new_count = _csv_rows(root / "outputs" / "unmatched_new.csv") or 0
    conflicts_old_count = _csv_rows(root / "outputs" / "conflicts_old_worker_id_resolution.csv") or 0
    conflicts_new_count = _csv_rows(root / "outputs" / "conflicts_new_worker_id_resolution.csv") or 0
    skipped_missing_count = _csv_rows(root / "outputs" / "skipped_missing_entity_keys.csv") or 0
    ambiguous_path = root / "audit" / "summary" / "ambiguous_identity_groups.csv"
    ambiguous_count = _csv_rows(ambiguous_path) if ambiguous_path.exists() else 0

    summary_rows.extend([
        ("Clean matched records", matched_count, "Records auto-matched by the system.", "outputs/matched_raw.csv"),
        ("Records requiring review", review_count, "Candidate pairs requiring manual review in review_queue.csv.", "audit/summary/review_queue.csv"),
        ("Unmatched source records", unmatched_old_count, "Old-source records with no safe match.", "outputs/unmatched_old.csv"),
        ("Unmatched target records", unmatched_new_count, "New-target records with no safe match.", "outputs/unmatched_new.csv"),
        ("Ambiguous identity records", ambiguous_count, "Rows that could not be resolved due to ambiguous identity.", "audit/summary/ambiguous_identity_groups.csv"),
        ("Conflicts (old side)", conflicts_old_count, "Rows blocked by one-to-one conflict while resolving matches.", "outputs/conflicts_old_worker_id_resolution.csv"),
        ("Conflicts (new side)", conflicts_new_count, "Rows blocked by one-to-one conflict while resolving matches.", "outputs/conflicts_new_worker_id_resolution.csv"),
        ("Skipped missing entity keys", skipped_missing_count, "Rows omitted due to missing entity keys required for safe resolution.", "outputs/skipped_missing_entity_keys.csv"),
    ])

    focus_issue_keys = FOCUS_AUDIT_QS.get(focus, set())

    def issue_entry(issue, key, file):
        if focus == "full" or key in focus_issue_keys:
            val = _csv_rows(root / file)
            if val is None:
                val = 0
            return val
        return "Not evaluated in this run"

    summary_rows.extend([
        ("Duplicate old worker IDs", issue_entry("Duplicate old worker IDs", "q0_old", "audit/audit_q0_duplicate_old_worker_id.csv"), "Duplicate old worker IDs risk bad reconciliation and payroll errors.", "audit/audit_q0_duplicate_old_worker_id.csv"),
        ("Duplicate new worker IDs", issue_entry("Duplicate new worker IDs", "q0_new", "audit/audit_q0_duplicate_new_worker_id.csv"), "Duplicate new worker IDs risk wrong downstream worker mapping.", "audit/audit_q0_duplicate_new_worker_id.csv"),
        ("Pay mismatches", issue_entry("Pay mismatches", "q2", "audit/audit_q2_pay_mismatches.csv"), "Pay mismatches indicate payroll values are out of sync and high-risk.", "audit/audit_q2_pay_mismatches.csv"),
        ("Status mismatches", issue_entry("Status mismatches", "q3", "audit/audit_q3_status_mismatches.csv"), "Status mismatches may cause improper active/inactive employer actions.", "audit/audit_q3_status_mismatches.csv"),
        ("Job org mismatches", issue_entry("Job org mismatches", "q4", "audit/audit_q4_job_org_mismatches.csv"), "Job/org mismatches impact cost accounting and reporting structures.", "audit/audit_q4_job_org_mismatches.csv"),
        ("Hire date mismatches", issue_entry("Hire date mismatches", "q5", "audit/audit_q5_hire_date_mismatches.csv"), "Hire date mismatches affect tenure; benefits and compliance reporting.", "audit/audit_q5_hire_date_mismatches.csv"),
    ])

    # Corrections separately
    corrections_file = "audit/corrections/out/corrections_manifest.csv"
    corrections_count = _csv_rows(root / corrections_file)
    if corrections_count is None:
        corrections_count = "Not evaluated in this run"
    summary_rows.append(("Corrections manifest", corrections_count, "Corrections manifest shows aggregated correction candidates.", corrections_file))

    summary_rows.append(("Audit actions available", "", "See audit_action_plan.csv for prioritized issues", "audit_action_plan.csv"))

    with open(str(path), "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["category", "record_count", "explanation", "file_reference"])
        for row in summary_rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the recon pipeline with optional focus area.")
    parser.add_argument(
        "--focus",
        choices=sorted(FOCUS_OPTIONS),
        default="full",
        help="Scoped focus for audit layer: full, identity, compensation, status, dates, job_org, corrections",
    )
    return parser.parse_args()


def main(focus: str = "full") -> None:
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
        # REQUIRED STEPS - abort on failure
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
        # OPTIONAL STEPS - failures are non-fatal
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

        # --- build_ui_pairs (first pass - before sanity gate) ----------
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

        # Triage: when gate fails, we do NOT block exports/workbook/report -
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
            print(f"\n[pipeline] WARNING: Sanity gate FAILED - triage mode active.")
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
            print("\n[pipeline] Skipping build_diy_exports - blocked by sanity gate.")
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
            print("\n[pipeline] Skipping generate_corrections - blocked by sanity gate (triage mode).")
            _write_receipt(run_paths, "generate_corrections", {
                "inputs":      [],
                "outputs":     [],
                "warnings":    ["skipped - gate failed, corrections blocked in triage mode"],
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
            print("\n[pipeline] Skipping build_workbook - blocked by sanity gate.")
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

        # --- build_ui_pairs (second pass - after exports + gate) -------
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

        _write_run_summary_with_focus(root, run_paths["run"], focus)
        _write_audit_action_plan(root, run_paths["run"], focus)

        run_summary_path = run_paths["run"] / "run_summary.csv"
        write_run_manifest(run_id, run_paths, extra={
            "artifact_copy": artifact_result,
            "pipeline_ok":   pipeline_ok,
            "gate_status":   "PASS" if gate_passed else "FAIL",
            "run_summary":   str(run_summary_path),
            "audit_action_plan": str(run_paths["run"] / "audit_action_plan.csv"),
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
    args = _parse_args()
    main(args.focus)
