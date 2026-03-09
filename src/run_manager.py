"""
run_manager.py — Run folder management for recon-kit pipeline.

Public API
----------
make_run_id() -> str
    Returns YYYY_MM_DD_HHMMSS (local time).

ensure_run_dirs(run_id: str) -> dict[str, Path]
    Creates runs/<run_id>/ subtree; returns named-path dict.

write_run_manifest(run_id, paths, extra=None) -> Path
    Writes runs/<run_id>/meta/manifest.json; returns the path.

copy_artifacts_to_run(run_id, paths) -> dict[str, list[str]]
    Copies pipeline outputs into the run folder.
    Returns {"copied": [...], "missing": [...], "errors": [...]}.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]   # repo root
RUNS = ROOT / "runs"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def make_run_id() -> str:
    """Return YYYY_MM_DD_HHMMSS (local time)."""
    return datetime.now().strftime("%Y_%m_%d_%H%M%S")


def ensure_run_dirs(run_id: str) -> dict[str, Path]:
    """
    Create the run folder tree under runs/<run_id>/.

    Returns a dict with keys:
        run, inputs, outputs, audit, summary, exports, corrections,
        ui, logs, meta
    """
    base = RUNS / run_id
    paths: dict[str, Path] = {
        "run":         base,
        "inputs":      base / "inputs",
        "outputs":     base / "outputs",
        "audit":       base / "audit",
        "summary":     base / "audit" / "summary",
        "exports":     base / "audit" / "exports",
        "corrections": base / "audit" / "corrections",
        "ui":          base / "ui",
        "logs":        base / "logs",
        "meta":        base / "meta",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    # receipts sub-dir under meta
    (base / "meta" / "receipts").mkdir(parents=True, exist_ok=True)
    return paths


def write_run_manifest(
    run_id: str,
    paths: dict[str, Path],
    extra: dict | None = None,
) -> Path:
    """
    Write runs/<run_id>/meta/manifest.json.

    Includes: run_id, created_at_local, python_exe, repo_root,
              git (best effort), key_output_files, and any extra keys.

    Returns the path to the written file.
    """
    manifest: dict = {
        "run_id":           run_id,
        "created_at_local": datetime.now().isoformat(timespec="seconds"),
        "python_exe":       sys.executable,
        "repo_root":        str(ROOT),
    }

    git_info = _get_git_info()
    if git_info:
        manifest["git"] = git_info

    manifest["key_output_files"] = _list_key_files(paths)

    if extra:
        manifest.update(extra)

    manifest_path = paths["meta"] / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(str(manifest_path), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=str)

    return manifest_path


def copy_artifacts_to_run(
    run_id: str,
    paths: dict[str, Path],
) -> dict[str, list[str]]:
    """
    Copy pipeline artifacts into the run folder.

    Returns
    -------
    {
        "copied":  [label, ...],
        "missing": [label, ...],
        "errors":  ["label: message", ...],
    }
    """
    copied:  list[str] = []
    missing: list[str] = []
    errors:  list[str] = []

    # ------------------------------------------------------------------
    # Inner helpers (close over copied / missing / errors lists)
    # ------------------------------------------------------------------

    def _cp(src: Path, dest: Path, label: str) -> bool:
        if not src.exists():
            missing.append(label)
            return False
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dest))
            copied.append(label)
            return True
        except Exception as exc:
            errors.append(f"{label}: {exc}")
            return False

    def _cp_glob(src_dir: Path, dest_dir: Path, pattern: str, prefix: str) -> None:
        if not src_dir.exists():
            return
        dest_dir.mkdir(parents=True, exist_ok=True)
        for f in sorted(src_dir.glob(pattern)):
            try:
                shutil.copy2(str(f), str(dest_dir / f.name))
                copied.append(f"{prefix}/{f.name}")
            except Exception as exc:
                errors.append(f"{prefix}/{f.name}: {exc}")

    def _cp_tree(src_dir: Path, dest_dir: Path, label: str) -> None:
        if not src_dir.exists():
            missing.append(label)
            return
        try:
            if dest_dir.exists():
                shutil.rmtree(str(dest_dir))
            shutil.copytree(str(src_dir), str(dest_dir))
            copied.append(label)
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    # Source roots
    audit_src       = ROOT / "audit"
    summary_src     = ROOT / "audit" / "summary"
    exports_src     = ROOT / "audit" / "exports"
    corrections_src = ROOT / "audit" / "corrections" / "out"
    ui_src          = ROOT / "audit" / "ui"

    # Destination roots (from paths dict)
    run_audit   = paths["audit"]
    run_summary = paths["summary"]
    run_exports = paths["exports"]
    run_corr    = paths["corrections"]
    run_ui      = paths.get("ui", paths["run"] / "ui")
    run_root    = paths["run"]

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------
    _cp(audit_src / "audit.db", run_audit / "audit.db", "audit/audit.db")

    # ------------------------------------------------------------------
    # Audit CSVs
    # ------------------------------------------------------------------
    _cp_glob(audit_src, run_audit, "audit_q*.csv", "audit")
    for name in (
        "audit_q0_duplicate_old_worker_id.csv",
        "audit_q0_duplicate_new_worker_id.csv",
    ):
        if (audit_src / name).exists():
            _cp(audit_src / name, run_audit / name, f"audit/{name}")

    # ------------------------------------------------------------------
    # Summary outputs
    # ------------------------------------------------------------------
    for name in (
        "recon_summary.md",
        "recon_summary.html",
        "recon_workbook.xlsx",
        "review_queue.csv",
        "sanity_results.json",
        "sanity_gate.json",
    ):
        _cp(summary_src / name, run_summary / name, f"audit/summary/{name}")

    _cp_glob(summary_src, run_summary, "sanity_*.csv", "audit/summary")
    _cp_tree(summary_src / "charts", run_summary / "charts", "audit/summary/charts/")

    # Root-cause reports (written by root_cause_hire_date.py)
    for name in (
        "root_cause_hire_date_defaults.csv",
        "root_cause_hire_date_samples.csv",
    ):
        if (summary_src / name).exists():
            _cp(summary_src / name, run_summary / name, f"audit/summary/{name}")

    # ------------------------------------------------------------------
    # UI pairs dataset
    # ------------------------------------------------------------------
    _cp(ui_src / "ui_pairs.csv", run_ui / "ui_pairs.csv", "ui/ui_pairs.csv")

    # ------------------------------------------------------------------
    # Step receipts (copy entire receipts/ subtree)
    # ------------------------------------------------------------------
    receipts_src  = ROOT / "runs" / "meta" / "receipts"   # not real; receipts are in run_dirs
    # Receipts are written directly into run_paths["meta"]/"receipts" by step_receipts.py
    # so they are already in the right place — no copy needed here.

    # ------------------------------------------------------------------
    # DIY exports
    # ------------------------------------------------------------------
    for name in ("xlookup_keys.csv", "wide_compare.csv"):
        _cp(exports_src / name, run_exports / name, f"audit/exports/{name}")

    # ------------------------------------------------------------------
    # Corrections
    # ------------------------------------------------------------------
    if corrections_src.exists():
        for f in sorted(corrections_src.iterdir()):
            if f.is_file():
                _cp(f, run_corr / f.name, f"audit/corrections/{f.name}")
    else:
        missing.append("audit/corrections/out/")

    # ------------------------------------------------------------------
    # Top-level convenience copies (run root)
    # ------------------------------------------------------------------
    _cp(
        summary_src     / "recon_workbook.xlsx",
        run_root / "recon_workbook.xlsx",
        "recon_workbook.xlsx (root)",
    )
    _cp(
        summary_src     / "review_queue.csv",
        run_root / "review_queue.csv",
        "review_queue.csv (root)",
    )
    _cp(
        exports_src     / "wide_compare.csv",
        run_root / "wide_compare.csv",
        "wide_compare.csv (root)",
    )
    _cp(
        corrections_src / "corrections_manifest.csv",
        run_root / "corrections_manifest.csv",
        "corrections_manifest.csv (root)",
    )

    return {"copied": copied, "missing": missing, "errors": errors}


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _get_git_info() -> dict[str, str] | None:
    """Return {"branch": ..., "commit": ...} via git, or None on any error."""
    try:
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(ROOT),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(ROOT),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return {"branch": branch, "commit": commit}
    except Exception:
        return None


def _list_key_files(paths: dict[str, Path]) -> list[str]:
    """Return POSIX-relative paths for key output files that exist in the run folder."""
    run = paths["run"]
    candidates = [
        "audit/audit.db",
        "audit/summary/recon_workbook.xlsx",
        "audit/summary/review_queue.csv",
        "audit/summary/sanity_gate.json",
        "audit/summary/sanity_results.json",
        "audit/summary/root_cause_hire_date_defaults.csv",
        "audit/summary/root_cause_hire_date_samples.csv",
        "audit/exports/wide_compare.csv",
        "audit/exports/xlookup_keys.csv",
        "audit/corrections/corrections_manifest.csv",
        "ui/ui_pairs.csv",
        "logs/pipeline.log",
        "meta/manifest.json",
    ]
    found = [rel for rel in candidates if (run / rel).exists()]

    # receipts
    receipts_dir = paths.get("meta", run / "meta") / "receipts"
    if receipts_dir.exists():
        for f in sorted(receipts_dir.glob("*.json")):
            rel = f.relative_to(run).as_posix()
            if rel not in found:
                found.append(rel)

    # audit CSVs
    audit_dir = paths.get("audit")
    if audit_dir and audit_dir.exists():
        for f in sorted(audit_dir.glob("audit_q*.csv")):
            rel = f.relative_to(run).as_posix()
            if rel not in found:
                found.append(rel)

    return found
