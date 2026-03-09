# Engineer Handoff — recon-kit

## Product Overview

`recon-kit` is a workforce reconciliation pipeline that matches employees across two payroll datasets (OLD system → NEW system), detects field-level discrepancies, gates corrections by confidence score, and produces Workday-ready correction files.

---

## Pipeline Phases

### Required (abort on failure)

| # | Script | Input | Output |
|---|--------|-------|--------|
| 1 | `src/mapping.py` | `inputs/old.csv`, `inputs/new.csv` | `outputs/mapped_old.csv`, `outputs/mapped_new.csv` |
| 2 | `src/matcher.py` | mapped CSVs | `outputs/matched_raw.csv`, `match_report.json` |
| 3 | `resolve_matched_raw.py` | `matched_raw.csv` | `matched_raw.csv` (overwritten), conflict CSVs |
| 4 | `audit/load_sqlite.py` | `matched_raw.csv` | `audit/audit.db` |
| 5 | `audit/schema_validator.py` | `audit.db` | (validates — exits 2 on missing columns) |
| 6 | `audit/run_audit.py` | `audit.db` | Q0–Q15 CSV outputs, `audit_runs/` package |

### Optional (pipeline continues on failure)

| # | Script | Output |
|---|--------|--------|
| 7 | `audit/summary/reconciliation_summary.py` | console report |
| 8 | `audit/summary/build_review_queue.py` | `audit/summary/review_queue.csv` |
| 9 | `audit/summary/run_sanity_gate.py` | `sanity_results.json`, `sanity_gate.json` |
| 10 | `audit/summary/root_cause_hire_date.py` | `root_cause_hire_date_*.csv` |
| 11 | `audit/exports/build_diy_exports.py` | `xlookup_keys.csv`, `wide_compare.csv` |
| 12 | `audit/corrections/generate_corrections.py` | 4 correction CSVs + manifest (blocked if gate fails) |
| 13 | `audit/summary/build_workbook.py` | `recon_workbook.xlsx` |
| 14 | `audit/ui/build_ui_pairs.py` | `audit/ui/ui_pairs.csv` |

---

## All Commands

```bash
# Run full pipeline (recommended)
PYTHONUTF8=1 venv/Scripts/python.exe src/run_pipeline.py

# Run individual steps
venv/Scripts/python.exe src/mapping.py
venv/Scripts/python.exe src/matcher.py
venv/Scripts/python.exe resolve_matched_raw.py
venv/Scripts/python.exe audit/load_sqlite.py
venv/Scripts/python.exe audit/schema_validator.py
venv/Scripts/python.exe audit/run_audit.py

# Optional downstream
venv/Scripts/python.exe audit/summary/reconciliation_summary.py
venv/Scripts/python.exe audit/summary/build_review_queue.py
venv/Scripts/python.exe audit/summary/run_sanity_gate.py
venv/Scripts/python.exe audit/exports/build_diy_exports.py
venv/Scripts/python.exe audit/corrections/generate_corrections.py
venv/Scripts/python.exe audit/summary/build_workbook.py
venv/Scripts/python.exe audit/ui/build_ui_pairs.py

# Single-type audit slice (no full re-run needed)
venv/Scripts/python.exe src/single_audit.py --type salary
venv/Scripts/python.exe src/single_audit.py --type status
venv/Scripts/python.exe src/single_audit.py --type hire_date
venv/Scripts/python.exe src/single_audit.py --type job_org
```

---

## I/O Contract

### Input CSVs (`inputs/old.csv`, `inputs/new.csv`)

Required columns: `first_name`, `last_name`, `position`, `dob`, `hire_date`, `location`, `salary`, `payrate`, `worker_status`, `worker_type`, `district`, `last4_ssn`, `address`, `worker_id`, `recon_id`

Extra columns matching `audit.extra_fields` in `config/policy.yaml` are preserved and flow downstream.

### `outputs/matched_raw.csv`

Produced by `matcher.py`, overwritten by `resolve_matched_raw.py`. Key columns:

| Column | Source |
|--------|--------|
| `old_worker_id`, `new_worker_id` | mapping.py |
| `match_source` | matcher.py (worker_id/recon_id/pk/last4_dob/dob_name/name_hire_date) |
| `confidence` | matcher.py — formula: name_sim×0.5 + dob×0.2 + last4×0.2 + location_state×0.1 |
| `pair_id` | resolve_matched_raw.py |

DOB (`old_dob`, `new_dob`) and SSN (`old_last4_ssn`, `new_last4_ssn`) are present in `matched_raw.csv` for matching purposes but are **not** loaded into `audit.db` (SSN stripped by `load_sqlite.py`; DOB suppression controlled by `pii.include_dob_in_ui/exports` in `policy.yaml`).

### `audit/ui/ui_pairs.csv`

1 row per matched pair. Schema governed by `audit/ui/contract_v1.json`. Stable required columns + optional extra field triplets (`old_<field>`, `new_<field>`, `mm_<field>`) + group booleans (`mismatch_group_<name>`).

---

## Gating Rules

Gating is applied in `audit/summary/gating.py` using thresholds from `audit/summary/confidence_policy.py` (loaded from `config/policy.yaml`).

**Auto-approve**: `match_source == worker_id` — bypasses all confidence checks.

**Fix-type thresholds** (minimum confidence required for APPROVE):

| Fix type | Min confidence |
|----------|---------------|
| salary | 0.97 |
| payrate | 0.97 |
| status | 0.98 |
| hire_date | 0.95 |
| job_org | 0.95 |

When confidence is below threshold → `REVIEW(below_threshold)`.
When confidence is missing (pk/lower tiers without score) → `REVIEW(missing_confidence)`.
When confidence is below `low_confidence_floor` (0.80) → `REVIEW(low_confidence)`.

**Important**: Thresholds in `config/policy.yaml` are NOT currently read live by `confidence_policy.py`. The constants in `confidence_policy.py` are the active source of truth. Editing only YAML does not change gating. See [Known Bugs](#known-bugs).

---

## PII Handling

- `old_dob` / `new_dob` present in `matched_raw.csv` (used for matching).
- `old_last4_ssn` / `new_last4_ssn` stripped by `load_sqlite.py` before DB load.
- DOB columns suppressed from downstream outputs (ui_pairs, wide_compare, workbook) when `pii.include_dob_in_ui = false` and `pii.include_dob_in_exports = false` in `config/policy.yaml` (both default to `false` in the shipped config).

---

## Security Notes

- **SQL injection** in `audit/load_sqlite.py`: CSV headers are interpolated into SQL without sanitization. A malformed CSV with a header like `worker_id; DROP TABLE` would be dangerous. Mitigation: validate CSV headers before load, or use parameterized schema creation.
- **CSV injection** in correction CSVs: cell values starting with `=`, `+`, `-`, `@` are not sanitized before writing. Excel/Google Sheets may execute them as formulas. Mitigation: prefix dangerous values with a single quote or tab.
- **Path traversal**: `--out-dir` CLI arguments are not validated. Mitigation: restrict to known output directories in production.

---

## Sanity Gate

`audit/summary/run_sanity_gate.py` runs after `build_review_queue` and checks:

1. **Suspicious hire-date defaults** (e.g., 2026-02) — blocks corrections if rate > 2% or count > 500.
2. **Suspicious salary defaults** (40000, 40003, 40013, 40073) — same thresholds.

When gate fails (exit code 3):
- Corrections blocked (`generate_corrections.py` skipped).
- Workbook and exports still run (triage mode).

Gate behavior is config-driven via `config/policy.yaml` (`sanity_gate` section).

---

## Run Folders

`src/run_pipeline.py` creates a timestamped run folder under `runs/<YYYY-MM-DD_HHMMSS>/`:

```
runs/<timestamp>/
  outputs/          mapped CSVs, matched_raw
  audit/            DB, audit run package
  exports/          wide_compare, xlookup_keys
  corrections/      4 correction CSVs
  ui/               ui_pairs.csv
  meta/
    receipts/       JSON receipt per pipeline step
  logs/
    pipeline.log
```

---

## Failure Modes

| Failure | Exit code | Cause | Fix |
|---------|-----------|-------|-----|
| Schema validation | 2 | `matched_pairs` missing required columns | Re-run pipeline after adding confidence column (re-run matcher) |
| Q0 duplicate | non-zero | Duplicate worker_ids after resolve | Check `conflicts_*.csv` outputs; investigate source data |
| Sanity gate | 3 | Suspicious defaults rate too high | Review `sanity_results.json`; fix source data or adjust thresholds |
| Missing DB | 2 | `audit.db` not found | Run `audit/load_sqlite.py` first |
| openpyxl MemoryError | crash | Writing large sheets in normal mode | Workbook uses `write_only=True`; if OOM, reduce data or increase available memory |

---

## Known Bugs

1. `confidence_policy.py` thresholds are hardcoded in Python. Editing `config/policy.yaml` does **not** change active gating thresholds. The YAML values are documentation only.
2. `load_sqlite.py` interpolates CSV headers directly into SQL — SQL injection risk with malformed input files.
3. Correction CSVs are not sanitized for CSV injection (formula-starting cell values).
4. Smoke test gap: no end-to-end test for `mapping.py` (requires client-specific raw input files).
