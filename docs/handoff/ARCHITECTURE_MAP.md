# Architecture Map - recon-kit

## Directory Structure

```
recon-kit/
  config/
    policy.yaml              - Central configuration (thresholds, gates, PII, extra fields)
  inputs/
    old.csv                  - Source: OLD payroll system (raw)
    new.csv                  - Source: NEW payroll system (raw)
  outputs/
    mapped_old.csv           - Normalized OLD data (post-mapping)
    mapped_new.csv           - Normalized NEW data (post-mapping)
    matched_raw.csv          - Match output (overwritten by resolver)
    unmatched_old.csv        - Unmatched OLD rows
    unmatched_new.csv        - Unmatched NEW rows
  src/
    mapping.py               - Input normalization
    matcher.py               - Multi-tier matching engine
    resolve_matched_raw.py   - Conflict resolution (1-to-1 guarantee)
    run_pipeline.py          - Orchestration with run folders
    run_manager.py           - Run folder creation/artifact copying
    step_receipts.py         - Per-step JSON receipts
    schema_validator.py      - (see audit/schema_validator.py)
    single_audit.py          - Single correction-type slice CLI
  audit/
    audit.db                 - SQLite database (matched_pairs_raw + view)
    schema.sql               - Table + index definitions
    run_audit.sql            - Q1-Q15 audit queries
    load_sqlite.py           - CSV → SQLite loader
    run_audit.py             - Runs audit queries + packager
    run_packager.py          - Archives audit run to audit_runs/
    export_mismatch_packs.py - 5 normalized mismatch CSVs
    schema_validator.py      - Pre-flight schema check (required cols + confidence)
    corrections/
      generate_corrections.py - Workday-ready correction CSVs
      out/                    - Output: corrections_*.csv, manifest, review_needed
    exports/
      build_diy_exports.py    - xlookup_keys.csv + wide_compare.csv
      out/                    - Output CSVs
    summary/
      config/
        policy.yaml           - (symlink / copy - see config/policy.yaml)
      config_loader.py        - Policy YAML loader with safe defaults + warnings
      confidence_policy.py    - Gating thresholds (ACTIVE source of truth)
      gating.py               - infer_fix_types, classify_all, classify_row
      build_review_queue.py   - review_queue.csv (mismatch + gating enriched)
      reconciliation_summary.py - Console stats report
      sanity_checks.py        - Mismatch distribution analysis
      sanity_gate.py          - Gate evaluation logic
      run_sanity_gate.py      - Sanity gate CLI
      build_workbook.py       - Excel workbook (8+ sheets)
      root_cause_hire_date.py - Hire-date default pattern analysis
    ui/
      build_ui_pairs.py       - ui_pairs.csv (UI contract v1)
      contract_v1.json        - JSON schema for ui_pairs
      smoke_check_ui_pairs.py - 6-assertion smoke test
      smoke_check_extra_fields.py - Extra fields in-memory test
  test_packs/                 - Phase 3 test fixtures (9 packs)
  tests/
    run_e2e_test.py           - E2E integration test (18-row fixture)
    smoke_check_mapping.py    - 3 assertions on mapped outputs
    smoke_check_matcher.py    - 4 assertions on matched_raw + confidence
    smoke_check_resolver.py   - 4 assertions on 1-to-1 guarantee
  docs/
    handoff/                  - This document set
```

---

## Module Map

### `src/mapping.py`

**Purpose**: Normalize raw input CSVs to a standard column schema.

**Functions**:
- `map_file(input_path, output_path, label)` - reads input CSV, applies normalization, deduplication, writes output
- `_dedupe_option_a(df, report)` - deterministic worker_id dedup (active > hire_date > salary > row order)
- `_norm_str`, `_norm_name`, `_to_date_series`, `_to_num_series` - field normalizers
- `_extract_state(location)` - extracts 2-letter US state from location string
- `_load_extra_fields()` - loads configured extra fields from policy.yaml (non-fatal)

**Inputs**: Raw CSV with standard + extra field columns
**Outputs**: `outputs/mapped_*.csv`, `outputs/mapping_report_*.json`
**Exit codes**: (no explicit exit - exceptions propagate to caller)

---

### `src/matcher.py`

**Purpose**: Match OLD and NEW employees across 6 tiers.

**Match tiers** (applied in order, each tier removes matched rows from the pool):
1. `worker_id` - exact match → confidence=1.0
2. `recon_id` - exact match → confidence=1.0
3. `pk` - full_name_norm + dob + last4_ssn → confidence formula
4. `last4_dob` - last4_ssn + dob → confidence formula
5. `dob_name` - dob + full_name_norm → confidence formula
6. `name_hire_date` - full_name_norm + hire_date → confidence formula

**Confidence formula** (non-worker_id/recon_id): `name_sim×0.5 + dob_match×0.2 + last4_match×0.2 + location_state_match×0.1`

**Functions**:
- `compute_confidence(row)` - returns float [0.0, 1.0]
- `_one_to_one_join(old, new, key_cols, source)` - ambiguous keys discarded
- `_load_extra_fields()` - loads extra fields from policy.yaml

**Outputs**: `outputs/matched_raw.csv`, `outputs/match_report.json`, `outputs/unmatched_*.csv`

---

### `resolve_matched_raw.py`

**Purpose**: Staged conflict resolution - guarantee 1-to-1 matching by resolving duplicate worker_id assignments.

**Key behaviors**:
- Blank worker_ids never compete for exclusivity (pass-through at all tiers)
- Priority by match_source score: worker_id=100, recon_id=90, pk=80, last4_dob=70, dob_name=60, name_hire_date=50
- Adds `pair_id` (unique per row), `source_score`, `exact_worker_id` columns
- Strips internal columns before output: `old_entity_key`, `new_entity_key`, `_ord`, etc.

**Outputs**: Overwrites `outputs/matched_raw.csv`; writes conflict CSVs to `outputs/`

---

### `audit/load_sqlite.py`

**Purpose**: Load `matched_raw.csv` + finalized matches CSV into `audit.db`.

**Key behaviors**:
- Strips `old_last4_ssn`, `new_last4_ssn` from DB load (PII)
- Creates `matched_pairs_raw` table from CSV headers (dynamic schema)
- Creates `matched_pairs` VIEW as `SELECT * FROM matched_pairs_raw`
- Rebuilds 3 indexes after load: `old_worker_id`, `new_worker_id`, `match_source`
- Finalized CSV priority: `finalized_matches.csv` > `finalized_matches_candidates.csv` > `finalized_matches_candidates_1to1.csv`

**Warning**: CSV headers are interpolated into SQL - SQL injection risk with malformed input.

---

### `audit/schema_validator.py`

**Purpose**: Pre-flight schema validation before audit queries run.

**Required columns**: `pair_id`, `match_source`, `confidence`, `old_worker_id`, `new_worker_id`, `old_salary`, `new_salary`, `old_worker_status`, `new_worker_status`, `old_hire_date`, `new_hire_date`

**Exit codes**: 0 = PASS, 2 = FAIL (missing columns or DB not found)

---

### `audit/summary/gating.py`

**Purpose**: Core gating logic - infer fix types and classify each row's action.

**Key functions**:
- `infer_fix_types(row)` - returns list of active mismatches: `salary`, `payrate`, `status`, `hire_date`, `job_org`
- `classify_all(row)` - returns `{action, reason, fix_types, per_fix}`
- `classify_row(row, audit_type)` - single-type classification for `single_audit.py`
- `salary_delta(row)` - numeric salary difference
- `build_summary_str(row, fix_types)` - human-readable change summary
- `_parse_confidence(v)` - safe float parse (returns None if missing/invalid)
- `_norm(v)` - normalize for comparison (strip, lower, None→"")

---

### `audit/summary/config_loader.py`

**Purpose**: Load `config/policy.yaml` with safe defaults; provides typed helpers.

**Key functions**:
- `load_policy(path=None)` - returns full policy dict; prints `[warn]` to stderr on any fallback
- `load_audit_config(policy=None)` - returns `{fields, groups, gate}` for Dynamic Audit Fields
- `load_extra_fields(policy=None)` - shortcut for `load_audit_config()["fields"]`
- `load_confidence_policy(policy=None)` - returns confidence gating thresholds
- `load_pii_config(policy=None)` - returns `{include_dob_in_ui, include_dob_in_exports}`

---

### `audit/ui/build_ui_pairs.py`

**Purpose**: Build UI-ready CSV with 1 row per pair.

**Schema**: Stable required columns + optional extra field triplets + group booleans.
See `docs/handoff/UI_CONTRACT.md` for full column reference.

**Key behaviors**:
- Appends `mismatch_group_<name>` booleans for configured groups
- Suppresses DOB columns if `pii.include_dob_in_ui = false`
- Prints `[warn] extra_field_missing: <field>` for configured but absent extra fields

---

### `src/single_audit.py`

**Purpose**: Run a single correction-type slice from an existing DB without re-running the full pipeline.

**CLI**: `--type {salary,status,hire_date,job_org}` (required), `--db`, `--out-dir`, `--rebuild-db`, `--only-approved`

**Outputs** (in `audit/single_audit/<type>_<timestamp>/`):
- `ui_pairs_<type>.csv`, `review_queue_<type>.csv`, `corrections_<type>.csv`, `manifest_<type>.csv`, `receipt.json`

---

## Data Flow Diagram

```
inputs/old.csv ──┐
                 ├─► mapping.py ─► mapped_old.csv ──┐
inputs/new.csv ──┘                mapped_new.csv ──┤
                                                    ├─► matcher.py ─► matched_raw.csv
                                                                          │
                                                              resolve_matched_raw.py
                                                                          │
                                                              load_sqlite.py → audit.db
                                                                          │
                                                              schema_validator.py
                                                                          │
                                                              run_audit.py → Q0-Q15 CSVs
                                                                          │
                                          ┌───────────────────────────────┤
                                          │                               │
                                   build_review_queue            run_sanity_gate
                                          │                               │
                                   build_diy_exports          generate_corrections (blocked if gate fails)
                                          │                               │
                                   build_workbook                  correction CSVs
                                          │
                                   build_ui_pairs → ui_pairs.csv
```

---

## Configuration Reference

All behavior controlled by `config/policy.yaml`. Key sections:

| Section | Purpose |
|---------|---------|
| `confidence_policy` | Per-source and per-fix-type confidence thresholds |
| `sanity_gate` | Suspicious default detection + block thresholds |
| `pii` | DOB suppression flags |
| `extra_fields` | Dynamic audit columns: fields, groups, gate |
| `ui_contract` | Contract version (currently `v1`) |
