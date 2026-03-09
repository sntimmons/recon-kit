# Changelog Since Start

Chronological record of engineering milestones for recon-kit. Most recent entries last.

---

## Phase 1 — Core Matching Pipeline

**Milestone**: Multi-tier employee matching engine with conflict resolution.

- `src/mapping.py` — Input normalization: name→`full_name_norm`, location→`location_state`, dedup via `_dedupe_option_a` (active > hire_date > salary)
- `src/matcher.py` — 6 match tiers: worker_id → recon_id → pk → last4_dob → dob_name → name_hire_date; 1-to-1 join per tier
- `resolve_matched_raw.py` — Staged exclusive matching by source priority score; adds `pair_id`; blank worker_id fix (blank IDs never compete for exclusivity)
- `audit/load_sqlite.py` — CSV→SQLite with SSN stripping; dynamic schema from CSV headers; `matched_pairs` VIEW
- `audit/run_audit.py` — Q0 duplicate checks + Q1–Q15 audit queries + packager + mismatch packs
- `audit/schema.sql` — 3-index schema (old_worker_id, new_worker_id, match_source)

---

## Phase 2 — Gating & Correction Files

**Milestone**: Confidence-gated corrections system with Workday-ready output.

- `audit/summary/confidence_policy.py` — Gating thresholds: worker_id=auto-approve, salary/payrate=0.97, status=0.98, hire_date/job_org=0.95
- `audit/summary/gating.py` — `infer_fix_types`, `classify_all`, `classify_row`, `salary_delta`, `build_summary_str`
- `audit/corrections/generate_corrections.py` — 4 correction CSVs + manifest + review_needed; `--only-approved` flag
- `audit/summary/build_review_queue.py` — `review_queue.csv` with priority scores (status=+50, salary=+30/+15, hire_date=+20)
- `audit/summary/reconciliation_summary.py` — Console stats by fix_type and match_source

---

## Phase 3 — Test Harness

**Milestone**: Automated test coverage for all match tiers.

- `generate_test_packs.py` — 8 deterministic test packs (20 rows each)
- `generate_international_pack.py` — International names test (accents, unicode)
- `run_test_pack.py` — Full pipeline per pack with suspicious-match detection; writes `phase3_scorecard.csv`
- `stress_scale_packs.py` — Scale packs by N replicates for load testing
- Known behaviors documented: dup_worker_id_new (correct: 19/20 matched), accent stripping, international_names (tier1=11, tier2=8, tier4=1)

---

## Phase 4 — DIY Exports & Excel Workbook

**Milestone**: Self-service analytics outputs for non-technical reviewers.

- `audit/exports/build_diy_exports.py` — `xlookup_keys.csv` + `wide_compare.csv` (8 key/gating cols + 10 field pairs + computed helpers including `salary_delta`, `salary_ratio`, `suggested_action`)
- `audit/summary/build_workbook.py` — `recon_workbook.xlsx` with 8 sheets (Summary, All_Matches, Salary_Mismatches, Status_Mismatches, HireDate_Mismatches, JobOrg_Mismatches, Review_Queue, Corrections_Manifest); write_only=True mode (MemoryError fix)
- `smoke_check_exports.py` — 4 assertions
- `smoke_check_workbook.py` — 5 assertions

---

## Phase 5 — Sanity Gate & Root Cause

**Milestone**: Automated suspicious-default detection with configurable blocking.

- `config/policy.yaml` — Full YAML config (sanity_gate, confidence_policy, pii, extra_fields, ui_contract)
- `audit/summary/config_loader.py` — `load_policy()` with 4 fallback cases + `[warn]` stderr output; `load_confidence_policy`, `load_extra_fields`, `load_pii_config`
- `audit/summary/sanity_checks.py` — Mismatch distributions + suspicious default detection; `run_sanity_checks()` callable
- `audit/summary/sanity_gate.py` — `evaluate_sanity_gate(results, policy)` — rate + count threshold checks
- `audit/summary/run_sanity_gate.py` — CLI; exits 0/2/3; writes `sanity_results.json` + `sanity_gate.json`
- `audit/summary/root_cause_hire_date.py` — Hire-date default pattern analysis from policy patterns

---

## Phase 6 — Run Folder System & Receipts

**Milestone**: Reproducible timestamped run artifacts with step-level receipts.

- `src/run_manager.py` — `ensure_run_dirs()` creates timestamped run folder with outputs/, audit/, exports/, corrections/, ui/, meta/receipts/, logs/
- `src/run_pipeline.py` — Full orchestration with tee logging; REQUIRED + optional steps; triage mode on gate FAIL
- `src/step_receipts.py` — `write_receipt(run_dirs, step, payload)` → `meta/receipts/<step>.json`
- `src/smoke_check_run_manager.py` — 4 assertions
- `src/smoke_check_pipeline_artifacts.py` — 5 assertions

---

## Phase 7 — UI Pairs Contract

**Milestone**: Stable frontend data contract with versioning.

- `audit/ui/build_ui_pairs.py` — 1 row/pair from matched_pairs; stable 37-column schema; extra field triplets appended
- `audit/ui/contract_v1.json` — Versioned JSON schema (required_columns, optional_columns)
- `audit/ui/smoke_check_ui_pairs.py` — 6 assertions including `ui_contract_version == "v1"`

---

## Phase 8 — Hardening Plan

**Milestone**: Confidence scoring, schema validation, PII controls, config warnings, upstream smoke tests, single-audit CLI.

### Phase 8.1 — Confidence Scoring (all tiers)
- `src/matcher.py`: `compute_confidence(row)` — `name_sim×0.5 + dob×0.2 + last4×0.2 + location_state×0.1`; worker_id/recon_id always 1.0
- `audit/summary/smoke_check_gating.py` Assertion 6: confidence in [0,1]; worker_id rows == 1.0
- E2E fixture: W019/W099 pk match with CA→TX location mismatch → confidence=0.9 < 0.97 → REVIEW(below_threshold)

### Phase 8.2 — Schema Validation
- `audit/schema_validator.py`: `validate_schema(db_path)` — exits 2 if required columns missing (including `confidence`)
- `audit/smoke_check_schema.py` — 3 assertions using temp DBs (not live DB)
- `src/run_pipeline.py`: schema_validator wired as REQUIRED step after load_sqlite

### Phase 8.3 — PII Minimization
- `config/policy.yaml`: `pii.include_dob_in_ui = false`, `pii.include_dob_in_exports = false`
- `audit/ui/build_ui_pairs.py`, `audit/exports/build_diy_exports.py`, `audit/summary/build_workbook.py`: DOB guard on load

### Phase 8.4 — Config Fallback Warnings
- `audit/summary/config_loader.py`: `load_policy()` prints `[warn]` to stderr for all 4 fallback cases (missing file, malformed YAML, ImportError, generic Exception)

### Phase 8.5 — Upstream Smoke Tests
- `tests/smoke_check_mapping.py` — 3 assertions: files exist, required columns present, no blank worker_id/full_name_norm
- `tests/smoke_check_matcher.py` — 4 assertions: files exist, required columns (including confidence), match_source distribution, confidence range
- `tests/smoke_check_resolver.py` — 4 assertions: file exists, no dup old_worker_id, no dup new_worker_id, pair_id unique

### Phase 8.6 — Single Audit CLI
- `src/single_audit.py`: `--type {salary,status,hire_date,job_org}` slice from existing DB; 5 output files
- `src/smoke_check_single_audit.py` — 3 assertions: --help, invalid --type, receipt.json round-trip

---

## Phase 9 — Dynamic Audit Fields

**Milestone**: Config-driven extra field auditing without code changes.

- `config/policy.yaml`: `extra_fields.groups` and `extra_fields.gate` sections
- `audit/summary/config_loader.py`: `load_audit_config(policy)` — returns `{fields, groups, gate}`
- `src/mapping.py`: Preserve configured extra fields from input CSVs after standard column selection
- `src/matcher.py`: Include `old_<field>`/`new_<field>` pairs in base DataFrame for each configured extra field
- `audit/ui/build_ui_pairs.py`: `mismatch_group_<name>` booleans added to output; `mm_<field>` booleans in triplet
- `audit/exports/build_diy_exports.py`: `mismatch_group_<name>` booleans in wide_compare
- `audit/ui/smoke_check_extra_fields.py`: 5-assertion in-memory test for mismatch detection and group booleans
- `tests/run_e2e_test.py`: `cost_center` column added to fixture; Assertion 11 checks `mm_cost_center` and `mismatch_group_org` in wide_compare

---

## Known Open Issues

| Issue | Severity | Notes |
|-------|----------|-------|
| `confidence_policy.py` thresholds not read from YAML at runtime | Medium | Editing policy.yaml does not change active gating; constants in confidence_policy.py are the source of truth |
| SQL injection in `load_sqlite.py` (CSV headers) | Medium | Mitigation: validate CSV before load; whitelist column names |
| CSV injection in correction CSVs | Low | Prefix `=` / `+` / `-` / `@` cells with apostrophe before write |
| No end-to-end test for `mapping.py` | Low | Requires client-specific raw input files |
