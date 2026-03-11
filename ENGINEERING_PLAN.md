# Engineering Plan - recon-kit

Generated: 2026-03-05
Status: SSOT for pre-production hardening work.

---

## Executive Summary

All smoke checks pass except `smoke_check_workbook.py`, which crashes with `MemoryError`
on real data. The workbook is non-functional on any realistic dataset until the streaming
fix is shipped. Before connecting a UI or running a real audit:

1. Fix workbook OOM (blocks main deliverable)
2. Unify confidence policy config (prevents silent operator mis-edits)
3. Strip SSN digits from DB and all downstream outputs (PII minimization)
4. Sanitize corrections CSVs against formula injection (security)
5. Sanitize column headers in SQLite loader (defense-in-depth)

---

## Section 1 - Fix Now (pre-real-audit blockers)

### 1A  Workbook streaming fix - `build_workbook.py`

**Problem:** `openpyxl` default write mode holds every cell object in memory. Writing
six large sheets (23k + 14k + 14k + 14k + 15k + 43k rows) exhausts the heap and raises
`MemoryError`. The workbook cannot be generated on any real dataset.

**Fix:** Switch to `write_only=True` mode. `ws.append()` works unchanged; cells are
streamed to a temp file and never held in memory simultaneously. Use `WriteOnlyCell` for
bold header rows. Drop `freeze_panes`, `auto_filter`, and `column_dimensions` (not
supported in write_only mode - these are cosmetic features).

**Acceptance criteria:**
- `smoke_check_workbook.py` passes all assertions without `MemoryError`
- Output `recon_workbook.xlsx` exists and opens in Excel
- All required sheet names present
- `All_Matches` row count matches `matched_pairs` count in DB
- `Salary_Mismatches` row count matches rows where `fix_types` contains "salary"
- Wall clock under 5 minutes on the current 23k-row dataset

---

### 1B  Confidence thresholds into `policy.yaml` - `confidence_policy.py`, `config_loader.py`

**Problem:** Two separate config files govern "policy":
- `config/policy.yaml` controls the sanity gate
- `confidence_policy.py` (Python source) controls every APPROVE/REVIEW gating decision

An operator editing `policy.yaml` thinking they changed thresholds does nothing.
The only way to change confidence thresholds is to edit Python source.

**Fix:** Add a `confidence_policy:` block to `policy.yaml`. Add `_DEFAULT_CONFIDENCE_POLICY`
and `load_confidence_policy(policy)` to `config_loader.py`. Update `confidence_policy.py`
to load thresholds from config at import time with a fallback to hardcoded defaults if YAML
is absent.

**Acceptance criteria:**
- Changing `status_min_confidence` in `policy.yaml` from 0.98 to 0.90 changes gating output
- Deleting `policy.yaml` falls back to existing hardcoded values - no crash
- `smoke_check_gating.py` still passes 5/5 with current thresholds unchanged

---

### 1C  Strip `last4_ssn` from DB - `load_sqlite.py`

**Problem:** `old_last4_ssn` and `new_last4_ssn` are written to `matched_raw.csv` and
loaded into `audit.db`, flowing into `wide_compare.csv`, `ui_pairs.csv`, the workbook,
and all correction CSVs. The SSN digits served their purpose at match time; retaining
them in the audit DB and every downstream export is unnecessary PII exposure.

**Fix:** Strip `old_last4_ssn` and `new_last4_ssn` from the column list before creating
`matched_pairs_raw` in SQLite. The columns will remain in `matched_raw.csv` for
debugging but will not enter the DB or any downstream file.

**Acceptance criteria:**
- `old_last4_ssn` and `new_last4_ssn` absent from `matched_pairs_raw` and `matched_pairs`
- `wide_compare.csv` and `ui_pairs.csv` do not contain these columns
- All existing smoke tests pass unchanged

---

### 1D  CSV injection sanitization - `generate_corrections.py`

**Problem:** Correction CSVs are opened by end users in Excel / Google Sheets. If any
source field value begins with `=`, `+`, `-`, or `@`, the spreadsheet interprets it as
a formula. There is no sanitization of cell values before writing.

**Fix:** Add `_safe_str(v)` helper. Apply it inside `_write()` to all string columns
before writing any correction CSV. Prefix with `\t` (tab) when a value starts with a
formula character; Excel treats tab-prefixed cells as text.

**Acceptance criteria:**
- A position value `=SUM(A1)` in source data appears as `\t=SUM(A1)` in the CSV
- `smoke_check_corrections.py` passes 8/8 (row counts unchanged)

---

### 1E  Header sanitization - `load_sqlite.py`

**Problem:** CSV column headers are interpolated directly into `CREATE TABLE` SQL. A
header containing `"` breaks the SQL statement. Column names from `PRAGMA table_info`
are used unquoted in `CREATE VIEW` SQL.

**Fix:** Add `_safe_col(name)` that strips `"` and control chars from header names.
Apply before building SQL in `_load_csv_to_table`. Add double-quoting around all column
names in the `CREATE VIEW` statement in `_create_views`.

**Acceptance criteria:**
- A CSV with a column header containing `"` loads without SQL error
- `matched_pairs` view is created correctly using quoted identifiers
- All smoke tests pass

---

## Section 2 - Pre-UI Hardening

These items are required before any web front-end connects to the data:

| # | Item | File | Why |
|---|------|------|-----|
| H1 | Schema validation before gating | `gating.py` / `build_ui_pairs.py` | `row.get()` returns None silently if DB is missing expected columns; add explicit check and fail loudly |
| H2 | Unify `sanity_gate` fallback to fail-closed | `config_loader.py` | Missing `policy.yaml` disables sanity gate silently; add a `[WARN]` print and document the `--allow-default-policy` escape hatch |
| H3 | Add data retention TTL to run archive | `audit/run_packager.py` | Archives accumulate unbounded; add `--purge-before DATE` CLI option |
| H4 | Operator runbook (separate doc) | new `docs/OPERATOR_RUNBOOK.md` | Document: what PII each file contains, who should have access, how to securely delete a run, what to do if a CSV is accidentally shared |
| H5 | Remove DOB from downstream outputs | `load_sqlite.py` | After matching, `old_dob` / `new_dob` have no audit utility; extend `_STRIP_COLS` to include them |
| H6 | Config integrity check | `run_pipeline.py` | If `policy.yaml` is missing, emit a visible error before corrections are generated rather than silently proceeding with defaults |

---

## Section 3 - Tests to Add

### T1  End-to-end integration test (wire into single command)

**File:** `tests/run_e2e_test.py`
**Command:** `PYTHONUTF8=1 venv/Scripts/python.exe tests/run_e2e_test.py`

**Fixture:** 20 pre-mapped employees covering all fix types:
- 16 worker_id matches: salary(4), status(3), hire_date(4), job_org(3), multi-fix(1), no-change(1)
- 2 pk matches: one with salary mismatch (→ REVIEW), one clean (→ APPROVE)
- 2 unmatched old + 2 unmatched new

**Asserts (correctness, not just shape):**
1. `matched_total == 18`, `unmatched_old == 2`, `unmatched_new == 2`
2. `matched_by_worker_id == 16`, `matched_by_pk == 2`
3. Q0 PASS (no duplicate worker_ids after resolve)
4. `corrections_salary.csv` has exactly 5 rows (4 direct + 1 multi-fix)
5. `corrections_status.csv` has exactly 4 rows
6. `corrections_hire_date.csv` has exactly 4 rows
7. `corrections_job_org.csv` has exactly 3 rows
8. `review_needed.csv` has exactly 1 row (the pk match with salary mismatch)
9. Sample value check: the salary correction for the multi-fix row has `compensation_amount` == the expected new salary

---

### T2  Correction value correctness

**Gap:** `smoke_check_corrections.py` only checks row counts and headers.

**Add:** For a known pair, assert that `compensation_amount` == `new_salary` from the
matched pair row, `hire_date` == `new_hire_date`, `worker_status` == `new_worker_status`.

---

### T3  Confidence threshold live-reload

**Gap:** No test verifies that changing `policy.yaml` changes gating output.

**Add:** Small in-memory test in `smoke_check_gating.py` that calls `classify_row` twice -
once with default policy, once with a modified policy that lowers a threshold - and
asserts different outcomes.

---

### T4  Upstream pipeline smoke checks

**Gap:** No smoke tests for `mapping.py`, `matcher.py`, or `resolve_matched_raw.py`.

**Add:**
- `src/smoke_check_matcher.py` - 3 assertions: runs on test pack, matched count > 0, Q0 PASS
- `src/smoke_check_resolver.py` - 3 assertions: runs on matched_raw, Q0 PASS after resolve, no blank entity key collisions

---

### Coverage summary

| Smoke check | Status | Gap |
|---|---|---|
| run_manager | 4/4 PASS | - |
| pipeline_artifacts | 6/6 PASS | - |
| sanity_gate | 2/2 PASS | JSON file check requires prior gate run |
| gating | 5/5 PASS | No live-reload test (T3) |
| ui_pairs | 6/6 PASS | - |
| exports | 4/4 PASS | - |
| corrections | 8/8 PASS | No value correctness (T2) |
| workbook | FAIL (OOM) | Blocked until 1A ships |
| matcher | missing | T4 |
| resolver | missing | T4 |
| mapping | missing | T4 |
| e2e | missing | T1 |
