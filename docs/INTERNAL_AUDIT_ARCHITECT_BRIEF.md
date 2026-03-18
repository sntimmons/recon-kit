# Internal Audit Engine - Architect Brief
## Strengths, Weaknesses, and Expansion Roadmap

**Prepared for:** Lead Architect
**Date:** March 2026
**System Version:** Data Whisperer v2.0
**Subject:** Honest assessment of the internal audit engine relative to the reconciliation engine, with a roadmap for where to invest next.

---

## 1. What These Two Systems Are

Before comparing them, it helps to understand what each system was built to do.

**The Reconciliation Engine** (`src/engine.py`, `src/matching.py`, `corrections/`) is the core product. It takes two HR files - one from the legacy system (ADP), one from the target system (Workday) - and matches every employee across both systems, computes what needs to change, and stages a correction manifest. It uses multi-strategy matching (deterministic via worker_id/pk, then probabilistic via name+hire date, last4+dob), confidence scoring, a three-tier gating system (APPROVE / REVIEW / REJECT), and sanity gates that can halt the pipeline if the data looks dangerous. It is a production-grade data migration engine.

**The Internal Audit Engine** (`audit/internal_audit.py`, `audit/reports/`) is a pre-migration data quality scanner. It takes one file - the source HR export - and runs a battery of checks against it. It has no concept of a target system, no matching, no corrections. It answers the question: "Is this file clean enough to run through the reconciliation engine?" Its job is done before the reconciliation engine starts.

They are **not competing systems**. They are sequential steps in a pipeline: audit first, then reconcile.

---

## 2. What the Internal Audit Does Well

### 2.1 Column Normalization (Recently Added)
The ALIASES dict and df_norm pattern correctly handle the wide variety of column name conventions used across HR systems. A file with `Employee_ID`, `Status`, `Join_Date`, `Department_Region` will be fully analyzed as if the columns were named `worker_id`, `worker_status`, `hire_date`, `department`. This is a real problem in the field and the current solution is solid.

**Strength rating:** HIGH. This solves the #1 reason audits missed issues in earlier versions.

### 2.2 Critical Integrity Checks
The following checks are well-implemented and catch the issues that matter most:
- `active_zero_salary` - finds the payroll bombs before they go live
- `duplicate_worker_id` / `duplicate_email` - catches the ID collision problems that corrupt reconciliation
- `ghost_employee_indicator` - multi-field composite check, not trivially fooled
- `phone_invalid` - uses digit-count and value-range logic, not just regex, catches the "all negative integers" problem that regex would miss

**Strength rating:** HIGH for CRITICAL and HIGH severity checks. These are solid.

### 2.3 Status Intelligence
The combination of `status_no_terminated`, `status_high_pending`, and `status_suspicious_value` gives a good picture of whether the status field is trustworthy. These three checks together often identify the single most common pre-migration problem: a workforce file that has been filtered, truncated, or partially loaded.

**Strength rating:** MEDIUM-HIGH. The logic is correct. The 25% threshold for "high pending" is defensible.

### 2.4 Combined Field Detection
The `combined_field` check (finds `Dept-Region` style combined fields using hyphen frequency + cardinality analysis) is clever and practical. This issue - two fields packed into one column - causes silent failures in system loads because the target system expects separate fields.

**Strength rating:** HIGH for what it does. It catches a problem that cannot be found by any column-name check alone.

### 2.5 Age Uniformity
Using unique-value ratio (< 5% = suspicious) to flag placeholder age data is a solid statistical approach. It catches the case where someone populated the age column with "25", "30", "35", "40" for all 1,020 employees without having to hard-code expected values.

**Strength rating:** MEDIUM-HIGH. Could be extended to other fields (salary uniformity, hire date uniformity).

### 2.6 PDF Report Quality
After the rebuild, the PDF report is genuinely useful as a pre-migration briefing document. WHAT/WHY/ACTION structure per finding, narrative executive summary, salary distribution bar chart, and completeness scoring make it something a CHRO can act on, not just a data dump.

**Strength rating:** HIGH (post-rebuild). This is close to production quality.

---

## 3. Where the Internal Audit Is Weak

This is the section your architect needs to read carefully. These are the gaps where issues will slip through to the reconciliation engine.

### 3.1 No Cross-File Comparison
**The biggest gap.** The internal audit analyzes one file in isolation. It cannot tell you:
- Whether worker_ids in the source file exist in the target file
- Whether the employee counts are consistent with what the target system expects
- Whether terminated employees in the source are still showing as active in the target
- Whether departments in the source file exist as valid values in the target system's configuration

The reconciliation engine solves this, but only after running. The ideal sequence is: audit source, audit target, compare counts before reconciling. Currently only the first step exists.

**Impact:** A file can pass all internal audit checks and still fail reconciliation if the target system has different data.

### 3.2 No Reference Data Validation
The audit has no awareness of what valid values look like. It cannot check:
- Whether department names match the target system's cost center codes
- Whether job titles are valid according to the target's position catalog
- Whether location codes are in the accepted list
- Whether pay grades are within policy-defined ranges for the given job title

The reconciliation engine's `corrections/` folder handles some of this, but only after matching. Pre-migration, there is no way to catch "this department code doesn't exist in Workday" before running reconciliation.

**Impact:** Silent mapping failures. The field looks populated, the audit passes, but it loads garbage into Workday.

### 3.3 Salary Outlier Detection Is Weak
The current `salary_outlier` check compares each employee to their department median using a 2.5x threshold. This has two problems:

1. **It uses the dirty file's own medians.** If a whole department has wrong salaries (e.g., all salaries divided by 100 due to a data entry error), the check won't fire because everyone is equally wrong relative to the group median.

2. **The 2.5x threshold is too loose for compliance purposes.** In pay equity analysis, a variance of more than 30% (0.3x) within the same title+department is a red flag. The audit catches this in `pay_equity_flag`, but the outlier check that flags a single employee is too permissive.

**Impact:** Salary errors in files where the whole department is wrong will be missed entirely.

### 3.4 Date Logic Is Not Comprehensive
`impossible_dates` catches the obvious cases (future hire dates, hire before 1950, DOB after hire date). It does not catch:
- Hire dates that are suspiciously clustered (e.g., 500 employees hired on the same day - a common data generation artifact)
- Hire dates that fall on weekends or holidays (not necessarily wrong, but worth flagging)
- Termination dates more than 10 years in the past (stale data that should be archived, not migrated)
- Missing termination dates for employees with "Terminated" status (this IS caught by `status_hire_date_mismatch` - but only if the termination_date column exists)

**Impact:** Wave-hire artifacts and stale historical data slip through to the reconciliation engine.

### 3.5 No Identity Verification Beyond Names and IDs
The audit checks for duplicate IDs and duplicate names. It does not check for:
- Same last4_ssn with different names (possible fraud indicator or data entry error)
- Same email with different names (alias account or shared account)
- Same phone with 50+ employees (clearly a placeholder, like using the HR office phone for everyone)
- Same address for more than N employees (possible ghost employee cluster)

These are composite-field signals that require looking at multiple fields simultaneously. The ghost_employee_indicator check is the only composite check currently, and it requires all four conditions to be true simultaneously - too strict for catching subtler cases.

**Impact:** Sophisticated data quality problems (address reuse, shared contact info) are invisible.

### 3.6 Manager Hierarchy Is Shallow
`manager_loop` only detects 2-level and 3-level cycles (A -> B -> A, A -> B -> C -> A). A 4-level cycle is invisible. More importantly:
- The check does not verify that every manager_id in the file actually corresponds to an existing worker_id
- It does not detect "orphan" branches where the entire management chain leads to a non-existent manager
- It does not count how many employees have no path to the CEO (organizational islands)

**Impact:** Broken org hierarchies load silently into Workday. Approval workflow breaks post-go-live.

### 3.7 No Completeness Thresholds Per Field Type
The current completeness check applies a single threshold (default 20%) to all fields. A critical field like `worker_id` should trigger at even 0.1% blank rate, while an optional field like `middle_name` is fine at 50% blank. The severity mapping does this partially (`_completeness_severity` returns CRITICAL for worker_id), but the threshold itself is still global.

**Impact:** A 15% blank rate on `salary` passes the completeness check even though 15% missing salary is unacceptable for any production migration.

### 3.8 No Encoding or Format Validation
The audit does not check:
- Whether names contain non-printable characters or mojibake (garbled UTF-8 that displays fine in Excel but breaks system loads)
- Whether dates are consistently formatted (mix of MM/DD/YYYY and YYYY-MM-DD in the same column)
- Whether IDs contain spaces, leading zeros, or special characters that will break matching
- Whether salary fields contain currency symbols or commas (`$50,000` vs `50000`)

The engine uses `dtype=str` for everything and `pd.to_numeric(errors='coerce')` which silently handles some of these, but the audit should explicitly flag them.

**Impact:** Format inconsistencies are the #1 cause of "it worked in staging but failed in production" problems.

### 3.9 Statistical Depth Is Limited
The reconciliation engine computes confidence scores for every match using multiple signals. The audit engine has no equivalent depth. It cannot tell you:
- What percentage of employee names are likely to be ambiguous during matching
- Which records are most likely to hit the REVIEW queue based on their data profile
- Whether the file has enough unique identifiers for deterministic matching to succeed

This means the audit gives a clean bill of health but cannot predict reconciliation outcomes.

**Impact:** A file can pass the audit but generate 40% REVIEW rate in reconciliation due to name ambiguity or missing deterministic keys.

---

## 4. Comparison: Internal Audit vs Reconciliation Engine

| Dimension | Internal Audit | Reconciliation Engine |
|---|---|---|
| Input | 1 file (source only) | 2 files (source + target) |
| Output | Issue report, PDF, CSVs | Correction manifest, workbook, matched pairs |
| Purpose | Pre-migration readiness | Migration execution |
| Identity matching | None | Multi-strategy (worker_id, pk, name+date, last4+dob) |
| Confidence scoring | None | Per-pair confidence score [0,1] |
| Salary validation | Statistical outlier | Pair-wise comparison with side-by-side diff |
| Status validation | Distribution analysis | Direct comparison old vs new status |
| Org structure | Shallow (2-3 level loops) | Full hierarchy (via manager_id matching) |
| Reference data | None | Target system values (via new.csv) |
| Automation trigger | Manual or API | API via dashboard |
| Gating | None (report only) | Sanity gate can halt pipeline |
| Encoding/format | Implicit (coerce) | Implicit (coerce) |
| Cross-file logic | None | Full - every check is a comparison |
| Throughput tested | 1,020 records (test) | 21,633+ records (production test) |

---

## 5. Expansion Roadmap - Prioritized

These are the highest-value improvements to the internal audit engine, ranked by impact and implementation effort.

### Priority 1 (Do First - High Impact, Low Effort)

**P1-A: Per-field completeness thresholds**
Define a `FIELD_THRESHOLDS` dict: `worker_id: 0.0` (zero tolerance), `salary: 0.02` (2% max), `email: 0.05` (5% max), `phone: 0.20` (20% max), etc. Apply per-field when building completeness findings instead of the global threshold. This is a config change, not a new check.

**P1-B: Date clustering detection**
After `impossible_dates`, add a check that flags when more than X% of employees share the same hire date. A threshold of 5% with a minimum of 20 employees sharing the same date would catch wave-hire artifacts and data generation errors without false positives.

**P1-C: Contact info reuse detection**
Extend the duplicate checks to flag: phone numbers shared by more than 10 employees, email domains shared by all employees (company email is fine, personal email should vary), addresses shared by more than 20 employees. These are distinct from the existing `duplicate_email` check which looks at exact duplicates.

**P1-D: Encoding/format validation for key fields**
Add `_detect_format_issues(df)` that checks: salary fields for currency symbols/commas (values that parse to NaN but contain digits), ID fields for leading spaces or non-alphanumeric characters, date fields for mixed format detection (regex check for YYYY-MM-DD vs MM/DD/YYYY vs ambiguous formats).

### Priority 2 (Next Quarter - High Impact, Medium Effort)

**P2-A: Reconciliation readiness score**
Add a `recon_readiness` section to the JSON summary that estimates: deterministic match rate (what % of records have a unique worker_id that is likely to match), projected REVIEW rate (based on name ambiguity and missing fields), projected REJECT rate (based on active_zero_salary, impossible_dates rates). This bridges the gap between the audit and the reconciliation engine - it tells the CHRO not just "your data has problems" but "here is how those problems will affect the migration."

**P2-B: Target system reference validation**
Add an optional `--reference-file` argument that accepts the target system export (new.csv). When provided, the audit additionally checks: department names exist in target, manager_ids resolve to employee_ids in target, expected record count is within 10% of target. This turns the single-file audit into a basic cross-file consistency check.

**P2-C: Deeper manager hierarchy analysis**
Replace the current 2-3 level cycle detection with a full graph traversal. Build the full org tree, detect all cycle lengths, identify organizational islands (groups with no path to a root manager), count span-of-control outliers (managers with 50+ direct reports), and flag the percentage of employees who cannot be placed in the hierarchy.

**P2-D: Salary uniformity check (complement to age_uniformity)**
Apply the same unique-ratio logic from `age_uniformity` to salary. If more than 40% of active employees have the same salary value (especially if it is a round number), flag it. This catches the case where a whole department was loaded with the same default salary.

### Priority 3 (Future - Strategic)

**P3-A: Longitudinal comparison (diff between audit runs)**
Store a fingerprint of key metrics from each audit run (total rows, blank rates, severity counts). On subsequent runs against updated files, generate a "what changed" diff: "Worker IDs decreased by 23 - possible record deletion", "Salary missing rate increased from 2% to 8%", "100 new Pending records appeared". This turns the audit from a point-in-time snapshot into a data health trend system.

**P3-B: Pre-flight simulation**
Run a lightweight mock of the reconciliation engine against the file to produce estimated match statistics: "Based on this file, we estimate 87% deterministic matches, 9% probabilistic matches requiring review, 4% unmatched." This requires the audit engine to have a simplified copy of the matching logic, but it is the single highest-value feature for pre-migration planning.

**P3-C: Policy-driven validation rules**
Extend `config/policy.yaml` to support custom field-level validation rules: minimum value, maximum value, allowed values list, required field combos, conditional rules (`if status == Active then salary must be > 0`). This lets enterprise clients configure the audit engine to their specific system requirements without touching the Python code.

---

## 6. Architecture Recommendation

The internal audit and reconciliation engines currently share almost nothing structurally. The audit was built as a standalone script, the reconciliation engine as a full pipeline. As the audit expands, three things need to happen:

1. **Shared utilities module.** `_blank_mask`, `_norm_str`, `_status_column`, `_first_present` exist in both engines with slight variations. These should be extracted to a shared `audit/utils.py` or `src/data_utils.py` that both engines import.

2. **Structured finding schema.** The current finding dict has no formal schema. As new checks are added, the dict grows with ad-hoc keys (`_unique_count`, `_example`, `_sample_value`). Define a `Finding` dataclass or Pydantic model with typed fields. This makes the JSON output stable and the report builder simpler.

3. **Audit as a pipeline gate, not just a report.** Today the audit runs but the reconciliation engine starts regardless of the result. The audit should be able to signal a hard stop: if CRITICAL issues exist above a configurable threshold, the reconciliation should refuse to start. This is the equivalent of the reconciliation engine's sanity gate, applied one step earlier.

---

*This document was generated from direct analysis of the Data Whisperer codebase. All findings reference specific functions and line ranges in the actual code.*
