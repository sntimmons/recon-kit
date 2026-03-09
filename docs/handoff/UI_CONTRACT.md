# UI Contract — ui_pairs.csv v1

## Overview

`audit/ui/ui_pairs.csv` is the stable frontend data contract. One row per matched pair. Schema version is embedded in every row as `ui_contract_version = "v1"`.

**Source**: `audit/ui/build_ui_pairs.py` reads from `audit/audit.db` (`matched_pairs` view).

---

## Required Columns (Stable — never reordered or removed in v1)

| Column | Type | Description |
|--------|------|-------------|
| `ui_contract_version` | string | Always `"v1"` — use to detect schema changes |
| `pair_id` | string | Unique identifier for this matched pair |
| `match_source` | string | How the pair was matched: `worker_id`, `recon_id`, `pk`, `last4_dob`, `dob_name`, `name_hire_date` |
| `old_worker_id` | string | Worker ID in the OLD system |
| `new_worker_id` | string | Worker ID in the NEW system |
| `fix_types` | string | Pipe-separated list of detected mismatches: `salary\|status\|hire_date\|job_org` (empty if no change) |
| `action` | string | Gating decision: `APPROVE` or `REVIEW` |
| `reason` | string | Why the action was assigned (e.g., `below_threshold`, `missing_confidence`, `auto_approve`) |
| `confidence` | float\|blank | Match confidence score [0.0–1.0]; blank when not applicable |
| `min_confidence` | float\|blank | Minimum threshold across active fix_types; blank if no mismatches |
| `priority_score` | int | Numeric priority (higher = more urgent review) |
| `summary` | string | Human-readable change summary (e.g., `salary: 50000→55000`) |
| `has_salary_mismatch` | bool | True if salary differs |
| `has_payrate_mismatch` | bool | True if payrate differs |
| `has_status_mismatch` | bool | True if worker_status differs |
| `has_hire_date_mismatch` | bool | True if hire_date differs |
| `has_job_org_mismatch` | bool | True if position, district, or location_state differs |
| `salary_delta` | float\|blank | new_salary − old_salary (blank if no salary change) |
| `payrate_delta` | float\|blank | new_payrate − old_payrate (blank if no payrate change) |
| `old_salary` | float\|blank | Salary in OLD system |
| `new_salary` | float\|blank | Salary in NEW system |
| `old_payrate` | float\|blank | Hourly payrate in OLD system |
| `new_payrate` | float\|blank | Hourly payrate in NEW system |
| `old_worker_status` | string | Employment status in OLD system |
| `new_worker_status` | string | Employment status in NEW system |
| `old_hire_date` | string | Hire date in OLD system (YYYY-MM-DD) |
| `new_hire_date` | string | Hire date in NEW system (YYYY-MM-DD) |
| `old_position` | string | Job title in OLD system |
| `new_position` | string | Job title in NEW system |
| `old_district` | string | Organizational district in OLD system |
| `new_district` | string | Organizational district in NEW system |
| `old_location_state` | string | US state abbreviation in OLD system |
| `new_location_state` | string | US state abbreviation in NEW system |
| `old_location` | string | Full location string in OLD system (blank if not in DB) |
| `new_location` | string | Full location string in NEW system (blank if not in DB) |
| `old_worker_type` | string | Employment type in OLD system (blank if not in DB) |
| `new_worker_type` | string | Employment type in NEW system (blank if not in DB) |

---

## Optional Columns (Extra Fields — appended after stable block)

When `extra_fields.enabled = true` in `config/policy.yaml`, additional columns are appended in this order:

1. **Group booleans** (one per configured group):
   - `mismatch_group_<name>` — `True` if any field in the group has a mismatch

2. **Per-field triplets** (three columns per extra field):
   - `old_<field>` — value in OLD system
   - `new_<field>` — value in NEW system
   - `mm_<field>` — `True` if old and new values differ (case/whitespace-normalized)

Example with `fields: [cost_center, company]` and `groups: {org: [cost_center, company]}`:
```
mismatch_group_org | old_cost_center | new_cost_center | mm_cost_center | old_company | new_company | mm_company
```

A `[warn] extra_field_missing: <field>` is printed (not fatal) when a configured field is absent from the DB.

---

## Filtering Semantics

| Goal | Filter |
|------|--------|
| Only pairs needing action | `action == "APPROVE"` |
| High-priority review items | `action == "REVIEW"` and `priority_score >= 50` |
| Salary changes only | `has_salary_mismatch == True` |
| Auto-approved corrections | `match_source == "worker_id"` and `action == "APPROVE"` |
| Low-confidence matches | `confidence < 0.80` |
| Extra field org mismatches | `mismatch_group_org == True` |

---

## Pagination

The file has no row limit. For large datasets (>50k rows):
- Filter server-side before sending to the UI.
- Prefer serving `action == "REVIEW"` or mismatch subsets rather than the full file.
- The `priority_score` column enables top-N pagination: `ORDER BY priority_score DESC LIMIT N`.

---

## Versioning

The `ui_contract_version` column allows consumers to detect schema changes. Current version: `v1`.

**v1 guarantees**:
- Required columns above are always present and in the listed order.
- Optional extra field columns are appended after the last required column.
- `pair_id` is unique per row.
- `action` is always `APPROVE` or `REVIEW`.
- `confidence` is in `[0.0, 1.0]` when present (blank for worker_id rows in legacy data).

**Breaking change policy**: A schema change that removes, renames, or reorders required columns increments the version to `v2`. Additive changes (new optional columns) do not increment the version.
