from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Dict, Any

import pandas as pd

# ---------------------------------------------------------------------------
# Column alias resolution
# ---------------------------------------------------------------------------

def _canonical_col(name: str) -> str:
    """Normalise a raw column header for alias lookup.

    Strips whitespace, lowercases, and collapses spaces/hyphens to underscores
    so that "First Name", "first-name", and "First_Name" all map to
    "first_name".
    """
    s = name.strip().lower()
    s = re.sub(r"[\s\-]+", "_", s)
    return s


# Built-in alias table: canonical → standard field name used throughout the
# pipeline.  This table handles the most common HR/payroll column naming
# conventions (SAP, Workday, ADP, legacy flat files, etc.).
_BUILTIN_ALIASES: Dict[str, str] = {
    # ── worker_id ───────────────────────────────────────────────────────────
    "associate_id":           "worker_id",
    "employee_id":            "worker_id",
    "emp_id":                 "worker_id",
    "emp_no":                 "worker_id",
    "employee_no":            "worker_id",
    "employee_number":        "worker_id",
    "staff_id":               "worker_id",
    "person_id":              "worker_id",
    "personnel_number":       "worker_id",
    "payroll_id":             "worker_id",
    "badge_number":           "worker_id",
    "badge_no":               "worker_id",
    "workday_system_id":      "worker_id",
    # ── first_name ──────────────────────────────────────────────────────────
    "preferred_first_name":   "first_name",
    "legal_first_name":       "first_name",
    "fname":                  "first_name",
    "given_name":             "first_name",
    "forename":               "first_name",
    # ── last_name ───────────────────────────────────────────────────────────
    "legal_last_name":        "last_name",
    "lname":                  "last_name",
    "surname":                "last_name",
    "family_name":            "last_name",
    # ── dob ─────────────────────────────────────────────────────────────────
    "date_of_birth":          "dob",
    "birth_date":             "dob",
    "birthdate":              "dob",
    "birthday":               "dob",
    "date_of_birth_mmddyyyy": "dob",
    "dob_date":               "dob",
    # ── hire_date ───────────────────────────────────────────────────────────
    "original_hire_date":     "hire_date",
    "start_date":             "hire_date",
    "employment_start_date":  "hire_date",
    "date_of_hire":           "hire_date",
    "hire_dt":                "hire_date",
    "service_date":           "hire_date",
    "seniority_date":         "hire_date",
    # ── position ────────────────────────────────────────────────────────────
    "job_title":              "position",
    "job_profile":            "position",
    "title":                  "position",
    "job_code":               "position",
    "job_name":               "position",
    "role":                   "position",
    "job_classification":     "position",
    "occupation":             "position",
    # ── salary ──────────────────────────────────────────────────────────────
    "annual_salary":          "salary",
    "annual_base_pay":        "salary",
    "base_salary":            "salary",
    "base_pay":               "salary",
    "gross_salary":           "salary",
    "total_salary":           "salary",
    "annual_compensation":    "salary",
    "annual_pay":             "salary",
    # ── payrate ─────────────────────────────────────────────────────────────
    "hourly_rate":            "payrate",
    "hourly_pay_rate":        "payrate",
    "pay_rate":               "payrate",
    "rate_of_pay":            "payrate",
    "hourly_wage":            "payrate",
    # ── worker_status ───────────────────────────────────────────────────────
    "employment_status":      "worker_status",
    "emp_status":             "worker_status",
    "active_status":          "worker_status",
    "employee_status":        "worker_status",
    # ── worker_type ─────────────────────────────────────────────────────────
    "employment_type":        "worker_type",
    "time_type":              "worker_type",
    "employment_classification": "worker_type",
    "flsa_status":            "worker_type",
    "pay_type":               "worker_type",
    # ── last4_ssn (full SSN accepted; _extract_last4 pulls last 4 digits) ──
    "ssn":                    "last4_ssn",
    "social_security_number": "last4_ssn",
    "social_security":        "last4_ssn",
    "ss_number":              "last4_ssn",
    "ss_no":                  "last4_ssn",
    "tax_id":                 "last4_ssn",
    "national_id":            "last4_ssn",
    # ── location ────────────────────────────────────────────────────────────
    "work_location":          "location",
    "work_location_name":     "location",
    "location_name":          "location",
    "office_location":        "location",
    "city_state":             "location",
    "work_site":              "location",
    "site":                   "location",
    "primary_work_location":  "location",
    # ── district ────────────────────────────────────────────────────────────
    "department_name":        "district",
    "department":             "district",
    "dept":                   "district",
    "dept_name":              "district",
    "cost_center_name":       "district",
    "division":               "district",
    "business_unit":          "district",
    "org_unit":               "district",
    "organizational_unit":    "district",
    "supervisory_org":        "district",
    # ── recon_id ────────────────────────────────────────────────────────────
    "reconciliation_id":      "recon_id",
    "recon_number":           "recon_id",
    # ── cost_center (pass-through extra field) ───────────────────────────────
    "cost_center_code":       "cost_center",
    "cost_center_id":         "cost_center",
    "cc_code":                "cost_center",
}


def _load_yaml_aliases() -> Dict[str, str]:
    """Load column_aliases.yml and return a canonical→standard dict.

    Returns an empty dict on any error so the pipeline degrades gracefully.
    """
    cfg_path = Path(__file__).resolve().parent.parent / "config" / "column_aliases.yml"
    if not cfg_path.exists():
        return {}
    try:
        import yaml  # PyYAML is in requirements.txt
        with cfg_path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
        result: Dict[str, str] = {}
        for standard_name, synonyms in raw.items():
            if isinstance(synonyms, list):
                for syn in synonyms:
                    result[_canonical_col(str(syn))] = standard_name
        return result
    except Exception:
        return {}


def _apply_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """Rename input columns to the standard pipeline names.

    Resolution order (highest wins):
      1. Column is already correctly named → no change.
      2. Built-in alias table (_BUILTIN_ALIASES).
      3. User overrides in config/column_aliases.yml.

    Rules:
    - Column names are canonicalised (lowered + underscores) for lookup only;
      the actual rename uses the original column names in the DataFrame.
    - If a target name is already present in the DataFrame, the ambiguous
      duplicate is left as-is (the existing column wins).
    - If two source columns map to the same target, only the first encountered
      is renamed; the rest are left unchanged.
    """
    aliases: Dict[str, str] = {**_BUILTIN_ALIASES, **_load_yaml_aliases()}

    claimed: set[str] = set(df.columns)   # track which standard names are taken
    rename_map: Dict[str, str] = {}

    for col in df.columns:
        can = _canonical_col(col)
        target = aliases.get(can)
        if target is None or target == col:
            continue
        if target in claimed:
            # Target already occupied (either native or previously renamed)
            continue
        rename_map[col] = target
        claimed.add(target)

    return df.rename(columns=rename_map)


def _extract_last4(x) -> str:
    """Return the last 4 digits of a SSN string (or any digit string).

    Accepts full SSN formats like "123-45-6789" → "6789" as well as values
    that already contain only the last 4 digits ("6789" → "6789").
    Returns "" for null/blank input.
    """
    s = _norm_str(x)
    if not s:
        return ""
    digits = re.sub(r"[^0-9]", "", s)
    return digits[-4:] if len(digits) >= 4 else digits


# Load extra_fields config from policy.yaml (non-fatal if unavailable).
def _load_extra_fields() -> list[str]:
    try:
        _summary = Path(__file__).resolve().parent.parent / "audit" / "summary"
        if str(_summary) not in sys.path:
            sys.path.insert(0, str(_summary))
        from config_loader import load_audit_config  # noqa: PLC0415
        return load_audit_config()["fields"]
    except Exception:
        return []


def _norm_str(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _norm_name(x) -> str:
    s = _norm_str(x).lower()
    s = re.sub(r"[^a-z\s\-']", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.date


def _to_num_series(s: pd.Series) -> pd.Series:
    def _clean(v):
        if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
            return None
        t = str(v).strip()
        if t == "":
            return None
        t = t.replace("$", "").replace(",", "")
        try:
            return float(t)
        except Exception:
            return None

    return s.apply(_clean)


def _ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def _build_full_name_norm(df: pd.DataFrame) -> pd.Series:
    fn = df.get("first_name", pd.Series([""] * len(df))).apply(_norm_name)
    ln = df.get("last_name", pd.Series([""] * len(df))).apply(_norm_name)
    full = (fn + " " + ln).str.strip()
    full = full.replace("", pd.NA)
    return full.astype("string")


def _extract_state(location: str) -> str:
    s = _norm_str(location).upper()

    # common patterns: "Houston, TX", "TX", "Austin TX 78701"
    m = re.search(r"\b([A-Z]{2})\b", s)
    if m:
        return m.group(1)

    return ""


def _dedupe_option_a(df: pd.DataFrame, report: Dict[str, Any]) -> pd.DataFrame:
    """
    Deterministic worker_id deduplication. Priority (highest wins):
      1. worker_status == 'active'
      2. most recent hire_date
      3. highest salary (numeric)
      4. salary present (non-null) over missing
      5. payrate present over missing
      6. original row order (stable tie-breaker)

    Blank worker_id rows are kept as-is (do NOT collapse nulls).
    Blank recon_id rows are kept as-is.
    """
    before = len(df)

    df = _ensure_columns(
        df, ["worker_id", "recon_id", "hire_date", "salary", "payrate", "worker_status"]
    )

    # Stable tie-breaker: preserve original row order
    df = df.copy()
    df["_orig_order"] = range(len(df))

    hd = pd.to_datetime(df["hire_date"], errors="coerce")
    sal = pd.to_numeric(df["salary"], errors="coerce")
    pr = pd.to_numeric(df["payrate"], errors="coerce")

    df["_hd"] = hd
    # active beats all other statuses
    df["_is_active"] = (
        df["worker_status"].astype("string").str.strip().str.lower() == "active"
    ).astype(int)
    df["_sal_val"] = sal          # actual numeric; NaN sorts last with na_position="last"
    df["_sal_nn"] = sal.notna().astype(int)
    df["_pr_nn"] = pr.notna().astype(int)

    df = df.sort_values(
        by=["_is_active", "_hd", "_sal_val", "_sal_nn", "_pr_nn", "_orig_order"],
        ascending=[False, False, False, False, False, True],
        na_position="last",
        kind="mergesort",
    )

    # Dedupe worker_id (worker_id nulls are allowed; do NOT collapse nulls)
    wid = df["worker_id"].astype("string")
    wid_nonnull = wid.notna() & (wid != "")
    worker_id_dupe_rows_before = int(wid[wid_nonnull].duplicated(keep=False).sum())

    keep_df = pd.concat(
        [
            df[~wid_nonnull],  # keep all null/blank worker_id rows
            df[wid_nonnull].drop_duplicates(subset=["worker_id"], keep="first"),
        ],
        ignore_index=True,
    )

    # Dedupe recon_id only where recon_id present (do NOT collapse nulls)
    rid = keep_df["recon_id"].astype("string")
    rid_nonnull = rid.notna() & (rid != "")
    recon_dupe_rows_before = int(rid[rid_nonnull].duplicated(keep=False).sum())

    keep_df = pd.concat(
        [
            keep_df[~rid_nonnull],  # keep all null/blank recon_id rows
            keep_df[rid_nonnull].sort_values(
                by=["_is_active", "_hd", "_sal_val", "_sal_nn", "_pr_nn", "_orig_order"],
                ascending=[False, False, False, False, False, True],
                na_position="last",
                kind="mergesort",
            ).drop_duplicates(subset=["recon_id"], keep="first"),
        ],
        ignore_index=True,
    )

    keep_df = keep_df.drop(
        columns=["_orig_order", "_hd", "_is_active", "_sal_val", "_sal_nn", "_pr_nn"],
        errors="ignore",
    )

    report["option_a"] = {
        "rows_before": int(before),
        "rows_after": int(len(keep_df)),
        "worker_id_dupe_rows_before": int(worker_id_dupe_rows_before),
        "recon_id_dupe_rows_before": int(recon_dupe_rows_before),
    }
    return keep_df


def map_file(input_path: str | Path, output_path: str | Path, label: str) -> None:
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path)
    df.columns = [c.strip() for c in df.columns]

    # Resolve non-standard column names (e.g. "Associate_ID" → "worker_id",
    # "Date_of_Birth" → "dob", "Annual_Salary" → "salary", etc.) before any
    # downstream normalization so that the rest of the function always sees
    # the expected standard names.
    df = _apply_aliases(df)

    expected = [
        "first_name", "last_name", "position", "dob", "hire_date", "location",
        "salary", "payrate", "worker_status", "worker_type", "district",
        "last4_ssn", "address", "worker_id", "recon_id",
    ]
    df = _ensure_columns(df, expected)

    df["worker_id"] = df["worker_id"].apply(_norm_str).replace("", pd.NA).astype("string")
    df["recon_id"] = df["recon_id"].apply(_norm_str).replace("", pd.NA).astype("string")

    df["first_name"] = df["first_name"].apply(_norm_str).astype("string")
    df["last_name"] = df["last_name"].apply(_norm_str).astype("string")
    df["full_name_norm"] = _build_full_name_norm(df)

    df["dob"] = _to_date_series(df["dob"]).astype("string")
    df["hire_date"] = _to_date_series(df["hire_date"]).astype("string")

    df["last4_ssn"] = df["last4_ssn"].apply(_extract_last4).replace("", pd.NA).astype("string")
    df["position"] = df["position"].apply(_norm_str).astype("string")
    df["district"] = df["district"].apply(_norm_str).astype("string")
    df["location"] = df["location"].apply(_norm_str).astype("string")
    df["location_state"] = df["location"].apply(_extract_state).replace("", pd.NA).astype("string")
    df["address"] = df["address"].apply(_norm_str).astype("string")
    df["worker_status"] = df["worker_status"].apply(_norm_str).str.lower().astype("string")
    df["worker_type"] = df["worker_type"].apply(_norm_str).str.lower().astype("string")

    df["salary"] = _to_num_series(df["salary"])
    df["payrate"] = _to_num_series(df["payrate"])

    report: Dict[str, Any] = {
        "label": label,
        "input": str(input_path),
        "output": str(output_path),
        "rows_in": int(len(df)),
        "worker_id_nulls": int(df["worker_id"].isna().sum()),
        "recon_id_nulls": int(df["recon_id"].isna().sum()),
        "salary_nulls": int(pd.isna(df["salary"]).sum()),
        "hire_date_nulls": int(df["hire_date"].isna().sum()),
        "location_state_nulls": int(df["location_state"].isna().sum()),
    }

    df = _dedupe_option_a(df, report)

    out_cols = [
        "worker_id",
        "recon_id",
        "first_name",
        "last_name",
        "full_name_norm",
        "dob",
        "hire_date",
        "last4_ssn",
        "worker_status",
        "worker_type",
        "position",
        "district",
        "location",
        "location_state",
        "address",
        "salary",
        "payrate",
    ]
    df = _ensure_columns(df, out_cols)
    # Save the full input df before column selection so extra fields can be preserved.
    df_full = df.copy()
    df = df[out_cols]

    # Append configured extra fields that were present in the input CSV.
    extra_fields = _load_extra_fields()
    for field in extra_fields:
        if field in df_full.columns:
            df[field] = df_full[field].apply(_norm_str).astype("string")
        else:
            df[field] = pd.NA

    df.to_csv(output_path, index=False)

    report_path = output_path.parent / f"mapping_report_{output_path.stem}.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"[mapping] rows: {report['rows_in']} -> {len(df)}")
    print(f"[mapping] wrote: {output_path}")
    print(f"[mapping] report: {report_path}")
