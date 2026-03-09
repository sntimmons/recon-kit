from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Dict, Any

import pandas as pd

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

    df["last4_ssn"] = df["last4_ssn"].apply(_norm_str).astype("string")
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
