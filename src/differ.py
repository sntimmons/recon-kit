import re
import pandas as pd


def _safe_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def _norm_text(x) -> str:
    s = _safe_str(x).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _to_float(x):
    s = _safe_str(x)
    if s == "":
        return None
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(s)
    except Exception:
        return None


def _approx_equal_num(a, b, tol=0.01) -> bool:
    if a is None or b is None:
        return False
    return abs(a - b) <= tol


def _severity(field: str, old_val, new_val) -> str:
    """
    More HRIS-realistic severity rules:
    - green: equal (with light normalization)
    - yellow: formatting/low-risk drift
    - red: business-critical mismatch
    """
    f = _norm_text(field)

    o_raw = _safe_str(old_val)
    n_raw = _safe_str(new_val)

    # Treat both blank as equal
    if o_raw == "" and n_raw == "":
        return "green"

    # Normalized equality for common text drift
    if _norm_text(o_raw) == _norm_text(n_raw):
        return "green"

    # Numeric tolerance for money/rates (tiny rounding should not be red)
    money_fields = {"salary", "annual_salary", "base_salary", "payrate", "hourly_rate", "rate"}
    if f in money_fields:
        o_num = _to_float(o_raw)
        n_num = _to_float(n_raw)

        if o_num is None or n_num is None:
            # If one side is blank and the other is not, that's important
            if (o_raw == "" and n_raw != "") or (o_raw != "" and n_raw == ""):
                return "red"
            return "yellow"

        # If equal within a cent, ignore
        if _approx_equal_num(o_num, n_num, tol=0.01):
            return "green"

        # Small drift might be yellow, big drift red
        if abs(o_num - n_num) <= 1.00:
            return "yellow"
        return "red"

    # High-risk business fields
    high_fields = {
        "worker_status",
        "employment_status",
        "status",
        "worker_type",
        "employee_type",
        "location_state",
        "state",
        "hire_date",
        "rehire_date",
        "termination_date",
        "last4_ssn",
        "ssn_last4",
        "ssn4",
        "dob",
        "date_of_birth",
    }
    if f in high_fields:
        # Missing on one side is a red flag for these
        if (o_raw == "" and n_raw != "") or (o_raw != "" and n_raw == ""):
            return "red"
        return "red"

    # Medium fields: could be real, could be mapping/format drift
    medium_fields = {
        "position",
        "job_profile",
        "job_title",
        "cost_center",
        "department",
        "manager",
        "location_city",
        "city",
    }
    if f in medium_fields:
        # Missing on one side is meaningful
        if (o_raw == "" and n_raw != "") or (o_raw != "" and n_raw == ""):
            return "yellow"
        return "yellow"

    # Default: treat as low risk drift
    return "yellow"


def diff_auto_matches(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    matches_df: pd.DataFrame,
    compare_fields: list[str],
) -> pd.DataFrame:
    if matches_df is None or matches_df.empty:
        return pd.DataFrame()

    rows = []
    for _, m in matches_df.iterrows():
        # Defensive: skip rows missing ids
        if "old_row_id" not in m or "new_row_id" not in m:
            continue

        try:
            old_id = int(m["old_row_id"])
            new_id = int(m["new_row_id"])
        except Exception:
            continue

        if old_id not in old_df.index or new_id not in new_df.index:
            continue

        o = old_df.loc[old_id]
        n = new_df.loc[new_id]

        for f in compare_fields:
            old_val = o.get(f, "")
            new_val = n.get(f, "")
            sev = _severity(f, old_val, new_val)
            if sev == "green":
                continue

            rows.append({
                "old_row_id": old_id,
                "new_row_id": new_id,
                "field": f,
                "old_value": old_val,
                "new_value": new_val,
                "severity": sev,
            })

    return pd.DataFrame(rows)
