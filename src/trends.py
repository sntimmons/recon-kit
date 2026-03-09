import pandas as pd


def _safe_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x)


def detect_trends(mismatches_df: pd.DataFrame, total_matched: int | None = None) -> pd.DataFrame:
    """
    Detect repeated mismatch patterns and return an audit-friendly summary.

    Output columns:
      - field
      - pattern_type
      - old_value
      - new_value
      - delta
      - count
      - percent_of_matched
      - sample_record_keys
      - possible_reason
    """
    if mismatches_df is None or mismatches_df.empty:
        return pd.DataFrame(
            columns=[
                "field",
                "pattern_type",
                "old_value",
                "new_value",
                "delta",
                "count",
                "percent_of_matched",
                "sample_record_keys",
                "possible_reason",
            ]
        )

    df = mismatches_df.copy()

    # Normalize record_key for sampling
    if "record_key" not in df.columns:
        df["record_key"] = ""

    # Determine total matched if not provided
    if total_matched is None or total_matched <= 0:
        total_matched = df["record_key"].nunique()

    def pct(count: int) -> str:
        if not total_matched:
            return "0%"
        return f"{round((count / total_matched) * 100, 1)}%"

    trends = []

    # ----- Numeric delta trends (salary/payrate) -----
    numeric_fields = {"salary", "payrate"}
    for field in numeric_fields:
        fdf = df[df["field"] == field].copy()
        if fdf.empty:
            continue

        fdf["old_num"] = pd.to_numeric(fdf["old_value"], errors="coerce")
        fdf["new_num"] = pd.to_numeric(fdf["new_value"], errors="coerce")
        fdf["delta_num"] = fdf["new_num"] - fdf["old_num"]

        # Count repeated deltas
        counts = fdf["delta_num"].value_counts(dropna=True)
        for delta_val, count in counts.items():
            if pd.isna(delta_val):
                continue
            if count < 2:
                continue

            sample_keys = (
                fdf[fdf["delta_num"] == delta_val]["record_key"]
                .dropna()
                .astype(str)
                .head(3)
                .tolist()
            )

            trends.append(
                {
                    "field": field,
                    "pattern_type": "repeated_numeric_difference",
                    "old_value": "",
                    "new_value": "",
                    "delta": float(delta_val),
                    "count": int(count),
                    "percent_of_matched": pct(int(count)),
                    "sample_record_keys": ", ".join(sample_keys),
                    "possible_reason": "Repeated pay delta detected. Common causes: COL overwrite, rounding, missed effective-dated change, mapping rule.",
                }
            )

    # ----- Repeated value changes (tx -> blank, active -> terminated, etc.) -----
    # Ensure strings for grouping
    df["old_value_s"] = df["old_value"].apply(_safe_str)
    df["new_value_s"] = df["new_value"].apply(_safe_str)

    grouped = (
        df.groupby(["field", "old_value_s", "new_value_s"])
        .agg(
            count=("record_key", "count"),
            sample_record_keys=("record_key", lambda s: ", ".join(s.astype(str).head(3).tolist())),
        )
        .reset_index()
    )

    for _, row in grouped.iterrows():
        count = int(row["count"])
        if count < 2:
            continue

        field = row["field"]
        old_v = row["old_value_s"]
        new_v = row["new_value_s"]

        # More helpful reason text for blanks
        if old_v != "" and new_v == "":
            reason = "Value present in OLD but missing in NEW. Possible mapping loss, export gap, or field not populated in new system."
        elif old_v == "" and new_v != "":
            reason = "Value missing in OLD but present in NEW. Possible enrichment in new system or old export missing column."
        else:
            reason = f"Repeated change from '{old_v}' to '{new_v}'."

        trends.append(
            {
                "field": field,
                "pattern_type": "repeated_value_change",
                "old_value": old_v,
                "new_value": new_v,
                "delta": "",
                "count": count,
                "percent_of_matched": pct(count),
                "sample_record_keys": row["sample_record_keys"],
                "possible_reason": reason,
            }
        )

    out = pd.DataFrame(trends)

    # Sort most impactful first
    if not out.empty:
        out = out.sort_values(by=["count", "field"], ascending=[False, True]).reset_index(drop=True)

    return out
