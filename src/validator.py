import pandas as pd


def _pct(n: int, d: int) -> str:
    if d == 0:
        return "0%"
    return f"{round((n / d) * 100, 1)}%"


def validate_clean_dataframes(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    compare_fields: list[str],
) -> dict:
    """
    Runs preflight checks after cleaning.

    Returns:
      {
        "errors": [str, ...],
        "warnings": [str, ...],
        "stats": { ... },
      }
    """
    errors: list[str] = []
    warnings: list[str] = []

    # Basic sanity
    if old_df is None or old_df.empty:
        errors.append("Old file: cleaned dataframe is empty.")
    if new_df is None or new_df.empty:
        errors.append("New file: cleaned dataframe is empty.")

    if errors:
        return {"errors": errors, "warnings": warnings, "stats": {}}

    old_rows = len(old_df)
    new_rows = len(new_df)

    # Required core columns we expect from cleaner
    required_core = ["full_name_norm", "dob"]
    for c in required_core:
        if c not in old_df.columns:
            errors.append(f"Old file missing required column: {c}")
        if c not in new_df.columns:
            errors.append(f"New file missing required column: {c}")

    # Compare fields existence check (not fatal, but warn)
    missing_in_old = [c for c in compare_fields if c not in old_df.columns]
    missing_in_new = [c for c in compare_fields if c not in new_df.columns]
    if missing_in_old:
        warnings.append(f"Some compare fields are missing in OLD: {missing_in_old}")
    if missing_in_new:
        warnings.append(f"Some compare fields are missing in NEW: {missing_in_new}")

    # Missing DOB (high impact for fuzzy matching)
    if "dob" in old_df.columns:
        old_missing_dob = int(old_df["dob"].isna().sum())
        if old_missing_dob > 0:
            warnings.append(
                f"Old file has {old_missing_dob}/{old_rows} missing DOB ({_pct(old_missing_dob, old_rows)}). "
                "This increases collision risk for fuzzy matching."
            )

    if "dob" in new_df.columns:
        new_missing_dob = int(new_df["dob"].isna().sum())
        if new_missing_dob > 0:
            warnings.append(
                f"New file has {new_missing_dob}/{new_rows} missing DOB ({_pct(new_missing_dob, new_rows)}). "
                "This increases collision risk for fuzzy matching."
            )

    # Worker ID checks (best matching key)
    if "worker_id" in new_df.columns:
        missing_worker_id = int(new_df["worker_id"].isna().sum()) + int((new_df["worker_id"].astype(str).str.strip() == "").sum())
        if missing_worker_id > 0:
            warnings.append(
                f"New file has {missing_worker_id}/{new_rows} missing worker_id ({_pct(missing_worker_id, new_rows)}). "
                "Worker_id matching will be limited."
            )

        # Duplicate worker_id is a red flag
        non_empty = new_df["worker_id"].astype(str).str.strip()
        non_empty = non_empty[non_empty != ""]
        dup_count = int(non_empty.duplicated().sum())
        if dup_count > 0:
            warnings.append(
                f"New file has {dup_count} duplicate worker_id values. "
                "This can create bad matches. Investigate duplicates before trusting results."
            )

    # Duplicate name+dob collisions (common real-world problem)
    if "full_name_norm" in old_df.columns and "dob" in old_df.columns:
        old_key = old_df["full_name_norm"].astype(str).str.strip() + "|" + old_df["dob"].astype(str).str.strip()
        old_collision = int(old_key.duplicated().sum())
        if old_collision > 0:
            warnings.append(
                f"Old file has {old_collision} duplicate full_name_norm|dob keys. "
                "Expect more 'needs_confirmation' records."
            )

    if "full_name_norm" in new_df.columns and "dob" in new_df.columns:
        new_key = new_df["full_name_norm"].astype(str).str.strip() + "|" + new_df["dob"].astype(str).str.strip()
        new_collision = int(new_key.duplicated().sum())
        if new_collision > 0:
            warnings.append(
                f"New file has {new_collision} duplicate full_name_norm|dob keys. "
                "Expect more 'needs_confirmation' records."
            )

    # Location completeness (common mapping issue)
    for side_name, df in [("Old", old_df), ("New", new_df)]:
        if "location_city" in df.columns:
            missing_city = int(df["location_city"].isna().sum()) + int((df["location_city"].astype(str).str.strip() == "").sum())
            if missing_city > 0:
                warnings.append(
                    f"{side_name} file has {missing_city} missing location_city ({_pct(missing_city, len(df))})."
                )
        if "location_state" in df.columns:
            missing_state = int(df["location_state"].isna().sum()) + int((df["location_state"].astype(str).str.strip() == "").sum())
            if missing_state > 0:
                warnings.append(
                    f"{side_name} file has {missing_state} missing location_state ({_pct(missing_state, len(df))}). "
                    "This may indicate mapping loss (city-only values) or export gaps."
                )

    stats = {
        "old_rows": old_rows,
        "new_rows": new_rows,
        "old_columns": len(old_df.columns),
        "new_columns": len(new_df.columns),
    }

    return {"errors": errors, "warnings": warnings, "stats": stats}


def format_preflight_report(result: dict) -> str:
    errors = result.get("errors", [])
    warnings = result.get("warnings", [])
    stats = result.get("stats", {})

    lines: list[str] = []
    lines.append("DATA WHISPERER - PREFLIGHT REPORT")
    lines.append("--------------------------------")
    lines.append(f"Old rows: {stats.get('old_rows', 'n/a')}")
    lines.append(f"New rows: {stats.get('new_rows', 'n/a')}")
    lines.append(f"Old columns: {stats.get('old_columns', 'n/a')}")
    lines.append(f"New columns: {stats.get('new_columns', 'n/a')}")
    lines.append("")

    if errors:
        lines.append("ERRORS (must fix):")
        for e in errors:
            lines.append(f"- {e}")
        lines.append("")
    else:
        lines.append("ERRORS: none")
        lines.append("")

    if warnings:
        lines.append("WARNINGS (review):")
        for w in warnings:
            lines.append(f"- {w}")
        lines.append("")
    else:
        lines.append("WARNINGS: none")
        lines.append("")

    return "\n".join(lines)
