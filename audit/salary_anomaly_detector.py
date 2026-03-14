from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np


ROOT = Path(__file__).resolve().parents[1]
AUDIT = ROOT / "audit"
OUTPUTS = ROOT / "outputs"

MATCHED_RAW = OUTPUTS / "matched_raw.csv"

OUT_ROWS = AUDIT / "audit_q17_salary_anomaly_rows.csv"
OUT_SUMMARY = AUDIT / "audit_q17_salary_anomaly_summary.csv"
OUT_DISTRICT = AUDIT / "audit_q17_salary_anomaly_by_district.csv"
OUT_POSITION = AUDIT / "audit_q17_salary_anomaly_by_position.csv"


def safe_float(x):
    try:
        return float(x)
    except:
        return np.nan


def anomaly_band(pct):
    if pd.isna(pct):
        return "invalid"
    if pct < 0:
        return "negative"
    if pct == 0:
        return "no_change"
    if pct >= 1.0:
        return "over_100_percent"
    if pct >= 0.50:
        return "over_50_percent"
    if pct >= 0.25:
        return "over_25_percent"
    if pct >= 0.15:
        return "over_15_percent"
    return "normal"


def main():

    if not MATCHED_RAW.exists():
        raise FileNotFoundError("Run pipeline first - matched_raw.csv missing")

    df = pd.read_csv(MATCHED_RAW)

    if "old_salary" not in df.columns or "new_salary" not in df.columns:
        raise ValueError("matched_raw missing salary columns")

    df["old_salary"] = df["old_salary"].map(safe_float)
    df["new_salary"] = df["new_salary"].map(safe_float)

    # Remove rows where either salary missing
    df_valid = df.dropna(subset=["old_salary", "new_salary"]).copy()

    # Percent change
    df_valid["salary_change"] = df_valid["new_salary"] - df_valid["old_salary"]
    df_valid["salary_change_pct"] = df_valid["salary_change"] / df_valid["old_salary"]

    df_valid["anomaly_band"] = df_valid["salary_change_pct"].map(anomaly_band)

    # Flag anomalies (anything not "normal")
    df_anom = df_valid[df_valid["anomaly_band"] != "normal"].copy()

    # Sort biggest first
    df_anom = df_anom.sort_values("salary_change_pct", ascending=False)

    AUDIT.mkdir(parents=True, exist_ok=True)
    df_anom.to_csv(OUT_ROWS, index=False)

    # ----------------------------
    # Summary counts
    # ----------------------------
    summary = (
        df_valid.groupby("anomaly_band")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    summary["percent_of_total"] = summary["count"] / len(df_valid)

    summary.to_csv(OUT_SUMMARY, index=False)

    # ----------------------------
    # District clustering
    # ----------------------------
    if "old_district" in df_valid.columns:
        district = (
            df_anom.groupby("old_district")
            .size()
            .reset_index(name="anomaly_count")
            .sort_values("anomaly_count", ascending=False)
        )
        district.to_csv(OUT_DISTRICT, index=False)

    # ----------------------------
    # Position clustering
    # ----------------------------
    if "old_position" in df_valid.columns:
        position = (
            df_anom.groupby("old_position")
            .size()
            .reset_index(name="anomaly_count")
            .sort_values("anomaly_count", ascending=False)
        )
        position.to_csv(OUT_POSITION, index=False)

    print("[salary-detector] wrote:", OUT_ROWS)
    print("[salary-detector] wrote:", OUT_SUMMARY)
    print("[salary-detector] wrote:", OUT_DISTRICT)
    print("[salary-detector] wrote:", OUT_POSITION)


if __name__ == "__main__":
    main()
