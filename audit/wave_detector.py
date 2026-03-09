from __future__ import annotations

from pathlib import Path
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
AUDIT = ROOT / "audit"
OUTPUTS = ROOT / "outputs"

MATCHED_RAW = OUTPUTS / "matched_raw.csv"
Q5_HIRE_DATE = AUDIT / "audit_q5_hire_date_mismatches.csv"

OUT_SUMMARY = AUDIT / "audit_q16_hire_date_wave_summary.csv"
OUT_ROWS = AUDIT / "audit_q16_hire_date_wave_rows.csv"
OUT_TOP_NEW_DATES = AUDIT / "audit_q16_hire_date_wave_top_new_dates.csv"


def id_prefix(worker_id: str) -> str:
    """
    Enterprise-friendly prefix bucket.
    Examples:
      wd00103 -> wd001
      wd10021 -> wd100
      empty/NaN -> (blank)
    """
    if worker_id is None or pd.isna(worker_id):
        return ""
    s = str(worker_id).strip().lower()
    if len(s) < 5:
        return s
    return s[:5]


def band_from_rate(rate: float) -> str:
    # keep this simple and readable for enterprise reviewers
    if rate >= 0.40:
        return "critical"
    if rate >= 0.25:
        return "high"
    if rate >= 0.10:
        return "medium"
    return "low"


def main() -> None:
    if not MATCHED_RAW.exists():
        raise FileNotFoundError(f"Missing {MATCHED_RAW}. Run pipeline first.")
    if not Q5_HIRE_DATE.exists():
        raise FileNotFoundError(f"Missing {Q5_HIRE_DATE}. Run audit first.")

    matched = pd.read_csv(MATCHED_RAW)
    q5 = pd.read_csv(Q5_HIRE_DATE)

    # Denominators: total matched pairs per old_worker_id prefix
    if "old_worker_id" not in matched.columns:
        raise ValueError("matched_raw.csv missing old_worker_id column")

    matched["old_id_prefix"] = matched["old_worker_id"].map(id_prefix)
    denom = (
        matched.groupby("old_id_prefix", dropna=False)
        .size()
        .reset_index(name="matched_pairs_total")
    )

    # Numerators: hire date mismatches per prefix
    q5["old_id_prefix"] = q5["old_worker_id"].map(id_prefix)
    numer = (
        q5.groupby("old_id_prefix", dropna=False)
        .size()
        .reset_index(name="hire_date_mismatch_count")
    )

    # Combine
    summary = denom.merge(numer, on="old_id_prefix", how="left")
    summary["hire_date_mismatch_count"] = summary["hire_date_mismatch_count"].fillna(0).astype(int)
    summary["mismatch_rate"] = summary["hire_date_mismatch_count"] / summary["matched_pairs_total"]
    summary["severity_band"] = summary["mismatch_rate"].map(band_from_rate)

    # Sort most suspicious first
    summary = summary.sort_values(
        ["severity_band", "mismatch_rate", "hire_date_mismatch_count"],
        ascending=[True, False, False],
    )

    # Write summary
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUT_SUMMARY, index=False)

    # Drilldown rows: attach total counts and rate to each mismatch row
    q5_rows = q5.merge(
        summary[["old_id_prefix", "matched_pairs_total", "hire_date_mismatch_count", "mismatch_rate", "severity_band"]],
        on="old_id_prefix",
        how="left",
    )

    # Detect repeating “new hire date waves”
    # If a single new_hire_date value appears a lot, it often indicates a default date used in a load.
    if "new_hire_date" in q5_rows.columns:
        new_date_counts = (
            q5_rows.groupby("new_hire_date", dropna=False)
            .size()
            .reset_index(name="cnt")
            .sort_values("cnt", ascending=False)
        )
        new_date_counts.to_csv(OUT_TOP_NEW_DATES, index=False)

        # Add wave_rank bucket
        # Top dates get labeled as wave candidates
        top_dates = set(new_date_counts.head(25)["new_hire_date"].astype(str).tolist())
        q5_rows["new_hire_date_str"] = q5_rows["new_hire_date"].astype(str)
        q5_rows["wave_candidate"] = q5_rows["new_hire_date_str"].isin(top_dates).map(lambda x: "yes" if x else "no")
    else:
        q5_rows["wave_candidate"] = "no"

    q5_rows.to_csv(OUT_ROWS, index=False)

    print("[wave-detector] wrote:", OUT_SUMMARY)
    print("[wave-detector] wrote:", OUT_ROWS)
    print("[wave-detector] wrote:", OUT_TOP_NEW_DATES)


if __name__ == "__main__":
    main()
