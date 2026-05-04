import os
import uuid
import pandas as pd

from cleaner import clean_dataframe
from matcher import match_records
from policy import MatchPolicy
from differ import diff_auto_matches
from history import utc_now_iso, file_fingerprint, append_run_history
from csv_safe import safe_to_csv

DATA_DIR = "data"
OUT_DIR = "outputs"

OLD_FILE = os.path.join(DATA_DIR, "adp.csv")
NEW_FILE = os.path.join(DATA_DIR, "workday.csv")
CONFIRM_FILE = os.path.join(OUT_DIR, "needs_confirmation.csv")

ENGINE_VERSION = "0.2.0"

# Compare mode:
# - "shared" compares all shared columns (minus ignored fields)
# - "fixed" compares only COMPARE_FIELDS_FIXED
COMPARE_MODE = "shared"

IGNORE_COMPARE_FIELDS = {
    "worker_id",
    "full_name_norm",
    "location",
    "address",
    "_name_dob_key",
}

COMPARE_FIELDS_FIXED = [
    "position",
    "hire_date",
    "location_city",
    "location_state",
    "address_norm",
    "salary",
    "payrate",
    "worker_status",
    "worker_type",
    "district",
]

def _safe_read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def _write_df(path: str, df: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    safe_to_csv(df, path)

def _ensure_confirmation_file(needs_confirmation_df: pd.DataFrame) -> None:
    """
    Always write outputs/needs_confirmation.csv.
    If an existing file has decision columns, preserve them by merging on old_recon_id (preferred) or old_row_id.
    """
    if os.path.exists(CONFIRM_FILE):
        try:
            existing = pd.read_csv(CONFIRM_FILE)
        except Exception:
            existing = pd.DataFrame()

        has_decisions = (
            not existing.empty
            and "decision" in existing.columns
            and "confirmed_new_row_id" in existing.columns
        )

        if has_decisions and not needs_confirmation_df.empty:
            merge_key = None
            if "old_recon_id" in existing.columns and "old_recon_id" in needs_confirmation_df.columns:
                merge_key = "old_recon_id"
            elif "old_row_id" in existing.columns and "old_row_id" in needs_confirmation_df.columns:
                merge_key = "old_row_id"

            if merge_key:
                merged = existing.merge(
                    needs_confirmation_df,
                    on=[merge_key],
                    how="left",
                    suffixes=("", "_new")
                )

                for col in needs_confirmation_df.columns:
                    if col == merge_key:
                        continue
                    new_col = f"{col}_new"
                    if new_col in merged.columns:
                        merged[col] = merged[col].where(~merged[col].isna(), merged[new_col])
                        merged = merged.drop(columns=[new_col])

                _write_df(CONFIRM_FILE, merged)
                return

    _write_df(CONFIRM_FILE, needs_confirmation_df)

def load_confirmations(old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    Reads outputs/needs_confirmation.csv if it exists.
    Expects user to fill:
      - decision: confirm | not_same | skip
      - confirmed_new_row_id: integer row id from new_df
    Returns a dataframe in same format as auto_matches_df.
    """
    if not os.path.exists(CONFIRM_FILE):
        return pd.DataFrame()

    try:
        df = pd.read_csv(CONFIRM_FILE)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    if "decision" not in df.columns or "confirmed_new_row_id" not in df.columns:
        return pd.DataFrame()

    df["decision"] = df["decision"].fillna("").astype(str).str.strip().str.lower()

    confirmed = df[df["decision"] == "confirm"].copy()
    if confirmed.empty:
        return pd.DataFrame()

    confirmed["old_row_id"] = pd.to_numeric(confirmed.get("old_row_id"), errors="coerce")
    confirmed["confirmed_new_row_id"] = pd.to_numeric(confirmed["confirmed_new_row_id"], errors="coerce")
    confirmed = confirmed.dropna(subset=["old_row_id", "confirmed_new_row_id"])

    rows = []
    for _, r in confirmed.iterrows():
        old_i = int(r["old_row_id"])
        new_i = int(r["confirmed_new_row_id"])

        if old_i not in old_df.index or new_i not in new_df.index:
            continue

        o = old_df.loc[old_i]
        n = new_df.loc[new_i]

        rows.append({
            "match_type": "confirmed",
            "confidence": float(r.get("confidence", 0) or 0),
            "worker_id": n.get("worker_id", ""),
            "old_row_id": old_i,
            "new_row_id": new_i,
            "old_recon_id": o.get("recon_id", ""),
            "new_recon_id": n.get("recon_id", ""),
            "old_full_name": o.get("full_name_norm", ""),
            "new_full_name": n.get("full_name_norm", ""),
            "reason": "User confirmed match",
        })

    return pd.DataFrame(rows)

def _build_compare_fields(old_df: pd.DataFrame, new_df: pd.DataFrame) -> list[str]:
    if COMPARE_MODE == "fixed":
        return [f for f in COMPARE_FIELDS_FIXED if (f in old_df.columns or f in new_df.columns)]

    shared = sorted(list(set(old_df.columns).intersection(set(new_df.columns))))
    return [c for c in shared if c not in IGNORE_COMPARE_FIELDS]

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    run_id = str(uuid.uuid4())
    run_ts = utc_now_iso()
    policy = MatchPolicy()

    old_raw = _safe_read_csv(OLD_FILE)
    new_raw = _safe_read_csv(NEW_FILE)

    print(
        f"Loaded raw files. old_raw rows={len(old_raw)} cols={len(old_raw.columns)} | "
        f"new_raw rows={len(new_raw)} cols={len(new_raw.columns)}"
    )

    old_df = clean_dataframe(old_raw)
    new_df = clean_dataframe(new_raw)

    _write_df(os.path.join(OUT_DIR, "clean_old.csv"), old_df)
    _write_df(os.path.join(OUT_DIR, "clean_new.csv"), new_df)

    results = match_records(old_df, new_df, policy=policy)
    auto_matches_df = results["auto_matches_df"]
    needs_confirmation_df = results["needs_confirmation_df"]
    unmatched_old_df = results["unmatched_old_df"]
    unmatched_new_df = results["unmatched_new_df"]

    # Stamp run metadata onto outputs
    for d in (auto_matches_df, needs_confirmation_df):
        if not d.empty:
            d["run_id"] = run_id
            d["run_timestamp_utc"] = run_ts
            d["engine_version"] = ENGINE_VERSION

    print(
        f"Matcher results: auto_matches={len(auto_matches_df)} "
        f"needs_confirmation={len(needs_confirmation_df)} "
        f"unmatched_old={len(unmatched_old_df)} unmatched_new={len(unmatched_new_df)}"
    )

    _write_df(os.path.join(OUT_DIR, "auto_matches.csv"), auto_matches_df)
    _ensure_confirmation_file(needs_confirmation_df)
    _write_df(os.path.join(OUT_DIR, "unmatched_old.csv"), unmatched_old_df)
    _write_df(os.path.join(OUT_DIR, "unmatched_new.csv"), unmatched_new_df)

    confirmed_matches_df = load_confirmations(old_df, new_df)
    if not confirmed_matches_df.empty:
        confirmed_matches_df["run_id"] = run_id
        confirmed_matches_df["run_timestamp_utc"] = run_ts
        confirmed_matches_df["engine_version"] = ENGINE_VERSION

    combined_matches = pd.concat([auto_matches_df, confirmed_matches_df], ignore_index=True)

    compare_fields = _build_compare_fields(old_df, new_df)
    print(f"Compare mode: {COMPARE_MODE}")
    print(f"Comparing {len(compare_fields)} fields.")

    mismatches_df = diff_auto_matches(old_df, new_df, combined_matches, compare_fields)
    if not mismatches_df.empty:
        mismatches_df["run_id"] = run_id
        mismatches_df["run_timestamp_utc"] = run_ts
        mismatches_df["engine_version"] = ENGINE_VERSION
    _write_df(os.path.join(OUT_DIR, "mismatches.csv"), mismatches_df)

    # Run history log (append)
    append_run_history(OUT_DIR, {
        "run_id": run_id,
        "run_timestamp_utc": run_ts,
        "engine_version": ENGINE_VERSION,
        "policy_json": policy.to_json(),
        "old_file": OLD_FILE,
        "new_file": NEW_FILE,
        "old_fingerprint": file_fingerprint(OLD_FILE),
        "new_fingerprint": file_fingerprint(NEW_FILE),
        "old_rows": len(old_raw),
        "new_rows": len(new_raw),
        "auto_matches": len(auto_matches_df),
        "needs_confirmation": len(needs_confirmation_df),
        "unmatched_old": len(unmatched_old_df),
        "unmatched_new": len(unmatched_new_df),
    })

    # Safety check: duplicate new_row_id in auto
    dup_new = 0
    if not auto_matches_df.empty and "new_row_id" in auto_matches_df.columns:
        dup_new = int(auto_matches_df["new_row_id"].duplicated().sum())
    print(f"duplicate new_row_id in auto: {dup_new}")

    print("Done.")
    print("- outputs/needs_confirmation.csv")
    print("- outputs/mismatches.csv")
    print("- outputs/run_history.csv")

if __name__ == "__main__":
    main()
