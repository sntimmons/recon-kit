# src/build_review_candidates.py
from __future__ import annotations

import os
import re

import pandas as pd

_WDX_RE = re.compile(r'^wdX\d+$')


OLD_CSV = "outputs/unmatched_old_raw.csv"
NEW_CSV = "outputs/unmatched_new_raw.csv"
OUT_CSV = "outputs/review_candidates.csv"

OUTPUT_COLS = [
    "confidence", "score", "name_similarity", "last4_ssn",
    "old_recon_id", "old_worker_id", "old_full_name_norm", "old_dob",
    "old_birth_year", "old_last_name_prefix3", "old_location_state",
    "new_recon_id", "new_worker_id", "new_full_name_norm", "new_dob",
    "new_birth_year", "new_last_name_prefix3", "new_location_state",
    "decision", "notes",
]


def _blankify(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        s = df[c].fillna("").astype(str).str.strip()
        df[c] = s.where(~s.str.lower().isin(["nan", "none", "null"]), "")
    return df


def _dup_note(key: str, old_counts: pd.Series, new_counts: pd.Series) -> str:
    parts = []
    if old_counts.get(key, 0) > 1:
        parts.append(f"dup_in_old({old_counts[key]})")
    if new_counts.get(key, 0) > 1:
        parts.append(f"dup_in_new({new_counts[key]})")
    return "|".join(parts)


def _build_pairs(
    old: pd.DataFrame,
    new: pd.DataFrame,
    key_col: str,
    confidence: str,
    score: int,
    name_similarity: str,
    used_old_idx: set,
    used_new_idx: set,
) -> pd.DataFrame:
    old_sub = old[~old.index.isin(used_old_idx)].copy()
    new_sub = new[~new.index.isin(used_new_idx)].copy()

    # Find keys that appear in both (any count)
    old_keys = set(old_sub[key_col].unique()) - {""}
    new_keys = set(new_sub[key_col].unique()) - {""}
    shared = old_keys & new_keys

    if not shared:
        return pd.DataFrame(columns=OUTPUT_COLS)

    old_counts = old_sub[key_col].value_counts().to_dict()
    new_counts = new_sub[key_col].value_counts().to_dict()

    left  = old_sub[old_sub[key_col].isin(shared)]
    right = new_sub[new_sub[key_col].isin(shared)]

    merged = pd.merge(left, right, on=key_col, suffixes=("_old", "_new"), how="inner")

    rows = []
    for _, r in merged.iterrows():
        key_val = r[key_col]
        note = _dup_note(key_val, old_counts, new_counts)
        rows.append({
            "confidence":              confidence,
            "score":                   str(score),
            "name_similarity":         name_similarity,
            "last4_ssn":               r.get("last4_ssn_old", "") or r.get("last4_ssn_new", "") or r.get("last4_ssn", ""),
            "old_recon_id":            r.get("recon_id_old", ""),
            "old_worker_id":           r.get("worker_id_old", ""),
            "old_full_name_norm":      r.get("full_name_norm_old", ""),
            "old_dob":                 r.get("dob_old", ""),
            "old_birth_year":          r.get("birth_year_old", ""),
            "old_last_name_prefix3":   r.get("last_name_prefix3_old", ""),
            "old_location_state":      r.get("location_state_old", ""),
            "new_recon_id":            r.get("recon_id_new", ""),
            "new_worker_id":           r.get("worker_id_new", ""),
            "new_full_name_norm":      r.get("full_name_norm_new", ""),
            "new_dob":                 r.get("dob_new", ""),
            "new_birth_year":          r.get("birth_year_new", ""),
            "new_last_name_prefix3":   r.get("last_name_prefix3_new", ""),
            "new_location_state":      r.get("location_state_new", ""),
            "decision":                "",
            "notes":                   note,
        })

    return pd.DataFrame(rows, columns=OUTPUT_COLS)


def _resolve_tier_a_duplicates(pairs: pd.DataFrame) -> tuple[pd.DataFrame, int, int, int]:
    """
    For Tier A pairs, when multiple NEW records map to the same OLD record,
    prefer the NEW record whose new_worker_id matches ^wdX\\d+$.

    Returns:
        (resolved_df, groups_found, groups_auto_resolved, rows_removed)
    """
    if pairs.empty:
        return pairs, 0, 0, 0

    group_col = "old_recon_id" if "old_recon_id" in pairs.columns else "old_worker_id"

    groups_found = 0
    groups_auto_resolved = 0
    rows_removed = 0
    drop_indices = []

    for group_key, group in pairs.groupby(group_col):
        if len(group) <= 1:
            continue

        groups_found += 1
        wdx_mask = group["new_worker_id"].str.match(_WDX_RE, na=False)
        wdx_rows = group[wdx_mask]

        if len(wdx_rows) == 1:
            # Exactly one wdX candidate - keep it, drop the rest
            keep_idx = wdx_rows.index[0]
            drop_idx = group.index[group.index != keep_idx].tolist()
            drop_indices.extend(drop_idx)
            rows_removed += len(drop_idx)
            groups_auto_resolved += 1
            # Annotate the kept row
            existing = pairs.at[keep_idx, "notes"]
            pairs.at[keep_idx, "notes"] = (
                (existing + "|" if existing else "") + "auto_preferred_wdX_id"
            )
        elif len(wdx_rows) > 1:
            # Multiple wdX candidates - keep all, annotate
            for idx in group.index:
                existing = pairs.at[idx, "notes"]
                pairs.at[idx, "notes"] = (
                    (existing + "|" if existing else "") + "multiple_wdX_candidates"
                )

    resolved = pairs.drop(index=drop_indices).reset_index(drop=True)
    return resolved, groups_found, groups_auto_resolved, rows_removed


def build_review_candidates(
    old_csv: str = OLD_CSV,
    new_csv: str = NEW_CSV,
    out_csv: str = OUT_CSV,
) -> None:
    if not os.path.exists(old_csv):
        raise FileNotFoundError(f"Not found: {old_csv}")
    if not os.path.exists(new_csv):
        raise FileNotFoundError(f"Not found: {new_csv}")

    old = _blankify(pd.read_csv(old_csv, dtype=str))
    new = _blankify(pd.read_csv(new_csv, dtype=str))

    print(f"loaded old={len(old)} rows  new={len(new)} rows")

    # Build match keys
    # Tier A: full_name_norm + dob
    for df in (old, new):
        df["_key_a"] = ""
        mask = (df["full_name_norm"] != "") & (df["dob"] != "")
        df.loc[mask, "_key_a"] = df.loc[mask, "full_name_norm"] + "|" + df.loc[mask, "dob"]

        # Tier B: full_name_norm + birth_year + last_name_prefix3
        # (location_state excluded - blank in source data)
        df["_key_b"] = ""
        mask2 = (
            (df["full_name_norm"] != "") &
            (df["birth_year"] != "") &
            (df["last_name_prefix3"] != "")
        )
        df.loc[mask2, "_key_b"] = (
            df.loc[mask2, "full_name_norm"] + "|" +
            df.loc[mask2, "birth_year"] + "|" +
            df.loc[mask2, "last_name_prefix3"]
        )

    used_old: set = set()
    used_new: set = set()

    # Tier A - HIGH confidence: name + dob exact match
    pairs_a = _build_pairs(old, new, "_key_a", "HIGH", 2, "1.0", used_old, used_new)

    # Apply wdX duplicate resolution to Tier A before consuming indices
    pairs_a, dup_groups_found, dup_groups_resolved, rows_removed = _resolve_tier_a_duplicates(pairs_a)

    # Track which rows were consumed by Tier A (unique pairs only - 1:1 after merge)
    if not pairs_a.empty:
        a_shared = set(old["_key_a"].unique()) & set(new["_key_a"].unique()) - {""}
        a_old_counts = old["_key_a"].value_counts()
        a_new_counts = new["_key_a"].value_counts()
        unique_a_keys = {k for k in a_shared if a_old_counts.get(k, 0) == 1 and a_new_counts.get(k, 0) == 1}
        used_old.update(old[old["_key_a"].isin(unique_a_keys)].index)
        used_new.update(new[new["_key_a"].isin(unique_a_keys)].index)

    # Tier B - MED confidence: name + birth_year + lname3 (excludes Tier A matches)
    pairs_b = _build_pairs(old, new, "_key_b", "MED", 1, "", used_old, used_new)

    all_pairs = pd.concat([pairs_a, pairs_b], ignore_index=True)

    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    all_pairs.to_csv(out_csv, index=False)

    print(f"Tier A (name+dob) after resolution    : {len(pairs_a)} pairs")
    print(f"Tier B (name+birth_year+lname3)       : {len(pairs_b)} pairs")
    print(f"Total rows written                    : {len(all_pairs)}")
    print(f"Output                                : {out_csv}")
    print()
    print(f"duplicate_groups_found                : {dup_groups_found}")
    print(f"duplicate_groups_auto_resolved        : {dup_groups_resolved}")
    print(f"rows_removed_by_resolution            : {rows_removed}")


if __name__ == "__main__":
    build_review_candidates()
