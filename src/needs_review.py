# src/needs_review.py
from __future__ import annotations

import os
import difflib
import pandas as pd


OLD_PATH = "outputs/unmatched_old.csv"
NEW_PATH = "outputs/unmatched_new.csv"
OUT_PATH = "outputs/needs_review_last4_conflicts.csv"


def _blankify(s: pd.Series) -> pd.Series:
    s = s.fillna("").astype(str).str.strip()
    lower = s.str.lower()
    return s.where(~lower.isin(["nan", "none", "null"]), "")


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name]
    return pd.Series([""] * len(df), index=df.index)


def _lname3(last_name: str) -> str:
    last_name = (last_name or "").strip().lower()
    if not last_name:
        return ""
    return last_name[:3]


def _name_similarity(a: str, b: str) -> float:
    a = (a or "").strip().lower()
    b = (b or "").strip().lower()
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def main() -> None:
    if not os.path.exists(OLD_PATH):
        raise FileNotFoundError(f"Missing file: {OLD_PATH}")
    if not os.path.exists(NEW_PATH):
        raise FileNotFoundError(f"Missing file: {NEW_PATH}")

    old_df = pd.read_csv(OLD_PATH, dtype=str)
    new_df = pd.read_csv(NEW_PATH, dtype=str)

    # Normalize fields used for review
    for df in (old_df, new_df):
        df["last4_ssn"] = _blankify(_col(df, "last4_ssn"))
        df["dob"] = _blankify(_col(df, "dob"))
        df["birth_year"] = _blankify(_col(df, "birth_year"))
        df["location_state"] = _blankify(_col(df, "location_state")).str.lower()
        df["worker_id"] = _blankify(_col(df, "worker_id"))

        # name + last name
        df["full_name_norm"] = _blankify(_col(df, "full_name_norm")).str.lower()
        df["last_name"] = _blankify(_col(df, "last_name"))
        df["last_name_norm"] = _blankify(_col(df, "last_name_norm")).str.lower()

        # build lname3 if missing
        lname_source = df["last_name_norm"]
        df["last_name_prefix3"] = _blankify(_col(df, "last_name_prefix3")).str.lower()
        df.loc[df["last_name_prefix3"] == "", "last_name_prefix3"] = lname_source.apply(_lname3)

        # carry recon_id if present
        if "recon_id" not in df.columns:
            df["recon_id"] = ""

    # Find overlapping last4s between unmatched sets
    old_last4 = set(old_df.loc[old_df["last4_ssn"] != "", "last4_ssn"].unique())
    new_last4 = set(new_df.loc[new_df["last4_ssn"] != "", "last4_ssn"].unique())
    overlap_last4 = sorted(old_last4.intersection(new_last4))

    if not overlap_last4:
        print("NEEDS REVIEW REPORT")
        print("-------------------")
        print("No overlapping last4_ssn between unmatched sets.")
        return

    old_overlap = old_df[old_df["last4_ssn"].isin(overlap_last4)].copy()
    new_overlap = new_df[new_df["last4_ssn"].isin(overlap_last4)].copy()

    rows = []

    # For each last4, cross-join small groups
    for last4 in overlap_last4:
        ogrp = old_overlap[old_overlap["last4_ssn"] == last4].copy()
        ngrp = new_overlap[new_overlap["last4_ssn"] == last4].copy()

        # Limit blow-ups if data is messy
        ogrp = ogrp.head(25)
        ngrp = ngrp.head(25)

        for _, o in ogrp.iterrows():
            for _, n in ngrp.iterrows():
                dob_match = (o["dob"] != "" and n["dob"] != "" and o["dob"] == n["dob"])
                by_match = (o["birth_year"] != "" and n["birth_year"] != "" and o["birth_year"] == n["birth_year"])
                l3_match = (o["last_name_prefix3"] != "" and n["last_name_prefix3"] != "" and o["last_name_prefix3"] == n["last_name_prefix3"])
                state_match = (o["location_state"] != "" and n["location_state"] != "" and o["location_state"] == n["location_state"])

                name_sim = _name_similarity(o["full_name_norm"], n["full_name_norm"])
                name_close = name_sim >= 0.90

                score = int(dob_match) + int(by_match) + int(l3_match) + int(state_match) + int(name_close)

                if score >= 4:
                    flag = "HIGH"
                elif score == 3:
                    flag = "MEDIUM"
                else:
                    flag = "LOW"

                rows.append({
                    "confidence": flag,
                    "score": score,
                    "name_similarity": round(name_sim, 3),
                    "last4_ssn": last4,

                    "dob_match": dob_match,
                    "birth_year_match": by_match,
                    "lname3_match": l3_match,
                    "state_match": state_match,

                    "old_recon_id": o.get("recon_id", ""),
                    "old_worker_id": o.get("worker_id", ""),
                    "old_full_name_norm": o.get("full_name_norm", ""),
                    "old_last_name": o.get("last_name", ""),
                    "old_dob": o.get("dob", ""),
                    "old_birth_year": o.get("birth_year", ""),
                    "old_last_name_prefix3": o.get("last_name_prefix3", ""),
                    "old_location_state": o.get("location_state", ""),

                    "new_recon_id": n.get("recon_id", ""),
                    "new_worker_id": n.get("worker_id", ""),
                    "new_full_name_norm": n.get("full_name_norm", ""),
                    "new_last_name": n.get("last_name", ""),
                    "new_dob": n.get("dob", ""),
                    "new_birth_year": n.get("birth_year", ""),
                    "new_last_name_prefix3": n.get("last_name_prefix3", ""),
                    "new_location_state": n.get("location_state", ""),

                    # reviewer fills these in
                    "review_decision": "",   # MATCH / NO_MATCH / NEEDS_MORE_INFO
                    "review_notes": "",
                })

    review = pd.DataFrame(rows)

    # Sort so easiest confirmations show first
    conf_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    review["confidence_rank"] = review["confidence"].map(conf_rank).fillna(9).astype(int)
    review = review.sort_values(
        ["confidence_rank", "score", "name_similarity", "last4_ssn"],
        ascending=[True, False, False, True],
    ).drop(columns=["confidence_rank"])

    os.makedirs(os.path.dirname(OUT_PATH) or ".", exist_ok=True)
    review.to_csv(OUT_PATH, index=False)

    print("NEEDS REVIEW REPORT")
    print("-------------------")
    print("overlap last4 count:", len(overlap_last4))
    print("rows in review file:", len(review))
    print("wrote:", OUT_PATH)
    print("HIGH:", int((review["confidence"] == "HIGH").sum()))
    print("MEDIUM:", int((review["confidence"] == "MEDIUM").sum()))
    print("LOW:", int((review["confidence"] == "LOW").sum()))


if __name__ == "__main__":
    main()
