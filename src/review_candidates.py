from __future__ import annotations

import os
import difflib
import pandas as pd


OLD_PATH = "outputs/mapped_unmatched_old.csv"
NEW_PATH = "outputs/mapped_unmatched_new.csv"
OUT_PATH = "outputs/review_last4_pairs.csv"


def _blankify(s: pd.Series) -> pd.Series:
    s = s.fillna("").astype(str).str.strip()
    lower = s.str.lower()
    return s.where(~lower.isin(["nan", "none", "null"]), "")


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name]
    return pd.Series([""] * len(df), index=df.index)


def _normalize_name_for_similarity(name: str) -> str:
    # token-sort to reduce "last first" vs "first last" penalty
    tokens = (name or "").strip().lower().split()
    tokens = [t for t in tokens if t]
    return " ".join(sorted(tokens))


def _name_sim(a: str, b: str) -> float:
    a2 = _normalize_name_for_similarity(a)
    b2 = _normalize_name_for_similarity(b)
    if not a2 or not b2:
        return 0.0
    return difflib.SequenceMatcher(None, a2, b2).ratio()


def _score(o: pd.Series, n: pd.Series) -> tuple[int, str]:
    score = 0

    dob_matched = bool(o["dob"] and o["dob"] == n["dob"])

    # Primary signal: exact DOB
    if dob_matched:
        score += 3
    else:
        # Secondary signal: birth_year only matters when full DOB is unavailable or not matching
        if o["birth_year"] and o["birth_year"] == n["birth_year"]:
            score += 2

    if o["last_name_prefix3"] and o["last_name_prefix3"] == n["last_name_prefix3"]:
        score += 2

    if o["location_state"] and o["location_state"] == n["location_state"]:
        score += 1

    sim = _name_sim(o["full_name_norm"], n["full_name_norm"])
    if sim >= 0.90:
        score += 2

    # Thresholds after removing DOB+birth_year double-counting:
    # - Without name, max is dob(3)+lname3(2)+state(1)=6 => MEDIUM
    # - HIGH should usually require name agreement signal too
    if score >= 7:
        label = "HIGH"
    elif score >= 4:
        label = "MEDIUM"
    else:
        label = "LOW"

    return score, label


def main() -> None:
    if not os.path.exists(OLD_PATH):
        raise FileNotFoundError(f"Missing file: {OLD_PATH}")
    if not os.path.exists(NEW_PATH):
        raise FileNotFoundError(f"Missing file: {NEW_PATH}")

    o = pd.read_csv(OLD_PATH, dtype=str)
    n = pd.read_csv(NEW_PATH, dtype=str)

    for df in (o, n):
        df["last4_ssn"] = _blankify(_col(df, "last4_ssn"))
        df["dob"] = _blankify(_col(df, "dob"))
        df["birth_year"] = _blankify(_col(df, "birth_year"))
        df["last_name_prefix3"] = _blankify(_col(df, "last_name_prefix3")).str.lower()
        df["location_state"] = _blankify(_col(df, "location_state")).str.lower()
        df["full_name_norm"] = _blankify(_col(df, "full_name_norm")).str.lower()
        df["worker_id"] = _blankify(_col(df, "worker_id"))
        df["recon_id"] = _blankify(_col(df, "recon_id"))

    old_last4 = set(o.loc[o["last4_ssn"] != "", "last4_ssn"].unique())
    new_last4 = set(n.loc[n["last4_ssn"] != "", "last4_ssn"].unique())
    overlap = sorted(old_last4.intersection(new_last4))

    rows = []
    for last4 in overlap:
        og = o[o["last4_ssn"] == last4].copy().head(25)
        ng = n[n["last4_ssn"] == last4].copy().head(25)

        for _, orow in og.iterrows():
            for _, nrow in ng.iterrows():
                sc, conf = _score(orow, nrow)
                sim = _name_sim(orow["full_name_norm"], nrow["full_name_norm"])

                rows.append({
                    "confidence": conf,
                    "score": sc,
                    "name_similarity": round(sim, 3),
                    "last4_ssn": last4,

                    "old_recon_id": orow.get("recon_id", ""),
                    "old_worker_id": orow.get("worker_id", ""),
                    "old_full_name_norm": orow.get("full_name_norm", ""),
                    "old_dob": orow.get("dob", ""),
                    "old_birth_year": orow.get("birth_year", ""),
                    "old_last_name_prefix3": orow.get("last_name_prefix3", ""),
                    "old_location_state": orow.get("location_state", ""),

                    "new_recon_id": nrow.get("recon_id", ""),
                    "new_worker_id": nrow.get("worker_id", ""),
                    "new_full_name_norm": nrow.get("full_name_norm", ""),
                    "new_dob": nrow.get("dob", ""),
                    "new_birth_year": nrow.get("birth_year", ""),
                    "new_last_name_prefix3": nrow.get("last_name_prefix3", ""),
                    "new_location_state": nrow.get("location_state", ""),

                    "decision": "",   # MATCH / NO_MATCH / SKIP
                    "notes": "",
                })

    out = pd.DataFrame(rows)

    conf_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    out["conf_rank"] = out["confidence"].map(conf_rank).fillna(9).astype(int)
    out = out.sort_values(["conf_rank", "score", "name_similarity", "last4_ssn"], ascending=[True, False, False, True]).drop(columns=["conf_rank"])

    os.makedirs(os.path.dirname(OUT_PATH) or ".", exist_ok=True)
    out.to_csv(OUT_PATH, index=False)

    print("REVIEW PAIRS REPORT")
    print("-------------------")
    print("overlap last4 count:", len(overlap))
    print("rows:", len(out))
    print("wrote:", OUT_PATH)
    print("HIGH:", int((out["confidence"] == "HIGH").sum()))
    print("MEDIUM:", int((out["confidence"] == "MEDIUM").sum()))
    print("LOW:", int((out["confidence"] == "LOW").sum()))


if __name__ == "__main__":
    main()
