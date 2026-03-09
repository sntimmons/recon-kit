# src/diagnostics.py
from __future__ import annotations

import pandas as pd


def _blank(s: pd.Series) -> pd.Series:
    s = s.fillna("").astype(str).str.strip()
    s = s.where(~s.str.lower().isin(["nan", "none", "null"]), "")
    return s


def _safe_get(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    return _blank(df[col])


def _print_overlap(a: pd.Series, b: pd.Series, label: str, min_len: int = 1) -> None:
    a_set = set(x for x in a.tolist() if len(x) >= min_len)
    b_set = set(x for x in b.tolist() if len(x) >= min_len)
    inter = a_set.intersection(b_set)
    print(f"\n[{label}]")
    print("  unique new:", len(a_set))
    print("  unique old:", len(b_set))
    print("  overlap:", len(inter))


def main() -> None:
    new_path = "outputs/mapped_unmatched_new.csv"
    old_path = "outputs/mapped_unmatched_old.csv"

    n = pd.read_csv(new_path, dtype=str)
    o = pd.read_csv(old_path, dtype=str)

    # Pull fields
    n_last4 = _safe_get(n, "last4_ssn")
    o_last4 = _safe_get(o, "last4_ssn")

    n_dob = _safe_get(n, "dob")
    o_dob = _safe_get(o, "dob")

    n_birth = _safe_get(n, "birth_year")
    o_birth = _safe_get(o, "birth_year")

    n_l3 = _safe_get(n, "last_name_prefix3").str.lower()
    o_l3 = _safe_get(o, "last_name_prefix3").str.lower()

    n_state = _safe_get(n, "location_state").str.lower()
    o_state = _safe_get(o, "location_state").str.lower()

    print("DIAGNOSTICS REPORT")
    print("------------------")
    print("new rows:", len(n))
    print("old rows:", len(o))

    # Missingness snapshot
    def pct_blank(s: pd.Series) -> float:
        return round(100.0 * (s.eq("").sum() / max(len(s), 1)), 2)

    print("\nBLANK RATES")
    print("  new last4_ssn blank:", n_last4.eq("").sum(), f"({pct_blank(n_last4)}%)")
    print("  old last4_ssn blank:", o_last4.eq("").sum(), f"({pct_blank(o_last4)}%)")
    print("  new dob blank:", n_dob.eq("").sum(), f"({pct_blank(n_dob)}%)")
    print("  old dob blank:", o_dob.eq("").sum(), f"({pct_blank(o_dob)}%)")
    print("  new birth_year blank:", n_birth.eq("").sum(), f"({pct_blank(n_birth)}%)")
    print("  old birth_year blank:", o_birth.eq("").sum(), f"({pct_blank(o_birth)}%)")
    print("  new last_name_prefix3 blank:", n_l3.eq("").sum(), f"({pct_blank(n_l3)}%)")
    print("  old last_name_prefix3 blank:", o_l3.eq("").sum(), f"({pct_blank(o_l3)}%)")
    print("  new location_state blank:", n_state.eq("").sum(), f"({pct_blank(n_state)}%)")
    print("  old location_state blank:", o_state.eq("").sum(), f"({pct_blank(o_state)}%)")

    # Basic overlaps
    _print_overlap(n_last4, o_last4, "last4_ssn overlap", min_len=1)
    _print_overlap(n_dob, o_dob, "dob overlap", min_len=4)
    _print_overlap(n_birth, o_birth, "birth_year overlap", min_len=4)
    _print_overlap(n_l3, o_l3, "last_name_prefix3 overlap", min_len=3)
    _print_overlap(n_state, o_state, "location_state overlap", min_len=2)

    # Combo overlaps that matter
    n_k_last4_dob = n_last4 + "|" + n_dob
    o_k_last4_dob = o_last4 + "|" + o_dob
    _print_overlap(n_k_last4_dob, o_k_last4_dob, "last4|dob overlap", min_len=6)

    n_k_last4_birth_l3 = n_last4 + "|" + n_birth + "|" + n_l3
    o_k_last4_birth_l3 = o_last4 + "|" + o_birth + "|" + o_l3
    _print_overlap(n_k_last4_birth_l3, o_k_last4_birth_l3, "last4|birth_year|lname3 overlap", min_len=8)

    # Deep dive: last4 overlaps that fail birth_year match
    overlap_last4 = sorted(set(n_last4[n_last4 != ""]).intersection(set(o_last4[o_last4 != ""])))
    print("\nDEEP DIVE: last4 values that overlap but fail stronger keys")
    print("  overlapping last4 count:", len(overlap_last4))

    if overlap_last4:
        # Take up to 15 samples for quick review
        samples = overlap_last4[:15]
        for v in samples:
            nb = sorted(set(n_birth[n_last4 == v].tolist()))
            ob = sorted(set(o_birth[o_last4 == v].tolist()))
            nl3 = sorted(set(n_l3[n_last4 == v].tolist()))
            ol3 = sorted(set(o_l3[o_last4 == v].tolist()))
            nd = sorted(set(n_dob[n_last4 == v].tolist()))
            od = sorted(set(o_dob[o_last4 == v].tolist()))
            print(f"\n  last4={v}")
            print("    new birth_year:", nb[:5])
            print("    old birth_year:", ob[:5])
            print("    new lname3:", nl3[:5])
            print("    old lname3:", ol3[:5])
            print("    new dob:", nd[:3])
            print("    old dob:", od[:3])

    print("\nNEXT ACTIONS (based on results):")
    print("  1) If last_name_prefix3 is blank, fix mapping to generate it from last_name.")
    print("  2) If birth_year is blank or inconsistent, generate it from dob in mapping.")
    print("  3) If last4 overlaps but dob/birth_year don't, check dob source quality.")


if __name__ == "__main__":
    main()
