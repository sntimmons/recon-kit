# generate_test_packs.py
"""Generate 8 test pack CSV pairs from the first 20 rows of adp.csv / workday.csv.

Each pack lives under test_packs/<packname>/ as old.csv + new.csv.
The packs cover one controlled mess each, chosen to exercise distinct pipeline paths.

Packs
-----
1. accent              - wd00014 (Jose Smith): first_name → "José", worker_id blanked in NEW
                         → must match via Tier 2 (last4+dob); accent doesn't prevent match.
2. middle_name         - wd00003 (Ricky Mitchell): first_name → "Ricky James" in NEW
                         → Tier 1 match; name discrepancy captured in output.
3. suffix              - wd00008 (Carl Rodriguez): last_name → "Rodriguez Jr." in NEW
                         → Tier 1 match; suffix discrepancy captured.
4. hyphen_apos         - wd00005: OLD last_name → "O'Cantrell", NEW → "OCantrell"
                         → Tier 1 match; punctuation diff captured.
5. dup_name_diff_salary- wd00017 and wd00011 both renamed to "Alex Kim" in both files,
                         but each gets a different salary bump in NEW.
                         → Two separate Tier 1 matches; both show salary mismatch.
6. swapped_salary      - wd00006 (Paul Poole) and wd00010 (Paul Diaz) have salaries
                         swapped in NEW. → Two Tier 1 matches; both show salary mismatch.
7. missing_worker_id_new - wd00020 (Chelsea Lopez): worker_id blanked in NEW only.
                         → Falls to Tier 2 (last4+dob); successfully matched.
8. dup_worker_id_new   - wd00015 (David Curtis): NEW gets a second phantom row with the
                         same worker_id but a bogus last4_ssn → both excluded from Tier 1;
                         real row still matches via Tier 2; phantom is unmatched.
"""
from __future__ import annotations

import pandas as pd
from pathlib import Path

REPO      = Path(__file__).parent
OLD_SRC   = REPO / "data" / "adp.csv"
NEW_SRC   = REPO / "data" / "workday.csv"
PACKS_DIR = REPO / "test_packs"

RAW_COLS = [
    "first_name", "last_name", "position", "dob", "hire_date",
    "location", "salary", "payrate", "worker_status", "worker_type",
    "district", "last4_ssn", "address", "worker_id",
]


def _write_pack(name: str, old_df: pd.DataFrame, new_df: pd.DataFrame) -> None:
    d = PACKS_DIR / name
    d.mkdir(parents=True, exist_ok=True)
    old_df[RAW_COLS].to_csv(d / "old.csv", index=False)
    new_df[RAW_COLS].to_csv(d / "new.csv", index=False)
    print(f"  [{name:<28}] {len(old_df):>3} old  /  {len(new_df):>3} new rows")


def main() -> None:
    old = pd.read_csv(OLD_SRC, dtype=str).fillna("")
    new = pd.read_csv(NEW_SRC, dtype=str).fillna("")

    # 20-row base: wd00001-wd00020 (present in both files)
    base_ids = [f"wd{i:05d}" for i in range(1, 21)]
    old_b = (
        old[old["worker_id"].isin(base_ids)]
        .set_index("worker_id").loc[base_ids].reset_index()
    )
    new_b = (
        new[new["worker_id"].isin(base_ids)]
        .set_index("worker_id").loc[base_ids].reset_index()
    )

    # ── 1. accent ──────────────────────────────────────────────────────────────
    # wd00014 (Jose Smith): accent added to first_name, worker_id blanked in NEW.
    # Falls to Tier 2 (last4+dob unchanged) → match found despite accent in name.
    o1, n1 = old_b.copy(), new_b.copy()
    m = n1["worker_id"] == "wd00014"
    n1.loc[m, "first_name"] = "José"
    n1.loc[m, "worker_id"] = ""
    _write_pack("accent", o1, n1)

    # ── 2. middle_name ─────────────────────────────────────────────────────────
    # wd00003 (Ricky Mitchell): middle name added to first_name in NEW.
    # Tier 1 match via worker_id; name discrepancy visible in matched_raw.csv.
    o2, n2 = old_b.copy(), new_b.copy()
    m = n2["worker_id"] == "wd00003"
    n2.loc[m, "first_name"] = "Ricky James"
    _write_pack("middle_name", o2, n2)

    # ── 3. suffix ──────────────────────────────────────────────────────────────
    # wd00008 (Carl Rodriguez): " Jr." appended to last_name in NEW.
    # Tier 1 match; suffix discrepancy captured in output.
    o3, n3 = old_b.copy(), new_b.copy()
    m = n3["worker_id"] == "wd00008"
    n3.loc[m, "last_name"] = "Rodriguez Jr."
    _write_pack("suffix", o3, n3)

    # ── 4. hyphen_apos ─────────────────────────────────────────────────────────
    # wd00005 (Carmen Cantrell): OLD gets apostrophe ("O'Cantrell"), NEW loses it ("OCantrell").
    # Tier 1 match; punctuation difference visible in matched name columns.
    o4, n4 = old_b.copy(), new_b.copy()
    o4.loc[o4["worker_id"] == "wd00005", "last_name"] = "O'Cantrell"
    n4.loc[n4["worker_id"] == "wd00005", "last_name"] = "OCantrell"
    _write_pack("hyphen_apos", o4, n4)

    # ── 5. dup_name_diff_salary ────────────────────────────────────────────────
    # wd00017 (Gary Gibbs) and wd00011 (Melissa Barton) both renamed to "Alex Kim"
    # in BOTH files → two workers with identical full names.
    # Each has a different salary bump in NEW → salary mismatch for both rows.
    o5, n5 = old_b.copy(), new_b.copy()
    for wid in ("wd00017", "wd00011"):
        for df in (o5, n5):
            m = df["worker_id"] == wid
            df.loc[m, "first_name"] = "Alex"
            df.loc[m, "last_name"]  = "Kim"
    n5.loc[n5["worker_id"] == "wd00017", "salary"] = "99999"
    n5.loc[n5["worker_id"] == "wd00011", "salary"] = "88888"
    _write_pack("dup_name_diff_salary", o5, n5)

    # ── 6. swapped_salary ─────────────────────────────────────────────────────
    # wd00006 (Paul Poole) and wd00010 (Paul Diaz): salaries swapped in NEW vs OLD.
    # Two Tier 1 matches; each shows a salary mismatch.
    o6, n6 = old_b.copy(), new_b.copy()
    sal_06 = new_b.loc[new_b["worker_id"] == "wd00006", "salary"].iloc[0]
    sal_10 = new_b.loc[new_b["worker_id"] == "wd00010", "salary"].iloc[0]
    n6.loc[n6["worker_id"] == "wd00006", "salary"] = sal_10
    n6.loc[n6["worker_id"] == "wd00010", "salary"] = sal_06
    _write_pack("swapped_salary", o6, n6)

    # ── 7. missing_worker_id_new ───────────────────────────────────────────────
    # wd00020 (Chelsea Lopez): worker_id blanked in NEW only.
    # Old row excluded from Tier 1 (no matching new worker_id); falls to Tier 2 via
    # last4+dob (both unchanged) → successfully matched.
    o7, n7 = old_b.copy(), new_b.copy()
    n7.loc[n7["worker_id"] == "wd00020", "worker_id"] = ""
    _write_pack("missing_worker_id_new", o7, n7)

    # ── 8. dup_worker_id_new ───────────────────────────────────────────────────
    # wd00015 (David Curtis): a phantom row appended to NEW with the SAME worker_id
    # but a bogus last4_ssn ("0000") so it can't match via any lower tier.
    # Both new rows excluded from Tier 1 (duplicate worker_id in NEW).
    # The real new row still has the correct last4+dob → matches old wd00015 via Tier 2.
    # The phantom row is left unmatched (no valid last4 key).
    o8, n8 = old_b.copy(), new_b.copy()
    real_row = n8[n8["worker_id"] == "wd00015"].iloc[0].copy()
    phantom = real_row.copy()
    phantom["first_name"] = "Dave"          # slightly different name
    phantom["last4_ssn"]  = "0000"          # bogus SSN4 → can't match lower tiers
    n8 = pd.concat([n8, pd.DataFrame([phantom])], ignore_index=True)
    _write_pack("dup_worker_id_new", o8, n8)

    print(f"\n[done] 8 test packs written to {PACKS_DIR}/")


if __name__ == "__main__":
    main()
