# generate_international_pack.py
"""Generate the 'international_names' test pack for the Phase 3 harness.

Writes to:  test_packs/international_names/old.csv
            test_packs/international_names/new.csv

Row layout (20 rows, all drawn from wd00001-wd00020)
======================================================
GROUP A - worker_id KEPT in NEW, name mutated (→ Tier 1 expected, 5 rows)
--------------------------------------------------------------------------
Mutation intent: worker_id guarantees a Tier 1 hit despite name variance.
Verifies the system is not confused by name differences when ID is intact.

  wd00014  Jose Smith        → NEW first_name "José"            (add accent)
             norm OLD: "jose smith"    norm NEW: "jos smith"
  wd00008  Carl Rodriguez    → NEW last_name  "Rodriguez-Torres"  (hyphen add)
             norm: "carl rodriguez"  →  "carl rodriguez-torres"
  wd00002  Derek Arellano    → NEW last_name  "Arellano Morales"  (space-part append)
             norm: "derek arellano" →  "derek arellano morales"
  wd00012  Isabella Alvarado → NEW last_name  "de Alvarado"       (particle prepend)
             norm: "isabella alvarado" → "isabella de alvarado"
  wd00010  Paul Diaz         → NEW last_name  "Díaz"              (add accent)
             norm: "paul diaz" → "paul d az"

GROUP B - worker_id BLANKED in NEW, name mutated (→ Tier 2 expected, 5 rows)
------------------------------------------------------------------------------
Mutation intent: without worker_id the system must recover via last4+dob.
Demonstrates accented / altered names do NOT block Tier 2.

  wd00005  Carmen Cantrell   → NEW last_name "de la Cantrell",  worker_id=""
  wd00019  Katherine Carrillo→ NEW last_name "Carrillo-Vega",   worker_id=""
  wd00001  Sophia Miller     → NEW last_name "Müller",           worker_id=""
             norm: "sophia miller" → "sophia m ller"  (ü stripped to space)
  wd00004  Kristina Richardson→NEW first_name "Kristína",        worker_id=""
             norm: "kristina richardson" → "kristi na richardson"
  wd00015  David Curtis      → NEW last_name "O'Curtis",         worker_id=""
             norm: "david curtis" → "david o'curtis"

TRAP 1 - same full_name_norm, different DOB/last4 (→ Tier 2, no collision, 2 rows)
------------------------------------------------------------------------------------
Purpose: two distinct people both renamed to "Ana Martinez" in old+new.
         Their identity keys (last4+dob) differ, so Tier 2 matches each
         to its correct counterpart. A name-only system would collide these.

  wd00011  Melissa Barton    → "Ana Martinez", last4=6037, dob=1985-04-18, worker_id=""
  wd00013  Lee Patel         → "Ana Martinez", last4=3405, dob=2001-09-22, worker_id=""
  Expected: tier2_last4_dob += 2.  No unsafe cross-match.

TRAP 2 - name+DOB match, last4 DIFFERS (→ Tier 4 fallback, NOT Tier 2, 1 row)
-------------------------------------------------------------------------------
Purpose: verify system does not blindly fall into Tier 2 when last4 changes.
         Name+dob are unique enough for Tier 4 to recover the match.

  wd00018  Ian Sanders       → NEW last4_ssn="9999" (corrupted), worker_id=""
  OLD k_last4_dob: "2148|1969-03-15"  NEW: "9999|1969-03-15" → no Tier 2 match.
  Fallback key OLD+NEW: "ian sanders|1969-03-15|"  (unique in both sides)
  Expected: tier4_fallback += 1.

TRAP 3 - last4+DOB match, name clearly different (→ Tier 2 match, name gap documented)
----------------------------------------------------------------------------------------
Purpose: document the known gap - Tier 2 auto-matches on identity keys alone
         with no name-similarity gate. The suspicious pair must be flagged in
         the `suspicious_matches.csv` output even though the system accepts it.

  wd00020  Chelsea Lopez     → NEW first_name="Xochitl", last_name="Dominguez",
                               same last4=4656, same dob=1987-06-09, worker_id=""
  Tier 2 key matches → auto-matched. Name similarity ≈ 0.47 (flagged as suspicious; threshold=0.60).
  Expected: tier2_last4_dob += 1,  suspicious_match_count == 1.

CLEAN - no mutations (→ Tier 1, 6 rows)
-----------------------------------------
  wd00003, wd00006, wd00007, wd00009, wd00016, wd00017

EXPECTED SCORECARD
==================
  tier1_worker_id          : 11  (Group A: 5 + Clean: 6)
  tier2_last4_dob          :  8  (Group B: 5 + Trap1: 2 + Trap3: 1)
  tier3_last4_year_lname3  :  0
  tier4_fallback           :  1  (Trap 2)
  resolved_pairs           : 20  (all 20 matched, 0 conflicts)
  q0_dup_old / q0_dup_new  :  0 / 0
  suspicious_match_count   :  1  (Trap 3 - name discrepancy flagged)
"""
from __future__ import annotations

import pandas as pd
from pathlib import Path

REPO      = Path(__file__).parent
OLD_SRC   = REPO / "data" / "adp.csv"
NEW_SRC   = REPO / "data" / "workday.csv"
PACK_DIR  = REPO / "test_packs" / "international_names"

RAW_COLS = [
    "first_name", "last_name", "position", "dob", "hire_date",
    "location", "salary", "payrate", "worker_status", "worker_type",
    "district", "last4_ssn", "address", "worker_id",
]

# Worker IDs used by this pack (all wd00001-wd00020)
_GROUP_A   = {"wd00014", "wd00008", "wd00002", "wd00012", "wd00010"}
_GROUP_B   = {"wd00005", "wd00019", "wd00001", "wd00004", "wd00015"}
_TRAP_1    = {"wd00011", "wd00013"}
_TRAP_2    = {"wd00018"}
_TRAP_3    = {"wd00020"}
_CLEAN     = {"wd00003", "wd00006", "wd00007", "wd00009", "wd00016", "wd00017"}
_ALL_IDS   = _GROUP_A | _GROUP_B | _TRAP_1 | _TRAP_2 | _TRAP_3 | _CLEAN  # 20


def _load_base() -> tuple[pd.DataFrame, pd.DataFrame]:
    old = pd.read_csv(OLD_SRC, dtype=str).fillna("")
    new = pd.read_csv(NEW_SRC, dtype=str).fillna("")
    ids = sorted(_ALL_IDS)
    old_b = old[old["worker_id"].isin(ids)].set_index("worker_id").loc[ids].reset_index()
    new_b = new[new["worker_id"].isin(ids)].set_index("worker_id").loc[ids].reset_index()
    return old_b, new_b


def _mut(df: pd.DataFrame, wid: str, col: str, val: str) -> None:
    """In-place mutation helper."""
    df.loc[df["worker_id"] == wid, col] = val


def main() -> None:
    old, new = _load_base()

    # ── GROUP A: worker_id kept, name mutated in new ─────────────────────────
    _mut(new, "wd00014", "first_name", "José")              # add accent
    _mut(new, "wd00008", "last_name",  "Rodriguez-Torres")  # hyphen suffix
    _mut(new, "wd00002", "last_name",  "Arellano Morales")  # space part append
    _mut(new, "wd00012", "last_name",  "de Alvarado")       # particle prepend
    _mut(new, "wd00010", "last_name",  "Díaz")              # accent on last

    # ── GROUP B: worker_id blanked, name mutated in new ──────────────────────
    _mut(new, "wd00005", "last_name",  "de la Cantrell")    # particle prepend
    _mut(new, "wd00019", "last_name",  "Carrillo-Vega")     # hyphen suffix
    _mut(new, "wd00001", "last_name",  "Müller")            # umlaut (ü strips to space)
    _mut(new, "wd00004", "first_name", "Kristína")          # accent mid-word
    _mut(new, "wd00015", "last_name",  "O'Curtis")          # apostrophe add
    for wid in _GROUP_B:
        _mut(new, wid, "worker_id", "")

    # ── TRAP 1: two distinct people both renamed to "Ana Martinez" ───────────
    # Both old + new sides get the same display name; identity keys preserved.
    for wid in _TRAP_1:
        for df in (old, new):
            _mut(df, wid, "first_name", "Ana")
            _mut(df, wid, "last_name",  "Martinez")
        _mut(new, wid, "worker_id", "")  # force Tier 2

    # ── TRAP 2: name+DOB intact, last4 corrupted → Tier 4, not Tier 2 ────────
    _mut(new, "wd00018", "last4_ssn",  "9999")
    _mut(new, "wd00018", "worker_id",  "")

    # ── TRAP 3: last4+DOB match, name wildly different → Tier 2 auto-match ──
    # Documents the known gap: no name-similarity gate on Tier 2.
    _mut(new, "wd00020", "first_name", "Xochitl")
    _mut(new, "wd00020", "last_name",  "Dominguez")
    _mut(new, "wd00020", "worker_id",  "")

    # ── Write pack ───────────────────────────────────────────────────────────
    PACK_DIR.mkdir(parents=True, exist_ok=True)
    old[RAW_COLS].to_csv(PACK_DIR / "old.csv", index=False)
    new[RAW_COLS].to_csv(PACK_DIR / "new.csv", index=False)

    print(f"[international_names] wrote {len(old)} old / {len(new)} new rows")
    print(f"  Group A (Tier 1, name-mutated):        wd00014,08,02,12,10")
    print(f"  Group B (Tier 2, wid blanked+name):    wd00005,19,01,04,15")
    print(f"  Trap 1  (same name, diff keys, Tier 2): wd00011, wd00013")
    print(f"  Trap 2  (name+DOB ok, last4 bad, T4):  wd00018")
    print(f"  Trap 3  (last4+DOB ok, name differ):   wd00020  [KNOWN GAP]")
    print(f"  Clean   (Tier 1):                      wd00003,06,07,09,16,17")
    print(f"  Output: {PACK_DIR}/")


if __name__ == "__main__":
    main()
