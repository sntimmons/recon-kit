"""
smoke_check_extra_fields.py - In-memory fixture test for Dynamic Audit Fields.

Assertions
----------
1. load_audit_config returns fields, groups, gate keys.
2. _build_row produces mm_<field> booleans for each extra field.
3. _build_row computes mismatch_group_<name> correctly (OR across group fields).
4. Group boolean is False when all fields in the group match.
5. Extra field columns absent from stable _OUTPUT_COLS (no duplication).

Run with:
    venv/Scripts/python.exe audit/ui/smoke_check_extra_fields.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_HERE        = Path(__file__).resolve().parent    # audit/ui/
_SUMMARY_DIR = _HERE.parent / "summary"           # audit/summary/
sys.path.insert(0, str(_SUMMARY_DIR))

from build_ui_pairs import _build_row, _OUTPUT_COLS  # noqa: E402
from config_loader import load_audit_config           # noqa: E402


def _fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Minimal fixture rows
# ---------------------------------------------------------------------------

_ROW_MISMATCH = {
    "pair_id":            "P001",
    "match_source":       "worker_id",
    "old_worker_id":      "W001",
    "new_worker_id":      "W001",
    "old_salary":         "50000",
    "new_salary":         "50000",
    "old_worker_status":  "active",
    "new_worker_status":  "active",
    "old_hire_date":      "2020-01-01",
    "new_hire_date":      "2020-01-01",
    "old_position":       "Analyst",
    "new_position":       "Analyst",
    "old_district":       "North",
    "new_district":       "North",
    "old_location_state": "CA",
    "new_location_state": "CA",
    "confidence":         "1.0",
    # Extra fields - cost_center differs, company matches, department differs
    "old_cost_center":    "CC100",
    "new_cost_center":    "CC200",
    "old_company":        "Acme",
    "new_company":        "Acme",
    "old_department":     "Ops",
    "new_department":     "Finance",
}

_ROW_MATCH = {
    **_ROW_MISMATCH,
    "old_cost_center": "CC100",
    "new_cost_center": "CC100",
    "old_company":     "Acme",
    "new_company":     "Acme",
    "old_department":  "Ops",
    "new_department":  "Ops",
}

_AVAILABLE_EXTRA = ["cost_center", "company", "department"]
_EXTRA_GROUPS    = {"org": ["cost_center", "company", "department"]}


def main() -> None:
    print("=" * 60)
    print("  SMOKE CHECK: extra fields (in-memory)")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Assertion 1: load_audit_config returns expected keys
    # ------------------------------------------------------------------
    cfg = load_audit_config()
    for key in ("fields", "groups", "gate"):
        if key not in cfg:
            _fail(f"Assertion 1: load_audit_config missing key '{key}'")
    print(f"  [PASS] Assertion 1: load_audit_config keys present ({list(cfg)})")

    # ------------------------------------------------------------------
    # Assertion 2: mm_ booleans correctly set for mismatch row
    # ------------------------------------------------------------------
    out_mm = _build_row(
        _ROW_MISMATCH, has_location=False, has_worker_type=False,
        available_extra=_AVAILABLE_EXTRA, extra_groups={},
    )
    if out_mm.get("mm_cost_center") is not True:
        _fail("Assertion 2: mm_cost_center should be True for mismatched row")
    if out_mm.get("mm_company") is not False:
        _fail("Assertion 2: mm_company should be False (both 'Acme')")
    if out_mm.get("mm_department") is not True:
        _fail("Assertion 2: mm_department should be True for mismatched row")
    print("  [PASS] Assertion 2: mm_<field> booleans correct for mismatch row")

    # ------------------------------------------------------------------
    # Assertion 3: mismatch_group_org is True when any field in group mismatches
    # ------------------------------------------------------------------
    out_grp = _build_row(
        _ROW_MISMATCH, has_location=False, has_worker_type=False,
        available_extra=_AVAILABLE_EXTRA, extra_groups=_EXTRA_GROUPS,
    )
    if out_grp.get("mismatch_group_org") is not True:
        _fail(
            "Assertion 3: mismatch_group_org should be True "
            "(cost_center and department differ)"
        )
    print("  [PASS] Assertion 3: mismatch_group_org=True when any group field mismatches")

    # ------------------------------------------------------------------
    # Assertion 4: mismatch_group_org is False when all group fields match
    # ------------------------------------------------------------------
    out_clean = _build_row(
        _ROW_MATCH, has_location=False, has_worker_type=False,
        available_extra=_AVAILABLE_EXTRA, extra_groups=_EXTRA_GROUPS,
    )
    if out_clean.get("mismatch_group_org") is not False:
        _fail("Assertion 4: mismatch_group_org should be False when all group fields match")
    print("  [PASS] Assertion 4: mismatch_group_org=False when all group fields match")

    # ------------------------------------------------------------------
    # Assertion 5: extra field columns not duplicated in stable _OUTPUT_COLS
    # ------------------------------------------------------------------
    stable = set(_OUTPUT_COLS)
    dups = []
    for field in _AVAILABLE_EXTRA:
        for col in (f"old_{field}", f"new_{field}", f"mm_{field}"):
            if col in stable:
                dups.append(col)
    if dups:
        _fail(f"Assertion 5: extra field columns already in stable _OUTPUT_COLS: {dups}")
    print("  [PASS] Assertion 5: no extra field columns duplicated in _OUTPUT_COLS")

    print(f"\n  All assertions PASSED.")
    print(f"  [done] smoke check complete.")


if __name__ == "__main__":
    main()
