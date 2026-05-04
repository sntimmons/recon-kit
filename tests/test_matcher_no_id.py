"""
tests/test_matcher_no_id.py
Validates Phase 2.1: ID-less rows participate in fallback matching tiers.

Scenarios
---------
1. blank worker_id (old side only) + dob_name fields match
   → matched via dob_name, missing_worker_id_flag=True

2. blank worker_id (both sides) + last4_ssn+dob match
   → matched via last4_dob, missing_worker_id_flag=True

3. worker_id present on both sides
   → matched via worker_id, missing_worker_id_flag=False

4. blank worker_id (old side), no fallback signal matches
   → 0 matches; old stays unmatched, reason="no_id"

5. two old rows share the same dob_name key (ambiguous), one new row
   → _one_to_one_join drops both ambiguous old rows; 0 matches; both unmatched, reason="no_id"

6. row matched on worker_id is consumed before fallback tiers fire
   → exactly 1 match; match_source="worker_id" (not duplicated in fallback)

Run:
    python -m pytest tests/test_matcher_no_id.py -v
    python tests/test_matcher_no_id.py
"""
from __future__ import annotations

import csv
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

ROOT    = Path(__file__).resolve().parents[1]
MATCHER = ROOT / "src" / "matcher.py"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _run(tmp: Path) -> subprocess.CompletedProcess:
    env = {**os.environ, "RK_WORK_DIR": str(tmp)}
    return subprocess.run(
        [sys.executable, str(MATCHER)],
        env=env,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )


def _matched(tmp: Path) -> pd.DataFrame:
    p = tmp / "outputs" / "matched_raw.csv"
    return pd.read_csv(p, dtype=str) if p.exists() else pd.DataFrame()


def _unmatched(tmp: Path, side: str) -> pd.DataFrame:
    p = tmp / "outputs" / f"unmatched_{side}.csv"
    return pd.read_csv(p, dtype=str) if p.exists() else pd.DataFrame()


def _row(**kwargs) -> dict:
    """Minimal mapped-CSV row; caller overrides the interesting fields."""
    base = dict(
        worker_id="", recon_id="",
        first_name="", last_name="",
        full_name_norm="", first_name_norm="", last_name_norm="",
        middle_name="", suffix="",
        dob="", hire_date="", last4_ssn="",
        worker_status="", worker_type="",
        position="", district="",
        location="", location_state="",
        address="", salary="", payrate="",
    )
    base.update(kwargs)
    return base


def _flag_true(val: object) -> bool:
    return str(val).strip().lower() in ("true", "1")


def _flag_false(val: object) -> bool:
    return str(val).strip().lower() in ("false", "0", "")


# ---------------------------------------------------------------------------
# Scenario 1 — blank worker_id (old side), dob_name match
# ---------------------------------------------------------------------------

def test_no_id_old_matches_dob_name():
    """
    Old row has no worker_id; new row has one.
    Both share dob + full_name_norm → Tier 5 (dob_name) match.
    missing_worker_id_flag must be True.
    """
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        out = tmp / "outputs"
        out.mkdir()

        # Give old a different last4_ssn so Tier 3 (pk) and Tier 4 (last4_dob)
        # do NOT fire before Tier 5.
        _write_csv(out / "mapped_old.csv", [
            _row(worker_id="",      full_name_norm="alice smith",
                 last_name_norm="smith", dob="1985-03-15", last4_ssn=""),
        ])
        _write_csv(out / "mapped_new.csv", [
            _row(worker_id="WD001", full_name_norm="alice smith",
                 last_name_norm="smith", dob="1985-03-15", last4_ssn="4321"),
        ])

        r = _run(tmp)
        assert r.returncode == 0, r.stderr

        m = _matched(tmp)
        assert len(m) == 1, f"expected 1 match, got {len(m)}\n{r.stdout}"
        assert m.iloc[0]["match_source"] == "dob_name", m.iloc[0]["match_source"]
        assert _flag_true(m.iloc[0]["missing_worker_id_flag"]), \
            f"missing_worker_id_flag should be True, got {m.iloc[0]['missing_worker_id_flag']!r}"

        u = _unmatched(tmp, "old")
        assert len(u) == 0, f"old row should have matched, not stayed unmatched"


# ---------------------------------------------------------------------------
# Scenario 2 — blank worker_id (both sides), last4_dob match
# ---------------------------------------------------------------------------

def test_no_id_both_match_last4_dob():
    """
    Both sides have no worker_id.
    Both share last4_ssn + dob → Tier 4 (last4_dob) match.
    Different full_name_norm prevents an earlier pk/dob_name match.
    missing_worker_id_flag must be True.
    """
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        (tmp / "outputs").mkdir()

        _write_csv(tmp / "outputs" / "mapped_old.csv", [
            _row(worker_id="", last4_ssn="1234", dob="1990-07-22",
                 full_name_norm="bob jones", last_name_norm="jones"),
        ])
        _write_csv(tmp / "outputs" / "mapped_new.csv", [
            _row(worker_id="", last4_ssn="1234", dob="1990-07-22",
                 full_name_norm="robert jones", last_name_norm="jones"),
        ])

        r = _run(tmp)
        assert r.returncode == 0, r.stderr

        m = _matched(tmp)
        assert len(m) == 1, f"expected 1 match, got {len(m)}"
        assert m.iloc[0]["match_source"] == "last4_dob", m.iloc[0]["match_source"]
        assert _flag_true(m.iloc[0]["missing_worker_id_flag"])


# ---------------------------------------------------------------------------
# Scenario 3 — worker_id present on both sides → flag is False
# ---------------------------------------------------------------------------

def test_worker_id_match_flag_false():
    """
    Normal worker_id match; missing_worker_id_flag must be False.
    """
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        (tmp / "outputs").mkdir()

        _write_csv(tmp / "outputs" / "mapped_old.csv", [
            _row(worker_id="EMP100", full_name_norm="carol white",
                 last_name_norm="white", dob="1978-11-01"),
        ])
        _write_csv(tmp / "outputs" / "mapped_new.csv", [
            _row(worker_id="EMP100", full_name_norm="carol white",
                 last_name_norm="white", dob="1978-11-01"),
        ])

        r = _run(tmp)
        assert r.returncode == 0, r.stderr

        m = _matched(tmp)
        assert len(m) == 1, f"expected 1 match, got {len(m)}"
        assert m.iloc[0]["match_source"] == "worker_id"
        assert _flag_false(m.iloc[0]["missing_worker_id_flag"]), \
            f"missing_worker_id_flag should be False, got {m.iloc[0]['missing_worker_id_flag']!r}"


# ---------------------------------------------------------------------------
# Scenario 4 — blank worker_id, no fallback signal → stays unmatched
# ---------------------------------------------------------------------------

def test_no_id_no_match_stays_unmatched():
    """
    Old row has no worker_id; fallback signals (dob, name, ssn) don't align
    with the new row.  0 matches expected; old row in unmatched_old as no_id.
    """
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        (tmp / "outputs").mkdir()

        _write_csv(tmp / "outputs" / "mapped_old.csv", [
            _row(worker_id="", full_name_norm="dave brown",
                 last_name_norm="brown", dob="1982-05-10", last4_ssn="9999"),
        ])
        _write_csv(tmp / "outputs" / "mapped_new.csv", [
            _row(worker_id="WD002", full_name_norm="eve green",
                 last_name_norm="green", dob="1991-08-20", last4_ssn="1111"),
        ])

        r = _run(tmp)
        assert r.returncode == 0, r.stderr

        m = _matched(tmp)
        assert len(m) == 0, f"expected 0 matches, got {len(m)}"

        u = _unmatched(tmp, "old")
        assert len(u) == 1, f"expected 1 unmatched old, got {len(u)}"
        assert u.iloc[0]["unmatched_reason"] == "no_id", \
            f"expected unmatched_reason=no_id, got {u.iloc[0]['unmatched_reason']!r}"


# ---------------------------------------------------------------------------
# Scenario 5 — two no_id old rows share the same dob_name key (ambiguous)
# ---------------------------------------------------------------------------

def test_no_id_ambiguous_key_rejected():
    """
    Two old rows with no worker_id share the same dob + full_name_norm.
    _one_to_one_join must drop both old rows (ambiguous key) at every
    fallback tier.  0 matches; both old rows stay unmatched with reason=no_id.
    """
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        (tmp / "outputs").mkdir()

        _write_csv(tmp / "outputs" / "mapped_old.csv", [
            _row(worker_id="", full_name_norm="frank lee",
                 last_name_norm="lee", dob="1975-02-28"),
            _row(worker_id="", full_name_norm="frank lee",
                 last_name_norm="lee", dob="1975-02-28"),  # duplicate key
        ])
        _write_csv(tmp / "outputs" / "mapped_new.csv", [
            _row(worker_id="WD003", full_name_norm="frank lee",
                 last_name_norm="lee", dob="1975-02-28"),
        ])

        r = _run(tmp)
        assert r.returncode == 0, r.stderr

        m = _matched(tmp)
        assert len(m) == 0, \
            f"ambiguous key should produce 0 matches, got {len(m)}"

        u = _unmatched(tmp, "old")
        assert len(u) == 2, f"both ambiguous old rows should be unmatched, got {len(u)}"
        for reason in u["unmatched_reason"]:
            assert reason == "no_id", \
                f"expected unmatched_reason=no_id, got {reason!r}"


# ---------------------------------------------------------------------------
# Scenario 6 — worker_id match is not duplicated in fallback tiers
# ---------------------------------------------------------------------------

def test_worker_id_match_not_duplicated():
    """
    A row matched via Tier 1 (worker_id) is consumed from the pools.
    Even though dob + name also align, it must NOT re-appear as a
    second match in any fallback tier.  Exactly 1 match, source=worker_id.
    """
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        (tmp / "outputs").mkdir()

        _write_csv(tmp / "outputs" / "mapped_old.csv", [
            _row(worker_id="EMP200", full_name_norm="grace hall",
                 last_name_norm="hall", dob="1988-09-03", last4_ssn="5678"),
        ])
        _write_csv(tmp / "outputs" / "mapped_new.csv", [
            _row(worker_id="EMP200", full_name_norm="grace hall",
                 last_name_norm="hall", dob="1988-09-03", last4_ssn="5678"),
        ])

        r = _run(tmp)
        assert r.returncode == 0, r.stderr

        m = _matched(tmp)
        assert len(m) == 1, \
            f"expected exactly 1 match (worker_id tier), got {len(m)}"
        assert m.iloc[0]["match_source"] == "worker_id", m.iloc[0]["match_source"]


# ---------------------------------------------------------------------------
# Schema check — missing_worker_id_flag column always present
# ---------------------------------------------------------------------------

def test_missing_worker_id_flag_column_in_schema():
    """
    The matched_raw.csv must always contain missing_worker_id_flag,
    including the zero-match case (where the empty DataFrame is written).
    """
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        (tmp / "outputs").mkdir()

        # No overlapping signals → 0 matches
        _write_csv(tmp / "outputs" / "mapped_old.csv", [
            _row(worker_id="X1"),
        ])
        _write_csv(tmp / "outputs" / "mapped_new.csv", [
            _row(worker_id="X2"),
        ])

        r = _run(tmp)
        assert r.returncode == 0, r.stderr

        p = tmp / "outputs" / "matched_raw.csv"
        assert p.exists(), "matched_raw.csv must always be written"
        df = pd.read_csv(p, dtype=str)
        assert "missing_worker_id_flag" in df.columns, \
            f"missing_worker_id_flag absent from schema; columns: {list(df.columns)}"


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

_TESTS = [
    test_no_id_old_matches_dob_name,
    test_no_id_both_match_last4_dob,
    test_worker_id_match_flag_false,
    test_no_id_no_match_stays_unmatched,
    test_no_id_ambiguous_key_rejected,
    test_worker_id_match_not_duplicated,
    test_missing_worker_id_flag_column_in_schema,
]


def main() -> None:
    print("=" * 60)
    print("  TEST: matcher Phase 2.1 — ID-less fallback matching")
    print("=" * 60)
    passed = failed = 0
    for t in _TESTS:
        try:
            t()
            print(f"  [PASS] {t.__name__}")
            passed += 1
        except AssertionError as exc:
            print(f"  [FAIL] {t.__name__}: {exc}", file=sys.stderr)
            failed += 1
        except Exception as exc:
            print(f"  [ERROR] {t.__name__}: {type(exc).__name__}: {exc}", file=sys.stderr)
            failed += 1

    print(f"\n  {passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
    print("  All assertions PASSED.")


if __name__ == "__main__":
    main()
