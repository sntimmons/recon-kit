from __future__ import annotations

import hashlib
import json
import os
import sys
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Any, List, Tuple

import pandas as pd

from csv_safe import safe_to_csv

ROOT = Path(__file__).resolve().parents[1]

# Per-run isolation: when RK_WORK_DIR is set by api_server.py, write all
# outputs into that run-specific directory instead of the global outputs/.
_rk_work = Path(os.environ["RK_WORK_DIR"]) if "RK_WORK_DIR" in os.environ else None
OUT = (_rk_work / "outputs") if _rk_work else (ROOT / "outputs")
OUT.mkdir(parents=True, exist_ok=True)


def _load_extra_fields() -> list[str]:
    """Load configured extra field names from policy.yaml (non-fatal if unavailable)."""
    try:
        _summary = ROOT / "audit" / "summary"
        if str(_summary) not in sys.path:
            sys.path.insert(0, str(_summary))
        from config_loader import load_audit_config  # noqa: PLC0415
        return load_audit_config()["fields"]
    except Exception:
        return []


def _load(label: str) -> pd.DataFrame:
    path = OUT / f"mapped_{label}.csv"
    df = pd.read_csv(path, dtype="string")
    df.columns = [c.strip() for c in df.columns]
    return df


_AMB_COLS = ("match_key", "tier_source", "side", "worker_id",
             "first_name", "last_name", "dob", "last4_ssn", "reason",
             "missing_worker_id_flag", "record_id", "ambiguity_event_id")

_MATCH_REASONS: dict[str, str] = {
    "worker_id":      "Exact match on worker_id",
    "recon_id":       "Exact match on recon_id",
    "pk":             "Matched on name + dob + last4_ssn",
    "last4_dob":      "Matched on last4_ssn + dob",
    "dob_name":       "Matched on dob + name similarity",
    "name_hire_date": "Matched on last name + hire date",
}


def _mk_key(df: pd.DataFrame, cols: List[str]) -> pd.Series:
    parts = []
    for c in cols:
        s = df.get(c, pd.Series([pd.NA] * len(df))).astype("string")
        s = s.fillna("").str.strip()
        parts.append(s)
    k = parts[0]
    for p in parts[1:]:
        k = k + "|" + p
    k = k.replace("", pd.NA)
    return k


def _one_to_one_join(
    old: pd.DataFrame,
    new: pd.DataFrame,
    key_cols: List[str],
    source: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Match old to new on key_cols (1-to-1, ambiguous keys on either side discarded).

    Returns (matches, old_remaining, new_remaining, ambiguous_rows).
    ambiguous_rows contains every row dropped because its key was duplicated on
    one or both sides, with columns defined by _AMB_COLS.  reason values:
      duplicate_old  — key appeared >1x on old side only
      duplicate_new  — key appeared >1x on new side only
      many_to_many   — key duplicated on BOTH sides

    Remaining pools are tracked by row-index so that rows with blank worker_id
    (or any other missing identifier) that DO match on this tier are correctly
    removed from the pools passed to the next tier.
    """
    # Reset index so __oidx / __nidx are stable 0-based positions into old/new.
    o = old.copy().reset_index(drop=True)
    n = new.copy().reset_index(drop=True)

    o["__oidx"] = range(len(o))
    n["__nidx"] = range(len(n))

    o["_k"] = _mk_key(o, key_cols)
    n["_k"] = _mk_key(n, key_cols)

    # Only consider rows that have a valid (non-null) key for this tier.
    o_keyed = o[o["_k"].notna()].copy()
    n_keyed = n[n["_k"].notna()].copy()

    # Discard keys that appear more than once on either side - ambiguous.
    o_dup_keys = set(o_keyed.loc[o_keyed["_k"].duplicated(keep=False), "_k"].tolist())
    n_dup_keys = set(n_keyed.loc[n_keyed["_k"].duplicated(keep=False), "_k"].tolist())
    bad = o_dup_keys | n_dup_keys

    amb_parts: list[pd.DataFrame] = []
    if bad:
        many_to_many = o_dup_keys & n_dup_keys
        for df_side, side_label, side_dup in (
            (o_keyed, "old", o_dup_keys),
            (n_keyed, "new", n_dup_keys),
        ):
            dup = df_side[df_side["_k"].isin(side_dup)].reset_index(drop=True)
            if dup.empty:
                continue
            amb_parts.append(pd.DataFrame({
                "match_key":   dup["_k"].tolist(),
                "tier_source": source,
                "side":        side_label,
                "worker_id":   dup["worker_id"].tolist() if "worker_id" in dup.columns else [pd.NA] * len(dup),
                "first_name":  dup["first_name"].tolist() if "first_name" in dup.columns else [pd.NA] * len(dup),
                "last_name":   dup["last_name"].tolist() if "last_name" in dup.columns else [pd.NA] * len(dup),
                "dob":         dup["dob"].tolist() if "dob" in dup.columns else [pd.NA] * len(dup),
                "last4_ssn":   dup["last4_ssn"].tolist() if "last4_ssn" in dup.columns else [pd.NA] * len(dup),
                "reason":      dup["_k"].apply(
                    lambda k: "many_to_many" if k in many_to_many
                    else ("duplicate_old" if side_label == "old" else "duplicate_new")
                ).tolist(),
                "missing_worker_id_flag": [
                    pd.isna(v) or str(v).strip() == ""
                    for v in (dup["worker_id"].tolist() if "worker_id" in dup.columns else [pd.NA] * len(dup))
                ],
                "record_id": [
                    hashlib.md5(
                        f"{side_label}|{wid}|{fn}|{ln}|{d}".encode("utf-8")
                    ).hexdigest()[:12]
                    for wid, fn, ln, d in zip(
                        dup["worker_id"].tolist() if "worker_id" in dup.columns else [""] * len(dup),
                        dup["first_name"].tolist() if "first_name" in dup.columns else [""] * len(dup),
                        dup["last_name"].tolist() if "last_name" in dup.columns else [""] * len(dup),
                        dup["dob"].tolist() if "dob" in dup.columns else [""] * len(dup),
                    )
                ],
                "ambiguity_event_id": [
                    hashlib.md5(
                        f"{mk}|{source}|{side_label}|{wid}|{d}".encode("utf-8")
                    ).hexdigest()[:12]
                    for mk, wid, d in zip(
                        dup["_k"].tolist(),
                        dup["worker_id"].tolist() if "worker_id" in dup.columns else [""] * len(dup),
                        dup["dob"].tolist() if "dob" in dup.columns else [""] * len(dup),
                    )
                ],
            }))
        o_keyed = o_keyed[~o_keyed["_k"].isin(bad)]
        n_keyed = n_keyed[~n_keyed["_k"].isin(bad)]

    ambiguous = (
        pd.concat(amb_parts, ignore_index=True) if amb_parts
        else pd.DataFrame(columns=list(_AMB_COLS))
    )

    m = o_keyed.merge(n_keyed, on="_k", how="inner", suffixes=("_old", "_new"))

    if len(m) == 0:
        # No matches: return originals unchanged.
        return pd.DataFrame(), old, new, ambiguous

    m["match_source"] = source

    # Track which original rows were consumed so they are excluded from future tiers.
    # __oidx / __nidx are unique per side (no suffix collision) so they survive merge as-is.
    matched_old_rows = set(m["__oidx"].tolist())
    matched_new_rows = set(m["__nidx"].tolist())

    old_remaining = (
        o[~o["__oidx"].isin(matched_old_rows)]
        .drop(columns=["__oidx", "_k"], errors="ignore")
        .copy()
    )
    new_remaining = (
        n[~n["__nidx"].isin(matched_new_rows)]
        .drop(columns=["__nidx", "_k"], errors="ignore")
        .copy()
    )

    m_out = m.drop(columns=["_k", "__oidx", "__nidx"], errors="ignore")
    return m_out, old_remaining, new_remaining, ambiguous


def compute_confidence(row: dict) -> float:
    """
    Compute a confidence score [0.0, 1.0] for a matched pair.

    Signals and weights (as specified in the engineering plan):
        name_similarity       * 0.5   (SequenceMatcher ratio on full_name_norm)
        dob_match             * 0.2   (1.0 if old_dob == new_dob, else 0.0)
        last4_match           * 0.2   (1.0 if old_last4_ssn == new_last4_ssn, else 0.0)
        location_state_match  * 0.1   (1.0 if old_location_state == new_location_state)

    worker_id and recon_id are exact business/system ID matches - always 1.0.
    """
    ms = str(row.get("match_source") or "").strip().lower()
    if ms in ("worker_id", "recon_id"):
        return 1.0

    old_name  = str(row.get("old_full_name_norm") or "").strip()
    new_name  = str(row.get("new_full_name_norm") or "").strip()
    old_dob   = str(row.get("old_dob") or "").strip()
    new_dob   = str(row.get("new_dob") or "").strip()
    old_l4    = str(row.get("old_last4_ssn") or "").strip()
    new_l4    = str(row.get("new_last4_ssn") or "").strip()
    old_state = str(row.get("old_location_state") or "").strip().lower()
    new_state = str(row.get("new_location_state") or "").strip().lower()

    name_sim  = SequenceMatcher(None, old_name, new_name).ratio() if (old_name and new_name) else 0.0
    dob_match = 1.0 if (old_dob and new_dob and old_dob == new_dob) else 0.0
    l4_match  = 1.0 if (old_l4 and new_l4 and old_l4 == new_l4) else 0.0
    loc_match = 1.0 if (old_state and new_state and old_state == new_state) else 0.0

    score = name_sim * 0.5 + dob_match * 0.2 + l4_match * 0.2 + loc_match * 0.1
    return round(min(1.0, max(0.0, score)), 4)


def _confidence_breakdown_str(row: dict) -> str:
    """Return confidence component scores as a compact JSON string.

    Components mirror compute_confidence() exactly; last4_match is 0/1 (no SSN value).
    """
    ms = str(row.get("match_source") or "").strip().lower()
    if ms in ("worker_id", "recon_id"):
        return '{"name_similarity":1.0,"dob_match":1,"last4_match":1,"state_match":1}'

    old_name  = str(row.get("old_full_name_norm") or "").strip()
    new_name  = str(row.get("new_full_name_norm") or "").strip()
    old_dob   = str(row.get("old_dob") or "").strip()
    new_dob   = str(row.get("new_dob") or "").strip()
    old_l4    = str(row.get("old_last4_ssn") or "").strip()
    new_l4    = str(row.get("new_last4_ssn") or "").strip()
    old_state = str(row.get("old_location_state") or "").strip().lower()
    new_state = str(row.get("new_location_state") or "").strip().lower()

    name_sim  = round(SequenceMatcher(None, old_name, new_name).ratio(), 4) if (old_name and new_name) else 0.0
    dob_match = 1 if (old_dob and new_dob and old_dob == new_dob) else 0
    l4_match  = 1 if (old_l4 and new_l4 and old_l4 == new_l4) else 0
    loc_match = 1 if (old_state and new_state and old_state == new_state) else 0

    return (
        f'{{"name_similarity":{name_sim},'
        f'"dob_match":{dob_match},'
        f'"last4_match":{l4_match},'
        f'"state_match":{loc_match}}}'
    )


def _ascii_fold_name_part(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return (
        unicodedata.normalize("NFKD", raw)
        .encode("ascii", "ignore")
        .decode("ascii")
        .strip()
        .lower()
    )


def _norm_id_for_match(value: object) -> str:
    raw = "" if pd.isna(value) else str(value)
    return raw.strip().lower()


def main() -> None:
    old = _load("old")
    new = _load("new")

    if "worker_id" in old.columns:
        old["_match_worker_id"] = old["worker_id"].apply(_norm_id_for_match).astype("string")
    if "worker_id" in new.columns:
        new["_match_worker_id"] = new["worker_id"].apply(_norm_id_for_match).astype("string")
    if "recon_id" in old.columns:
        old["_match_recon_id"] = old["recon_id"].apply(_norm_id_for_match).astype("string")
    if "recon_id" in new.columns:
        new["_match_recon_id"] = new["recon_id"].apply(_norm_id_for_match).astype("string")

    def _is_missing_id(v: object) -> bool:
        return _norm_id_for_match(v) == ""

    old_no_id = pd.DataFrame(columns=old.columns)
    new_no_id = pd.DataFrame(columns=new.columns)
    if "worker_id" in old.columns:
        old_no_id = old[old["worker_id"].apply(_is_missing_id)].copy()
        old = old[~old["worker_id"].apply(_is_missing_id)].copy()
    if "worker_id" in new.columns:
        new_no_id = new[new["worker_id"].apply(_is_missing_id)].copy()
        new = new[~new["worker_id"].apply(_is_missing_id)].copy()

    report: Dict[str, Any] = {
        "matched_total": 0,
        "matched_by_worker_id": 0,
        "matched_by_recon_id": 0,
        "matched_by_pk": 0,
        "matched_by_last4_dob": 0,
        "matched_by_dob_name": 0,
        "matched_by_name_hire_date": 0,
        "unmatched_old": 0,
        "unmatched_new": 0,
        "ambiguous_old_count": 0,
        "ambiguous_new_count": 0,
        "ambiguous_total": 0,
    }

    all_matches: list[pd.DataFrame] = []
    all_ambiguous: list[pd.DataFrame] = []

    # Tier 1: worker_id exact (case-insensitive, trimmed comparison only)
    m, old, new, amb = _one_to_one_join(old, new, ["_match_worker_id"], "worker_id")
    report["matched_by_worker_id"] = int(len(m))
    all_matches.append(m)
    all_ambiguous.append(amb)

    # Tier 2: recon_id exact (only if column present on both sides)
    if "_match_recon_id" in old.columns and "_match_recon_id" in new.columns:
        m, old, new, amb = _one_to_one_join(old, new, ["_match_recon_id"], "recon_id")
        report["matched_by_recon_id"] = int(len(m))
        all_matches.append(m)
        all_ambiguous.append(amb)

    # Reintroduce ID-less rows for identity-based fallback tiers.
    # Tiers 1–2 require a business ID and correctly excluded them.
    # Tiers 3–6 match on name / DOB / SSN, so no worker_id is needed.
    if not old_no_id.empty:
        old = pd.concat([old, old_no_id], ignore_index=True)
        old_no_id = pd.DataFrame(columns=old_no_id.columns)
    if not new_no_id.empty:
        new = pd.concat([new, new_no_id], ignore_index=True)
        new_no_id = pd.DataFrame(columns=new_no_id.columns)

    # Tier 3: pk = full_name_norm + dob + last4_ssn
    m, old, new, amb = _one_to_one_join(old, new, ["full_name_norm", "dob", "last4_ssn"], "pk")
    report["matched_by_pk"] = int(len(m))
    all_matches.append(m)
    all_ambiguous.append(amb)

    # Tier 4: last4_ssn + dob
    m, old, new, amb = _one_to_one_join(old, new, ["last4_ssn", "dob"], "last4_dob")
    report["matched_by_last4_dob"] = int(len(m))
    all_matches.append(m)
    all_ambiguous.append(amb)

    # Tier 5: dob + full_name_norm
    m, old, new, amb = _one_to_one_join(old, new, ["dob", "full_name_norm"], "dob_name")
    report["matched_by_dob_name"] = int(len(m))
    all_matches.append(m)
    all_ambiguous.append(amb)

    # Tier 6: last_name_norm + hire_date (more robust than full_name_norm + hire_date)
    # Using last name as the join key catches cases where first name differs slightly
    # between systems (John vs Johnny, Mary vs Marie).  First name similarity is
    # captured in the confidence score via SequenceMatcher on full_name_norm.
    # Falls back to full_name_norm + hire_date when last_name_norm is unavailable.
    _t6_keys = (["last_name_norm", "hire_date"]
                if "last_name_norm" in old.columns and "last_name_norm" in new.columns
                else ["full_name_norm", "hire_date"])
    m, old, new, amb = _one_to_one_join(old, new, _t6_keys, "name_hire_date")
    report["matched_by_name_hire_date"] = int(len(m))
    all_matches.append(m)
    all_ambiguous.append(amb)

    matched = (
        pd.concat([x for x in all_matches if len(x) > 0], ignore_index=True)
        if any(len(x) > 0 for x in all_matches)
        else pd.DataFrame()
    )

    if len(matched) > 0:
        base = pd.DataFrame()

        base["old_worker_id"] = matched.get("worker_id_old", pd.NA)
        base["new_worker_id"] = matched.get("worker_id_new", pd.NA)
        base["old_recon_id"] = matched.get("recon_id_old", pd.NA)
        base["new_recon_id"] = matched.get("recon_id_new", pd.NA)

        base["old_full_name_norm"] = matched.get("full_name_norm_old", pd.NA)
        base["new_full_name_norm"] = matched.get("full_name_norm_new", pd.NA)
        # Name components (from mapping.py _build_name_components)
        base["old_first_name_norm"] = matched.get("first_name_norm_old", pd.NA)
        base["new_first_name_norm"] = matched.get("first_name_norm_new", pd.NA)
        base["old_last_name_norm"]  = matched.get("last_name_norm_old",  pd.NA)
        base["new_last_name_norm"]  = matched.get("last_name_norm_new",  pd.NA)
        base["old_middle_name"]     = matched.get("middle_name_old", pd.NA)
        base["new_middle_name"]     = matched.get("middle_name_new", pd.NA)
        base["old_suffix"]          = matched.get("suffix_old", pd.NA)
        base["new_suffix"]          = matched.get("suffix_new", pd.NA)
        # name_change_detected: True only for genuine last-name changes.
        # Accent-only differences (García vs Garcia) should not be flagged.
        def _name_changed(row: dict) -> bool:
            old_ln = _ascii_fold_name_part(row.get("old_last_name_norm"))
            new_ln = _ascii_fold_name_part(row.get("new_last_name_norm"))
            return bool(old_ln and new_ln and old_ln != new_ln)
        base["name_change_detected"] = [
            _name_changed(r) for r in base.to_dict(orient="records")
        ]
        base["old_dob"] = matched.get("dob_old", pd.NA)
        base["new_dob"] = matched.get("dob_new", pd.NA)
        base["old_hire_date"] = matched.get("hire_date_old", pd.NA)
        base["new_hire_date"] = matched.get("hire_date_new", pd.NA)

        base["old_last4_ssn"] = matched.get("last4_ssn_old", pd.NA)
        base["new_last4_ssn"] = matched.get("last4_ssn_new", pd.NA)

        base["old_salary"] = matched.get("salary_old", pd.NA)
        base["new_salary"] = matched.get("salary_new", pd.NA)
        base["old_payrate"] = matched.get("payrate_old", pd.NA)
        base["new_payrate"] = matched.get("payrate_new", pd.NA)

        base["old_position"] = matched.get("position_old", pd.NA)
        base["new_position"] = matched.get("position_new", pd.NA)
        base["old_district"] = matched.get("district_old", pd.NA)
        base["new_district"] = matched.get("district_new", pd.NA)

        base["old_location_state"] = matched.get("location_state_old", pd.NA)
        base["new_location_state"] = matched.get("location_state_new", pd.NA)

        base["old_worker_status"] = matched.get("worker_status_old", pd.NA)
        base["new_worker_status"] = matched.get("worker_status_new", pd.NA)
        base["old_worker_type"] = matched.get("worker_type_old", pd.NA)
        base["new_worker_type"] = matched.get("worker_type_new", pd.NA)

        base["match_source"] = matched["match_source"].astype("string")

        # Flag pairs where either side was missing a worker_id at match time.
        # Used downstream for explainability and review prioritization.
        base["missing_worker_id_flag"] = (
            base["old_worker_id"].isna()
            | (base["old_worker_id"].fillna("").astype(str).str.strip() == "")
            | base["new_worker_id"].isna()
            | (base["new_worker_id"].fillna("").astype(str).str.strip() == "")
        )

        # Append configured extra fields (old_/new_ pair for each field).
        # After the merge, extra field columns have _old/_new suffixes.
        for _field in _load_extra_fields():
            base[f"old_{_field}"] = matched.get(f"{_field}_old", pd.NA)
            base[f"new_{_field}"] = matched.get(f"{_field}_new", pd.NA)

        # Compute confidence score for every matched pair.
        # last4_ssn is still present in base here and used by compute_confidence.
        base["confidence"] = [
            compute_confidence(r) for r in base.to_dict(orient="records")
        ]

        # Explainability columns — computed while last4_ssn is still in base.
        base["match_reason"] = base["match_source"].map(
            lambda ms: _MATCH_REASONS.get(str(ms or "").strip().lower(), str(ms or ""))
        )
        base["confidence_breakdown"] = [
            _confidence_breakdown_str(r) for r in base.to_dict(orient="records")
        ]
        matched_raw = base
    else:
        # Zero-match case: emit all standard columns so that downstream steps
        # (load_sqlite.py indexes, run_audit.py queries) see a consistent schema.
        matched_raw = pd.DataFrame(columns=[
            "old_worker_id", "new_worker_id",
            "old_recon_id",  "new_recon_id",
            "old_full_name_norm",    "new_full_name_norm",
            "old_first_name_norm",   "new_first_name_norm",
            "old_last_name_norm",    "new_last_name_norm",
            "old_middle_name",       "new_middle_name",
            "old_suffix",            "new_suffix",
            "name_change_detected",
            "old_dob",       "new_dob",
            "old_hire_date", "new_hire_date",
            "old_salary",    "new_salary",
            "old_payrate",   "new_payrate",
            "old_position",  "new_position",
            "old_district",  "new_district",
            "old_location_state", "new_location_state",
            "old_worker_status",  "new_worker_status",
            "old_worker_type",    "new_worker_type",
            "match_source",  "confidence",
            "missing_worker_id_flag",
            "match_reason",  "confidence_breakdown",
        ])

    # Strip SSN last4 before writing, then sanitize CSV output.
    safe_to_csv(
        matched_raw.drop(
            columns=["old_last4_ssn", "new_last4_ssn"], errors="ignore"
        ),
        OUT / "matched_raw.csv"
    )

    # Ambiguous rows: all rows dropped by _one_to_one_join across every tier.
    # last4_ssn is intentionally retained here for internal identity review only.
    ambiguous = (
        pd.concat([x for x in all_ambiguous if len(x) > 0], ignore_index=True)
        if any(len(x) > 0 for x in all_ambiguous)
        else pd.DataFrame(columns=list(_AMB_COLS))
    )
    safe_to_csv(ambiguous, OUT / "ambiguous_matches.csv")

    # Add unmatched_reason: "no_id" for blank/null worker_id rows,
    # "no_match_found" for all non-empty worker_id rows that did not match.
    # ID-less rows were merged into old/new before the fallback tiers, so any
    # that remain unmatched here are already in the pool — tag by worker_id value.
    old = old.drop(columns=["_match_worker_id", "_match_recon_id"], errors="ignore").copy()
    old["unmatched_reason"] = old["worker_id"].apply(
        lambda v: "no_id" if _is_missing_id(v) else "no_match_found"
    ) if "worker_id" in old.columns else "no_match_found"

    new = new.drop(columns=["_match_worker_id", "_match_recon_id"], errors="ignore").copy()
    new["unmatched_reason"] = new["worker_id"].apply(
        lambda v: "no_id" if _is_missing_id(v) else "no_match_found"
    ) if "worker_id" in new.columns else "no_match_found"

    safe_to_csv(old, OUT / "unmatched_old.csv")
    safe_to_csv(new, OUT / "unmatched_new.csv")

    report["matched_total"] = int(len(matched_raw))
    report["unmatched_old"] = int(len(old))
    report["unmatched_new"] = int(len(new))
    report["ambiguous_old_count"] = int((ambiguous["side"] == "old").sum()) if not ambiguous.empty else 0
    report["ambiguous_new_count"] = int((ambiguous["side"] == "new").sum()) if not ambiguous.empty else 0
    report["ambiguous_total"] = int(len(ambiguous))

    (OUT / "match_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("[matcher] complete")
    print(f"[matcher] matched_total: {report['matched_total']}")
    print(f"[matcher] matched_by_worker_id: {report['matched_by_worker_id']}")
    print(f"[matcher] matched_by_recon_id: {report['matched_by_recon_id']}")
    print(f"[matcher] matched_by_pk: {report['matched_by_pk']}")
    print(f"[matcher] matched_by_last4_dob: {report['matched_by_last4_dob']}")
    print(f"[matcher] matched_by_dob_name: {report['matched_by_dob_name']}")
    print(f"[matcher] matched_by_name_hire_date: {report['matched_by_name_hire_date']}")
    print(f"[matcher] unmatched_old: {report['unmatched_old']}")
    print(f"[matcher] unmatched_new: {report['unmatched_new']}")
    print(f"[matcher] ambiguous_total: {report['ambiguous_total']}")
    print(f"[matcher] ambiguous_old_count: {report['ambiguous_old_count']}")
    print(f"[matcher] ambiguous_new_count: {report['ambiguous_new_count']}")


if __name__ == "__main__":
    main()
