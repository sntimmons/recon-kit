from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import pandas as pd

# Normalize confidence values before gate check.
# "med" is treated as equivalent to "medium".
_CONF_NORM: dict[str, str] = {
    "high": "high",
    "med": "medium",
    "medium": "medium",
    "low": "low",
}


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _blankify_series(s: pd.Series) -> pd.Series:
    s = s.fillna("").astype(str).str.strip()
    lower = s.str.lower()
    return s.where(~lower.isin(["nan", "none", "null"]), "")


def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in cols}
    for want in candidates:
        got = lower_map.get(want.lower())
        if got:
            return got
    return None


def _find_decision_col(cols: List[str]) -> Optional[str]:
    common = ["decision", "review_decision", "final_decision", "review", "status"]
    c = _pick_col(cols, common)
    if c:
        return c
    for col in cols:
        if "decision" in col.lower():
            return col
    return None


def _is_match_value(v: str) -> bool:
    v = (v or "").strip().lower()
    return v in {"match", "matched", "yes", "y", "true", "1", "approve", "approved"}


def _hash12(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()[:12]


def _ensure_candidate_id(df: pd.DataFrame) -> None:
    if "candidate_id" in df.columns:
        df["candidate_id"] = _blankify_series(df["candidate_id"])
        if (df["candidate_id"] != "").all():
            return

    cols = list(df.columns)

    old_rid = _pick_col(cols, ["old_recon_id", "recon_id_old", "old.recon_id"])
    new_rid = _pick_col(cols, ["new_recon_id", "recon_id_new", "new.recon_id"])

    old_wid = _pick_col(cols, ["old_worker_id", "worker_id_old", "old.worker_id"])
    new_wid = _pick_col(cols, ["new_worker_id", "worker_id_new", "new.worker_id"])

    old_nm = _pick_col(cols, ["old_full_name_norm", "full_name_norm_old"])
    new_nm = _pick_col(cols, ["new_full_name_norm", "full_name_norm_new"])
    old_dob = _pick_col(cols, ["old_dob", "dob_old"])
    new_dob = _pick_col(cols, ["new_dob", "dob_new"])

    def build_row_key(r: pd.Series) -> str:
        if old_rid and new_rid:
            parts = [r.get(old_rid, ""), r.get(new_rid, "")]
        elif old_wid and new_wid:
            parts = [r.get(old_wid, ""), r.get(new_wid, "")]
        elif old_nm and new_nm and old_dob and new_dob:
            parts = [r.get(old_nm, ""), r.get(old_dob, ""), r.get(new_nm, ""), r.get(new_dob, "")]
        else:
            raise ValueError(
                "Cannot generate a stable candidate_id: review file has none of the expected "
                "identity column pairs (old_recon_id/new_recon_id, old_worker_id/new_worker_id, "
                "or old_full_name_norm+old_dob/new_full_name_norm+new_dob)."
            )

        raw = "|".join([str(p or "").strip().lower() for p in parts])
        return _hash12(raw)

    df["candidate_id"] = df.apply(build_row_key, axis=1)


def _to_float(x: str, default: float = 0.0) -> float:
    try:
        return float((x or "").strip())
    except Exception:
        return default


def _to_int(x: str, default: int = 0) -> int:
    try:
        return int(float((x or "").strip()))
    except Exception:
        return default


def finalize(
    review_csv: str = "outputs/review_last4_pairs.csv",
    out_matches_csv: str = "outputs/finalized_matches.csv",
    out_report_json: str = "outputs/finalized_report.json",
    out_ambiguous_csv: str = "outputs/ambiguous_identity_groups.csv",
    allowed_confidence: Optional[Set[str]] = None,
    min_name_similarity: float = 0.90,
) -> None:
    if not os.path.exists(review_csv):
        raise FileNotFoundError(f"Review file not found: {review_csv}")

    allowed_confidence = allowed_confidence or {"high", "medium"}

    df = pd.read_csv(review_csv, dtype=str).copy()

    for c in df.columns:
        df[c] = _blankify_series(df[c])

    decision_col = _find_decision_col(list(df.columns))
    if not decision_col:
        raise ValueError(
            "No decision column found in review file. "
            "Add a column named 'decision' and mark rows as MATCH / NO_MATCH."
        )

    _ensure_candidate_id(df)

    # optional columns (if present)
    conf_col = _pick_col(list(df.columns), ["confidence"])
    score_col = _pick_col(list(df.columns), ["score"])
    name_sim_col = _pick_col(list(df.columns), ["name_similarity"])
    old_dob_col = _pick_col(list(df.columns), ["old_dob"])
    new_dob_col = _pick_col(list(df.columns), ["new_dob"])

    # 1) must be manually approved
    manual_match = df[decision_col].apply(_is_match_value)

    # 2) confidence gate (if column exists) - normalize first so "med" == "medium"
    if conf_col:
        normalized_conf = df[conf_col].str.lower().map(lambda v: _CONF_NORM.get(v, v))
        conf_ok = normalized_conf.isin(allowed_confidence)
    else:
        conf_ok = pd.Series([True] * len(df), index=df.index)

    # 3) evidence gate (score/name_similarity/dob) to prevent "MATCH everything"
    score_ok = pd.Series([False] * len(df), index=df.index)
    if score_col:
        score_ok = df[score_col].apply(lambda x: _to_int(x, 0) >= 1)

    name_ok = pd.Series([False] * len(df), index=df.index)
    if name_sim_col:
        name_ok = df[name_sim_col].apply(lambda x: _to_float(x, 0.0) >= min_name_similarity)

    dob_ok = pd.Series([False] * len(df), index=df.index)
    if old_dob_col and new_dob_col:
        dob_ok = (df[old_dob_col] != "") & (df[new_dob_col] != "") & (df[old_dob_col] == df[new_dob_col])

    evidence_ok = score_ok | name_ok | dob_ok

    match_mask = manual_match & conf_ok & evidence_ok
    rows_marked_match = int(match_mask.sum())
    matches = df[match_mask].copy()

    # ------------------------------------------------------------------
    # 1-to-1 integrity check: remove any pair where old_worker_id or
    # new_worker_id appears more than once in the MATCH set.
    # Excluded rows go to ambiguous_identity_groups.csv for human review.
    # ------------------------------------------------------------------
    cols = list(matches.columns)
    old_wid_col = _pick_col(cols, ["old_worker_id", "worker_id_old"])
    new_wid_col = _pick_col(cols, ["new_worker_id", "worker_id_new"])

    ambiguous_old_ids: set[str] = set()
    ambiguous_new_ids: set[str] = set()
    rows_removed_old = 0
    rows_removed_new = 0

    if old_wid_col:
        old_counts = matches[old_wid_col].value_counts()
        dup_old = old_counts[old_counts > 1].index
        ambiguous_old_ids = set(dup_old.astype(str))
        rows_removed_old = int(matches[old_wid_col].isin(ambiguous_old_ids).sum())

    if new_wid_col:
        # Recompute after potentially marking old-side ambiguous rows too
        new_counts = matches[new_wid_col].value_counts()
        dup_new = new_counts[new_counts > 1].index
        ambiguous_new_ids = set(dup_new.astype(str))
        rows_removed_new = int(matches[new_wid_col].isin(ambiguous_new_ids).sum())

    ambiguous_mask = pd.Series(False, index=matches.index)
    if old_wid_col and ambiguous_old_ids:
        ambiguous_mask |= matches[old_wid_col].isin(ambiguous_old_ids)
    if new_wid_col and ambiguous_new_ids:
        ambiguous_mask |= matches[new_wid_col].isin(ambiguous_new_ids)

    ambiguous = matches[ambiguous_mask].copy()
    clean_matches = matches[~ambiguous_mask].copy()

    # Write ambiguous groups (select preferred columns if present)
    _AMBIGUOUS_COLS = [
        "old_worker_id", "new_worker_id",
        "old_full_name_norm", "new_full_name_norm",
        "old_dob", "new_dob",
        "confidence", "score", "name_similarity",
        "notes", "ai_recommendation", "ai_reason",
    ]
    ambiguous_out_cols = [c for c in _AMBIGUOUS_COLS if c in ambiguous.columns]
    # Include any remaining columns not in the preferred list
    ambiguous_out_cols += [c for c in ambiguous.columns if c not in ambiguous_out_cols]

    os.makedirs(os.path.dirname(out_ambiguous_csv) or ".", exist_ok=True)
    ambiguous[ambiguous_out_cols].to_csv(out_ambiguous_csv, index=False)

    os.makedirs(os.path.dirname(out_matches_csv) or ".", exist_ok=True)
    clean_matches.to_csv(out_matches_csv, index=False)

    report = {
        "review_csv": review_csv,
        "rows_in": int(len(df)),
        "rows_marked_match": rows_marked_match,
        "decision_col": decision_col,
        "ambiguous_old_ids_count": len(ambiguous_old_ids),
        "ambiguous_old_rows_removed": rows_removed_old,
        "ambiguous_new_ids_count": len(ambiguous_new_ids),
        "ambiguous_new_rows_removed": rows_removed_new,
        "finalized_rows_written": int(len(clean_matches)),
        "output_matches_csv": out_matches_csv,
        "output_ambiguous_csv": out_ambiguous_csv,
        "created_at_utc": _now_utc_iso(),
        "guardrails": {
            "allowed_confidence": sorted(list(allowed_confidence)),
            "min_name_similarity": min_name_similarity,
            "evidence_rule": "score>=1 OR name_similarity>=min OR old_dob==new_dob",
            "one_to_one": "enforced - ambiguous old/new ids excluded to ambiguous_identity_groups.csv",
        },
        "notes": [
            "Finalizes only rows with manual MATCH plus guardrails to prevent accidental bulk approvals.",
            "candidate_id is generated deterministically from recon_id pair when available.",
            "1-to-1 enforced: any old or new worker_id appearing more than once is excluded.",
        ],
    }
    with open(out_report_json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"[finalize] decision_col          : {decision_col}")
    print(f"[finalize] rows_in_review         : {len(df)}")
    print(f"[finalize] rows_marked_match      : {rows_marked_match}")
    print(f"[finalize] ambiguous_old_ids      : {len(ambiguous_old_ids)}  ({rows_removed_old} rows removed)")
    print(f"[finalize] ambiguous_new_ids      : {len(ambiguous_new_ids)}  ({rows_removed_new} rows removed)")
    print(f"[finalize] finalized_rows_written : {len(clean_matches)}")
    print(f"[finalize] wrote: {out_matches_csv}")
    print(f"[finalize] wrote: {out_ambiguous_csv}")
    print(f"[finalize] wrote: {out_report_json}")


if __name__ == "__main__":
    finalize()
