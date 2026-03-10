"""
Core gating engine for the reconciliation confidence threshold system.

Public API
----------
infer_fix_types(row: dict) -> list[str]
    Detect which field groups differ between old and new in a matched-pair row.

classify_row(row: dict, fix_type: str) -> dict
    Gate a single fix_type.  Returns:
        action        : "APPROVE" | "REVIEW"
        min_confidence: float | None
        confidence    : float | None
        reason        : str
        match_source  : str

classify_all(row: dict) -> dict
    Classify all detected fix_types.  Returns:
        fix_types : list[str]
        action    : "APPROVE" | "REVIEW"   (REVIEW if ANY fix_type is REVIEW)
        reason    : str
        per_fix   : dict[str, dict]        (one entry per fix_type)

salary_delta(row: dict) -> float | None
    Compute new_salary - old_salary if both parse; else None.

payrate_delta(row: dict) -> float | None
    Compute new_payrate - old_payrate if both parse; else None.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Allow running this file directly or importing from a sibling script.
sys.path.insert(0, str(Path(__file__).parent))

from confidence_policy import (
    is_auto_approve_source,
    get_min_confidence,
    LOW_CONFIDENCE_FLOOR,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_num(x) -> float | None:
    """Parse a value that may be a number, formatted string, or blank."""
    if x is None:
        return None
    s = str(x).strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _parse_confidence(x) -> float | None:
    """Parse a confidence value and validate it is in [0, 1]."""
    v = _parse_num(x)
    if v is None:
        return None
    if 0.0 <= v <= 1.0:
        return v
    return None   # out-of-range confidence treated as missing


def _norm(x) -> str:
    """Normalise a field value for string comparison."""
    if x is None:
        return ""
    return str(x).strip().lower()


def _str_changed(old_val, new_val) -> bool:
    return _norm(old_val) != _norm(new_val)


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def salary_delta(row: dict) -> float | None:
    """Return new_salary - old_salary if both are numeric; else None."""
    old = _parse_num(row.get("old_salary"))
    new = _parse_num(row.get("new_salary"))
    if old is None or new is None:
        return None
    return new - old


def _salary_ratio(row: dict) -> float | None:
    """Return new_salary / old_salary if both are positive numbers; else None."""
    old = _parse_num(row.get("old_salary"))
    new = _parse_num(row.get("new_salary"))
    if old is None or new is None or old == 0.0:
        return None
    return new / old


# Status values that represent a worker leaving the organisation.
_TERMINATED_STATUSES: frozenset[str] = frozenset({
    "terminated", "term", "inactive", "inactivated",
    "separated", "offboarded", "resigned", "released",
    "terminated (involuntary)", "terminated (voluntary)",
})


def payrate_delta(row: dict) -> float | None:
    """Return new_payrate - old_payrate if both are numeric; else None."""
    old = _parse_num(row.get("old_payrate"))
    new = _parse_num(row.get("new_payrate"))
    if old is None or new is None:
        return None
    return new - old


def infer_fix_types(row: dict) -> list[str]:
    """
    Detect which field groups differ between old and new.

    Returns an ordered list of fix-type strings; may be empty if no changes
    are detected.  Order: salary, payrate, status, hire_date, job_org.
    """
    fix_types: list[str] = []

    # Salary: only counts when BOTH sides parse as a number and differ.
    d_sal = salary_delta(row)
    if d_sal is not None and d_sal != 0.0:
        fix_types.append("salary")

    # Payrate: same rule.
    d_pr = payrate_delta(row)
    if d_pr is not None and d_pr != 0.0:
        fix_types.append("payrate")

    # Worker status: string comparison after normalisation.
    if _str_changed(row.get("old_worker_status"), row.get("new_worker_status")):
        fix_types.append("status")

    # Hire date: string comparison (dates stored as text).
    if _str_changed(row.get("old_hire_date"), row.get("new_hire_date")):
        fix_types.append("hire_date")

    # Job / org: any of position, district, location_state changing.
    if (
        _str_changed(row.get("old_position"),       row.get("new_position"))
        or _str_changed(row.get("old_district"),     row.get("new_district"))
        or _str_changed(row.get("old_location_state"), row.get("new_location_state"))
    ):
        fix_types.append("job_org")

    return fix_types


def classify_row(row: dict, fix_type: str) -> dict:
    """
    Gate a single fix_type for a matched-pair row.

    Decision logic
    --------------
    1. If match_source is auto-approve -> APPROVE ("worker_id_auto_approve")
    2. If confidence is None/blank     -> REVIEW  ("missing_confidence")
    3. If confidence >= min_confidence -> APPROVE ("confidence_ok")
    4. If confidence >= 0.80           -> REVIEW  ("below_threshold")
    5. If confidence < 0.80            -> REVIEW  ("low_confidence")
    """
    match_source = _norm(row.get("match_source")) or "unknown"
    confidence   = _parse_confidence(row.get("confidence"))
    min_conf     = get_min_confidence(match_source, fix_type)

    result = {
        "action":         "APPROVE",
        "min_confidence": None if is_auto_approve_source(match_source) else min_conf,
        "confidence":     confidence,
        "reason":         "",
        "match_source":   match_source,
        "fix_type":       fix_type,
    }

    # Rule 1: auto-approve source
    if is_auto_approve_source(match_source):
        result["action"] = "APPROVE"
        result["reason"] = "worker_id_auto_approve"
        return result

    # Rule 2: confidence missing
    if confidence is None:
        result["action"] = "REVIEW"
        result["reason"] = "missing_confidence"
        return result

    # Rule 3: confidence meets threshold
    if confidence >= min_conf:
        result["action"] = "APPROVE"
        result["reason"] = f"confidence_ok ({confidence:.3f}>={min_conf:.3f})"
        return result

    # Rule 4/5: below threshold
    if confidence >= LOW_CONFIDENCE_FLOOR:
        result["action"] = "REVIEW"
        result["reason"] = f"below_threshold ({confidence:.3f}<{min_conf:.3f})"
    else:
        result["action"] = "REVIEW"
        result["reason"] = f"low_confidence ({confidence:.3f}<{LOW_CONFIDENCE_FLOOR:.2f})"

    return result


def classify_all(row: dict, wave_dates: "frozenset[str] | None" = None) -> dict:
    """
    Classify all detected fix_types for a matched-pair row.

    Parameters
    ----------
    row        : matched-pair dict
    wave_dates : optional frozenset of new_hire_date strings detected as bulk-import
                 waves by detect_wave_dates().  Any record whose new_hire_date is in
                 this set is forced to action=REVIEW with reason "hire_date_wave",
                 even if no other field changes are detected.

    Returns
    -------
    dict with keys:
        fix_types : list[str]
        action    : "APPROVE" | "REVIEW"
        reason    : str
        per_fix   : dict[str, dict]
    """
    fix_types = infer_fix_types(row)

    # -------------------------------------------------------------------
    # Override 3: hire_date_wave — evaluated before the early-return so it
    # catches records with no other field changes.
    # -------------------------------------------------------------------
    wave_flagged = False
    if wave_dates:
        new_hd = str(row.get("new_hire_date", "") or "").strip()
        if new_hd and new_hd in wave_dates:
            wave_flagged = True

    if not fix_types:
        if wave_flagged:
            return {
                "fix_types": [],
                "action":    "REVIEW",
                "reason":    "hire_date_wave",
                "per_fix":   {},
            }
        return {
            "fix_types": [],
            "action":    "APPROVE",
            "reason":    "no_changes_detected",
            "per_fix":   {},
        }

    per_fix: dict[str, dict] = {}
    for ft in fix_types:
        per_fix[ft] = classify_row(row, ft)

    # -------------------------------------------------------------------
    # Override 1: extreme salary ratio — fires even for auto-approve
    # sources (e.g. worker_id).  Any ratio outside [0.85, 1.15] → REVIEW.
    # -------------------------------------------------------------------
    if "salary" in per_fix:
        ratio = _salary_ratio(row)
        if ratio is not None and (ratio < 0.85 or ratio > 1.15):
            per_fix["salary"]["action"] = "REVIEW"
            per_fix["salary"]["reason"] = (
                f"salary_ratio_extreme ({ratio:.4f} outside [0.85, 1.15])"
            )

    # -------------------------------------------------------------------
    # Override 2: active → terminated / inactive — always routes to REVIEW
    # regardless of confidence score or match_source auto-approve flag.
    # -------------------------------------------------------------------
    if "status" in per_fix:
        old_status = _norm(row.get("old_worker_status"))
        new_status = _norm(row.get("new_worker_status"))
        if old_status == "active" and new_status in _TERMINATED_STATUSES:
            per_fix["status"]["action"] = "REVIEW"
            per_fix["status"]["reason"] = (
                f"active_to_terminated ({old_status}->{new_status})"
            )

    # Overall: REVIEW if ANY fix_type is REVIEW
    overall_action = (
        "REVIEW" if any(v["action"] == "REVIEW" for v in per_fix.values())
        else "APPROVE"
    )
    review_reasons = [
        f"{ft}:{v['reason']}"
        for ft, v in per_fix.items()
        if v["action"] == "REVIEW"
    ]

    # Append wave flag to reasons and force REVIEW
    if wave_flagged:
        overall_action = "REVIEW"
        review_reasons.append("hire_date_wave")

    overall_reason = "|".join(review_reasons) if review_reasons else "all_fix_types_approved"

    return {
        "fix_types": fix_types,
        "action":    overall_action,
        "reason":    overall_reason,
        "per_fix":   per_fix,
    }


def build_summary_str(row: dict, fix_types: list[str]) -> str:
    """Build a short human-readable summary of what changed."""
    parts: list[str] = []

    if "salary" in fix_types:
        d = salary_delta(row)
        if d is not None:
            sign = "+" if d >= 0 else ""
            parts.append(f"salary:{sign}{d:,.0f}")

    if "payrate" in fix_types:
        d = payrate_delta(row)
        if d is not None:
            sign = "+" if d >= 0 else ""
            parts.append(f"payrate:{sign}{d:,.2f}")

    if "status" in fix_types:
        old = _norm(row.get("old_worker_status")) or "blank"
        new = _norm(row.get("new_worker_status")) or "blank"
        parts.append(f"status:{old}->{new}")

    if "hire_date" in fix_types:
        old = _norm(row.get("old_hire_date")) or "blank"
        new = _norm(row.get("new_hire_date")) or "blank"
        parts.append(f"hire_date:{old}->{new}")

    if "job_org" in fix_types:
        sub: list[str] = []
        if _str_changed(row.get("old_position"), row.get("new_position")):
            sub.append("position")
        if _str_changed(row.get("old_district"), row.get("new_district")):
            sub.append("district")
        if _str_changed(row.get("old_location_state"), row.get("new_location_state")):
            sub.append("location_state")
        parts.append(f"job_org({'+'.join(sub)})")

    return " | ".join(parts) if parts else "no_changes"
