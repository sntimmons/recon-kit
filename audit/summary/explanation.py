"""
explanation.py — Plain-language match explanation generator.

Public API
----------
generate_explanation(row: dict, result: dict) -> str

    Return a plain English sentence a consultant can read and defend out loud,
    describing how the record was matched, what changed, and why it was
    approved or sent for human review.

    Inputs
    ------
    row    : matched-pair dict (same as passed to classify_all)
    result : dict returned by classify_all()

    Output example
    --------------
    "Matched on Worker ID. Salary unchanged. Job title changed from Charge Nurse
     to Accountant — flagged for review."

    "Matched on Worker ID with full confidence. No field changes detected.
     Approved automatically."

    "Matched on name and hire date. Confidence 0.60. Salary increased $256,000,
     status changed active to terminated, hire date changed, job title changed
     — sent to human review."

    "Matched on Worker ID. Salary appears to be a decimal shift — old shows
     $318,500, new shows $31,850. Flagged for review."
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from gating import salary_delta, _parse_confidence, _salary_ratio, _norm
from confidence_policy import is_auto_approve_source

# Human-readable labels for known match_source values
_SOURCE_LABELS: dict[str, str] = {
    "worker_id":      "Worker ID",
    "name_hire_date": "name and hire date",
    "name":           "name only",
    "name_dob":       "name and date of birth",
    "hire_date":      "hire date only",
}

# Salary ratio thresholds: outside this band → likely a decimal-shift data error
_DECIMAL_RATIO_LOW  = 0.2
_DECIMAL_RATIO_HIGH = 5.0


def _fmt_salary(raw) -> str:
    """Format a raw salary value as a dollar string."""
    try:
        v = float(str(raw or "").replace(",", "").replace("$", ""))
        return f"${v:,.0f}"
    except Exception:
        return str(raw or "unknown")


def generate_explanation(row: dict, result: dict) -> str:
    """
    Return a plain English sentence describing this matched-pair record.

    Uses match_source, confidence, fix_types, salary_delta, salary_ratio,
    old/new status, old/new hire date, and old/new position to build the sentence.
    """
    fix_types = result.get("fix_types", [])
    action    = result.get("action",    "APPROVE")
    reason    = result.get("reason",    "")

    # ------------------------------------------------------------------
    # Part 1: How was the match made
    # ------------------------------------------------------------------
    ms        = str(row.get("match_source", "") or "").strip()
    ms_lower  = ms.lower()
    src_label = _SOURCE_LABELS.get(ms_lower, ms or "unknown source")
    match_part = f"Matched on {src_label}"

    # ------------------------------------------------------------------
    # Part 2: Confidence (only shown for non-auto-approve sources)
    # ------------------------------------------------------------------
    conf     = _parse_confidence(row.get("confidence"))
    conf_str = ""
    if not is_auto_approve_source(ms_lower):
        if conf is not None:
            conf_str = f"Confidence {conf:.2f}."
        else:
            conf_str = "Confidence not recorded."

    # ------------------------------------------------------------------
    # Part 3: Field changes — collect change phrases and unchanged notes
    # ------------------------------------------------------------------
    unchanged: list[str] = []   # mentioned explicitly to reassure reader
    changes:   list[str] = []   # things that actually changed

    # Salary
    sal_d = salary_delta(row)
    if "salary" in fix_types and sal_d is not None:
        ratio = _salary_ratio(row)
        o_raw = row.get("old_salary")
        n_raw = row.get("new_salary")
        if ratio is not None and (ratio < _DECIMAL_RATIO_LOW or ratio > _DECIMAL_RATIO_HIGH):
            # Likely a decimal-shift data error
            o_fmt = _fmt_salary(o_raw)
            n_fmt = _fmt_salary(n_raw)
            changes.append(
                f"Salary appears to be a decimal shift — old shows {o_fmt}, new shows {n_fmt}"
            )
        else:
            direction = "increased" if sal_d > 0 else "decreased"
            changes.append(f"Salary {direction} ${abs(sal_d):,.0f}")
    elif fix_types:
        # Other changes present but salary is untouched — state it for clarity
        unchanged.append("Salary unchanged")

    # Status
    if "status" in fix_types:
        old_st = str(row.get("old_worker_status", "") or "").strip() or "unknown"
        new_st = str(row.get("new_worker_status", "") or "").strip() or "unknown"
        changes.append(f"Status changed {old_st} to {new_st}")

    # Hire date
    if "hire_date" in fix_types:
        old_hd = str(row.get("old_hire_date", "") or "").strip() or "unknown"
        new_hd = str(row.get("new_hire_date", "") or "").strip() or "unknown"
        changes.append(f"Hire date changed from {old_hd} to {new_hd}")

    # Job / org
    if "job_org" in fix_types:
        old_pos = str(row.get("old_position", "") or "").strip()
        new_pos = str(row.get("new_position", "") or "").strip()
        if old_pos and new_pos and _norm(old_pos) != _norm(new_pos):
            changes.append(f"Job title changed from {old_pos} to {new_pos}")
        else:
            changes.append("Job title or organization changed")

    # Wave hire-date flag (may be the only reason for REVIEW on an otherwise-clean record)
    wave_only = "hire_date_wave" in reason and "hire_date" not in fix_types
    if wave_only:
        new_hd = str(row.get("new_hire_date", "") or "").strip()
        changes.append(
            f"Hire date {new_hd} matches a bulk import wave"
            if new_hd else "Hire date matches a bulk import wave"
        )

    # ------------------------------------------------------------------
    # Part 4: Decision suffix
    # ------------------------------------------------------------------
    if action == "APPROVE":
        if not fix_types:
            decision = "No field changes detected. Approved automatically."
        else:
            decision = "Approved automatically."
    else:
        # Any REVIEW
        decision = "Flagged for review."

    # ------------------------------------------------------------------
    # Assemble the final sentence
    # ------------------------------------------------------------------
    parts: list[str] = [match_part + "."]

    if conf_str:
        parts.append(conf_str)

    if not fix_types and not wave_only:
        # Completely clean record
        parts.append("No field changes detected. Approved automatically.")
    else:
        # Unchanged notes as short standalone sentences
        for note in unchanged:
            parts.append(note + ".")

        if changes:
            changes_joined = ", ".join(changes)
            if action == "REVIEW":
                parts.append(changes_joined + " — flagged for review.")
            else:
                parts.append(changes_joined + ". Approved automatically.")
        else:
            parts.append(decision)

    return " ".join(parts)
