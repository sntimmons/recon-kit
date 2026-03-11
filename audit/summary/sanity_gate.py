"""
sanity_gate.py — Evaluate sanity check results against policy thresholds.

Public API
----------
evaluate_sanity_gate(results: dict, policy: dict) -> dict

    results : dict returned by run_sanity_checks() (extended by run_sanity_gate.py
              with approve_rate / approve_count in health_metrics)
    policy  : dict returned by load_policy()

    Returns:
    {
      "passed"         : bool,
      "reasons"        : [str, ...],
      "blocked_outputs": {"corrections": bool, "workbook": bool, "exports": bool},
      "metrics"        : {key: {"count": int, "rate": float,
                                "rate_threshold": float|None,
                                "count_threshold": int|None}},
      "health_checks"  : {key: {"value": ..., "threshold": ..., "passed": bool}},
    }
"""
from __future__ import annotations


def evaluate_sanity_gate(results: dict, policy: dict) -> dict:
    """
    Evaluate sanity check results against policy thresholds.

    If sanity_gate.enabled is False, returns passed=True immediately.

    Two groups of checks are performed:
    1. Suspicious-pattern checks (existing) — rate/count thresholds from policy.
    2. Health-metric checks (Fix 6) — det_rate, approve_rate, active_zero_salary.

    If any check fails, passed=False and blocked_outputs reflects the
    block_corrections / block_workbook / block_exports flags in policy.
    """
    sg      = policy.get("sanity_gate", {})
    enabled = sg.get("enabled", False)

    _no_block = {"corrections": False, "workbook": False, "exports": False}

    if not enabled:
        return {
            "passed":          True,
            "reasons":         ["sanity_gate_disabled"],
            "blocked_outputs": _no_block,
            "metrics":         {},
            "health_checks":   {},
        }

    suspicious  = results.get("suspicious", {})
    rate_thres  = sg.get("block_if_rate_greater_than",  {})
    count_thres = sg.get("block_if_count_greater_than", {})

    reasons: list[str] = []
    metrics: dict      = {}

    # Evaluate every key mentioned in any threshold or in results
    all_keys = sorted(set(rate_thres) | set(count_thres) | set(suspicious))

    for key in all_keys:
        item  = suspicious.get(key, {"count": 0, "rate": 0.0})
        count = item.get("count", 0)
        rate  = item.get("rate",  0.0)

        rt = rate_thres.get(key)
        ct = count_thres.get(key)

        metrics[key] = {
            "count":           count,
            "rate":            rate,
            "rate_threshold":  rt,
            "count_threshold": ct,
        }

        if rt is not None and rate > rt:
            reasons.append(f"{key} rate {rate:.6f} > {rt}")
        if ct is not None and count > ct:
            reasons.append(f"{key} count {count:,} > {ct:,}")

    # -------------------------------------------------------------------
    # Three-part health gate (redefined per user spec):
    #   1. det_rate >= min_det_rate        (worker_id + pk / total)
    #   2. approve_rate >= min_approve_rate (APPROVE / total)
    #   3. active_zero_approved == 0       (active/$0 workers with APPROVE action)
    #      — "active/$0 with corrections staged = 0"
    #      — active_zero_approved is always 0 because salary_ratio override routes
    #        all active/$0 workers to REVIEW before corrections can be staged.
    #      Falls back to the legacy active_zero_salary count if active_zero_approved
    #      is not present in health_metrics (older pipeline runs).
    # -------------------------------------------------------------------
    ht = sg.get("health_thresholds", {})
    min_det_rate     = float(ht.get("min_det_rate",          0.95))
    min_approve_rate = float(ht.get("min_approve_rate",      0.80))
    max_active_zero  = int(  ht.get("max_active_zero_salary", 0))

    hm           = results.get("health_metrics", {})
    det_rate     = float(hm.get("det_rate",           0.0))
    approve_rate = float(hm.get("approve_rate",       -1.0))   # -1 = not computed
    active_zero  = int(  hm.get("active_zero_salary", 0))

    # active_zero_approved: active/$0 workers that received APPROVE action.
    # Only populated by run_sanity_gate.py (not by older sanity_checks.py).
    # Use it when available; fall back to legacy active_zero_salary otherwise.
    if "active_zero_approved" in hm:
        active_zero_check_val = int(hm["active_zero_approved"])
        active_zero_label     = "active/$0 workers with corrections staged (APPROVE action)"
    else:
        active_zero_check_val = active_zero
        active_zero_label     = "active workers with $0 salary detected"

    health_checks: dict[str, dict] = {}

    # det_rate
    health_checks["det_rate"] = {
        "value":     det_rate,
        "threshold": f">= {min_det_rate}",
        "passed":    det_rate >= min_det_rate,
    }
    if det_rate < min_det_rate:
        reasons.append(
            f"det_rate {det_rate:.4f} < min {min_det_rate:.2f} "
            f"(too few deterministic matches)"
        )

    # approve_rate (only checked when it was computed)
    if approve_rate >= 0.0:
        health_checks["approve_rate"] = {
            "value":     approve_rate,
            "threshold": f">= {min_approve_rate}",
            "passed":    approve_rate >= min_approve_rate,
        }
        if approve_rate < min_approve_rate:
            reasons.append(
                f"approve_rate {approve_rate:.4f} < min {min_approve_rate:.2f} "
                f"(too many records need review)"
            )
    else:
        health_checks["approve_rate"] = {
            "value":     "not_computed",
            "threshold": f">= {min_approve_rate}",
            "passed":    True,   # don't fail if not computed
        }

    # Check 3: active_zero_approved (critical staged-corrections check)
    health_checks["active_zero_salary"] = {
        "value":      active_zero,           # total active/$0 workers (informational)
        "value_approved": active_zero_check_val,  # with APPROVE action (gate check)
        "threshold":  f"corrections staged <= {max_active_zero}",
        "passed":     active_zero_check_val <= max_active_zero,
    }
    if active_zero_check_val > max_active_zero:
        reasons.append(
            f"active_zero_approved {active_zero_check_val:,} > max {max_active_zero:,} "
            f"({active_zero_label})"
        )

    passed = len(reasons) == 0

    blocked = {
        "corrections": (not passed) and bool(sg.get("block_corrections", False)),
        "workbook":    (not passed) and bool(sg.get("block_workbook",    False)),
        "exports":     (not passed) and bool(sg.get("block_exports",     False)),
    }

    return {
        "passed":          passed,
        "reasons":         reasons,
        "blocked_outputs": blocked,
        "metrics":         metrics,
        "health_checks":   health_checks,
    }
