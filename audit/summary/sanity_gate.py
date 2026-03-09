"""
sanity_gate.py — Evaluate sanity check results against policy thresholds.

Public API
----------
evaluate_sanity_gate(results: dict, policy: dict) -> dict

    results : dict returned by run_sanity_checks()
    policy  : dict returned by load_policy()

    Returns:
    {
      "passed"         : bool,
      "reasons"        : [str, ...],
      "blocked_outputs": {"corrections": bool, "workbook": bool, "exports": bool},
      "metrics"        : {key: {"count": int, "rate": float,
                                "rate_threshold": float|None,
                                "count_threshold": int|None}},
    }
"""
from __future__ import annotations


def evaluate_sanity_gate(results: dict, policy: dict) -> dict:
    """
    Evaluate sanity check results against policy thresholds.

    If sanity_gate.enabled is False, returns passed=True immediately.

    Checks both rate and count thresholds for each suspicious-pattern key.
    If any threshold is violated, passed=False and blocked_outputs reflects
    the block_corrections / block_workbook / block_exports flags in policy.
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
    }
