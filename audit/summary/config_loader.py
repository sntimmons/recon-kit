"""
config_loader.py - Load config/policy.yaml with safe defaults.

Usage
-----
    from config_loader import load_policy

    policy = load_policy()        # reads config/policy.yaml from repo root
    policy = load_policy(path)    # custom path

Returns a dict.  Falls back to safe defaults (sanity gate disabled) if the
file is missing, unreadable, or PyYAML is unavailable.

A visible warning is printed to stderr whenever the fallback is used, so the
condition is never silently swallowed in pipeline.log.
"""
from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent   # audit/summary/
ROOT  = _HERE.parents[1]                  # repo root

# ---------------------------------------------------------------------------
# Default confidence gating policy - mirrors confidence_policy.py constants.
# Loaded by confidence_policy.py at import time; only used as fallback when
# policy.yaml is absent or unreadable.
# ---------------------------------------------------------------------------
_DEFAULT_CONFIDENCE_POLICY: dict = {
    "low_confidence_floor": 0.80,
    "match_source": {
        "worker_id":      {"auto_approve": True},
        "recon_id":       {"min_confidence": 0.95},
        "pk":             {"min_confidence": 0.95},
        "last4_dob":      {"min_confidence": 0.97},
        "dob_name":       {"min_confidence": 0.97},
        "name_hire_date": {"min_confidence": 0.97},
        "_default":       {"min_confidence": 0.95},
    },
    "fix_type": {
        "salary":    {"min_confidence": 0.97},
        "payrate":   {"min_confidence": 0.97},
        "status":    {"min_confidence": 0.98},
        "hire_date": {"min_confidence": 0.95},
        "job_org":   {"min_confidence": 0.95},
        "_default":  {"min_confidence": 0.95},
    },
}

# Defaults used when policy.yaml is absent or unreadable.
# sanity_gate.enabled=False so the gate is a no-op without a config file.
_DEFAULT_POLICY: dict = {
    "client_name": "Your Organization",
    "client": {
        "name": "Your Organization",
        "chro_name": "",
        "chro_title": "Chief Human Resources Officer",
    },
    "systems": {
        "old_system": "ADP Workforce Now",
        "new_system": "Workday",
    },
    "gating": {
        "salary_payrate_min_confidence": 0.97,
        "status_min_confidence":         0.98,
        "hire_date_job_org_min_confidence": 0.95,
    },
    "confidence_policy": _DEFAULT_CONFIDENCE_POLICY,
    "ui_contract": {
        "version": "v1",
    },
    "retention": {
        "run_output_hours": 72,
    },
    "extra_fields": {
        "enabled": False,
        "fields":  [],
        "groups":  {},
        "gate":    {"enabled": False, "min_confidence": 0.95},
    },
    "pii": {
        "include_dob_in_ui":      False,   # safe default: suppress DOB - opt in explicitly via policy.yaml
        "include_dob_in_exports": False,
    },
    "sanity_gate": {
        "enabled":      False,
        "fail_exit_code": 3,
        "block_if_rate_greater_than": {
            "hire_date_default_2026_02":  0.02,
            "salary_suspicious_default":  0.02,
        },
        "block_if_count_greater_than": {
            "hire_date_default_2026_02":  500,
            "salary_suspicious_default":  500,
        },
        "block_corrections": True,
        "block_workbook":    False,
        "block_exports":     False,
        "patterns": {
            "hire_date_default_months":  ["2026-02", "2026-03"],
            "salary_suspicious_values":  [40000, 40003, 40013, 40073],
        },
    },
}


def load_policy(path: Path | None = None) -> dict:
    """
    Load config/policy.yaml and return as a dict merged over safe defaults.

    Falls back to defaults (sanity gate disabled) if:
    - The file does not exist
    - PyYAML is not installed
    - The file is malformed

    A visible warning is printed to stderr in each fallback case.
    """
    if path is None:
        path = ROOT / "config" / "policy.yaml"

    if not path.exists():
        print(
            f"[warn] policy.yaml not found at {path} - using internal defaults.",
            file=sys.stderr,
        )
        return _deep_merge({}, _DEFAULT_POLICY)

    try:
        import yaml  # PyYAML

        with open(str(path), "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict):
            print(
                f"[warn] policy.yaml is malformed (expected a mapping, got "
                f"{type(data).__name__}) - using internal defaults.",
                file=sys.stderr,
            )
            return _deep_merge({}, _DEFAULT_POLICY)

        # Merge: defaults provide missing keys; file values win on conflicts.
        return _deep_merge(_DEFAULT_POLICY, data)

    except ImportError:
        print(
            "[warn] PyYAML is not installed - using internal defaults. "
            "Run: venv/Scripts/pip.exe install pyyaml",
            file=sys.stderr,
        )
        return _deep_merge({}, _DEFAULT_POLICY)

    except Exception as exc:
        print(
            f"[warn] policy.yaml could not be read ({exc}) - using internal defaults.",
            file=sys.stderr,
        )
        return _deep_merge({}, _DEFAULT_POLICY)


def load_extra_fields(policy: dict | None = None) -> list[str]:
    """
    Return the list of configured extra field names when extra_fields.enabled=True.

    Each name in the returned list corresponds to old_<name> / new_<name> columns
    that may or may not exist in matched_pairs.  Callers must check for presence
    before using; missing fields should log a warning and be skipped.

    Returns an empty list when disabled or policy is None.
    """
    if policy is None:
        policy = load_policy()
    ef = policy.get("extra_fields", {})
    if not ef.get("enabled", False):
        return []
    fields = ef.get("fields", [])
    return [str(f).strip() for f in fields if f]


def load_audit_config(policy: dict | None = None) -> dict:
    """
    Return the dynamic audit configuration dict.

    Keys:
        fields (list[str])              - extra field names (empty if extra_fields.enabled=False)
        groups (dict[str, list[str]])   - named field groups for mismatch aggregation
        gate   (dict)                   - extra_field_gate config: {enabled, min_confidence}

    The 'groups' map is used to compute mismatch_group_<name>=True when any field
    in the group has a mismatch.  Returns empty fields/groups when disabled.
    """
    if policy is None:
        policy = load_policy()
    ef = policy.get("extra_fields", {})
    enabled = ef.get("enabled", False)
    fields  = [str(f).strip() for f in ef.get("fields", []) if f] if enabled else []
    groups  = dict(ef.get("groups", {})) if enabled else {}
    gate    = dict(ef.get("gate", {"enabled": False, "min_confidence": 0.95}))
    return {"fields": fields, "groups": groups, "gate": gate}


def load_confidence_policy(policy: dict | None = None) -> dict:
    """
    Return the confidence gating policy dict.

    The returned dict has the same structure as _DEFAULT_CONFIDENCE_POLICY:
        {
            "low_confidence_floor": float,
            "match_source": { source_key: {"auto_approve": bool} | {"min_confidence": float} },
            "fix_type":     { ft_key:     {"min_confidence": float} },
        }

    Falls back to _DEFAULT_CONFIDENCE_POLICY when the 'confidence_policy' key
    is absent from the loaded YAML.
    """
    if policy is None:
        policy = load_policy()
    return policy.get("confidence_policy", _DEFAULT_CONFIDENCE_POLICY)


def load_retention_config(policy: dict | None = None) -> dict:
    """
    Return retention configuration.

    Keys:
        run_output_hours (int) - hours to retain dashboard_runs output folders
    """
    if policy is None:
        policy = load_policy()
    return policy.get("retention", {"run_output_hours": 72})


def load_pii_config(policy: dict | None = None) -> dict:
    """
    Return the PII minimization configuration dict.

    Keys:
        include_dob_in_ui      (bool) - include old_dob/new_dob in ui_pairs.csv
        include_dob_in_exports (bool) - include old_dob/new_dob in wide_compare/workbook

    Safe default is False (suppress DOB) so that this module is safe to
    import even without policy.yaml. Exposure requires explicit opt-in.
    """
    if policy is None:
        policy = load_policy()
    return policy.get("pii", {"include_dob_in_ui": False, "include_dob_in_exports": False})


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into a copy of base; override wins on conflicts."""
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result
