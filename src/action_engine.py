import csv
from pathlib import Path

VALID_ISSUE_TYPES = {"compensation", "identity", "status", "dates", "job_org", "unmatched"}
VALID_ACTIONS = {"view", "export", "generate_corrections"}


def safe_load_csv(path: Path):
    """Load from CSV path safely; return list[dict] or empty list."""
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        return list(reader)


def ensure_path(run_path):
    p = Path(run_path)
    if not p.exists():
        raise FileNotFoundError(f"Run path not found: {run_path}")
    return p


def load_run_artifacts(run_path):
    root = ensure_path(run_path)

    artifacts = {
        "run_summary": safe_load_csv(root / "run_summary.csv"),
        "audit_action_plan": safe_load_csv(root / "audit_action_plan.csv"),
        "review_queue": safe_load_csv(root / "audit" / "summary" / "review_queue.csv"),
        "unmatched_old": safe_load_csv(root / "outputs" / "unmatched_old.csv"),
        "unmatched_new": safe_load_csv(root / "outputs" / "unmatched_new.csv"),
        "audit_q2": safe_load_csv(root / "audit" / "audit_q2_pay_mismatches.csv"),
        "audit_q3": safe_load_csv(root / "audit" / "audit_q3_status_mismatches.csv"),
        "audit_q4": safe_load_csv(root / "audit" / "audit_q4_job_org_mismatches.csv"),
        "audit_q5": safe_load_csv(root / "audit" / "audit_q5_hire_date_mismatches.csv"),
        "audit_q0_old": safe_load_csv(root / "audit" / "audit_q0_duplicate_old_worker_id.csv"),
        "audit_q0_new": safe_load_csv(root / "audit" / "audit_q0_duplicate_new_worker_id.csv"),
        "corrections_manifest": safe_load_csv(root / "audit" / "corrections" / "out" / "corrections_manifest.csv"),
    }
    return artifacts


def start_action_session(run_path):
    assays = load_run_artifacts(run_path)
    print("What do you want to do next?")
    return {
        "available_actions": list(VALID_ACTIONS),
        "artifacts": assays,
    }


def run_action_session(run_path):
    """Runner entry point: accepts run_path, calls start_action_session, returns summary."""
    session = start_action_session(run_path)
    artifacts = session["artifacts"]
    summary = {
        "run_path": run_path,
        "available_actions": session["available_actions"],
        "artifacts_loaded": {k: len(v) for k, v in artifacts.items()},
    }
    return summary


def select_issue(issue_type, artifacts):
    issue_type = issue_type.lower()
    if issue_type == "compensation":
        return artifacts.get("audit_q2", [])
    if issue_type == "identity":
        duplicates = artifacts.get("audit_q0_old", []) + artifacts.get("audit_q0_new", [])
        unmatched = artifacts.get("unmatched_old", []) + artifacts.get("unmatched_new", [])
        return duplicates + unmatched
    if issue_type == "status":
        return artifacts.get("audit_q3", [])
    if issue_type == "dates":
        return artifacts.get("audit_q5", [])
    if issue_type == "job_org":
        return artifacts.get("audit_q4", [])
    if issue_type == "unmatched":
        return artifacts.get("unmatched_old", []) + artifacts.get("unmatched_new", [])
    return []


def view_records(data):
    return data


def filter_records(data, filters):
    if not filters or not isinstance(filters, dict):
        return data
    filtered = []
    for row in data:
        keep = True
        for key, condition in filters.items():
            if not isinstance(condition, dict):
                continue
            op = condition.get("op", "equals")
            value = condition.get("value")
            row_val = row.get(key, "")
            if op == "equals":
                if str(row_val) != str(value):
                    keep = False
                    break
            elif op == "contains":
                if str(value) not in str(row_val):
                    keep = False
                    break
            elif op == "greater_than":
                try:
                    if float(row_val) <= float(value):
                        keep = False
                        break
                except (ValueError, TypeError):
                    keep = False
                    break
        if keep:
            filtered.append(row)
    return filtered


def export_records(data, run_path, filename):
    run_root = ensure_path(run_path)
    export_dir = run_root / "post_run_actions" / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    target = export_dir / filename
    if not data:
        with open(target, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([])
        return str(target)
    keys = list(data[0].keys())
    with open(target, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    return str(target)


def generate_corrections(data, run_path):
    if not data:
        return "No data to generate corrections for."
    run_root = ensure_path(run_path)
    corr_dir = run_root / "post_run_actions" / "corrections"
    corr_dir.mkdir(parents=True, exist_ok=True)
    target = corr_dir / "corrections_subset.csv"
    keys = list(data[0].keys())
    with open(target, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    return str(target)


def confirm_action(action_name, record_count, target_path=None):
    """
    Returns a structured confirmation dict.

    Returns confirmed=False when record_count is 0 so callers
    can detect empty datasets without executing the action.
    """
    if record_count == 0:
        return {
            "confirmed": False,
            "action_name": action_name,
            "record_count": 0,
            "message": f"No records to act on for '{action_name}'. Action will not proceed.",
        }

    msg = f"Ready to {action_name} for {record_count} record(s)."
    if target_path:
        msg += f" Output: {target_path}"

    return {
        "confirmed": True,
        "action_name": action_name,
        "record_count": record_count,
        "message": msg,
    }


def run_guided_action_flow(run_path, issue_type, action, filters=None):
    """
    Guided action flow: load run, select issue, filter, confirm, execute action.

    Returns a structured result dict with keys:
        status, run_path, issue_type, action, record_count, output_path, message
    """
    # --- Input validation ---
    if not Path(run_path).exists():
        return {
            "status": "error",
            "run_path": run_path,
            "issue_type": issue_type,
            "action": action,
            "record_count": 0,
            "output_path": None,
            "message": f"Run path does not exist: {run_path}",
        }

    normalized_issue = issue_type.lower() if isinstance(issue_type, str) else ""
    if normalized_issue not in VALID_ISSUE_TYPES:
        return {
            "status": "error",
            "run_path": run_path,
            "issue_type": issue_type,
            "action": action,
            "record_count": 0,
            "output_path": None,
            "message": (
                f"Invalid issue_type '{issue_type}'. "
                f"Must be one of: {sorted(VALID_ISSUE_TYPES)}"
            ),
        }

    normalized_action = action.lower() if isinstance(action, str) else ""
    if normalized_action not in VALID_ACTIONS:
        return {
            "status": "error",
            "run_path": run_path,
            "issue_type": issue_type,
            "action": action,
            "record_count": 0,
            "output_path": None,
            "message": (
                f"Invalid action '{action}'. "
                f"Must be one of: {sorted(VALID_ACTIONS)}"
            ),
        }

    # --- Load artifacts ---
    try:
        artifacts = load_run_artifacts(run_path)
    except FileNotFoundError as exc:
        return {
            "status": "error",
            "run_path": run_path,
            "issue_type": issue_type,
            "action": action,
            "record_count": 0,
            "output_path": None,
            "message": str(exc),
        }

    # --- Select and filter dataset ---
    data = select_issue(normalized_issue, artifacts)

    if filters:
        try:
            data = filter_records(data, filters)
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "error",
                "run_path": run_path,
                "issue_type": issue_type,
                "action": action,
                "record_count": 0,
                "output_path": None,
                "message": f"Filter failed: {exc}",
            }

    record_count = len(data)

    # --- Confirm action (also handles record_count == 0) ---
    target_filename = None
    if normalized_action == "export":
        target_filename = f"{normalized_issue}_export.csv"
    elif normalized_action == "generate_corrections":
        target_filename = "corrections_subset.csv"

    confirm = confirm_action(normalized_action, record_count, target_filename)
    if not confirm["confirmed"]:
        return {
            "status": "error",
            "run_path": run_path,
            "issue_type": issue_type,
            "action": action,
            "record_count": record_count,
            "output_path": None,
            "message": confirm["message"],
        }

    # --- Execute action ---
    output_path = None

    if normalized_action == "view":
        message = f"Returned {record_count} record(s) for issue type '{normalized_issue}'."

    elif normalized_action == "export":
        try:
            output_path = export_records(data, run_path, target_filename)
            message = f"Exported {record_count} record(s) to {output_path}."
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "error",
                "run_path": run_path,
                "issue_type": issue_type,
                "action": action,
                "record_count": record_count,
                "output_path": None,
                "message": f"Export failed: {exc}",
            }

    elif normalized_action == "generate_corrections":
        try:
            output_path = generate_corrections(data, run_path)
            message = f"Generated corrections for {record_count} record(s) at {output_path}."
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "error",
                "run_path": run_path,
                "issue_type": issue_type,
                "action": action,
                "record_count": record_count,
                "output_path": None,
                "message": f"Corrections generation failed: {exc}",
            }

    return {
        "status": "success",
        "run_path": run_path,
        "issue_type": issue_type,
        "action": action,
        "record_count": record_count,
        "output_path": output_path,
        "message": message,
    }
