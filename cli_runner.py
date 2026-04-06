import os
from pathlib import Path
from src.action_engine import run_guided_action_flow


def get_latest_run_path():
    runs_dir = Path("runs")
    if not runs_dir.exists():
        print("No runs directory found.")
        return None
    run_folders = [d for d in runs_dir.iterdir() if d.is_dir()]
    if not run_folders:
        print("No run folders found.")
        return None
    latest = max(run_folders, key=lambda d: d.stat().st_mtime)
    return str(latest)


def main():
    run_path = input("Enter run path (or press enter to use latest): ").strip()
    if not run_path:
        run_path = get_latest_run_path()
        if not run_path:
            print("No valid run path found. Exiting.")
            return
        print(f"Using latest run: {run_path}")

    print("\nSelect issue type:")
    issue_types = [
        "compensation",
        "identity",
        "status",
        "dates",
        "job_org",
        "unmatched",
    ]
    for idx, itype in enumerate(issue_types, 1):
        print(f"{idx}. {itype}")
    try:
        issue_idx = int(input("Enter number: ").strip())
        issue_type = issue_types[issue_idx - 1]
    except (ValueError, IndexError):
        print("Invalid selection. Exiting.")
        return

    print("\nSelect action:")
    actions = ["view", "export", "generate_corrections"]
    for idx, act in enumerate(actions, 1):
        print(f"{idx}. {act}")
    try:
        action_idx = int(input("Enter number: ").strip())
        action = actions[action_idx - 1]
    except (ValueError, IndexError):
        print("Invalid selection. Exiting.")
        return

    filters = None
    if action in ("view", "export", "generate_corrections"):
        filt = input("Do you want to filter? (y/n): ").strip().lower()
        if filt == "y":
            col = input("Column name: ").strip()
            op = input("Operator (equals / contains / greater_than): ").strip()
            val = input("Value: ").strip()
            filters = {col: {"op": op, "value": val}}

    print("\nRunning action...")
    result = run_guided_action_flow(run_path, issue_type, action, filters)
    print("\n--- Result ---")
    print(f"Status: {result.get('status')}")
    print(f"Message: {result.get('message')}")
    print(f"Records: {result.get('record_count')}")
    if result.get("output_path"):
        print(f"Output: {result.get('output_path')}")

if __name__ == "__main__":
    main()
