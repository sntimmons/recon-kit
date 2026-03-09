import pandas as pd

RAW_OLD = "inputs/old.csv"
RAW_NEW = "inputs/new.csv"

MAPPED_OLD = "outputs/mapped_unmatched_old.csv"
MAPPED_NEW = "outputs/mapped_unmatched_new.csv"


def norm_series(s: pd.Series) -> pd.Series:
    s = s.fillna("").astype(str).str.strip()
    s = s.where(~s.str.lower().isin(["", "nan", "none", "null"]), "")
    return s


def overlap_report(label: str, old_path: str, new_path: str) -> None:
    print("\n" + "=" * 70)
    print(f"{label}")
    print(f"OLD: {old_path}")
    print(f"NEW: {new_path}")
    print("=" * 70)

    o = pd.read_csv(old_path, dtype=str)
    n = pd.read_csv(new_path, dtype=str)

    print(f"rows old={len(o)} new={len(n)}")
    print(f"cols old={len(o.columns)} new={len(n.columns)}")

    def col(df, name):
        return norm_series(df[name]) if name in df.columns else pd.Series([""] * len(df))

    o_worker = col(o, "worker_id")
    n_worker = col(n, "worker_id")

    o_name = col(o, "full_name_norm")
    n_name = col(n, "full_name_norm")

    o_dob = col(o, "dob")
    n_dob = col(n, "dob")

    o_last4 = col(o, "last4_ssn")
    n_last4 = col(n, "last4_ssn")

    def set_nonblank(s):
        return set(v for v in s.unique().tolist() if v)

    def show_overlap(title, a, b, show_samples=True):
        A = set_nonblank(a)
        B = set_nonblank(b)
        inter = A.intersection(B)
        print(f"\n{title}")
        print(f"  unique old={len(A)} unique new={len(B)} overlap={len(inter)}")
        if show_samples and inter:
            samp = sorted(list(inter))[:10]
            print(f"  sample overlap: {samp}")

    print("\nblank counts")
    print(f"  worker_id old blank={(o_worker=='').sum()} / {len(o_worker)} | new blank={(n_worker=='').sum()} / {len(n_worker)}")
    print(f"  full_name_norm old blank={(o_name=='').sum()} / {len(o_name)} | new blank={(n_name=='').sum()} / {len(n_name)}")
    print(f"  dob old blank={(o_dob=='').sum()} / {len(o_dob)} | new blank={(n_dob=='').sum()} / {len(n_dob)}")
    print(f"  last4_ssn old blank={(o_last4=='').sum()} / {len(o_last4)} | new blank={(n_last4=='').sum()} / {len(n_last4)}")

    show_overlap("worker_id", o_worker, n_worker)
    show_overlap("full_name_norm", o_name, n_name)
    show_overlap("name|dob", o_name + "|" + o_dob, n_name + "|" + n_dob, show_samples=False)
    show_overlap("last4|dob", o_last4 + "|" + o_dob, n_last4 + "|" + n_dob, show_samples=False)

    # quick sanity samples
    print("\nOLD samples")
    print("  worker_id:", [v for v in o_worker.head(10).tolist()])
    print("  full_name_norm:", [v for v in o_name.head(5).tolist()])
    print("  dob:", [v for v in o_dob.head(5).tolist()])
    print("  last4_ssn:", [v for v in o_last4.head(5).tolist()])

    print("\nNEW samples")
    print("  worker_id:", [v for v in n_worker.head(10).tolist()])
    print("  full_name_norm:", [v for v in n_name.head(5).tolist()])
    print("  dob:", [v for v in n_dob.head(5).tolist()])
    print("  last4_ssn:", [v for v in n_last4.head(5).tolist()])


def main():
    # Run raw inputs check (only if those files exist)
    try:
        overlap_report("RAW INPUTS CHECK", RAW_OLD, RAW_NEW)
    except FileNotFoundError as e:
        print("\nRAW INPUTS CHECK skipped:", e)

    # Run mapped outputs check
    overlap_report("MAPPED OUTPUTS CHECK", MAPPED_OLD, MAPPED_NEW)


if __name__ == "__main__":
    main()
