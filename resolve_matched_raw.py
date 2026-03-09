# resolve_matched_raw.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import pandas as pd


def _s(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def _pair_id(old_key: str, new_key: str) -> str:
    h = hashlib.sha256(f"{old_key}|{new_key}".encode("utf-8")).hexdigest()
    return h[:12]


@dataclass
class Weights:
    worker_id: int = 100
    recon_id: int = 90
    pk: int = 80
    last4_dob: int = 70
    dob_name: int = 60
    name_hire_date: int = 50


W = Weights()


def _score(match_source: str) -> int:
    return getattr(W, match_source, 10)


def _first_nonempty(*vals: str) -> str:
    for v in vals:
        if v:
            return v
    return ""


# Columns added internally during resolution that should NOT appear in the output CSV.
_INTERNAL_COLS = [
    "old_entity_key",
    "new_entity_key",
    "_ord",
    "old_pk",
    "new_pk",
    "exact_recon_id",
    "exact_pk",
]


def resolve(input_path: Path, output_path: Path) -> None:
    df = pd.read_csv(input_path, dtype="string", keep_default_na=False)

    if df.empty:
        df.to_csv(output_path, index=False)
        print("[resolve] loaded 0 candidate rows from matched_raw.csv")
        print("[resolve] wrote 0 1-to-1 rows -> matched_raw.csv")
        print("[resolve] conflicts_new_worker_id_resolution.csv: 0 rows written")
        print("[resolve] conflicts_old_worker_id_resolution.csv: 0 rows written")
        print("[resolve] skipped_missing_entity_keys.csv: 0 rows written")
        return

    # Always-available columns
    df["old_worker_id"] = _s(df.get("old_worker_id", pd.Series([""] * len(df))))
    df["new_worker_id"] = _s(df.get("new_worker_id", pd.Series([""] * len(df))))

    # Optional columns (some tiers may not populate these)
    df["old_recon_id"] = _s(df.get("old_recon_id", pd.Series([""] * len(df))))
    df["new_recon_id"] = _s(df.get("new_recon_id", pd.Series([""] * len(df))))

    df["old_pk"] = _s(df.get("old_pk", pd.Series([""] * len(df))))
    df["new_pk"] = _s(df.get("new_pk", pd.Series([""] * len(df))))

    # Preserve match_source; only substitute "unknown" when truly missing/blank.
    df["match_source"] = _s(df.get("match_source", pd.Series(["unknown"] * len(df))))
    df.loc[df["match_source"] == "", "match_source"] = "unknown"

    # Best entity key per side for enforcing 1-to-1: worker_id > recon_id > pk
    df["old_entity_key"] = [
        _first_nonempty(w, r, p)
        for w, r, p in zip(
            df["old_worker_id"].tolist(),
            df["old_recon_id"].tolist(),
            df["old_pk"].tolist(),
        )
    ]
    df["new_entity_key"] = [
        _first_nonempty(w, r, p)
        for w, r, p in zip(
            df["new_worker_id"].tolist(),
            df["new_recon_id"].tolist(),
            df["new_pk"].tolist(),
        )
    ]

    # Pair id uses entity keys so it works even when worker_id is blank.
    df["pair_id"] = [
        _pair_id(o, n)
        for o, n in zip(df["old_entity_key"].tolist(), df["new_entity_key"].tolist())
    ]

    df["source_score"] = df["match_source"].map(_score).fillna(10).astype(int)

    # Tie-breakers
    df["exact_worker_id"] = (
        (df["old_worker_id"] != "") & (df["old_worker_id"] == df["new_worker_id"])
    ).astype(int)
    df["exact_recon_id"] = (
        (df["old_recon_id"] != "") & (df["old_recon_id"] == df["new_recon_id"])
    ).astype(int)
    df["exact_pk"] = (
        (df["old_pk"] != "") & (df["old_pk"] == df["new_pk"])
    ).astype(int)

    # Stable ordering
    df["_ord"] = range(len(df))

    # Skip rows where we cannot enforce 1-to-1 at all
    missing_mask = (df["old_entity_key"] == "") | (df["new_entity_key"] == "")
    skipped_missing = df[missing_mask].drop(columns=_INTERNAL_COLS, errors="ignore").copy()

    work = df[~missing_mask].copy()

    # Sort best evidence first, then greedy 1-to-1
    work = work.sort_values(
        by=["exact_worker_id", "exact_recon_id", "exact_pk", "source_score", "_ord"],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    ).copy()

    used_old: set[str] = set()
    used_new: set[str] = set()
    kept_rows: list[dict] = []

    for row in work.itertuples(index=False):
        o = row.old_entity_key
        n = row.new_entity_key
        if o in used_old or n in used_new:
            continue
        used_old.add(o)
        used_new.add(n)
        kept_rows.append(row._asdict())

    out = pd.DataFrame(kept_rows)

    # Conflicts: rows we did NOT keep that were blocked by already-claimed keys
    if not work.empty:
        kept_pair_ids = set(out["pair_id"].tolist()) if not out.empty else set()
        not_kept = work[~work["pair_id"].isin(kept_pair_ids)].copy()

        conflicts_old = not_kept[not_kept["old_entity_key"].isin(used_old)].copy()
        conflicts_new = not_kept[not_kept["new_entity_key"].isin(used_new)].copy()
    else:
        conflicts_old = work.copy()
        conflicts_new = work.copy()

    # Strip internal columns before writing any output file
    out = out.drop(columns=_INTERNAL_COLS, errors="ignore")
    conflicts_old = conflicts_old.drop(columns=_INTERNAL_COLS, errors="ignore")
    conflicts_new = conflicts_new.drop(columns=_INTERNAL_COLS, errors="ignore")

    root = output_path.resolve().parents[0]
    conflicts_new.to_csv(root / "conflicts_new_worker_id_resolution.csv", index=False)
    conflicts_old.to_csv(root / "conflicts_old_worker_id_resolution.csv", index=False)
    skipped_missing.to_csv(root / "skipped_missing_entity_keys.csv", index=False)

    out.to_csv(output_path, index=False)

    print(f"[resolve] loaded {len(df):,} candidate rows from matched_raw.csv")
    print(f"[resolve] wrote {len(out):,} 1-to-1 rows -> matched_raw.csv")
    print(f"[resolve] conflicts_new_worker_id_resolution.csv: {len(conflicts_new):,} rows written")
    print(f"[resolve] conflicts_old_worker_id_resolution.csv: {len(conflicts_old):,} rows written")
    print(f"[resolve] skipped_missing_entity_keys.csv: {len(skipped_missing):,} rows written")


if __name__ == "__main__":
    # Called as a script by api_server.py:
    #   python resolve_matched_raw.py
    # Resolves outputs/matched_raw.csv in-place (adds pair_id, enforces 1-to-1).
    _root = Path(__file__).resolve().parent
    _path = _root / "outputs" / "matched_raw.csv"
    resolve(input_path=_path, output_path=_path)
