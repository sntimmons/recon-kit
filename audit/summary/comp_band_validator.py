"""
audit/summary/comp_band_validator.py - Optional compensation band validation.

Reads wide_compare.csv and an optional compensation_bands.csv.
Annotates each matched pair with:
  comp_band_status  : within_band | below_band_min | above_band_max | no_band_found
  comp_band_min     : float or blank
  comp_band_mid     : float or blank
  comp_band_max     : float or blank
  comp_band_match   : job_title string used for the band lookup

For rows flagged below_band_min or above_band_max, action is overridden to
REVIEW and the comp_band reason is appended to the existing reason string.

Band file format (CSV):
  job_title, location_state (optional), band_min, band_mid, band_max

Match priority:
  1. Exact job_title + location_state (if state column present in bands file)
  2. Exact job_title only
  3. Fuzzy job_title (>= 0.85) + location_state
  4. Fuzzy job_title only (>= 0.85)
  5. no_band_found

Usage:
  python audit/summary/comp_band_validator.py \\
         --wide  <wide_compare.csv> \\
         --bands <compensation_bands.csv> \\
         [--out  <output_path>]      # default: overwrite --wide
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

try:
    from rapidfuzz import fuzz as _rfuzz
    _HAS_RAPIDFUZZ = True
except ImportError:
    _HAS_RAPIDFUZZ = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_FUZZY_THRESHOLD = 0.85   # 0..1  (rapidfuzz returns 0..100, converted below)
_COMP_BAND_COLS  = [
    "comp_band_status",
    "comp_band_min",
    "comp_band_mid",
    "comp_band_max",
    "comp_band_match",
]


# ---------------------------------------------------------------------------
# Band loading
# ---------------------------------------------------------------------------

def _norm_title(s: str) -> str:
    """Lower-case, strip, collapse whitespace for job-title comparison."""
    return " ".join(str(s or "").lower().split())


def load_bands(bands_path: Path) -> "tuple[pd.DataFrame, bool]":
    """Read and normalise the compensation_bands.csv file."""
    df = pd.read_csv(bands_path, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]

    # Rename common aliases
    col_aliases = {
        "title": "job_title",
        "role":  "job_title",
        "state": "location_state",
        "min":   "band_min",
        "mid":   "band_mid",
        "max":   "band_max",
        "min_salary": "band_min",
        "max_salary": "band_max",
    }
    df.rename(columns={k: v for k, v in col_aliases.items() if k in df.columns}, inplace=True)

    for req in ("job_title", "band_min", "band_max"):
        if req not in df.columns:
            raise ValueError(
                f"compensation_bands.csv is missing required column '{req}'. "
                f"Found columns: {list(df.columns)}"
            )

    if "band_mid" not in df.columns:
        df["band_mid"] = None

    # Numeric coercion
    for col in ("band_min", "band_mid", "band_max"):
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "").str.replace("$", ""),
            errors="coerce",
        )

    df["_title_norm"] = df["job_title"].apply(_norm_title)
    has_state = "location_state" in df.columns
    if has_state:
        df["_state_norm"] = df["location_state"].fillna("").str.strip().str.lower()

    return df, has_state


# ---------------------------------------------------------------------------
# Band lookup
# ---------------------------------------------------------------------------

def _fuzzy_score(a: str, b: str) -> float:
    """Return fuzzy similarity 0..1 between two normalised strings."""
    if not _HAS_RAPIDFUZZ:
        # Fallback: Jaccard over tokens
        ta = set(a.split())
        tb = set(b.split())
        if not ta and not tb:
            return 1.0
        if not ta or not tb:
            return 0.0
        return len(ta & tb) / len(ta | tb)
    return _rfuzz.token_sort_ratio(a, b) / 100.0


def lookup_band(
    title: str,
    state: str,
    bands: pd.DataFrame,
    has_state: bool,
) -> "dict | None":
    """
    Find the best-matching band row for (title, state).
    Returns a dict with band fields or None.
    """
    t_norm = _norm_title(title)
    s_norm = (state or "").strip().lower()

    # ---- 1. Exact title + state ----
    if has_state and s_norm:
        exact = bands[(bands["_title_norm"] == t_norm) & (bands["_state_norm"] == s_norm)]
        if not exact.empty:
            return exact.iloc[0].to_dict()

    # ---- 2. Exact title only ----
    exact_t = bands[bands["_title_norm"] == t_norm]
    if not exact_t.empty:
        return exact_t.iloc[0].to_dict()

    # ---- 3. Fuzzy title + state ----
    if has_state and s_norm:
        state_bands = bands[bands["_state_norm"] == s_norm]
        if not state_bands.empty:
            scores = state_bands["_title_norm"].apply(lambda x: _fuzzy_score(t_norm, x))
            best_idx = scores.idxmax()
            if scores[best_idx] >= _FUZZY_THRESHOLD:
                return state_bands.loc[best_idx].to_dict()

    # ---- 4. Fuzzy title only ----
    if not bands.empty:
        scores = bands["_title_norm"].apply(lambda x: _fuzzy_score(t_norm, x))
        best_idx = scores.idxmax()
        if scores[best_idx] >= _FUZZY_THRESHOLD:
            return bands.loc[best_idx].to_dict()

    return None


# ---------------------------------------------------------------------------
# Main annotation logic
# ---------------------------------------------------------------------------

def annotate_wide(
    wide_path: Path,
    bands_path: Path,
    out_path: Path,
) -> dict:
    """
    Annotate wide_compare.csv with compensation band status.

    Returns a summary dict:
        n_total, n_within, n_below, n_above, n_no_band
    """
    bands_df, has_state = load_bands(bands_path)

    wide = pd.read_csv(wide_path, dtype=str, low_memory=False)

    # Ensure output columns exist (fill with defaults)
    for col in _COMP_BAND_COLS:
        if col not in wide.columns:
            wide[col] = ""

    n_within  = 0
    n_below   = 0
    n_above   = 0
    n_no_band = 0

    # Collect annotation results in lists (vectorised assignment - avoids O(n²) iterrows loop)
    statuses:    list[str] = []
    band_mins:   list      = []
    band_mids:   list      = []
    band_maxes:  list      = []
    band_matches: list[str] = []
    new_actions: list[str] = []
    new_reasons: list[str] = []

    for _, row in wide.iterrows():
        title       = str(row.get("new_position") or row.get("old_position") or "").strip()
        state       = str(row.get("new_location_state") or row.get("old_location_state") or "").strip()
        new_sal_raw = str(row.get("new_salary") or "").strip()

        # Parse new_salary
        try:
            new_sal = float(new_sal_raw.replace(",", "").replace("$", "")) if new_sal_raw else None
        except (ValueError, TypeError):
            new_sal = None

        band = lookup_band(title, state, bands_df, has_state) if title else None

        if band is None:
            status     = "no_band_found"
            band_min   = ""
            band_mid   = ""
            band_max   = ""
            band_match = ""
            n_no_band += 1
        else:
            band_min   = band.get("band_min")
            band_mid   = band.get("band_mid")
            band_max   = band.get("band_max")
            band_match = str(band.get("job_title", ""))

            if new_sal is None:
                status = "no_band_found"
                n_no_band += 1
            elif band_min is not None and new_sal < band_min:
                status  = "below_band_min"
                n_below += 1
            elif band_max is not None and new_sal > band_max:
                status  = "above_band_max"
                n_above += 1
            else:
                status    = "within_band"
                n_within += 1

        # Compute action / reason override for out-of-band records
        current_action = str(row.get("action") or "").strip()
        current_reason = str(row.get("reason") or "").strip()
        if status in ("below_band_min", "above_band_max") and current_action != "REJECT_MATCH":
            band_reason = f"comp_band:{status} ({band_match})"
            new_actions.append("REVIEW")
            new_reasons.append(f"{current_reason}|{band_reason}" if current_reason else band_reason)
        else:
            new_actions.append(current_action)
            new_reasons.append(current_reason)

        statuses.append(status)
        band_mins.append("" if band_min is None else band_min)
        band_mids.append("" if band_mid is None else band_mid)
        band_maxes.append("" if band_max is None else band_max)
        band_matches.append(band_match)

    # Vectorised write-back (fast - single assignment per column)
    wide["comp_band_status"] = statuses
    wide["comp_band_min"]    = band_mins
    wide["comp_band_mid"]    = band_mids
    wide["comp_band_max"]    = band_maxes
    wide["comp_band_match"]  = band_matches
    wide["action"]           = new_actions
    wide["reason"]           = new_reasons

    wide.to_csv(out_path, index=False)

    n_total = len(wide)
    print(
        f"[comp_band] annotated {n_total:,} rows: "
        f"within={n_within:,}  below={n_below:,}  above={n_above:,}  "
        f"no_band={n_no_band:,}"
    )

    return {
        "n_total":   n_total,
        "n_within":  n_within,
        "n_below":   n_below,
        "n_above":   n_above,
        "n_no_band": n_no_band,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Compensation band validation for recon-kit wide_compare.csv."
    )
    parser.add_argument("--wide",  required=True, help="Path to wide_compare.csv")
    parser.add_argument("--bands", required=True, help="Path to compensation_bands.csv")
    parser.add_argument("--out",   default=None,  help="Output path (default: overwrite --wide)")
    args = parser.parse_args(argv)

    wide_path  = Path(args.wide)
    bands_path = Path(args.bands)
    out_path   = Path(args.out) if args.out else wide_path

    if not wide_path.exists():
        print(f"[error] wide_compare not found: {wide_path}", file=sys.stderr)
        sys.exit(1)
    if not bands_path.exists():
        print(f"[error] bands file not found: {bands_path}", file=sys.stderr)
        sys.exit(1)

    annotate_wide(wide_path, bands_path, out_path)


if __name__ == "__main__":
    main()
