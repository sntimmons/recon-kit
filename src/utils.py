from __future__ import annotations

import re
from datetime import datetime
import pandas as pd


_NULLS = {"", "nan", "none", "null"}


def clean_dob_value(s: str) -> str:
    """
    Normalize DOB to YYYY-MM-DD.
    Uses explicit formats first to reduce ambiguous parsing risk.
    Returns "" if it cannot parse safely.
    """
    if s is None:
        return ""

    s = str(s).strip()
    if s.lower() in _NULLS:
        return ""

    # Strip common timestamp tails: "2020-01-01 00:00:00", "01/01/2020 2:30 PM"
    s = re.sub(r"\s+\d{1,2}:\d{2}:\d{2}.*$", "", s).strip()
    s = re.sub(r"\s+\d{1,2}:\d{2}\s*(AM|PM)$", "", s, flags=re.IGNORECASE).strip()

    formats = [
        "%Y-%m-%d", "%Y/%m/%d",
        "%m/%d/%Y", "%m-%d-%Y",
        "%d/%m/%Y", "%d-%m-%Y",
        "%Y%m%d",
        "%d-%b-%Y",
        "%b %d %Y", "%B %d, %Y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    try:
        return pd.to_datetime(s, errors="raise").strftime("%Y-%m-%d")
    except Exception:
        return ""
