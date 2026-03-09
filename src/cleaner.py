import re
import hashlib
import pandas as pd

_NICKNAMES = {
    "jon": "john",
    "johnny": "john",
    "mike": "michael",
    "rob": "robert",
    "bob": "robert",
    "bill": "william",
    "liz": "elizabeth",
    "beth": "elizabeth",
    "kate": "katherine",
}

_WORKER_TYPE_MAP = {
    "full time": "regular",
    "full-time": "regular",
    "part time": "regular",
    "part-time": "regular",
    "temp": "contingent",
    "temporary": "contingent",
    "contractor": "contingent",
    "contract": "contingent",
}

_ST_ABBR = {
    "texas": "tx",
    "florida": "fl",
    "california": "ca",
    "new york": "ny",
}

_ADDR_REPL = {
    " street ": " st ",
    " st. ": " st ",
    " avenue ": " ave ",
    " ave. ": " ave ",
    " road ": " rd ",
    " rd. ": " rd ",
    " boulevard ": " blvd ",
    " blvd. ": " blvd ",
    " drive ": " dr ",
    " dr. ": " dr ",
    " lane ": " ln ",
    " ln. ": " ln ",
    " suite ": " ste ",
    " apartment ": " apt ",
}

def _safe_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

def _norm_basic(s: str) -> str:
    s = _safe_str(s).lower()
    s = re.sub(r"[^\w\s-]", " ", s)  # keep hyphen as token separator
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _normalize_name(full_name: str) -> str:
    s = _norm_basic(full_name)
    if not s:
        return ""
    parts = s.split()
    if parts:
        first = parts[0]
        if first in _NICKNAMES:
            parts[0] = _NICKNAMES[first]
    return " ".join(parts)

def _split_last_name(full_name_norm: str) -> str:
    if not full_name_norm:
        return ""
    parts = full_name_norm.split()
    return parts[-1] if parts else ""

def _last_name_prefix(last_name: str, n: int) -> str:
    ln = _norm_basic(last_name)
    return ln[:n] if ln else ""

def _normalize_state(state: str) -> str:
    s = _norm_basic(state)
    if not s:
        return ""
    if s in _ST_ABBR:
        return _ST_ABBR[s]
    if len(s) == 2:
        return s
    return s

def _normalize_location(loc: str) -> tuple[str, str]:
    s = _safe_str(loc)
    if not s:
        return "", ""
    s2 = s.replace(",", " ").strip()
    parts = [p for p in s2.split() if p]
    if len(parts) >= 2:
        city = " ".join(parts[:-1])
        state = parts[-1]
        return _norm_basic(city), _normalize_state(state)
    return _norm_basic(s2), ""

def _normalize_ssn_last4(x) -> str:
    s = _safe_str(x)
    digits = re.sub(r"\D", "", s)
    if len(digits) >= 4:
        return digits[-4:]
    return ""

def _excel_serial_to_date(n: int):
    # Excel serial date (1900 system). 1 -> 1899-12-31, but Excel has a known 1900 leap bug.
    # Pandas handles this well via origin with offset.
    try:
        return pd.to_datetime(n, unit="D", origin="1899-12-30", errors="coerce")
    except Exception:
        return pd.NaT

def _normalize_date(x) -> str:
    s = _safe_str(x)
    if not s:
        return ""

    # Handle pure digits like YYYYMMDD
    if re.fullmatch(r"\d{8}", s):
        try:
            dt = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
            if pd.isna(dt):
                return ""
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return ""

    # Handle Excel serials (common after “export through Excel”)
    if re.fullmatch(r"\d{4,6}", s):
        # likely a serial if it’s a plausible Excel range
        try:
            n = int(s)
            if 20000 <= n <= 60000:
                dt = _excel_serial_to_date(n)
                if pd.isna(dt):
                    return ""
                return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d")

def _normalize_address(addr: str) -> str:
    s = " " + _norm_basic(addr) + " "
    if s.strip() == "":
        return ""
    for k, v in _ADDR_REPL.items():
        s = s.replace(k, v)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _normalize_worker_type(x: str) -> str:
    s = _norm_basic(x)
    if not s:
        return ""
    return _WORKER_TYPE_MAP.get(s, s)

def _hash_recon_id(full_name_norm: str, dob_iso: str, last4_ssn: str) -> str:
    base = f"{full_name_norm}|{dob_iso}|{last4_ssn}".encode("utf-8")
    return hashlib.sha1(base).hexdigest()[:12]

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Flexible-ish expected columns. If the inputs already match, we use them directly.
    # If not, you can add an alias mapping layer later without breaking these.
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = out[col].astype(str)

    # Names
    if "full_name_norm" not in out.columns:
        if "full_name" in out.columns:
            out["full_name_norm"] = out["full_name"].apply(_normalize_name)
        elif "name" in out.columns:
            out["full_name_norm"] = out["name"].apply(_normalize_name)
        else:
            out["full_name_norm"] = ""

    out["last_name_norm"] = out["full_name_norm"].apply(_split_last_name)

    # DOB and hire_date
    if "dob" in out.columns:
        out["dob"] = out["dob"].apply(_normalize_date)
    else:
        out["dob"] = ""

    if "hire_date" in out.columns:
        out["hire_date"] = out["hire_date"].apply(_normalize_date)
    else:
        out["hire_date"] = ""

    # Location parsing
    if "location" in out.columns:
        city_state = out["location"].apply(_normalize_location)
        out["location_city"] = city_state.apply(lambda t: t[0])
        out["location_state"] = city_state.apply(lambda t: t[1])
    else:
        out.setdefault("location_city", "")
        out.setdefault("location_state", "")

    out["location_state"] = out["location_state"].apply(_normalize_state)

    # Address
    if "address_norm" not in out.columns:
        if "address" in out.columns:
            out["address_norm"] = out["address"].apply(_normalize_address)
        else:
            out["address_norm"] = ""

    # SSN last4
    if "last4_ssn" in out.columns:
        out["last4_ssn"] = out["last4_ssn"].apply(_normalize_ssn_last4)
    else:
        out["last4_ssn"] = ""

    # Worker type
    if "worker_type" in out.columns:
        out["worker_type"] = out["worker_type"].apply(_normalize_worker_type)
    else:
        out["worker_type"] = ""

    # Convenience blocking fields
    out["birth_year"] = out["dob"].apply(lambda d: d[:4] if isinstance(d, str) and len(d) >= 4 else "")
    out["last_name_prefix3"] = out["last_name_norm"].apply(lambda ln: _last_name_prefix(ln, 3))

    # Stable recon ids (used for safe merges across reruns)
    out["recon_id"] = [
        _hash_recon_id(n, d, s) for n, d, s in zip(out["full_name_norm"], out["dob"], out["last4_ssn"])
    ]

    return out
