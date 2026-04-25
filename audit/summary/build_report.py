from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from csv_safe import safe_to_csv

DB_PATH = ROOT / "audit" / "audit.db"
SUMMARY_DIR = ROOT / "audit" / "summary"
CHARTS_DIR = SUMMARY_DIR / "charts"

_REQUIRED_COLS = [
    "pair_id",
    "match_source",
    "old_worker_id",
    "new_worker_id",
    "old_full_name_norm",
    "new_full_name_norm",
    "old_salary",
    "new_salary",
    "old_payrate",
    "new_payrate",
    "old_worker_status",
    "new_worker_status",
    "old_hire_date",
    "new_hire_date",
    "old_position",
    "new_position",
    "old_district",
    "new_district",
    "old_location_state",
    "new_location_state",
]

_REVIEW_COLS = [
    "pair_id",
    "match_source",
    "old_worker_id",
    "new_worker_id",
    "old_full_name_norm",
    "new_full_name_norm",
    "old_salary",
    "new_salary",
    "salary_delta",
    "old_worker_status",
    "new_worker_status",
    "old_hire_date",
    "new_hire_date",
    "old_position",
    "new_position",
    "old_district",
    "new_district",
    "old_location_state",
    "new_location_state",
    "priority_reason",
    "priority_score",
]


def _parse_num(x) -> float | None:
    if x is None:
        return None
    s = str(x).strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _safe_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].fillna("").astype(str).str.strip()
    return pd.Series([""] * len(df), index=df.index)


def _load_data() -> pd.DataFrame:
    if not DB_PATH.exists():
        print(f"[error] audit.db not found at {DB_PATH}", file=sys.stderr)
        sys.exit(2)
    con = sqlite3.connect(str(DB_PATH))
    try:
        try:
            mp = pd.read_sql_query("SELECT * FROM matched_pairs", con)
        except Exception as exc:
            print(f"[error] could not query matched_pairs: {exc}", file=sys.stderr)
            sys.exit(2)
    finally:
        con.close()
    return mp


def _check_columns(mp: pd.DataFrame) -> None:
    cols = set(mp.columns)
    missing = [c for c in _REQUIRED_COLS if c not in cols]
    if missing:
        print(
            f"[error] matched_pairs is missing required columns: {sorted(missing)}",
            file=sys.stderr,
        )
        sys.exit(2)


def _build_salary_features(mp: pd.DataFrame) -> pd.DataFrame:
    df = mp.copy()
    df["_old_sal"] = df["old_salary"].map(_parse_num)
    df["_new_sal"] = df["new_salary"].map(_parse_num)
    df["_both_valid"] = df["_old_sal"].notna() & df["_new_sal"].notna()
    df["_sal_differ"] = df["_both_valid"] & (df["_old_sal"] != df["_new_sal"])
    df["salary_delta"] = df.apply(
        lambda r: (r["_new_sal"] - r["_old_sal"]) if r["_sal_differ"] else None,
        axis=1,
    )
    return df


def _make_charts(df: pd.DataFrame) -> dict[str, Path]:
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    mismatch = df[df["_sal_differ"]].copy()
    deltas = mismatch["salary_delta"].dropna()

    # Chart 1: salary delta histogram
    if len(deltas) > 0:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.hist(deltas, bins=50, edgecolor="black")
        ax.set_xlabel("Salary Delta (new - old)")
        ax.set_ylabel("Count")
        ax.set_title("Salary Change Distribution")
        fig.tight_layout()
        p = CHARTS_DIR / "salary_delta_hist.png"
        fig.savefig(str(p), dpi=100)
        plt.close(fig)
        paths["salary_delta_hist"] = p

    # Chart 2: salary ratio histogram (new/old)
    if len(deltas) > 0:
        old_vals = mismatch["_old_sal"].replace(0, None).dropna()
        new_vals = mismatch.loc[old_vals.index, "_new_sal"]
        ratios = (new_vals / old_vals).dropna()
        ratios = ratios[(ratios > 0) & (ratios < 5)]
        if len(ratios) > 0:
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.hist(ratios, bins=50, edgecolor="black")
            ax.axvline(x=1.0, color="red", linestyle="--", label="ratio=1 (no change)")
            ax.set_xlabel("Salary Ratio (new / old)")
            ax.set_ylabel("Count")
            ax.set_title("Salary Ratio Distribution (mismatches only)")
            ax.legend()
            fig.tight_layout()
            p = CHARTS_DIR / "salary_ratio_hist.png"
            fig.savefig(str(p), dpi=100)
            plt.close(fig)
            paths["salary_ratio_hist"] = p

    # Chart 3: match_source bar
    src_counts = (
        df["match_source"]
        .fillna("unknown")
        .replace("", "unknown")
        .value_counts()
    )
    fig, ax = plt.subplots(figsize=(8, 4))
    src_counts.plot(kind="bar", ax=ax, edgecolor="black")
    ax.set_xlabel("Match Source")
    ax.set_ylabel("Pair Count")
    ax.set_title("Matched Pairs by Source Tier")
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    p = CHARTS_DIR / "match_source_bar.png"
    fig.savefig(str(p), dpi=100)
    plt.close(fig)
    paths["match_source_bar"] = p

    return paths


def _build_review_queue(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        reasons = []
        score = 0

        src = str(r.get("match_source", "")).strip()
        if src != "worker_id":
            reasons.append("non_worker_id_match")
            score += 50

        old_st = str(r.get("old_worker_status", "")).strip()
        new_st = str(r.get("new_worker_status", "")).strip()
        if old_st != new_st:
            reasons.append("status_mismatch")
            score += 40

        delta = r.get("salary_delta")
        if delta is not None and not pd.isna(delta):
            abs_d = abs(float(delta))
            if abs_d >= 5000:
                reasons.append("salary_delta_ge5000")
                score += 30
            elif abs_d >= 1000:
                reasons.append("salary_delta_1000_4999")
                score += 20

        old_hd = str(r.get("old_hire_date", "")).strip()
        new_hd = str(r.get("new_hire_date", "")).strip()
        if old_hd != new_hd:
            reasons.append("hire_date_mismatch")
            score += 15

        old_pos = str(r.get("old_position", "")).strip()
        new_pos = str(r.get("new_position", "")).strip()
        if old_pos != new_pos:
            reasons.append("position_mismatch")
            score += 10

        old_dist = str(r.get("old_district", "")).strip()
        new_dist = str(r.get("new_district", "")).strip()
        if old_dist != new_dist:
            reasons.append("district_mismatch")
            score += 10

        old_ls = str(r.get("old_location_state", "")).strip()
        new_ls = str(r.get("new_location_state", "")).strip()
        if old_ls != new_ls:
            reasons.append("location_state_mismatch")
            score += 10

        delta_val = delta if (delta is not None and not pd.isna(delta)) else None

        rows.append({
            "pair_id": r.get("pair_id", ""),
            "match_source": src,
            "old_worker_id": r.get("old_worker_id", ""),
            "new_worker_id": r.get("new_worker_id", ""),
            "old_full_name_norm": r.get("old_full_name_norm", ""),
            "new_full_name_norm": r.get("new_full_name_norm", ""),
            "old_salary": r.get("old_salary", ""),
            "new_salary": r.get("new_salary", ""),
            "salary_delta": delta_val,
            "old_worker_status": old_st,
            "new_worker_status": new_st,
            "old_hire_date": old_hd,
            "new_hire_date": new_hd,
            "old_position": old_pos,
            "new_position": new_pos,
            "old_district": old_dist,
            "new_district": new_dist,
            "old_location_state": old_ls,
            "new_location_state": new_ls,
            "priority_reason": "|".join(reasons) if reasons else "none",
            "priority_score": score,
        })

    queue = pd.DataFrame(rows, columns=_REVIEW_COLS)
    queue = queue.sort_values(
        by=["priority_score", "salary_delta"],
        ascending=[False, True],
        key=lambda s: s.abs() if s.name == "salary_delta" else s,
        na_position="last",
    )
    return queue


def _md_table(headers: list[str], rows: list[list]) -> str:
    sep = " | "
    lines = [sep.join(headers)]
    lines.append(sep.join(["---"] * len(headers)))
    for row in rows:
        lines.append(sep.join(str(v) for v in row))
    return "\n".join(lines)


def _build_markdown(
    mp: pd.DataFrame,
    df: pd.DataFrame,
    queue: pd.DataFrame,
    charts: dict[str, Path],
    run_ts: str,
) -> str:
    total = len(mp)
    src_counts = (
        df["match_source"]
        .fillna("unknown")
        .replace("", "unknown")
        .value_counts()
        .reset_index()
    )
    src_counts.columns = ["match_source", "count"]

    mismatch_rows = df[df["_sal_differ"]]
    n_mismatch = len(mismatch_rows)
    n_increase = int((mismatch_rows["salary_delta"] > 0).sum()) if n_mismatch else 0
    n_decrease = int((mismatch_rows["salary_delta"] < 0).sum()) if n_mismatch else 0

    status_diff = df[
        df["old_worker_status"].fillna("").str.strip()
        != df["new_worker_status"].fillna("").str.strip()
    ]

    job_org_diff = df[
        (df["old_position"].fillna("").str.strip() != df["new_position"].fillna("").str.strip())
        | (df["old_district"].fillna("").str.strip() != df["new_district"].fillna("").str.strip())
        | (df["old_location_state"].fillna("").str.strip() != df["new_location_state"].fillna("").str.strip())
    ]

    hire_diff = df[
        df["old_hire_date"].fillna("").str.strip()
        != df["new_hire_date"].fillna("").str.strip()
    ]

    lines = [
        f"# Reconciliation Summary Report",
        f"",
        f"Generated: {run_ts}  ",
        f"Source: `{DB_PATH}`",
        f"",
        f"---",
        f"",
        f"## Overview",
        f"",
        f"| Metric | Value |",
        f"| --- | --- |",
        f"| Total matched pairs | {total:,} |",
        f"| Salary mismatches | {n_mismatch:,} |",
        f"| Salary increases | {n_increase:,} |",
        f"| Salary decreases | {n_decrease:,} |",
        f"| Status mismatches | {len(status_diff):,} |",
        f"| Job/org mismatches | {len(job_org_diff):,} |",
        f"| Hire date mismatches | {len(hire_diff):,} |",
        f"",
        f"### Match Source Breakdown",
        f"",
    ]

    src_rows = [[r["match_source"], f"{r['count']:,}", f"{r['count']/total*100:.1f}%"]
                for _, r in src_counts.iterrows()]
    lines.append(_md_table(["Match Source", "Count", "Pct"], src_rows))
    lines.append("")

    if "match_source_bar" in charts:
        lines.append(f"![Match Source Bar](charts/match_source_bar.png)")
        lines.append("")

    lines += [
        "---",
        "",
        "## Key Findings",
        "",
        "### Salary Mismatches",
        "",
        f"- **{n_mismatch:,}** of **{total:,}** matched pairs have a salary change",
        f"- {n_increase:,} increases / {n_decrease:,} decreases",
        "",
    ]

    if n_mismatch > 0:
        d = mismatch_rows["salary_delta"].dropna()
        lines += [
            "| Stat | Value |",
            "| --- | --- |",
            f"| Min delta | {d.min():,.2f} |",
            f"| P10 | {d.quantile(0.10):,.2f} |",
            f"| Median | {d.median():,.2f} |",
            f"| Mean | {d.mean():,.2f} |",
            f"| P90 | {d.quantile(0.90):,.2f} |",
            f"| Max delta | {d.max():,.2f} |",
            "",
        ]

        if "salary_delta_hist" in charts:
            lines.append(f"![Salary Delta Histogram](charts/salary_delta_hist.png)")
            lines.append("")
        if "salary_ratio_hist" in charts:
            lines.append(f"![Salary Ratio Histogram](charts/salary_ratio_hist.png)")
            lines.append("")

    lines += [
        "### Status Mismatches",
        "",
        f"- **{len(status_diff):,}** pairs have a worker status change",
        "",
    ]

    if len(status_diff) > 0:
        ex = status_diff[
            ["old_worker_id", "old_full_name_norm", "old_worker_status", "new_worker_status"]
        ].head(10)
        ex_rows = [[
            str(r["old_worker_id"]),
            str(r["old_full_name_norm"]),
            str(r["old_worker_status"]),
            str(r["new_worker_status"]),
        ] for _, r in ex.iterrows()]
        lines.append(_md_table(["old_worker_id", "name", "old_status", "new_status"], ex_rows))
        lines.append("")

    lines += [
        "### Job / Org Mismatches",
        "",
        f"- **{len(job_org_diff):,}** pairs have a position, district, or location change",
        "",
    ]

    if len(job_org_diff) > 0:
        ex = job_org_diff[
            ["old_worker_id", "old_full_name_norm", "old_position", "new_position",
             "old_district", "new_district", "old_location_state", "new_location_state"]
        ].head(10)
        ex_rows = [[
            str(r["old_worker_id"]),
            str(r["old_full_name_norm"]),
            str(r["old_position"]),
            str(r["new_position"]),
            str(r["old_district"]),
            str(r["new_district"]),
            str(r["old_location_state"]),
            str(r["new_location_state"]),
        ] for _, r in ex.iterrows()]
        lines.append(_md_table(
            ["old_worker_id", "name", "old_pos", "new_pos", "old_dist", "new_dist", "old_state", "new_state"],
            ex_rows,
        ))
        lines.append("")

    lines += [
        "### Hire Date Mismatches",
        "",
        f"- **{len(hire_diff):,}** pairs have a hire date change",
        "",
    ]

    if len(hire_diff) > 0:
        ex = hire_diff[
            ["old_worker_id", "old_full_name_norm", "old_hire_date", "new_hire_date"]
        ].head(10)
        ex_rows = [[
            str(r["old_worker_id"]),
            str(r["old_full_name_norm"]),
            str(r["old_hire_date"]),
            str(r["new_hire_date"]),
        ] for _, r in ex.iterrows()]
        lines.append(_md_table(["old_worker_id", "name", "old_hire_date", "new_hire_date"], ex_rows))
        lines.append("")

    lines += [
        "---",
        "",
        "## Review Queue",
        "",
        f"See `review_queue.csv` for the full prioritized list ({len(queue):,} rows).",
        "",
        "Priority scoring:",
        "- +50 if match_source is not `worker_id`",
        "- +40 if worker status changed",
        "- +30 if |salary delta| >= 5,000",
        "- +20 if |salary delta| is 1,000-4,999",
        "- +15 if hire date changed",
        "- +10 each for position / district / location_state change",
        "",
    ]

    high_pri = queue[queue["priority_score"] > 0].head(10)
    if len(high_pri) > 0:
        ex_rows = [[
            str(r["pair_id"]),
            str(r["match_source"]),
            str(r["old_worker_id"]),
            str(r["old_full_name_norm"]),
            str(r["priority_score"]),
            str(r["priority_reason"]),
        ] for _, r in high_pri.iterrows()]
        lines.append(_md_table(
            ["pair_id", "match_source", "old_worker_id", "name", "score", "reasons"],
            ex_rows,
        ))
        lines.append("")

    return "\n".join(lines)


def _build_html(md_content: str, run_ts: str) -> str:
    # Convert minimal markdown → HTML (tables, headings, bullets, code)
    import re

    lines = md_content.split("\n")
    html_lines = []
    in_table = False

    for line in lines:
        # Headings
        m = re.match(r"^(#{1,4})\s+(.*)", line)
        if m:
            if in_table:
                html_lines.append("</table>")
                in_table = False
            lvl = len(m.group(1))
            html_lines.append(f"<h{lvl}>{m.group(2)}</h{lvl}>")
            continue

        # HR
        if re.match(r"^---+$", line.strip()):
            if in_table:
                html_lines.append("</table>")
                in_table = False
            html_lines.append("<hr>")
            continue

        # Image
        img = re.match(r"^!\[([^\]]*)\]\(([^)]+)\)$", line)
        if img:
            if in_table:
                html_lines.append("</table>")
                in_table = False
            html_lines.append(f'<img src="{img.group(2)}" alt="{img.group(1)}" style="max-width:100%">')
            continue

        # Table row
        if line.startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if all(re.match(r"^-+$", c) for c in cells):
                # separator row - already handled by opening table
                continue
            if not in_table:
                html_lines.append('<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;width:100%">')
                in_table = True
                tag = "th"
            else:
                tag = "td"
            html_lines.append("<tr>" + "".join(f"<{tag}>{c}</{tag}>" for c in cells) + "</tr>")
            continue

        # End table
        if in_table:
            html_lines.append("</table>")
            in_table = False

        # Bullet
        m_bullet = re.match(r"^[-*]\s+(.*)", line)
        if m_bullet:
            content = m_bullet.group(1)
            content = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", content)
            content = re.sub(r"`([^`]+)`", r"<code>\1</code>", content)
            html_lines.append(f"<li>{content}</li>")
            continue

        # Blank line
        if not line.strip():
            html_lines.append("<br>")
            continue

        # Regular paragraph
        content = line
        content = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", content)
        content = re.sub(r"`([^`]+)`", r"<code>\1</code>", content)
        html_lines.append(f"<p>{content}</p>")

    if in_table:
        html_lines.append("</table>")

    body = "\n".join(html_lines)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Reconciliation Summary Report</title>
<style>
  body {{ font-family: Arial, sans-serif; margin: 2rem; color: #222; }}
  h1 {{ border-bottom: 2px solid #444; padding-bottom: 0.3em; }}
  h2 {{ border-bottom: 1px solid #ccc; padding-bottom: 0.2em; margin-top: 2em; }}
  h3 {{ margin-top: 1.5em; }}
  table {{ border-collapse: collapse; width: 100%; margin: 1em 0; }}
  th, td {{ border: 1px solid #bbb; padding: 4px 8px; text-align: left; font-size: 0.85em; }}
  th {{ background: #f0f0f0; font-weight: bold; }}
  tr:nth-child(even) {{ background: #fafafa; }}
  code {{ background: #eee; padding: 0 3px; border-radius: 3px; }}
  li {{ margin: 0.2em 0; }}
  img {{ max-width: 100%; margin: 1em 0; border: 1px solid #ddd; }}
  hr {{ border: none; border-top: 1px solid #ccc; margin: 1.5em 0; }}
  p {{ margin: 0.4em 0; }}
</style>
</head>
<body>
{body}
<hr>
<p style="color:#888;font-size:0.8em">Generated {run_ts} by audit/summary/build_report.py</p>
</body>
</html>
"""


def main() -> None:
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print("[build_report] loading data from audit.db ...")
    mp = _load_data()
    _check_columns(mp)

    total = len(mp)
    print(f"[build_report] {total:,} matched pairs loaded.")

    df = _build_salary_features(mp)

    print("[build_report] generating charts ...")
    charts = _make_charts(df)
    for name, path in charts.items():
        print(f"  wrote chart: {path.relative_to(ROOT)}")

    print("[build_report] building review queue ...")
    queue = _build_review_queue(df)
    queue_path = SUMMARY_DIR / "review_queue.csv"
    safe_to_csv(queue, str(queue_path))
    print(f"  wrote: {queue_path.relative_to(ROOT)}  ({len(queue):,} rows)")

    print("[build_report] writing markdown report ...")
    md = _build_markdown(mp, df, queue, charts, run_ts)
    md_path = SUMMARY_DIR / "recon_summary.md"
    md_path.write_text(md, encoding="utf-8")
    print(f"  wrote: {md_path.relative_to(ROOT)}")

    print("[build_report] writing HTML report ...")
    html = _build_html(md, run_ts)
    html_path = SUMMARY_DIR / "recon_summary.html"
    html_path.write_text(html, encoding="utf-8")
    print(f"  wrote: {html_path.relative_to(ROOT)}")

    print(f"\n[build_report] done - outputs in audit/summary/")


if __name__ == "__main__":
    main()
