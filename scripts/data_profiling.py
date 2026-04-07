"""
Data profiling script for TECH_ECOSYSTEM raw tables.

Covers:
  - NULL rates
  - Duplicate detection
  - Value distributions
  - Row counts per year
  - Visualizations (saved to profiling_output/ directory)

Usage:
  python scripts/data_profiling.py [--table so|jb|gh|all]  (default: all)
"""

import os
import sys
import argparse
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # headless — saves to files instead of opening windows
import matplotlib.pyplot as plt
import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = Path(__file__).parent.parent / "profiling_output"
OUTPUT_DIR.mkdir(exist_ok=True)


# ── Connection ────────────────────────────────────────────────────────────────

def get_conn():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="TECH_WH",
        database="TECH_ECOSYSTEM",
        schema="RAW",
    )


def run_query(conn, sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, conn)


# ── Generic profiling helpers ─────────────────────────────────────────────────

def null_report(df: pd.DataFrame, table_label: str) -> pd.DataFrame:
    report = pd.DataFrame({
        "column": df.columns,
        "total_rows": len(df),
        "null_count": df.isnull().sum().values,
        "null_pct": (df.isnull().sum() / len(df) * 100).round(2).values,
    }).sort_values("null_pct", ascending=False).reset_index(drop=True)
    print(f"\n{'='*60}")
    print(f"NULL ANALYSIS — {table_label}  ({len(df):,} rows)")
    print(f"{'='*60}")
    print(report.to_string(index=False))
    return report


def duplicate_report(df: pd.DataFrame, key_cols: list[str], table_label: str) -> pd.DataFrame:
    existing_keys = [c for c in key_cols if c in df.columns]
    dups = df[df.duplicated(subset=existing_keys, keep=False)]
    print(f"\nDUPLICATES — {table_label}")
    print(f"  Key columns : {existing_keys}")
    print(f"  Duplicate rows: {len(dups):,}  ({len(dups)/len(df)*100:.2f}%)")
    return dups


def value_dist(df: pd.DataFrame, col: str, top_n: int = 20) -> pd.Series | None:
    if col not in df.columns:
        print(f"  [skip] column '{col}' not in dataframe")
        return None
    vc = df[col].value_counts(dropna=False).head(top_n)
    print(f"\n  Top {top_n} values — {col}:")
    print(vc.to_string())
    return vc


def save_barh(series: pd.Series, title: str, xlabel: str, filename: str):
    fig, ax = plt.subplots(figsize=(10, max(4, len(series) * 0.35)))
    series.sort_values().plot.barh(ax=ax, color="#4C78A8")
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    plt.tight_layout()
    path = OUTPUT_DIR / filename
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"  Saved → {path}")


def save_hist(series: pd.Series, title: str, xlabel: str, filename: str, bins: int = 50):
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        print(f"  [skip] no numeric values in '{series.name}'")
        return
    fig, ax = plt.subplots(figsize=(10, 5))
    numeric.hist(bins=bins, ax=ax, color="#4C78A8", edgecolor="white")
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Respondents")
    plt.tight_layout()
    path = OUTPUT_DIR / filename
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"  Saved → {path}")
    print(f"  Stats:\n{numeric.describe().round(2).to_string()}")


def save_null_chart(null_df: pd.DataFrame, filename: str):
    non_zero = null_df[null_df["null_pct"] > 0].set_index("column")["null_pct"]
    if non_zero.empty:
        print("  No NULLs found — skipping chart.")
        return
    save_barh(non_zero, "NULL % by column", "NULL %", filename)


# ── Per-table profiling ───────────────────────────────────────────────────────

def profile_stackoverflow(conn):
    print("\n" + "#" * 70)
    print("# STACK OVERFLOW SURVEY  (raw_stackoverflow_survey)")
    print("#" * 70)

    df = run_query(conn, "SELECT * FROM TECH_ECOSYSTEM.RAW.raw_stackoverflow_survey")

    # Row counts per year
    print(f"\nRow counts by year:")
    print(df["SURVEY_YEAR"].value_counts().sort_index().to_string())

    # NULLs
    null_df = null_report(df, "raw_stackoverflow_survey")
    save_null_chart(null_df, "so_null_pct.png")

    # Duplicates
    duplicate_report(df, ["RESPONSE_ID", "SURVEY_YEAR"], "raw_stackoverflow_survey")

    # Dev type distribution
    vc = value_dist(df, "DEV_TYPE", top_n=25)
    if vc is not None:
        save_barh(vc, "Stack Overflow — Dev Type distribution", "Respondents", "so_dev_type.png")

    # Compensation histogram (COMP_TOTAL is TEXT → cast)
    save_hist(df["COMP_TOTAL"], "Stack Overflow — Compensation distribution", "Comp Total (raw)", "so_compensation.png")

    # Remote work
    value_dist(df, "REMOTE_WORK", top_n=10)

    # Country top 15
    vc_country = value_dist(df, "COUNTRY", top_n=15)
    if vc_country is not None:
        save_barh(vc_country, "Stack Overflow — Top 15 Countries", "Respondents", "so_countries.png")

    # AI tools adoption
    value_dist(df, "AI_SELECT", top_n=10)

    # Semicolon-list columns: show unique tech counts per year
    list_cols = [
        "LANGUAGE_HAVE_WORKED_WITH",
        "DATABASE_HAVE_WORKED_WITH",
        "WEBFRAME_HAVE_WORKED_WITH",
        "MISC_TECH_HAVE_WORKED_WITH",
    ]
    print("\nSemicolon-list column fill rates by year:")
    for col in list_cols:
        if col in df.columns:
            filled = df.groupby("SURVEY_YEAR")[col].apply(lambda s: s.notna().sum())
            total = df.groupby("SURVEY_YEAR")[col].size()
            pct = (filled / total * 100).round(1)
            print(f"  {col}: {pct.to_dict()}")


def profile_jetbrains(conn):
    print("\n" + "#" * 70)
    print("# JETBRAINS DEVELOPER ECOSYSTEM SURVEY  (raw_jetbrains_survey)")
    print("#" * 70)

    df = run_query(conn, "SELECT * FROM TECH_ECOSYSTEM.RAW.raw_jetbrains_survey")

    print(f"\nRow counts by year:")
    print(df["SURVEY_YEAR"].value_counts().sort_index().to_string())

    null_df = null_report(df, "raw_jetbrains_survey")
    save_null_chart(null_df, "jb_null_pct.png")

    duplicate_report(df, ["RESPONSE_ID", "SURVEY_YEAR"], "raw_jetbrains_survey")

    # Job role
    vc = value_dist(df, "JOB_ROLE", top_n=20)
    if vc is not None:
        save_barh(vc, "JetBrains — Job Role distribution", "Respondents", "jb_job_role.png")

    # Primary language
    vc_lang = value_dist(df, "PRIMARY_LANGUAGE", top_n=20)
    if vc_lang is not None:
        save_barh(vc_lang, "JetBrains — Primary Language", "Respondents", "jb_primary_language.png")

    value_dist(df, "EMPLOYMENT_STATUS", top_n=10)
    value_dist(df, "COMPANY_SIZE", top_n=10)
    value_dist(df, "YEARS_OF_EXPERIENCE", top_n=10)

    # Country top 15
    vc_country = value_dist(df, "COUNTRY", top_n=15)
    if vc_country is not None:
        save_barh(vc_country, "JetBrains — Top 15 Countries", "Respondents", "jb_countries.png")


def profile_github(conn):
    print("\n" + "#" * 70)
    print("# GITHUB MONTHLY ACTIVITY  (raw_github_monthly_activity)")
    print("#" * 70)

    df = run_query(conn, "SELECT * FROM TECH_ECOSYSTEM.RAW.raw_github_monthly_activity")

    print(f"\nTotal rows: {len(df):,}")
    print(f"Date range: {df['ACTIVITY_MONTH'].min()} → {df['ACTIVITY_MONTH'].max()}")
    print(f"Unique languages: {df['REPO_LANGUAGE'].nunique()}")
    print(f"Unique event types: {df['EVENT_TYPE'].nunique()}")

    null_df = null_report(df, "raw_github_monthly_activity")
    save_null_chart(null_df, "gh_null_pct.png")

    duplicate_report(df, ["ACTIVITY_MONTH", "REPO_LANGUAGE", "EVENT_TYPE"], "raw_github_monthly_activity")

    # Event type distribution
    vc = value_dist(df, "EVENT_TYPE", top_n=20)
    if vc is not None:
        save_barh(vc, "GitHub — Event Type distribution", "Row count", "gh_event_type.png")

    # Top languages by event count
    top_langs = (
        df.groupby("REPO_LANGUAGE")["EVENT_COUNT"]
        .sum()
        .sort_values(ascending=False)
        .head(20)
    )
    print(f"\n  Top 20 languages by total event count:")
    print(top_langs.to_string())
    save_barh(top_langs, "GitHub — Top 20 Languages by Event Count", "Total Events", "gh_top_languages.png")

    # Monthly event trend (all languages combined)
    monthly = df.groupby("ACTIVITY_MONTH")["EVENT_COUNT"].sum().sort_index()
    fig, ax = plt.subplots(figsize=(12, 5))
    monthly.plot(ax=ax, color="#4C78A8", linewidth=1.5)
    ax.set_title("GitHub — Monthly Event Count (all languages)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Events")
    plt.tight_layout()
    path = OUTPUT_DIR / "gh_monthly_trend.png"
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"  Saved → {path}")

    # Numeric stats
    print("\nNumeric column stats:")
    numeric_cols = ["EVENT_COUNT", "UNIQUE_ACTORS", "UNIQUE_REPOS", "TOTAL_COMMITS", "AVG_REPO_STARS"]
    print(df[numeric_cols].describe().round(2).to_string())


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Profile TECH_ECOSYSTEM raw tables")
    parser.add_argument(
        "--table",
        choices=["so", "jb", "gh", "all"],
        default="all",
        help="Which table to profile (default: all)",
    )
    args = parser.parse_args()

    conn = get_conn()
    try:
        if args.table in ("so", "all"):
            profile_stackoverflow(conn)
        if args.table in ("jb", "all"):
            profile_jetbrains(conn)
        if args.table in ("gh", "all"):
            profile_github(conn)
    finally:
        conn.close()

    print(f"\nAll charts saved to: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
