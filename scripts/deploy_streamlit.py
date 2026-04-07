#!/usr/bin/env python3
"""
Deploy Tech Ecosystem Tracker Streamlit app to Snowflake.
Uses snowflake-connector-python — no snowsql CLI required.

Usage:
    python scripts/deploy_streamlit.py
    python scripts/deploy_streamlit.py --dry-run
    python scripts/deploy_streamlit.py --skip-sql   # skip SQL setup, just upload files
"""

import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector

# ── Load env ──────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

ACCOUNT  = os.environ["SNOWFLAKE_ACCOUNT"]
USER     = os.environ["SNOWFLAKE_USER"]
PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]

APP_DIR  = ROOT / "app"
SQL_FILE = ROOT / "snowflake" / "04_streamlit_setup.sql"

APP_FILES = [
    APP_DIR / "streamlit_app.py",
    APP_DIR / "environment.yml",
]


def get_conn(role: str = "SYSADMIN") -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        password=PASSWORD,
        database="TECH_ECOSYSTEM",
        schema="PUBLIC",
        warehouse="TECH_WH",
        role=role,
    )


def run_sql(sql: str, role: str = "SYSADMIN", dry_run: bool = False) -> bool:
    """Execute one or more semicolon-separated SQL statements."""
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    if dry_run:
        print(f"[DRY RUN] Would execute {len(statements)} statement(s) as {role}:")
        for s in statements[:3]:
            print(f"  {s[:120]}…" if len(s) > 120 else f"  {s}")
        if len(statements) > 3:
            print(f"  … and {len(statements) - 3} more")
        return True
    try:
        conn = get_conn(role)
        cur = conn.cursor()
        for stmt in statements:
            cur.execute(stmt)
        conn.close()
        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False



def step_run_setup_sql(dry_run: bool) -> bool:
    print("\n── Step 1: Running 04_streamlit_setup.sql ────────────────────────────")
    sql = SQL_FILE.read_text()
    if not run_sql(sql, role="SYSADMIN", dry_run=dry_run):
        return False
    print("  SQL setup complete.")
    return True


def step_upload_files(dry_run: bool) -> bool:
    print("\n── Step 2: Uploading app files to stage ──────────────────────────────")
    if dry_run:
        for f in APP_FILES:
            print(f"  [DRY RUN] Would upload {f.name}")
        return True

    try:
        conn = get_conn("SYSADMIN")
        cur = conn.cursor()
        for f in APP_FILES:
            if not f.exists():
                print(f"  WARNING: {f} not found — skipping.")
                continue
            print(f"  Uploading {f.name}…", end=" ", flush=True)
            cur.execute(
                f"PUT 'file://{f}' @TECH_ECOSYSTEM.PUBLIC.STREAMLIT_STAGE "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            rows = cur.fetchall()
            status = rows[0][6] if rows else "unknown"
            print(status)
        conn.close()
        return True
    except Exception as e:
        print(f"\n  ERROR uploading files: {e}")
        return False


def step_verify(dry_run: bool) -> bool:
    print("\n── Step 3: Verifying deployment ──────────────────────────────────────")
    if dry_run:
        print("  [DRY RUN] Would verify Streamlit app and stage contents.")
        return True
    try:
        conn = get_conn("SYSADMIN")
        cur = conn.cursor()

        cur.execute("SHOW STREAMLITS LIKE 'TECH_ECOSYSTEM_TRACKER'")
        apps = cur.fetchall()
        if apps:
            print(f"  Streamlit app: {apps[0][1]} ✓")
        else:
            print("  WARNING: Streamlit app not found after deployment.")

        cur.execute("LIST @TECH_ECOSYSTEM.PUBLIC.STREAMLIT_STAGE")
        files = cur.fetchall()
        print(f"  Stage files ({len(files)}):")
        for row in files:
            print(f"    {row[0]}  ({row[1]} bytes)")

        conn.close()
        return bool(apps)
    except Exception as e:
        print(f"  ERROR during verification: {e}")
        return False


def print_success():
    print("""
╔══════════════════════════════════════════════════════════════╗
║  Deployment complete!                                        ║
║                                                              ║
║  Open in Snowsight:                                          ║
║    Projects > Streamlit > TECH_ECOSYSTEM_TRACKER             ║
╚══════════════════════════════════════════════════════════════╝
""")


def main():
    parser = argparse.ArgumentParser(description="Deploy Streamlit app to Snowflake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-sql", action="store_true", help="Skip SQL setup, only upload files")
    args = parser.parse_args()

    ok = True
    if not args.skip_sql:
        ok = step_run_setup_sql(args.dry_run)
    if ok:
        ok = step_upload_files(args.dry_run)
    if ok:
        ok = step_verify(args.dry_run)

    if ok:
        print_success()
    else:
        print("\nDeployment failed. See errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
