#!/usr/bin/env python3
"""
Rollback Tech Ecosystem Tracker Streamlit deployment from Snowflake.

Usage:
    python scripts/rollback_streamlit.py
    python scripts/rollback_streamlit.py --dry-run
    python scripts/rollback_streamlit.py --full   # also drop integrations/secrets
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

ACCOUNT  = os.environ["SNOWFLAKE_ACCOUNT"]
USER     = os.environ["SNOWFLAKE_USER"]
PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]


def run_snowsql(sql: str, dry_run: bool = False) -> bool:
    if dry_run:
        print(f"[DRY RUN] Would execute:\n{sql}\n")
        return True
    cmd = [
        "snowsql", "-a", ACCOUNT, "-u", USER, "--password", PASSWORD,
        "-q", sql, "-o", "friendly=false", "-o", "timing=false",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"ERROR:\n{result.stderr}")
        return False
    print(result.stdout)
    return True


def confirm(prompt: str) -> bool:
    answer = input(f"{prompt} [y/N] ").strip().lower()
    return answer == "y"


def main():
    parser = argparse.ArgumentParser(description="Rollback Streamlit deployment from Snowflake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--full", action="store_true",
                        help="Also drop network rule, integration, and secret")
    args = parser.parse_args()

    if not args.dry_run:
        if not confirm("This will remove the Streamlit app from Snowflake. Continue?"):
            print("Aborted.")
            sys.exit(0)

    print("\n── Step 1: Drop Streamlit app ────────────────────────────────────────")
    run_snowsql("""
        USE ROLE SYSADMIN;
        USE DATABASE TECH_ECOSYSTEM;
        USE SCHEMA PUBLIC;
        DROP STREAMLIT IF EXISTS TECH_ECOSYSTEM_TRACKER;
    """, args.dry_run)

    print("\n── Step 2: Remove files from stage ──────────────────────────────────")
    run_snowsql("""
        USE ROLE SYSADMIN;
        USE DATABASE TECH_ECOSYSTEM;
        USE SCHEMA PUBLIC;
        REMOVE @STREAMLIT_STAGE/streamlit_app.py;
        REMOVE @STREAMLIT_STAGE/environment.yml;
    """, args.dry_run)

    if args.full:
        print("\n── Step 3 (--full): Drop integration, secret, network rule ──────────")
        if args.dry_run or confirm("Drop external access integration, secret, and network rule?"):
            run_snowsql("""
                USE ROLE ACCOUNTADMIN;
                DROP EXTERNAL ACCESS INTEGRATION IF EXISTS anthropic_access_integration;
                DROP SECRET IF EXISTS TECH_ECOSYSTEM.PUBLIC.ANTHROPIC_SECRET;
                DROP NETWORK RULE IF EXISTS TECH_ECOSYSTEM.PUBLIC.anthropic_network_rule;
            """, args.dry_run)

    print("\nRollback complete.")


if __name__ == "__main__":
    main()
