#!/usr/bin/env python3
"""
Deploy Tech Ecosystem Tracker Streamlit app to Snowflake.

Usage:
    python scripts/deploy_streamlit.py
    python scripts/deploy_streamlit.py --dry-run
    python scripts/deploy_streamlit.py --skip-sql   # skip SQL setup, just upload files
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# ── Load env ──────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

ACCOUNT  = os.environ["SNOWFLAKE_ACCOUNT"]
USER     = os.environ["SNOWFLAKE_USER"]
PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
API_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")

STAGE    = "@TECH_ECOSYSTEM.PUBLIC.STREAMLIT_STAGE"
APP_DIR  = ROOT / "app"
SQL_FILE = ROOT / "snowflake" / "04_streamlit_setup.sql"

APP_FILES = [
    APP_DIR / "streamlit_app.py",
    APP_DIR / "environment.yml",
]


def run_snowsql(commands: list[str], dry_run: bool = False) -> bool:
    """Execute SnowSQL commands. Returns True on success."""
    script = "\n".join(commands)
    if dry_run:
        print("[DRY RUN] Would execute SnowSQL:\n" + script)
        return True

    cmd = [
        "snowsql",
        "-a", ACCOUNT,
        "-u", USER,
        "--password", PASSWORD,
        "-q", script,
        "-o", "friendly=false",
        "-o", "timing=false",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"ERROR: SnowSQL failed:\n{result.stderr}")
        return False
    print(result.stdout)
    return True


def check_snowsql_installed() -> bool:
    result = subprocess.run(["snowsql", "--version"], capture_output=True)
    return result.returncode == 0


def inject_api_key(sql_path: Path) -> str:
    """Return SQL with <YOUR_KEY> replaced by actual API key."""
    sql = sql_path.read_text()
    if "<YOUR_KEY>" in sql:
        if not API_KEY:
            print("ERROR: ANTHROPIC_API_KEY not set in .env — cannot inject into SQL.")
            sys.exit(1)
        sql = sql.replace("<YOUR_KEY>", API_KEY)
    return sql


def step_run_setup_sql(dry_run: bool) -> bool:
    print("\n── Step 1: Running 04_streamlit_setup.sql ────────────────────────────")
    sql = inject_api_key(SQL_FILE)
    if dry_run:
        print("[DRY RUN] Would execute setup SQL (API key injected).")
        return True
    return run_snowsql([sql], dry_run=False)


def step_upload_files(dry_run: bool) -> bool:
    print("\n── Step 2: Uploading app files to stage ──────────────────────────────")
    for f in APP_FILES:
        if not f.exists():
            print(f"WARNING: {f} not found — skipping.")
            continue
        put_cmd = f"PUT 'file://{f}' {STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
        print(f"  Uploading {f.name}…")
        if not run_snowsql([put_cmd], dry_run=dry_run):
            return False
    return True


def step_verify(dry_run: bool) -> bool:
    print("\n── Step 3: Verifying deployment ──────────────────────────────────────")
    verify_sql = """
        USE DATABASE TECH_ECOSYSTEM;
        USE SCHEMA PUBLIC;
        SHOW STREAMLITS LIKE 'TECH_ECOSYSTEM_TRACKER';
        LIST @STREAMLIT_STAGE;
    """
    return run_snowsql([verify_sql], dry_run=dry_run)


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
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing")
    parser.add_argument("--skip-sql", action="store_true", help="Skip SQL setup, only upload files")
    args = parser.parse_args()

    if not check_snowsql_installed():
        print("ERROR: snowsql not found. Install from https://docs.snowflake.com/en/user-guide/snowsql-install-config")
        sys.exit(1)

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
