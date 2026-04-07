#!/usr/bin/env python3
"""
Tech Ecosystem Tracker — Automated Data Quality Report

Runs dbt tests, captures results, writes them to Snowflake (dbt_test_results table),
and prints a human-readable summary.

Usage:
    python monitoring/quality_report.py
    python monitoring/quality_report.py --models fct_technology_adoption fct_said_vs_did
    python monitoring/quality_report.py --no-upload   # skip Snowflake upload
    python monitoring/quality_report.py --html        # also write report.html
"""

import os
import re
import sys
import json
import uuid
import argparse
import subprocess
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

ACCOUNT  = os.environ.get("SNOWFLAKE_ACCOUNT", "")
USER     = os.environ.get("SNOWFLAKE_USER", "")
PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "")


# ── Run dbt tests ─────────────────────────────────────────────────────────────

def run_dbt_tests(models: list[str] | None = None) -> dict:
    """Run dbt test and return parsed results."""
    cmd = ["dbt", "test", "--profiles-dir", str(ROOT), "--project-dir", str(ROOT)]
    if models:
        cmd += ["--select", " ".join(models)]

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    return {
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode,
    }


def parse_dbt_output(output: str) -> list[dict]:
    """Parse dbt test output into structured records."""
    records = []
    run_at = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid.uuid4())

    # Match lines like:
    # PASS not_null_fct_technology_adoption_technology_name ........................ [PASS in 0.85s]
    # FAIL unique_combination_of_columns_fct_said_vs_did .......................... [FAIL 3 in 1.2s]
    pattern = re.compile(
        r"(PASS|FAIL|WARN)\s+([\w\.]+)\s+\.+\s+\[(PASS|FAIL|WARN)"
        r"(?:\s+(\d+))?"
        r"\s+in\s+([\d\.]+)s\]"
    )

    for line in output.splitlines():
        m = pattern.search(line)
        if not m:
            continue

        status_raw, full_test_name, _, failure_count_str, exec_time = m.groups()
        status = status_raw.lower()
        failure_count = int(failure_count_str) if failure_count_str else 0
        exec_ms = int(float(exec_time) * 1000)

        # Extract model name from test name (e.g. not_null_fct_adoption_col → fct_adoption)
        model_name = _extract_model_name(full_test_name)
        column_name = _extract_column_name(full_test_name)
        test_type = _extract_test_type(full_test_name)

        records.append({
            "run_id": run_id,
            "run_at": run_at,
            "model_name": model_name,
            "test_name": full_test_name,
            "test_type": test_type,
            "column_name": column_name,
            "status": status,
            "failure_count": failure_count,
            "severity": "error",
            "execution_ms": exec_ms,
        })

    return records


def _extract_model_name(test_name: str) -> str:
    """Best-effort extract of the model name from a dbt test identifier."""
    for prefix in ["not_null_", "unique_", "accepted_values_", "accepted_range_",
                   "relationships_", "unique_combination_of_columns_",
                   "dbt_utils_", "technology_name_standardization_",
                   "survey_response_ranges_", "github_activity_validity_"]:
        if test_name.startswith(prefix):
            remainder = test_name[len(prefix):]
            # Model names start with fct_, dim_, stg_, int_
            for marker in ["fct_", "dim_", "stg_", "int_"]:
                if marker in remainder:
                    idx = remainder.index(marker)
                    model_part = remainder[idx:]
                    # Cut off at first double-underscore or trailing column name
                    parts = model_part.split("_")
                    return "_".join(parts[:3]) if len(parts) >= 3 else model_part
    # Fallback: return first 3 underscore-joined parts
    return "_".join(test_name.split("_")[:3])


def _extract_column_name(test_name: str) -> str | None:
    """Extract column name from test identifier if present."""
    parts = test_name.split("_")
    if len(parts) > 4:
        return parts[-1]
    return None


def _extract_test_type(test_name: str) -> str:
    """Extract test type from test identifier."""
    for t in ["not_null", "unique", "accepted_values", "accepted_range",
              "relationships", "unique_combination_of_columns",
              "technology_name_standardization", "survey_response_ranges",
              "github_activity_validity"]:
        if test_name.startswith(t):
            return t
    return "custom"


# ── Upload to Snowflake ───────────────────────────────────────────────────────

def upload_results(records: list[dict]) -> bool:
    """Insert test results into TECH_ECOSYSTEM.PUBLIC.dbt_test_results."""
    if not records:
        print("No records to upload.")
        return True

    try:
        import snowflake.connector
    except ImportError:
        print("snowflake-connector-python not installed — skipping upload.")
        return False

    try:
        conn = snowflake.connector.connect(
            account=ACCOUNT, user=USER, password=PASSWORD,
            database="TECH_ECOSYSTEM", schema="PUBLIC", warehouse="TECH_WH",
        )
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT INTO dbt_test_results
                (run_id, run_at, model_name, test_name, column_name,
                 status, failure_count, severity, execution_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    r["run_id"], r["run_at"], r["model_name"], r["test_name"],
                    r.get("column_name"), r["status"], r["failure_count"],
                    r["severity"], r["execution_ms"],
                )
                for r in records
            ],
        )
        conn.commit()
        conn.close()
        print(f"Uploaded {len(records)} test results to Snowflake.")
        return True
    except Exception as e:
        print(f"Upload failed: {e}")
        return False


# ── Report generation ─────────────────────────────────────────────────────────

def print_summary(records: list[dict], dbt_output: dict):
    total = len(records)
    passed = sum(1 for r in records if r["status"] == "pass")
    failed = sum(1 for r in records if r["status"] == "fail")
    warned = sum(1 for r in records if r["status"] == "warn")
    pass_rate = passed / total * 100 if total else 0

    border = "═" * 62
    print(f"\n╔{border}╗")
    print(f"║  Data Quality Report — {datetime.now().strftime('%Y-%m-%d %H:%M')}{'':>22}║")
    print(f"╠{border}╣")
    print(f"║  Total tests:   {total:<10} Pass rate: {pass_rate:>5.1f}%{'':>17}║")
    print(f"║  Passed:        {passed:<10} Failed:    {failed:<10} Warned: {warned:<6}║")
    print(f"╠{border}╣")

    # Per-model summary
    models = {}
    for r in records:
        m = r["model_name"]
        if m not in models:
            models[m] = {"pass": 0, "fail": 0, "warn": 0}
        models[m][r["status"]] += 1

    for model, counts in sorted(models.items()):
        total_m = sum(counts.values())
        rate_m = counts["pass"] / total_m * 100 if total_m else 0
        icon = "✓" if counts["fail"] == 0 else "✗"
        print(f"║  {icon} {model:<30} {rate_m:>5.1f}%  ({counts['fail']} failures){'':>3}║")

    print(f"╠{border}╣")
    overall = "HEALTHY (≥95%)" if pass_rate >= 95 else ("DEGRADED (≥80%)" if pass_rate >= 80 else "CRITICAL (<80%)")
    print(f"║  Overall: {overall:<52}║")
    print(f"╚{border}╝\n")

    if failed > 0:
        print("Failures:")
        for r in records:
            if r["status"] == "fail":
                print(f"  FAIL  {r['model_name']}.{r['test_name']}  ({r['failure_count']} rows)")


def write_html_report(records: list[dict], output_path: Path):
    total = len(records)
    passed = sum(1 for r in records if r["status"] == "pass")
    pass_rate = passed / total * 100 if total else 0

    rows_html = "".join(
        f"<tr class='{r['status']}'>"
        f"<td>{r['model_name']}</td>"
        f"<td>{r['test_name']}</td>"
        f"<td>{r.get('column_name', '')}</td>"
        f"<td class='status-{r['status']}'>{r['status'].upper()}</td>"
        f"<td>{r['failure_count']}</td>"
        f"<td>{r['execution_ms']}ms</td>"
        f"</tr>"
        for r in records
    )

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Data Quality Report</title>
<style>
  body {{ font-family: system-ui; background:#0e1117; color:#fafafa; padding:2rem; }}
  h1 {{ color:#00d4ff; }} table {{ width:100%; border-collapse:collapse; }}
  th {{ background:#161c27; padding:.5rem; text-align:left; }}
  td {{ padding:.4rem .5rem; border-bottom:1px solid #2a3347; }}
  .status-pass {{ color:#4ecdc4; font-weight:600; }}
  .status-fail {{ color:#ff6b6b; font-weight:600; }}
  .status-warn {{ color:#ffd93d; font-weight:600; }}
  .summary {{ background:#161c27; border:1px solid #2a3347; border-radius:8px;
              padding:1rem; margin-bottom:1.5rem; display:flex; gap:2rem; }}
</style>
</head><body>
<h1>Data Quality Report</h1>
<p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
<div class="summary">
  <span>Total: <strong>{total}</strong></span>
  <span>Passed: <strong style="color:#4ecdc4">{passed}</strong></span>
  <span>Failed: <strong style="color:#ff6b6b">{total - passed}</strong></span>
  <span>Pass rate: <strong>{pass_rate:.1f}%</strong></span>
</div>
<table>
<tr><th>Model</th><th>Test</th><th>Column</th><th>Status</th><th>Failures</th><th>Time</th></tr>
{rows_html}
</table>
</body></html>"""

    output_path.write_text(html)
    print(f"HTML report written to {output_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Run dbt tests and generate quality report")
    parser.add_argument("--models", nargs="*", help="Specific models to test")
    parser.add_argument("--no-upload", action="store_true", help="Skip Snowflake upload")
    parser.add_argument("--html", action="store_true", help="Write HTML report")
    args = parser.parse_args()

    # 1. Run dbt tests
    dbt_result = run_dbt_tests(args.models)

    # 2. Parse output
    records = parse_dbt_output(dbt_result["stdout"])
    if not records:
        print("No test results parsed. Raw output:")
        print(dbt_result["stdout"][-2000:])
        sys.exit(1 if dbt_result["returncode"] != 0 else 0)

    # 3. Upload to Snowflake
    if not args.no_upload:
        upload_results(records)

    # 4. Print summary
    print_summary(records, dbt_result)

    # 5. Optional HTML
    if args.html:
        write_html_report(records, ROOT / "monitoring" / "quality_report.html")

    # Exit 1 if any failures
    if any(r["status"] == "fail" for r in records):
        sys.exit(1)


if __name__ == "__main__":
    main()
