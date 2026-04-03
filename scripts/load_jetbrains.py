"""
Load JetBrains Developer Ecosystem Survey data into Snowflake.

JetBrains uses wide-format CSVs with one column per answer option
(e.g., proglang::Python = "Python" if selected, empty if not).
This script pivots the wide format into our normalized raw table.
"""

import os
import csv
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
csv.field_size_limit(10_000_000)

BATCH_SIZE = 5000

# Files per year
YEAR_FILES = {
    2023: "data/jetbrains/survey_2023/2023_sharing_data_outside.csv",
    2024: "data/jetbrains/survey_2024/2024_sharing_data_outside.csv",
    2025: "data/jetbrains/survey_2025/developer_ecosystem_2025_external.csv",
}

# Column prefixes vary by year
PROGLANG_PREFIX = {2023: "proglang::", 2024: "proglang::", 2025: "proglang::"}
PRIMARY_LANG_PREFIX = {2023: "primary_proglang::", 2024: "primary_lang::", 2025: "primary_lang::"}
ADOPT_PREFIX = {2023: "adopt_proglang::", 2024: "adopt_proglang::", 2025: "adopt_proglang::"}


def extract_multi_select(row, prefix):
    """Extract selected values from wide-format multi-select columns."""
    values = []
    for col, val in row.items():
        if col.startswith(prefix) and val and val.strip():
            # The value is the language name (e.g., "Python")
            values.append(val.strip())
    return values


def load_year(cursor, filepath, year):
    print(f"\n{'='*60}")
    print(f"Loading JetBrains {year} from {filepath}")

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        print(f"  CSV has {len(headers)} columns")

        # Count relevant columns
        proglang_cols = [h for h in headers if h.startswith(PROGLANG_PREFIX[year])]
        primary_cols = [h for h in headers if h.startswith(PRIMARY_LANG_PREFIX[year])]
        adopt_cols = [h for h in headers if h.startswith(ADOPT_PREFIX[year])]
        print(f"  proglang: {len(proglang_cols)}, primary: {len(primary_cols)}, adopt: {len(adopt_cols)}")

        # Check for IDE and framework columns
        ide_cols = [h for h in headers if h.startswith("ide::") or h.startswith("ide_primary::")]
        os_cols = [h for h in headers if h.startswith("os::") or h.startswith("os_dev::")]

        insert_sql = """INSERT INTO raw_jetbrains_survey
            (response_id, survey_year, country, age, gender,
             employment_status, company_size, job_role,
             years_of_experience, primary_language, languages_used,
             languages_plan_to_adopt, frameworks_used, ides_used,
             operating_systems, industry, team_size)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        batch = []
        total = 0

        for row in reader:
            # Extract languages used (semicolon-joined)
            langs_used = extract_multi_select(row, PROGLANG_PREFIX[year])
            primary_langs = extract_multi_select(row, PRIMARY_LANG_PREFIX[year])
            adopt_langs = extract_multi_select(row, ADOPT_PREFIX[year])

            # Extract IDEs
            ides = extract_multi_select(row, "ide::")
            # Extract OS
            operating_systems = extract_multi_select(row, "os::")
            if not operating_systems:
                operating_systems = extract_multi_select(row, "os_dev::")

            # Extract job roles (multi-select)
            job_roles = extract_multi_select(row, "job_role::")

            # Extract industry/sector
            industries = extract_multi_select(row, "company_sector::")

            primary_lang = primary_langs[0] if primary_langs else None

            values = (
                row.get("response_id", ""),
                year,
                row.get("country", ""),
                row.get("age_range", ""),
                row.get("gender", ""),
                row.get("employment_status", ""),
                row.get("company_size", ""),
                ";".join(job_roles) if job_roles else None,
                row.get("dev_experience", row.get("experience", "")),
                primary_lang,
                ";".join(langs_used) if langs_used else None,
                ";".join(adopt_langs) if adopt_langs else None,
                None,  # frameworks_used - not cleanly available in wide format
                ";".join(ides) if ides else None,
                ";".join(operating_systems) if operating_systems else None,
                ";".join(industries) if industries else None,
                row.get("team_size", row.get("company_coders", "")),
            )
            batch.append(values)

            if len(batch) >= BATCH_SIZE:
                cursor.executemany(insert_sql, batch)
                total += len(batch)
                print(f"  Inserted {total} rows...", end="\r", flush=True)
                batch = []

        if batch:
            cursor.executemany(insert_sql, batch)
            total += len(batch)

        print(f"  Inserted {total} rows for {year}         ")
        return total


def main():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="TECH_ECOSYSTEM",
        schema="RAW",
        warehouse="TECH_WH",
    )
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE IF EXISTS raw_jetbrains_survey")
    print("Truncated raw_jetbrains_survey")

    grand_total = 0
    for year, filepath in YEAR_FILES.items():
        full_path = os.path.join(os.path.dirname(__file__), "..", filepath)
        if os.path.exists(full_path):
            grand_total += load_year(cursor, full_path, year)
        else:
            print(f"SKIP: {full_path} not found")

    conn.commit()

    # Verify
    cursor.execute(
        "SELECT survey_year, COUNT(*) FROM raw_jetbrains_survey GROUP BY 1 ORDER BY 1"
    )
    print(f"\n{'='*60}")
    print("Final row counts:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,} rows")
    print(f"  TOTAL: {grand_total:,} rows")

    # Show top languages
    cursor.execute("""
        SELECT primary_language, COUNT(*) as cnt
        FROM raw_jetbrains_survey
        WHERE primary_language IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """)
    print("\nTop 10 primary languages:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
