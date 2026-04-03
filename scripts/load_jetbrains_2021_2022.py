"""
Load JetBrains 2021 and 2022 survey data into raw_jetbrains_survey.

These years use '.' delimiter (e.g., proglang.Python) instead of '::'
used in 2023-2025. Appends to existing table without truncating.
"""

import os
import csv
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
csv.field_size_limit(10_000_000)

BATCH_SIZE = 5000

YEAR_FILES = {
    2021: "data/jetbrains/survey_2021.csv",
    2022: "data/jetbrains/survey_2022.csv",
}

# 2021/2022 use '.' delimiter
PROGLANG_PREFIX = "proglang."
PRIMARY_LANG_PREFIX = "primary_proglang."
ADOPT_PREFIX = "adopt_proglang."
OS_PREFIX = "os_devenv."
JOB_ROLE_PREFIX = "job_role."
SECTOR_PREFIX = "company_sector."


def extract_multi_select(row, prefix):
    """Extract selected values from wide-format columns using '.' delimiter."""
    values = []
    for col, val in row.items():
        if col.startswith(prefix) and val and val.strip():
            # Skip meta-answers
            if "I don't use" in val or "not planning" in val.lower():
                continue
            # Skip _103 suffix duplicates (2022 artifact)
            if col.endswith("_103"):
                continue
            values.append(val.strip())
    return values


def load_year(cursor, filepath, year):
    print(f"\n{'='*60}")
    print(f"Loading JetBrains {year} from {filepath}")

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        print(f"  CSV has {len(headers)} columns")

        proglang_cols = [h for h in headers if h.startswith(PROGLANG_PREFIX)]
        primary_cols = [h for h in headers if h.startswith(PRIMARY_LANG_PREFIX)]
        adopt_cols = [h for h in headers if h.startswith(ADOPT_PREFIX)]
        print(f"  proglang: {len(proglang_cols)}, primary: {len(primary_cols)}, adopt: {len(adopt_cols)}")

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
            langs_used = extract_multi_select(row, PROGLANG_PREFIX)
            primary_langs = extract_multi_select(row, PRIMARY_LANG_PREFIX)
            adopt_langs = extract_multi_select(row, ADOPT_PREFIX)
            operating_systems = extract_multi_select(row, OS_PREFIX)
            job_roles = extract_multi_select(row, JOB_ROLE_PREFIX)
            industries = extract_multi_select(row, SECTOR_PREFIX)

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
                row.get("dev_experience", ""),
                primary_lang,
                ";".join(langs_used) if langs_used else None,
                ";".join(adopt_langs) if adopt_langs else None,
                None,  # frameworks_used
                None,  # ides_used (2021/2022 don't have clean ide. prefix)
                ";".join(operating_systems) if operating_systems else None,
                ";".join(industries) if industries else None,
                row.get("team_size", ""),
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

    # Delete only 2021/2022 rows (preserve 2023-2025)
    cursor.execute("DELETE FROM raw_jetbrains_survey WHERE survey_year IN (2021, 2022)")
    print("Cleared any existing 2021/2022 rows")

    grand_total = 0
    for year, filepath in YEAR_FILES.items():
        full_path = os.path.join(os.path.dirname(__file__), "..", filepath)
        if os.path.exists(full_path):
            grand_total += load_year(cursor, full_path, year)
        else:
            print(f"SKIP: {full_path} not found")

    conn.commit()

    # Verify all 5 years
    cursor.execute(
        "SELECT survey_year, COUNT(*) FROM raw_jetbrains_survey GROUP BY 1 ORDER BY 1"
    )
    print(f"\n{'='*60}")
    print("All years in raw_jetbrains_survey:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,} rows")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
