"""Load Stack Overflow survey CSVs into Snowflake raw table."""
import os
import csv
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# Column mapping: SO CSV column name -> our raw table column name
# Maps varying SO column names across years to our standardized schema
COLUMN_MAP = {
    "ResponseId": "response_id",
    "MainBranch": "main_branch",
    "Employment": "employment",
    "RemoteWork": "remote_work",
    "Country": "country",
    "US_State": "us_state",
    "EdLevel": "ed_level",
    "Age": "age",
    "Gender": "gender",
    "Trans": "trans",
    "Sexuality": "sexuality",
    "Ethnicity": "ethnicity",
    "Accessibility": "accessibility",
    "YearsCode": "years_code",
    "YearsCodePro": "years_code_pro",
    "DevType": "dev_type",
    "OrgSize": "org_size",
    "Currency": "currency",
    "CompTotal": "comp_total",
    "CompFreq": "comp_freq",
    "LanguageHaveWorkedWith": "language_have_worked_with",
    "LanguageWantToWorkWith": "language_want_to_work_with",
    "DatabaseHaveWorkedWith": "database_have_worked_with",
    "DatabaseWantToWorkWith": "database_want_to_work_with",
    "PlatformHaveWorkedWith": "platform_have_worked_with",
    "PlatformWantToWorkWith": "platform_want_to_work_with",
    "WebframeHaveWorkedWith": "webframe_have_worked_with",
    "WebframeWantToWorkWith": "webframe_want_to_work_with",
    "MiscTechHaveWorkedWith": "misc_tech_have_worked_with",
    "MiscTechWantToWorkWith": "misc_tech_want_to_work_with",
    "ToolsTechHaveWorkedWith": "tools_tech_have_worked_with",
    "ToolsTechWantToWorkWith": "tools_tech_want_to_work_with",
    "NEWCollabToolsHaveWorkedWith": "collab_tools_have_worked_with",
    "NEWCollabToolsWantToWorkWith": "collab_tools_want_to_work_with",
    "OpSysProfessional use": "op_sys_professional",
    "OpSysPersonal use": "op_sys_personal",
    "AISelect": "ai_select",
    "AISent": "ai_sent",
    "AIToolCurrently Using": "ai_tools_currently_using",
    "AIToolInterested in": "ai_tools_want_to_work_with",
    "AIToolNot interested in": "ai_tools_want_to_work_with",
    "JobSat": "job_sat",
    # 2024+ columns
    "OfficeStackSyncHaveWorkedWith": "collab_tools_have_worked_with",
    "OfficeStackSyncWantToWorkWith": "collab_tools_want_to_work_with",
    "OfficeStackAsyncHaveWorkedWith": "collab_tools_have_worked_with",
    "OfficeStackAsyncWantToWorkWith": "collab_tools_want_to_work_with",
    "NEWSOSites": "ne_tech_have_worked_with",
    # 2025 specific
    "WorkExp": "years_code_pro",
    "AIToolHaveWorkedWith": "ai_tools_currently_using",
    "AIToolWantToWorkWith": "ai_tools_want_to_work_with",
}

BATCH_SIZE = 5000
csv.field_size_limit(10_000_000)


def load_year(cursor, filepath, year):
    print(f"\n{'='*60}")
    print(f"Loading {year} from {filepath}")

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        csv_columns = reader.fieldnames
        print(f"  CSV has {len(csv_columns)} columns")

        # Find mappable columns (deduplicate target columns)
        mapped = {}
        seen_targets = set()
        for csv_col in csv_columns:
            clean = csv_col.strip().strip('"')
            if clean in COLUMN_MAP:
                target = COLUMN_MAP[clean]
                if target not in seen_targets:
                    mapped[clean] = target
                    seen_targets.add(target)

        print(f"  Mapped {len(mapped)} columns to raw table")

        # Build INSERT
        target_cols = ["survey_year"] + list(mapped.values())
        placeholders = ", ".join(["%s"] * len(target_cols))
        col_names = ", ".join(target_cols)
        insert_sql = f"INSERT INTO raw_stackoverflow_survey ({col_names}) VALUES ({placeholders})"

        batch = []
        total = 0
        for row in reader:
            values = [year]
            for csv_col, _ in mapped.items():
                val = row.get(csv_col, "").strip().strip('"')
                values.append(val if val else None)
            batch.append(values)

            if len(batch) >= BATCH_SIZE:
                cursor.executemany(insert_sql, batch)
                total += len(batch)
                print(f"  Inserted {total} rows...", end="\r")
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

    # Truncate before reload
    cursor.execute("TRUNCATE TABLE IF EXISTS raw_stackoverflow_survey")
    print("Truncated raw_stackoverflow_survey")

    data_dir = os.path.join(os.path.dirname(__file__), "..", "data", "stackoverflow")
    grand_total = 0

    for year in [2021, 2022, 2023, 2024, 2025]:
        filepath = os.path.join(data_dir, f"survey_{year}.csv")
        if os.path.exists(filepath):
            grand_total += load_year(cursor, filepath, year)
        else:
            print(f"SKIP: {filepath} not found")

    conn.commit()

    # Verify
    cursor.execute("SELECT survey_year, COUNT(*) FROM raw_stackoverflow_survey GROUP BY 1 ORDER BY 1")
    print(f"\n{'='*60}")
    print("Final row counts:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,} rows")
    print(f"  TOTAL: {grand_total:,} rows")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
