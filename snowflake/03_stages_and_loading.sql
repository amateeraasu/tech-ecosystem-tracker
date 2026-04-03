-- =============================================================================
-- Internal Stages and COPY INTO commands for data loading
-- =============================================================================
USE DATABASE TECH_ECOSYSTEM;
USE SCHEMA RAW;
USE WAREHOUSE TECH_WH;

-- Internal stages for file upload
CREATE OR REPLACE STAGE stackoverflow_stage FILE_FORMAT = csv_format;
CREATE OR REPLACE STAGE jetbrains_stage FILE_FORMAT = csv_format;
CREATE OR REPLACE STAGE github_stage FILE_FORMAT = csv_format;

-- =============================================================================
-- Loading workflow (run after uploading files with PUT):
--
-- 1. Upload files:
--    PUT file:///path/to/stackoverflow/survey_2021.csv @stackoverflow_stage/2021;
--    PUT file:///path/to/stackoverflow/survey_2022.csv @stackoverflow_stage/2022;
--    ... etc
--
-- 2. Then run the COPY commands below
-- =============================================================================

-- Stack Overflow: Load each year
-- Note: Column mappings need adjustment per year since SO changes column names.
-- Below is a generalized approach using MATCH_BY_COLUMN_NAME.

-- For 2024-2025 format (most recent column names)
COPY INTO raw_stackoverflow_survey (
  response_id, survey_year,
  main_branch, employment, remote_work, country, ed_level,
  age, years_code, years_code_pro, dev_type, org_size,
  currency, comp_total, comp_freq,
  language_have_worked_with, language_want_to_work_with,
  database_have_worked_with, database_want_to_work_with,
  platform_have_worked_with, platform_want_to_work_with,
  webframe_have_worked_with, webframe_want_to_work_with,
  misc_tech_have_worked_with, misc_tech_want_to_work_with,
  tools_tech_have_worked_with, tools_tech_want_to_work_with,
  ne_tech_have_worked_with, ne_tech_want_to_work_with,
  ai_select, ai_sent, ai_tools_currently_using
)
FROM @stackoverflow_stage
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- JetBrains
COPY INTO raw_jetbrains_survey (
  response_id, survey_year, country, age, gender,
  employment_status, company_size, job_role,
  years_of_experience, primary_language, languages_used,
  languages_plan_to_adopt, frameworks_used, ides_used,
  operating_systems, industry, team_size
)
FROM @jetbrains_stage
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- GitHub monthly activity (pre-aggregated from BigQuery)
COPY INTO raw_github_monthly_activity (
  activity_month, repo_language, event_type,
  event_count, unique_actors, unique_repos,
  total_commits, total_additions, total_deletions, avg_repo_stars
)
FROM @github_stage
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';
