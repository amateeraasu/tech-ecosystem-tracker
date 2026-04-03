-- =============================================================================
-- Raw Tables - Stack Overflow Developer Survey
-- Covers 2021-2025. Columns vary by year; we use a wide superset approach.
-- =============================================================================
USE DATABASE TECH_ECOSYSTEM;
USE SCHEMA RAW;
USE WAREHOUSE TECH_WH;

-- Stack Overflow survey responses (union-friendly wide table)
CREATE OR REPLACE TABLE raw_stackoverflow_survey (
  response_id         VARCHAR,
  survey_year         INTEGER,
  main_branch         VARCHAR,    -- professional developer status
  employment          VARCHAR,
  remote_work         VARCHAR,
  country             VARCHAR,
  us_state            VARCHAR,
  ed_level            VARCHAR,    -- education level
  age                 VARCHAR,
  gender              VARCHAR,
  trans               VARCHAR,
  sexuality           VARCHAR,
  ethnicity           VARCHAR,
  accessibility       VARCHAR,
  years_code          VARCHAR,
  years_code_pro      VARCHAR,
  dev_type            VARCHAR,    -- developer roles (semicolon-delimited)
  org_size            VARCHAR,
  currency            VARCHAR,
  comp_total          VARCHAR,    -- total compensation
  comp_freq           VARCHAR,    -- annual/monthly/weekly
  -- Technology columns (semicolon-delimited multi-select)
  language_have_worked_with       VARCHAR,
  language_want_to_work_with      VARCHAR,
  database_have_worked_with       VARCHAR,
  database_want_to_work_with      VARCHAR,
  platform_have_worked_with       VARCHAR,
  platform_want_to_work_with      VARCHAR,
  webframe_have_worked_with       VARCHAR,
  webframe_want_to_work_with      VARCHAR,
  misc_tech_have_worked_with      VARCHAR,
  misc_tech_want_to_work_with     VARCHAR,
  tools_tech_have_worked_with     VARCHAR,
  tools_tech_want_to_work_with    VARCHAR,
  ne_tech_have_worked_with        VARCHAR,
  ne_tech_want_to_work_with       VARCHAR,
  collab_tools_have_worked_with   VARCHAR,
  collab_tools_want_to_work_with  VARCHAR,
  op_sys_professional             VARCHAR,
  op_sys_personal                 VARCHAR,
  ai_select                       VARCHAR,    -- AI tool usage
  ai_sent                         VARCHAR,    -- AI sentiment
  ai_tools_currently_using        VARCHAR,
  ai_tools_want_to_work_with      VARCHAR,
  -- Job satisfaction
  job_sat                         VARCHAR,
  -- Catch-all for additional columns
  _extra_fields                   VARIANT,
  _loaded_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- Raw Tables - JetBrains Developer Ecosystem Survey
-- =============================================================================
CREATE OR REPLACE TABLE raw_jetbrains_survey (
  response_id             VARCHAR,
  survey_year             INTEGER,
  country                 VARCHAR,
  age                     VARCHAR,
  gender                  VARCHAR,
  employment_status       VARCHAR,
  company_size            VARCHAR,
  job_role                VARCHAR,
  years_of_experience     VARCHAR,
  -- Primary language and all languages
  primary_language        VARCHAR,
  languages_used          VARCHAR,       -- semicolon or comma-delimited
  languages_plan_to_adopt VARCHAR,
  -- Frameworks and tools
  frameworks_used         VARCHAR,
  ides_used               VARCHAR,
  operating_systems       VARCHAR,
  -- Industry
  industry                VARCHAR,
  team_size               VARCHAR,
  -- Catch-all
  _extra_fields           VARIANT,
  _loaded_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- Raw Tables - GitHub Archive Events (from BigQuery export)
-- =============================================================================
CREATE OR REPLACE TABLE raw_github_events (
  event_id            VARCHAR,
  event_type          VARCHAR,       -- PushEvent, PullRequestEvent, etc.
  created_at          TIMESTAMP_NTZ,
  repo_name           VARCHAR,
  repo_id             NUMBER,
  actor_login         VARCHAR,
  actor_id            NUMBER,
  org_login           VARCHAR,
  -- Repo metadata
  repo_language       VARCHAR,       -- primary language of repo
  repo_stars          NUMBER,
  repo_forks          NUMBER,
  repo_size           NUMBER,
  -- Event-specific
  action              VARCHAR,       -- opened, closed, merged, etc.
  commits_count       NUMBER,        -- for PushEvents
  additions           NUMBER,
  deletions           NUMBER,
  -- Partitioning
  event_month         VARCHAR,       -- YYYY-MM for partitioning
  _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Monthly aggregated version (more practical for analysis)
CREATE OR REPLACE TABLE raw_github_monthly_activity (
  activity_month      DATE,
  repo_language       VARCHAR,
  event_type          VARCHAR,
  event_count         NUMBER,
  unique_actors       NUMBER,
  unique_repos        NUMBER,
  total_commits       NUMBER,
  total_additions     NUMBER,
  total_deletions     NUMBER,
  avg_repo_stars      FLOAT,
  _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
