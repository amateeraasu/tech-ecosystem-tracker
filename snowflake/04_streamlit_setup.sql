-- =============================================================================
-- Tech Ecosystem Tracker — Streamlit in Snowflake deployment
-- AI insights use Snowflake Cortex (no external access integration needed).
-- Run as SYSADMIN.
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE TECH_ECOSYSTEM;
USE SCHEMA PUBLIC;
USE WAREHOUSE TECH_WH;

-- ── 1. Stage for app files (must exist before Streamlit object) ───────────────
CREATE STAGE IF NOT EXISTS TECH_ECOSYSTEM.PUBLIC.STREAMLIT_STAGE
  DIRECTORY = (ENABLE = TRUE);

-- ── 2. Create the Streamlit app ───────────────────────────────────────────────
CREATE OR REPLACE STREAMLIT TECH_ECOSYSTEM_TRACKER
  ROOT_LOCATION = '@TECH_ECOSYSTEM.PUBLIC.STREAMLIT_STAGE'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = TECH_WH
  COMMENT = 'Tech Ecosystem Tracker — SO + JetBrains + GitHub analytics';

-- ── 3. Grant access ───────────────────────────────────────────────────────────
GRANT USAGE ON STREAMLIT TECH_ECOSYSTEM_TRACKER TO ROLE SYSADMIN;

-- ── 4. Grant Cortex usage for AI insights ────────────────────────────────────
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE SYSADMIN;
