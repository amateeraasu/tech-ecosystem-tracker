-- =============================================================================
-- Tech Ecosystem Tracker — Streamlit in Snowflake deployment
-- Run as ACCOUNTADMIN (network rules require ACCOUNTADMIN or a privileged role)
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TECH_ECOSYSTEM;
USE SCHEMA PUBLIC;
USE WAREHOUSE TECH_WH;

-- ── 1. Network rule: allow outbound HTTPS to Anthropic API ────────────────────
CREATE OR REPLACE NETWORK RULE anthropic_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.anthropic.com:443');

-- ── 2. Secret: store the Anthropic API key ────────────────────────────────────
-- Replace <YOUR_KEY> with your actual key before running.
CREATE OR REPLACE SECRET ANTHROPIC_SECRET
  TYPE = GENERIC_STRING
  SECRET_STRING = '<YOUR_KEY>';

-- ── 3. External access integration ───────────────────────────────────────────
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION anthropic_access_integration
  ALLOWED_NETWORK_RULES = (anthropic_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (ANTHROPIC_SECRET)
  ENABLED = TRUE;

-- ── 4. Grant access to SYSADMIN so the Streamlit app can use it ───────────────
GRANT READ ON SECRET ANTHROPIC_SECRET TO ROLE SYSADMIN;
GRANT USAGE ON INTEGRATION anthropic_access_integration TO ROLE SYSADMIN;

-- ── 5. Create the Streamlit app ───────────────────────────────────────────────
-- Switch to SYSADMIN to own the app (consistent with rest of project)
USE ROLE SYSADMIN;
USE DATABASE TECH_ECOSYSTEM;
USE SCHEMA PUBLIC;

CREATE OR REPLACE STREAMLIT TECH_ECOSYSTEM_TRACKER
  ROOT_LOCATION = '@TECH_ECOSYSTEM.PUBLIC.STREAMLIT_STAGE'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = TECH_WH
  EXTERNAL_ACCESS_INTEGRATIONS = (anthropic_access_integration)
  COMMENT = 'Tech Ecosystem Tracker — SO + JetBrains + GitHub analytics';

-- ── 6. Stage for uploading app files ─────────────────────────────────────────
CREATE STAGE IF NOT EXISTS TECH_ECOSYSTEM.PUBLIC.STREAMLIT_STAGE
  DIRECTORY = (ENABLE = TRUE);

-- =============================================================================
-- After running this script:
--
-- Upload files via SnowSQL:
--   snowsql -a RWFZGOV-YPB85851 -u amateraasu
--   PUT file://app/streamlit_app.py @TECH_ECOSYSTEM.PUBLIC.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--   PUT file://app/environment.yml  @TECH_ECOSYSTEM.PUBLIC.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--
-- Or use Snowsight: Data > Streamlit > + Streamlit App > upload files manually.
--
-- Don't forget to replace <YOUR_KEY> in step 2 before running.
-- =============================================================================
