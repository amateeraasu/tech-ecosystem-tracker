-- =============================================================================
-- Tech Ecosystem Tracker - Snowflake Infrastructure Setup
-- Run this as SYSADMIN (or ACCOUNTADMIN for warehouse creation)
-- =============================================================================

-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS TECH_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Database
CREATE DATABASE IF NOT EXISTS TECH_ECOSYSTEM;

-- Schemas
USE DATABASE TECH_ECOSYSTEM;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- File formats
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NA', 'NULL', 'null', 'None')
  FIELD_DELIMITER = ','
  ESCAPE_UNENCLOSED_FIELD = NONE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE FILE FORMAT csv_tab_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NA', 'NULL', 'null', 'None')
  FIELD_DELIMITER = '\t'
  ESCAPE_UNENCLOSED_FIELD = NONE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
