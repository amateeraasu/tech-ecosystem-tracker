-- =============================================================================
-- Tech Ecosystem Tracker — Data Quality Dashboard Views
-- Run in TECH_ECOSYSTEM.PUBLIC schema (or RAW_ANALYTICS if preferred)
-- =============================================================================

USE DATABASE TECH_ECOSYSTEM;
USE SCHEMA PUBLIC;
USE WAREHOUSE TECH_WH;

-- =============================================================================
-- Supporting table: dbt test results log
-- Populate via: INSERT INTO this table from your dbt test runner output,
-- or use the quality_report.py script which does this automatically.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dbt_test_results (
    run_id          VARCHAR        DEFAULT uuid_string(),
    run_at          TIMESTAMP_NTZ  DEFAULT current_timestamp(),
    model_name      VARCHAR        NOT NULL,
    test_name       VARCHAR        NOT NULL,
    column_name     VARCHAR,
    status          VARCHAR        NOT NULL,  -- 'pass' | 'fail' | 'warn'
    failure_count   INTEGER        DEFAULT 0,
    severity        VARCHAR        DEFAULT 'error',  -- 'error' | 'warn'
    execution_ms    INTEGER,
    error_message   VARCHAR
);

-- =============================================================================
-- VW_DATA_QUALITY_DASHBOARD
-- High-level pass rates, failure counts, and data freshness per model.
-- =============================================================================
CREATE OR REPLACE VIEW VW_DATA_QUALITY_DASHBOARD AS
with latest_run as (
    select max(run_at) as max_run_at from dbt_test_results
),

run_summary as (
    select
        r.model_name,
        r.run_at,
        count(*)                                               as total_tests,
        sum(case when r.status = 'pass' then 1 else 0 end)    as passed_tests,
        sum(case when r.status = 'fail' then 1 else 0 end)    as failed_tests,
        sum(case when r.status = 'warn' then 1 else 0 end)    as warned_tests,
        round(
            sum(case when r.status = 'pass' then 1 else 0 end) * 100.0
            / nullif(count(*), 0),
            1
        )                                                      as pass_rate_pct,
        sum(r.failure_count)                                   as total_failure_rows,
        max(r.run_at)                                          as last_tested_at
    from dbt_test_results r
    inner join latest_run lr on r.run_at = lr.max_run_at
    group by r.model_name, r.run_at
),

freshness as (
    -- Check _loaded_at timestamps on raw source tables
    select 'raw_stackoverflow_survey' as model_name,
           max(_loaded_at)            as last_loaded_at,
           datediff('hour', max(_loaded_at), current_timestamp()) as hours_since_load
    from TECH_ECOSYSTEM.RAW.raw_stackoverflow_survey

    union all

    select 'raw_jetbrains_survey',
           max(_loaded_at),
           datediff('hour', max(_loaded_at), current_timestamp())
    from TECH_ECOSYSTEM.RAW.raw_jetbrains_survey

    union all

    select 'raw_github_activity',
           max(_loaded_at),
           datediff('hour', max(_loaded_at), current_timestamp())
    from TECH_ECOSYSTEM.RAW.raw_github_activity
)

select
    coalesce(rs.model_name, f.model_name)    as model_name,
    rs.total_tests,
    rs.passed_tests,
    rs.failed_tests,
    rs.warned_tests,
    rs.pass_rate_pct,
    rs.total_failure_rows,
    rs.last_tested_at,
    f.last_loaded_at,
    f.hours_since_load,
    case
        when rs.pass_rate_pct >= 95             then 'healthy'
        when rs.pass_rate_pct >= 80             then 'degraded'
        when rs.pass_rate_pct is null           then 'untested'
        else                                         'critical'
    end                                             as health_status,
    case
        when f.hours_since_load <= 72           then 'fresh'
        when f.hours_since_load <= 168          then 'stale'
        when f.hours_since_load is null         then 'unknown'
        else                                         'very_stale'
    end                                             as freshness_status
from run_summary rs
full outer join freshness f on rs.model_name = f.model_name;


-- =============================================================================
-- VW_DATA_QUALITY_DETAILS
-- Individual test results with severity context.
-- =============================================================================
CREATE OR REPLACE VIEW VW_DATA_QUALITY_DETAILS AS
with latest_run as (
    select max(run_at) as max_run_at from dbt_test_results
)
select
    r.model_name,
    r.test_name,
    r.column_name,
    r.status,
    r.failure_count,
    r.severity,
    r.execution_ms,
    r.error_message,
    r.run_at,
    case
        when r.severity = 'error' and r.status = 'fail' then 'P1'
        when r.severity = 'warn'  and r.status = 'fail' then 'P2'
        when r.status = 'warn'                          then 'P3'
        else                                                 'OK'
    end as priority
from dbt_test_results r
inner join latest_run lr on r.run_at = lr.max_run_at
order by
    case priority when 'P1' then 1 when 'P2' then 2 when 'P3' then 3 else 4 end,
    r.model_name,
    r.test_name;


-- =============================================================================
-- VW_DATA_QUALITY_TRENDS
-- Historical pass rates per model to track quality over time.
-- =============================================================================
CREATE OR REPLACE VIEW VW_DATA_QUALITY_TRENDS AS
select
    model_name,
    date_trunc('day', run_at)::date                        as run_date,
    count(*)                                               as total_tests,
    sum(case when status = 'pass' then 1 else 0 end)       as passed_tests,
    sum(case when status = 'fail' then 1 else 0 end)       as failed_tests,
    round(
        sum(case when status = 'pass' then 1 else 0 end) * 100.0
        / nullif(count(*), 0),
        1
    )                                                      as pass_rate_pct,
    sum(failure_count)                                     as total_failure_rows
from dbt_test_results
group by 1, 2
order by model_name, run_date desc;
