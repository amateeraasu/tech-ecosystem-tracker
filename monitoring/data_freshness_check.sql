-- =============================================================================
-- Tech Ecosystem Tracker — Data Freshness Check
-- Run manually or on a schedule to detect stale source data.
-- Thresholds:
--   SO/JetBrains surveys: warn after 30 days, critical after 60 days
--   GitHub activity:      warn after 72 hours, critical after 168 hours (1 week)
-- =============================================================================

USE DATABASE TECH_ECOSYSTEM;
USE SCHEMA RAW;
USE WAREHOUSE TECH_WH;

with freshness_checks as (

    -- Stack Overflow Survey
    select
        'raw_stackoverflow_survey'          as source_name,
        'stackoverflow'                     as source_type,
        max(survey_year)                    as latest_year,
        max(_loaded_at)                     as last_loaded_at,
        datediff('hour', max(_loaded_at), current_timestamp())  as hours_since_load,
        datediff('day',  max(_loaded_at), current_timestamp())  as days_since_load,
        count(*)                            as total_rows,
        count(distinct survey_year)         as year_count,
        30 * 24                             as warn_hours,
        60 * 24                             as critical_hours
    from TECH_ECOSYSTEM.RAW.raw_stackoverflow_survey

    union all

    -- JetBrains Survey
    select
        'raw_jetbrains_survey',
        'jetbrains',
        max(survey_year),
        max(_loaded_at),
        datediff('hour', max(_loaded_at), current_timestamp()),
        datediff('day',  max(_loaded_at), current_timestamp()),
        count(*),
        count(distinct survey_year),
        30 * 24,
        60 * 24
    from TECH_ECOSYSTEM.RAW.raw_jetbrains_survey

    union all

    -- GitHub Activity
    select
        'raw_github_activity',
        'github',
        max(activity_year),
        max(_loaded_at),
        datediff('hour', max(_loaded_at), current_timestamp()),
        datediff('day',  max(_loaded_at), current_timestamp()),
        count(*),
        count(distinct activity_year),
        72,
        168
    from TECH_ECOSYSTEM.RAW.raw_github_activity

),

with_status as (
    select
        source_name,
        source_type,
        latest_year,
        last_loaded_at,
        hours_since_load,
        days_since_load,
        total_rows,
        year_count,
        case
            when hours_since_load >= critical_hours then 'CRITICAL'
            when hours_since_load >= warn_hours     then 'WARNING'
            else                                         'OK'
        end as freshness_status,
        case
            when hours_since_load >= critical_hours
                then 'Data is ' || days_since_load || ' days old — exceeds critical threshold'
            when hours_since_load >= warn_hours
                then 'Data is ' || days_since_load || ' days old — exceeds warning threshold'
            else
                'Data is fresh (' || hours_since_load || ' hours old)'
        end as message
    from freshness_checks
)

select * from with_status
order by
    case freshness_status
        when 'CRITICAL' then 1
        when 'WARNING'  then 2
        else                 3
    end,
    source_name;

-- =============================================================================
-- Summary: any critical issues?
-- =============================================================================
select
    sum(case when freshness_status = 'CRITICAL' then 1 else 0 end) as critical_count,
    sum(case when freshness_status = 'WARNING'  then 1 else 0 end) as warning_count,
    sum(case when freshness_status = 'OK'       then 1 else 0 end) as ok_count,
    case
        when sum(case when freshness_status = 'CRITICAL' then 1 else 0 end) > 0
            then 'ACTION REQUIRED: critical freshness failures detected'
        when sum(case when freshness_status = 'WARNING' then 1 else 0 end) > 0
            then 'ATTENTION: freshness warnings detected'
        else
            'All sources are fresh'
    end as overall_status
from with_status;
