-- models/monitoring/alert_adoption_anomalies.sql
-- Flags technologies whose adoption % in a given year deviates more than
-- 2 standard deviations from their own 2-year trailing average.
--
-- Uses survey sources only (stackoverflow, jetbrains) — GitHub event_share
-- is on a different scale and would skew the stddev calculation.
--
-- Output: one row per anomaly, ordered by severity.

with survey_adoption as (
    select
        technology_name,
        technology_category,
        data_source,
        year,
        survey_adoption_pct as adoption_pct
    from {{ ref('fct_technology_adoption') }}
    where data_source in ('stackoverflow', 'jetbrains')
      and survey_adoption_pct is not null
),

with_stats as (
    select
        technology_name,
        technology_category,
        data_source,
        year,
        adoption_pct,

        -- 2-year trailing average (excludes current year)
        avg(adoption_pct) over (
            partition by technology_name, data_source
            order by year
            rows between 2 preceding and 1 preceding
        ) as trailing_avg,

        -- 2-year trailing stddev (excludes current year)
        stddev(adoption_pct) over (
            partition by technology_name, data_source
            order by year
            rows between 2 preceding and 1 preceding
        ) as trailing_stddev,

        -- How many years of history we have (need ≥ 2 to flag anomalies)
        count(*) over (
            partition by technology_name, data_source
            order by year
            rows between unbounded preceding and 1 preceding
        ) as prior_years_count
    from survey_adoption
),

anomalies as (
    select
        technology_name,
        technology_category,
        data_source,
        year,
        round(adoption_pct, 2)       as adoption_pct,
        round(trailing_avg, 2)       as trailing_avg,
        round(trailing_stddev, 2)    as trailing_stddev,
        round(adoption_pct - trailing_avg, 2) as deviation,
        round(
            case
                when trailing_stddev > 0
                then (adoption_pct - trailing_avg) / trailing_stddev
                else null
            end, 2
        ) as z_score,
        case
            when adoption_pct > trailing_avg then 'spike'
            else 'drop'
        end as anomaly_direction,
        case
            when abs((adoption_pct - trailing_avg) / nullif(trailing_stddev, 0)) >= 3 then 'high'
            when abs((adoption_pct - trailing_avg) / nullif(trailing_stddev, 0)) >= 2 then 'medium'
        end as severity
    from with_stats
    where
        -- Need at least 2 prior years for a meaningful baseline
        prior_years_count >= 2
        -- Stddev must be non-zero (flat lines don't have anomalies)
        and trailing_stddev > 0
        -- Flag anything beyond 2 standard deviations
        and abs(adoption_pct - trailing_avg) > 2 * trailing_stddev
)

select *
from anomalies
order by abs(z_score) desc, year desc, technology_name
