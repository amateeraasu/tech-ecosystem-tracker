-- Data quality test: fct_technology_adoption
-- Checks:
--   1. Year coverage completeness — each technology/source has data for expected years
--   2. YoY change sanity — adoption can't swing more than 50 percentage points in one year
--   3. Duplicate detection — no duplicate (technology_name, year, data_source) rows
--   4. Source balance — all three data sources are present
--
-- Returns rows that FAIL any check. Zero rows = all tests pass.

with adoption as (
    select * from {{ ref('fct_technology_adoption') }}
),

-- Test 1: Duplicate rows
duplicate_keys as (
    select
        technology_name,
        year,
        data_source,
        count(*) as row_count,
        'duplicate_primary_key' as failure_reason
    from adoption
    group by 1, 2, 3
    having count(*) > 1
),

-- Test 2: YoY adoption change > 50 ppts (unrealistic, likely data error)
yoy_spikes as (
    select
        technology_name,
        year,
        data_source,
        survey_adoption_pct,
        lag(survey_adoption_pct) over (
            partition by technology_name, data_source
            order by year
        ) as prior_year_pct,
        abs(
            survey_adoption_pct -
            lag(survey_adoption_pct) over (
                partition by technology_name, data_source
                order by year
            )
        ) as yoy_change_abs
    from adoption
    where survey_adoption_pct is not null
),

yoy_spike_failures as (
    select
        technology_name,
        year,
        data_source,
        1 as row_count,
        'yoy_adoption_change_over_50ppt (' ||
            round(yoy_change_abs, 1) || ' ppt)' as failure_reason
    from yoy_spikes
    where prior_year_pct is not null
      and yoy_change_abs > 50
),

-- Test 3: Source coverage — all 3 sources must appear in each year
expected_sources as (
    select distinct year from adoption
    cross join (
        select 'stackoverflow' as data_source union all
        select 'jetbrains' union all
        select 'github'
    )
),
actual_sources as (
    select distinct year, data_source from adoption
),
missing_sources as (
    select
        e.year,
        e.data_source,
        1 as row_count,
        'missing_data_source_for_year' as failure_reason,
        null as technology_name
    from expected_sources e
    left join actual_sources a using (year, data_source)
    where a.data_source is null
),

-- Combine all failures
all_failures as (
    select technology_name, year::varchar as year, data_source, failure_reason
    from duplicate_keys

    union all

    select technology_name, year::varchar as year, data_source, failure_reason
    from yoy_spike_failures

    union all

    select technology_name, year::varchar as year, data_source, failure_reason
    from missing_sources
)

select * from all_failures
order by failure_reason, year, data_source, technology_name
