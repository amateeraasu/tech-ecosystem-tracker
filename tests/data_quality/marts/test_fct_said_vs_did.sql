-- Data quality test: fct_said_vs_did
-- Checks:
--   1. Gap calculation accuracy — said_vs_did_gap must equal avg_survey - github_activity (±0.5)
--   2. Classification consistency — classification must match the gap value
--   3. Data completeness — rows should have at least one of survey or GitHub data
--   4. Duplicate detection — no duplicate (technology_name, year)
--
-- Returns rows that FAIL any check. Zero rows = all tests pass.

with svd as (
    select * from {{ ref('fct_said_vs_did') }}
),

-- Test 1: Duplicate primary keys
duplicate_keys as (
    select
        technology_name,
        year::varchar as year,
        count(*) as cnt,
        'duplicate_primary_key' as failure_reason
    from svd
    group by 1, 2
    having count(*) > 1
),

-- Test 2: Gap arithmetic consistency
-- said_vs_did_gap = avg_survey_adoption_pct - coalesce(github_activity_pct, 0)
-- Allow ±0.5 tolerance for rounding
gap_math_failures as (
    select
        technology_name,
        year::varchar as year,
        said_vs_did_gap,
        round(avg_survey_adoption_pct - coalesce(github_activity_pct, 0), 2) as expected_gap,
        abs(said_vs_did_gap - round(avg_survey_adoption_pct - coalesce(github_activity_pct, 0), 2)) as delta,
        'gap_calculation_error (actual=' || said_vs_did_gap ||
            ', expected=' || round(avg_survey_adoption_pct - coalesce(github_activity_pct, 0), 2) ||
            ')' as failure_reason
    from svd
    where avg_survey_adoption_pct is not null
      and said_vs_did_gap is not null
      and abs(said_vs_did_gap - (avg_survey_adoption_pct - coalesce(github_activity_pct, 0))) > 0.5
),

-- Test 3: Classification consistency with gap value
-- over_reported  → gap > 0 (survey > github by >50% relative)
-- under_reported → gap < 0 (github > survey by >50% relative)
-- consistent     → everything else
classification_failures as (
    select
        technology_name,
        year::varchar as year,
        said_vs_did_classification,
        avg_survey_adoption_pct,
        github_activity_pct,
        said_vs_did_gap,
        'classification_inconsistent_with_gap' as failure_reason
    from svd
    where
        -- over_reported but gap is negative
        (said_vs_did_classification = 'over_reported' and said_vs_did_gap < 0)
        -- under_reported but gap is positive
        or (said_vs_did_classification = 'under_reported' and said_vs_did_gap > 0)
),

-- Test 4: Rows with no data at all
no_data_rows as (
    select
        technology_name,
        year::varchar as year,
        'no_survey_or_github_data' as failure_reason
    from svd
    where avg_survey_adoption_pct is null
      and github_activity_pct is null
),

all_failures as (
    select technology_name, year, failure_reason
    from duplicate_keys

    union all

    select technology_name, year, failure_reason
    from gap_math_failures

    union all

    select technology_name, year, failure_reason
    from classification_failures

    union all

    select technology_name, year, failure_reason
    from no_data_rows
)

select * from all_failures
order by failure_reason, year, technology_name
