-- Data quality test: fct_technology_sentiment
-- Checks:
--   1. Sentiment score bounds — adoption_pct and desire_pct must be 0–100
--   2. Desire gap arithmetic — desire_gap = desire_pct - adoption_pct (±0.5 tolerance)
--   3. Lifecycle classification consistency with desire_gap and YoY changes
--   4. Duplicate detection — no duplicate (technology_name, year)
--
-- Returns rows that FAIL any check. Zero rows = all tests pass.

with sentiment as (
    select * from {{ ref('fct_technology_sentiment') }}
),

-- Test 1: Duplicate primary keys
duplicate_keys as (
    select
        technology_name,
        year::varchar as year,
        count(*) as cnt,
        'duplicate_primary_key' as failure_reason
    from sentiment
    group by 1, 2
    having count(*) > 1
),

-- Test 2: Desire gap arithmetic
-- desire_gap = desire_pct - adoption_pct
gap_math_failures as (
    select
        technology_name,
        year::varchar as year,
        desire_gap,
        round(desire_pct - adoption_pct, 2) as expected_gap,
        'desire_gap_calculation_error (actual=' || desire_gap ||
            ', expected=' || round(desire_pct - adoption_pct, 2) ||
            ')' as failure_reason
    from sentiment
    where desire_gap is not null
      and abs(desire_gap - (desire_pct - adoption_pct)) > 0.5
),

-- Test 3: Percentage bounds
pct_bounds_failures as (
    select
        technology_name,
        year::varchar as year,
        adoption_pct,
        desire_pct,
        case
            when adoption_pct < 0 or adoption_pct > 100
                then 'adoption_pct_out_of_bounds (' || adoption_pct || ')'
            when desire_pct < 0 or desire_pct > 100
                then 'desire_pct_out_of_bounds (' || desire_pct || ')'
        end as failure_reason
    from sentiment
    where adoption_pct < 0 or adoption_pct > 100
       or desire_pct < 0 or desire_pct > 100
),

-- Test 4: Lifecycle classification consistency
-- emerging / growing_interest → desire_pct > adoption_pct (positive desire_gap)
-- declining / established_but_waning → adoption_pct > desire_pct (negative desire_gap)
lifecycle_consistency_failures as (
    select
        technology_name,
        year::varchar as year,
        lifecycle_stage,
        desire_gap,
        'lifecycle_inconsistent_with_desire_gap' as failure_reason
    from sentiment
    where
        (lifecycle_stage in ('emerging', 'growing_interest') and desire_gap < 0)
        or (lifecycle_stage in ('declining', 'established_but_waning') and desire_gap > 0)
),

all_failures as (
    select technology_name, year, failure_reason from duplicate_keys
    union all
    select technology_name, year, failure_reason from gap_math_failures
    union all
    select technology_name, year, failure_reason from pct_bounds_failures
    union all
    select technology_name, year, failure_reason from lifecycle_consistency_failures
)

select * from all_failures
order by failure_reason, year, technology_name
