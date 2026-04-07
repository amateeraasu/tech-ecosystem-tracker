-- Data quality test: fct_salary_by_stack
-- Checks:
--   1. Quartile ordering — p25 <= median <= p75 (must always hold)
--   2. Salary range realism — median between $20k and $500k
--   3. Premium calculation accuracy — premium = (median - global_median) / global_median * 100
--   4. Min respondent threshold — salary estimate needs ≥10 respondents to be meaningful
--
-- Returns rows that FAIL any check. Zero rows = all tests pass.

with salary as (
    select * from {{ ref('fct_salary_by_stack') }}
),

-- Test 1: Quartile ordering violation (p25 > median or median > p75)
quartile_order_failures as (
    select
        technology_name,
        survey_year::varchar as year,
        p25_salary,
        median_salary,
        p75_salary,
        case
            when p25_salary > median_salary
                then 'p25_greater_than_median (' || p25_salary || ' > ' || median_salary || ')'
            when median_salary > p75_salary
                then 'median_greater_than_p75 (' || median_salary || ' > ' || p75_salary || ')'
        end as failure_reason
    from salary
    where p25_salary is not null and p75_salary is not null
      and (p25_salary > median_salary or median_salary > p75_salary)
),

-- Test 2: Salary out of realistic range
salary_range_failures as (
    select
        technology_name,
        survey_year::varchar as year,
        median_salary,
        'median_salary_out_of_range ($' || median_salary || ')' as failure_reason
    from salary
    where median_salary < 20000 or median_salary > 500000
),

-- Test 3: Premium calculation sanity
-- expected_premium = (median - global_median) / global_median * 100
-- Allow ±1% tolerance for rounding
premium_calc_failures as (
    select
        technology_name,
        survey_year::varchar as year,
        salary_premium_pct,
        round(
            (median_salary - global_median_salary) * 100.0
            / nullif(global_median_salary, 0),
            2
        ) as expected_premium,
        'salary_premium_calculation_error' as failure_reason
    from salary
    where salary_premium_pct is not null
      and global_median_salary is not null
      and global_median_salary > 0
      and abs(
            salary_premium_pct -
            round(
                (median_salary - global_median_salary) * 100.0
                / nullif(global_median_salary, 0),
                2
            )
          ) > 1.0
),

-- Test 4: Low respondent count (< 10 makes salary estimates unreliable)
low_respondent_failures as (
    select
        technology_name,
        survey_year::varchar as year,
        respondent_count,
        'respondent_count_below_threshold (' || respondent_count || ')' as failure_reason
    from salary
    where respondent_count < 10
),

all_failures as (
    select technology_name, year, failure_reason from quartile_order_failures
    union all
    select technology_name, year, failure_reason from salary_range_failures
    union all
    select technology_name, year, failure_reason from premium_calc_failures
    union all
    select technology_name, year, failure_reason from low_respondent_failures
)

select * from all_failures
order by failure_reason, year, technology_name
