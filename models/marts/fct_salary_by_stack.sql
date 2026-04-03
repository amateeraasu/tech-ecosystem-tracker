-- Fact table: compensation analysis by technology stack
-- Answers: "What salary premiums come with specific tech stacks?"

with survey as (
    select * from {{ ref('stg_stackoverflow__surveys') }}
    where compensation_total is not null
      and compensation_total > 0
      and compensation_frequency = 'Yearly'
      -- Filter extreme outliers (likely data entry errors)
      and compensation_total between 10000 and 1000000
),

tech as (
    select * from {{ ref('stg_stackoverflow__technologies_unpivoted') }}
    where is_currently_used = true
),

salary_by_tech as (
    select
        s.survey_year,
        spine.canonical_name as technology_name,
        spine.technology_category,
        spine.technology_subcategory,
        s.country,
        s.developer_roles,
        s.years_coding_professional,
        s.education_level,

        -- Compensation metrics
        count(*) as respondent_count,
        round(avg(s.compensation_total), 0) as avg_salary,
        round(median(s.compensation_total), 0) as median_salary,
        round(percentile_cont(0.25) within group (order by s.compensation_total), 0) as p25_salary,
        round(percentile_cont(0.75) within group (order by s.compensation_total), 0) as p75_salary,
        min(s.compensation_total) as min_salary,
        max(s.compensation_total) as max_salary

    from survey s
    inner join tech t
        on s.survey_response_id = t.survey_response_id
    inner join {{ ref('int_unified__technology_spine') }} spine
        on t.technology_name = spine.source_name
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

-- Add premium calculation relative to overall median
with_premium as (
    select
        f.*,
        d.technology_id,

        -- Global median per year for comparison
        median(median_salary) over (partition by survey_year) as global_median_salary,

        -- Salary premium: how much above/below the global median
        round(
            (median_salary - median(median_salary) over (partition by survey_year))
            * 100.0 / nullif(median(median_salary) over (partition by survey_year), 0),
            2
        ) as salary_premium_pct

    from salary_by_tech f
    left join {{ ref('dim_technologies') }} d
        on f.technology_name = d.technology_name
)

select * from with_premium
