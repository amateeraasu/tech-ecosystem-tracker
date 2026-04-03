-- Fact table: technology sentiment and desire metrics
-- Answers: "Which technologies do developers WANT to use vs. currently use?"
-- Key metric: desire_gap = desire_pct - adoption_pct (positive = growing interest)

with so_data as (
    select
        survey_year as year,
        technology_name,
        technology_category,
        adoption_pct,
        desire_pct,
        users_count as current_users,
        desired_count as aspiring_users,
        total_respondents
    from {{ ref('int_stackoverflow__yearly_adoption') }}
    where desire_pct is not null
),

sentiment as (
    select
        year,
        technology_name,
        technology_category,
        adoption_pct,
        desire_pct,
        current_users,
        aspiring_users,
        total_respondents,

        -- Desire gap: positive = more people want it than use it (growing)
        round(desire_pct - adoption_pct, 2) as desire_gap,

        -- Year-over-year change (requires window)
        adoption_pct - lag(adoption_pct) over (
            partition by technology_name order by year
        ) as adoption_yoy_change,

        desire_pct - lag(desire_pct) over (
            partition by technology_name order by year
        ) as desire_yoy_change,

        -- Classification
        case
            when desire_pct > adoption_pct
                 and (desire_pct - lag(desire_pct) over (partition by technology_name order by year)) > 0
                then 'emerging'
            when desire_pct > adoption_pct
                then 'growing_interest'
            when adoption_pct > desire_pct
                 and (adoption_pct - lag(adoption_pct) over (partition by technology_name order by year)) < 0
                then 'declining'
            when adoption_pct > desire_pct
                then 'established_but_waning'
            else 'stable'
        end as lifecycle_stage

    from so_data
),

final as (
    select
        s.*,
        d.technology_id,
        d.technology_subcategory
    from sentiment s
    left join {{ ref('dim_technologies') }} d
        on s.technology_name = d.technology_name
)

select * from final
