-- Fact table: the headline analysis — survey claims vs. actual GitHub activity
-- Compares SO/JetBrains "I use X" with GitHub "repos actually using X"

with survey_adoption as (
    -- Average adoption across SO and JetBrains per year
    select
        year,
        technology_name,
        technology_category,
        avg(survey_adoption_pct) as avg_survey_adoption_pct,
        max(survey_desire_pct) as max_survey_desire_pct
    from {{ ref('fct_technology_adoption') }}
    where data_source in ('stackoverflow', 'jetbrains')
      and survey_adoption_pct is not null
    group by 1, 2, 3
),

github_activity as (
    select
        year,
        technology_name,
        github_event_share * 100 as github_activity_pct,
        github_actor_share * 100 as github_actor_pct,
        github_total_events,
        github_unique_actors
    from {{ ref('fct_technology_adoption') }}
    where data_source = 'github'
),

compared as (
    select
        coalesce(s.year, g.year) as year,
        coalesce(s.technology_name, g.technology_name) as technology_name,
        s.technology_category,
        s.avg_survey_adoption_pct,
        s.max_survey_desire_pct,
        g.github_activity_pct,
        g.github_actor_pct,
        g.github_total_events,
        g.github_unique_actors,

        -- The "said vs did" gap
        -- Positive = people claim to use it more than GitHub shows
        -- Negative = more GitHub activity than survey claims suggest
        round(s.avg_survey_adoption_pct - coalesce(g.github_activity_pct, 0), 2)
            as said_vs_did_gap,

        case
            when s.avg_survey_adoption_pct > coalesce(g.github_activity_pct, 0) * 1.5
                then 'over_reported'   -- people say they use it but GitHub disagrees
            when coalesce(g.github_activity_pct, 0) > s.avg_survey_adoption_pct * 1.5
                then 'under_reported'  -- more GH activity than surveys suggest
            else 'consistent'
        end as said_vs_did_classification

    from survey_adoption s
    full outer join github_activity g
        on s.year = g.year
        and s.technology_name = g.technology_name
)

select
    c.*,
    d.technology_id,
    d.technology_subcategory
from compared c
left join {{ ref('dim_technologies') }} d
    on c.technology_name = d.technology_name
