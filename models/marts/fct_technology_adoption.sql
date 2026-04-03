-- Fact table: unified adoption metrics across all sources per year
-- Answers: "Which technologies are emerging vs. dying?"
-- Answers: "Said vs. Did — do survey responses match GitHub activity?"

with so_adoption as (
    select
        survey_year as year,
        technology_name,
        technology_category,
        'stackoverflow' as data_source,
        adoption_pct as survey_adoption_pct,
        desire_pct as survey_desire_pct,
        users_count,
        total_respondents,
        null::float as github_event_share,
        null::float as github_actor_share,
        null::int as github_total_events,
        null::int as github_unique_actors,
        null::int as github_unique_repos
    from {{ ref('int_stackoverflow__yearly_adoption') }}
),

jb_adoption as (
    select
        survey_year as year,
        technology_name,
        technology_category,
        'jetbrains' as data_source,
        adoption_pct as survey_adoption_pct,
        null as survey_desire_pct,
        users_count,
        total_respondents,
        null::float as github_event_share,
        null::float as github_actor_share,
        null::int as github_total_events,
        null::int as github_unique_actors,
        null::int as github_unique_repos
    from {{ ref('int_jetbrains__yearly_adoption') }}
),

gh_activity as (
    select
        activity_year as year,
        technology_name,
        technology_category,
        'github' as data_source,
        null::float as survey_adoption_pct,
        null::float as survey_desire_pct,
        null::int as users_count,
        null::int as total_respondents,
        event_share as github_event_share,
        actor_share as github_actor_share,
        total_events as github_total_events,
        total_unique_actors as github_unique_actors,
        total_unique_repos as github_unique_repos
    from {{ ref('int_github__yearly_activity') }}
),

unioned as (
    select * from so_adoption
    union all
    select * from jb_adoption
    union all
    select * from gh_activity
),

-- Create the cross-source "said vs did" comparison
final as (
    select
        u.year,
        u.technology_name,
        u.technology_category,
        u.data_source,
        u.survey_adoption_pct,
        u.survey_desire_pct,
        u.users_count,
        u.total_respondents,
        u.github_event_share,
        u.github_actor_share,
        u.github_total_events,
        u.github_unique_actors,
        u.github_unique_repos,
        d.technology_id
    from unioned u
    left join {{ ref('dim_technologies') }} d
        on u.technology_name = d.technology_name
)

select * from final
