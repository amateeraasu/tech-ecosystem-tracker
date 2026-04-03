-- Aggregate GitHub activity to yearly level, mapped through technology spine
with monthly as (
    select * from {{ ref('stg_github__monthly_activity') }}
),

yearly as (
    select
        extract(year from activity_month) as activity_year,
        spine.canonical_name as technology_name,
        spine.technology_category,
        spine.technology_subcategory,
        'github' as data_source,

        -- Activity metrics
        sum(event_count) as total_events,
        sum(unique_actors) as total_unique_actors,
        sum(unique_repos) as total_unique_repos,
        sum(total_commits) as total_commits,
        sum(total_additions) as total_additions,
        sum(total_deletions) as total_deletions,
        avg(avg_repo_stars) as avg_stars

    from monthly m
    inner join {{ ref('int_unified__technology_spine') }} spine
        on m.repo_language = spine.source_name
    group by 1, 2, 3, 4, 5
),

-- Rank languages by total activity per year for relative sizing
ranked as (
    select
        *,
        total_events * 1.0 / nullif(sum(total_events) over (partition by activity_year), 0)
            as event_share,
        total_unique_actors * 1.0 / nullif(sum(total_unique_actors) over (partition by activity_year), 0)
            as actor_share
    from yearly
)

select * from ranked
