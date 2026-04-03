with source as (
    select * from {{ source('raw', 'raw_github_monthly_activity') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['activity_month', 'repo_language', 'event_type']) }}
            as activity_id,
        activity_month,
        date_trunc('year', activity_month)::date as activity_year,
        repo_language,
        event_type,

        -- Metrics
        event_count,
        unique_actors,
        unique_repos,
        coalesce(total_commits, 0) as total_commits,
        coalesce(total_additions, 0) as total_additions,
        coalesce(total_deletions, 0) as total_deletions,
        coalesce(avg_repo_stars, 0) as avg_repo_stars,

        _loaded_at

    from source
    where repo_language != 'Unknown'
      and event_count > 0
)

select * from cleaned
