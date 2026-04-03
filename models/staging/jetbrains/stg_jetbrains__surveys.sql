with source as (
    select * from {{ source('raw', 'raw_jetbrains_survey') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['response_id', 'survey_year']) }} as survey_response_id,
        response_id,
        survey_year,

        -- Demographics
        country,
        age,
        gender,
        employment_status,
        company_size,
        job_role,
        years_of_experience,
        industry,
        team_size,

        -- Technologies
        primary_language,
        languages_used,
        languages_plan_to_adopt,
        frameworks_used,
        ides_used,
        operating_systems,

        _loaded_at

    from source
    where response_id is not null
)

select * from cleaned
