with source as (
    select * from {{ source('raw', 'raw_stackoverflow_survey') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['response_id', 'survey_year']) }} as survey_response_id,
        response_id,
        survey_year,

        -- Demographics
        main_branch as developer_status,
        employment,
        remote_work,
        country,
        ed_level as education_level,
        age,
        try_to_number(years_code) as years_coding_total,
        try_to_number(years_code_pro) as years_coding_professional,
        dev_type as developer_roles,
        org_size as organization_size,

        -- Compensation
        currency as comp_currency,
        try_to_number(comp_total) as compensation_total,
        comp_freq as compensation_frequency,

        -- Technologies - current usage
        language_have_worked_with as languages_used,
        database_have_worked_with as databases_used,
        platform_have_worked_with as platforms_used,
        webframe_have_worked_with as web_frameworks_used,
        misc_tech_have_worked_with as other_tech_used,
        tools_tech_have_worked_with as dev_tools_used,

        -- Technologies - desired
        language_want_to_work_with as languages_desired,
        database_want_to_work_with as databases_desired,
        platform_want_to_work_with as platforms_desired,
        webframe_want_to_work_with as web_frameworks_desired,

        -- AI
        ai_select as ai_usage,
        ai_sent as ai_sentiment,
        ai_tools_currently_using,

        -- Satisfaction
        job_sat as job_satisfaction,

        _loaded_at

    from source
    where response_id is not null
)

select * from cleaned
