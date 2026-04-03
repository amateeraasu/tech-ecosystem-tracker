with survey as (
    select * from {{ ref('stg_jetbrains__surveys') }}
),

languages_used as (
    select
        survey_response_id,
        survey_year,
        trim(lang.value::string) as technology_name,
        'language' as technology_category,
        true as is_currently_used,
        case
            when primary_language = trim(lang.value::string) then true
            else false
        end as is_primary
    from survey,
    lateral flatten(input => split(languages_used, ';')) as lang
    where languages_used is not null
),

languages_planned as (
    select
        survey_response_id,
        survey_year,
        trim(lang.value::string) as technology_name,
        'language' as technology_category,
        false as is_currently_used,
        false as is_primary
    from survey,
    lateral flatten(input => split(languages_plan_to_adopt, ';')) as lang
    where languages_plan_to_adopt is not null
),

frameworks as (
    select
        survey_response_id,
        survey_year,
        trim(fw.value::string) as technology_name,
        'framework' as technology_category,
        true as is_currently_used,
        false as is_primary
    from survey,
    lateral flatten(input => split(frameworks_used, ';')) as fw
    where frameworks_used is not null
),

all_tech as (
    select survey_response_id, survey_year, technology_name, technology_category,
           is_currently_used, is_primary
    from languages_used
    union all
    select survey_response_id, survey_year, technology_name, technology_category,
           is_currently_used, false
    from languages_planned
    union all
    select survey_response_id, survey_year, technology_name, technology_category,
           is_currently_used, is_primary
    from frameworks
),

consolidated as (
    select
        survey_response_id,
        survey_year,
        technology_name,
        technology_category,
        max(is_currently_used) as is_currently_used,
        max(is_primary) as is_primary
    from all_tech
    where technology_name != ''
    group by 1, 2, 3, 4
)

select * from consolidated
