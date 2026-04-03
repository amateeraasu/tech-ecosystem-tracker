-- Unpivot the semicolon-delimited technology columns into one row per tech per respondent
-- This is the key transformation for cross-source analysis

with survey as (
    select * from {{ ref('stg_stackoverflow__surveys') }}
),

-- Macro to split semicolons and unpivot
languages_used as (
    select
        survey_response_id,
        survey_year,
        trim(lang.value::string) as technology_name,
        'language' as technology_category,
        true as is_currently_used,
        false as is_desired
    from survey,
    lateral flatten(input => split(languages_used, ';')) as lang
    where languages_used is not null
),

languages_desired as (
    select
        survey_response_id,
        survey_year,
        trim(lang.value::string) as technology_name,
        'language' as technology_category,
        false as is_currently_used,
        true as is_desired
    from survey,
    lateral flatten(input => split(languages_desired, ';')) as lang
    where languages_desired is not null
),

databases_used as (
    select
        survey_response_id,
        survey_year,
        trim(db.value::string) as technology_name,
        'database' as technology_category,
        true as is_currently_used,
        false as is_desired
    from survey,
    lateral flatten(input => split(databases_used, ';')) as db
    where databases_used is not null
),

databases_desired as (
    select
        survey_response_id,
        survey_year,
        trim(db.value::string) as technology_name,
        'database' as technology_category,
        false as is_currently_used,
        true as is_desired
    from survey,
    lateral flatten(input => split(databases_desired, ';')) as db
    where databases_desired is not null
),

webframeworks_used as (
    select
        survey_response_id,
        survey_year,
        trim(fw.value::string) as technology_name,
        'web_framework' as technology_category,
        true as is_currently_used,
        false as is_desired
    from survey,
    lateral flatten(input => split(web_frameworks_used, ';')) as fw
    where web_frameworks_used is not null
),

webframeworks_desired as (
    select
        survey_response_id,
        survey_year,
        trim(fw.value::string) as technology_name,
        'web_framework' as technology_category,
        false as is_currently_used,
        true as is_desired
    from survey,
    lateral flatten(input => split(web_frameworks_desired, ';')) as fw
    where web_frameworks_desired is not null
),

platforms_used as (
    select
        survey_response_id,
        survey_year,
        trim(p.value::string) as technology_name,
        'platform' as technology_category,
        true as is_currently_used,
        false as is_desired
    from survey,
    lateral flatten(input => split(platforms_used, ';')) as p
    where platforms_used is not null
),

platforms_desired as (
    select
        survey_response_id,
        survey_year,
        trim(p.value::string) as technology_name,
        'platform' as technology_category,
        false as is_currently_used,
        true as is_desired
    from survey,
    lateral flatten(input => split(platforms_desired, ';')) as p
    where platforms_desired is not null
),

all_technologies as (
    select * from languages_used
    union all select * from languages_desired
    union all select * from databases_used
    union all select * from databases_desired
    union all select * from webframeworks_used
    union all select * from webframeworks_desired
    union all select * from platforms_used
    union all select * from platforms_desired
),

-- Consolidate: one row per respondent per tech, with both flags
consolidated as (
    select
        survey_response_id,
        survey_year,
        technology_name,
        technology_category,
        max(is_currently_used) as is_currently_used,
        max(is_desired) as is_desired
    from all_technologies
    where technology_name != ''
    group by 1, 2, 3, 4
)

select * from consolidated
