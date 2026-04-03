-- Aggregate SO survey responses to yearly adoption rates per technology
with tech as (
    select * from {{ ref('stg_stackoverflow__technologies_unpivoted') }}
),

respondent_counts as (
    select
        survey_year,
        count(distinct survey_response_id) as total_respondents
    from {{ ref('stg_stackoverflow__surveys') }}
    group by 1
),

adoption as (
    select
        t.survey_year,
        spine.canonical_name as technology_name,
        spine.technology_category,
        spine.technology_subcategory,
        'stackoverflow' as data_source,
        count(distinct case when t.is_currently_used then t.survey_response_id end) as users_count,
        count(distinct case when t.is_desired then t.survey_response_id end) as desired_count,
        r.total_respondents
    from tech t
    inner join {{ ref('int_unified__technology_spine') }} spine
        on t.technology_name = spine.source_name
    inner join respondent_counts r
        on t.survey_year = r.survey_year
    group by 1, 2, 3, 4, 5, r.total_respondents
)

select
    *,
    round(users_count * 100.0 / nullif(total_respondents, 0), 2) as adoption_pct,
    round(desired_count * 100.0 / nullif(total_respondents, 0), 2) as desire_pct
from adoption
