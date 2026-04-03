-- Dimension table: one row per canonical technology
with spine as (
    select distinct
        technology_id,
        canonical_name,
        technology_category,
        technology_subcategory
    from {{ ref('int_unified__technology_spine') }}
),

-- Enrich with source coverage flags
so_techs as (
    select distinct technology_name
    from {{ ref('int_stackoverflow__yearly_adoption') }}
),

jb_techs as (
    select distinct technology_name
    from {{ ref('int_jetbrains__yearly_adoption') }}
),

gh_techs as (
    select distinct technology_name
    from {{ ref('int_github__yearly_activity') }}
)

select
    s.technology_id,
    s.canonical_name as technology_name,
    s.technology_category,
    s.technology_subcategory,
    case when so.technology_name is not null then true else false end as in_stackoverflow,
    case when jb.technology_name is not null then true else false end as in_jetbrains,
    case when gh.technology_name is not null then true else false end as in_github,
    (case when so.technology_name is not null then 1 else 0 end
     + case when jb.technology_name is not null then 1 else 0 end
     + case when gh.technology_name is not null then 1 else 0 end
    ) as source_coverage_count
from spine s
left join so_techs so on s.canonical_name = so.technology_name
left join jb_techs jb on s.canonical_name = jb.technology_name
left join gh_techs gh on s.canonical_name = gh.technology_name
