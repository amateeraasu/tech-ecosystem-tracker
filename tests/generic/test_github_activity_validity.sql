-- Generic test: github_activity_validity
-- Validates GitHub activity metrics for:
--   - No negative values
--   - No extreme outliers (single tech > 90% of all activity is suspicious)
--   - No suspicious YoY spikes (>10x growth in a single year)
--
-- Usage in schema.yml:
--   tests:
--     - github_activity_validity:
--         column_name: github_event_share
--         max_share: 0.9        # optional: max fraction for a single tech
--         max_yoy_multiplier: 10 # optional: max YoY growth multiplier

{% test github_activity_validity(
    model,
    column_name,
    max_share=0.9,
    max_yoy_multiplier=10
) %}

with base as (
    select
        technology_name,
        year,
        {{ column_name }} as metric_value
    from {{ model }}
    where {{ column_name }} is not null
),

with_yoy as (
    select
        technology_name,
        year,
        metric_value,
        lag(metric_value) over (
            partition by technology_name order by year
        ) as prior_year_value
    from base
),

failures as (
    -- Negative values
    select
        technology_name,
        year,
        metric_value,
        'negative_value' as failure_reason
    from base
    where metric_value < 0

    union all

    -- Suspiciously dominant single technology
    select
        technology_name,
        year,
        metric_value,
        'exceeds_max_share_(' || {{ max_share }} || ')' as failure_reason
    from base
    where metric_value > {{ max_share }}

    union all

    -- Suspicious year-over-year spike
    select
        technology_name,
        year,
        metric_value,
        'yoy_spike_over_' || {{ max_yoy_multiplier }} || 'x' as failure_reason
    from with_yoy
    where
        prior_year_value is not null
        and prior_year_value > 0
        and metric_value > prior_year_value * {{ max_yoy_multiplier }}
)

select * from failures

{% endtest %}
