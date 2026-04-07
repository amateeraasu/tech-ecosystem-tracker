-- Generic test: survey_response_ranges
-- Validates that survey sample sizes fall within realistic bounds.
-- Stack Overflow surveys: ~40k–90k respondents
-- JetBrains surveys:      ~10k–30k respondents
-- Both together:           ~10k–300k is a safe upper bound
--
-- Usage in schema.yml:
--   tests:
--     - survey_response_ranges:
--         column_name: total_respondents
--         min_value: 100       # optional override
--         max_value: 300000    # optional override

{% test survey_response_ranges(model, column_name, min_value=100, max_value=300000) %}

select
    {{ column_name }} as failing_value,
    'survey_response_ranges' as test_name,
    case
        when {{ column_name }} < {{ min_value }}
            then 'below_minimum_(' || {{ min_value }} || ')'
        when {{ column_name }} > {{ max_value }}
            then 'above_maximum_(' || {{ max_value }} || ')'
    end as failure_reason
from {{ model }}
where
    {{ column_name }} is not null
    and (
        {{ column_name }} < {{ min_value }}
        or {{ column_name }} > {{ max_value }}
    )

{% endtest %}
