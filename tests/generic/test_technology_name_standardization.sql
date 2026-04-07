-- Generic test: technology_name_standardization
-- Validates that technology names are clean canonical strings:
--   - No leading/trailing whitespace
--   - No version numbers (e.g. "Python 3.9", "Node.js 18")
--   - No special characters except hyphens, dots, slashes, and plus signs
--   - Not empty string
--
-- Usage in schema.yml:
--   tests:
--     - technology_name_standardization:
--         column_name: technology_name

{% test technology_name_standardization(model, column_name) %}

select
    {{ column_name }} as failing_value,
    'technology_name_standardization' as test_name,
    case
        when {{ column_name }} != trim({{ column_name }})
            then 'leading_or_trailing_whitespace'
        when {{ column_name }} = ''
            then 'empty_string'
        when regexp_like({{ column_name }}, '.*\\s+[0-9]+\\.?[0-9]*.*')
            then 'contains_version_number'
        when regexp_like({{ column_name }}, '.*[^a-zA-Z0-9\\s\\-\\.\\+/\\#].*')
            then 'invalid_special_character'
    end as failure_reason
from {{ model }}
where
    -- Fail if any of the above conditions are true
    {{ column_name }} != trim({{ column_name }})
    or {{ column_name }} = ''
    or regexp_like({{ column_name }}, '.*\\s+[0-9]+\\.?[0-9]*.*')
    or regexp_like({{ column_name }}, '.*[^a-zA-Z0-9\\s\\-\\.\\+/\\#].*')

{% endtest %}
