# Test Coverage Report

## Summary

| Layer | Tables / Models | Tests Defined | Test Types |
|---|---|---|---|
| Marts (schema.yml) | 5 | 32 | not_null, accepted_values, accepted_range, unique, unique_combination |
| Generic (reusable) | All models | 3 | Custom Jinja macros |
| Mart-specific (singular) | 4 fct tables | 4 files / ~16 checks | Business rule SQL |
| Monitoring | 3 raw sources | Freshness views | Dashboard views |

---

## Schema Tests (models/marts/_marts__models.yml)

### dim_technologies
| Column | Tests |
|---|---|
| technology_id | unique, not_null |
| technology_name | unique, not_null |
| technology_category | not_null, accepted_values (8 categories) |

### fct_technology_adoption
| Column / Level | Tests |
|---|---|
| Model level | unique_combination_of_columns(technology_name, year, data_source) |
| technology_name | not_null |
| year | not_null, accepted_values (2021–2025) |
| data_source | not_null, accepted_values (stackoverflow, jetbrains, github) |
| survey_adoption_pct | accepted_range (0–100) where not null |
| survey_desire_pct | accepted_range (0–100) where not null |
| github_event_share | accepted_range (0–1) where not null |
| total_respondents | accepted_range (100–300,000) where not null |

### fct_said_vs_did
| Column / Level | Tests |
|---|---|
| Model level | unique_combination_of_columns(technology_name, year) |
| technology_name | not_null |
| year | not_null, accepted_values (2021–2025) |
| said_vs_did_classification | not_null, accepted_values (over_reported, under_reported, consistent) |
| avg_survey_adoption_pct | accepted_range (0–100) where not null |
| github_activity_pct | accepted_range (0–100) where not null |
| said_vs_did_gap | accepted_range (-100–100) |

### fct_salary_by_stack
| Column | Tests |
|---|---|
| technology_name | not_null |
| survey_year | not_null, accepted_values (2021–2025) |
| median_salary | not_null, accepted_range ($20k–$500k) |
| p25_salary | accepted_range ($20k–$500k) where not null |
| p75_salary | accepted_range ($20k–$500k) where not null |
| respondent_count | accepted_range (1–100,000) |
| salary_premium_pct | accepted_range (-90–500%) where not null |

### fct_technology_sentiment
| Column / Level | Tests |
|---|---|
| Model level | unique_combination_of_columns(technology_name, year) |
| technology_name | not_null |
| year | not_null, accepted_values (2021–2025) |
| lifecycle_stage | not_null, accepted_values (5 stages) |
| adoption_pct | not_null, accepted_range (0–100) |
| desire_pct | not_null, accepted_range (0–100) |
| desire_gap | accepted_range (-100–100) |

---

## Generic Tests (tests/generic/)

| Test Macro | What It Validates | Used On |
|---|---|---|
| `technology_name_standardization` | No whitespace, version numbers, or invalid chars | technology_name columns |
| `survey_response_ranges` | Sample sizes 100–300,000 (configurable) | total_respondents |
| `github_activity_validity` | No negatives, no >90% dominance, no 10x YoY spikes | github_event_share |

To apply in schema.yml:
```yaml
columns:
  - name: technology_name
    tests:
      - technology_name_standardization
  - name: total_respondents
    tests:
      - survey_response_ranges
  - name: github_event_share
    tests:
      - github_activity_validity
```

---

## Mart-Specific Tests (tests/data_quality/marts/)

### test_fct_technology_adoption.sql
- Duplicate (technology_name, year, data_source) key detection
- YoY adoption change > 50 ppt flagged as unrealistic
- Source coverage: all 3 sources must appear per year

### test_fct_said_vs_did.sql
- Duplicate (technology_name, year) key detection
- Gap arithmetic: said_vs_did_gap ≈ survey − github (±0.5 ppt)
- Classification consistency: over_reported → gap > 0; under_reported → gap < 0
- Rows with no survey AND no GitHub data flagged

### test_fct_salary_by_stack.sql
- Quartile ordering: p25 ≤ median ≤ p75 always enforced
- Salary range: $20k–$500k
- Premium calculation accuracy (±1%)
- Minimum 10 respondents for valid salary estimate

### test_fct_technology_sentiment.sql
- Duplicate (technology_name, year) key detection
- Desire gap arithmetic: desire_gap ≈ desire_pct − adoption_pct (±0.5 ppt)
- Percentage bounds: both adoption_pct and desire_pct must be 0–100
- Lifecycle classification consistency with desire_gap sign

---

## Monitoring Views (monitoring/)

| View | Purpose |
|---|---|
| VW_DATA_QUALITY_DASHBOARD | Per-model pass rates, freshness status, health label |
| VW_DATA_QUALITY_DETAILS | Individual test results with P1/P2/P3 priority |
| VW_DATA_QUALITY_TRENDS | Historical pass rate trends by model and day |

---

## Coverage Gaps

The following areas have limited or no automated test coverage:

| Gap | Risk | Recommended Fix |
|---|---|---|
| Staging models (stg_*) | Medium — errors could propagate silently | Add not_null + accepted_values to staging schema YMLs |
| Intermediate models (int_*) | Medium — spine logic is critical | Add relationship + uniqueness tests to _int__models.yml |
| Cross-year trend continuity | Low — YoY spikes only checked in adoption | Extend generic github_activity_validity to sentiment |
| Country/currency normalization in salary | Medium — $USD-only assumption | Add accepted_values test on compensation_frequency |
| GitHub event type distribution | Low | Add ratio tests on event type breakdown |
