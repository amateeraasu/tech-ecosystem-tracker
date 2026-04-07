# Data Quality Standards

## Overview

This document defines the data quality standards for the Tech Ecosystem Tracker.
All data flowing from raw sources through dbt models and into the Streamlit app
must meet these standards before being considered production-ready.

---

## Quality Dimensions

### 1. Completeness
Data must be present where expected.

| Table | Required Columns | Null Tolerance |
|---|---|---|
| fct_technology_adoption | technology_name, year, data_source | survey_* columns null for GitHub rows; github_* null for survey rows |
| fct_said_vs_did | technology_name, year, said_vs_did_classification | At least one of survey or GitHub data must be non-null |
| fct_salary_by_stack | technology_name, survey_year, median_salary | p25/p75 may be null if too few respondents |
| fct_technology_sentiment | technology_name, year, adoption_pct, desire_pct | All key metrics required |

### 2. Validity
Values must fall within defined acceptable ranges.

| Metric | Valid Range | Notes |
|---|---|---|
| Adoption % | 0–100 | Percentage of respondents |
| Desire % | 0–100 | Percentage of respondents |
| GitHub event share | 0–1 | Fraction (not percentage) |
| Survey sample size | 100–300,000 | Per year per source |
| Median salary | $20,000–$500,000 | Annual USD |
| Salary premium | -90% to +500% | Vs. global median |
| Year | 2021–2025 | Coverage window |

### 3. Consistency
Derived metrics must be mathematically consistent with their source fields.

| Derived Field | Formula | Tolerance |
|---|---|---|
| said_vs_did_gap | avg_survey_adoption_pct − github_activity_pct | ±0.5 ppt |
| desire_gap | desire_pct − adoption_pct | ±0.5 ppt |
| salary_premium_pct | (median − global_median) / global_median × 100 | ±1% |

Classifications (said_vs_did_classification, lifecycle_stage) must be consistent
with the underlying numeric values.

### 4. Uniqueness
Primary keys must be unique across all mart tables.

| Table | Primary Key |
|---|---|
| dim_technologies | technology_id, technology_name |
| fct_technology_adoption | (technology_name, year, data_source) |
| fct_said_vs_did | (technology_name, year) |
| fct_salary_by_stack | (technology_name, survey_year, country) |
| fct_technology_sentiment | (technology_name, year) |

### 5. Timeliness (Freshness)
Source data must be loaded within acceptable windows.

| Source | Warn Threshold | Critical Threshold |
|---|---|---|
| Stack Overflow Survey | 30 days | 60 days |
| JetBrains Survey | 30 days | 60 days |
| GitHub Archive | 72 hours | 168 hours (1 week) |

---

## SLAs and Targets

| Metric | Target | Minimum Acceptable |
|---|---|---|
| Overall test pass rate | ≥ 98% | 95% |
| P1 (critical) failures | 0 | 0 |
| P2 (warning) failures | 0 | ≤ 3 |
| Data freshness | All sources fresh | No critical staleness |
| dbt run success rate | 100% | 99% |

---

## Technology Name Standards

Technology names must conform to the canonical names defined in `int_unified__technology_spine.sql`.

**Rules:**
- No leading or trailing whitespace
- No version numbers appended (e.g. "Python 3.9" → "Python")
- No special characters except: hyphens (`-`), dots (`.`), slashes (`/`), plus signs (`+`), hash (`#`)
- Use official casing: "JavaScript" not "javascript", "PostgreSQL" not "Postgresql"

---

## Test Execution

Tests run automatically as part of the dbt pipeline. To run manually:

```bash
# All tests
dbt test

# Specific mart
dbt test --select fct_technology_adoption

# Generate quality report
python monitoring/quality_report.py

# Check data freshness
snowsql -f monitoring/data_freshness_check.sql
```

---

## Escalation

See `incident_playbook.md` for response procedures when standards are not met.
