# Incident Playbook — Data Quality

## Severity Definitions

| Severity | Criteria | Response Time | Examples |
|---|---|---|---|
| **P0 — Pipeline Down** | dbt run fails completely; no data served to app | Immediate | Snowflake connection failure, broken SQL in mart model |
| **P1 — Critical Data Error** | Mart test failures; incorrect data served to users | < 1 hour | Quartile ordering violated, duplicate primary keys, math inconsistency |
| **P2 — Significant Degradation** | Multiple test failures affecting one mart; data may be wrong | < 4 hours | YoY spike anomalies, source coverage gaps, classification drift |
| **P3 — Warning / Freshness** | Stale data or non-critical test warnings | < 24 hours | Survey data not refreshed within 30 days, minor range warnings |

---

## P0 — Pipeline Down

### Symptoms
- `dbt run` exits non-zero
- Streamlit app throws SQL errors
- Snowflake mart tables are empty or missing

### Steps
1. Check dbt logs: `cat logs/dbt.log | tail -100`
2. Run `dbt debug` to verify Snowflake connection
3. Identify failing model: `dbt run --select <model> --fail-fast`
4. Check if raw source tables are populated:
   ```sql
   select count(*), max(_loaded_at) from TECH_ECOSYSTEM.RAW.raw_stackoverflow_survey;
   ```
5. If raw tables are empty → re-run data loading scripts (`scripts/04_load_to_snowflake.sh`)
6. If SQL error → check recent git changes to the failing model
7. Fix and re-run: `dbt run && dbt test`

---

## P1 — Critical Data Error

### Symptoms
- `dbt test` returns failures on mart tables
- `monitoring/quality_report.py` shows pass rate < 80%
- VW_DATA_QUALITY_DASHBOARD shows `health_status = 'critical'`

### Steps
1. Identify failing tests:
   ```bash
   python monitoring/quality_report.py
   ```
2. Inspect specific failures — run the failing test SQL directly in Snowsight
3. Trace back to source:
   - Uniqueness failure → check for duplicate rows in staging
   - Math inconsistency → check the mart model SQL logic
   - Range violation → check raw data for corrupt records
4. Common fixes:
   - **Duplicate keys**: add `QUALIFY ROW_NUMBER() OVER (...) = 1` in mart model
   - **Math error**: verify formula in mart SQL matches test expectation
   - **Bad source data**: add filter in staging model, document in `data_quality_standards.md`
5. After fix: `dbt run --select <affected_model>+ && dbt test`
6. Verify dashboard shows `health_status = 'healthy'`

---

## P2 — Significant Degradation

### Symptoms
- Multiple test warnings or isolated failures
- `monitoring/quality_report.py` shows pass rate 80–94%
- Specific mart shows degraded quality in VW_DATA_QUALITY_DASHBOARD

### Steps
1. Run quality report and review failures:
   ```bash
   python monitoring/quality_report.py --html
   open monitoring/quality_report.html
   ```
2. Classify each failure as data issue vs. test threshold issue
3. For data issues: investigate in Snowsight, trace to raw source
4. For threshold issues: if business definition changed, update test in schema.yml
5. Document the root cause in a git commit message

---

## P3 — Warning / Freshness

### Symptoms
- VW_DATA_QUALITY_DASHBOARD shows `freshness_status = 'stale'` or `'very_stale'`
- `monitoring/data_freshness_check.sql` shows WARNING or CRITICAL

### Steps
1. Run freshness check:
   ```sql
   -- In Snowsight
   \i monitoring/data_freshness_check.sql
   ```
2. Identify stale source
3. Re-run the appropriate loading script:
   - Stack Overflow: `python scripts/load_stackoverflow.py`
   - JetBrains: `python scripts/load_jetbrains.py`
   - GitHub: `python scripts/load_github_api.py`
4. Verify freshness resolved: re-run the freshness check

---

## Runbook: Full Pipeline Re-run

Use when multiple sources are stale or after a schema change.

```bash
# 1. Load fresh source data
python scripts/load_stackoverflow.py
python scripts/load_jetbrains.py
python scripts/load_github_api.py

# 2. Run full dbt pipeline
dbt run --full-refresh   # only if schema changed
# OR
dbt run                  # incremental

# 3. Run all tests
dbt test

# 4. Generate quality report
python monitoring/quality_report.py

# 5. Check freshness
snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -f monitoring/data_freshness_check.sql
```

---

## Escalation Path

| Who | When |
|---|---|
| Self-serve (this playbook) | P2, P3 |
| Review with team | P1 persistent > 2 hours |
| Escalate to data owner | P0 not resolved within 30 minutes |
