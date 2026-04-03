#!/bin/bash
# Load data files into Snowflake via SnowSQL
# Prerequisite: SnowSQL installed and configured, or use Snowflake UI upload
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== Loading data into Snowflake ==="
echo "Make sure SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD are set in .env"
echo ""

# Source .env if it exists
[ -f "$PROJECT_DIR/.env" ] && source "$PROJECT_DIR/.env"

SNOWSQL_CMD="snowsql -a ${SNOWFLAKE_ACCOUNT} -u ${SNOWFLAKE_USER} -d TECH_ECOSYSTEM -s RAW -w TECH_WH"

echo "--- Step 1: Run infrastructure setup ---"
$SNOWSQL_CMD -f "$PROJECT_DIR/snowflake/01_setup_infrastructure.sql"
$SNOWSQL_CMD -f "$PROJECT_DIR/snowflake/02_raw_tables.sql"

echo ""
echo "--- Step 2: Upload Stack Overflow CSVs ---"
for YEAR in 2021 2022 2023 2024 2025; do
  CSV="$PROJECT_DIR/data/stackoverflow/survey_${YEAR}.csv"
  if [ -f "$CSV" ]; then
    echo "Uploading SO $YEAR..."
    $SNOWSQL_CMD -q "PUT file://$CSV @stackoverflow_stage/${YEAR} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
  else
    echo "SKIP: $CSV not found"
  fi
done

echo ""
echo "--- Step 3: Upload JetBrains CSVs ---"
for YEAR in 2023 2024 2025; do
  CSV="$PROJECT_DIR/data/jetbrains/devecosystem_${YEAR}.csv"
  if [ -f "$CSV" ]; then
    echo "Uploading JetBrains $YEAR..."
    $SNOWSQL_CMD -q "PUT file://$CSV @jetbrains_stage/${YEAR} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
  else
    echo "SKIP: $CSV not found"
  fi
done

echo ""
echo "--- Step 4: Upload GitHub monthly activity ---"
CSV="$PROJECT_DIR/data/github/monthly_activity.csv"
if [ -f "$CSV" ]; then
  echo "Uploading GitHub monthly activity..."
  $SNOWSQL_CMD -q "PUT file://$CSV @github_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
else
  echo "SKIP: $CSV not found"
fi

echo ""
echo "--- Step 5: Run COPY INTO commands ---"
$SNOWSQL_CMD -f "$PROJECT_DIR/snowflake/03_stages_and_loading.sql"

echo ""
echo "--- Step 6: Verify row counts ---"
$SNOWSQL_CMD -q "
SELECT 'stackoverflow' AS source, COUNT(*) AS rows FROM raw_stackoverflow_survey
UNION ALL
SELECT 'jetbrains', COUNT(*) FROM raw_jetbrains_survey
UNION ALL
SELECT 'github_monthly', COUNT(*) FROM raw_github_monthly_activity;
"

echo ""
echo "=== Done! ==="
