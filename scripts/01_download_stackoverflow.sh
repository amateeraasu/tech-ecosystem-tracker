#!/bin/bash
# Download Stack Overflow Developer Survey data (2021-2025)
# Source: https://survey.stackoverflow.co/
set -euo pipefail

DATA_DIR="$(cd "$(dirname "$0")/../data/stackoverflow" && pwd)"
mkdir -p "$DATA_DIR"

echo "=== Downloading Stack Overflow Developer Surveys ==="

for YEAR in 2021 2022 2023 2024 2025; do
  DEST="$DATA_DIR/survey_${YEAR}.zip"
  if [ -f "$DEST" ]; then
    echo "[$YEAR] Already downloaded, skipping."
    continue
  fi
  echo "[$YEAR] Downloading..."
  # SO uses consistent URL pattern for survey downloads
  curl -L -o "$DEST" \
    "https://info.stackoverflowsolutions.com/rs/719-EMH-566/images/stack-overflow-developer-survey-${YEAR}.zip" \
    2>/dev/null || echo "[$YEAR] Direct link failed. Download manually from https://survey.stackoverflow.co/"
done

echo ""
echo "=== Extracting CSVs ==="
for YEAR in 2021 2022 2023 2024 2025; do
  ZIP="$DATA_DIR/survey_${YEAR}.zip"
  if [ -f "$ZIP" ]; then
    echo "[$YEAR] Extracting..."
    unzip -o -j "$ZIP" "*.csv" -d "$DATA_DIR/" 2>/dev/null || true
    # Rename to consistent naming
    for f in "$DATA_DIR"/survey_results_public.csv "$DATA_DIR"/Survey*.csv; do
      [ -f "$f" ] && mv "$f" "$DATA_DIR/survey_${YEAR}.csv" 2>/dev/null && break
    done
  fi
done

echo ""
echo "=== Done. Files in $DATA_DIR ==="
ls -lh "$DATA_DIR"/*.csv 2>/dev/null || echo "No CSVs found - check downloads above."
