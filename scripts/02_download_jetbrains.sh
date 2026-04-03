#!/bin/bash
# Download JetBrains Developer Ecosystem Survey data (2023-2025)
# Source: https://www.jetbrains.com/lp/devecosystem-{year}/
# Note: JetBrains doesn't always offer direct CSV downloads.
#       You may need to download manually from their interactive reports.
set -euo pipefail

DATA_DIR="$(cd "$(dirname "$0")/../data/jetbrains" && pwd)"
mkdir -p "$DATA_DIR"

echo "=== JetBrains Developer Ecosystem Survey ==="
echo ""
echo "JetBrains survey data requires manual download or scraping from:"
echo ""
for YEAR in 2023 2024 2025; do
  CSV="$DATA_DIR/devecosystem_${YEAR}.csv"
  if [ -f "$CSV" ]; then
    echo "  [$YEAR] Already have: $CSV"
  else
    echo "  [$YEAR] NEEDED - Download from: https://www.jetbrains.com/lp/devecosystem-${YEAR}/"
  fi
done
echo ""
echo "Tips:"
echo "  - Look for 'Download raw data' or 'Data' links on each survey page"
echo "  - If only interactive charts are available, use the sharing/export options"
echo "  - Save CSVs as: data/jetbrains/devecosystem_YYYY.csv"
echo ""
echo "Required CSV columns (at minimum):"
echo "  country, age, employment_status, primary_language, languages_used,"
echo "  frameworks_used, years_of_experience, company_size, job_role"
