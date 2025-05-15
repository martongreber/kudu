#!/bin/bash
# This script should be run from the 'java/' directory.
set -euo pipefail

SUMMARY_FILE="build/jacocoLogAggregatedCoverage.txt"
HTML_REPORT="build/reports/jacoco/jacocoAggregatedReport/html/index.html"

echo "Running JaCoCo CLI summary..."
# Run the jacocoLogAggregatedCoverage task and extract only the summary section
# (starting with "Test Coverage:" until the next blank line), stripping all other Gradle logs.
if ! ./gradlew jacocoLogAggregatedCoverage | awk '/^Test Coverage:/,/^$/' > "$SUMMARY_FILE"; then
  echo "Gradle jacocoLogAggregatedCoverage task failed."
  exit 1
fi

if [[ -f "$HTML_REPORT" ]]; then
  echo "Appending summary to HTML report..."
  {
    echo "<pre>"
    cat "$SUMMARY_FILE"
    echo "</pre>"
  } >> "$HTML_REPORT"
  echo "Summary appended to $HTML_REPORT"
else
  echo "HTML report not found at $HTML_REPORT"
  exit 1
fi
