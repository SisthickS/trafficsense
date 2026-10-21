#!/usr/bin/env bash
# Build, test and run the Scala RDD demonstration (scala-rdd/) with spark-submit.
# Usage (from WSL):  bash scripts/run_scala_rdd.sh [--skip-build]
# Output: reports/scala_rdd/scala_rdd_report.{md,json} and reports/scala_rdd/sensor_profiles_csv/part-00000
set -euo pipefail
. "$HOME/.trafficsense_env.sh"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
ROOT="$(pwd)"
LOGS="$HOME/.cache/trafficsense/logs"; mkdir -p "$LOGS"
JAR="$ROOT/scala-rdd/target/scala-2.12/trafficsense-rdd_2.12-0.1.0.jar"
RAW="$ROOT/data/raw/pems_bay"
SCRATCH="$HOME/.cache/trafficsense/scala-rdd"

if [ "${1:-}" != "--skip-build" ]; then
  echo "=== sbt test package (log: $LOGS/sbt_build.log)"
  (cd scala-rdd && sbt -batch -no-colors test package > "$LOGS/sbt_build.log" 2>&1) || { tail -40 "$LOGS/sbt_build.log"; exit 1; }
  grep -E "Tests:|All tests passed|\[error\]" "$LOGS/sbt_build.log" || true
fi
[ -f "$JAR" ] || { echo "jar not found: $JAR"; exit 1; }

rm -rf "$SCRATCH"; mkdir -p "$SCRATCH"
echo "=== spark-submit trafficsense.rdd.TrafficRddDemo (log: $LOGS/scala_rdd.log)"
s=$(date +%s)
spark-submit \
  --class trafficsense.rdd.TrafficRddDemo \
  --master 'local[*]' \
  --driver-memory 3g \
  --conf spark.local.dir="$HOME/.cache/trafficsense/spark-local" \
  --conf spark.ui.showConsoleProgress=false \
  "$JAR" "$RAW/PEMS-BAY.csv" "$RAW/graph_sensor_locations_bay.csv" "$ROOT/reports/scala_rdd" "$SCRATCH" \
  > "$LOGS/scala_rdd.log" 2>&1 || { grep -vE "^\s+at " "$LOGS/scala_rdd.log" | tail -40; exit 1; }
grep "\[TrafficRddDemo\]" "$LOGS/scala_rdd.log"
echo "  -> done in $(( $(date +%s) - s ))s"
rm -rf "$SCRATCH"
