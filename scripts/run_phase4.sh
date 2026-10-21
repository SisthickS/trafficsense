#!/usr/bin/env bash
# Phase 4 runner: Spark core demonstrations + benchmarks -> reports/spark_features/
# Usage (from WSL):  bash scripts/run_phase4.sh [--quick] [--repeats N] [--skip-tests]
#   --quick      one month (2017-01), 1 repeat  (~6 min)
#   default      all 6 months (16.9M rows), 3 repeats after 1 warm-up (~35-45 min)
# Prerequisite: Phase 3 outputs (bash scripts/run_phase3.sh). Stop Docker containers first (RAM).
set -euo pipefail
ARGS=(); RUN_TESTS=1
while [ $# -gt 0 ]; do
  case "$1" in
    --quick) ARGS+=(--quick); shift ;;
    --repeats) ARGS+=(--repeats "$2"); shift 2 ;;
    --skip-tests) RUN_TESTS=0; shift ;;
    *) echo "unknown option $1"; exit 2 ;;
  esac
done

. "$HOME/.trafficsense_env.sh"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
LOGS="$HOME/.cache/trafficsense/logs"; mkdir -p "$LOGS"
OUT=reports/spark_features
step(){ printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
started=$(date +%s)

rm -rf "$OUT"; mkdir -p "$OUT"
for mod in dataframe_sql storage_benchmarks partition_benchmarks optimization; do
  step "$mod ${ARGS[*]:-}  (log: $LOGS/$mod.log)"
  s=$(date +%s)
  if ! python -m "src.spark_features.$mod" "${ARGS[@]}" > "$LOGS/$mod.log" 2>&1; then
    grep -vE "WARN|^\s+at " "$LOGS/$mod.log" | tail -25; echo "FAILED: $mod"; exit 1
  fi
  grep -E " INFO " "$LOGS/$mod.log" | grep -vE "Closing down|NumExpr|report:" | sed 's/^[0-9:]* INFO    /  /' | cut -c1-150
  echo "  -> done in $(( $(date +%s) - s ))s"
done

if [ "$RUN_TESTS" = 1 ]; then
  step "Unit tests (harness)"
  python -m pytest -q -p no:cacheprovider tests/test_spark_features.py 2>&1 | tail -2
fi

step "Verification"
fail=0
check(){ if eval "$2"; then echo "  [OK]   $1"; else echo "  [FAIL] $1"; fail=1; fi; }
for r in dataframe_sql storage_benchmarks partition_benchmarks optimization; do
  check "$r report (.md + .json)" "[ -s $OUT/$r.md ] && [ -s $OUT/$r.json ]"
done
check "SUMMARY.md lists 4 reports" "[ \$(grep -c '^## \[' $OUT/SUMMARY.md) -eq 4 ]"
check "DataFrame vs SQL: identical optimized + physical plans and results" \
  "python -c \"import json,sys; p=json.load(open('$OUT/dataframe_sql.json'))['sql']['pairs']; sys.exit(not all(v['same_optimized_plan'] and v['same_physical_plan'] and v['same_result'] for v in p.values()))\""
check "explain('formatted') plan files saved" "[ \$(ls $OUT/plans/*.txt | wc -l) -ge 10 ]"
check "lazy chain launched 0 jobs" "python -c \"import json,sys; sys.exit(json.load(open('$OUT/dataframe_sql.json'))['lazy']['lazy_jobs']!=0)\""
check "coalesce shuffled 0 bytes, repartition > 0" \
  "python -c \"import json,sys; d={r['op'].split('(')[0]:r for r in json.load(open('$OUT/partition_benchmarks.json'))['down']['rows']}; sys.exit(not (d['coalesce']['shuffleWriteBytes']==0 and d['repartition']['shuffleWriteBytes']>0))\""
check "broadcast join shuffled 0 bytes, sort-merge > 0" \
  "python -c \"import json,sys; j={r['strategy']:r for r in json.load(open('$OUT/optimization.json'))['joins']}; sys.exit(not (j['broadcast_hash']['shuffleWriteBytes']==0 and j['sort_merge']['shuffleWriteBytes']>0))\""
check "unpersist released all cached blocks" \
  "python -c \"import json,sys; c=json.load(open('$OUT/optimization.json'))['caching']; sys.exit(any(r.get('after_unpersist_storage_entries',0) for r in c))\""
check "Parquet smaller than CSV" \
  "python -c \"import json,sys; w={r['key']:r for r in json.load(open('$OUT/storage_benchmarks.json'))['writes']}; sys.exit(not w['parquet_snappy']['size_bytes'] < w['csv']['size_bytes'])\""
check "charts present" "[ \$(ls $OUT/*.png | wc -l) -ge 5 ]"
check "benchmark scratch data cleaned up" "[ ! -d \$HOME/.cache/trafficsense/bench/storage ] && [ ! -d \$HOME/.cache/trafficsense/bench/partitions ]"

echo; echo "Summary: $OUT/SUMMARY.md"
echo "Phase 4 finished in $(( $(date +%s) - started ))s - $([ $fail = 0 ] && echo ALL CHECKS PASSED || echo SOME CHECKS FAILED)"
exit $fail
