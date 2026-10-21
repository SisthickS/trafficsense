#!/usr/bin/env bash
# Phase 2 one-shot runner: acquire -> validate -> build Parquet -> tests -> EDA notebook -> verify.
# Usage (from WSL):  bash scripts/run_phase2.sh [--dataset pems-bay|metr-la] [--source zenodo|huggingface] [--skip-notebook]
set -euo pipefail

DATASET="pems-bay"; SOURCE="zenodo"; RUN_NOTEBOOK=1
while [ $# -gt 0 ]; do
  case "$1" in
    --dataset) DATASET="$2"; shift 2 ;;
    --source) SOURCE="$2"; shift 2 ;;
    --skip-notebook) RUN_NOTEBOOK=0; shift ;;
    *) echo "unknown option $1"; exit 2 ;;
  esac
done

. "$HOME/.trafficsense_env.sh"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
SLUG="${DATASET//-/_}"
RAW="data/raw/$SLUG"; PROC="data/processed/$SLUG"; REPORT="reports/validation/${SLUG}_validation_report"
step(){ printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
started=$(date +%s)

step "0. Environment"
echo "project: $(pwd)"; python --version; java -version 2>&1 | head -1; echo "spark: $(readlink -f "$SPARK_HOME")"

step "1. Inspect source (no download)"
python -m src.preprocessing.download_dataset --dataset "$DATASET" --source "$SOURCE" --inspect

step "2. Download + checksum-verify into $RAW (skips verified files)"
python -m src.preprocessing.download_dataset --dataset "$DATASET" --source "$SOURCE"

step "3. PySpark validation -> $REPORT.{json,md}"
python -m src.preprocessing.validate_dataset --dataset "$DATASET" 2>&1 | grep -E '\] C[0-9]+|Overall' | sed 's/^[0-9:]* INFO    //'

step "4. Build processed Parquet -> $PROC"
python -m src.preprocessing.build_processed --dataset "$DATASET" 2>&1 | grep -E 'readings:|sensors :' | sed 's/^[0-9:]* INFO    //'

step "5. Unit tests"
python -m pytest -q -p no:cacheprovider 2>&1 | tail -2

if [ "$RUN_NOTEBOOK" = 1 ] && [ "$DATASET" = "pems-bay" ]; then
  step "6. Execute EDA notebook -> reports/figures/eda/"
  jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1200 notebooks/eda.ipynb 2>&1 | grep -E 'Writing|Error' || true
fi

step "7. Verification"
fail=0
check(){ if eval "$2"; then echo "  [OK]   $1"; else echo "  [FAIL] $1"; fail=1; fi; }
check "raw files match MANIFEST sha256" \
  "python -c \"import json,hashlib,sys; m=json.load(open('$RAW/MANIFEST.json')); sys.exit(any(hashlib.sha256(open('$RAW/'+n,'rb').read()).hexdigest()!=f['sha256'] for n,f in m['files'].items()))\""
check "raw data files are read-only" "! find '$RAW' -type f ! -name MANIFEST.json -writable | grep -q ."
check "validation report not FAIL" "python -c \"import json,sys; sys.exit(json.load(open('$REPORT.json'))['overall_status']=='FAIL')\""
check "Parquet row count == validated readings" \
  "python -c \"import json,sys; v=json.load(open('$REPORT.json'))['profile']['sensor_readings']; b=json.load(open('$PROC/_build_info.json'))['readings']['rows']; print('    ',f'{b:,} rows'); sys.exit(v!=b)\""
check "Parquet is partitioned by month" "ls -d $PROC/readings/month=* >/dev/null 2>&1"
check "nothing derived was written into data/raw" "! find data/raw -name '*.parquet' -o -name '_SUCCESS' | grep -q ."
if [ "$RUN_NOTEBOOK" = 1 ] && [ "$DATASET" = "pems-bay" ]; then
  check "notebook executed without errors" \
    "python -c \"import json,sys; nb=json.load(open('notebooks/eda.ipynb')); sys.exit(any(o.get('output_type')=='error' for c in nb['cells'] for o in c.get('outputs',[])))\""
  check "10 EDA figures present" "[ \$(ls reports/figures/eda/*.png 2>/dev/null | wc -l) -ge 10 ]"
fi
echo; du -sh "$RAW" "$PROC" 2>/dev/null
echo "Phase 2 finished in $(( $(date +%s) - started ))s - $([ $fail = 0 ] && echo ALL CHECKS PASSED || echo SOME CHECKS FAILED)"
exit $fail
