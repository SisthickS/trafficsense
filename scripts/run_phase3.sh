#!/usr/bin/env bash
# Phase 3 runner: clean -> features/target/splits -> tests -> independent verification.
# Usage (from WSL):  bash scripts/run_phase3.sh [--skip-tests]
# Prerequisite: Phase 2 outputs (bash scripts/run_phase2.sh). Peak RAM ~6.5 GB: stop Docker containers first.
set -euo pipefail
RUN_TESTS=1
[ "${1:-}" = "--skip-tests" ] && RUN_TESTS=0

. "$HOME/.trafficsense_env.sh"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
LOGS="$HOME/.cache/trafficsense/logs"; mkdir -p "$LOGS"
step(){ printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
started=$(date +%s)
filter(){ grep -E " INFO " | grep -vE "Closing down|NumExpr" | sed 's/^[0-9:]* INFO    //'; }

step "1. Clean raw readings -> data/processed/pems_bay/clean  (log: $LOGS/clean.log)"
python -m src.preprocessing.clean_traffic > "$LOGS/clean.log" 2>&1 || { tail -30 "$LOGS/clean.log"; exit 1; }
filter < "$LOGS/clean.log"

step "2. Features, target, chronological splits -> data/processed/pems_bay/features  (log: $LOGS/features.log)"
python -m src.preprocessing.build_features > "$LOGS/features.log" 2>&1 || { tail -30 "$LOGS/features.log"; exit 1; }
filter < "$LOGS/features.log"

if [ "$RUN_TESTS" = 1 ]; then
  step "3. Unit tests"
  python -m pytest -q -p no:cacheprovider 2>&1 | tail -2
fi

step "4. Verification"
python -m src.preprocessing.verify_features 2>&1 | grep -E "^\s+\[|checks passed"
rc=${PIPESTATUS[0]}
echo; du -sh data/processed/pems_bay/clean data/processed/pems_bay/features
echo "Reports: reports/preprocessing/pems_bay_cleaning_report.md, reports/preprocessing/pems_bay_feature_report.md"
echo "Phase 3 finished in $(( $(date +%s) - started ))s - $([ "$rc" = 0 ] && echo ALL CHECKS PASSED || echo SOME CHECKS FAILED)"
exit "$rc"
