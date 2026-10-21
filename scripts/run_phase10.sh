#!/usr/bin/env bash
# Phase 10 runner: build the dashboard, start API + dashboard if needed, run every automated test layer, benchmark, write logs.
#
#   bash scripts/run_phase10.sh            full verification (build, pytest, vitest, benchmark)
#   SKIP_FULL_PYTEST=1 bash scripts/run_phase10.sh   only the API / contract / end-to-end tests (faster)
#
# Logs: reports/final/logs/*.txt     Benchmark: reports/final/dashboard_benchmark.json
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f "$HOME/.trafficsense_env.sh" ] && . "$HOME/.trafficsense_env.sh"

LOGS=reports/final/logs; mkdir -p "$LOGS"
API=http://127.0.0.1:8000; WEB=http://127.0.0.1:3000
STARTED=(); FAIL=0
step() { printf '\n=== %s ===\n' "$1"; }
ok()   { printf '  PASS  %s\n' "$1"; }
bad()  { printf '  FAIL  %s\n' "$1"; FAIL=1; }
up()   { curl -fs -o /dev/null --max-time 3 "$1"; }
cleanup() { for p in "${STARTED[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done; }
trap cleanup EXIT

step "1. MongoDB"
docker compose up -d --wait mongo >"$LOGS/mongo.txt" 2>&1 && ok "mongo healthy" || { bad "mongo did not start (see $LOGS/mongo.txt)"; exit 1; }
python - <<'PY' && ok "database loaded" || { bad "database empty: run python -m src.mongo.load_outputs"; exit 1; }
from src.mongo.connection import make_client
d = make_client(timeout_ms=3000)["trafficsense"]
n = {c: d[c].count_documents({}) for c in ("sensor_metadata", "analysis_results", "congestion_hotspots", "traffic_predictions", "model_evaluations", "stream_metrics", "pipeline_runs")}
print(n); assert n["analysis_results"] >= 90 and n["sensor_metadata"] >= 300
PY

step "2. Frontend type-check and production build"
( cd frontend && { [ -d node_modules/next ] || npm ci; } && npm run typecheck && npm run build ) >"$LOGS/frontend_build.txt" 2>&1 \
  && ok "typecheck + next build (see $LOGS/frontend_build.txt)" || { bad "frontend build failed"; tail -n 30 "$LOGS/frontend_build.txt"; exit 1; }

step "3. Start API (:8000) and dashboard (:3000) if they are not running"
if ! up "$API/ready"; then
  uvicorn src.api.main:app --host 127.0.0.1 --port 8000 --log-level warning >"$LOGS/api_server.txt" 2>&1 & STARTED+=($!)
fi
if ! up "$WEB/"; then
  ( cd frontend && exec node node_modules/next/dist/bin/next start -p 3000 ) >"$LOGS/web_server.txt" 2>&1 & STARTED+=($!)
fi
for _ in $(seq 1 60); do up "$API/ready" && up "$WEB/" && break; sleep 1; done
up "$API/ready" && ok "API ready" || { bad "API not reachable"; exit 1; }
up "$WEB/" && ok "dashboard reachable" || { bad "dashboard not reachable"; exit 1; }

step "4. Python tests: contract + end-to-end + API"
python -m pytest -q -p no:cacheprovider tests/test_frontend_contract.py tests/test_integration_e2e.py tests/test_api.py tests/test_mongo.py >"$LOGS/pytest_integration.txt" 2>&1 \
  && ok "$(tail -n 1 "$LOGS/pytest_integration.txt")" || { bad "integration pytest"; tail -n 30 "$LOGS/pytest_integration.txt"; }

if [ -z "${SKIP_FULL_PYTEST:-}" ]; then
  step "5. Full Python suite (all phases)"
  python -m pytest -q -p no:cacheprovider tests >"$LOGS/pytest_full.txt" 2>&1 \
    && ok "$(tail -n 1 "$LOGS/pytest_full.txt")" || { bad "full pytest"; tail -n 30 "$LOGS/pytest_full.txt"; }
fi

step "6. Frontend tests (vitest against the live API)"
( cd frontend && npx vitest run ) >"$LOGS/vitest.txt" 2>&1 \
  && ok "$(grep -E 'Tests ' "$LOGS/vitest.txt" | tail -n 1)" || { bad "vitest"; tail -n 40 "$LOGS/vitest.txt"; }

step "7. Performance benchmark"
PYTHONPATH=. python scripts/benchmark_dashboard.py >"$LOGS/benchmark.txt" 2>&1 && ok "reports/final/dashboard_benchmark.json" || { bad "benchmark"; tail -n 20 "$LOGS/benchmark.txt"; }

printf '\n'; [ "$FAIL" = 0 ] && echo "PHASE 10: ALL CHECKS PASSED" || echo "PHASE 10: FAILURES (see $LOGS)"
exit "$FAIL"
