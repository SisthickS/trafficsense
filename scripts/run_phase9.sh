#!/usr/bin/env bash
# Phase 9 runner: MongoDB (docker compose) -> schema + data load -> tests -> end-to-end API verification (real server processes).
# Usage (from WSL):  bash scripts/run_phase9.sh [--skip-tests] [--seed-predictions] [--no-inference]
#   ~5 min.  --seed-predictions: if `traffic_predictions` is empty, start Kafka, replay 6 h and run the streaming job into MongoDB (~3 min extra)
#   --no-inference: skip the Spark-backed POST /models/predict checks (saves ~1 min)
# Prerequisites: Phase 3 (processed data), Phase 5/6 (reports/analytics), Phase 7 (model + reports/ml), Phase 8 (reports/streaming), Docker.
set -euo pipefail
TESTS=1; SEED=0; INFER=1
for a in "$@"; do
  case "$a" in --skip-tests) TESTS=0 ;; --seed-predictions) SEED=1 ;; --no-inference) INFER=0 ;; *) echo "unknown option $a"; exit 2 ;; esac
done
. "$HOME/.trafficsense_env.sh"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
step(){ printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
quiet(){ grep -vE "WARN|^\s*$|^%|Deprecat|testclient" || true; }
started=$(date +%s)

step "1. MongoDB (docker compose)"
docker compose up -d --wait mongo 2>&1 | tail -n 2
python - <<'EOF'
from src.mongo.connection import make_client
c = make_client(); print("  MongoDB", c.server_info()["version"], "databases:", [d for d in c.list_database_names() if d not in ("admin", "config", "local")])
EOF

step "2. Schema (collections, validators, indexes) + load of the pipeline outputs"
python -m src.mongo.load_outputs 2>&1 | quiet

if [ "$SEED" = 1 ]; then
  n=$(python -c "from src.mongo.connection import get_db; print(get_db().traffic_predictions.estimated_document_count())")
  if [ "$n" = 0 ]; then
    step "2b. Seed traffic_predictions with a streaming run (Kafka -> Spark -> MongoDB)"
    docker compose up -d --wait kafka 2>&1 | tail -n 1
    python -m src.streaming.replay_events --hours 6 --speedup 0 --recreate-topic 2>&1 | grep -E "messages_sent|delivery_errors" || true
    python -m src.streaming.traffic_stream --run "api_seed_$(date +%s)" --trigger available-now --max-offsets-per-trigger 3000 2>&1 | grep -E "stopped|ERROR|Traceback" || true
    docker compose stop kafka 2>&1 | tail -n 1
  else
    echo "  traffic_predictions already holds $n documents"
  fi
fi

if [ "$TESTS" = 1 ]; then
  step "3. Tests: Mongo layer, API (CRUD, validation, auth, errors, persistence, real-model inference), streaming (collection rename)"
  python -m pytest -q -p no:cacheprovider tests/test_mongo.py tests/test_api.py tests/test_streaming.py 2>&1 | quiet | tail -n 4
fi

step "4. End-to-end verification (real uvicorn processes, kill -9, MongoDB outage, loader re-run, inference)"
rc=0
python -m src.api.verify_api $([ "$INFER" = 0 ] && echo --skip-inference) 2>&1 | quiet || rc=$?

echo; echo "Reports: reports/api/verification.md, reports/api/openapi.json"
echo "Run the API:  uvicorn src.api.main:app --host 127.0.0.1 --port 8000   (docs at http://127.0.0.1:8000/docs)"
echo "Phase 9 finished in $(( $(date +%s) - started ))s - $([ $rc = 0 ] && echo ALL CHECKS PASSED || echo SOME CHECKS FAILED)"
exit $rc
