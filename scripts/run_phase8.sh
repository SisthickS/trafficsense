#!/usr/bin/env bash
# Phase 8 runner: Kafka + MongoDB (docker compose) -> replay producer -> Spark Structured Streaming -> predictions in MongoDB,
# with parity verification, fault accounting, the restart/recovery test, live runs and a final verification.
# Usage (from WSL):  bash scripts/run_phase8.sh [--skip-tests] [--skip-recovery] [--skip-live]
#   full run ~25 min (recovery test ~7 min, two live runs ~2.5 min each). Needs ~4 GB free RAM besides Docker (Kafka 0.8 GB, MongoDB 0.5 GB).
# Prerequisites: Phase 3 outputs (clean/, features/, sensor_stats/, slot_profile/), Phase 7 model (data/models/pems_bay/ml_pipeline/best),
#                Docker with the compose plugin, internet on the first run (Spark Kafka connector + container images).
set -euo pipefail
TESTS=1; RECOVERY=1; LIVE=1
for a in "$@"; do
  case "$a" in --skip-tests) TESTS=0 ;; --skip-recovery) RECOVERY=0 ;; --skip-live) LIVE=0 ;; *) echo "unknown option $a"; exit 2 ;; esac
done
. "$HOME/.trafficsense_env.sh"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
OUT=reports/streaming; mkdir -p "$OUT/metrics"
step(){ printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
quiet(){ grep -vE "WARN|^\s*$|^%|Ivy|^\s+(confs|found|\||:)|downloading|SUCCESSFUL|resolution report|artifacts|retrieving|copied" || true; }
started=$(date +%s)

step "1. Infrastructure: docker compose up (Kafka KRaft + MongoDB)"
docker compose up -d --wait 2>&1 | tail -3
docker compose ps --format 'table {{.Name}}\t{{.Status}}'

if [ "$TESTS" = 1 ]; then
  step "2. Unit tests (streaming + feature refactor)"
  python -m pytest -q -p no:cacheprovider tests/test_streaming.py tests/test_features.py 2>&1 | tail -2
fi

step "3. Clean replay (6 h, unthrottled) -> stream (3,000-event micro-batches) -> parity with batch features / model / pandas windows"
python -m src.streaming.replay_events --hours 6 --speedup 0 --recreate-topic --summary-file $OUT/replay_clean.json 2>&1 | grep -E "unique_events|messages_sent|achieved_msgs|delivery_errors"
python -m src.streaming.traffic_stream --run parity --db ts_parity --reset --trigger available-now --max-offsets-per-trigger 3000 2>&1 | quiet | grep -E "INFO +trafficsense.stream (fresh|batch 0|stopped)|ERROR|Traceback" || true
python -m src.streaming.verify_stream parity --db ts_parity --hours 6 --out $OUT/parity.json 2>&1 | quiet || true

if [ "$RECOVERY" = 1 ]; then
  step "4. Restart / recovery test (kill -9, and crash between output write and commit) + fault accounting"
  python -m src.streaming.recovery_test 2>&1 | quiet | grep -vE "^\s+sent " || true
  python -m src.streaming.verify_stream accounting --db ts_rec_ref --summary $OUT/recovery_replay.json --out $OUT/accounting.json 2>&1 | quiet || true
fi

if [ "$LIVE" = 1 ]; then
  live(){  # $1 run name, $2 extra stream args, $3 report dir
    python -c "from src.streaming.kafka_tools import ensure_topic; ensure_topic(recreate=True)" 2>&1 | grep -v "^%" || true
    rm -f $OUT/metrics/progress_$1.jsonl
    : > /tmp/ts_$1.log
    python -m src.streaming.traffic_stream --run "$1" --db "ts_$1" --reset --trigger "processing:2 seconds" --duration 110 $2 > /tmp/ts_$1.log 2>&1 &
    until grep -q "started 4 queries" /tmp/ts_$1.log; do sleep 1; done
    python -m src.streaming.replay_events --hours 1 --speedup 60 --summary-file $OUT/replay_$1.json 2>&1 | grep -E "messages_sent|achieved_speedup|delivery_errors"
    sleep 10
    python -m src.streaming.streaming_metrics health --db "ts_$1" --max-age 60 | tee $OUT/health_$1.json || true
    wait
    python -m src.streaming.streaming_metrics report --db "ts_$1" --run "$1" --out "$3" > /dev/null 2>&1
  }
  step "5a. Live run, deterministic mode (replay at 60x real time, 2 s trigger)"
  live live "" $OUT
  step "5b. Live run, real-time mode (--idle-finalize-seconds 1.5)"
  live live_rt "--idle-finalize-seconds 1.5" $OUT/live_rt
fi

step "6. Verification"
rc=0
python - <<'EOF' || rc=$?
import json, subprocess, sys
from pathlib import Path
ok = True
def check(name, cond, info=""):
    global ok
    ok &= bool(cond)
    print(f"  [{'OK' if cond else 'FAIL':4}] {name}  {info}")
out = Path("reports/streaming")
ps = subprocess.run(["docker", "compose", "ps", "--format", "json"], capture_output=True, text=True).stdout
states = [json.loads(l) for l in ps.splitlines() if l.strip()]
check("Kafka and MongoDB containers healthy", len(states) >= 2 and all(s.get("Health") == "healthy" for s in states), ", ".join(f"{s['Service']}:{s.get('Health')}" for s in states))

def all_ok(path, label):
    p = out / path
    if not p.exists():
        check(label, False, f"{path} missing"); return
    c = json.loads(p.read_text())
    bad = [k for k, v in c.items() if not v["ok"]]
    check(label, not bad, f"{len(c)} checks" + (f", FAILED: {bad}" if bad else ""))
all_ok("parity.json", "stream == batch: features/model probabilities, coverage, windows")
if (out / "accounting.json").exists():
    all_ok("accounting.json", "fault accounting: dead letters, duplicates, late events")
if (out / "recovery_test.json").exists():
    r = json.loads((out / "recovery_test.json").read_text())
    n = sum(len(s["checks"]) for k, s in r["scenarios"].items() if k != "reference")
    check("restart/recovery test (kill -9 + crash before commit)", r["passed"], f"{n} checks, identical to the uninterrupted run")
for run, d in (("live", out), ("live_rt", out / "live_rt")):
    if not (d / "stream_metrics.json").exists():
        continue
    m = json.loads((d / "stream_metrics.json").read_text())
    q = m["queries"]
    check(f"{run}: all four queries reported progress", {"predictions", "sensor_windows", "network_windows", "dead_letters"} <= set(q), ", ".join(f"{k}:{v['batches_with_data']} batches" for k, v in q.items()))
    lat = m["end_to_end_latency"]["since_slot_complete"]
    warm = m["end_to_end_latency"].get("since_slot_complete_after_warmup", lat)
    check(f"{run}: prediction latency well inside the 5-minute horizon", lat["p95_s"] < 60 and lat["share_under_5_min_horizon"] == 1.0,
          f"all slots p50 {lat['p50_s']:.1f}s p95 {lat['p95_s']:.1f}s; after warm-up p50 {warm['p50_s']:.1f}s p95 {warm['p95_s']:.1f}s ({m['end_to_end_latency']['finalized_by']})")
    check(f"{run}: no backlog at the end, health OK", q["predictions"]["final_backlog_events"] in (0, None) and json.loads((out / f"health_{run}.json").read_text())["healthy"],
          f"final backlog {q['predictions']['final_backlog_events']} events")
    check(f"{run}: metrics report + chart written", (d / "stream_metrics.md").exists() and (d / "stream_metrics.png").exists())
sys.exit(0 if ok else 1)
EOF
echo; echo "Reports: reports/streaming/{recovery_test,stream_metrics}.md, parity.json, accounting.json, live_rt/"
echo "Phase 8 finished in $(( $(date +%s) - started ))s - $([ $rc = 0 ] && echo ALL CHECKS PASSED || echo SOME CHECKS FAILED)"
exit $rc
