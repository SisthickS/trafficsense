#!/usr/bin/env bash
# Phase 5 runner: Scala RDD demonstration + analytical questions Q1-Q9 + tests + verification.
# Usage (from WSL):  bash scripts/run_phase5.sh [--skip-scala] [--skip-tests]
# Prerequisite: Phase 3 outputs (clean/, features/, sensor_stats/). Stop Docker containers first (RAM).
set -euo pipefail
RUN_SCALA=1; RUN_TESTS=1
for a in "$@"; do
  case "$a" in --skip-scala) RUN_SCALA=0 ;; --skip-tests) RUN_TESTS=0 ;; *) echo "unknown option $a"; exit 2 ;; esac
done
. "$HOME/.trafficsense_env.sh"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
LOGS="$HOME/.cache/trafficsense/logs"; mkdir -p "$LOGS"
step(){ printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
started=$(date +%s)

if [ "$RUN_SCALA" = 1 ]; then
  step "1. Scala RDD module: sbt test package + spark-submit"
  bash scripts/run_scala_rdd.sh
fi

step "2. Analytical questions Q1-Q9 (log: $LOGS/analytics.log)"
rm -rf reports/analytics reports/figures/analytics
python -m src.analytics.run_all > "$LOGS/analytics.log" 2>&1 || true
grep -E " INFO .*(done in|finished)| ERROR " "$LOGS/analytics.log" | sed 's/^[0-9:]* //'

if [ "$RUN_TESTS" = 1 ]; then
  step "3. Python tests (analytics)"
  python -m pytest -q -p no:cacheprovider tests/test_analytics.py 2>&1 | tail -2
fi

step "4. Verification"
python - <<'EOF'
import json, sys
from pathlib import Path
ok = True
def check(name, cond, info=""):
    global ok
    ok &= bool(cond)
    print(f"  [{'OK' if cond else 'FAIL':4}] {name}  {info}")

sc = Path("reports/scala_rdd/scala_rdd_report.json")
if sc.exists():
    m = json.loads(sc.read_text())
    check("Scala: flatMap produced 52,116 x 325 readings", m["observations"] == 16_937_700, f"{m['observations']:,}")
    check("Scala: accumulator zero readings = 521 (matches Phase 2 validation)", m["zero_readings"] == 521)
    check("Scala: aggregateByKey == combineByKey means", m["aggregate_vs_combine_max_diff"] < 1e-6, f"{m['aggregate_vs_combine_max_diff']:.1e}")
    check("Scala: TrafficStatePartitioner partitions are pure", m["state_partition_pure"])
    check("Scala: co-partitioned reduceByKey has no ShuffleDependency", not m["partitioned_reduce_is_shuffle"] and m["plain_reduce_is_shuffle"])
    check("Scala: frequency partitioner balances better than hash", m["freq_max_over_mean"] < m["hash_max_over_mean"],
          f"{m['freq_max_over_mean']:.3f} vs {m['hash_max_over_mean']:.3f}")
    check("Scala: both partitioners find identical episodes", m["partitioners_agree"], f"{m['episodes']:,} episodes")
    check("Scala: RDD checkpointed", m["checkpointed"])
    check("Scala: saveAsTextFile output (325 rows)", len(Path("reports/scala_rdd/sensor_profiles_csv/part-00000").read_text().splitlines()) == 325)
else:
    check("Scala report present", False, "(run without --skip-scala)")

status = json.loads(Path("reports/analytics/run_status.json").read_text())
check("Analytics: Q1-Q9 all succeeded", len(status) == 9 and all(s["ok"] for s in status.values()),
      ", ".join(f"{q}:{'ok' if s['ok'] else 'FAIL'}" for q, s in status.items()))
for q in sorted(status, key=lambda x: int(x[1:])):
    d = json.loads(Path(f"reports/analytics/{q}.json").read_text())
    tables = [Path("reports/analytics") / t for t in d["tables"].values()]
    figs = [Path("reports/analytics") / f for f in d["figures"]]
    check(f"{q}: {len(tables)} tables, {len(figs)} figures, {len(d['findings'])} findings",
          tables and all(t.exists() and t.stat().st_size > 0 for t in tables) and len(figs) >= 1
          and all(f.exists() for f in figs) and len(d["findings"]) >= 3)
check("Analytics SUMMARY.md lists 9 questions", Path("reports/analytics/SUMMARY.md").read_text().count("\n## [Q") == 9)
sys.exit(0 if ok else 1)
EOF
rc=$?
echo; echo "Reports: reports/scala_rdd/scala_rdd_report.md · reports/analytics/SUMMARY.md · figures: reports/figures/analytics/"
echo "Phase 5 finished in $(( $(date +%s) - started ))s - $([ $rc = 0 ] && echo ALL CHECKS PASSED || echo SOME CHECKS FAILED)"
exit $rc
