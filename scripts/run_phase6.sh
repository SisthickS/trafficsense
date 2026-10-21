#!/usr/bin/env bash
# Phase 6 runner: analytical questions Q10-Q18 (descriptive, modeling, streaming) + tests + verification.
# Usage (from WSL):  bash scripts/run_phase6.sh [--quick] [--skip-tests]
#   default  full samples (~45-60 min: Q16 trains 4 MLlib models, Q17 9 ablation models, Q18 3 hold-out models)
#   --quick  small samples / fewer stream files (~15 min) - an end-to-end check, not for reporting
# Prerequisite: Phase 3 outputs. Stop Docker containers first (RAM).
set -euo pipefail
QUICK=(); RUN_TESTS=1
for a in "$@"; do
  case "$a" in --quick) QUICK=(--quick) ;; --skip-tests) RUN_TESTS=0 ;; *) echo "unknown option $a"; exit 2 ;; esac
done
. "$HOME/.trafficsense_env.sh"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
LOGS="$HOME/.cache/trafficsense/logs"; mkdir -p "$LOGS"
step(){ printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
started=$(date +%s)

step "1. Q10-Q18 ${QUICK[*]:-} (log: $LOGS/analytics_phase6.log)"
python -m src.analytics.run_all "${QUICK[@]}" --only q10 q11 q12 q13 q14 q15 q16 q17 q18 > "$LOGS/analytics_phase6.log" 2>&1 || true
grep -E " INFO .*(done in|finished)| ERROR " "$LOGS/analytics_phase6.log" | sed 's/^[0-9:]* //'

if [ "$RUN_TESTS" = 1 ]; then
  step "2. Tests (modeling + analytics)"
  python -m pytest -q -p no:cacheprovider tests/test_modeling.py tests/test_analytics.py 2>&1 | tail -2
fi

step "3. Verification"
python - <<'EOF'
import json, sys
from pathlib import Path
ok = True
def check(name, cond, info=""):
    global ok
    ok &= bool(cond)
    print(f"  [{'OK' if cond else 'FAIL':4}] {name}  {info}")

status = json.loads(Path("reports/analytics/run_status.json").read_text())
qs = [f"q{i}" for i in range(10, 19)]
check("Q10-Q18 all succeeded", all(status.get(q, {}).get("ok") for q in qs),
      ", ".join(f"{q}:{'ok' if status.get(q, {}).get('ok') else 'FAIL'}" for q in qs))
for q in qs:
    p = Path(f"reports/analytics/{q}.json")
    if not p.exists():
        check(f"{q} report", False); continue
    d = json.loads(p.read_text())
    tables = [Path("reports/analytics") / t for t in d["tables"].values()]
    figs = [Path("reports/analytics") / f for f in d["figures"]]
    check(f"{q}: {len(tables)} tables, {len(figs)} figures, {len(d['findings'])} findings, {len(d['implications'])} recommendations",
          tables and all(t.exists() and t.stat().st_size > 0 for t in tables) and figs and all(f.exists() for f in figs)
          and len(d["findings"]) >= 3 and len(d["implications"]) >= 2)

reg_path = Path("data/models/pems_bay/registry.json")
check("model registry written by Q16", reg_path.exists())
if reg_path.exists():
    reg = json.loads(reg_path.read_text())
    check("best model saved (Spark PipelineModel)", (Path(reg["model_path"]) / "metadata").exists(), reg["best_model_label"])
    check("validation/test predictions saved", (Path(reg["predictions_path"]) / "split=test").exists())
    check("threshold tuned on validation in (0, 1)", 0 < reg["threshold"] < 1, f"{reg['threshold']:.3f}")
    models = reg["models"]
    check("4 MLlib models + 2 baselines evaluated", len(models) == 6, ", ".join(models))
    best = models[reg["best_model"]]
    check("best model and persistence evaluated on test (with bootstrap CI)",
          "f1" in best["test"] and "f1" in models["persistence"]["test"],
          f"best {best['test']['f1']:.3f} vs persistence {models['persistence']['test']['f1']:.3f}")
    check("best model detects onsets (persistence cannot)", best["test"]["onset_recall"] > 0, f"{best['test']['onset_recall']:.3f}")
q17 = json.loads(Path("reports/analytics/q17.json").read_text()) if Path("reports/analytics/q17.json").exists() else {}
check("Q17 ablated every feature group", "top_group" in q17.get("metrics", {}))
q15 = json.loads(Path("reports/analytics/q15.json").read_text()) if Path("reports/analytics/q15.json").exists() else {}
m = q15.get("metrics", {})
check("Q15 stream faster than real time", m.get("realtime_factor", 0) > 1, f"{m.get('realtime_factor', 0):,.0f}x")
check("Q15 p95 latency below the 5-min horizon", 0 < m.get("latency_p95_s", 1e9) < 300, f"{m.get('latency_p95_s', float('nan')):.2f} s")
check("stream scratch cleaned up", not Path.home().joinpath(".cache/trafficsense/stream_bench").exists())
check("SUMMARY.md lists 18 questions", Path("reports/analytics/SUMMARY.md").read_text().count("\n## [Q") == 18)
sys.exit(0 if ok else 1)
EOF
rc=$?
echo; echo "Reports: reports/analytics/q10.md … q18.md, SUMMARY.md · model: data/models/pems_bay/"
echo "Phase 6 finished in $(( $(date +%s) - started ))s - $([ $rc = 0 ] && echo ALL CHECKS PASSED || echo SOME CHECKS FAILED)"
exit $rc
