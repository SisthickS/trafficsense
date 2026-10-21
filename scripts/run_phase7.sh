#!/usr/bin/env bash
# Phase 7 runner: 7-stage MLlib pipeline - tune (chronological), evaluate (precision/recall/F1/ROC-AUC/PR-AUC/confusion/cost),
# serialize + reload, tests and verification.
# Usage (from WSL):  bash scripts/run_phase7.sh [--quick] [--skip-tests] [--cost-fn 10] [--cost-fp 1]
#   default  8% training sample, 11-candidate grid (~35-45 min)
#   --quick  1% sample, 6-candidate grid (~12 min) - an end-to-end check, not for reporting
# Prerequisite: Phase 3 outputs (features/). Stop Docker containers first (RAM).
set -euo pipefail
QUICK=(); COST=(); RUN_TESTS=1
while [ $# -gt 0 ]; do
  case "$1" in
    --quick) QUICK=(--quick) ;;
    --skip-tests) RUN_TESTS=0 ;;
    --cost-fn|--cost-fp) COST+=("$1" "$2"); shift ;;
    *) echo "unknown option $1"; exit 2 ;;
  esac
  shift
done
. "$HOME/.trafficsense_env.sh"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
LOGS="$HOME/.cache/trafficsense/logs"; mkdir -p "$LOGS"
step(){ printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
started=$(date +%s)

if [ "$RUN_TESTS" = 1 ]; then
  step "1. Unit tests (pipeline, costs, model_io)"
  python -m pytest -q -p no:cacheprovider tests/test_ml.py 2>&1 | tail -3
fi

step "2. Train + tune ${QUICK[*]:-} (log: $LOGS/ml_train.log)"
python -m src.ml.train_model "${QUICK[@]}" "${COST[@]}" > "$LOGS/ml_train.log" 2>&1 || true
grep -E " INFO .*(PR-AUC|saved|reload|train sample)| ERROR |Error" "$LOGS/ml_train.log" | sed 's/^[0-9:]* //' || true

step "3. Evaluate on the test period (log: $LOGS/ml_eval.log)"
python -m src.ml.evaluate_model "${COST[@]}" > "$LOGS/ml_eval.log" 2>&1 || true
grep -E " INFO .*(scored|reload|wrote)| ERROR |Error" "$LOGS/ml_eval.log" | sed 's/^[0-9:]* //' || true

step "4. Verification (fresh process: reload the saved best model and run inference)"
rc=0
python - <<'EOF' || rc=$?
import json, sys
from pathlib import Path
ok = True
def check(name, cond, info=""):
    global ok
    ok &= bool(cond)
    print(f"  [{'OK' if cond else 'FAIL':4}] {name}  {info}")

from src.ml.config import ML_MODEL_DIR, SELECTION_FILE, ML_REPORT_DIR
check("selection.json written", SELECTION_FILE.exists())
sel = json.loads(SELECTION_FILE.read_text()) if SELECTION_FILE.exists() else {}
algos = ["logistic_regression", "decision_tree", "random_forest"]
check("3 algorithm bundles + best/ saved", all((ML_MODEL_DIR / a / "model" / "metadata").exists() for a in algos + ["best"]))
check("tuning candidates", sel.get("grid_candidates", 0) >= 6, str(sel.get("grid_candidates")))
val_cost = {a: sel["algorithms"][a]["cost_point"]["cost_per_1000"] for a in algos} if sel else {}
check("best = lowest validation cost", sel and sel["best_algorithm"] == min(val_cost, key=val_cost.get), f"{sel.get('best_algorithm')} {val_cost}")
check("train-time reload check passed", sel.get("reload_check", {}).get("ok"))

ev = json.loads((ML_REPORT_DIR / "evaluation.json").read_text()) if (ML_REPORT_DIR / "evaluation.json").exists() else {}
check("evaluation report written", bool(ev))
if ev:
    check("eval-time reload check passed", ev["reload_check"]["ok"], f"max|dp|={ev['reload_check']['max_abs_probability_diff']:.1e}")
    tm = [r for r in ev["test_metrics"] if r["split"] == "test"]
    check("metrics cover 5 models x 2 operating points", len(tm) == 10)
    check("precision/recall/F1/ROC-AUC/PR-AUC in [0,1]", all(0 <= r[k] <= 1 for r in tm for k in ("precision", "recall", "f1", "roc_auc", "pr_auc")))
    cms = [r for r in ev["confusion_matrices"] if r["split"] == "test"]
    check("confusion matrices sum to the same test rows", len({round(r["tp"] + r["fp"] + r["fn"] + r["tn"]) for r in cms}) == 1,
          f"{round(cms[0]['tp'] + cms[0]['fp'] + cms[0]['fn'] + cms[0]['tn']):,} rows")
    cost_rows = {r["model"]: r for r in ev["cost_bootstrap"]}
    check("cost comparison vs persistence with CI", {"logistic_regression", "decision_tree", "random_forest"} <= set(cost_rows))
    check("figures + tables exist", all((ML_REPORT_DIR / f).exists() for f in ev["figures"]) and len(list((ML_REPORT_DIR / "tables").glob("*.csv"))) >= 6)
    check("findings and recommendations present", len(ev["findings"]) >= 4 and len(ev["recommendations"]) >= 2)

# independent reload: new Spark session, load best/, score real test rows
from pyspark.sql import functions as F
from src.common.spark import get_spark
from src.ml.model_io import load_model, predict
from src.modeling.data import load_split
spark = get_spark("trafficsense-ml-verify", driver_memory="2g", shuffle_partitions=8)
try:
    bundle = load_model(ML_MODEL_DIR / "best")
    check("reloaded best/ (7 stages)", len(bundle.model.stages) == 7, f"{bundle.metadata['label']}, threshold {bundle.threshold:.3f}")
    day = load_split(spark, "test").filter(F.col("date_local") == F.lit("2017-06-14").cast("date"))
    scored = predict(bundle, day).select("p", "alert", "label").toPandas()
    check("inference on the reloaded model (one test day)", len(scored) > 50000 and scored.p.between(0, 1).all(), f"{len(scored):,} rows, alert rate {scored.alert.mean():.3f}")
    recall = scored.loc[scored.label == 1, "alert"].mean()
    check("reloaded model alerts on most real congestion (sanity)", recall > 0.8, f"recall {recall:.3f}")
finally:
    spark.stop()
sys.exit(0 if ok else 1)
EOF
echo; echo "Reports: reports/ml/training.md, reports/ml/evaluation.md · models: data/models/pems_bay/ml_pipeline/"
echo "Phase 7 finished in $(( $(date +%s) - started ))s - $([ $rc = 0 ] && echo ALL CHECKS PASSED || echo SOME CHECKS FAILED)"
exit $rc
