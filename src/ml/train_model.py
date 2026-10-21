"""Train and tune the three MLlib pipelines chronologically, select the optimal one, serialize it.

    python -m src.ml.train_model [--quick] [--train-fraction 0.08] [--cost-fn 10] [--cost-fp 1]

Chronological protocol (no random folds, no look-ahead):
  * every candidate is fitted on a class-weighted random sample of the TRAIN period (Jan - early May 2017);
  * hyper-parameters are chosen on the VALIDATION period that follows it (per algorithm: highest PR-AUC);
  * the overall optimal model is the one with the lowest expected cost on validation at its own cost-optimal threshold
    (default cost: missed congestion = 10, false alarm = 1);
  * the TEST period (last 5 weeks) is untouched here - evaluate_model.py reads it once.
Outputs: data/models/<ds>/ml_pipeline/{logistic_regression,decision_tree,random_forest,best}/, selection.json,
reports/ml/training.{md,json}, reports/ml/tables/tuning_*.csv, reports/figures/ml/tuning.png.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time

import pandas as pd
from pyspark.sql import functions as F

from src.analytics.common import start_spark
from src.ml.config import ALGORITHMS, ML_MODEL_DIR, SELECTION_FILE
from src.ml.costs import CostModel, profile
from src.ml.model_io import save_model, update_metadata, verify_reload
from src.ml.pipeline import NUMERIC, build_pipeline, make_estimator, param_grids, prepare
from src.ml.reporting import df_to_md, log, plt_setup, save_fig, setup_logging, write_report, write_table
from src.modeling import evaluation as ev
from src.modeling.data import cached_split, training_sample
from src.modeling.models import score


def null_profile(train) -> pd.DataFrame:
    """Missing / NaN count per numeric feature in the training sample (what the Imputer actually repairs)."""
    row = train.select([F.sum((F.col(c).isNull() | F.isnan(F.col(c).cast("double"))).cast("int")).alias(c) for c in NUMERIC]).first()
    return pd.DataFrame({"feature": NUMERIC, "missing": [int(row[c] or 0) for c in NUMERIC]}).sort_values("missing", ascending=False)


def _fit(algo: str, params: dict, train):
    t0 = time.time()
    return build_pipeline(make_estimator(algo, params)).fit(train), time.time() - t0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--quick", action="store_true", help="1%% sample and a small grid (end-to-end check, not for reporting)")
    ap.add_argument("--train-fraction", type=float, default=None, help="share of the train period sampled (default 0.08, quick 0.01)")
    ap.add_argument("--cost-fn", type=float, default=10.0, help="cost of a missed congestion")
    ap.add_argument("--cost-fp", type=float, default=1.0, help="cost of a false alarm")
    a = ap.parse_args(argv)
    cm = CostModel(a.cost_fn, a.cost_fp)
    frac = a.train_fraction or (0.01 if a.quick else 0.08)
    setup_logging()
    t_start = time.time()
    spark = start_spark("trafficsense-ml-train")
    cache: dict = {}
    check = {"ok": False}
    try:
        val = prepare(cached_split(spark, "val", cache))
        base, info = training_sample(spark, frac)
        train = prepare(base)
        nulls = null_profile(train)
        log.info("train sample: %s rows, %.2f%% positive; numeric features with missing values: %d",
                 f"{info['rows']:,}", 100 * info["positive_rate"], int((nulls.missing > 0).sum()))

        rows, best = [], {}
        for algo, combos in param_grids(a.quick).items():
            for params in combos:
                pm, secs = _fit(algo, params, train)
                p = profile(ev.histogram(score(pm, val)), cm)
                rows.append({"algorithm": algo, "params": json.dumps(params), "train_s": secs,
                             "val_pr_auc": p["pr_auc"], "val_roc_auc": p["roc_auc"], "val_f1": p["f1_point"]["f1"],
                             "val_cost_per_1000": p["cost_point"]["cost_per_1000"], "cost_threshold": p["cost_point"]["threshold"]})
                log.info("%-20s %-45s PR-AUC %.4f  F1 %.4f  cost/1000 %.2f  (%.0fs)", algo, params, p["pr_auc"],
                         p["f1_point"]["f1"], p["cost_point"]["cost_per_1000"], secs)
                if algo not in best or p["pr_auc"] > best[algo]["profile"]["pr_auc"]:
                    best[algo] = {"model": pm, "params": params, "profile": p, "train_s": secs}
        base.unpersist()

        cand = pd.DataFrame(rows)
        cand["selected"] = [json.dumps(best[r.algorithm]["params"]) == r.params for r in cand.itertuples()]

        selection = {"cost_model": {"fn": cm.fn, "fp": cm.fp}, "train_fraction": frac, "train_sample": info,
                     "grid_candidates": len(cand), "algorithms": {}}
        for algo, b in best.items():
            p = b["profile"]
            path = ML_MODEL_DIR / algo
            meta = {"algorithm": algo, "label": ALGORITHMS[algo], "params": b["params"], "train_seconds": b["train_s"],
                    "threshold": p["cost_point"]["threshold"], "threshold_f1": p["f1_point"]["threshold"],
                    "threshold_rule": f"minimum expected cost on validation (FN={cm.fn:g}, FP={cm.fp:g})",
                    "cost_model": {"fn": cm.fn, "fp": cm.fp}, "train_fraction": frac, "train_rows": info["rows"],
                    "validation": {"pr_auc": p["pr_auc"], "roc_auc": p["roc_auc"], "f1_point": p["f1_point"], "cost_point": p["cost_point"]}}
            save_model(b["model"], path, meta, reference=val)
            selection["algorithms"][algo] = {"path": str(path), "params": b["params"], "threshold": meta["threshold"],
                                             "threshold_f1": meta["threshold_f1"], **meta["validation"]}
            log.info("saved %s -> %s", algo, path)

        optimal = min(best, key=lambda k: (best[k]["profile"]["cost_point"]["cost_per_1000"], -best[k]["profile"]["pr_auc"]))
        best_path = ML_MODEL_DIR / "best"
        if best_path.exists():
            shutil.rmtree(best_path)
        shutil.copytree(ML_MODEL_DIR / optimal, best_path)
        update_metadata(best_path, role="best", selected_from=sorted(best))
        selection.update(best_algorithm=optimal, best_path=str(best_path))

        check = verify_reload(best_path, val)
        log.info("reload check of best model: %s", "OK" if check["ok"] else f"FAILED {check}")
        selection["reload_check"] = check
        SELECTION_FILE.write_text(json.dumps(selection, indent=2, default=str) + "\n")

        _report(cand, nulls, selection, info, cm, frac, time.time() - t_start, a.quick)
    finally:
        for df in cache.values():
            df.unpersist()
        spark.stop()
    return 0 if check["ok"] else 1


def tuning_figure(cand: pd.DataFrame, cm: CostModel) -> str:
    plt, sns = plt_setup()
    cand = cand.copy()
    cand["config"] = cand.algorithm.str.replace("_", " ") + " " + cand.params.str.replace('"', "").str.strip("{}")
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.2))
    for ax, col, title in ((axes[0], "val_pr_auc", "Validation PR-AUC (higher is better)"),
                           (axes[1], "val_cost_per_1000", f"Validation cost per 1,000 readings (FN={cm.fn:g}, FP={cm.fp:g}; lower is better)")):
        sns.barplot(data=cand, y="config", x=col, hue="algorithm", dodge=False, ax=ax, legend=False)
        for patch, flag in zip(ax.patches, cand.selected):
            if flag:
                patch.set_edgecolor("black")
                patch.set_linewidth(2)
        ax.set_title(title, fontsize=10)
        ax.set_ylabel("")
    lo = cand.val_pr_auc.min()
    axes[0].set_xlim(max(0.0, lo - 0.02), min(1.0, cand.val_pr_auc.max() + 0.01))
    axes[1].set_yticklabels([])
    fig.suptitle("Chronological tuning: fit on the train period, score on the following validation period (selected = outlined)")
    return save_fig(fig, "tuning")


def _report(cand: pd.DataFrame, nulls: pd.DataFrame, sel: dict, info: dict, cm: CostModel, frac: float, seconds: float,
            quick: bool) -> None:
    fig_ref = tuning_figure(cand, cm)
    out = cand.copy()
    t_ref = write_table(out, "tuning_candidates")
    n_ref = write_table(nulls, "tuning_missing_values")

    best = sel["best_algorithm"]
    per_algo = pd.DataFrame([{"algorithm": ALGORITHMS[k], "params": json.dumps(v["params"]), "val_pr_auc": v["pr_auc"],
                              "val_roc_auc": v["roc_auc"], "val_f1": v["f1_point"]["f1"], "cost_threshold": v["threshold"],
                              "val_cost_per_1000": v["cost_point"]["cost_per_1000"], "optimal": k == best}
                             for k, v in sel["algorithms"].items()])
    chk = sel["reload_check"]
    quick_note = "**QUICK RUN - small sample and grid; not for reporting.**\n\n" if quick else ""
    missing_md = (df_to_md(nulls[nulls.missing > 0], floatfmt=0) if (nulls.missing > 0).any() else
                  "No numeric feature has missing values in the training sample: Phase 3 already drops rows with unobserved inputs, "
                  "so the Imputer is a guard for inference-time data (exercised by `tests/test_ml.py`).")
    md = f"""# Phase 7 - MLlib pipeline training and tuning

{quick_note}## Pipeline (7 stages)

`Imputer (median, {len(NUMERIC)} numeric features)` → `StringIndexer (hour_cat, dow_cat, speed_state)` → `OneHotEncoder` →
`VectorAssembler (numeric)` → `StandardScaler (mean 0, sd 1)` → `VectorAssembler (scaled numeric + one-hot → features)` →
`Estimator` (logistic regression | decision tree | random forest, class-weighted).

## Protocol

- Training data: random {frac:.0%} sample of the **training period only** ({info['rows']:,} rows, {100 * info['positive_rate']:.2f}% congested next slot),
  class weights w₊ = {info['w_pos']:.1f}, w₋ = {info['w_neg']:.2f} so both classes carry equal total weight. Imputation medians, scaler
  statistics and category indices are learned on this sample only.
- Tuning is chronological: each of the {sel['grid_candidates']} grid candidates is fitted on the train period and scored on the **following**
  validation period (no random folds). Best hyper-parameters per algorithm = highest validation PR-AUC (threshold-free).
- Optimal model = lowest validation cost per 1,000 readings at its own cost-optimal threshold, with cost(missed congestion) = {cm.fn:g}
  and cost(false alarm) = {cm.fp:g} (break-even probability {cm.breakeven_probability:.3f}; configurable with `--cost-fn/--cost-fp`).
- The test period is not used in this step.

## Selected model per algorithm (validation)

{df_to_md(per_algo, floatfmt=4)}

**Optimal: {ALGORITHMS[best]}** ({json.dumps(sel['algorithms'][best]['params'])}), operating threshold {sel['algorithms'][best]['threshold']:.3f}.

## All candidates

{df_to_md(out, max_rows=20, floatfmt=4)}

![tuning]({fig_ref})

## Missing values repaired by the Imputer (training sample)

{missing_md}

Tables: [{t_ref}]({t_ref}), [{n_ref}]({n_ref}).

## Serialization check

Saved bundles under `{ML_MODEL_DIR}` (one per algorithm plus `best/`). Reload check of `best/` on validation rows:
{'**PASSED**' if chk['ok'] else '**FAILED**'} - {chk['reference_rows']} reference rows, max |Δp| = {chk['max_abs_probability_diff']:.1e}, checks: {json.dumps(chk['checks'])}.

Total training time {seconds / 60:.1f} min.
"""
    write_report("training", md, {"selection": sel, "candidates": out.to_dict("records"), "seconds": seconds, "quick": quick})


if __name__ == "__main__":
    sys.exit(main())
