"""Q16 - Five-minute-ahead congestion prediction: comparative metrics.

Trains Spark MLlib models on a class-weighted random sample of the TRAINING period, tunes each decision threshold on
VALIDATION (max F1), and reports TEST metrics against two baselines. Saves the best model, its validation/test
predictions and a model registry used by Q15, Q17 and Q18.

Usage: python -m src.analytics.q16_prediction_metrics
"""
from __future__ import annotations

import sys
import time

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.analytics.common import AnalyticsContext, QuestionResult, pct, plt_setup, run_single
from src.modeling import evaluation as ev
from src.modeling.data import (
    EVAL_COLUMNS, cached_split, model_dir, predictions_dir, training_sample,
)
from src.modeling.models import BASELINES, PROB, fit, importances, model_specs, save_json, score, score_baseline
from src.preprocessing.build_features import FEATURE_COLUMNS, FEATURES, LABEL

REGISTRY = "registry.json"
METRIC_COLS = ["precision", "recall", "f1", "pr_auc", "roc_auc", "onset_recall", "clearance_accuracy",
               "transition_accuracy", "false_alarm_rate", "threshold"]


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q16", "Five-minute-ahead congestion prediction: comparative metrics",
                       "How well do Spark MLlib models predict whether a sensor will be congested in the next 5 minutes, "
                       "compared with simple baselines, and where do they add value?")
    spark = ctx.spark
    ctx.release()
    frac = ctx.pick(0.08, 0.01)
    val = cached_split(spark, "val", ctx._cache)
    test = cached_split(spark, "test", ctx._cache)
    train, info = training_sample(spark, frac)
    specs = model_specs(ctx.quick)
    r.method = [
        f"Training data: random {frac:.0%} sample of the **training period only** ({info['rows']:,} rows, "
        f"{pct(info['positive_rate'])} positive), class-weighted so both classes carry equal weight "
        f"(w₊ = {info['w_pos']:.1f}, w₋ = {info['w_neg']:.2f}). All {len(FEATURE_COLUMNS)} Phase-3 features.",
        "Models (Spark MLlib Pipeline: VectorAssembler → classifier): " + "; ".join(
            f"{s.label} {s.params}" for s in specs.values()) + ".",
        "Baselines: **persistence** (next state = current state) and **historical profile** (train-period congestion "
        "rate of this sensor at the target's weekday and slot).",
        "Threshold for each model = maximum F1 on **validation**, then applied unchanged to **test** "
        "(persistence uses its natural 0/1 output).",
        "Evaluation from one Spark aggregation per model and split (counts per day × current state × label × "
        f"{ev.BINS} probability buckets): precision, recall, F1, PR-AUC (average precision), ROC-AUC, onset recall, "
        "clearance accuracy, transition accuracy and false-alarm rate. Test-F1 confidence intervals come from a "
        "day-block bootstrap (1,000 resamples of test days).",
    ]

    rows, hists, fitted, timing = [], {}, {}, {}
    for name, (label, _) in BASELINES.items():
        hists[name] = {"val": ev.histogram(score_baseline(name, val)), "test": ev.histogram(score_baseline(name, test))}
        timing[name] = {"label": label, "train_s": 0.0, "kind": "baseline"}
    for name, spec in specs.items():
        model, secs = fit(spec, train, FEATURE_COLUMNS)
        t0 = time.time()
        hv = ev.histogram(score(model, val))
        ht = ev.histogram(score(model, test))
        score_s = time.time() - t0
        hists[name] = {"val": hv, "test": ht}
        fitted[name] = model
        timing[name] = {"label": spec.label, "train_s": secs, "kind": "model",
                        "score_rows_per_s": (hv.n.sum() + ht.n.sum()) / score_s}
        r.metrics[f"{name}_train_s"] = secs
    train.unpersist()

    thresholds, val_m, test_m = {}, {}, {}
    for name, h in hists.items():
        b = ev.BINS // 2 if name == "persistence" else ev.best_bucket(ev.curve(h["val"]))
        thresholds[name] = b
        val_m[name] = ev.metrics(h["val"], b)
        test_m[name] = ev.metrics(h["test"], b)
    model_names = list(specs)
    best = max(model_names, key=lambda m: (val_m[m]["f1"], val_m[m]["pr_auc"]))

    boot = ev.bootstrap_f1({m: ev.per_day(h["test"], thresholds[m]) for m, h in hists.items()}, "persistence")
    comp = []
    for name in list(BASELINES) + model_names:
        for split, m in (("val", val_m[name]), ("test", test_m[name])):
            comp.append({"model": timing[name]["label"], "key": name, "split": split, **{k: m[k] for k in METRIC_COLS},
                         "train_s": timing[name]["train_s"], "rows": m["rows"]})
    comp = pd.DataFrame(comp)
    test_tab = comp[comp.split == "test"].drop(columns=["split"]).merge(
        boot[["model", "f1_ci_low", "f1_ci_high", "delta_vs_persistence", "delta_ci_low", "delta_ci_high", "share_boot_better"]]
        .rename(columns={"model": "key"}), on="key").sort_values("f1", ascending=False)

    # ---- persist the best model, its predictions and the registry --------------------------------------------
    mdir = model_dir()
    best_path = mdir / "best_model"
    fitted[best].write().overwrite().save(str(best_path))
    pred_path = predictions_dir() / "best"
    score(fitted[best], val).withColumn("split", F.lit("val")).unionByName(
        score(fitted[best], test).withColumn("split", F.lit("test"))) \
        .write.mode("overwrite").partitionBy("split").parquet(str(pred_path))
    imps = {m: importances(fitted[m], FEATURE_COLUMNS) for m in model_names}
    registry = {
        "best_model": best, "best_model_label": timing[best]["label"], "model_path": str(best_path),
        "predictions_path": str(pred_path), "threshold": thresholds[best] / ev.BINS, "threshold_bucket": thresholds[best],
        "bins": ev.BINS, "feature_columns": FEATURE_COLUMNS, "label": LABEL, "train_sample": info,
        "models": {m: {"label": timing[m]["label"], "params": specs[m].params if m in specs else {},
                       "threshold": thresholds[m] / ev.BINS, "train_seconds": timing[m]["train_s"],
                       "val": {k: val_m[m][k] for k in METRIC_COLS}, "test": {k: test_m[m][k] for k in METRIC_COLS}}
                   for m in hists},
        "importances": imps, "generated_by": "src/analytics/q16_prediction_metrics.py",
    }
    save_json(mdir / REGISTRY, registry)

    # ---- findings ---------------------------------------------------------------------------------------------
    bt, pt, ht_ = test_m[best], test_m["persistence"], test_m["historical_profile"]
    bb = boot.set_index("model").loc[best]
    significant = bb.delta_ci_low > 0
    ranking = test_tab[test_tab.key.isin(model_names)][["model", "f1"]]
    r.findings = [
        f"Best model on validation: **{timing[best]['label']}**. Test F1 **{bt['f1']:.3f}** (95% CI {bb.f1_ci_low:.3f}–"
        f"{bb.f1_ci_high:.3f}), PR-AUC {bt['pr_auc']:.3f}, ROC-AUC {bt['roc_auc']:.3f}, precision {bt['precision']:.3f}, "
        f"recall {bt['recall']:.3f} at threshold {thresholds[best] / ev.BINS:.3f}.",
        (f"It beats persistence (F1 {pt['f1']:.3f}) by **{bb.delta_vs_persistence:+.3f}** F1 (95% CI {bb.delta_ci_low:+.3f} "
         f"to {bb.delta_ci_high:+.3f}; better in {pct(bb.share_boot_better)} of bootstrap resamples), a statistically "
         "clear gain.") if significant else
        (f"It does **not** clearly beat persistence (F1 {pt['f1']:.3f}): difference {bb.delta_vs_persistence:+.3f} with 95% CI "
         f"{bb.delta_ci_low:+.3f} to {bb.delta_ci_high:+.3f}. At a 5-minute horizon the current state is already very informative."),
        f"The value is in **state changes**: the model catches {pct(bt['onset_recall'])} of congestion onsets and correctly "
        f"predicts {pct(bt['clearance_accuracy'])} of clearances (persistence: 0% and 0% by construction); transition "
        f"accuracy {pct(bt['transition_accuracy'])}, at a false-alarm rate of {pct(bt['false_alarm_rate'], 2)} on stable free-flow rows.",
        f"The historical-profile baseline alone reaches F1 {ht_['f1']:.3f} (PR-AUC {ht_['pr_auc']:.3f}): recurrence helps, "
        "but live state dominates at 5 minutes.",
        "Model ranking by test F1: " + ", ".join(f"{m} {f:.3f}" for m, f in ranking.itertuples(index=False)) +
        f". Training on {info['rows']:,} rows took " + ", ".join(
            f"{timing[m]['label']} {timing[m]['train_s']:.0f}s" for m in model_names) + ".",
    ]
    trans_best = max(model_names, key=lambda m: test_m[m]["transition_accuracy"])
    if trans_best != best:
        tb = test_m[trans_best]
        r.findings.append(
            f"Trade-off: **{timing[trans_best]['label']}** handles state changes best (transition accuracy "
            f"{pct(tb['transition_accuracy'])} vs {pct(bt['transition_accuracy'])}; clearances {pct(tb['clearance_accuracy'])} vs "
            f"{pct(bt['clearance_accuracy'])}) at F1 {tb['f1']:.3f}. Pick it if the dashboard's priority is signalling when "
            "congestion will clear.")
    r.metrics.update({"best_model": best, "test_f1": bt["f1"], "test_pr_auc": bt["pr_auc"], "persistence_f1": pt["f1"],
                      "delta_f1_vs_persistence": float(bb.delta_vs_persistence), "delta_ci": [float(bb.delta_ci_low), float(bb.delta_ci_high)],
                      "onset_recall": bt["onset_recall"], "clearance_accuracy": bt["clearance_accuracy"]})
    r.table("test_metrics", test_tab, "Test metrics (thresholds tuned on validation) with day-block bootstrap CIs vs persistence")
    r.table("val_metrics", comp[comp.split == "val"].drop(columns=["split"]), "Validation metrics")
    imp_best = next((m for m in [best, "gbt", "random_forest"] if imps.get(m)), None)
    if imp_best:
        imp = (pd.DataFrame({"feature": list(imps[imp_best]), "importance": list(imps[imp_best].values())})
               .assign(group=lambda d: d.feature.map(lambda f: FEATURES[f][0]))
               .sort_values("importance", ascending=False))
        r.table("importances", imp.head(20), f"Top-20 feature importances ({timing[imp_best]['label']})")
    r.table("training", pd.DataFrame([{"model": timing[m]["label"], "train_seconds": timing[m]["train_s"],
                                       "scoring_rows_per_second": timing[m].get("score_rows_per_s")} for m in model_names]),
            "Training time and scoring throughput (val + test, including aggregation)")

    plt, sns = plt_setup()
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    for name, h in hists.items():
        c = ev.curve(h["test"])
        lw = 2.5 if name == best else 1.2
        if name == "persistence":
            m = test_m[name]
            axes[0].scatter([m["recall"]], [m["precision"]], s=60, color="black", zorder=5, label=f"persistence (F1 {m['f1']:.3f})")
            continue
        axes[0].plot(c.recall, c.precision, lw=lw, label=f"{timing[name]['label']} (AP {test_m[name]['pr_auc']:.3f})")
        b = thresholds[name]
        axes[0].scatter([c.recall[b]], [c.precision[b]], s=25)
    axes[0].set(title="Precision–recall on TEST (dots = thresholds tuned on validation)", xlabel="recall", ylabel="precision",
                xlim=(0, 1), ylim=(0, 1.02))
    axes[0].legend(fontsize=8, loc="lower left")
    keys = list(BASELINES) + model_names
    x = np.arange(len(keys))
    for i, (metric, col) in enumerate((("f1", "#4c72b0"), ("onset_recall", "#dd8452"), ("transition_accuracy", "#55a868"))):
        axes[1].bar(x + (i - 1) * 0.27, [test_m[k][metric] for k in keys], width=0.27, color=col, label=metric)
    axes[1].set_xticks(x, [timing[k]["label"] for k in keys], rotation=20, ha="right")
    axes[1].set(title="Test: overall F1 vs performance on state changes", ylim=(0, 1))
    axes[1].legend(fontsize=8)
    r.figure(fig, "pr_and_bars", "Precision–recall curves and transition metrics on the test period")

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    for ax, name in zip(axes, ["persistence", best]):
        m = ev._counts(hists[name]["test"], thresholds[name])
        cm = np.array([[m["tn"], m["fp"]], [m["fn"], m["tp"]]])
        sns.heatmap(cm / cm.sum(axis=1, keepdims=True), annot=cm.astype(int), fmt=",", cmap="Blues", ax=ax, cbar=False,
                    xticklabels=["pred 0", "pred 1"], yticklabels=["true 0", "true 1"])
        ax.set(title=f"{timing[name]['label']} (test)")
    r.figure(fig, "confusion", "Confusion matrices on test (colour = row-normalised rate, numbers = counts)")

    r.implications = [
        f"Deploy **{timing[best]['label']}** in the streaming pipeline (saved as a Spark PipelineModel at "
        f"`{best_path.relative_to(best_path.parents[3])}`) with threshold {thresholds[best] / ev.BINS:.3f}. "
        "Report the persistence baseline beside it on the dashboard so users can judge its added value.",
        "Judge the model on onset/clearance metrics, not overall accuracy: on stable rows persistence is almost always right.",
        "Class weights shift probabilities upwards; use the tuned threshold, and do not read raw probabilities as "
        "calibrated risks (see Q18 calibration).",
    ]
    r.limitations = [
        f"Trained on a {frac:.0%} sample of the training period to fit a 7.6 GB laptop; more data and hyper-parameter "
        "search (CrossValidator with time-blocked folds) may improve the tree models.",
        f"PR-AUC/ROC-AUC are computed from a {ev.BINS}-bucket histogram of probabilities (the same approximation MLlib uses with numBins).",
        "One random seed per model; the bootstrap captures test-day variability, not training variability.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
