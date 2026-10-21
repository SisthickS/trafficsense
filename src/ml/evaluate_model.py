"""Evaluate the saved pipelines on the untouched TEST period: metrics, confusion matrices, cost of misses vs false alarms.

    python -m src.ml.evaluate_model [--cost-fn 10] [--cost-fp 1] [--bootstrap 1000]

Reads data/models/<ds>/ml_pipeline/ (written by train_model.py). Operating thresholds are re-derived from VALIDATION only
(F1-optimal and cost-optimal) and applied unchanged to TEST. Baselines: persistence (next = now) and the historical slot profile.
Also reloads the optimal bundle from disk and verifies it reproduces its recorded probabilities on the validation reference rows.
Outputs: reports/ml/evaluation.{md,json}, reports/ml/tables/eval_*.csv, reports/figures/ml/eval_*.png.
"""
from __future__ import annotations

import argparse
import json
import sys
import time

import numpy as np
import pandas as pd

from src.analytics.common import pct, start_spark
from src.ml.config import ALGORITHMS, SELECTION_FILE
from src.ml.costs import (CostModel, bootstrap_cost_saving, operating_point, profile, reference_costs, sensitivity)
from src.ml.model_io import load_model, verify_reload
from src.ml.pipeline import prepare
from src.ml.reporting import df_to_md, log, plt_setup, save_fig, setup_logging, write_report, write_table
from src.modeling import evaluation as ev
from src.modeling.data import cached_split
from src.modeling.models import BASELINES, score, score_baseline

RATIOS = [1, 2, 5, 10, 20, 50]
POINTS = {"f1": "F1-optimal threshold", "cost": "cost-optimal threshold"}
LABELS = {**{k: v[0] for k, v in BASELINES.items()}, **ALGORITHMS}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cost-fn", type=float, default=None, help="cost of a missed congestion (default: value used in training)")
    ap.add_argument("--cost-fp", type=float, default=None, help="cost of a false alarm (default: value used in training)")
    ap.add_argument("--bootstrap", type=int, default=1000, help="day-block bootstrap resamples")
    a = ap.parse_args(argv)
    if not SELECTION_FILE.exists():
        raise SystemExit(f"{SELECTION_FILE} not found - run `python -m src.ml.train_model` first.")
    sel = json.loads(SELECTION_FILE.read_text())
    cm = CostModel(a.cost_fn or sel["cost_model"]["fn"], a.cost_fp or sel["cost_model"]["fp"])
    setup_logging()
    t0 = time.time()
    spark = start_spark("trafficsense-ml-eval")
    cache: dict = {}
    try:
        val, test = (prepare(cached_split(spark, s, cache)) for s in ("val", "test"))
        bundles = {algo: load_model(v["path"]) for algo, v in sel["algorithms"].items()}
        hists = {}
        for name in BASELINES:
            hists[name] = {"val": ev.histogram(score_baseline(name, val)), "test": ev.histogram(score_baseline(name, test))}
        for algo, b in bundles.items():
            hists[algo] = {"val": ev.histogram(score(b.model, val)), "test": ev.histogram(score(b.model, test))}
            log.info("scored %s", algo)

        res = analyse(hists, cm, a.bootstrap)
        reload_check = verify_reload(sel["best_path"], val)
        log.info("reload check of best/ (validation reference rows): %s", "OK" if reload_check["ok"] else f"FAILED {reload_check}")
        write_outputs(res, sel, bundles, cm, reload_check, time.time() - t0)
    finally:
        for df in cache.values():
            df.unpersist()
        spark.stop()
    return 0 if reload_check["ok"] else 1


def analyse(hists: dict, cm: CostModel, n_boot: int) -> dict:
    """All numbers of the report from the per-model validation/test histograms."""
    prof = {m: {s: profile(h[s], cm) for s in ("val", "test")} for m, h in hists.items()}
    # thresholds come from validation only; persistence is a fixed 0/1 rule
    bucket = {}
    for m in hists:
        if m == "persistence":
            bucket[m] = {"f1": ev.BINS // 2, "cost": ev.BINS // 2}
        else:
            bucket[m] = {"f1": prof[m]["val"]["bucket_f1"], "cost": prof[m]["val"]["bucket_cost"]}

    rows, cms = [], []
    for m in hists:
        for point in POINTS:
            for split in ("val", "test"):
                b = bucket[m][point]
                full = ev.metrics(hists[m][split], b)
                op = operating_point(prof[m][split]["curve"], b, cm)
                rows.append({"model": m, "label": LABELS[m], "point": point, "split": split, "threshold": full["threshold"],
                             "precision": full["precision"], "recall": full["recall"], "f1": full["f1"], "accuracy": full["accuracy"],
                             "pr_auc": full["pr_auc"], "roc_auc": full["roc_auc"], "onset_recall": full["onset_recall"],
                             "clearance_accuracy": full["clearance_accuracy"], "false_alarm_rate": full["false_alarm_rate"],
                             "miss_rate": op["miss_rate"], "cost": op["cost"], "cost_per_1000": op["cost_per_1000"]})
                cms.append({"model": m, "label": LABELS[m], "point": point, "split": split, "threshold": full["threshold"],
                            **{k: op[k] for k in ("tp", "fp", "fn", "tn")}})
    metrics_df, cm_df = pd.DataFrame(rows), pd.DataFrame(cms)

    refs = reference_costs(prof["persistence"]["test"]["curve"], cm)
    daily_f1 = {m: ev.per_day(hists[m]["test"], bucket[m]["f1"]) for m in hists}
    daily_cost = {m: ev.per_day(hists[m]["test"], bucket[m]["cost"]) for m in hists}
    f1_boot = ev.bootstrap_f1(daily_f1, "persistence", n_boot)
    cost_boot = bootstrap_cost_saving(daily_cost, "persistence", cm, n_boot)

    sens = []
    for m in hists:
        cv, ct = prof[m]["val"]["curve"], prof[m]["test"]["curve"]
        if m == "persistence":
            n = float(ct.iloc[0][["tp", "fp", "fn", "tn"]].sum())
            op = ct.iloc[ev.BINS // 2]
            s = pd.DataFrame({"fn_fp_ratio": RATIOS, "cost_per_1000": [1000 * (r * cm.fp * op.fn + cm.fp * op.fp) / n for r in RATIOS]})
        else:
            s = sensitivity(cv, ct, RATIOS, cm.fp)
        s.insert(0, "model", m)
        sens.append(s)
    sens = pd.concat(sens, ignore_index=True)
    n_rows = float(prof["persistence"]["test"]["curve"].iloc[0][["tp", "fp", "fn", "tn"]].sum())
    pos = float(prof["persistence"]["test"]["curve"].iloc[0][["tp", "fn"]].sum())
    never = pd.DataFrame({"fn_fp_ratio": RATIOS, "cost_per_1000": [1000 * r * cm.fp * pos / n_rows for r in RATIOS], "model": "never_alert"})
    sens = pd.concat([sens, never], ignore_index=True)
    return {"prof": prof, "bucket": bucket, "metrics": metrics_df, "confusion": cm_df, "refs": refs, "f1_boot": f1_boot,
            "cost_boot": cost_boot, "sens": sens, "test_rows": n_rows, "test_positives": pos}


def write_outputs(res: dict, sel: dict, bundles: dict, cm: CostModel, reload_check: dict, seconds: float) -> None:
    metrics, conf, sens = res["metrics"], res["confusion"], res["sens"]
    algos = list(ALGORITHMS)
    models = list(BASELINES) + algos
    test = metrics[metrics.split == "test"]
    best = sel["best_algorithm"]
    t_metrics = write_table(metrics, "eval_metrics")
    t_conf = write_table(conf, "eval_confusion_matrices")
    t_sens = write_table(sens, "eval_cost_sensitivity")

    # ---- figures
    plt, sns = plt_setup()
    colors = dict(zip(models, sns.color_palette("tab10", len(models))))
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    for m in algos + ["historical_profile"]:
        c = res["prof"][m]["test"]["curve"]
        m_t = test[(test.model == m) & (test.point == "f1")].iloc[0]
        axes[0].plot(c.fpr, c.recall, color=colors[m], label=f"{LABELS[m]} (AUC {m_t.roc_auc:.3f})")
        axes[1].plot(c.recall, c.precision, color=colors[m], label=f"{LABELS[m]} (AP {m_t.pr_auc:.3f})")
    pm = test[(test.model == "persistence") & (test.point == "f1")].iloc[0]
    axes[0].scatter([pm.false_alarm_rate], [pm.recall], color="black", zorder=5, label="Persistence (fixed point)")
    axes[1].scatter([pm.recall], [pm.precision], color="black", zorder=5, label=f"Persistence (F1 {pm.f1:.3f})")
    axes[0].plot([0, 1], [0, 1], "k:", lw=1)
    axes[0].set(xlabel="False-positive rate", ylabel="Recall (true-positive rate)", title="ROC - test period", xlim=(0, 0.2), ylim=(0.6, 1.0))
    axes[1].set(xlabel="Recall", ylabel="Precision", title="Precision-recall - test period", xlim=(0.5, 1.0), ylim=(0.6, 1.0))
    axes[0].legend(loc="lower right", fontsize=8)
    axes[1].legend(loc="lower left", fontsize=8)
    f_roc = save_fig(fig, "eval_roc_pr")

    fig, axes = plt.subplots(2, len(models) - 1, figsize=(4 * (len(models) - 1), 7.5))
    shown = ["persistence"] + algos + ["historical_profile"]
    for r, point in enumerate(POINTS):
        for k, m in enumerate(shown[: len(models) - 1]):
            row = conf[(conf.model == m) & (conf.point == point) & (conf.split == "test")].iloc[0]
            mat = np.array([[row.tn, row.fp], [row.fn, row.tp]])
            share = mat / mat.sum(axis=1, keepdims=True)
            annot = np.array([[f"{mat[i, j]:,.0f}\n{share[i, j]:.1%}" for j in range(2)] for i in range(2)])
            sns.heatmap(share, annot=annot, fmt="", cmap="Blues", cbar=False, vmin=0, vmax=1, ax=axes[r, k],
                        xticklabels=["no alert", "alert"], yticklabels=["free / slowed", "congested"])
            axes[r, k].set_title(f"{LABELS[m]}\n{POINTS[point]} ({row.threshold:.2f})", fontsize=9)
    fig.suptitle("Confusion matrices, test period (rows = actual next-5-min state; % of actual class)")
    fig.subplots_adjust(hspace=0.5)
    f_cm = save_fig(fig, "eval_confusion_matrices")

    fig, axes = plt.subplots(1, 2, figsize=(13, 4.8))
    for m in algos + ["historical_profile"]:
        c = res["prof"][m]["test"]["curve"]
        axes[0].plot(c.threshold, 1000 * cm.cost(c.fn, c.fp) / res["test_rows"], color=colors[m], label=LABELS[m])
        b = res["bucket"][m]["cost"]
        axes[0].scatter([c.threshold[b]], [1000 * cm.cost(c.fn[b], c.fp[b]) / res["test_rows"]], color=colors[m], zorder=5, edgecolor="black")
    pcost = test[(test.model == "persistence") & (test.point == "cost")].iloc[0].cost_per_1000
    axes[0].axhline(pcost, color="black", ls="--", label="Persistence")
    axes[0].set(xlabel="Alert threshold on P(congested in 5 min)", ylabel="Cost per 1,000 readings", ylim=(0, pcost * 2.2),
                title=f"Test cost vs threshold (FN={cm.fn:g}, FP={cm.fp:g})")
    axes[0].text(0.02, 0.02, "dots = threshold tuned on validation", transform=axes[0].transAxes, fontsize=8)
    axes[0].legend(fontsize=8)
    for m in algos + ["persistence", "historical_profile", "never_alert"]:
        s = sens[sens.model == m]
        axes[1].plot(s.fn_fp_ratio, s.cost_per_1000, marker="o", color=colors.get(m, "gray"), ls=":" if m == "never_alert" else "-",
                     label=LABELS.get(m, "Never alert"))
    axes[1].set(xscale="log", yscale="log", xlabel="Cost ratio  missed congestion : false alarm", ylabel="Test cost per 1,000 readings",
                title="Cost-ratio sensitivity (threshold re-tuned on validation)")
    axes[1].legend(fontsize=8)
    f_cost = save_fig(fig, "eval_cost")

    # ---- findings (all numbers computed)
    g = lambda m, p: test[(test.model == m) & (test.point == p)].iloc[0]
    cb, fb = res["cost_boot"].set_index("model"), res["f1_boot"].set_index("model")
    pers_c = g("persistence", "cost")
    best_c, best_f = g(best, "cost"), g(best, "f1")
    best_cm = conf[(conf.model == best) & (conf.point == "cost") & (conf.split == "test")].iloc[0]
    test_best_by_cost = min(algos, key=lambda m: g(m, "cost").cost)
    findings = [
        f"Selected on validation (lowest expected cost): **{ALGORITHMS[best]}**. On the untouched test period at its F1-optimal threshold it scores "
        f"precision {best_f.precision:.3f}, recall {best_f.recall:.3f}, F1 {best_f.f1:.3f} (persistence F1 {g('persistence', 'f1').f1:.3f}), "
        f"PR-AUC {best_f.pr_auc:.3f}, ROC-AUC {best_f.roc_auc:.3f}. At its cost-optimal (deployed) threshold: precision {best_c.precision:.3f}, "
        f"recall {best_c.recall:.3f}, F1 {best_c.f1:.3f}.",
        f"F1 gain over persistence: {fb.loc[best, 'delta_vs_persistence']:+.3f} (95% day-block CI {fb.loc[best, 'delta_ci_low']:+.3f} to "
        f"{fb.loc[best, 'delta_ci_high']:+.3f}; better in {pct(fb.loc[best, 'share_boot_better'])} of resamples).",
        f"Cost at FN:FP = {cm.fn:g}:{cm.fp:g}: {ALGORITHMS[best]} {best_c.cost_per_1000:.2f} per 1,000 readings vs persistence "
        f"{pers_c.cost_per_1000:.2f} ({pct(cb.loc[best, 'saving_pct'])} saving, 95% CI {cb.loc[best, 'saving_ci_low']:,.0f} to "
        f"{cb.loc[best, 'saving_ci_high']:,.0f} cost units; cheaper in {pct(cb.loc[best, 'share_boot_cheaper'])} of resamples). "
        f"Never alerting costs {1000 * res['refs']['never_alert_cost'] / res['test_rows']:.2f}; always alerting {1000 * res['refs']['always_alert_cost'] / res['test_rows']:.2f}.",
        f"Missed congestion vs false alarms: at the cost-optimal point {ALGORITHMS[best]} has {best_c.miss_rate:.1%} misses "
        f"({best_cm.fn:,.0f} readings, {pct(cm.fn * best_cm.fn / best_c.cost)} of its cost) and a "
        f"{best_c.false_alarm_rate:.2%} false-alarm rate on stable free-flow rows; persistence misses {g('persistence', 'cost').miss_rate:.1%} "
        f"(every onset).",
        f"Onsets (free/slowed now, congested in 5 min): {ALGORITHMS[best]} recalls {best_f.onset_recall:.1%} at the F1 point and "
        f"{best_c.onset_recall:.1%} at the cost point; persistence 0% by construction.",
    ]
    cross = sens[sens.model == best].merge(sens[sens.model == "persistence"][["fn_fp_ratio", "cost_per_1000"]], on="fn_fp_ratio", suffixes=("", "_pers"))
    wins = cross[cross.cost_per_1000 < cross.cost_per_1000_pers]
    findings.append(
        (f"Across cost ratios {RATIOS[0]}-{RATIOS[-1]}:1 {ALGORITHMS[best]} beats persistence from ratio {int(wins.fn_fp_ratio.min())}:1 upward "
         f"(at {RATIOS[-1]}:1: {cross.iloc[-1].cost_per_1000:.2f} vs {cross.iloc[-1].cost_per_1000_pers:.2f}).") if len(wins) else
        f"{ALGORITHMS[best]} does not beat persistence on cost at any tested ratio ({RATIOS[0]}-{RATIOS[-1]}:1).")
    if test_best_by_cost != best:
        findings.append(f"Note: on test, {ALGORITHMS[test_best_by_cost]} has the lowest cost; the validation-selected model is kept because test must not drive selection.")
    recs = [
        f"Deploy the `best/` bundle with its validation-tuned threshold ({bundles[best].threshold:.3f}) for alerting; revisit the threshold whenever the FN:FP cost ratio changes "
        "(`python -m src.ml.evaluate_model --cost-fn X --cost-fp Y`).",
        "Alert on the cost-optimal threshold for operations (misses are expensive) and keep the F1-optimal threshold for dashboards that need balanced precision/recall.",
        "Monitor onset recall and false-alarm rate per week; overall accuracy and F1 are dominated by stable rows that persistence already gets right.",
    ]

    summary = metrics[(metrics.split == "test")][["label", "point", "threshold", "precision", "recall", "f1", "pr_auc", "roc_auc", "onset_recall",
                                                   "false_alarm_rate", "cost_per_1000"]]
    summary = summary.assign(point=summary.point.map(POINTS))
    conf_t = conf[conf.split == "test"].drop(columns=["model", "split"]).assign(point=lambda d: d.point.map(POINTS))
    conf_t["threshold"] = conf_t.threshold.map("{:.3f}".format)
    boot = res["f1_boot"].merge(res["cost_boot"], on="model", suffixes=("", "_cost"))
    boot.insert(1, "label", boot.model.map(LABELS))
    t_boot = write_table(boot, "eval_bootstrap_vs_persistence")
    ck = reload_check
    md = f"""# Phase 7 - Evaluation on the test period

**Question.** Which tuned MLlib pipeline best predicts congestion 5 minutes ahead, and what do missed congestion and false alarms cost?

## Method

- Models: the three saved 7-stage pipelines ({", ".join(ALGORITHMS.values())}) plus two baselines. Test period: {res['test_rows']:,.0f} readings,
  {res['test_positives']:,.0f} congested ({pct(res['test_positives'] / res['test_rows'])}), strictly after training and validation.
- Thresholds: F1-optimal and cost-optimal, both chosen on **validation** and applied unchanged to **test**. Persistence (next = now) is a fixed 0/1 rule.
- Cost model: missed congestion = {cm.fn:g}, false alarm = {cm.fp:g} (alert is worthwhile when P(congestion) > {cm.breakeven_probability:.3f}; class-weighted
  probabilities are not calibrated, so thresholds are tuned empirically). Sensitivity over ratios {RATIOS}.
- Metrics from one Spark aggregation per model and split ({ev.BINS} probability buckets); confidence intervals from a day-block bootstrap of test days.

## Key findings

{chr(10).join('- ' + f for f in findings)}

## Test metrics

{df_to_md(summary, max_rows=20)}

![roc and pr]({f_roc})

## Confusion matrices (test)

{df_to_md(conf_t, max_rows=20, floatfmt=0)}

![confusion]({f_cm})

## Cost of missed congestion vs false alarms

{df_to_md(res['cost_boot'].assign(model=res['cost_boot'].model.map(LABELS)), max_rows=10)}

Cost sensitivity (test cost per 1,000 readings, threshold re-tuned on validation for each ratio):

{df_to_md(sens.pivot(index='fn_fp_ratio', columns='model', values='cost_per_1000').reset_index(), max_rows=10)}

![cost]({f_cost})

## F1 and cost against persistence (day-block bootstrap)

{df_to_md(boot[['label', 'f1', 'f1_ci_low', 'f1_ci_high', 'delta_vs_persistence', 'delta_ci_low', 'delta_ci_high', 'saving_pct', 'saving_ci_low', 'saving_ci_high']], max_rows=10)}

## Recommendations

{chr(10).join('- ' + r for r in recs)}

## Serialization / reload verification

Reloaded `best/` ({ALGORITHMS[best]}) from disk and re-scored {ck['reference_rows']} deterministic validation rows (the reference sample recorded at save time; test
data is never touched during training): **{'PASSED' if ck['ok'] else 'FAILED'}**, max |Δp| vs saved probabilities = {ck['max_abs_probability_diff']:.1e}; checks {json.dumps(ck['checks'])}.
All test metrics above were themselves produced by models loaded from disk.

Tables: [{t_metrics}]({t_metrics}), [{t_conf}]({t_conf}), [{t_sens}]({t_sens}), [{t_boot}]({t_boot}). Evaluation took {seconds / 60:.1f} min.
"""
    payload = {"best_algorithm": best, "cost_model": {"fn": cm.fn, "fp": cm.fp}, "findings": findings, "recommendations": recs,
               "test_metrics": test.to_dict("records"), "confusion_matrices": conf.to_dict("records"),
               "f1_bootstrap": res["f1_boot"].to_dict("records"), "cost_bootstrap": res["cost_boot"].to_dict("records"),
               "reference_costs": res["refs"], "reload_check": reload_check, "figures": [f_roc, f_cm, f_cost]}
    write_report("evaluation", md, payload)
    log.info("wrote reports/ml/evaluation.md")


if __name__ == "__main__":
    sys.exit(main())
