"""Q17 - Feature-group contribution and ablation.

Re-trains a compact GBT once with all features and once without each feature group (leave-one-group-out), plus a
minimal 'current state + lags' model. The threshold is tuned on the first half of the validation days and metrics are
reported on the second half, so tuning and evaluation never share data. Built-in tree importances from Q16
(registry) are aggregated by group for comparison.

Usage: python -m src.analytics.q17_feature_ablation
"""
from __future__ import annotations

import json
import sys

import numpy as np
import pandas as pd

from src.analytics.common import AnalyticsContext, QuestionResult, pct, plt_setup, run_single
from src.modeling import evaluation as ev
from src.modeling.data import FEATURE_GROUPS, cached_split, model_dir, training_sample
from src.modeling.models import compact_gbt, fit, score
from src.preprocessing.build_features import FEATURE_COLUMNS, FEATURES

KEY_METRICS = ["pr_auc", "f1", "onset_recall", "clearance_accuracy", "transition_accuracy", "false_alarm_rate"]


def split_days(hist: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    days = sorted(hist.date_local.unique())
    half = set(days[: len(days) // 2])
    return hist[hist.date_local.isin(half)], hist[~hist.date_local.isin(half)]


def evaluate(hist: pd.DataFrame) -> dict:
    tune, held = split_days(hist)
    b = ev.best_bucket(ev.curve(tune))
    return ev.metrics(held, b)


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q17", "Feature-group contribution and ablation",
                       "Which groups of engineered features actually drive the 5-minute congestion predictions, and "
                       "which are redundant?")
    spark = ctx.spark
    ctx.release()
    frac = ctx.pick(0.03, 0.005)
    spec = compact_gbt(ctx.quick)
    val = cached_split(spark, "val", ctx._cache)
    train, info = training_sample(spark, frac, seed=43)
    r.method = [
        f"Compact GBT {spec.params} trained on a {frac:.0%} class-weighted sample of the training period "
        f"({info['rows']:,} rows) for every variant, with the same seed and sample.",
        "Variants: all features; **leave one group out** for each of the " + f"{len(FEATURE_GROUPS)} groups (" +
        ", ".join(f"{g} ({len(c)})" for g, c in FEATURE_GROUPS.items()) + "); and a minimal model using only the "
        "current state and lags.",
        "Validation days are split chronologically: the threshold is tuned (max F1) on the first half and metrics are "
        "reported on the second half. The contribution of a group = metric(all) − metric(without group).",
        "For comparison, the built-in tree importances of the Q16 models (from the model registry) are summed per group.",
    ]
    variants = {"all features": FEATURE_COLUMNS}
    for g, cols in FEATURE_GROUPS.items():
        variants[f"without {g}"] = [c for c in FEATURE_COLUMNS if c not in cols]
    variants["current + lag only"] = FEATURE_GROUPS["current"] + FEATURE_GROUPS["lag"]

    rows = []
    for name, cols in variants.items():
        model, secs = fit(spec, train, cols)
        m = evaluate(ev.histogram(score(model, val)))
        rows.append({"variant": name, "features": len(cols), "train_s": secs, **{k: m[k] for k in KEY_METRICS}})
    train.unpersist()
    tab = pd.DataFrame(rows)
    full = tab.set_index("variant").loc["all features"]
    for k in KEY_METRICS:
        tab[f"delta_{k}"] = tab[k] - full[k]
    abl = tab[tab.variant.str.startswith("without")].copy()
    abl["group"] = abl.variant.str.replace("without ", "", regex=False)
    abl["contribution_pr_auc"] = -abl.delta_pr_auc
    abl["contribution_onset_recall"] = -abl.delta_onset_recall
    # PR-AUC is dominated by continuing congestion (easy for every variant), so rank by onset recall, the hard part.
    abl = abl.sort_values(["contribution_onset_recall", "contribution_pr_auc"], ascending=False)

    reg_path = model_dir() / "registry.json"
    imp_rows = []
    if reg_path.exists():
        reg = json.loads(reg_path.read_text())
        for model_name, imp in reg.get("importances", {}).items():
            if not imp:
                continue
            for g, cols in FEATURE_GROUPS.items():
                imp_rows.append({"model": reg["models"][model_name]["label"], "key": model_name, "group": g,
                                 "importance": sum(imp.get(c, 0.0) for c in cols)})
    imp_tab = pd.DataFrame(imp_rows)

    top = abl.iloc[0]
    second = abl.iloc[1]
    pr_range = (tab.pr_auc.min(), tab.pr_auc.max())
    negligible = abl[(abl.contribution_pr_auc.abs() < 0.005) & (abl.contribution_onset_recall.abs() < 0.02)].group.tolist()
    harmful = abl[abl.contribution_onset_recall <= -0.02].group.tolist()
    minimal = tab.set_index("variant").loc["current + lag only"]
    r.findings = [
        f"With all {len(FEATURE_COLUMNS)} features the compact GBT reaches PR-AUC {full.pr_auc:.3f}, F1 {full.f1:.3f} and onset "
        f"recall {pct(full.onset_recall)} on the held-out validation days.",
        f"PR-AUC is saturated: every variant lies between {pr_range[0]:.3f} and {pr_range[1]:.3f}, because most positives are "
        "continuing congestion that any variant predicts. Differences show up in **onset recall** (congestion that has not "
        "started yet), which is used to rank the groups.",
        f"Largest contribution to onset detection: **{top.group}** (removing it costs {top.contribution_onset_recall * 100:.1f} pp "
        f"onset recall and {top.contribution_pr_auc:.3f} PR-AUC), followed by **{second.group}** "
        f"({second.contribution_onset_recall * 100:.1f} pp).",
        (f"Redundant given the others (|ΔPR-AUC| < 0.005 and |Δonset recall| < 2 pp when removed): {', '.join(negligible)}; "
         "correlated features carry their information.") if negligible else
        "No group is fully redundant: each one moves PR-AUC or onset recall noticeably when removed.",
        f"A minimal model with only the current state and lags ({int(minimal.features)} features) reaches PR-AUC "
        f"{minimal.pr_auc:.3f} ({minimal.pr_auc - full.pr_auc:+.3f} vs all) and onset recall {pct(minimal.onset_recall)} "
        f"({(minimal.onset_recall - full.onset_recall) * 100:+.1f} pp)"
        + (f"; removing {', '.join(harmful)} even *raises* onset recall, so those groups add noise for onsets." if harmful else "."),
    ]
    if len(imp_tab):
        ref = "gbt" if "gbt" in set(imp_tab.key) else imp_tab.key.iloc[0]
        g = imp_tab[imp_tab.key == ref].sort_values("importance", ascending=False)
        r.findings.append(
            f"Built-in importance ({g.model.iloc[0]}, Q16) ranks {g.group.iloc[0]} ({pct(g.importance.iloc[0])}) and "
            f"{g.group.iloc[1]} ({pct(g.importance.iloc[1])}) highest. Importance measures how often a group is used, ablation "
            "how much it is irreplaceable; the two need not agree when features are correlated.")
    r.metrics = {"full_pr_auc": float(full.pr_auc), "top_group": top.group,
                 "top_contribution_onset_recall": float(top.contribution_onset_recall),
                 "negligible_groups": negligible, "minimal_pr_auc": float(minimal.pr_auc)}
    r.table("ablation", abl[["group", "features", "contribution_pr_auc", "contribution_onset_recall", "delta_f1",
                             "delta_transition_accuracy", "delta_false_alarm_rate", "pr_auc", "onset_recall", "train_s"]],
            "Leave-one-group-out ablation (contribution = all − without; higher = more important)")
    r.table("variants", tab, "All variants with absolute metrics (held-out validation days)")
    if len(imp_tab):
        r.table("importance_by_group", imp_tab.pivot(index="group", columns="model", values="importance").reset_index(),
                "Built-in tree importance summed per feature group (Q16 models)")
    r.table("groups", pd.DataFrame([{"group": g, "features": ", ".join(c)} for g, c in FEATURE_GROUPS.items()]), "Feature groups")

    plt, sns = plt_setup()
    fig, axes = plt.subplots(1, 2 if len(imp_tab) else 1, figsize=(14 if len(imp_tab) else 8, 4.5), squeeze=False)
    ax = axes[0, 0]
    y = np.arange(len(abl))
    ax.barh(y - 0.2, abl.contribution_pr_auc, height=0.4, color="#4c72b0", label="PR-AUC")
    ax.barh(y + 0.2, abl.contribution_onset_recall, height=0.4, color="#dd8452", label="onset recall")
    ax.set_yticks(y, abl.group)
    ax.invert_yaxis()
    ax.axvline(0, color="black", lw=0.8)
    ax.set(title="Drop when the group is removed (ablation)", xlabel="metric(all) − metric(without group)")
    ax.legend()
    if len(imp_tab):
        piv = imp_tab.pivot(index="group", columns="model", values="importance").reindex(abl.group)
        piv.plot.barh(ax=axes[0, 1])
        axes[0, 1].invert_yaxis()
        axes[0, 1].set(title="Built-in tree importance by group (Q16)", xlabel="share of total importance", ylabel="")
    fig.tight_layout()
    r.figure(fig, "ablation", "Feature-group contribution: ablation vs built-in importance")

    r.implications = [
        f"Keep the {top.group} and {second.group} groups in the streaming feature pipeline; they carry most of the onset-detection signal.",
        ("Redundant groups (" + ", ".join(negligible) + ") could be dropped from the streaming job to cut latency and state "
         "size, with negligible accuracy loss.") if negligible else
        "No group is redundant enough to drop without measurable loss.",
        "Ablation is the evidence to quote in the report; built-in importances are biased towards high-cardinality, "
        "frequently split features.",
    ]
    r.limitations = [
        f"Ablation uses a compact GBT on a {frac:.0%} sample and evaluates on the second half of the validation days; "
        "absolute numbers are not comparable with Q16 (different model, sample and evaluation period), but all variants "
        "are compared under identical conditions.",
        "Leave-one-group-out underestimates groups whose information is duplicated elsewhere (correlated features).",
        "Onset recall depends on a threshold tuned per variant on ~9 validation days, so differences of a few points are noisy.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
