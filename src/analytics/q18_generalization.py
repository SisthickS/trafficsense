"""Q18 - Model generalization across time periods and sensors.

Uses the best model saved by Q16 (its validation/test predictions and threshold), plus three compact re-trainings
for a sensor hold-out experiment.

Usage: python -m src.analytics.q18_generalization
"""
from __future__ import annotations

import json
import sys

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.analytics.common import AnalyticsContext, QuestionResult, pct, plt_setup, run_single
from src.modeling import evaluation as ev
from src.modeling.data import FEATURE_GROUPS, cached_split, model_dir, sensor_holdout, training_sample
from src.modeling.models import PROB, compact_gbt, fit, score
from src.preprocessing.build_features import FEATURE_COLUMNS, LABEL

MIN_POS_SENSOR = 30


def _segment(preds, groups: tuple[str, ...], b_model: int) -> pd.DataFrame:
    """Model and persistence metrics per segment from ONE aggregation (persistence uses congested_now as p)."""
    hm = ev.histogram(preds, groups)
    hp = ev.histogram(preds.withColumn(PROB, F.col("congested_now").cast("double")), groups)
    keys = list(groups)
    m = ev.metrics_by(hm, b_model, keys)
    p = ev.metrics_by(hp, ev.BINS // 2, keys)[keys + ["f1"]].rename(columns={"f1": "persistence_f1"})
    out = m.merge(p, on=keys)
    out["gain_vs_persistence"] = out.f1 - out.persistence_f1
    return out


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q18", "Model generalization across time periods and sensors",
                       "Does the selected model perform consistently across weeks, months, times of day, day types and "
                       "individual sensors, and can it generalize to sensors it was not trained on?")
    reg_path = model_dir() / "registry.json"
    if not reg_path.exists():
        raise RuntimeError("Model registry not found - run Q16 first (python -m src.analytics.q16_prediction_metrics).")
    reg = json.loads(reg_path.read_text())
    spark = ctx.spark
    ctx.release()
    b = int(reg["threshold_bucket"])
    preds = (spark.read.parquet(reg["predictions_path"])
                  .withColumn("week", F.date_format(F.date_trunc("week", "date_local"), "yyyy-MM-dd"))
                  .withColumn("month", F.date_format("date_local", "yyyy-MM"))
                  .withColumn("day_type", F.when(F.col("is_holiday") == 1, "holiday")
                                           .when(F.col("is_weekend") == 1, "weekend").otherwise("working day"))
                  .cache())
    test = preds.filter(F.col("split") == "test")
    r.method = [
        f"Model: **{reg['best_model_label']}** from Q16, threshold {reg['threshold']:.3f} (tuned on validation); its saved "
        "validation and test predictions are segmented. Every segment also gets the persistence baseline (gain = F1 − persistence F1).",
        "Time: ISO week (validation + test), month, hour of day and day type (test). Drift is the trend of weekly F1.",
        f"Sensors: per-sensor test F1 for sensors with ≥ {MIN_POS_SENSOR} positive targets; relation to congestion level.",
        "Calibration on test: reliability by probability decile, Brier score, expected calibration error (ECE).",
        "Sensor hold-out: about 20% of sensors are hidden (deterministic hash). A compact GBT is trained (a) on all "
        "sensors, (b) on the other 80% only, and (c) on the other 80% without sensor-specific history (sensor + profile "
        "groups, a 'cold-start' sensor). All three are evaluated on the hidden sensors' test rows, with thresholds tuned "
        "on validation rows of the training sensors.",
    ]

    weeks = _segment(preds, ("split", "week"), b).sort_values(["week", "split"], ascending=[True, False])
    months = _segment(preds, ("month",), b).sort_values("month")
    hours = _segment(test, ("hour",), b).sort_values("hour")
    daytypes = _segment(test, ("day_type",), b)
    hs = ev.histogram(test, ("sensor_id",))
    sensors = ev.metrics_by(hs, b, "sensor_id", min_positives=MIN_POS_SENSOR)
    sensors["positive_rate"] = sensors.positives / sensors.rows
    cal, brier, ece = ev.calibration(ev.histogram(test, ()))
    preds.unpersist()

    # ---- sensor hold-out ---------------------------------------------------------------------------------------
    spec = compact_gbt(ctx.quick)
    frac = ctx.pick(0.03, 0.005)
    hidden = sensor_holdout()
    val = cached_split(spark, "val", ctx._cache)
    test_full = cached_split(spark, "test", ctx._cache)
    cold_cols = [c for c in FEATURE_COLUMNS if c not in FEATURE_GROUPS["sensor"] + FEATURE_GROUPS["profile"]]
    configs = {
        "trained on all sensors": (None, FEATURE_COLUMNS),
        "trained without hidden sensors": (~hidden, FEATURE_COLUMNS),
        "cold start (no sensor history features)": (~hidden, cold_cols),
    }
    hold_rows = []
    for name, (row_filter, cols) in configs.items():
        train, info = training_sample(spark, frac, seed=44, row_filter=row_filter)
        model, secs = fit(spec, train, cols)
        train.unpersist()
        bt = ev.best_bucket(ev.curve(ev.histogram(score(model, val.filter(~hidden)))))
        m = ev.metrics(ev.histogram(score(model, test_full.filter(hidden))), bt)
        hold_rows.append({"setting": name, "features": len(cols), "train_rows": info["rows"], **{k: m[k] for k in
                          ("f1", "pr_auc", "precision", "recall", "onset_recall", "transition_accuracy")}})
    pm = ev.metrics(ev.histogram(test_full.filter(hidden).withColumn(PROB, F.col("congested_now").cast("double"))), ev.BINS // 2)
    hold_rows.append({"setting": "persistence baseline", "features": 1, "train_rows": 0,
                      **{k: pm[k] for k in ("f1", "pr_auc", "precision", "recall", "onset_recall", "transition_accuracy")}})
    hold = pd.DataFrame(hold_rows)
    n_hidden = test_full.filter(hidden).select("sensor_id").distinct().count()

    # ---- findings ---------------------------------------------------------------------------------------------
    wt = weeks[weeks.split == "test"]
    wk_f1 = weeks.f1.to_numpy()
    slope = np.polyfit(np.arange(len(wk_f1)), wk_f1, 1)[0] if len(wk_f1) > 2 else 0.0
    vm = months.set_index("month")
    best_h = hours.loc[hours.gain_vs_persistence.idxmax()]
    worst_h = hours[hours.positives >= 100].sort_values("f1").iloc[0] if (hours.positives >= 100).any() else hours.loc[hours.f1.idxmin()]
    sens_corr = sensors[["f1", "positive_rate"]].corr(method="spearman").iloc[0, 1]
    hd = hold.set_index("setting")
    drop_unseen = hd.loc["trained on all sensors", "f1"] - hd.loc["trained without hidden sensors", "f1"]
    drop_cold = hd.loc["trained without hidden sensors", "f1"] - hd.loc["cold start (no sensor history features)", "f1"]
    r.findings = [
        f"Across {len(weeks)} weeks (validation + test), weekly F1 ranges {wk_f1.min():.3f}–{wk_f1.max():.3f} "
        f"(std {wk_f1.std():.3f}) with a trend of {slope * 10:+.3f} per 10 weeks: "
        + ("no meaningful drift." if abs(slope * 10) < 0.02 else "a noticeable drift; schedule periodic retraining."),
        "By month: " + ", ".join(f"{k} F1 {v.f1:.3f} (gain {v.gain_vs_persistence:+.3f})" for k, v in vm.iterrows()) + ".",
        f"By hour (test), the model adds most over persistence at {int(best_h.hour):02d}:00 ({best_h.gain_vs_persistence:+.3f} F1); "
        f"F1 is lowest at {int(worst_h.hour):02d}:00 ({worst_h.f1:.3f}, {int(worst_h.positives):,} positives; hours with ≥ 100 positives). "
        "Day types: " + ", ".join(f"{row.day_type} {row.f1:.3f}" for row in daytypes.itertuples()) + ".",
        f"Per sensor ({len(sensors)} sensors with ≥ {MIN_POS_SENSOR} positives): median F1 {sensors.f1.median():.3f}, "
        f"IQR {sensors.f1.quantile(0.25):.3f}–{sensors.f1.quantile(0.75):.3f}. Spearman ρ = {sens_corr:.2f} with the sensor's "
        "congestion rate, so the model is " + ("better on frequently congested sensors." if sens_corr > 0.3 else
                                              "weaker on frequently congested sensors." if sens_corr < -0.3 else
                                              "roughly equally good on rarely and frequently congested sensors."),
        f"Sensor hold-out ({n_hidden} hidden sensors): F1 {hd.loc['trained on all sensors', 'f1']:.3f} when trained on all "
        f"sensors, {hd.loc['trained without hidden sensors', 'f1']:.3f} when the sensors are unseen ({-drop_unseen:+.3f}), and "
        f"{hd.loc['cold start (no sensor history features)', 'f1']:.3f} for a cold-start sensor without history features "
        f"(persistence {hd.loc['persistence baseline', 'f1']:.3f}).",
        f"Calibration (test): Brier {brier:.4f}, ECE {ece:.3f}. Probabilities are "
        + ("over-confident (above observed rates)" if (cal.mean_predicted - cal.observed_rate).mean() > 0.02 else
           "reasonably calibrated") + " as expected from class weighting, so use the tuned threshold rather than raw probabilities.",
    ]
    r.metrics = {"weekly_f1_min": float(wk_f1.min()), "weekly_f1_max": float(wk_f1.max()), "weekly_slope_per_10w": float(slope * 10),
                 "sensor_median_f1": float(sensors.f1.median()), "holdout": hold.set_index("setting").f1.to_dict(),
                 "brier": brier, "ece": ece}
    cols = ["f1", "persistence_f1", "gain_vs_persistence", "precision", "recall", "onset_recall", "transition_accuracy", "positives", "rows"]
    r.table("weeks", weeks[["split", "week"] + cols], "Weekly performance (validation + test)")
    r.table("months", months[["month"] + cols], "Monthly performance")
    r.table("hours", hours[["hour"] + cols], "Performance by hour of day (test)")
    r.table("day_types", daytypes[["day_type"] + cols], "Performance by day type (test)")
    r.table("sensor_holdout", hold, "Sensor hold-out experiment (metrics on hidden sensors' test rows)")
    r.table("calibration", cal, "Reliability by predicted-probability decile (test)")
    r.table("sensors", sensors.sort_values("f1")[["sensor_id", "f1", "precision", "recall", "onset_recall", "positives",
                                                  "rows", "positive_rate"]], "Per-sensor test performance (weakest first)")

    plt, sns = plt_setup()
    fig, axes = plt.subplots(1, 2, figsize=(15, 4.2))
    x = np.arange(len(weeks))
    axes[0].plot(x, weeks.f1, marker="o", color="#4c72b0", label="model")
    axes[0].plot(x, weeks.persistence_f1, marker="o", color="grey", label="persistence")
    split_at = int((weeks.split == "val").sum())
    axes[0].axvline(split_at - 0.5, color="black", ls=":", lw=1)
    axes[0].text(split_at - 0.4, axes[0].get_ylim()[0], " test →", va="bottom")
    axes[0].set_xticks(x, weeks.week.str[5:], rotation=45)
    axes[0].set(title="Weekly F1 (validation → test)", ylabel="F1")
    axes[0].legend()
    axes[1].bar(hours.hour, hours.gain_vs_persistence, color=np.where(hours.gain_vs_persistence >= 0, "#55a868", "#c44e52"))
    ax2 = axes[1].twinx()
    ax2.plot(hours.hour, hours.positives, color="grey", lw=1)
    ax2.set_ylabel("positive targets (test)", color="grey")
    axes[1].set(title="F1 gain over persistence by hour (test)", xlabel="hour", ylabel="ΔF1", xticks=range(0, 24, 2))
    r.figure(fig, "time", "Generalization over time: weekly F1 and hourly gain over persistence")

    fig, axes = plt.subplots(1, 3, figsize=(16, 4.3))
    axes[0].hist(sensors.f1, bins=30, color="#4c72b0", edgecolor="white")
    axes[0].set(title=f"Per-sensor test F1 ({len(sensors)} sensors)", xlabel="F1", ylabel="sensors")
    axes[1].scatter(sensors.positive_rate * 100, sensors.f1, s=12, alpha=0.7)
    axes[1].set(title=f"F1 vs sensor congestion rate (ρ = {sens_corr:.2f})", xlabel="% positive targets", ylabel="F1")
    axes[2].plot([0, 1], [0, 1], color="grey", ls=":")
    axes[2].plot(cal.mean_predicted, cal.observed_rate, marker="o", color="#c44e52")
    axes[2].set(title=f"Calibration (Brier {brier:.3f}, ECE {ece:.3f})", xlabel="mean predicted probability", ylabel="observed rate")
    r.figure(fig, "sensors_calibration", "Generalization across sensors and probability calibration")

    r.implications = [
        "Performance is stable over the validation and test weeks, so a monthly retraining schedule with weekly "
        "monitoring of F1 against persistence is sufficient." if abs(slope * 10) < 0.02 else
        "Weekly F1 drifts; retrain more often and monitor F1 against persistence weekly.",
        "New sensors: the model still beats persistence without sensor history (cold start), but accuracy improves once a "
        "few weeks of history exist for the sensor-statistics and profile features."
        if hd.loc["cold start (no sensor history features)", "f1"] > hd.loc["persistence baseline", "f1"] else
        "For new sensors without history, fall back to persistence until enough history exists.",
        "On the dashboard, show the per-sensor reliability (F1 from this analysis) next to each prediction.",
    ]
    r.limitations = [
        "Six months of data allow only short-term drift analysis; seasonal drift (summer/holidays) is not covered.",
        "The hold-out experiment uses compact models on a small sample; its absolute F1 is lower than Q16's, so compare settings with each other.",
        "Hidden sensors are still neighbours of training sensors, so spatial generalization to a different city is not tested.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
