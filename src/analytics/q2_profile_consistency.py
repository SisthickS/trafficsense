"""Q2 - Consistency of recurring traffic profiles.

Usage: python -m src.analytics.q2_profile_consistency
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.analytics.common import AnalyticsContext, QuestionResult, hhmm, pct, plt_setup, run_single

RECURRENT = 0.5          # a sensor-slot is "recurrently congested" if congested on >= 50% of working days
PRONE = 0.02             # congestion-prone sensor: >= 2% of working-day readings congested


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q2", "Consistency of recurring traffic profiles",
                       "Is congestion recurrent (same place, same time, most working days) or sporadic, and how "
                       "consistent are each sensor's daily speed profiles from day to day?")
    r.method = [
        "Working days only (weekends and federal holidays excluded), observed readings.",
        "`groupBy(sensor, date, slot15).max(congested)` → was the sensor congested in that 15-min slot that day?",
        f"Recurrence of a (sensor, slot15) = share of working days it was congested; ≥ {RECURRENT:.0%} = **recurrent** slot. "
        "Each congested sensor-slot-day is classified as recurrent or non-recurrent (the FHWA recurrent/non-recurrent split).",
        "Profile consistency: hourly mean speed per (sensor, day) vs the sensor's average working-day profile, using "
        "Spark's `corr` aggregate and the RMSE per day, then the median per sensor.",
        "Timing consistency: for congestion-prone sensors, the standard deviation across days of the first congested "
        "15-min slot in the PM window (15–20h).",
    ]
    work = ctx.obs.filter(F.col("is_workday"))
    slot_day = (work.groupBy("sensor_id", "date_local", "slot15").agg(F.max("congested").alias("cong"))).cache()
    recur = slot_day.groupBy("sensor_id", "slot15").agg(F.avg("cong").alias("recurrence"), F.count(F.lit(1)).alias("days"))
    cls = (slot_day.filter("cong = 1").join(recur, ["sensor_id", "slot15"])
           .withColumn("recurrent", (F.col("recurrence") >= RECURRENT).cast("int")))
    network = cls.agg(F.count(F.lit(1)).alias("n"), F.avg("recurrent").alias("share")).first()
    per_sensor = (cls.groupBy("sensor_id").agg(F.count(F.lit(1)).alias("congested_slot_days"),
                                               F.avg("recurrent").alias("recurrent_share")))
    rate = work.groupBy("sensor_id").agg(F.avg("congested").alias("workday_congestion_rate"))

    dh = work.groupBy("sensor_id", "date_local", "hour").agg(F.avg("speed").alias("v"))
    mh = dh.groupBy("sensor_id", "hour").agg(F.avg("v").alias("m"))
    day_fit = (dh.join(mh, ["sensor_id", "hour"]).groupBy("sensor_id", "date_local")
                 .agg(F.corr("v", "m").alias("r"), F.sqrt(F.avg((F.col("v") - F.col("m")) ** 2)).alias("rmse")))
    fit = day_fit.groupBy("sensor_id").agg(F.expr("percentile_approx(r, 0.5)").alias("median_profile_corr"),
                                           F.expr("percentile_approx(rmse, 0.5)").alias("median_profile_rmse_mph"))
    onset = (slot_day.filter("cong = 1 AND slot15 BETWEEN 60 AND 79")
             .groupBy("sensor_id", "date_local").agg(F.min("slot15").alias("first_pm_slot")))
    timing = onset.groupBy("sensor_id").agg((F.stddev("first_pm_slot") * 15).alias("pm_onset_std_min"),
                                             F.count(F.lit(1)).alias("pm_congested_days"),
                                             F.expr("percentile_approx(first_pm_slot, 0.5)").alias("median_pm_onset_slot"))
    sens = (rate.join(per_sensor, "sensor_id", "left").join(fit, "sensor_id", "left").join(timing, "sensor_id", "left")
                .toPandas().sort_values("workday_congestion_rate", ascending=False))
    recur_pd = recur.toPandas()
    slot_day.unpersist()

    prone = sens[sens.workday_congestion_rate >= PRONE].copy()
    prone["median_pm_onset"] = prone["median_pm_onset_slot"].map(lambda s: hhmm(s) if pd.notna(s) else None)
    highly = prone[prone.recurrent_share >= 0.7]
    sporadic = prone[prone.recurrent_share < 0.3]
    quiet = sens[sens.workday_congestion_rate < PRONE]
    best = prone.nlargest(1, "median_profile_corr").iloc[0]

    r.findings = [
        f"**{pct(network.share)}** of all congested sensor-slot-days on working days are **recurrent** (the slot is "
        f"congested on ≥ {RECURRENT:.0%} of working days); the remaining {pct(1 - network.share)} are non-recurrent "
        f"(incidents, special events, demand spikes). Total: {network.n:,} congested sensor-slot-days.",
        f"Of {len(prone)} congestion-prone sensors (≥ {PRONE:.0%} of working-day readings congested), {len(highly)} are "
        f"highly recurrent (≥ 70% of their congestion is recurrent) and {len(sporadic)} are mostly sporadic (< 30%).",
        f"Daily speed profiles of congestion-prone sensors closely follow their average profile: median day-vs-average "
        f"correlation {prone.median_profile_corr.median():.2f}, median RMSE {prone.median_profile_rmse_mph.median():.1f} mph.",
        f"PM congestion starts at a consistent time: median day-to-day standard deviation of the onset is "
        f"{prone.pm_onset_std_min.median():.0f} minutes (sensor {int(best.sensor_id)} is the most consistent profile, "
        f"r = {best.median_profile_corr:.2f}).",
        f"{len(quiet)} sensors rarely congest (< {PRONE:.0%}); their profiles are flat, so correlation is not meaningful for them.",
    ]
    r.metrics = {"recurrent_share": float(network.share), "congested_slot_days": int(network.n),
                 "prone_sensors": len(prone), "highly_recurrent_sensors": len(highly),
                 "median_profile_corr_prone": float(prone.median_profile_corr.median()),
                 "median_pm_onset_std_min": float(prone.pm_onset_std_min.median())}

    cols = ["sensor_id", "workday_congestion_rate", "congested_slot_days", "recurrent_share", "median_profile_corr",
            "median_profile_rmse_mph", "pm_congested_days", "median_pm_onset", "pm_onset_std_min"]
    r.table("prone_sensors", prone[cols], f"Congestion-prone sensors (≥ {PRONE:.0%} of working-day readings), most congested first")
    r.table("all_sensors", sens, "Consistency metrics for all sensors")

    plt, sns = plt_setup()
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.hist(prone.recurrent_share.dropna() * 100, bins=20, color="#4c72b0", edgecolor="white")
    ax.axvline(network.share * 100, color="#c44e52", ls="--", label=f"network {pct(network.share)}")
    ax.set(title="Share of each sensor's congestion that is recurrent (congestion-prone sensors)",
           xlabel="% of congested slot-days that are recurrent", ylabel="sensors")
    ax.legend()
    r.figure(fig, "recurrent_share_hist", "Recurrent share of congestion per sensor")

    top = prone.head(20).sensor_id.tolist()
    mat = (recur_pd[recur_pd.sensor_id.isin(top) & recur_pd.slot15.between(20, 83)]
           .pivot(index="sensor_id", columns="slot15", values="recurrence").reindex(top))
    fig, ax = plt.subplots(figsize=(14, 6))
    sns.heatmap(mat * 100, cmap="magma_r", ax=ax, vmin=0, vmax=100, cbar_kws={"label": "% of working days congested"})
    ax.set_xticks(np.arange(0, mat.shape[1], 4) + 0.5, [hhmm(s) for s in mat.columns[::4]], rotation=0)
    ax.set(title="Recurrence of congestion by 15-min slot (20 most congested sensors, working days)", xlabel="local time", ylabel="sensor")
    r.figure(fig, "recurrence_heatmap", "Recurrence heat-map: dark = congested on most working days at that time")

    sid = int(best.sensor_id)
    prof = dh.filter(F.col("sensor_id") == sid).toPandas()
    fig, ax = plt.subplots(figsize=(10, 3.8))
    for _, d in prof.groupby("date_local"):
        d = d.sort_values("hour")
        ax.plot(d.hour, d.v, color="grey", alpha=0.15, lw=0.8)
    mean = prof.groupby("hour").v.mean()
    ax.plot(mean.index, mean.values, color="#c44e52", lw=2.5, label="average working day")
    ax.axhline(35, color="black", ls=":", lw=1, label="35 mph threshold")
    ax.set(title=f"Sensor {sid}: every working day's hourly speed profile", xlabel="hour (local)", ylabel="mph", xticks=range(0, 24, 2))
    ax.legend()
    r.figure(fig, "example_profiles", f"Day-to-day consistency of sensor {sid}")

    r.implications = [
        "Most congestion is recurrent, which is why the historical slot profile features (`hist_speed_next`, "
        "`hist_congested_rate_next`) and calendar features carry strong signal.",
        "The non-recurrent share is what a calendar-only model cannot anticipate; real-time features (lags, "
        "neighbours, rolling statistics) must capture it. Evaluate the model on these unusual events separately.",
        "Highly recurrent sensors are candidates for schedule-based alerts on the dashboard. Sporadic sensors need live detection.",
    ]
    r.limitations = [
        "A 15-min slot counts as congested if any of its three readings is below 35 mph.",
        "The 50% recurrence cut-off is a convention; the per-sensor shares in the CSV allow other cut-offs.",
        "Incidents are not labelled in PEMS-BAY, so non-recurrent congestion is identified only statistically.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
