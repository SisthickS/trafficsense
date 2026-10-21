"""Q4 - Temporal speed volatility.

Usage: python -m src.analytics.q4_speed_volatility
"""
from __future__ import annotations

import sys

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.analytics.common import DOW_NAMES, PM_PEAK, AnalyticsContext, QuestionResult, pct, plt_setup, run_single

SUDDEN = 10.0     # |change| >= 10 mph within 5 minutes


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q4", "Temporal speed volatility",
                       "When and where are speeds most volatile from one 5-minute interval to the next, and is "
                       "volatility linked to congestion?")
    r.method = [
        "5-minute speed change Δ = speed(t) − speed(t−5 min), computed with `lag` over a window partitioned by sensor "
        "and ordered by UTC time, only when both readings are observed.",
        "Volatility = standard deviation of Δ. Also the mean |Δ|, the coefficient of variation of speed, and the share of "
        f"sudden changes (|Δ| ≥ {SUDDEN:.0f} mph).",
        "Aggregated by (working day, hour), by (day of week, hour), and per sensor. Pearson correlation between "
        "per-sensor volatility and congestion rate.",
    ]
    w = Window.partitionBy("sensor_id").orderBy("ts_utc")
    d = (ctx.grid.select("sensor_id", "ts_utc", "hour", "dow", "is_workday", "observed", "speed", "congested")
         .withColumn("prev_obs", F.lag("observed").over(w))
         .withColumn("delta", F.when(F.col("observed") & F.col("prev_obs"), F.col("speed") - F.lag("speed").over(w)))
         .filter(F.col("delta").isNotNull())
         .withColumn("sudden", (F.abs("delta") >= SUDDEN).cast("int"))
         .cache())
    agg = [F.stddev("delta").alias("vol_std_delta"), F.avg(F.abs("delta")).alias("mean_abs_delta"),
           F.avg("sudden").alias("sudden_share"), (F.stddev("speed") / F.avg("speed")).alias("speed_cv"),
           F.count(F.lit(1)).alias("n")]
    hourly = d.groupBy("is_workday", "hour").agg(*agg).toPandas().sort_values(["is_workday", "hour"])
    dowh = d.groupBy("dow", "hour").agg(F.stddev("delta").alias("vol")).toPandas()
    peak = F.col("is_workday") & F.col("hour").between(PM_PEAK[0], PM_PEAK[1] - 1)
    sensor = (d.groupBy("sensor_id").agg(*agg, F.avg("congested").alias("congestion_rate"),
                                         F.stddev(F.when(peak, F.col("delta"))).alias("pm_peak_vol"))
               .toPandas())
    net = d.agg(F.stddev("delta"), F.avg("sudden"), F.count(F.lit(1))).first()
    d.unpersist()

    work = hourly[hourly.is_workday].set_index("hour")
    off = hourly[~hourly.is_workday].set_index("hour")
    peak_h = int(work.vol_std_delta.idxmax())
    quiet = work.loc[1:4, "vol_std_delta"].mean()
    corr = sensor[["vol_std_delta", "congestion_rate"]].corr().iloc[0, 1]
    corr_sp = sensor[["vol_std_delta", "congestion_rate"]].corr(method="spearman").iloc[0, 1]
    top = sensor.sort_values("vol_std_delta", ascending=False).head(10)
    pm_sudden = work.loc[PM_PEAK[0]:PM_PEAK[1] - 1, "sudden_share"].mean()
    night_sudden = work.loc[1:4, "sudden_share"].mean()

    r.findings = [
        f"Across {net[2]:,} consecutive observed pairs, the 5-min speed change has std **{net[0]:.2f} mph**; "
        f"{pct(net[1], 2)} are sudden (|Δ| ≥ {SUDDEN:.0f} mph).",
        f"On working days volatility peaks at **{peak_h:02d}:00** (std Δ {work.vol_std_delta.max():.2f} mph), "
        f"{work.vol_std_delta.max() / quiet:.1f}× the 01–04h level ({quiet:.2f} mph). Weekends peak at "
        f"{int(off.vol_std_delta.idxmax()):02d}:00 with {off.vol_std_delta.max():.2f} mph.",
        f"Sudden changes are {pm_sudden / max(night_sudden, 1e-9):.1f}× more frequent in the working-day PM peak "
        f"({pct(pm_sudden, 2)}) than at night ({pct(night_sudden, 2)}).",
        f"Per sensor, volatility rises with congestion: Pearson r = {corr:.2f} (Spearman {corr_sp:.2f}) between std Δ "
        f"and congestion rate over {len(sensor)} sensors. The most volatile sensor is {int(top.iloc[0].sensor_id)} "
        f"(std Δ {top.iloc[0].vol_std_delta:.2f} mph, {pct(top.iloc[0].congestion_rate)} congested).",
    ]
    r.metrics = {"network_std_delta": float(net[0]), "sudden_share": float(net[1]), "peak_hour": peak_h,
                 "corr_vol_congestion": float(corr), "spearman_vol_congestion": float(corr_sp)}
    r.table("hourly", hourly, "Volatility by working day × hour")
    r.table("top_volatile_sensors", top[["sensor_id", "vol_std_delta", "pm_peak_vol", "mean_abs_delta", "sudden_share",
                                         "speed_cv", "congestion_rate"]], "Ten most volatile sensors")
    r.table("sensors", sensor, "Per-sensor volatility metrics")

    plt, sns = plt_setup()
    fig, ax = plt.subplots(figsize=(11, 3.8))
    ax.plot(work.index, work.vol_std_delta, marker="o", color="#c44e52", label="working days: std Δ")
    ax.plot(off.index, off.vol_std_delta, marker="o", color="#4c72b0", label="weekends/holidays: std Δ")
    ax2 = ax.twinx()
    ax2.bar(work.index, work.sudden_share * 100, alpha=0.2, color="#c44e52", label="working-day sudden changes")
    ax2.set_ylabel(f"% |Δ| ≥ {SUDDEN:.0f} mph")
    ax.set(title="5-minute speed volatility by hour", xlabel="hour (local)", ylabel="std of Δspeed (mph)", xticks=range(24))
    ax.legend(loc="upper left")
    r.figure(fig, "hourly", "Volatility (std of 5-min speed change) and sudden-change share by hour")

    fig, axes = plt.subplots(1, 2, figsize=(14, 4))
    heat = dowh.pivot(index="dow", columns="hour", values="vol").rename(index=DOW_NAMES)
    sns.heatmap(heat, cmap="viridis", ax=axes[0], cbar_kws={"label": "std Δ (mph)"})
    axes[0].set(title="Volatility by day × hour", xlabel="hour", ylabel="")
    axes[1].scatter(sensor.congestion_rate * 100, sensor.vol_std_delta, s=14, alpha=0.7, color="#4c72b0")
    axes[1].set(title=f"Per-sensor volatility vs congestion (r = {corr:.2f})", xlabel="% readings congested", ylabel="std Δ (mph)")
    r.figure(fig, "dow_and_sensors", "Volatility heat-map and per-sensor relation to congestion")

    r.implications = [
        "Volatility is concentrated at congestion onset/clearance times, so the model gets `delta_5m`, `delta_15m`, "
        "`roll_std_60m` and `roll_min_60m` features.",
        "High-volatility sensors are where 5-minute predictions will be hardest; report per-sensor error alongside volatility.",
        "For the dashboard, smooth with a short rolling mean before displaying, to avoid flicker from 5-minute noise.",
    ]
    r.limitations = [
        "Δ is only computed between two consecutive observed readings; forward-filled values are excluded.",
        "Volatility reflects both real traffic dynamics and detector noise, which cannot be separated without ground truth.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
