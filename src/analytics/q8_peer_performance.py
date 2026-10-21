"""Q8 - Sensor performance relative to peers.

Peers = the sensor's road-graph neighbours. The adjacency-weighted upstream/downstream neighbour speeds were
computed in Phase 3 (features/: nbr_up_speed, nbr_down_speed, ...), so this question reads the feature table.

Usage: python -m src.analytics.q8_peer_performance
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.analytics.common import PM_PEAK, AnalyticsContext, QuestionResult, pct, plt_setup, run_single

Z_FLAG = 1.5
LOW_AGREEMENT = 0.6


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q8", "Sensor performance relative to peers",
                       "Which sensors are consistently slower or more congested than their neighbouring sensors "
                       "(local bottlenecks), and which disagree with their peers in ways that suggest detector problems?")
    r.method = [
        "Feature table (all splits, 16.1M rows). Peer speed = mean of the adjacency-weighted upstream and downstream "
        "neighbour speeds (only the existing side for sensors without upstream or downstream neighbours; sensors with "
        "neither are excluded).",
        "Per sensor: mean residual (speed − peer speed) overall and in the working-day PM peak; mean |residual|; "
        "Pearson correlation of speed with peer speed (Spark `corr` aggregate); own congestion rate vs peer congestion share.",
        f"Peer z-score of the PM-peak residual across sensors. **Local bottleneck**: z ≤ −{Z_FLAG} and own congestion ≥ "
        f"1.5× the peers'. **Low peer agreement**: correlation < {LOW_AGREEMENT}, cross-checked against the share of "
        "readings that were not observed (missing or stuck) in cleaning.",
    ]
    meta = ctx.sensors_pd[["sensor_id", "in_degree", "out_degree", "latitude", "longitude"]]
    deg = ctx.spark.createDataFrame(meta[["sensor_id", "in_degree", "out_degree"]])
    f = (ctx.spark.read.parquet(str(ctx.base / "features"))
            .select("sensor_id", "speed", "nbr_up_speed", "nbr_down_speed", "congested_now", "nbr_up_congested_frac",
                    "nbr_down_congested_frac", "hour", "is_weekend", "is_holiday")
            .join(F.broadcast(deg), "sensor_id"))
    up, down = F.col("in_degree") > 0, F.col("out_degree") > 0
    peer = (F.when(up & down, (F.col("nbr_up_speed") + F.col("nbr_down_speed")) / 2)
             .when(up, F.col("nbr_up_speed")).when(down, F.col("nbr_down_speed")))
    peer_c = (F.when(up & down, (F.col("nbr_up_congested_frac") + F.col("nbr_down_congested_frac")) / 2)
               .when(up, F.col("nbr_up_congested_frac")).when(down, F.col("nbr_down_congested_frac")))
    pm = (F.col("is_weekend") == 0) & (F.col("is_holiday") == 0) & F.col("hour").between(PM_PEAK[0], PM_PEAK[1] - 1)
    f = f.withColumn("peer", peer).withColumn("peer_c", peer_c).filter(F.col("peer").isNotNull()) \
         .withColumn("resid", F.col("speed") - F.col("peer"))
    per = (f.groupBy("sensor_id").agg(
        F.avg("resid").alias("mean_residual_mph"), F.avg(F.when(pm, F.col("resid"))).alias("pm_residual_mph"),
        F.avg(F.abs("resid")).alias("mean_abs_residual_mph"), F.corr("speed", "peer").alias("peer_corr"),
        F.avg("congested_now").alias("congestion_rate"), F.avg("peer_c").alias("peer_congestion"),
        F.avg(F.when(pm, F.col("congested_now"))).alias("pm_congestion_rate"),
        F.avg(F.when(pm, F.col("peer_c"))).alias("pm_peer_congestion"), F.count(F.lit(1)).alias("rows"))
        .toPandas())
    quality = (ctx.grid.groupBy("sensor_id").agg(F.avg((~F.col("observed")).cast("int")).alias("not_observed_share"))
                  .toPandas())
    s = per.merge(meta, on="sensor_id").merge(quality, on="sensor_id", how="left")
    excluded = len(meta) - len(s)
    s["pm_residual_z"] = (s.pm_residual_mph - s.pm_residual_mph.mean()) / s.pm_residual_mph.std()
    s["congestion_vs_peers"] = (s.pm_congestion_rate + 0.005) / (s.pm_peer_congestion + 0.005)
    s["flag"] = np.select(
        [(s.pm_residual_z <= -Z_FLAG) & (s.congestion_vs_peers >= 1.5), s.peer_corr < LOW_AGREEMENT, s.pm_residual_z >= Z_FLAG],
        ["local bottleneck", "low peer agreement", "faster than peers"], "in line with peers")
    s = s.sort_values("pm_residual_z")
    bott = s[s.flag == "local bottleneck"]
    low = s[s.flag == "low peer agreement"]
    rest = s[s.flag != "low peer agreement"]
    rho_q = s[["peer_corr", "not_observed_share"]].corr(method="spearman").iloc[0, 1]
    ratio_q = low.not_observed_share.mean() / max(rest.not_observed_share.mean(), 1e-12)
    quality_linked = abs(rho_q) >= 0.3 and ratio_q >= 1.5
    faster = s[s.flag == "faster than peers"]
    edges = ctx.edges_pd()
    bott_ids = set(bott.sensor_id)
    after_bottleneck = faster.sensor_id.isin(set(edges[edges.src.isin(bott_ids)].dst)).sum()

    r.findings = [
        f"Most sensors track their peers closely: median correlation with peer speed **{s.peer_corr.median():.2f}**, median "
        f"|residual| {s.mean_abs_residual_mph.median():.1f} mph ({excluded} isolated sensors without graph neighbours excluded).",
        f"**{len(bott)} local bottlenecks** are much slower than their neighbours in the PM peak (z ≤ −{Z_FLAG}) and "
        "congested ≥ 1.5× as often" + (f"; worst {int(bott.iloc[0].sensor_id)}: {bott.iloc[0].pm_residual_mph:+.1f} mph vs peers, "
        f"{pct(bott.iloc[0].pm_congestion_rate)} vs {pct(bott.iloc[0].pm_peer_congestion)} congested." if len(bott) else "."),
        (f"{len(low)} sensors have low agreement with their peers (r < {LOW_AGREEMENT}) and clearly worse data quality "
         f"({pct(low.not_observed_share.mean(), 2)} vs {pct(rest.not_observed_share.mean(), 2)} non-observed readings; "
         f"Spearman ρ = {rho_q:.2f}). This points to detector issues.") if quality_linked else
        (f"{len(low)} sensors have low agreement with their peers (r < {LOW_AGREEMENT}). Their data quality is only marginally "
         f"different ({pct(low.not_observed_share.mean(), 2)} vs {pct(rest.not_observed_share.mean(), 2)} non-observed; "
         f"Spearman ρ = {rho_q:.2f}), so low agreement more likely reflects genuinely local traffic conditions than "
         "detector faults, though detector drift cannot be ruled out."),
        f"{len(faster)} sensors are consistently faster than their peers (z ≥ +{Z_FLAG}); {after_bottleneck} of them are "
        "directly downstream of a flagged bottleneck, where traffic discharges after the restriction.",
    ]
    r.metrics = {"median_peer_corr": float(s.peer_corr.median()), "bottlenecks": len(bott), "low_agreement": len(low),
                 "excluded_isolated": excluded, "spearman_corr_vs_quality": float(rho_q), "quality_linked": bool(quality_linked),
                 "faster_downstream_of_bottleneck": int(after_bottleneck)}
    cols = ["sensor_id", "flag", "pm_residual_mph", "pm_residual_z", "pm_congestion_rate", "pm_peer_congestion",
            "congestion_vs_peers", "peer_corr", "mean_abs_residual_mph", "not_observed_share"]
    r.table("bottlenecks", bott[cols], "Local bottlenecks (slower and more congested than peers in the PM peak)")
    r.table("low_agreement", low.sort_values("peer_corr")[cols], "Sensors with low agreement with their peers")
    r.table("flags", s.flag.value_counts().rename_axis("flag").reset_index(name="sensors"), "Sensor classification")
    r.table("all_sensors", s, "Peer metrics for all sensors")

    plt, sns = plt_setup()
    colors = {"local bottleneck": "#c44e52", "low peer agreement": "#8172b2", "faster than peers": "#55a868",
              "in line with peers": "#bdbdbd"}
    fig, axes = plt.subplots(1, 2, figsize=(15, 5.5))
    for flag, g in s.groupby("flag"):
        axes[0].scatter(g.peer_corr, g.pm_residual_mph, s=16, color=colors[flag], label=f"{flag} ({len(g)})", alpha=0.85)
        axes[1].scatter(g.longitude, g.latitude, s=18, color=colors[flag], alpha=0.85)
    axes[0].axhline(0, color="black", lw=0.8)
    axes[0].axvline(LOW_AGREEMENT, color="grey", ls=":", lw=1)
    axes[0].set(title="PM-peak residual vs agreement with peers", xlabel="correlation with peer speed", ylabel="speed − peer speed (mph)")
    axes[0].legend(fontsize=8)
    axes[1].set(title="Where the flagged sensors are", xlabel="longitude", ylabel="latitude")
    r.figure(fig, "peer_scatter_map", "Peer-relative performance and sensor flags")

    r.implications = [
        "Local bottlenecks are where congestion **originates**; they are the natural anchor points for early warnings "
        "to adjacent sensors (Q7).",
        "Low-agreement sensors should be marked on the dashboard and their model errors read with care. The "
        "`imputed_count_60m` feature already gives the model a data-quality signal.",
    ]
    r.limitations = [
        "Peer speeds are adjacency-weighted means; a sensor at a true bottleneck has slower upstream and faster downstream "
        "neighbours, which partly cancel in the average.",
        "The feature table excludes the first 7 days (lag warm-up) and rows with gaps; cleaning-stage missingness is used "
        "for the quality cross-check.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
