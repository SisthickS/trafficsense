"""Q13 - Missing data and sensor coverage bias.

Usage: python -m src.analytics.q13_missing_coverage
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.analytics.common import THR, AnalyticsContext, QuestionResult, pct, plt_setup, run_single

GAP_KM = 3.0


def haversine_matrix(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    la, lo = np.radians(lat)[:, None], np.radians(lon)[:, None]
    d = np.sin((la - la.T) / 2) ** 2 + np.cos(la) * np.cos(la.T) * np.sin((lo - lo.T) / 2) ** 2
    return 2 * 6371 * np.arcsin(np.sqrt(d))


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q13", "Missing data and sensor coverage bias",
                       "Where and when are readings missing or invalid, is missingness related to congestion (so that "
                       "rates and models are biased), and how evenly do sensors cover the network?")
    r.method = [
        "Quality flags from cleaning (Phase 3): observed / imputed (forward-filled ≤ 15 min) / missing, and the reason "
        "(zero reading, stuck detector, ...). Shares by hour, by date and by sensor.",
        "Missing-not-at-random check: for each (sensor, day, hour) cell, compare the congestion rate of its observed readings "
        "when the cell has invalid readings vs when it is complete, **stratified by hour** (difference averaged over hours).",
        "Label loss: stuck-detector readings whose raw value was below 35 mph (removed as stuck, possibly true congestion).",
        "Model coverage: share of the clean grid kept in the feature table, by hour and split, and the congestion rate of "
        "dropped vs kept observed rows (excluding the structural 7-day warm-up).",
        f"Spatial coverage: great-circle distance from each sensor to its nearest **road-graph** neighbour (either direction); "
        f"co-located sensors (< 100 m, usually the opposite carriageway) are counted separately. Sensors without a graph "
        f"neighbour within {GAP_KM:.0f} km are isolated.",
    ]
    g = ctx.grid
    q = g.groupBy("quality").count().toPandas().set_index("quality")["count"]
    issues = g.filter(F.col("issue").isNotNull()).groupBy("issue", "quality").count().toPandas()
    by_hour = (g.groupBy("is_workday", "hour").agg(F.avg((~F.col("observed")).cast("int")).alias("not_observed_share"),
                                                   F.avg((F.col("quality") == "missing").cast("int")).alias("missing_share"))
                .toPandas().sort_values(["is_workday", "hour"]))
    by_date = (g.groupBy("date_local").agg(F.avg((~F.col("observed")).cast("int")).alias("not_observed_share"),
                                           F.countDistinct(F.when(~F.col("observed"), F.col("sensor_id"))).alias("sensors_affected"))
                .toPandas().sort_values("date_local"))
    by_sensor = (g.groupBy("sensor_id").agg(F.avg((~F.col("observed")).cast("int")).alias("not_observed_share"),
                                            F.avg((F.col("issue") == "stuck_sensor").cast("int")).alias("stuck_share"),
                                            F.avg((F.col("issue") == "non_positive").cast("int")).alias("zero_share"),
                                            F.avg("congested").alias("congestion_rate"))
                  .toPandas())
    cells = (g.groupBy("sensor_id", "date_local", "hour")
              .agg(F.max((~F.col("observed")).cast("int")).alias("has_invalid"), F.avg("congested").alias("rate"),
                   F.count("congested").alias("observed_n"))
              .filter("observed_n > 0").groupBy("hour", "has_invalid")
              .agg(F.avg("rate").alias("rate"), F.count(F.lit(1)).alias("cells")).toPandas())
    lost = g.filter((F.col("issue") == "stuck_sensor") & (F.col("speed_raw") < THR) & (F.col("speed_raw") > 0)).count()
    stuck_total = int(issues.loc[issues.issue == "stuck_sensor", "count"].sum())
    total_cong = g.agg(F.sum("congested")).first()[0]

    feats = ctx.spark.read.parquet(str(ctx.base / "features")).select("sensor_id", "ts_utc", "split")
    start = g.agg(F.min("ts_utc")).first()[0]
    cov = (g.join(feats, ["sensor_id", "ts_utc"], "left")
            .withColumn("kept", F.col("split").isNotNull())
            .withColumn("warmup", F.col("ts_utc") < F.lit(start) + F.expr("INTERVAL 7 DAYS 5 MINUTES")))
    cov_hour = (cov.filter(~F.col("warmup")).groupBy("hour").agg(F.avg(F.col("kept").cast("int")).alias("kept_share"))
                   .toPandas().sort_values("hour"))
    bias = (cov.filter(~F.col("warmup") & F.col("observed")).groupBy("kept")
               .agg(F.avg("congested").alias("congestion_rate"), F.count(F.lit(1)).alias("rows")).toPandas().set_index("kept"))

    piv = cells.pivot(index="hour", columns="has_invalid", values="rate").dropna()
    w = cells[cells.has_invalid == 1].set_index("hour").cells.reindex(piv.index)
    mnar_diff = float(np.average(piv[1] - piv[0], weights=w)) if len(piv) else float("nan")
    base_rate = float(np.average(piv[0], weights=w)) if len(piv) else float("nan")
    mnar_rel = mnar_diff / base_rate if base_rate else float("nan")
    mnar_level = "no material" if abs(mnar_rel) < 0.10 else "a modest" if abs(mnar_rel) < 0.30 else "a strong"

    meta = ctx.sensors_pd
    D = haversine_matrix(meta.latitude.to_numpy(), meta.longitude.to_numpy())
    np.fill_diagonal(D, np.inf)
    ids, A = ctx.adjacency
    order = {int(s): i for i, s in enumerate(ids)}
    idx = meta.sensor_id.map(order).to_numpy()
    D_graph = D[np.ix_(idx, idx)]
    linked = (A + A.T)[np.ix_(idx, idx)] > 0
    nn_graph = np.where(linked, D_graph, np.inf).min(axis=1)
    meta = meta.assign(nn_any_km=D.min(axis=1), nn_km=nn_graph, colocated=D.min(axis=1) < 0.1)
    isolated = meta[~np.isfinite(meta.nn_km) | (meta.nn_km > GAP_KM)]
    by_sensor = by_sensor.merge(meta[["sensor_id", "nn_km", "latitude", "longitude"]], on="sensor_id")
    rho = by_sensor[["not_observed_share", "congestion_rate"]].corr(method="spearman").iloc[0, 1]
    total = q.sum()
    wkh = by_hour[by_hour.is_workday].set_index("hour")
    top_sensors = by_sensor.sort_values("not_observed_share", ascending=False).head(15)
    drop_rate, keep_rate = bias.loc[False, "congestion_rate"], bias.loc[True, "congestion_rate"]

    r.findings = [
        f"Data are {pct(q.get('observed', 0) / total, 2)} observed, {pct(q.get('imputed', 0) / total, 3)} forward-filled and "
        f"{pct(q.get('missing', 0) / total, 2)} missing. Almost all invalid readings come from stuck detectors "
        f"({stuck_total:,}), and they are concentrated: the worst 15 sensors hold "
        f"{pct(top_sensors.not_observed_share.sum() / by_sensor.not_observed_share.sum())} of all invalid readings.",
        f"Missingness by time: working-day share ranges {pct(wkh.not_observed_share.min(), 2)}–{pct(wkh.not_observed_share.max(), 2)} by "
        f"hour (highest at {int(wkh.not_observed_share.idxmax()):02d}:00); worst day {by_date.loc[by_date.not_observed_share.idxmax(), 'date_local']} "
        f"({pct(by_date.not_observed_share.max(), 2)}, {int(by_date.sensors_affected.max())} sensors affected on the worst day).",
        (f"There is {mnar_level} missing-not-at-random effect: hour-cells containing invalid readings have a congestion "
         f"rate {mnar_diff * 100:+.2f} pp ({mnar_rel:+.0%} relative) different from complete cells at the same hour "
         f"(base {pct(base_rate, 2)})" + ("; invalid readings occur somewhat more often around congestion." if mnar_rel >= 0.10 else
                                          "; invalid readings occur somewhat more often in free flow." if mnar_rel <= -0.10 else "."))
        + f" Across sensors, Spearman ρ between invalid share and congestion rate = {rho:.2f}.",
        f"Label loss: {lost:,} stuck readings were below 35 mph ({pct(lost / max(stuck_total, 1))} of stuck readings, "
        f"{pct(lost / (total_cong + lost), 2)} of all congested readings). They were removed as implausible flat lines "
        "and slightly understate congestion.",
        f"Model coverage after the warm-up: the feature table keeps {pct(bias.rows.sum() and bias.loc[True, 'rows'] / bias.rows.sum(), 2)} "
        f"of observed rows. Dropped rows have congestion rate {pct(drop_rate, 2)} vs {pct(keep_rate, 2)} kept, "
        + ("a bias towards " + ("congested" if drop_rate > keep_rate else "free-flow") + " periods in what the model never sees."
           if abs(drop_rate - keep_rate) > 0.01 else "so dropping them does not bias the class balance."),
        f"Spatial coverage: {int(meta.colocated.sum())} sensors have another sensor within 100 m (typically the opposite "
        f"carriageway). Spacing to the nearest **road-graph** neighbour is a median {np.median(meta.nn_km[np.isfinite(meta.nn_km)]):.2f} km "
        f"(max {meta.nn_km[np.isfinite(meta.nn_km)].max():.1f} km); "
        + (f"{len(isolated)} sensors have no graph neighbour within {GAP_KM:.0f} km, so congestion on those stretches is "
           "observed at a single point." if len(isolated) else f"no sensor is more than {GAP_KM:.0f} km from a graph neighbour, "
           "so the network inside the study area is evenly covered."),
    ]
    r.metrics = {"observed_share": float(q.get("observed", 0) / total), "mnar_diff_pp": mnar_diff * 100,
                 "stuck_congested_lost": int(lost), "isolated_sensors": len(isolated),
                 "median_graph_spacing_km": float(np.median(meta.nn_km[np.isfinite(meta.nn_km)])), "colocated_sensors": int(meta.colocated.sum())}
    r.table("quality", q.rename_axis("quality").reset_index(name="readings").assign(share=lambda d: d.readings / d.readings.sum()),
            "Final quality of the 16.9M grid readings")
    r.table("issues", issues, "Invalid-reading reasons and how they were resolved")
    r.table("worst_sensors", top_sensors[["sensor_id", "not_observed_share", "stuck_share", "zero_share", "congestion_rate", "nn_km"]],
            "Fifteen sensors with the most invalid readings")
    r.table("mnar_by_hour", piv.reset_index().rename(columns={0: "rate_complete_cells", 1: "rate_cells_with_invalid"}),
            "Congestion rate of observed readings in hour-cells with vs without invalid readings")
    r.table("model_coverage", bias.reset_index().assign(kept=lambda d: d.kept.map({True: "kept in features", False: "dropped"})),
            "Observed rows kept vs dropped by the feature table (after warm-up)")
    r.table("isolated_sensors", isolated[["sensor_id", "latitude", "longitude", "nn_km"]].sort_values("nn_km", ascending=False),
            f"Sensors more than {GAP_KM:.0f} km from their nearest neighbour")

    plt, sns = plt_setup()
    fig, axes = plt.subplots(1, 3, figsize=(18, 4.4))
    for wd, col, lab in ((True, "#c44e52", "working days"), (False, "#4c72b0", "weekends/holidays")):
        h = by_hour[by_hour.is_workday == wd]
        axes[0].plot(h.hour, h.not_observed_share * 100, marker="o", color=col, label=lab)
    axes[0].set(title="Invalid readings by hour", xlabel="hour", ylabel="% not observed", xticks=range(0, 24, 2))
    axes[0].legend()
    axes[1].plot(pd.to_datetime(by_date.date_local), by_date.not_observed_share * 100, color="grey")
    axes[1].set(title="Invalid readings by day", ylabel="% not observed")
    axes[1].tick_params(axis="x", rotation=30)
    ts = top_sensors.set_index(top_sensors.sensor_id.astype(str))
    (ts[["stuck_share", "zero_share"]] * 100).plot.barh(stacked=True, ax=axes[2], color=["#8172b2", "#dd8452"])
    axes[2].invert_yaxis()
    axes[2].set(title="Sensors with most invalid readings", xlabel="% of readings", ylabel="")
    fig.tight_layout()
    r.figure(fig, "missingness", "When and where readings are missing or invalid")

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
    axes[0].hist(meta.nn_km[np.isfinite(meta.nn_km)], bins=40, color="#4c72b0", edgecolor="white")
    axes[0].axvline(GAP_KM, color="#c44e52", ls="--", label=f"{GAP_KM:.0f} km")
    axes[0].set(title="Distance to nearest road-graph neighbour", xlabel="km (straight line)", ylabel="sensors")
    axes[0].legend()
    sc = axes[1].scatter(by_sensor.longitude, by_sensor.latitude, c=by_sensor.not_observed_share * 100, cmap="Reds",
                         s=10 + 400 * by_sensor.not_observed_share, edgecolor="grey", linewidth=0.3)
    axes[1].scatter(isolated.longitude, isolated.latitude, facecolor="none", edgecolor="black", s=80, label="isolated")
    fig.colorbar(sc, ax=axes[1], label="% invalid readings")
    axes[1].set(title="Data quality and coverage gaps", xlabel="longitude", ylabel="latitude")
    axes[1].legend()
    r.figure(fig, "coverage", "Spatial coverage and per-sensor data quality")

    r.implications = [
        "**Recommendation:** send the worst stuck-detector sensors for maintenance first; a handful of detectors cause most invalid data.",
        "The cleaning rule for stuck congested readings is conservative; report congestion rates with that small known "
        "under-count, and keep the `quality` flag visible in the dashboard.",
        "Evaluate the model separately for low-quality sensors; their feature gaps reduce coverage.",
        "Isolated sensors represent long road sections. New detectors or probe data (GPS) would close the coverage gaps.",
    ]
    r.limitations = [
        "The true speed during invalid periods is unknown, so the MNAR test can only use the observed readings around them.",
        "Spatial coverage uses straight-line distance; road distance between sensors is longer.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
