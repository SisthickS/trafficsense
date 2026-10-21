"""Q6 - Congestion by road characteristics.

PEMS-BAY ships no road attributes (no lane counts, road class, ramps), so characteristics are DERIVED from what
the data does contain: train-period free-flow speed (design/geometry proxy), road-graph sensor density, the
upstream/downstream balance of the graph (merge- vs diverge-like position) and geographic corridor clusters
(Spark MLlib KMeans on projected coordinates).

Usage: python -m src.analytics.q6_road_characteristics
"""
from __future__ import annotations

import math
import sys

import numpy as np
import pandas as pd
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from src.analytics.common import PM_PEAK, AnalyticsContext, QuestionResult, eta_squared, pct, plt_setup, run_single

COMPASS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q6", "Congestion by road characteristics",
                       "Do sensors with different road characteristics (design speed, network density, merge/diverge "
                       "position, corridor) experience systematically different congestion?")
    r.method = [
        "Per-sensor working-day congestion rate and PM-peak congestion rate (observed readings).",
        "Derived characteristics (PEMS-BAY has no lane or road-class data): **free-flow band** = quartile of the "
        "train-period 85th-percentile speed; **network density** = tercile of in+out graph degree (sensors within "
        "road-network reach); **graph position** = in-degree vs out-degree (merge-like / diverge-like / balanced); "
        "**corridor** = Spark MLlib KMeans on coordinates projected to km, k chosen by silhouette score (3–6).",
        "Per characteristic: group means/medians and η² (share of variance in the PM-peak rate explained by the group); "
        "Spearman correlation for the continuous variables.",
    ]
    work = ctx.obs.filter(F.col("is_workday"))
    pm = F.col("hour").between(PM_PEAK[0], PM_PEAK[1] - 1)
    cong = (work.groupBy("sensor_id").agg(F.avg("congested").alias("congestion_rate"),
                                          F.avg(F.when(pm, F.col("congested"))).alias("pm_peak_rate"),
                                          F.avg("speed").alias("mean_speed")).toPandas())
    s = ctx.sensors_pd.merge(cong, on="sensor_id")
    lat0 = s.latitude.mean()
    s["x_km"] = (s.longitude - s.longitude.mean()) * 111.32 * math.cos(math.radians(lat0))
    s["y_km"] = (s.latitude - lat0) * 110.57

    sdf = ctx.spark.createDataFrame(s[["sensor_id", "x_km", "y_km"]])
    vec = VectorAssembler(inputCols=["x_km", "y_km"], outputCol="features").transform(sdf).cache()
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette")
    scores = {}
    models = {}
    for k in range(3, 7):
        m = KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster").fit(vec)
        scores[k] = evaluator.evaluate(m.transform(vec))
        models[k] = m
    best_k = max(scores, key=scores.get)
    clusters = models[best_k].transform(vec).select("sensor_id", "cluster").toPandas()
    vec.unpersist()
    s = s.merge(clusters, on="sensor_id")
    names = {}
    for c, g in s.groupby("cluster"):
        cx, cy = g.x_km.mean(), g.y_km.mean()
        dist = math.hypot(cx, cy)
        bearing = (math.degrees(math.atan2(cx, cy)) + 360) % 360
        names[c] = "Central" if dist < 3 else f"{COMPASS[int((bearing + 22.5) // 45) % 8]} ({dist:.0f} km)"
    s["corridor"] = s.cluster.map(lambda c: f"C{c + 1} {names[c]}")

    s["free_flow_band"] = pd.qcut(s.ff_speed, 4, labels=["Q1 lowest ff", "Q2", "Q3", "Q4 highest ff"])
    s["degree"] = s.in_degree + s.out_degree
    s["network_density"] = pd.qcut(s.degree.rank(method="first"), 3, labels=["sparse", "medium", "dense"])
    bal = s.in_degree - s.out_degree
    s["graph_position"] = np.select([bal >= 2, bal <= -2], ["merge-like (in > out)", "diverge-like (out > in)"], "balanced")

    chars = {"free_flow_band": "Free-flow speed band", "network_density": "Network density",
             "graph_position": "Graph position", "corridor": "Corridor (KMeans)"}
    groups, effects = [], []
    for col, label in chars.items():
        g = (s.groupby(col, observed=True).agg(sensors=("sensor_id", "size"), mean_congestion_rate=("congestion_rate", "mean"),
                                               median_congestion_rate=("congestion_rate", "median"),
                                               mean_pm_peak_rate=("pm_peak_rate", "mean"),
                                               mean_ff_speed=("ff_speed", "mean"), mean_speed=("mean_speed", "mean"))
             .reset_index().rename(columns={col: "group"}))
        g.insert(0, "characteristic", label)
        groups.append(g)
        effects.append({"characteristic": label, "groups": len(g), "eta_squared_pm_peak": eta_squared(s, col, "pm_peak_rate"),
                        "eta_squared_overall": eta_squared(s, col, "congestion_rate"),
                        "highest_group": g.loc[g.mean_pm_peak_rate.idxmax(), "group"],
                        "lowest_group": g.loc[g.mean_pm_peak_rate.idxmin(), "group"]})
    groups = pd.concat(groups, ignore_index=True)
    effects = pd.DataFrame(effects).sort_values("eta_squared_pm_peak", ascending=False)
    rho_ff = s[["ff_speed", "pm_peak_rate"]].corr(method="spearman").iloc[0, 1]
    rho_deg = s[["degree", "pm_peak_rate"]].corr(method="spearman").iloc[0, 1]
    strongest = effects.iloc[0]
    ffg = groups[groups.characteristic == "Free-flow speed band"].set_index("group")

    max_eta = effects.eta_squared_pm_peak.max()
    weak = max_eta < 0.06    # Cohen: eta^2 < 0.06 is a small effect
    rho_word = lambda rho: "no meaningful" if abs(rho) < 0.1 else ("a weak" if abs(rho) < 0.3 else "a moderate")
    r.findings = [
        (f"**None of the derived characteristics explains much of the congestion differences**: the largest η² is "
         f"{max_eta:.3f} ({strongest.characteristic}), a small effect, and all four lie between "
         f"{effects.eta_squared_pm_peak.min():.3f} and {max_eta:.3f}. Congestion is site-specific rather than determined by "
         "these coarse attributes.") if weak else
        (f"The strongest characteristic is **{strongest.characteristic}** (η² = {max_eta:.2f} of the variance in PM-peak "
         "congestion); the others explain " + ", ".join(f"{row.characteristic} {row.eta_squared_pm_peak:.2f}"
                                                       for row in effects.iloc[1:].itertuples()) + "."),
        f"Free-flow speed band: {pct(ffg.loc['Q1 lowest ff', 'mean_pm_peak_rate'])} PM-peak congestion in the lowest band vs "
        f"{pct(ffg.loc['Q4 highest ff', 'mean_pm_peak_rate'])} in the highest; Spearman ρ = {rho_ff:.2f}, i.e. "
        f"{rho_word(rho_ff)} monotonic relation.",
        f"Network density: Spearman ρ = {rho_deg:.2f} between graph degree and PM-peak congestion ({rho_word(rho_deg)} "
        f"relation); the highest graph-position group is "
        f"'{effects.set_index('characteristic').loc['Graph position', 'highest_group']}'.",
        f"KMeans found {best_k} corridors (silhouette {scores[best_k]:.2f}); most congested: "
        f"{effects.set_index('characteristic').loc['Corridor (KMeans)', 'highest_group']}, least congested: "
        f"{effects.set_index('characteristic').loc['Corridor (KMeans)', 'lowest_group']} (η² "
        f"{effects.set_index('characteristic').loc['Corridor (KMeans)', 'eta_squared_pm_peak']:.3f}).",
    ]
    r.metrics = {"best_k": best_k, "silhouette": scores, "spearman_ff_pm": float(rho_ff), "spearman_degree_pm": float(rho_deg),
                 "strongest_characteristic": strongest.characteristic, "eta_squared": dict(zip(effects.characteristic, effects.eta_squared_pm_peak))}
    r.table("effects", effects, "How much of the PM-peak congestion variance each characteristic explains (η²)")
    r.table("groups", groups, "Congestion by characteristic group")
    r.table("kmeans_silhouette", pd.DataFrame({"k": list(scores), "silhouette": list(scores.values())}), "KMeans model selection")
    r.table("sensors", s.drop(columns=["x_km", "y_km", "cluster"]), "Per-sensor characteristics and congestion")

    plt, sns = plt_setup()
    fig, axes = plt.subplots(2, 2, figsize=(14, 8))
    for ax, (col, label) in zip(axes.ravel(), chars.items()):
        order = list(s[col].cat.categories) if hasattr(s[col], "cat") else sorted(s[col].unique())
        sns.boxplot(data=s, x=col, y=s.pm_peak_rate * 100, order=order, ax=ax, color="#9ecae1", showfliers=False)
        sns.stripplot(data=s, x=col, y=s.pm_peak_rate * 100, order=order, ax=ax, color="#08519c", size=2.5, alpha=0.5)
        eta = effects.set_index("characteristic").loc[label, "eta_squared_pm_peak"]
        ax.set(title=f"{label} (η² = {eta:.2f})", xlabel="", ylabel="PM-peak % congested")
        ax.tick_params(axis="x", rotation=15)
    fig.tight_layout()
    r.figure(fig, "boxplots", "PM-peak congestion by derived road characteristic")

    fig, ax = plt.subplots(figsize=(8, 7))
    palette = sns.color_palette("tab10", best_k)
    for c, g in s.groupby("cluster"):
        ax.scatter(g.longitude, g.latitude, s=8 + g.pm_peak_rate * 300, color=palette[c], alpha=0.75,
                   label=f"C{c + 1} {names[c]} ({len(g)})", edgecolor="white", linewidth=0.3)
    ax.set(title="Corridors (KMeans) — marker size = PM-peak congestion rate", xlabel="longitude", ylabel="latitude")
    ax.legend(fontsize=8)
    r.figure(fig, "corridors_map", "Geographic corridors and congestion")

    r.implications = [
        "Coarse sensor-level characteristics explain little; per-sensor history features (`ff_speed`, "
        "`sensor_congested_rate`, slot profile) and live features capture site effects directly. Adding characteristic "
        "groups as model features is unlikely to help.",
        "Corridor membership remains useful for **reporting** (per-corridor dashboard views and evaluation), "
        "even if it adds little predictive power.",
    ]
    r.limitations = [
        "**No true road attributes**: PEMS-BAY lacks lane counts, functional class, ramp locations and volumes, so these "
        "are proxies. With PeMS station metadata (lanes, freeway, direction) this analysis can be redone directly.",
        "Graph degree reflects how many sensors lie within the DCRNN distance threshold, so it measures sensor density, "
        "not road topology.",
        "η² is descriptive (no causal claim); groups are unbalanced in size.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
