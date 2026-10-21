"""Q5 - Persistent congestion hotspots.

Usage: python -m src.analytics.q5_hotspots
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.analytics.common import AnalyticsContext, QuestionResult, pct, plt_setup, run_single, spearman

DAILY_MIN = 60          # a "congested day" has >= 60 congested minutes (primary definition)
SENSITIVITY_MIN = (30, 60, 120)
HOTSPOT_PERSISTENCE = 0.5
TOP_N = 20


def clustering_lift(hot: set, edges: pd.DataFrame, n_sensors: int) -> tuple[float, float]:
    """(share of hotspot out-neighbours that are hotspots, share expected at random)."""
    e = edges[edges.src.isin(hot)]
    if len(hot) < 2 or not len(e):
        return float("nan"), float("nan")
    return float(e.dst.isin(hot).mean()), (len(hot) - 1) / (n_sensors - 1)


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q5", "Persistent congestion hotspots",
                       "Which sensors are congested persistently (most working days, for substantial time), are the "
                       "hotspots stable month to month, and do they cluster along the road network?")
    r.method = [
        "Working days, observed readings. Per (sensor, day): congested minutes = 5 × congested readings.",
        f"Persistence = share of working days with ≥ {DAILY_MIN} congested minutes. A **persistent hotspot** has persistence "
        f"≥ {HOTSPOT_PERSISTENCE:.0%}.",
        "Monthly stability: rank sensors by mean daily congested minutes per month; Spearman correlation of ranks "
        f"between consecutive months and Jaccard overlap of the monthly top {TOP_N}.",
        "Spatial clustering: share of hotspot→hotspot edges in the road-graph adjacency vs the share expected if "
        "hotspots were placed at random.",
    ]
    work = ctx.obs.filter(F.col("is_workday"))
    daily = (work.groupBy("sensor_id", "date_local", "month")
                 .agg((F.sum("congested") * 5).alias("congested_min")))
    per_sensor = (daily.groupBy("sensor_id")
                       .agg(F.avg("congested_min").alias("mean_daily_congested_min"),
                            *[F.avg((F.col("congested_min") >= m).cast("int")).alias(f"persistence_{m}") for m in SENSITIVITY_MIN],
                            F.max("congested_min").alias("max_daily_congested_min"),
                            F.count(F.lit(1)).alias("workdays"))
                       .toPandas())
    per_sensor["persistence"] = per_sensor[f"persistence_{DAILY_MIN}"]
    monthly = (daily.groupBy("sensor_id", "month").agg(F.avg("congested_min").alias("m")).toPandas()
                    .pivot(index="sensor_id", columns="month", values="m").fillna(0))
    meta = ctx.sensors_pd[["sensor_id", "latitude", "longitude"]]
    s = per_sensor.merge(meta, on="sensor_id", how="left")
    s["class"] = np.select([s.persistence >= HOTSPOT_PERSISTENCE, s.persistence >= 0.2, s.persistence > 0],
                           ["persistent hotspot", "frequent", "occasional"], "rarely congested")
    s = s.sort_values("mean_daily_congested_min", ascending=False)
    s["rank"] = np.arange(1, len(s) + 1)

    months = list(monthly.columns)
    ranks = monthly.rank(ascending=False)
    stab = []
    for a, b in zip(months, months[1:]):
        ta = set(monthly[a].nlargest(TOP_N).index)
        tb = set(monthly[b].nlargest(TOP_N).index)
        stab.append({"from": a, "to": b, "spearman_rank_corr": spearman(monthly[a], monthly[b]),
                     f"top{TOP_N}_jaccard": len(ta & tb) / len(ta | tb)})
    stab = pd.DataFrame(stab)
    in_top_all = set.intersection(*[set(monthly[m].nlargest(TOP_N).index) for m in months])

    hot = set(s.loc[s["class"] == "persistent hotspot", "sensor_id"])
    edges = ctx.edges_pd()
    hot_hot, expected = clustering_lift(hot, edges, len(s))
    sens_rows = []
    for m in SENSITIVITY_MIN:
        h = set(s.loc[s[f"persistence_{m}"] >= HOTSPOT_PERSISTENCE, "sensor_id"])
        share, exp = clustering_lift(h, edges, len(s))
        sens_rows.append({"daily_congested_min_threshold": m, "hotspots": len(h), "share_of_sensors": len(h) / len(s),
                          "hotspot_neighbour_share": share, "random_expectation": exp,
                          "lift": share / exp if exp and not np.isnan(exp) else float("nan")})
    sensitivity = pd.DataFrame(sens_rows)
    lift = hot_hot / expected if expected and not np.isnan(expected) else float("nan")
    lifts = sensitivity.lift.dropna()

    cls_counts = s["class"].value_counts().reindex(["persistent hotspot", "frequent", "occasional", "rarely congested"]).fillna(0)
    top = s.head(15)
    share_top10 = s.head(10).mean_daily_congested_min.sum() / s.mean_daily_congested_min.sum()
    r.findings = [
        f"**{len(hot)} persistent hotspots** ({pct(len(hot) / len(s))} of sensors) have ≥ {DAILY_MIN} congested minutes on at "
        f"least half of working days. A further {int(cls_counts['frequent'])} sensors reach that on 20–50% of working days, "
        f"and {int(cls_counts['rarely congested'])} never do (sensitivity: "
        + ", ".join(f"{int(x.hotspots)} hotspots at ≥ {x.daily_congested_min_threshold} min" for x in sensitivity.itertuples()) + ").",
        f"Congestion is concentrated: the top 10 sensors ({pct(10 / len(s))} of sensors) account for {pct(share_top10)} of all "
        f"working-day congested minutes. The #1 sensor {int(top.iloc[0].sensor_id)} averages "
        f"{top.iloc[0].mean_daily_congested_min:.0f} congested min/day (persistence {pct(top.iloc[0].persistence)}).",
        f"Hotspots are stable: month-to-month Spearman rank correlation {stab.spearman_rank_corr.min():.2f}–"
        f"{stab.spearman_rank_corr.max():.2f}, top-{TOP_N} Jaccard overlap {stab[f'top{TOP_N}_jaccard'].mean():.2f} on "
        f"average; {len(in_top_all)} sensors are in the top {TOP_N} in every month.",
        (f"Hotspots cluster on the road network: {pct(hot_hot)} of a hotspot's graph neighbours are also hotspots, vs "
         f"{pct(expected)} expected at random (**{lift:.1f}× lift**; {lifts.min():.1f}–{lifts.max():.1f}× across thresholds)."
         if lift >= 1.5 else
         f"Spatial clustering is weak: {pct(hot_hot)} of a hotspot's graph neighbours are hotspots vs {pct(expected)} at random "
         f"({lift:.1f}× lift).") if not np.isnan(lift) else "Too few hotspots to test spatial clustering.",
    ]
    r.metrics = {"hotspots": len(hot), "top10_share": float(share_top10),
                 "min_month_spearman": float(stab.spearman_rank_corr.min()), "always_top": len(in_top_all),
                 "hotspot_neighbour_share": float(hot_hot), "random_expectation": float(expected)}
    r.table("top_hotspots", top[["rank", "sensor_id", "class", "mean_daily_congested_min", "persistence",
                                 "max_daily_congested_min", "latitude", "longitude"]], "Fifteen most congested sensors (working days)")
    r.table("monthly_stability", stab, "Month-to-month stability of the hotspot ranking")
    r.table("threshold_sensitivity", sensitivity, "Hotspot count and spatial clustering for different daily-minute thresholds (≥ 50% of working days)")
    r.table("classes", cls_counts.rename_axis("class").reset_index(name="sensors"), "Sensor classes")
    r.table("all_sensors", s, "Hotspot metrics for all sensors")

    plt, sns = plt_setup()
    fig, ax = plt.subplots(figsize=(8, 7))
    colors = {"persistent hotspot": "#c44e52", "frequent": "#dd8452", "occasional": "#8c8c8c", "rarely congested": "#c7c7c7"}
    for cls, g in s.groupby("class"):
        ax.scatter(g.longitude, g.latitude, s=10 + g.mean_daily_congested_min * 0.8, color=colors[cls], alpha=0.8,
                   label=f"{cls} ({len(g)})", edgecolor="white", linewidth=0.3)
    for _, row in s.head(5).iterrows():
        ax.annotate(str(int(row.sensor_id)), (row.longitude, row.latitude), fontsize=8, xytext=(4, 4), textcoords="offset points")
    ax.set(title="Congestion hotspots (size = mean daily congested minutes)", xlabel="longitude", ylabel="latitude")
    ax.legend(loc="lower left", fontsize=8)
    r.figure(fig, "map", "Hotspot map")

    fig, axes = plt.subplots(1, 2, figsize=(15, 5), gridspec_kw={"width_ratios": [1, 1.2]})
    axes[0].barh(top.sensor_id.astype(str)[::-1], top.mean_daily_congested_min[::-1],
                 color=[colors[c] for c in top["class"][::-1]])
    axes[0].set(title="Top 15 sensors: mean congested minutes per working day", xlabel="minutes")
    top20 = s.head(TOP_N).sensor_id
    sns.heatmap(ranks.loc[top20], annot=True, fmt=".0f", cmap="YlOrRd_r", ax=axes[1], cbar_kws={"label": "monthly rank"})
    axes[1].set(title=f"Monthly rank of the overall top {TOP_N}", xlabel="month", ylabel="sensor")
    r.figure(fig, "ranking_stability", "Top hotspots and their month-by-month rank")

    r.implications = [
        "A small, stable set of hotspots carries most congestion. The dashboard should give them a fixed watch-list panel, "
        "and model metrics should be reported for hotspot vs non-hotspot sensors.",
        "Stable rankings justify train-period per-sensor statistics (`sensor_congested_rate`, `hist_*`) as features: "
        "they stay valid in the test months.",
        "Neighbour features (`nbr_up_*`, `nbr_down_*`) let the model use congestion at adjacent sensors; Q7 quantifies "
        "how strongly and with what lag neighbours are linked.",
    ]
    r.limitations = [
        f"The hotspot definition (≥ {DAILY_MIN} min on ≥ 50% of working days) is a transparent convention; the sensitivity "
        "table and the per-sensor CSV allow other cut-offs.",
        "Sensor locations are points; one congested freeway segment can contain several sensors.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
