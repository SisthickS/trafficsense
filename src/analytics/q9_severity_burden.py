"""Q9 - Combined congestion severity and burden.

Usage: python -m src.analytics.q9_severity_burden
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.analytics.common import AM_PEAK, PM_PEAK, AnalyticsContext, QuestionResult, minmax, pct, plt_setup, run_single, spearman

COMPONENTS = {
    "frequency": "share of working-day readings congested",
    "intensity": "mean speed deficit while congested: 1 − speed / free-flow",
    "duration": "mean working-day episode duration (min)",
    "unreliability": "Planning Time Index in the peaks: free-flow / 5th-percentile speed",
}


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q9", "Combined congestion severity and burden",
                       "Which sensors and days carry the greatest overall congestion burden when frequency, intensity, "
                       "duration and travel-time reliability are combined, and is the ranking robust?")
    r.method = [
        "Free-flow speed per sensor = train-period 85th percentile (`sensor_stats/`). Peaks = working days "
        f"{AM_PEAK[0]:02d}–{AM_PEAK[1]:02d}h and {PM_PEAK[0]:02d}–{PM_PEAK[1]:02d}h.",
        "Standard mobility measures per sensor (FHWA-style): **Travel Time Index** TTI = mean(ff / speed) in the peaks; "
        "**Planning Time Index** PTI = ff / 5th-percentile peak speed; **excess travel time** = mean(max(0, 60/speed − 60/ff)) "
        "minutes per mile in the peaks; congested hours per working day.",
        "Composite **Congestion Severity & Burden Index (CSBI)** = mean of min–max-normalised components: "
        + "; ".join(f"{k} ({v})" for k, v in COMPONENTS.items()) + ".",
        "Robustness: Spearman correlation and top-10 overlap between the CSBI and every drop-one-component variant.",
        "Daily network burden: congested sensor-hours and network-mean TTI per day (Spark `groupBy(date)`).",
    ]
    ff = ctx.spark.createDataFrame(ctx.sensors_pd[["sensor_id", "ff_speed"]])
    obs = ctx.obs.join(F.broadcast(ff), "sensor_id")
    peak = F.col("is_workday") & (F.col("hour").between(AM_PEAK[0], AM_PEAK[1] - 1) | F.col("hour").between(PM_PEAK[0], PM_PEAK[1] - 1))
    work = obs.filter(F.col("is_workday"))
    per = (work.groupBy("sensor_id").agg(
        F.avg("congested").alias("frequency"),
        F.avg(F.when(F.col("congested") == 1, 1 - F.col("speed") / F.col("ff_speed"))).alias("intensity"),
        F.avg(F.when(peak, F.col("ff_speed") / F.col("speed"))).alias("tti_peak"),
        F.expr("percentile_approx(CASE WHEN hour BETWEEN 6 AND 9 OR hour BETWEEN 15 AND 18 THEN speed END, 0.05)").alias("p05_peak_speed"),
        F.avg(F.when(peak, F.greatest(F.lit(0.0), 60 / F.col("speed") - 60 / F.col("ff_speed")))).alias("excess_min_per_mile_peak"),
        (F.sum("congested") * 5 / 60 / F.countDistinct("date_local")).alias("congested_hours_per_workday"),
        F.first("ff_speed").alias("ff_speed")).toPandas())
    per["unreliability"] = per.ff_speed / per.p05_peak_speed
    per["intensity"] = per.intensity.fillna(0.0)
    ep = ctx.episodes()
    dur = ep[ep.is_workday].groupby("sensor_id").duration_min.mean().rename("duration")
    s = per.merge(dur, on="sensor_id", how="left").merge(ctx.sensors_pd[["sensor_id", "latitude", "longitude"]], on="sensor_id")
    s["duration"] = s.duration.fillna(0.0)
    for c in COMPONENTS:
        s[f"{c}_norm"] = minmax(s[c])
    norm_cols = [f"{c}_norm" for c in COMPONENTS]
    s["csbi"] = s[norm_cols].mean(axis=1)
    s = s.sort_values("csbi", ascending=False).reset_index(drop=True)
    s["rank"] = np.arange(1, len(s) + 1)

    robust = []
    top10 = set(s.head(10).sensor_id)
    for c in COMPONENTS:
        alt = s[[x for x in norm_cols if x != f"{c}_norm"]].mean(axis=1)
        alt_top = set(s.sensor_id[alt.nlargest(10).index])
        robust.append({"variant": f"without {c}", "spearman_vs_csbi": spearman(alt, s.csbi),
                       "top10_overlap": len(top10 & alt_top)})
    robust = pd.DataFrame(robust)
    comp_corr = s[list(COMPONENTS)].corr(method="spearman")

    daily = (obs.groupBy("date_local", "is_workday")
                .agg((F.sum("congested") * 5 / 60).alias("congested_sensor_hours"),
                     F.avg(F.col("ff_speed") / F.col("speed")).alias("network_tti"),
                     F.avg(F.when(peak, F.col("ff_speed") / F.col("speed"))).alias("network_peak_tti"))
                .toPandas().sort_values("date_local"))
    daily["date_local"] = pd.to_datetime(daily.date_local)
    worst_days = daily.nlargest(10, "congested_sensor_hours")
    wd = daily[daily.is_workday]
    total_hours = s.congested_hours_per_workday.sum()
    top10_share = s.head(10).congested_hours_per_workday.sum() / total_hours

    lead = s.iloc[0]
    r.findings = [
        f"Highest burden: sensor **{int(lead.sensor_id)}** (CSBI {lead.csbi:.2f}): {pct(lead.frequency)} of working-day "
        f"readings congested, {pct(lead.intensity)} below free flow while congested, {lead.duration:.0f}-min average "
        f"episodes, PTI {lead.unreliability:.1f} (to be on time 95% of the time, a traveller must budget "
        f"{lead.unreliability:.1f}× the free-flow travel time).",
        f"Burden is concentrated: the 10 highest-CSBI sensors ({pct(10 / len(s))} of sensors) carry {pct(top10_share)} of all "
        f"congested hours per working day; network peak TTI averages {s.tti_peak.mean():.2f}, with a mean excess of "
        f"{s.excess_min_per_mile_peak.mean() * 60:.0f} s per mile in the peaks.",
        f"The ranking is robust: dropping any single component keeps Spearman ρ ≥ {robust.spearman_vs_csbi.min():.2f} with "
        f"the full index and at least {robust.top10_overlap.min()}/10 of the top 10.",
        f"Components are related but not redundant (Spearman: frequency–unreliability {comp_corr.loc['frequency', 'unreliability']:.2f}, "
        f"frequency–intensity {comp_corr.loc['frequency', 'intensity']:.2f}, frequency–duration {comp_corr.loc['frequency', 'duration']:.2f}).",
        f"Worst day for the network: {worst_days.iloc[0].date_local:%a %Y-%m-%d} with {worst_days.iloc[0].congested_sensor_hours:,.0f} "
        f"congested sensor-hours (working-day mean {wd.congested_sensor_hours.mean():,.0f}); peak TTI that day "
        f"{worst_days.iloc[0].network_peak_tti:.2f}.",
    ]
    r.metrics = {"top_sensor": int(lead.sensor_id), "top10_share_hours": float(top10_share),
                 "min_robust_spearman": float(robust.spearman_vs_csbi.min()), "min_top10_overlap": int(robust.top10_overlap.min()),
                 "network_mean_peak_tti": float(s.tti_peak.mean())}
    cols = ["rank", "sensor_id", "csbi", "frequency", "intensity", "duration", "unreliability", "tti_peak",
            "excess_min_per_mile_peak", "congested_hours_per_workday", "ff_speed"]
    r.table("top_sensors", s[cols].head(15), "Fifteen highest-burden sensors (CSBI)")
    r.table("robustness", robust, "Robustness of the CSBI ranking to dropping one component")
    r.table("component_correlation", comp_corr.reset_index().rename(columns={"index": "component"}), "Spearman correlation between components")
    r.table("worst_days", worst_days.assign(date_local=worst_days.date_local.dt.strftime("%Y-%m-%d")), "Ten days with the highest network burden")
    r.table("all_sensors", s, "CSBI and components for all sensors")
    r.table("daily", daily.assign(date_local=daily.date_local.dt.strftime("%Y-%m-%d")), "Daily network burden")

    plt, sns = plt_setup()
    top = s.head(15)
    fig, ax = plt.subplots(figsize=(11, 5))
    left = np.zeros(len(top))
    palette = ["#c44e52", "#dd8452", "#8172b2", "#4c72b0"]
    for c, col in zip(COMPONENTS, palette):
        vals = top[f"{c}_norm"].to_numpy() / len(COMPONENTS)
        ax.barh(top.sensor_id.astype(str), vals, left=left, color=col, label=c)
        left += vals
    ax.invert_yaxis()
    ax.set(title="Top 15 sensors by CSBI: contribution of each component", xlabel="CSBI (0–1)")
    ax.legend(loc="lower right")
    r.figure(fig, "top_csbi", "CSBI decomposition for the highest-burden sensors")

    fig, axes = plt.subplots(1, 2, figsize=(15, 5), gridspec_kw={"width_ratios": [1, 1.4]})
    sc = axes[0].scatter(s.longitude, s.latitude, c=s.csbi, cmap="inferno_r", s=12 + 60 * s.csbi, edgecolor="grey", linewidth=0.3)
    fig.colorbar(sc, ax=axes[0], label="CSBI")
    axes[0].set(title="Congestion Severity & Burden Index", xlabel="longitude", ylabel="latitude")
    axes[1].plot(daily.date_local, daily.congested_sensor_hours, color="grey", lw=0.8)
    axes[1].scatter(wd.date_local, wd.congested_sensor_hours, s=8, color="#c44e52", label="working day")
    ax2 = axes[1].twinx()
    ax2.plot(daily.date_local, daily.network_peak_tti.rolling(7, min_periods=1, center=True).mean(), color="#4c72b0", lw=1.5,
             label="peak TTI (7-day mean)")
    ax2.set_ylabel("peak TTI", color="#4c72b0")
    axes[1].set(title="Daily network burden", ylabel="congested sensor-hours")
    axes[1].legend(loc="upper left")
    r.figure(fig, "map_and_daily", "Where the burden is (map) and when (daily series)")

    r.implications = [
        "The CSBI gives the dashboard a single, explainable ranking (with component breakdown) for prioritising sensors.",
        "TTI/PTI use standard FHWA reliability measures, so results are comparable with published mobility reports.",
        "High-PTI sensors are where predictions add the most value to travellers, because travel time is least reliable there.",
    ]
    r.limitations = [
        "No traffic volumes in PEMS-BAY, so burden is per sensor-reading, not per vehicle (vehicle-hours of delay cannot be computed).",
        "Equal weights are a transparent default; the robustness table shows the ranking is not driven by any one component.",
        "Free-flow speed comes from the training period (Jan–May 7) for consistency with the model features.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
