"""Q14 - Traffic anomalies versus sensor anomalies.

Usage: python -m src.analytics.q14_anomalies
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.analytics.common import AnalyticsContext, QuestionResult, pct, plt_setup, run_single

Z_ANOM = 3.5          # Iglewicz & Hoaglin modified z-score cut-off
Z_SUPPORT = 2.0       # neighbour deviation that counts as corroboration
MIN_RSD = 1.0         # mph floor for the robust spread


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q14", "Traffic anomalies versus sensor anomalies",
                       "When a sensor reports an unusual speed, is it a real traffic event (seen by neighbouring sensors "
                       "too) or a sensor problem (an isolated glitch)?")
    r.method = [
        "Robust baseline per (sensor, working day vs not, 5-min slot): median speed and MAD over all observed readings. "
        f"Modified z = (speed − median) / (1.4826 · MAD), with the spread floored at {MIN_RSD:.0f} mph. Anomaly: |z| ≥ {Z_ANOM} "
        "(Iglewicz & Hoaglin's cut-off).",
        f"Corroboration: an anomaly is a **traffic anomaly** if at least one road-graph neighbour (either direction) deviates "
        f"in the same direction with |z| ≥ {Z_SUPPORT} at the same timestamp. A spatial join of the anomalies with the "
        "edge list and the z-table does this.",
        "Uncorroborated anomalies are **sensor-suspect**: an *isolated spike* if the sensor's own readings before and "
        f"after are normal (|z| < {Z_SUPPORT}), otherwise *isolated persistent* (a local event or sensor drift). Sensors "
        "without neighbours are 'unverifiable'.",
        "Cross-check: per-sensor rate of isolated anomalies vs the share of invalid readings found by cleaning.",
    ]
    obs = ctx.obs.select("sensor_id", "ts_utc", "ts_local", "hour", "is_workday", "slot", "speed")
    keys = ["sensor_id", "is_workday", "slot"]
    med = obs.groupBy(*keys).agg(F.expr("percentile_approx(speed, 0.5, 200)").alias("med"))
    mad = (obs.join(F.broadcast(med), keys).groupBy(*keys)
              .agg(F.expr("percentile_approx(abs(speed - med), 0.5, 200)").alias("mad")))
    base = med.join(mad, keys)
    w = Window.partitionBy("sensor_id").orderBy("ts_utc")
    z = (obs.join(F.broadcast(base), keys)
            .withColumn("z", (F.col("speed") - F.col("med")) / F.greatest(F.lit(MIN_RSD), 1.4826 * F.col("mad")))
            .withColumn("z_prev", F.lag("z").over(w)).withColumn("z_next", F.lead("z").over(w))
            .select("sensor_id", "ts_utc", "ts_local", "hour", "is_workday", "speed", "med", "z", "z_prev", "z_next")
            .persist(StorageLevel.MEMORY_AND_DISK))
    n_obs = z.count()
    anom = z.filter(F.abs("z") >= Z_ANOM)
    edges = ctx.edges_pd()
    nb = pd.concat([edges[["src", "dst"]].rename(columns={"src": "sensor_id", "dst": "nbr"}),
                    edges[["dst", "src"]].rename(columns={"dst": "sensor_id", "src": "nbr"})]).drop_duplicates()
    nb_df = ctx.spark.createDataFrame(nb.astype("int32"))
    has_nb = set(nb.sensor_id)
    zn = z.select(F.col("sensor_id").alias("nbr"), "ts_utc", F.col("z").alias("z_nbr"))
    support = (anom.select("sensor_id", "ts_utc", "z").join(F.broadcast(nb_df), "sensor_id")
                   .join(zn, ["nbr", "ts_utc"])
                   .groupBy("sensor_id", "ts_utc")
                   .agg(F.max(F.when((F.signum("z_nbr") == F.signum("z")) & (F.abs("z_nbr") >= Z_SUPPORT), 1).otherwise(0)).alias("supported"),
                        F.count(F.lit(1)).alias("nbrs_checked")))
    a = (anom.join(support, ["sensor_id", "ts_utc"], "left")
             .withColumn("spike", (F.coalesce(F.abs("z_prev"), F.lit(0.0)) < Z_SUPPORT) & (F.coalesce(F.abs("z_next"), F.lit(0.0)) < Z_SUPPORT))
             .withColumn("kind", F.when(~F.col("sensor_id").isin(list(has_nb)), "unverifiable (no neighbours)")
                                  .when(F.col("supported") == 1, "traffic anomaly (corroborated)")
                                  .when(F.col("spike"), "isolated spike")
                                  .otherwise("isolated persistent"))
             .withColumn("direction", F.when(F.col("z") < 0, "slowdown").otherwise("speed-up")))
    pa = a.select("sensor_id", "ts_utc", "ts_local", "hour", "is_workday", "speed", "med", "z", "kind", "direction").toPandas()
    quality = (ctx.grid.groupBy("sensor_id").agg(F.avg((~F.col("observed")).cast("int")).alias("invalid_share")).toPandas())
    z.unpersist()

    kinds = (pa.groupby(["kind", "direction"]).size().unstack(fill_value=0).assign(total=lambda d: d.sum(axis=1))
               .sort_values("total", ascending=False).reset_index())
    kinds["share"] = kinds.total / kinds.total.sum()
    by_hour = pa.groupby(["hour", "kind"]).size().unstack(fill_value=0)
    per_sensor = (pa.groupby("sensor_id").kind.value_counts().unstack(fill_value=0)
                    .assign(anomalies=lambda d: d.sum(axis=1)).reset_index())
    for c in ("isolated spike", "isolated persistent", "traffic anomaly (corroborated)"):
        if c not in per_sensor:
            per_sensor[c] = 0
    per_sensor["isolated_share"] = (per_sensor["isolated spike"] + per_sensor["isolated persistent"]) / per_sensor.anomalies
    per_sensor = per_sensor.merge(quality, on="sensor_id", how="right").fillna({"anomalies": 0, "isolated spike": 0})
    per_sensor["spikes_per_10k"] = per_sensor["isolated spike"] / (n_obs / len(per_sensor)) * 1e4
    rho = per_sensor[["spikes_per_10k", "invalid_share"]].corr(method="spearman").iloc[0, 1]
    suspects = per_sensor[per_sensor.anomalies >= 20].sort_values("isolated_share", ascending=False).head(15)

    traffic = pa[pa.kind.str.startswith("traffic")]
    isolated = pa[pa.kind.str.startswith("isolated")]
    peak_share = lambda d: d.hour.between(6, 9).mean() + d.hour.between(15, 18).mean()
    trf_slow = (traffic.direction == "slowdown").mean() if len(traffic) else np.nan
    share = lambda k: kinds.set_index("kind").share.get(k, 0.0)
    r.findings = [
        f"{len(pa):,} anomalous readings ({pct(len(pa) / n_obs, 2)} of {n_obs:,} observed): **{pct(share('traffic anomaly (corroborated)'))} are "
        f"traffic anomalies** corroborated by a neighbour, {pct(share('isolated persistent'))} isolated persistent, "
        f"{pct(share('isolated spike'))} isolated spikes and {pct(share('unverifiable (no neighbours)'))} unverifiable.",
        f"Traffic anomalies are mostly slowdowns ({pct(trf_slow)}) and cluster in peak hours ({pct(peak_share(traffic))} at 06–10/15–19h), "
        f"while isolated anomalies are spread through the day ({pct(peak_share(isolated))} in the same hours).",
        f"Isolated spikes are concentrated on few sensors: the 15 most spike-prone sensors carry "
        f"{pct(per_sensor.nlargest(15, 'isolated spike')['isolated spike'].sum() / max(per_sensor['isolated spike'].sum(), 1))} of all spikes.",
        f"Spike rate and the invalid-reading share from cleaning are "
        + ("positively related" if rho > 0.2 else "negatively related" if rho < -0.2 else "essentially unrelated")
        + f" across sensors (Spearman ρ = {rho:.2f}). "
        + ("Both point to the same unreliable detectors." if rho > 0.2 else
           "Spikes therefore reveal a *different* kind of sensor problem than the stuck/zero readings found in cleaning."),
    ]
    r.metrics = {"anomalies": len(pa), "share_traffic": float(share("traffic anomaly (corroborated)")),
                 "share_isolated_spike": float(share("isolated spike")), "spearman_spikes_invalid": float(rho)}
    r.table("kinds", kinds, "Anomaly classification")
    r.table("suspect_sensors", suspects[["sensor_id", "anomalies", "isolated_share", "isolated spike", "isolated persistent",
                                         "traffic anomaly (corroborated)", "invalid_share"]],
            "Sensors whose anomalies are mostly uncorroborated (≥ 20 anomalies)")
    r.table("by_hour", by_hour.reset_index(), "Anomalies by hour and kind")
    r.table("per_sensor", per_sensor, "Per-sensor anomaly counts")

    plt, sns = plt_setup()
    fig, axes = plt.subplots(1, 2, figsize=(15, 4.4))
    share_h = by_hour.div(by_hour.sum(axis=0), axis=1) * 100
    for k, col in zip(share_h.columns, ["#c44e52", "#8172b2", "#dd8452", "#8c8c8c"]):
        axes[0].plot(share_h.index, share_h[k], marker="o", label=k, color=col)
    axes[0].set(title="When anomalies happen (each kind sums to 100%)", xlabel="hour", ylabel="% of that kind", xticks=range(0, 24, 2))
    axes[0].legend(fontsize=8)
    kk = kinds.set_index("kind")[["slowdown", "speed-up"]] if "speed-up" in kinds else kinds.set_index("kind")[["slowdown"]]
    kk.plot.barh(stacked=True, ax=axes[1], color=["#c44e52", "#4c72b0"])
    axes[1].set(title="Anomalies by kind and direction", xlabel="readings", ylabel="")
    r.figure(fig, "kinds", "Traffic vs sensor anomalies: timing and direction")

    if len(traffic) and len(isolated):
        ex_t = traffic.loc[traffic.z.abs().idxmax()]
        ex_s = pa[pa.kind == "isolated spike"]
        ex_s = ex_s.loc[ex_s.z.abs().idxmax()] if len(ex_s) else isolated.iloc[0]
        fig, axes = plt.subplots(1, 2, figsize=(15, 4))
        for ax, ex, title in ((axes[0], ex_t, "Corroborated traffic anomaly"), (axes[1], ex_s, "Isolated sensor anomaly")):
            sid = int(ex.sensor_id)
            peers = nb[nb.sensor_id == sid].nbr.head(4).tolist()
            lo, hi = ex.ts_utc - pd.Timedelta(hours=2), ex.ts_utc + pd.Timedelta(hours=2)
            win = (ctx.obs.filter(F.col("sensor_id").isin([sid] + peers) & F.col("ts_utc").between(lo, hi))
                      .select("sensor_id", "ts_local", "speed").toPandas())
            for s_, d in win.groupby("sensor_id"):
                d = d.sort_values("ts_local")
                ax.plot(d.ts_local, d.speed, lw=2.5 if s_ == sid else 1, label=f"{s_}{' (anomaly)' if s_ == sid else ''}")
            ax.axvline(ex.ts_local, color="black", ls=":")
            ax.set(title=f"{title}: sensor {sid}, z = {ex.z:.1f}", ylabel="mph")
            ax.tick_params(axis="x", rotation=30)
            ax.legend(fontsize=7)
        r.figure(fig, "examples", "Examples: neighbours confirm a traffic event (left) but not a sensor glitch (right)")

    r.implications = [
        "**Recommendation:** suppress dashboard alerts for uncorroborated isolated spikes (or flag them 'unconfirmed'), and "
        "only escalate anomalies that a neighbour confirms.",
        "Sensors in the suspect table should be checked by maintenance; their isolated anomalies would otherwise trigger false congestion alerts.",
        "Neighbour corroboration can run in the streaming job with the same graph join (Phase 7).",
    ]
    r.limitations = [
        "Corroboration needs a neighbour within the DCRNN graph; isolated sensors cannot be verified.",
        "A real but very local event (e.g. a lane closure right at one detector) can look like a sensor anomaly.",
        f"The thresholds (|z| ≥ {Z_ANOM}, neighbour |z| ≥ {Z_SUPPORT}) are conventional; results scale with them.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
