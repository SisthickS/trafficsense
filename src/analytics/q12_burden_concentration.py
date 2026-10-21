"""Q12 - Concentration of congestion burden (Pareto analysis).

Usage: python -m src.analytics.q12_burden_concentration
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.analytics.common import AnalyticsContext, QuestionResult, pct, plt_setup, run_single


def lorenz(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    v = np.sort(np.asarray(values, dtype=float))
    cum = np.concatenate([[0], np.cumsum(v)])
    return np.linspace(0, 1, len(cum)), cum / cum[-1] if cum[-1] else cum


def gini(values) -> float:
    x, y = lorenz(values)
    return float(1 - 2 * np.trapz(y, x))


def top_share(values, share: float) -> float:
    v = np.sort(np.asarray(values, dtype=float))[::-1]
    k = max(1, int(round(share * len(v))))
    return float(v[:k].sum() / v.sum()) if v.sum() else 0.0


def units_for(values, target: float) -> float:
    v = np.sort(np.asarray(values, dtype=float))[::-1]
    c = np.cumsum(v) / v.sum()
    return float((np.searchsorted(c, target) + 1) / len(v))


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q12", "Concentration of congestion burden (Pareto analysis)",
                       "How concentrated is the congestion burden across sensors, days and times, does the 80/20 rule "
                       "hold, and how well would targeting the worst locations work on future data?")
    r.method = [
        "Burden measures per observed reading: **congested minutes** (5 min if speed < 35 mph) and **delay** = "
        "max(0, 60/speed − 60/free-flow) minutes per vehicle-mile, using the train-period free-flow speed.",
        "Units: sensors, sensor-days, days, and sensor × weekday × hour cells (325 × 7 × 24). For each: Lorenz curve, "
        "Gini coefficient, share carried by the top 10%/20%, and the share of units needed for 50%/80% of the burden.",
        "Out-of-sample targeting: select the top-20% sensors in the first half of the period (Jan–Mar) and measure the "
        "share of second-half (Apr–Jun) burden they capture, against the best possible (top 20% chosen in hindsight).",
    ]
    ff = ctx.spark.createDataFrame(ctx.sensors_pd[["sensor_id", "ff_speed"]])
    o = (ctx.obs.join(F.broadcast(ff), "sensor_id")
             .withColumn("cmin", F.col("congested") * 5)
             .withColumn("delay", F.greatest(F.lit(0.0), 60 / F.col("speed") - 60 / F.col("ff_speed")))
             .withColumn("half", F.when(F.col("month") <= "2017-03", "H1").otherwise("H2")))
    sens = o.groupBy("sensor_id", "half").agg(F.sum("cmin").alias("cmin"), F.sum("delay").alias("delay")).toPandas()
    sday = o.groupBy("sensor_id", "date_local").agg(F.sum("cmin").alias("cmin"), F.sum("delay").alias("delay")).toPandas()
    cells = o.groupBy("sensor_id", "dow", "hour").agg(F.sum("cmin").alias("cmin"), F.sum("delay").alias("delay")).toPandas()
    days = sday.groupby("date_local")[["cmin", "delay"]].sum().reset_index()
    sens_tot = sens.groupby("sensor_id")[["cmin", "delay"]].sum().reset_index()

    units = {"sensors": sens_tot, "sensor-days": sday, "days": days, "sensor × weekday × hour cells": cells}
    conc = []
    for name, df in units.items():
        for m, label in (("cmin", "congested minutes"), ("delay", "delay")):
            conc.append({"unit": name, "measure": label, "units": len(df), "gini": gini(df[m]),
                         "top10pct_share": top_share(df[m], 0.10), "top20pct_share": top_share(df[m], 0.20),
                         "units_for_50pct": units_for(df[m], 0.5), "units_for_80pct": units_for(df[m], 0.8)})
    conc = pd.DataFrame(conc)

    piv = sens.pivot(index="sensor_id", columns="half", values=["cmin", "delay"]).fillna(0)
    target = []
    for m, label in (("cmin", "congested minutes"), ("delay", "delay")):
        h1, h2 = piv[(m, "H1")], piv[(m, "H2")]
        k = int(round(0.2 * len(piv)))
        chosen = set(h1.nlargest(k).index)
        oracle = set(h2.nlargest(k).index)
        target.append({"measure": label, "selected_sensors": k, "h2_share_captured": h2[list(chosen)].sum() / h2.sum(),
                       "oracle_share": h2[list(oracle)].sum() / h2.sum(), "overlap_with_oracle": len(chosen & oracle) / k})
    target = pd.DataFrame(target)
    target["efficiency_vs_oracle"] = target.h2_share_captured / target.oracle_share

    c = conc.set_index(["unit", "measure"])
    s_cm = c.loc[("sensors", "congested minutes")]
    s_dl = c.loc[("sensors", "delay")]
    cell = c.loc[("sensor × weekday × hour cells", "congested minutes")]
    d_cm = c.loc[("days", "congested minutes")]
    t_cm = target.set_index("measure").loc["congested minutes"]
    pareto = "holds" if s_cm.top20pct_share >= 0.75 else "is softened" if s_cm.top20pct_share >= 0.5 else "does not hold"
    r.findings = [
        f"Across sensors the 80/20 rule {pareto}: the top 20% of sensors carry **{pct(s_cm.top20pct_share)}** of congested "
        f"minutes (Gini {s_cm.gini:.2f}); half of all congestion sits on {pct(s_cm.units_for_50pct)} of sensors, and 80% "
        f"on {pct(s_cm.units_for_80pct)}.",
        f"Delay is {'similarly' if abs(s_dl.gini - s_cm.gini) < 0.05 else 'less' if s_dl.gini < s_cm.gini else 'more'} "
        f"concentrated: top 20% of sensors carry {pct(s_dl.top20pct_share)} (Gini {s_dl.gini:.2f} vs {s_cm.gini:.2f})"
        + (", because moderate slowdowns at many sensors add delay without crossing the 35 mph threshold."
           if s_dl.gini < s_cm.gini - 0.05 else "."),
        f"In space-time the burden is far more concentrated: the top 10% of sensor × weekday × hour cells carry "
        f"**{pct(cell.top10pct_share)}** of congested minutes (Gini {cell.gini:.2f}); 80% of the burden sits in "
        f"{pct(cell.units_for_80pct)} of cells.",
        f"Across days the burden is evenly spread (Gini {d_cm.gini:.2f}; top 10% of days = {pct(d_cm.top10pct_share)}), "
        "so congestion is a daily routine rather than a few extreme days.",
        f"Targeting works out of sample: the top-20% sensors chosen on Jan–Mar data capture **{pct(t_cm.h2_share_captured)}** "
        f"of Apr–Jun congested minutes, {pct(t_cm.efficiency_vs_oracle)} of the hindsight-optimal {pct(t_cm.oracle_share)} "
        f"(overlap {pct(t_cm.overlap_with_oracle)}).",
    ]
    r.metrics = {"gini_sensors": float(s_cm.gini), "top20_sensors_share": float(s_cm.top20pct_share),
                 "top10_cells_share": float(cell.top10pct_share), "targeting_capture": float(t_cm.h2_share_captured),
                 "targeting_efficiency": float(t_cm.efficiency_vs_oracle)}
    r.table("concentration", conc, "Concentration by unit and burden measure")
    r.table("targeting", target, "Out-of-sample targeting: top-20% sensors chosen on Jan–Mar, evaluated on Apr–Jun")
    top_cells = cells.sort_values("cmin", ascending=False).head(15).assign(congested_hours=lambda d: d.cmin / 60)
    r.table("top_cells", top_cells[["sensor_id", "dow", "hour", "congested_hours", "delay"]], "Fifteen heaviest sensor × weekday × hour cells")
    r.table("sensors", sens_tot.sort_values("cmin", ascending=False).assign(
        cum_share=lambda d: d.cmin.cumsum() / d.cmin.sum()), "Sensors ranked by congested minutes with cumulative share")

    plt, sns = plt_setup()
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    for (name, df), col in zip(units.items(), ["#c44e52", "#dd8452", "#4c72b0", "#8172b2"]):
        x, y = lorenz(df["cmin"])
        axes[0].plot(x, y, color=col, lw=2, label=f"{name} (Gini {gini(df['cmin']):.2f})")
    axes[0].plot([0, 1], [0, 1], color="grey", ls=":")
    axes[0].set(title="Lorenz curves of congested minutes", xlabel="cumulative share of units (lowest → highest)",
                ylabel="cumulative share of burden")
    axes[0].legend(fontsize=8)
    st = sens_tot.sort_values("cmin", ascending=False).reset_index(drop=True)
    axes[1].bar(np.arange(len(st)), st.cmin / 60, color="#4c72b0", width=1.0)
    ax2 = axes[1].twinx()
    ax2.plot(np.arange(len(st)), st.cmin.cumsum() / st.cmin.sum() * 100, color="#c44e52", lw=2)
    ax2.axhline(80, color="#c44e52", ls=":", lw=1)
    ax2.set_ylabel("cumulative % of congested minutes", color="#c44e52")
    axes[1].set(title="Pareto chart of sensors", xlabel="sensors ranked by burden", ylabel="congested hours")
    r.figure(fig, "lorenz_pareto", "Lorenz curves and Pareto chart")

    r.implications = [
        f"**Recommendation:** a targeted programme (signal/ramp-metering review, incident patrols) on the top 20% of sensors "
        f"would address about {pct(t_cm.h2_share_captured, 0)} of future congestion, and the choice can be made from past data.",
        "Because burden concentrates in specific sensor × weekday × hour cells, interventions can be time-limited (e.g. weekday "
        "PM peaks only) rather than all-day.",
        "The dashboard's watch-list should be driven by the cell ranking (sensor × weekday × hour), not just by sensor.",
    ]
    r.limitations = [
        "Burden is per sensor-reading; without traffic volumes it is not vehicle-weighted.",
        "Sensors are points of unequal spacing; a long uncovered segment can hide burden (see Q13 on coverage).",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
