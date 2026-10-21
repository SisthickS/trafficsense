"""Q1 - Daily and weekly congestion patterns.

Usage: python -m src.analytics.q1_daily_weekly_patterns
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.analytics.common import (
    AM_PEAK, DOW_NAMES, PM_PEAK, AnalyticsContext, QuestionResult, pct, plt_setup, run_single,
)


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q1", "Daily and weekly congestion patterns",
                       "When during the day and the week does congestion occur, how strong are the peaks, and how do "
                       "weekends and holidays differ from working days?")
    r.method = [
        "Observed readings only; congestion = speed < 35 mph; times are local (Pacific).",
        "`groupBy(dow, hour)` → congestion rate and mean speed (7 × 24 profile).",
        "`groupBy(is_workday, hour)` → working-day vs weekend/holiday hourly curves; AM/PM peak = hour with the highest "
        f"rate in {AM_PEAK[0]}–{AM_PEAK[1]}h / {PM_PEAK[0]}–{PM_PEAK[1]}h.",
        "`groupBy(date_local)` → daily congestion rate and congested sensor-hours (Σ congested readings × 5 min / 60).",
        "Week-of-year aggregation for week-to-week variation.",
    ]
    obs = ctx.obs
    dh = (obs.groupBy("dow", "hour").agg(F.avg("congested").alias("rate"), F.avg("speed").alias("mean_speed"),
                                         F.count(F.lit(1)).alias("readings")).toPandas().sort_values(["dow", "hour"]))
    wk = (obs.groupBy("is_workday", "hour").agg(F.avg("congested").alias("rate"), F.avg("speed").alias("mean_speed"))
             .toPandas())
    daily = (obs.groupBy("date_local", "dow", "is_holiday", "is_workday")
                .agg(F.avg("congested").alias("rate"), (F.sum("congested") * 5 / 60).alias("congested_sensor_hours"),
                     F.avg("speed").alias("mean_speed"))
                .toPandas().sort_values("date_local"))
    weekly = (obs.withColumn("week", F.weekofyear("date_local")).filter("is_workday")
                 .groupBy("week").agg(F.avg("congested").alias("rate"), F.countDistinct("date_local").alias("workdays"))
                 .toPandas().sort_values("week"))

    work = wk[wk.is_workday].set_index("hour").sort_index()
    off = wk[~wk.is_workday].set_index("hour").sort_index()
    am = work.loc[AM_PEAK[0]:AM_PEAK[1] - 1, "rate"]
    pm = work.loc[PM_PEAK[0]:PM_PEAK[1] - 1, "rate"]
    am_h, pm_h = int(am.idxmax()), int(pm.idxmax())
    midday = work.loc[11:13, "rate"].mean()
    night = work.loc[0:4, "rate"].mean()
    daily["dow_name"] = daily["dow"].map(DOW_NAMES)
    daily["date_local"] = pd.to_datetime(daily["date_local"])
    dow_tab = (daily[~daily.is_holiday].groupby(["dow", "dow_name"])
               .agg(days=("rate", "size"), mean_rate=("rate", "mean"),
                    mean_congested_sensor_hours=("congested_sensor_hours", "mean"),
                    mean_speed=("mean_speed", "mean")).reset_index().sort_values("dow"))
    dow_tab["rank_by_congestion"] = dow_tab["mean_rate"].rank(ascending=False).astype(int)
    workdays = daily[daily.is_workday]
    weekend = daily[~daily.is_workday & ~daily.is_holiday]
    hol = daily[daily.is_holiday]
    worst = daily.nlargest(5, "congested_sensor_hours")[["date_local", "dow_name", "rate", "congested_sensor_hours"]]
    weekday_rows = dow_tab[dow_tab.dow <= 5]
    worst_dow = weekday_rows.loc[weekday_rows.mean_rate.idxmax()]
    best_dow = weekday_rows.loc[weekday_rows.mean_rate.idxmin()]
    weekly_cv = weekly["rate"].std() / weekly["rate"].mean()

    r.findings = [
        f"Working-day **PM peak at {pm_h:02d}:00** ({pct(pm.max())} of readings congested) is "
        f"{pm.max() / am.max():.1f}× the **AM peak at {am_h:02d}:00** ({pct(am.max())}); midday {pct(midday)}, night {pct(night, 2)}.",
        f"Working days average {pct(workdays.rate.mean(), 2)} congested readings vs {pct(weekend.rate.mean(), 2)} on weekends "
        f"({workdays.rate.mean() / max(weekend.rate.mean(), 1e-9):.1f}×). The {len(hol)} federal holidays average "
        f"{pct(hol.rate.mean(), 2)}, much closer to weekends than to working days "
        f"({hol.rate.mean() / max(weekend.rate.mean(), 1e-9):.1f}× the weekend level)."
        if len(hol) else
        f"Working days average {pct(workdays.rate.mean(), 2)} congested readings vs {pct(weekend.rate.mean(), 2)} on weekends.",
        f"Most congested weekday: **{worst_dow.dow_name}** ({pct(worst_dow.mean_rate, 2)}, "
        f"{worst_dow.mean_congested_sensor_hours:,.0f} congested sensor-hours/day); lightest: {best_dow.dow_name} "
        f"({pct(best_dow.mean_rate, 2)}).",
        f"A typical working day accumulates {workdays.congested_sensor_hours.mean():,.0f} congested sensor-hours "
        f"(worst day {worst.iloc[0].date_local:%Y-%m-%d}, {worst.iloc[0].dow_name}: {worst.iloc[0].congested_sensor_hours:,.0f}).",
        f"Week-to-week variation of the working-day rate is moderate (coefficient of variation {weekly_cv:.2f} over "
        f"{len(weekly)} weeks), so the weekly cycle is stable.",
    ]
    r.metrics = {"am_peak_hour": am_h, "am_peak_rate": float(am.max()), "pm_peak_hour": pm_h, "pm_peak_rate": float(pm.max()),
                 "workday_rate": float(workdays.rate.mean()), "weekend_rate": float(weekend.rate.mean()),
                 "holiday_rate": float(hol.rate.mean()) if len(hol) else None, "weekly_cv": float(weekly_cv)}

    hourly_tab = pd.DataFrame({"hour": work.index, "workday_rate": work.rate.values, "workday_mean_speed": work.mean_speed.values,
                               "weekend_holiday_rate": off.rate.reindex(work.index).values,
                               "weekend_holiday_mean_speed": off.mean_speed.reindex(work.index).values})
    r.table("hourly_profile", hourly_tab, "Hourly congestion rate and mean speed, working days vs weekends/holidays")
    r.table("day_of_week", dow_tab.drop(columns="dow"), "Day-of-week summary (holidays excluded)")
    r.table("worst_days", worst.assign(date_local=worst.date_local.dt.strftime("%Y-%m-%d")), "Five days with the most congested sensor-hours")
    r.table("daily", daily.assign(date_local=daily.date_local.dt.strftime("%Y-%m-%d")).drop(columns="dow"), "Daily series")
    r.table("dow_hour", dh, "Congestion rate by ISO day of week × hour")

    plt, sns = plt_setup()
    heat = dh.pivot(index="dow", columns="hour", values="rate").rename(index=DOW_NAMES) * 100
    fig, ax = plt.subplots(figsize=(13, 3.8))
    sns.heatmap(heat, cmap="Reds", ax=ax, cbar_kws={"label": "% readings congested"}, linewidths=0.2)
    ax.set(title="Congestion rate by day of week and hour (local)", xlabel="hour", ylabel="")
    r.figure(fig, "heatmap_dow_hour", "Congestion rate by day of week × hour")

    fig, ax = plt.subplots(figsize=(11, 3.8))
    ax.plot(work.index, work.rate * 100, marker="o", color="#c44e52", label="working days")
    ax.plot(off.index, off.rate * 100, marker="o", color="#4c72b0", label="weekends & holidays")
    for h, lab in ((am_h, "AM peak"), (pm_h, "PM peak")):
        ax.annotate(f"{lab} {h:02d}:00", (h, work.rate[h] * 100), textcoords="offset points", xytext=(0, 8), ha="center")
    ax.set(title="Hourly congestion profile", xlabel="hour (local)", ylabel="% readings congested", xticks=range(24))
    ax.legend()
    r.figure(fig, "hourly_profile", "Working-day vs weekend/holiday hourly profile")

    fig, ax = plt.subplots(figsize=(13, 3.8))
    ax.plot(daily.date_local, daily.congested_sensor_hours, lw=0.8, color="grey", label="daily")
    ax.plot(daily.date_local, daily.congested_sensor_hours.rolling(7, center=True).mean(), color="#c44e52", lw=2, label="7-day mean")
    we = daily[~daily.is_workday & ~daily.is_holiday]
    ax.scatter(we.date_local, we.congested_sensor_hours, s=10, color="#4c72b0", label="weekend", zorder=3)
    ax.scatter(hol.date_local, hol.congested_sensor_hours, s=40, marker="*", color="#dd8452", label="federal holiday", zorder=4)
    ax.set(title="Network congested sensor-hours per day", ylabel="sensor-hours")
    ax.legend(ncol=4, loc="upper left")
    r.figure(fig, "daily_series", "Daily congested sensor-hours with weekly cycle and holidays")

    r.implications = [
        "Hour of day and working-day status explain most of the variation in congestion; they are already model "
        "features (`hour`, `slot_of_day`, `is_weekend`, `is_holiday`, cyclical encodings).",
        "The PM peak is the critical operating window: dashboard alerts and model evaluation should report "
        "PM-peak performance separately.",
        "Holidays sit far closer to weekends than to working days, which confirms the separate `is_holiday` flag.",
    ]
    r.limitations = [
        "Rates are shares of 5-minute sensor readings, not vehicle-weighted: PEMS-BAY has speeds only, no flows.",
        "Six months (Jan–Jun 2017) cover no summer or holiday-season peaks.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
