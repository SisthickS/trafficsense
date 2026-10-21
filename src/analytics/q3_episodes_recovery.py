"""Q3 - Congestion episode duration and recovery.

Usage: python -m src.analytics.q3_episodes_recovery
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from src.analytics.common import AM_PEAK, PM_PEAK, AnalyticsContext, QuestionResult, pct, plt_setup, run_single

RECOVERY_HORIZON_MIN = 180


def start_period(h: int) -> str:
    if AM_PEAK[0] <= h < AM_PEAK[1]:
        return "AM peak (06–10)"
    if PM_PEAK[0] <= h < PM_PEAK[1]:
        return "PM peak (15–19)"
    if 10 <= h < 15:
        return "midday (10–15)"
    if 19 <= h < 22:
        return "evening (19–22)"
    return "night (22–06)"


def duration_bucket(m: float) -> str:
    for hi, lab in ((5, "5 min"), (10, "10 min"), (25, "15–25 min"), (55, "30–55 min"), (115, "1–2 h")):
        if m <= hi:
            return lab
    return "> 2 h"


BUCKET_ORDER = ["5 min", "10 min", "15–25 min", "30–55 min", "1–2 h", "> 2 h"]
PERIOD_ORDER = ["night (22–06)", "AM peak (06–10)", "midday (10–15)", "PM peak (15–19)", "evening (19–22)"]


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q3", "Congestion episode duration and recovery",
                       "How long do congestion episodes last, when do the long ones happen, and how quickly does "
                       "traffic recover to free flow after an episode ends?")
    r.method = [
        "Complete 5-min grid per sensor (`clean/`). An episode is a maximal run of consecutive readings < 35 mph; "
        "a missing reading ends it.",
        "Run detection with window functions: `lag` state change → running `sum` = run id → `groupBy(sensor, run_id)`.",
        "Speed 15 min before the start (`lag(speed, 3)`) and 15 min after the end (`lead(speed, 3)`) → onset drop and "
        "recovery rise; `min_by`/`max_by` pick the values at the first and last reading of each episode.",
        "Recovery time = minutes from the episode end until the speed first reaches free flow (≥ 60 mph). Found in one "
        "pass with a descending running window: `last(ts where speed ≥ 60, ignorenulls)` gives the next free-flow time.",
    ]
    ep = ctx.episodes().copy()
    ep["period"] = ep["start_hour"].map(start_period)
    ep["bucket"] = ep["duration_min"].map(duration_bucket)
    ep["recovered_within_3h"] = ep["recovery_min"] <= RECOVERY_HORIZON_MIN
    n = len(ep)
    d = ep["duration_min"]
    sustained = ep[d >= 15]
    long_ = ep[d >= 60]
    minutes_total = d.sum()
    rec = ep.loc[ep.recovered_within_3h, "recovery_min"]

    summary = pd.DataFrame([{
        "episodes": n, "sensors": ep.sensor_id.nunique(), "median_min": d.median(), "mean_min": d.mean(),
        "p75_min": d.quantile(0.75), "p90_min": d.quantile(0.9), "p99_min": d.quantile(0.99), "max_min": d.max(),
        "share_ge_15min": (d >= 15).mean(), "share_ge_60min": (d >= 60).mean(),
        "minutes_in_ge_60min_episodes": long_.duration_min.sum() / minutes_total,
        "median_recovery_min": rec.median(), "p90_recovery_min": rec.quantile(0.9),
        "share_not_recovered_3h": 1 - ep.recovered_within_3h.mean()}])
    by_period = (ep.groupby("period").agg(episodes=("duration_min", "size"), median_duration_min=("duration_min", "median"),
                                          p90_duration_min=("duration_min", lambda s: s.quantile(0.9)),
                                          share_ge_60min=("duration_min", lambda s: (s >= 60).mean()),
                                          median_recovery_min=("recovery_min", "median"),
                                          median_min_speed=("min_speed", "median"))
                 .reindex(PERIOD_ORDER).reset_index())
    by_bucket = (ep.groupby("bucket").agg(episodes=("duration_min", "size"), median_min_speed=("min_speed", "median"),
                                          median_onset_drop_mph=("onset_drop_mph", "median"),
                                          median_recovery_rise_mph=("recovery_rise_mph", "median"),
                                          median_recovery_min=("recovery_min", "median"),
                                          share_recovered_3h=("recovered_within_3h", "mean"))
                 .reindex(BUCKET_ORDER).reset_index())
    by_bucket["share_of_episodes"] = by_bucket["episodes"] / n
    sus = sustained.dropna(subset=["onset_drop_mph", "recovery_rise_mph"])
    onset_med, rise_med = sus.onset_drop_mph.median(), sus.recovery_rise_mph.median()
    longest = (ep.nlargest(10, "duration_min")[["sensor_id", "start_local", "duration_min", "min_speed", "mean_speed", "recovery_min"]]
               .assign(start_local=lambda x: pd.to_datetime(x.start_local).dt.strftime("%Y-%m-%d %H:%M")))
    pm = by_period.set_index("period").loc["PM peak (15–19)"]
    am = by_period.set_index("period").loc["AM peak (06–10)"]
    lp, lr, sp, sr = ("AM peak", am, "PM peak", pm) if am.median_duration_min >= pm.median_duration_min else ("PM peak", pm, "AM peak", am)
    fast_recovery = rise_med > onset_med

    r.findings = [
        f"{n:,} episodes on {ep.sensor_id.nunique()} sensors. The median lasts **{d.median():.0f} min** (p90 {d.quantile(0.9):.0f} min, "
        f"longest {d.max() / 60:.1f} h), and {pct((d >= 15).mean())} last ≥ 15 min (the Caltrans duration criterion).",
        f"Duration is heavy-tailed: episodes ≥ 1 h are {pct((d >= 60).mean())} of episodes but "
        f"**{pct(long_.duration_min.sum() / minutes_total)} of all congested minutes**.",
        f"{lp} episodes last longer (median {lr.median_duration_min:.0f} min, {pct(lr.share_ge_60min)} ≥ 1 h) than {sp} "
        f"episodes (median {sr.median_duration_min:.0f} min, {pct(sr.share_ge_60min)} ≥ 1 h); {sp} has "
        f"{int(sr.episodes):,} episodes vs {int(lr.episodes):,}"
        + (" (more, shorter episodes)." if sr.episodes > lr.episodes else "."),
        f"Recovery to free flow (≥ 60 mph) takes a median **{rec.median():.0f} min** after the episode ends (p90 "
        f"{rec.quantile(0.9):.0f} min); {pct(1 - ep.recovered_within_3h.mean())} of episodes do not reach free flow within 3 h.",
        (f"In sustained episodes (≥ 15 min), speed falls a median {onset_med:.1f} mph in the 15 min before onset and rises "
         f"{rise_med:.1f} mph in the 15 min after the end. The first rebound is quick, but the last stretch to free flow "
         f"is slow (median {rec.median():.0f} min).") if fast_recovery else
        (f"In sustained episodes (≥ 15 min), speed falls a median {onset_med:.1f} mph in the 15 min before onset but "
         f"rises only {rise_med:.1f} mph in the 15 min after the end, consistent with sudden breakdown and gradual queue discharge."),
    ]
    r.metrics = {"episodes": n, "median_duration_min": float(d.median()), "p90_duration_min": float(d.quantile(0.9)),
                 "share_ge_15": float((d >= 15).mean()), "minutes_share_ge_60": float(long_.duration_min.sum() / minutes_total),
                 "median_recovery_min": float(rec.median()), "onset_drop_mph": float(onset_med), "recovery_rise_mph": float(rise_med)}
    r.table("summary", summary, "Episode duration and recovery summary")
    r.table("by_start_period", by_period, "Episodes by start period (local time)")
    r.table("by_duration", by_bucket, "Episodes by duration bucket: severity, onset/recovery speed changes, recovery time")
    r.table("longest", longest, "Ten longest episodes")

    plt, sns = plt_setup()
    fig, axes = plt.subplots(1, 2, figsize=(13, 4))
    srt = np.sort(d.values)
    ccdf = 1 - np.arange(len(srt)) / len(srt)
    axes[0].loglog(srt, ccdf, color="#4c72b0")
    axes[0].axvline(15, color="grey", ls=":", label="15 min")
    axes[0].axvline(60, color="grey", ls="--", label="1 h")
    axes[0].set(title="Episode duration (CCDF, log-log)", xlabel="duration (min)", ylabel="P(duration ≥ x)")
    axes[0].legend()
    order = [p for p in PERIOD_ORDER if p in set(ep.period)]
    sns.boxplot(data=ep[d >= 15], x="period", y="duration_min", order=order, ax=axes[1], showfliers=False, color="#dd8452")
    axes[1].set(title="Sustained episodes (≥ 15 min) by start period", xlabel="", ylabel="duration (min)")
    axes[1].tick_params(axis="x", rotation=20)
    r.figure(fig, "duration", "Duration distribution (heavy tail) and duration by start period")

    fig, axes = plt.subplots(1, 2, figsize=(13, 4))
    rb = ep[ep.recovered_within_3h]
    sns.boxplot(data=rb, x="bucket", y="recovery_min", order=BUCKET_ORDER, ax=axes[0], showfliers=False, color="#55a868")
    axes[0].set(title="Recovery time to free flow by episode duration", xlabel="episode duration", ylabel="minutes to ≥ 60 mph")
    vals = [sus.onset_drop_mph.clip(-40, 70), sus.recovery_rise_mph.clip(-40, 70)]
    axes[1].hist(vals, bins=40, label=["drop in 15 min before onset", "rise in 15 min after end"], color=["#c44e52", "#55a868"], alpha=0.75)
    axes[1].set(title="Onset vs recovery speed change (sustained episodes)", xlabel="mph per 15 min", ylabel="episodes")
    axes[1].legend()
    r.figure(fig, "recovery", "Recovery time and onset/recovery asymmetry")

    r.implications = [
        "Most episodes are short, but most congested **time** falls in long (≥ 1 h) episodes. A 5-minute-ahead model "
        "therefore mainly has to get onset and clearance right; inside a long episode, persistence is already accurate.",
        "Full recovery to free flow lags the end of congestion. The dashboard should show a 'recovering' state "
        "(35–60 mph after an episode) rather than an immediate 'all clear'.",
        "Episode duration and recovery are inputs to the severity index (Q9).",
    ]
    r.limitations = [
        "Short gaps (≤ 15 min) were forward-filled during cleaning, so an episode can span a filled reading; longer gaps split episodes.",
        "Episodes that reach the end of the data have no observed recovery and are counted as 'not recovered'.",
        "Local wall-clock hours are used for start periods (DST-aware), while durations use true UTC time.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
