"""Q11 - Congestion recovery performance across sensors.

Builds on the shared episode table (AnalyticsContext.episodes(), also used by Q3 and Q9): Q3 describes recovery for
the network as a whole, while Q11 compares sensors with each other and with their road-graph peers.

Usage: python -m src.analytics.q11_recovery_performance
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from src.analytics.common import AnalyticsContext, QuestionResult, pct, plt_setup, run_single

MIN_EPISODES = 10
SUSTAINED_MIN = 15
HORIZON = 180


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q11", "Congestion recovery performance across sensors",
                       "Which sensors recover slowly from congestion, how consistent is their recovery, and does recovery "
                       "depend on time of day or episode length?")
    r.method = [
        f"Sustained episodes (≥ {SUSTAINED_MIN} min) from the shared episode table. Recovery time = minutes from the "
        f"episode end until speed ≥ 60 mph (censored at {HORIZON} min).",
        f"Per sensor with ≥ {MIN_EPISODES} sustained episodes: median and IQR of recovery time, share recovered within "
        f"30/60 min and not within {HORIZON} min, and median episode length.",
        "Peer comparison: each sensor's median recovery vs the median of its road-graph neighbours (both directions), "
        "giving a recovery gap in minutes and a z-score across sensors.",
        "Drivers: Spearman correlation of recovery with episode duration; recovery by the hour in which the episode ends.",
    ]
    ep = ctx.episodes()
    ep = ep[ep.duration_min >= SUSTAINED_MIN].copy()
    ep["rec"] = ep.recovery_min.where(ep.recovery_min <= HORIZON)
    ep["censored"] = ep.rec.isna()
    ep["end_hour"] = (pd.to_datetime(ep.start_local) + pd.to_timedelta(ep.duration_min - 5, unit="min")).dt.hour
    recov = ep.rec.fillna(HORIZON)

    g = ep.assign(rec_c=recov).groupby("sensor_id")
    s = pd.DataFrame({
        "sustained_episodes": g.size(),
        "median_duration_min": g.duration_min.median(),
        "median_recovery_min": g.rec_c.median(),
        "recovery_iqr_min": g.rec_c.quantile(0.75) - g.rec_c.quantile(0.25),
        "share_recovered_30min": g.rec_c.apply(lambda x: (x <= 30).mean()),
        "share_recovered_60min": g.rec_c.apply(lambda x: (x <= 60).mean()),
        "share_not_recovered": g.censored.mean(),
        "congested_minutes": g.duration_min.sum(),
    }).reset_index()
    s = s[s.sustained_episodes >= MIN_EPISODES].copy()
    edges = ctx.edges_pd()
    nb = pd.concat([edges[["src", "dst"]].rename(columns={"src": "sensor_id", "dst": "peer"}),
                    edges[["dst", "src"]].rename(columns={"dst": "sensor_id", "src": "peer"})]).drop_duplicates()
    med = s.set_index("sensor_id").median_recovery_min
    nb = nb[nb.peer.isin(med.index)]
    peer = nb.assign(peer_rec=nb.peer.map(med)).groupby("sensor_id").peer_rec.median().rename("peer_median_recovery_min")
    s = s.merge(peer, on="sensor_id", how="left")
    s["recovery_gap_vs_peers_min"] = s.median_recovery_min - s.peer_median_recovery_min
    s["recovery_z"] = (s.median_recovery_min - s.median_recovery_min.mean()) / s.median_recovery_min.std()
    s = s.merge(ctx.sensors_pd[["sensor_id", "latitude", "longitude"]], on="sensor_id", how="left")
    slow = s[(s.recovery_z >= 1) & (s.recovery_gap_vs_peers_min > 0)].sort_values("median_recovery_min", ascending=False)
    fast = s.sort_values("median_recovery_min").head(10)

    rho = ep[["duration_min"]].assign(rec=recov).corr(method="spearman").iloc[0, 1]
    by_end = (ep.assign(rec_c=recov).groupby("end_hour")
                .agg(episodes=("rec_c", "size"), median_recovery_min=("rec_c", "median"),
                     share_not_recovered=("censored", "mean")).reset_index())
    busy = by_end[by_end.episodes >= 200]
    worst_end = busy.loc[busy.median_recovery_min.idxmax()]
    best_end = busy.loc[busy.median_recovery_min.idxmin()]
    rec_hot = np.corrcoef(s.congested_minutes.rank(), s.median_recovery_min.rank())[0, 1]

    r.findings = [
        f"{len(s)} sensors have ≥ {MIN_EPISODES} sustained episodes. The median sensor recovers to free flow in "
        f"**{s.median_recovery_min.median():.0f} min**, but sensors range from {s.median_recovery_min.min():.0f} to "
        f"{s.median_recovery_min.max():.0f} min (IQR across sensors {s.median_recovery_min.quantile(0.25):.0f}–"
        f"{s.median_recovery_min.quantile(0.75):.0f} min).",
        f"**{len(slow)} slow-recovery sensors** are ≥ 1 sd slower than average *and* slower than their neighbours"
        + (f"; worst {int(slow.iloc[0].sensor_id)}: median {slow.iloc[0].median_recovery_min:.0f} min "
           f"(peers {slow.iloc[0].peer_median_recovery_min:.0f} min), {pct(slow.iloc[0].share_not_recovered)} of episodes "
           f"not recovered within {HORIZON} min." if len(slow) else "."),
        f"Recovery depends on when an episode ends: median {worst_end.median_recovery_min:.0f} min for episodes ending at "
        f"{int(worst_end.end_hour):02d}:00 vs {best_end.median_recovery_min:.0f} min at {int(best_end.end_hour):02d}:00 "
        "(hours with ≥ 200 episodes). Demand that remains high delays full recovery.",
        f"Longer episodes recover {'more slowly' if rho > 0.1 else 'faster' if rho < -0.1 else 'at a similar pace'} "
        f"(episode-level Spearman ρ = {rho:.2f} between duration and recovery time)"
        + (", likely because long episodes typically end as evening demand falls, while short ones end inside busy "
           "periods that keep speeds below free flow" if rho < -0.1 else "")
        + f". Across sensors, total congested time and median recovery have rank correlation {rec_hot:.2f}.",
        f"Recovery is unpredictable at many sensors: median within-sensor IQR is {s.recovery_iqr_min.median():.0f} min.",
    ]
    r.metrics = {"sensors": len(s), "median_sensor_recovery_min": float(s.median_recovery_min.median()),
                 "slow_recovery_sensors": len(slow), "spearman_duration_recovery": float(rho)}
    cols = ["sensor_id", "sustained_episodes", "median_recovery_min", "peer_median_recovery_min", "recovery_gap_vs_peers_min",
            "recovery_iqr_min", "share_recovered_30min", "share_recovered_60min", "share_not_recovered", "median_duration_min"]
    r.table("slow_recovery_sensors", slow[cols], "Slow-recovery sensors (≥ 1 sd slower than average and slower than neighbours)")
    r.table("fastest_recovery", fast[cols], "Ten fastest-recovering sensors")
    r.table("by_end_hour", by_end, "Recovery by the hour the episode ends")
    r.table("all_sensors", s, "Recovery metrics for all sensors with enough episodes")

    plt, sns = plt_setup()
    fig, axes = plt.subplots(1, 3, figsize=(18, 4.6))
    axes[0].hist(s.median_recovery_min, bins=30, color="#4c72b0", edgecolor="white")
    axes[0].set(title="Median recovery time per sensor", xlabel="minutes to ≥ 60 mph", ylabel="sensors")
    sc = axes[1].scatter(s.median_duration_min, s.median_recovery_min, c=s.share_not_recovered * 100, cmap="magma_r", s=18)
    fig.colorbar(sc, ax=axes[1], label=f"% not recovered in {HORIZON} min")
    axes[1].set(title="Episode length vs recovery (per sensor)", xlabel="median episode (min)", ylabel="median recovery (min)")
    axes[2].bar(by_end.end_hour, by_end.median_recovery_min, color="#55a868")
    axes[2].set(title="Recovery by hour the episode ends", xlabel="end hour", ylabel="median recovery (min)", xticks=range(0, 24, 2))
    fig.tight_layout()
    r.figure(fig, "recovery", "How recovery varies between sensors and through the day")

    fig, ax = plt.subplots(figsize=(8, 6.5))
    ax.scatter(s.longitude, s.latitude, c="#d9d9d9", s=12)
    if len(slow):
        ax.scatter(slow.longitude, slow.latitude, c=slow.median_recovery_min, cmap="Reds", s=50, edgecolor="black")
        for _, row in slow.head(5).iterrows():
            ax.annotate(str(int(row.sensor_id)), (row.longitude, row.latitude), fontsize=8, xytext=(4, 4), textcoords="offset points")
    ax.set(title="Slow-recovery sensors (red)", xlabel="longitude", ylabel="latitude")
    r.figure(fig, "map", "Where recovery is slow")

    r.implications = [
        "**Recommendation:** prioritise the slow-recovery sensors for incident-clearance response and for a review of "
        "ramp metering or merge geometry: they lose the most time after the queue starts to clear.",
        "The dashboard should show expected recovery time per sensor (its median) when an episode ends, not just 'cleared'.",
        "End-hour effects show recovery is demand-driven; recovery forecasts should condition on time of day.",
    ]
    r.limitations = [
        f"Recovery is censored at {HORIZON} min and at missing data; censored episodes are counted as {HORIZON} min in medians.",
        "Without incident logs, slow recovery cannot be attributed to incidents versus geometry.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
