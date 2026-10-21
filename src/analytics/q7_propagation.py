"""Q7 - Congestion propagation via lagged associations between connected sensors.

Spark builds the time x sensor speed matrix (pivot of the complete clean grid); numpy then computes, for every
directed road-graph edge upstream -> downstream, lagged correlations and event-conditional probabilities.

Usage: python -m src.analytics.q7_propagation
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.analytics.common import THR, AnalyticsContext, QuestionResult, pct, plt_setup, run_single

LAGS = np.arange(-12, 13)          # ±60 minutes in 5-minute steps
DAY_SLOTS = (60, 252)              # 05:00-21:00 local, working days
CHUNK = 400


def _lagged_corr(Z: np.ndarray, rows: np.ndarray, src: np.ndarray, dst: np.ndarray) -> np.ndarray:
    """corr(Z[t, src], Z[t+k, dst]) for each lag k and edge, ignoring NaNs. Returns (len(LAGS), E)."""
    T = Z.shape[0]
    out = np.full((len(LAGS), len(src)), np.nan, dtype=np.float32)
    for ki, k in enumerate(LAGS):
        r2 = rows + k
        ok = (r2 >= 0) & (r2 < T)
        A, B = Z[rows[ok]], Z[r2[ok]]
        for c in range(0, len(src), CHUNK):
            a, b = A[:, src[c:c + CHUNK]], B[:, dst[c:c + CHUNK]]
            v = ~(np.isnan(a) | np.isnan(b))
            n = v.sum(0).astype(np.float64)
            a0, b0 = np.where(v, a, 0.0), np.where(v, b, 0.0)
            with np.errstate(invalid="ignore", divide="ignore"):
                ma, mb = a0.sum(0) / n, b0.sum(0) / n
                cov = (a0 * b0).sum(0) / n - ma * mb
                va, vb = (a0 * a0).sum(0) / n - ma ** 2, (b0 * b0).sum(0) / n - mb ** 2
                out[ki, c:c + CHUNK] = cov / np.sqrt(va * vb)
    return out


def _conditional(C: np.ndarray, O: np.ndarray, rows: np.ndarray, trig: np.ndarray, resp: np.ndarray) -> np.ndarray:
    """P(resp sensor congested at t+k | onset at trig sensor at t), pooled over edges, for each lag k."""
    T = C.shape[0]
    probs = np.full(len(LAGS), np.nan)
    for ki, k in enumerate(LAGS):
        r2 = rows + k
        ok = (r2 >= 0) & (r2 < T)
        Ot, Cr = O[rows[ok]], C[r2[ok]]
        num = den = 0.0
        for c in range(0, len(trig), CHUNK):
            o = Ot[:, trig[c:c + CHUNK]]
            cr = Cr[:, resp[c:c + CHUNK]]
            valid = ~np.isnan(cr)
            num += float((o * np.where(valid, cr, 0.0)).sum())
            den += float((o * valid).sum())
        probs[ki] = num / den if den else np.nan
    return probs


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q7", "Congestion propagation (lagged associations)",
                       "Does congestion travel along the road network, in which direction (with or against the "
                       "traffic flow), and with what time lag?")
    r.method = [
        "Spark `groupBy(ts).pivot(sensor_id)` turns the complete clean grid into a 52,116 × 325 speed matrix "
        "(rows are consecutive 5-min UTC slots, so a row shift of k equals k × 5 minutes).",
        "Working days 05:00–21:00. Speeds are converted to anomalies (minus each sensor's working-day mean for that "
        "5-min slot) to remove the shared daily cycle.",
        "For every directed edge upstream i → downstream j of the DCRNN road graph (2,369 edges): correlation of "
        "anomaly_i(t) with anomaly_j(t+k), k = −60…+60 min. **k > 0: upstream leads**, i.e. propagation in the "
        "direction of travel. **k < 0: downstream leads**, i.e. a queue spreading backwards against the flow.",
        "Asymmetry per edge = mean correlation at k = −5…−30 min minus mean at k = +5…+30 min (> 0 means downstream leads).",
        "Event view: P(sensor congested at t+k | congestion onset at its neighbour at t), in both directions, against "
        "the baseline congestion probability.",
    ]
    ids, A = ctx.adjacency
    cols = [int(i) for i in ids]
    wide = (ctx.grid.select("ts_utc", "slot", "is_workday", "sensor_id", "speed")
                .groupBy("ts_utc", "slot", "is_workday").pivot("sensor_id", cols).agg(F.first("speed"))
                .orderBy("ts_utc").toPandas())
    steps = wide.ts_utc.diff().dropna().dt.total_seconds().unique()
    assert len(steps) == 1 and steps[0] == 300, f"time axis is not a regular 5-min grid: {steps[:5]}"
    S = wide[[str(c) if str(c) in wide.columns else c for c in cols]].to_numpy(dtype=np.float64)
    workday = wide.is_workday.to_numpy(bool)
    slot = wide.slot.to_numpy(int)
    del wide
    rows = np.where(workday & (slot >= DAY_SLOTS[0]) & (slot < DAY_SLOTS[1]))[0]

    prof = pd.DataFrame(S[workday]).groupby(slot[workday]).mean()
    anom = S - prof.reindex(slot).to_numpy()
    anom[~workday] = np.nan
    std = np.nanstd(anom[rows], axis=0)
    Z = (anom / np.where(std > 0, std, np.nan)).astype(np.float32)
    del anom, prof

    edges = ctx.edges_pd()
    meta = ctx.sensors_pd.set_index("sensor_id")[["latitude", "longitude"]]
    la1, lo1 = np.radians(meta.loc[edges.src].to_numpy()).T
    la2, lo2 = np.radians(meta.loc[edges.dst].to_numpy()).T
    edges["distance_km"] = 2 * 6371 * np.arcsin(np.sqrt(np.sin((la2 - la1) / 2) ** 2 +
                                                        np.cos(la1) * np.cos(la2) * np.sin((lo2 - lo1) / 2) ** 2))
    src, dst = edges.src_idx.to_numpy(), edges.dst_idx.to_numpy()
    R = _lagged_corr(Z, rows, src, dst)
    edges["corr_0"] = R[LAGS == 0][0]
    edges["peak_corr"] = np.nanmax(R, axis=0)
    edges["peak_lag_min"] = LAGS[np.nanargmax(np.nan_to_num(R, nan=-9), axis=0)] * 5
    neg = (LAGS < 0) & (LAGS >= -6)
    pos = (LAGS > 0) & (LAGS <= 6)
    edges["asymmetry"] = np.nanmean(R[neg], axis=0) - np.nanmean(R[pos], axis=0)
    informative = edges[edges.peak_corr >= 0.3]
    mean_curve = np.nanmean(R, axis=1)

    C = np.where(np.isnan(S), np.nan, (S < THR)).astype(np.float32)
    del S
    prev = np.vstack([np.full((1, C.shape[1]), np.nan, dtype=np.float32), C[:-1]])
    O = ((C == 1) & (prev == 0)).astype(np.float32)
    del prev
    up_given_down = _conditional(C, O, rows, dst, src)     # downstream onset -> upstream congested?
    down_given_up = _conditional(C, O, rows, src, dst)     # upstream onset -> downstream congested?
    base_up = np.nanmean(C[rows][:, src])
    base_down = np.nanmean(C[rows][:, dst])
    k15 = int(np.where(LAGS == 3)[0][0])
    auto = np.array([np.nanmean([np.corrcoef(*_pair(Z[:, j], rows, k)) [0, 1] for j in range(0, Z.shape[1], 5)])
                     for k in (1, 3, 6, 12)])

    share_down = (informative.asymmetry > 0).mean()
    share_lag0 = (informative.peak_lag_min == 0).mean()
    direction = ("mostly **downstream leads**, i.e. congestion spreads backwards against the flow like a growing queue"
                 if share_down >= 0.6 else
                 "mostly **upstream leads**, i.e. congestion moves with the flow" if share_down <= 0.4 else
                 "**no dominant direction**")
    gap = up_given_down[k15] - down_given_up[k15]
    lift_up, lift_down = up_given_down[k15] / base_up, down_given_up[k15] / base_down
    r.findings = [
        f"Neighbouring sensors move together: median correlation of same-time speed anomalies {np.nanmedian(edges.corr_0):.2f} "
        f"over {len(edges):,} directed edges; {len(informative):,} edges ({pct(len(informative) / len(edges))}) have a peak "
        "correlation ≥ 0.3.",
        f"Direction at 5-minute resolution: {direction}. On {pct(share_down)} of the informative edges the downstream sensor "
        f"leads (asymmetry > 0); {pct(share_lag0)} peak at lag 0 (median peak lag {informative.peak_lag_min.median():+.0f} min, "
        f"IQR {informative.peak_lag_min.quantile(0.25):+.0f} to {informative.peak_lag_min.quantile(0.75):+.0f}). Linked sensors "
        f"are a median {edges.distance_km.median():.1f} km apart (straight line). Vehicles cover that in about "
        f"{edges.distance_km.median() / 96.6 * 60:.0f} min at 60 mph, and a backward-moving queue front (typically ~15–20 km/h) "
        f"in about {edges.distance_km.median() / 20 * 60:.0f}–{edges.distance_km.median() / 15 * 60:.0f} min, at or below the "
        "5-minute resolution, so the direction is hard to resolve.",
        f"Event view: 15 min after a downstream onset, the upstream neighbour is congested with probability "
        f"{pct(up_given_down[k15])} ({lift_up:.1f}× its baseline {pct(base_up, 2)}); after an upstream onset, the downstream "
        f"neighbour is congested with probability {pct(down_given_up[k15])} ({lift_down:.1f}× baseline). "
        + ("The two directions are practically symmetric." if abs(gap) < 0.03 else
           f"The {'upstream' if gap > 0 else 'downstream'} response is stronger by {abs(gap) * 100:.1f} percentage points."),
        f"Each sensor's own anomaly persists: median autocorrelation {auto[0]:.2f} at 5 min, {auto[1]:.2f} at 15 min, "
        f"{auto[2]:.2f} at 30 min and {auto[3]:.2f} at 60 min. This memory is why the persistence baseline is strong at 5 minutes.",
    ]
    r.metrics = {"median_corr0": float(np.nanmedian(edges.corr_0)), "informative_edges": len(informative),
                 "share_downstream_leads": float(share_down), "p_up_after_down_onset_15m": float(up_given_down[k15]),
                 "p_down_after_up_onset_15m": float(down_given_up[k15]), "baseline_up": float(base_up),
                 "baseline_down": float(base_down), "autocorr": dict(zip(["5m", "15m", "30m", "60m"], auto.round(3).tolist()))}
    curve = pd.DataFrame({"lag_min": LAGS * 5, "mean_corr_all_edges": mean_curve,
                          "median_corr_informative": np.nanmedian(R[:, edges.peak_corr.to_numpy() >= 0.3], axis=1),
                          "p_upstream_cong_after_downstream_onset": up_given_down,
                          "p_downstream_cong_after_upstream_onset": down_given_up})
    r.table("lag_curves", curve, "Lagged correlation and event-conditional probabilities (lag = t_response − t_trigger)")
    r.table("edges_top", informative.sort_values("peak_corr", ascending=False)
            [["src", "dst", "weight", "distance_km", "corr_0", "peak_corr", "peak_lag_min", "asymmetry"]].head(20),
            "Strongest-linked edges (src = upstream, dst = downstream)")
    r.table("edges", edges.drop(columns=["src_idx", "dst_idx"]), "All edges")

    plt, sns = plt_setup()
    fig, axes = plt.subplots(1, 2, figsize=(14, 4))
    inf_R = R[:, edges.peak_corr.to_numpy() >= 0.3]
    q25, q50, q75 = np.nanpercentile(inf_R, [25, 50, 75], axis=1)
    axes[0].fill_between(LAGS * 5, q25, q75, alpha=0.25, color="#4c72b0", label="IQR")
    axes[0].plot(LAGS * 5, q50, color="#4c72b0", lw=2, label="median (informative edges)")
    axes[0].axvline(0, color="grey", lw=0.8)
    axes[0].set(title="corr(upstream(t), downstream(t+k))", xlabel="k (min): <0 downstream leads, >0 upstream leads", ylabel="correlation")
    axes[0].legend()
    axes[1].hist(informative.asymmetry, bins=40, color="#dd8452", edgecolor="white")
    axes[1].axvline(0, color="black", lw=1)
    axes[1].set(title=f"Asymmetry per edge ({pct(share_down)} > 0: downstream leads)", xlabel="mean r(k<0) − mean r(k>0)", ylabel="edges")
    r.figure(fig, "lagged_correlation", "Lagged correlation along road-graph edges")

    fig, ax = plt.subplots(figsize=(11, 4))
    ax.plot(LAGS * 5, up_given_down * 100, marker="o", color="#c44e52", label="upstream congested | downstream onset at 0")
    ax.plot(LAGS * 5, down_given_up * 100, marker="o", color="#4c72b0", label="downstream congested | upstream onset at 0")
    ax.axhline(base_up * 100, color="#c44e52", ls=":", lw=1, label="baseline (upstream)")
    ax.axhline(base_down * 100, color="#4c72b0", ls=":", lw=1, label="baseline (downstream)")
    ax.axvline(0, color="grey", lw=0.8)
    ax.set(title="Event-conditional congestion probability around a neighbour's onset", xlabel="minutes relative to onset",
           ylabel="% congested")
    ax.legend(fontsize=8)
    r.figure(fig, "event_conditional", "How an onset at one sensor raises its neighbour's congestion probability over time")

    r.implications = [
        f"An onset at a neighbour raises a sensor's congestion probability {min(lift_up, lift_down):.1f}–{max(lift_up, lift_down):.1f}× "
        "within 15 minutes, so the `nbr_*` features carry real predictive signal in both directions.",
        "Because most co-movement happens within the same 5-minute interval, same-time neighbour features matter more "
        "than lagged neighbour features at this horizon. Longer horizons (15–30 min) are where lagged spatial features "
        "should add the most.",
        "In the dashboard, a fresh onset can trigger an early warning for the adjacent sensors in both directions.",
    ]
    r.limitations = [
        "Correlation is not causation: shared demand surges can make neighbours co-move without propagation.",
        f"With a 5-minute resolution and linked sensors a median {edges.distance_km.median():.1f} km apart, propagation "
        "faster than one interval looks simultaneous; finer data (30 s PeMS) would be needed to resolve the direction.",
        "Graph edges come from the DCRNN road-distance kernel; distances above are straight-line, not along the road, so "
        "propagation speeds are not derived.",
        "Autocorrelation is computed on every 5th sensor, for speed.",
    ]
    return r


def _pair(x: np.ndarray, rows: np.ndarray, k: int):
    r2 = rows + k
    ok = r2 < len(x)
    a, b = x[rows[ok]], x[r2[ok]]
    v = ~(np.isnan(a) | np.isnan(b))
    return a[v], b[v]


if __name__ == "__main__":
    sys.exit(run_single(run))
