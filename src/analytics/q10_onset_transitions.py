"""Q10 - Congestion onset and state transitions.

Usage: python -m src.analytics.q10_onset_transitions
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.analytics.common import FREE, THR, AnalyticsContext, QuestionResult, pct, plt_setup, run_single
from src.modeling.data import load_split

STATES = ["free", "slowed", "congested"]
PRECURSORS = {
    "delta_15m": "speed change over the last 15 min (mph)",
    "delta_5m": "speed change over the last 5 min (mph)",
    "roll_std_60m": "speed std over the last hour",
    "speed_ratio_ff": "speed / free-flow speed",
    "nbr_down_congested_frac": "share of downstream neighbours congested",
    "nbr_up_congested_frac": "share of upstream neighbours congested",
    "congested_frac_60m": "share of the last hour congested",
    "hist_congested_rate_next": "historical congestion rate for the target slot",
}


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q10", "Congestion onset and state transitions",
                       "How does traffic move between free, slowed and congested states, how likely is a new congestion "
                       "onset at each time and speed, and which signals precede an onset?")
    r.method = [
        f"Three states per observed reading: free (≥ {FREE:.0f} mph), slowed ({THR:.0f}–{FREE:.0f}), congested (< {THR:.0f}). "
        "The transition from t to t + 5 min uses `lead()` over a window per sensor ordered by UTC time; both readings must be observed.",
        "First-order Markov transition matrix (overall and by working-day hour). Expected dwell time in a state = "
        "5 min / (1 − P(stay)). Stationary distribution from the matrix, compared with the observed state shares.",
        "Onset hazard = P(congested at t+5 | not congested at t), by hour and by 5-mph speed band.",
        "Precursors (training split of the feature table only, so no test data is used): standardized mean difference "
        "(SMD) between onset rows (now not congested, next congested) and non-onset rows. Computed overall and within "
        f"the at-risk band {THR:.0f}–50 mph, so the comparison is not just 'slow vs fast'.",
    ]
    w = Window.partitionBy("sensor_id").orderBy("ts_utc")
    state = (F.when(~F.col("observed"), F.lit(None)).when(F.col("speed") < THR, "congested")
              .when(F.col("speed") < FREE, "slowed").otherwise("free"))
    t = (ctx.grid.select("sensor_id", "ts_utc", "hour", "is_workday", "observed", "speed")
             .withColumn("s", state).withColumn("s_next", F.lead("s").over(w))
             .filter(F.col("s").isNotNull() & F.col("s_next").isNotNull()))
    trans = t.groupBy("is_workday", "hour", "s", "s_next").count().toPandas()
    band = (F.floor(F.col("speed") / 5) * 5).cast("int")
    hazard_speed = (t.filter(F.col("s") != "congested").groupBy(band.alias("speed_band"), "is_workday")
                     .agg(F.avg((F.col("s_next") == "congested").cast("int")).alias("onset_prob"),
                          F.count(F.lit(1)).alias("n")).toPandas().sort_values("speed_band"))

    M = trans.groupby(["s", "s_next"])["count"].sum().unstack().reindex(index=STATES, columns=STATES).fillna(0)
    P = M.div(M.sum(axis=1), axis=0)
    dwell = 5 / (1 - np.diag(P.to_numpy()))
    evals, evecs = np.linalg.eig(P.to_numpy().T)
    stat = np.real(evecs[:, np.argmin(np.abs(evals - 1))])
    stat = stat / stat.sum()
    observed_share = M.sum(axis=1) / M.to_numpy().sum()
    tm = pd.DataFrame({"from": STATES, **{f"to_{s}": P[s].values for s in STATES}, "expected_dwell_min": dwell,
                       "stationary_share": stat, "observed_share": observed_share.values})

    wk = trans[trans.is_workday & (trans.s != "congested")]
    hz = (wk.assign(onset=lambda d: np.where(d.s_next == "congested", d["count"], 0))
            .groupby(["hour", "s"]).agg(onset=("onset", "sum"), n=("count", "sum")).reset_index())
    hz["onset_prob"] = hz.onset / hz.n
    hz_h = hz.pivot(index="hour", columns="s", values="onset_prob").reindex(columns=["free", "slowed"])
    all_from = wk.groupby("hour").apply(lambda d: d.loc[d.s_next == "congested", "count"].sum() / d["count"].sum(),
                                        include_groups=False).rename("onset_prob_any")
    hz_h = hz_h.join(all_from).reset_index()
    peak_h = int(hz_h.loc[hz_h.onset_prob_any.idxmax(), "hour"])

    # precursors on the training split
    f = load_split(ctx.spark, "train", columns=["speed", "congested_now", "label"] + list(PRECURSORS))
    f = f.filter(F.col("congested_now") == 0).withColumn("onset", F.col("label"))
    aggs = []
    for c in PRECURSORS:
        aggs += [F.avg(c).alias(f"{c}__mean"), F.stddev(c).alias(f"{c}__sd")]
    def smd_table(df, scope):
        res = df.groupBy("onset").agg(F.count(F.lit(1)).alias("n"), *aggs).toPandas().set_index("onset")
        rows = []
        for c, desc in PRECURSORS.items():
            m1, m0 = res.loc[1, f"{c}__mean"], res.loc[0, f"{c}__mean"]
            s1, s0 = res.loc[1, f"{c}__sd"], res.loc[0, f"{c}__sd"]
            pooled = np.sqrt((s1 ** 2 + s0 ** 2) / 2)
            rows.append({"scope": scope, "feature": c, "description": desc, "mean_onset": m1, "mean_no_onset": m0,
                         "smd": (m1 - m0) / pooled if pooled else np.nan, "onset_rows": int(res.loc[1, "n"]),
                         "no_onset_rows": int(res.loc[0, "n"])})
        return pd.DataFrame(rows)
    smd = pd.concat([smd_table(f, "all non-congested rows"),
                     smd_table(f.filter(F.col("speed").between(THR, 50)), f"at-risk band {THR:.0f}–50 mph")])
    traj_cols = ["speed_lag_30m", "speed_lag_15m", "speed_lag_10m", "speed_lag_5m", "speed"]
    traj = (load_split(ctx.spark, "train", columns=traj_cols + ["congested_now", "label"])
            .filter((F.col("congested_now") == 0) & F.col("speed").between(THR, 50))
            .groupBy("label").agg(*[F.avg(c).alias(c) for c in traj_cols]).toPandas().set_index("label"))

    at_risk = smd[smd.scope.str.startswith("at-risk")].reindex(columns=smd.columns)
    top = at_risk.reindex(at_risk.smd.abs().sort_values(ascending=False).index).head(3)
    hs = hazard_speed[hazard_speed.is_workday]
    near = hs[hs.speed_band == 35].onset_prob.iloc[0] if (hs.speed_band == 35).any() else np.nan
    fast = hs[hs.speed_band >= 60]
    fast_p = np.average(fast.onset_prob, weights=fast.n) if len(fast) else np.nan
    r.findings = [
        f"States are sticky: P(stay) = {P.loc['free', 'free']:.3f} free, {P.loc['slowed', 'slowed']:.3f} slowed, "
        f"{P.loc['congested', 'congested']:.3f} congested, giving expected dwell times of {dwell[0]:.0f}, {dwell[1]:.0f} and "
        f"{dwell[2]:.0f} min.",
        f"Congestion is almost always entered via 'slowed': P(free → congested) = {P.loc['free', 'congested']:.1e} vs "
        f"P(slowed → congested) = {P.loc['slowed', 'congested']:.4f} ({P.loc['slowed', 'congested'] / max(P.loc['free', 'congested'], 1e-9):,.0f}×).",
        f"The first-order chain reproduces the state mix: stationary shares {', '.join(f'{s} {pct(v)}' for s, v in zip(STATES, stat))} "
        f"vs observed {', '.join(f'{s} {pct(v)}' for s, v in zip(STATES, observed_share))}.",
        f"Onset hazard on working days peaks at **{peak_h:02d}:00** ({pct(hz_h.onset_prob_any.max(), 2)} of non-congested "
        f"readings tip into congestion within 5 min). By speed: {pct(near, 1)} in the 35–40 mph band vs {pct(fast_p, 3)} at ≥ 60 mph.",
        "Strongest precursors in the at-risk band (|SMD|): " + "; ".join(
            f"{row.feature} {row.smd:+.2f} (onset {row.mean_onset:.2f} vs {row.mean_no_onset:.2f})" for row in top.itertuples()) + ".",
        f"Speed trajectory before an onset (at-risk band, training split): {traj.loc[1, 'speed_lag_30m']:.1f} → "
        f"{traj.loc[1, 'speed']:.1f} mph over 30 min, vs {traj.loc[0, 'speed_lag_30m']:.1f} → {traj.loc[0, 'speed']:.1f} mph "
        "without an onset.",
    ]
    r.metrics = {"p_stay": {s: float(P.loc[s, s]) for s in STATES}, "dwell_min": dict(zip(STATES, map(float, dwell))),
                 "p_free_to_congested": float(P.loc["free", "congested"]), "p_slowed_to_congested": float(P.loc["slowed", "congested"]),
                 "onset_peak_hour": peak_h, "top_precursor": top.iloc[0].feature}
    r.table("transition_matrix", tm, "Transition matrix P(state at t+5 | state at t), dwell time and stationary distribution")
    r.table("onset_hazard_by_hour", hz_h, "Working-day onset probability by hour, from free, from slowed, and either")
    r.table("onset_hazard_by_speed", hazard_speed, "Onset probability by 5-mph speed band")
    r.table("precursors", smd.sort_values(["scope", "smd"], key=lambda s: s if s.dtype == object else -s.abs()),
            "Precursor signals: standardized mean difference, onset vs no onset (training split)")
    r.table("pre_onset_trajectory", traj.reset_index().rename(columns={"label": "onset_next"}),
            "Mean speed over the 30 min before t (at-risk band, training split)")

    plt, sns = plt_setup()
    fig, axes = plt.subplots(1, 3, figsize=(17, 4.3), gridspec_kw={"width_ratios": [0.8, 1.2, 1.2]})
    sns.heatmap(P, annot=True, fmt=".3f", cmap="Blues", ax=axes[0], cbar=False)
    axes[0].set(title="P(next | now), all readings", xlabel="state at t+5 min", ylabel="state at t")
    axes[1].plot(hz_h.hour, hz_h.slowed * 100, marker="o", color="#dd8452", label="from slowed")
    axes[1].plot(hz_h.hour, hz_h.free * 100, marker="o", color="#55a868", label="from free")
    axes[1].plot(hz_h.hour, hz_h.onset_prob_any * 100, color="black", lw=2, label="any non-congested")
    axes[1].set_yscale("log")
    axes[1].set(title="Onset hazard by hour (working days)", xlabel="hour", ylabel="% tipping into congestion (log)", xticks=range(0, 24, 2))
    axes[1].legend(fontsize=8)
    axes[2].bar(hs.speed_band.astype(str), hs.onset_prob * 100, color="#c44e52")
    axes[2].set_yscale("log")
    axes[2].set(title="Onset hazard by current speed (working days)", xlabel="speed band (mph, lower edge)", ylabel="% onset (log)")
    axes[2].tick_params(axis="x", rotation=45)
    fig.tight_layout()
    r.figure(fig, "transitions", "Transition matrix and onset hazard by hour and by speed")

    fig, axes = plt.subplots(1, 2, figsize=(14, 4.3))
    sub = at_risk.sort_values("smd")
    axes[0].barh(sub.feature, sub.smd, color=np.where(sub.smd > 0, "#c44e52", "#4c72b0"))
    axes[0].axvline(0, color="black", lw=0.8)
    axes[0].set(title=f"Precursors in the {THR:.0f}–50 mph band (SMD onset vs no onset)", xlabel="standardized mean difference")
    mins = [-30, -15, -10, -5, 0]
    axes[1].plot(mins, traj.loc[1, traj_cols], marker="o", color="#c44e52", label="onset follows")
    axes[1].plot(mins, traj.loc[0, traj_cols], marker="o", color="#4c72b0", label="no onset")
    axes[1].set(title="Mean speed before t (at-risk band)", xlabel="minutes relative to t", ylabel="mph")
    axes[1].legend()
    r.figure(fig, "precursors", "Which signals precede a congestion onset")

    r.implications = [
        "Since onsets come almost exclusively from 'slowed', the dashboard can show an 'at risk' state (35–50 mph and "
        "falling) as an early warning before the model's congestion alert fires.",
        "The strongest precursors (falling speed, congested neighbours, recurring slot) correspond to existing features, "
        "which supports the Phase 3 design (see Q17 for their measured contribution).",
        "Long dwell times explain why persistence is a strong 5-minute baseline (Q16).",
    ]
    r.limitations = [
        "The Markov chain is first-order and pooled over sensors; real dynamics depend on longer history (captured by lags/rolling features).",
        "SMD shows association, not causation; precursors overlap (correlated features).",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
