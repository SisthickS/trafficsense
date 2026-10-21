"""Feature engineering, congestion target and chronological splits for TrafficSense.

    data/processed/<dataset>/clean/                 (from clean_traffic.py)
      -> data/processed/<dataset>/features/split=train|val|test/   model-ready rows (Parquet)
      -> data/processed/<dataset>/sensor_stats/     train-only per-sensor statistics (for streaming reuse)
      -> data/processed/<dataset>/slot_profile/     train-only sensor x day-of-week x 5-min-slot profile
      -> data/processed/<dataset>/features_meta.json  feature list, label, split boundaries (read by MLlib phase)
      -> reports/preprocessing/<dataset>_feature_report.{json,md}

Prediction task: for each (sensor, t) predict label = 1 if the OBSERVED speed at t + 5 min is below the
Caltrans congestion threshold (35 mph), using only information available at time t.

Leakage controls:
  * every lag / rolling feature looks backwards only (window frames end at the current row);
  * the clean data is a complete UTC grid, so row offsets are exact time offsets (lag 1 = 5 min);
  * sensor statistics and historical profiles are computed from the training period only;
  * chronological split with a purge gap before each boundary; no shuffling across time;
  * targets come from observed readings only (never from forward-filled values).

Usage (project root, env sourced):
    python -m src.preprocessing.build_features
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import shutil
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, RAW_DIR, SCRATCH_DIR, dataset_slug, processed_dataset_dir, raw_dataset_dir
from src.common.spark import get_spark
from src.preprocessing.clean_traffic import PREPROCESSING_REPORT_DIR, QUALITY_OBSERVED
from src.preprocessing.congestion import (
    CONGESTION_SPEED_MPH, FREE_FLOW_SPEED_MPH, HORIZON_MINUTES, LEVELS, SOURCES, congestion_level, is_congested,
)
from src.preprocessing.download_dataset import load_verified_manifest
from src.preprocessing.raw_reader import safe_load_adjacency
from src.preprocessing.sources import SPECS
from src.preprocessing.splits import SPLITS, SplitBoundaries, SplitConfig, compute_boundaries, in_purge_gap, split_column, train_mask

log = logging.getLogger("build_features")

KEY_COLUMNS = ["sensor_id", "ts_utc", "ts_local", "date_local"]
LABEL = "label"
AUX_TARGETS = ["speed_next", "level_next"]

# name -> (group, description). Order defines the model feature vector.
FEATURES: dict[str, tuple[str, str]] = {
    "hour": ("temporal", "Local hour of day (0-23)"),
    "slot_of_day": ("temporal", "Local 5-minute slot of the day (0-287)"),
    "day_of_week": ("temporal", "ISO day of week, Monday=1 ... Sunday=7 (local)"),
    "is_weekend": ("temporal", "1 on Saturday/Sunday"),
    "is_holiday": ("temporal", "1 on US federal holidays (observed dates)"),
    "tod_sin": ("temporal", "sin(2*pi*slot_of_day/288) - cyclical time of day"),
    "tod_cos": ("temporal", "cos(2*pi*slot_of_day/288)"),
    "dow_sin": ("temporal", "sin(2*pi*(day_of_week-1)/7) - cyclical day of week"),
    "dow_cos": ("temporal", "cos(2*pi*(day_of_week-1)/7)"),
    "speed": ("current", "Cleaned speed at t (mph)"),
    "is_imputed": ("current", "1 if the speed at t was forward-filled"),
    "congested_now": ("current", "1 if speed at t < 35 mph"),
    "speed_lag_5m": ("lag", "Speed at t-5 min"),
    "speed_lag_10m": ("lag", "Speed at t-10 min"),
    "speed_lag_15m": ("lag", "Speed at t-15 min"),
    "speed_lag_30m": ("lag", "Speed at t-30 min"),
    "speed_lag_60m": ("lag", "Speed at t-60 min"),
    "speed_target_slot_1d": ("lag", "Speed 24 h before the target slot (t+5 min-24 h)"),
    "speed_target_slot_1w": ("lag", "Speed 7 days before the target slot (t+5 min-7 d)"),
    "delta_5m": ("lag", "speed - speed_lag_5m (mph per 5 min)"),
    "delta_15m": ("lag", "speed - speed_lag_15m"),
    "roll_mean_15m": ("rolling", "Mean speed over (t-15 min, t]"),
    "roll_mean_30m": ("rolling", "Mean speed over (t-30 min, t]"),
    "roll_mean_60m": ("rolling", "Mean speed over (t-60 min, t]"),
    "roll_std_60m": ("rolling", "Std-dev of speed over (t-60 min, t]"),
    "roll_min_60m": ("rolling", "Minimum speed over (t-60 min, t]"),
    "congested_frac_60m": ("rolling", "Share of the last 12 slots below 35 mph"),
    "imputed_count_60m": ("rolling", "Forward-filled readings in the last 12 slots (data quality)"),
    "nbr_up_speed": ("graph", "Adjacency-weighted mean speed of upstream sensors at t (own speed if none)"),
    "nbr_down_speed": ("graph", "Adjacency-weighted mean speed of downstream sensors at t (own speed if none)"),
    "nbr_up_congested_frac": ("graph", "Adjacency-weighted share of upstream sensors below 35 mph at t"),
    "nbr_down_congested_frac": ("graph", "Adjacency-weighted share of downstream sensors below 35 mph at t"),
    "latitude": ("sensor", "Sensor latitude"),
    "longitude": ("sensor", "Sensor longitude"),
    "in_degree": ("sensor", "Number of upstream neighbours in the road graph"),
    "out_degree": ("sensor", "Number of downstream neighbours in the road graph"),
    "ff_speed": ("sensor", "Free-flow speed: 85th percentile of observed speed (train period only)"),
    "speed_ratio_ff": ("current", "Current speed / sensor free-flow speed (current state normalised per sensor)"),
    "sensor_congested_rate": ("sensor", "Share of observed readings below 35 mph (train period only)"),
    "hist_speed_next": ("profile", "Mean train-period speed for this sensor at the target's day-of-week and slot "
                                   "(leave-one-out for training rows)"),
    "hist_congested_rate_next": ("profile", "Train-period congestion rate for this sensor at the target's day-of-week and "
                                            "slot (leave-one-out for training rows)"),
}
FEATURE_COLUMNS = list(FEATURES)


@dataclass(frozen=True)
class FeatureConfig:
    slots_per_day: int = 288
    neighbor_batch_rows: int = 1000


# ------------------------------------------------------------------------------------------- calendar
def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    nxt = date(year + (month == 12), month % 12 + 1, 1)
    last = nxt - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _observed(d: date) -> date:
    return d - timedelta(days=1) if d.weekday() == 5 else d + timedelta(days=1) if d.weekday() == 6 else d


def us_federal_holidays(years) -> list[date]:
    """Observed US federal holidays (rule-based, so it works for any dataset year)."""
    out = []
    for y in years:
        out += [_observed(date(y, 1, 1)), _nth_weekday(y, 1, 0, 3), _nth_weekday(y, 2, 0, 3), _last_weekday(y, 5, 0),
                _observed(date(y, 7, 4)), _nth_weekday(y, 9, 0, 1), _nth_weekday(y, 10, 0, 2),
                _observed(date(y, 11, 11)), _nth_weekday(y, 11, 3, 4), _observed(date(y, 12, 25))]
        if y >= 2021:
            out.append(_observed(date(y, 6, 19)))
    return sorted(set(out))


# ------------------------------------------------------------------------------------------- features
def add_temporal(df: DataFrame, holidays: list[date], slots_per_day: int = 288) -> DataFrame:
    slot = F.hour("ts_local") * 12 + F.floor(F.minute("ts_local") / 5)
    iso_dow = ((F.dayofweek("ts_local") + 5) % 7) + 1
    two_pi = 2 * math.pi
    return (df.withColumn("hour", F.hour("ts_local"))
              .withColumn("slot_of_day", slot.cast("int"))
              .withColumn("day_of_week", iso_dow.cast("int"))
              .withColumn("is_weekend", (F.col("day_of_week") >= 6).cast("int"))
              .withColumn("is_holiday", F.col("date_local").isin(holidays).cast("int"))
              .withColumn("tod_sin", F.sin(two_pi * F.col("slot_of_day") / slots_per_day))
              .withColumn("tod_cos", F.cos(two_pi * F.col("slot_of_day") / slots_per_day))
              .withColumn("dow_sin", F.sin(two_pi * (F.col("day_of_week") - 1) / 7))
              .withColumn("dow_cos", F.cos(two_pi * (F.col("day_of_week") - 1) / 7)))


def add_history(df: DataFrame, slots_per_day: int = 288) -> DataFrame:
    """Lags, deltas and rolling windows per sensor. Every frame ends at the current row (no look-ahead)."""
    w = Window.partitionBy("sensor_id").orderBy("ts_utc")
    last = lambda n: w.rowsBetween(-(n - 1), Window.currentRow)
    s = F.col("speed")
    congested = is_congested(s)
    return (df.withColumn("is_imputed", (F.col("quality") != QUALITY_OBSERVED).cast("int"))
              .withColumn("congested_now", congested)
              .withColumn("speed_lag_5m", F.lag(s, 1).over(w))
              .withColumn("speed_lag_10m", F.lag(s, 2).over(w))
              .withColumn("speed_lag_15m", F.lag(s, 3).over(w))
              .withColumn("speed_lag_30m", F.lag(s, 6).over(w))
              .withColumn("speed_lag_60m", F.lag(s, 12).over(w))
              .withColumn("speed_target_slot_1d", F.lag(s, slots_per_day - 1).over(w))
              .withColumn("speed_target_slot_1w", F.lag(s, 7 * slots_per_day - 1).over(w))
              .withColumn("delta_5m", s - F.col("speed_lag_5m"))
              .withColumn("delta_15m", s - F.col("speed_lag_15m"))
              .withColumn("roll_mean_15m", F.avg(s).over(last(3)))
              .withColumn("roll_mean_30m", F.avg(s).over(last(6)))
              .withColumn("roll_mean_60m", F.avg(s).over(last(12)))
              .withColumn("roll_std_60m", F.stddev_samp(s).over(last(12)))
              .withColumn("roll_min_60m", F.min(s).over(last(12)))
              .withColumn("congested_frac_60m", F.avg("congested_now").over(last(12)))
              .withColumn("imputed_count_60m", F.sum("is_imputed").over(last(12))))


def add_target(df: DataFrame, step_seconds: int, local_timezone: str) -> DataFrame:
    """label = congestion at t + 5 min, from the next OBSERVED reading only (null if it was not observed)."""
    w = Window.partitionBy("sensor_id").orderBy("ts_utc")
    observed_speed = F.when(F.col("quality") == QUALITY_OBSERVED, F.col("speed"))
    next_local = F.from_utc_timestamp(F.col("ts_utc") + F.expr(f"INTERVAL {step_seconds} SECONDS"), local_timezone)
    return (df.withColumn("speed_next", F.lead(observed_speed, 1).over(w))
              .withColumn(LABEL, is_congested(F.col("speed_next")))
              .withColumn("level_next", congestion_level(F.col("speed_next")))
              .withColumn("next_day_of_week", (((F.dayofweek(next_local) + 5) % 7) + 1).cast("int"))
              .withColumn("next_slot_of_day", (F.hour(next_local) * 12 + F.floor(F.minute(next_local) / 5)).cast("int")))


def neighbor_stats(S: np.ndarray, A: np.ndarray, threshold: float = CONGESTION_SPEED_MPH):
    """Adjacency-weighted upstream/downstream mean speed and congested share for each row of S (timestamps x sensors).

    S holds speeds with NaN for unobserved sensors; A has a zero diagonal. Returns (up, down, up_congested, down_congested),
    NaN where a sensor has no observed neighbour. Shared by the batch features and the live streaming features.
    """
    M = (~np.isnan(S)).astype(np.float64)
    Sf = np.nan_to_num(S)
    C = M * (Sf < threshold)
    with np.errstate(invalid="ignore", divide="ignore"):
        den_up, den_dn = M @ A, M @ A.T
        up = np.where(den_up > 0, (Sf @ A) / den_up, np.nan)
        dn = np.where(den_dn > 0, (Sf @ A.T) / den_dn, np.nan)
        up_c = np.where(den_up > 0, (C @ A) / den_up, np.nan)
        dn_c = np.where(den_dn > 0, (C @ A.T) / den_dn, np.nan)
    return up, dn, up_c, dn_c


def neighbor_features(spark: SparkSession, clean: DataFrame, sensor_ids: list[str], adj: np.ndarray,
                      threshold: float = CONGESTION_SPEED_MPH, batch_rows: int = 1000) -> DataFrame:
    """Adjacency-weighted upstream/downstream speed and congestion share at the same timestamp.

    DCRNN's adj_mx[i, j] is a Gaussian kernel of the road distance from sensor i to sensor j, so i is
    upstream of j. Each timestamp's 325 speeds are gathered into one row and multiplied by the (broadcast)
    adjacency matrix inside a vectorised pandas function - one matrix product per batch of timestamps
    instead of a ~250M-row edge join.
    """
    n = len(sensor_ids)
    A = adj.astype(np.float64).copy()
    np.fill_diagonal(A, 0.0)
    ids = np.array([int(s) for s in sensor_ids], dtype=np.int32)
    order = spark.createDataFrame([(int(s), i) for i, s in enumerate(sensor_ids)], "sensor_id int, column_index int")
    wide = (clean.select("sensor_id", "ts_utc", F.col("speed_mph").alias("speed"))
                 .join(F.broadcast(order), "sensor_id")
                 .groupBy("ts_utc")
                 .agg(F.sort_array(F.collect_list(F.struct("column_index", "speed"))).alias("v"))
                 .select("ts_utc", F.col("v.column_index").alias("idx"), F.col("v.speed").alias("spd")))

    def compute(batches):
        for pdf in batches:
            b = len(pdf)
            S = np.full((b, n), np.nan)
            for r, (ix, sp) in enumerate(zip(pdf["idx"], pdf["spd"])):
                S[r, np.asarray(ix, dtype=np.int64)] = np.asarray(sp, dtype=np.float64)
            up, dn, up_c, dn_c = neighbor_stats(S, A, threshold)
            yield pd.DataFrame({
                "ts_utc": np.repeat(pdf["ts_utc"].to_numpy(), n),
                "sensor_id": np.tile(ids, b),
                "nbr_up_speed": up.ravel().astype(np.float32),
                "nbr_down_speed": dn.ravel().astype(np.float32),
                "nbr_up_congested_frac": up_c.ravel().astype(np.float32),
                "nbr_down_congested_frac": dn_c.ravel().astype(np.float32),
            })

    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(batch_rows))
    schema = ("ts_utc timestamp, sensor_id int, nbr_up_speed float, nbr_down_speed float, "
              "nbr_up_congested_frac float, nbr_down_congested_frac float")
    return wide.mapInPandas(compute, schema)


def sensor_static(spark: SparkSession, train_clean: DataFrame, sensors_meta: DataFrame, sensor_ids: list[str],
                  adj: np.ndarray) -> DataFrame:
    A = adj.copy()
    np.fill_diagonal(A, 0.0)
    degrees = spark.createDataFrame(
        [(int(s), int((A[:, i] > 0).sum()), int((A[i, :] > 0).sum())) for i, s in enumerate(sensor_ids)],
        "sensor_id int, in_degree int, out_degree int")
    stats = (train_clean.groupBy("sensor_id")
                        .agg(F.expr("percentile_approx(speed_mph, 0.85, 10000)").alias("ff_speed"),
                             F.avg(is_congested(F.col("speed_mph"))).alias("sensor_congested_rate"),
                             F.count("*").alias("train_observed_readings")))
    return (sensors_meta.select("sensor_id", "latitude", "longitude")
                        .join(degrees, "sensor_id", "left").join(stats, "sensor_id", "left"))


def slot_profile(train_clean: DataFrame) -> DataFrame:
    """Train-period profile per (sensor, weekday, slot). Sums are kept so training rows can use a leave-one-out
    value that excludes their own target reading (see assemble)."""
    cong = is_congested(F.col("speed_mph"))
    return (add_temporal(train_clean, holidays=[])
            .groupBy("sensor_id", "day_of_week", "slot_of_day")
            .agg(F.avg("speed_mph").alias("hist_speed"),
                 F.avg(cong).alias("hist_congested_rate"),
                 F.sum(F.col("speed_mph").cast("double")).alias("hist_speed_sum"),
                 F.sum(cong).alias("hist_congested_sum"),
                 F.count("*").alias("hist_n")))


def assemble(clean: DataFrame, nbr: DataFrame, static: DataFrame, profile: DataFrame, b: SplitBoundaries,
             holidays: list[date], step_seconds: int, local_timezone: str, cfg: FeatureConfig) -> DataFrame:
    """All rows (before dropping incomplete ones) with features, label, split and drop reason."""
    df = clean.select("sensor_id", "ts_utc", "ts_local", "date_local", "quality", F.col("speed_mph").alias("speed"))
    df = add_temporal(df, holidays, cfg.slots_per_day)
    df = add_history(df, cfg.slots_per_day)
    df = add_target(df, step_seconds, local_timezone)
    df = df.join(nbr, ["sensor_id", "ts_utc"], "left")
    df = df.join(F.broadcast(static.drop("train_observed_readings")), "sensor_id", "left")
    prof = profile.select(F.col("sensor_id"), F.col("day_of_week").alias("next_day_of_week"),
                          F.col("slot_of_day").alias("next_slot_of_day"),
                          F.col("hist_speed").alias("_hs"), F.col("hist_congested_rate").alias("_hc"),
                          F.col("hist_speed_sum").alias("_hs_sum"), F.col("hist_congested_sum").alias("_hc_sum"),
                          F.col("hist_n").alias("_hn"))
    df = df.join(F.broadcast(prof), ["sensor_id", "next_day_of_week", "next_slot_of_day"], "left")
    # Leave-one-out for training rows: their target reading (t + 5 min) is itself part of the training profile, so
    # it is removed from the average. Validation/test targets are never in the profile and use it unchanged.
    target_in_profile = ((F.col("ts_utc") + F.expr(f"INTERVAL {step_seconds} SECONDS") < F.lit(b.val_start - b.purge))
                         & F.col("speed_next").isNotNull() & (F.col("_hn") > 1))
    df = (df.withColumn("hist_speed_next", F.when(target_in_profile, (F.col("_hs_sum") - F.col("speed_next")) / (F.col("_hn") - 1))
                                            .otherwise(F.col("_hs")))
            .withColumn("hist_congested_rate_next", F.when(target_in_profile, (F.col("_hc_sum") - F.col(LABEL)) / (F.col("_hn") - 1))
                                                     .otherwise(F.col("_hc")))
            .drop("_hs", "_hc", "_hs_sum", "_hc_sum", "_hn"))
    df = (df.withColumn("nbr_up_speed", F.coalesce("nbr_up_speed", "speed"))
            .withColumn("nbr_down_speed", F.coalesce("nbr_down_speed", "speed"))
            .withColumn("nbr_up_congested_frac", F.coalesce("nbr_up_congested_frac", F.col("congested_now").cast("float")))
            .withColumn("nbr_down_congested_frac", F.coalesce("nbr_down_congested_frac", F.col("congested_now").cast("float")))
            .withColumn("speed_ratio_ff", F.col("speed") / F.col("ff_speed"))
            .withColumn("split", split_column(F.col("ts_utc"), b)))

    feature_null = F.greatest(*[F.col(c).isNull().cast("int") for c in FEATURE_COLUMNS]) == 1
    reason = (F.when(in_purge_gap(F.col("ts_utc"), b), "purge_gap")
               .when(F.col("speed").isNull(), "current_speed_missing")
               .when(F.col("speed_target_slot_1w").isNull() & (F.col("ts_utc") < F.lit(b.start + timedelta(days=7, minutes=5))), "warmup_7d")
               .when(F.col(LABEL).isNull(), "target_not_observed")
               .when(feature_null, "feature_gap"))
    return df.withColumn("drop_reason", reason)


# ------------------------------------------------------------------------------------------- reporting
def threshold_sensitivity(train_clean: DataFrame, static: DataFrame) -> list[dict]:
    s = F.col("speed_mph")
    df = train_clean.join(F.broadcast(static.select("sensor_id", "ff_speed")), "sensor_id")
    absolute = [25, 30, 35, 40, 45, 50, 60]
    relative = [0.5, 0.6, 0.7]
    row = df.agg(*[F.avg((s < t).cast("double")).alias(f"abs_{t}") for t in absolute],
                 *[F.avg((s < F.col("ff_speed") * r).cast("double")).alias(f"rel_{int(r * 100)}") for r in relative],
                 *[F.avg(((s < 35) != (s < F.col("ff_speed") * r)).cast("double")).alias(f"dis_{int(r * 100)}") for r in relative]
                 ).first().asDict()
    out = [{"rule": f"speed < {t} mph", "positive_rate": row[f"abs_{t}"], "selected": t == CONGESTION_SPEED_MPH}
           for t in absolute]
    out += [{"rule": f"speed < {int(r * 100)}% of sensor free-flow", "positive_rate": row[f"rel_{int(r * 100)}"],
             "disagreement_with_35mph": row[f"dis_{int(r * 100)}"], "selected": False} for r in relative]
    return out


def congestion_episodes(train_clean: DataFrame, step_seconds: int) -> dict:
    """How much of the congested time belongs to sustained (>= 15 min) episodes, as in the Caltrans definition."""
    w = Window.partitionBy("sensor_id").orderBy("ts_utc")
    c = is_congested(F.col("speed_mph"))
    df = (train_clean.select("sensor_id", "ts_utc", c.alias("c"))
                     .withColumn("_new", F.when(F.col("c") != F.lag("c").over(w), 1).otherwise(0))
                     .withColumn("_ep", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0)))
                     .filter("c = 1").groupBy("sensor_id", "_ep").agg(F.count("*").alias("len")))
    min15 = 15 * 60 // step_seconds
    r = df.agg(F.count("*").alias("episodes"), F.sum("len").alias("readings"),
               F.sum(F.when(F.col("len") >= min15, F.col("len")).otherwise(0)).alias("in_sustained"),
               F.expr("percentile_approx(len, 0.5)").alias("median_len"), F.max("len").alias("max_len")).first()
    return {"episodes": r.episodes, "congested_readings": r.readings,
            "share_in_episodes_ge_15min": (r.in_sustained or 0) / r.readings if r.readings else 0.0,
            "median_episode_minutes": (r.median_len or 0) * step_seconds // 60,
            "longest_episode_hours": round((r.max_len or 0) * step_seconds / 3600, 1)}


def split_stats(features: DataFrame) -> dict:
    rows = (features.groupBy("split")
                    .agg(F.count("*").alias("rows"), F.sum(LABEL).alias("positives"),
                         F.date_format(F.min("ts_local"), "yyyy-MM-dd HH:mm").alias("first_local"),
                         F.date_format(F.max("ts_local"), "yyyy-MM-dd HH:mm").alias("last_local"),
                         F.min("ts_utc").alias("min_utc"), F.max("ts_utc").alias("max_utc"),
                         F.countDistinct("sensor_id").alias("sensors"),
                         # persistence baseline: predict label = congested_now
                         F.sum(((F.col("congested_now") == 1) & (F.col(LABEL) == 1)).cast("int")).alias("tp"),
                         F.sum(((F.col("congested_now") == 1) & (F.col(LABEL) == 0)).cast("int")).alias("fp"),
                         F.sum(((F.col("congested_now") == 0) & (F.col(LABEL) == 1)).cast("int")).alias("fn"))
                    .collect())
    out = {}
    for r in rows:
        d = r.asDict()
        p = d["tp"] / (d["tp"] + d["fp"]) if d["tp"] + d["fp"] else 0.0
        rc = d["tp"] / (d["tp"] + d["fn"]) if d["tp"] + d["fn"] else 0.0
        d.update(positive_rate=d["positives"] / d["rows"] if d["rows"] else 0.0,
                 persistence={"precision": p, "recall": rc, "f1": 2 * p * rc / (p + rc) if p + rc else 0.0},
                 min_utc=str(d["min_utc"]), max_utc=str(d["max_utc"]))
        out[d.pop("split")] = d
    return {s: out[s] for s in SPLITS if s in out}


def render_markdown(rep: dict) -> str:
    sp, b = rep["splits"], rep["boundaries"]
    L = [f"# Feature & Target Report — {rep['dataset'].upper()}", "",
         f"- Generated (UTC): {rep['generated_at_utc']} · runtime {rep['duration_seconds']} s · Spark {rep['spark_version']}",
         f"- Output: `{rep['features_path']}` (Parquet, partitioned by `split`) · {len(FEATURE_COLUMNS)} features",
         "", "## 1. Prediction target", "",
         f"**`label` = 1 if the observed speed of the sensor at t + {HORIZON_MINUTES} min is below "
         f"{CONGESTION_SPEED_MPH:.0f} mph, else 0.** Features use only information available at t.", "",
         "### Why 35 mph", ""]
    L += [f"- {s}" for s in SOURCES]
    L += [f"- Free-flow speed (train-period 85th percentile) ranges {rep['ff_speed']['min']:.1f}–{rep['ff_speed']['max']:.1f} mph "
          f"(median {rep['ff_speed']['median']:.1f}); all sensors are freeway mainline, so one absolute threshold is "
          f"meaningful and 35 mph ≈ {CONGESTION_SPEED_MPH / rep['ff_speed']['median']:.0%} of typical free-flow (flow breakdown).",
          f"- Duration: Caltrans counts congestion lasting ≥ 15 min. In the training period "
          f"**{rep['episodes']['share_in_episodes_ge_15min']:.1%}** of congested readings belong to episodes ≥ 15 min "
          f"(median episode {rep['episodes']['median_episode_minutes']} min, {rep['episodes']['episodes']:,} episodes), so the "
          f"5-minute label tracks sustained congestion rather than noise.", "",
          "### Sensitivity (training period, observed readings)", "",
          "| Rule | Positive rate | Disagreement with 35 mph |", "|---|---|---|"]
    for r in rep["threshold_sensitivity"]:
        dis = f"{r['disagreement_with_35mph']:.2%}" if "disagreement_with_35mph" in r else ""
        L.append(f"| {'**' if r['selected'] else ''}{r['rule']}{'** ✔' if r['selected'] else ''} | {r['positive_rate']:.2%} | {dis} |")
    L += ["", "Descriptive levels for the dashboard (`level_next`): "
          + "; ".join(f"**{k}** {v}" for k, v in LEVELS.items()) + ".", "",
          "## 2. Chronological split", "",
          f"Fractions {rep['split_config']['train_frac']:.0%} / {rep['split_config']['val_frac']:.0%} / "
          f"{1 - rep['split_config']['train_frac'] - rep['split_config']['val_frac']:.0%} of the time axis, boundaries snapped to "
          f"local midnight; the last {b['purge_minutes']} min before each boundary are purged.", "",
          "| Split | From (local) | To (local) | Rows | Positives | Positive rate | Persistence F1 |", "|---|---|---|---|---|---|---|"]
    for name, s in sp.items():
        L.append(f"| {name} | {s['first_local']} | {s['last_local']} | {s['rows']:,} | {s['positives']:,} | "
                 f"{s['positive_rate']:.2%} | {s['persistence']['f1']:.3f} |")
    L += ["", f"Boundaries (local): train starts {b['local']['start']}, val starts {b['local']['val_start']}, "
          f"test starts {b['local']['test_start']}, data ends {b['local']['end']} (exclusive).", "",
          "**Persistence baseline** = predict that the next slot has the same state as now. Any model must beat this "
          "F1 on validation/test to be useful.", "",
          "## 3. Rows not used for modelling", "", "| Reason | Rows |", "|---|---|"]
    L += [f"| `{k}` | {v:,} |" for k, v in rep["dropped_rows"].items()]
    L += [f"| **kept** | **{rep['kept_rows']:,}** of {rep['total_rows']:,} |", "",
          "## 4. Leakage controls", "",
          "| Control | How |", "|---|---|",
          "| No look-ahead in features | All lags/rolling windows use frames ending at the current row; the grid is complete in UTC, so row offsets are exact time offsets |",
          "| Target from real observations | `label` uses the next **observed** reading; forward-filled values are never targets |",
          "| Train-only statistics | `ff_speed`, `sensor_congested_rate`, `hist_*` profiles use training rows before the purge gap only |",
          "| Leave-one-out profiles | For training rows, `hist_speed_next` / `hist_congested_rate_next` exclude the row's own target reading "
          "((sum − own value) / (n − 1)); otherwise models learn to trust a profile that secretly contains the label |",
          "| Chronological split | Contiguous time blocks, no shuffling; the model never sees the future during training |",
          f"| Purge gap | Last {b['purge_minutes']} min before each boundary dropped, so no target crosses a split |",
          "| Same code for streaming | Feature functions are importable; `sensor_stats/` and `slot_profile/` are reused by the streaming job |",
          "", "`sensor_congested_rate` and `ff_speed` also include each training row once among ~35,000 readings per sensor; "
          "this in-sample effect is negligible, so no leave-one-out is applied to them.", "",
          "## 5. Feature dictionary", "", "| # | Feature | Group | Description |", "|---|---|---|---|"]
    L += [f"| {i} | `{n}` | {g} | {d} |" for i, (n, (g, d)) in enumerate(FEATURES.items(), 1)]
    L += ["", "Other columns: keys `sensor_id`, `ts_utc`, `ts_local`, `date_local`; target `label`; auxiliary targets "
          "`speed_next` (regression) and `level_next` (3-level); partition `split`.", ""]
    return "\n".join(L)


# ------------------------------------------------------------------------------------------- driver
def run(dataset: str, raw_dir: Path, processed_dir: Path, report_dir: Path, scfg: SplitConfig, fcfg: FeatureConfig,
        spark: SparkSession | None = None) -> dict:
    spec = SPECS[dataset]
    out_dir = processed_dataset_dir(dataset, processed_dir)
    clean_path, sensors_path = out_dir / "clean", out_dir / "sensors"
    for p in (clean_path, sensors_path):
        if not p.exists():
            raise SystemExit(f"{p} not found. Run build_processed (Phase 2) and clean_traffic first.")
    ds_raw = raw_dataset_dir(dataset, raw_dir)
    manifest = load_verified_manifest(ds_raw)
    sensor_ids, _, adj = safe_load_adjacency(ds_raw / manifest["adjacency_pkl"])

    own = spark is None
    spark = spark or get_spark(f"trafficsense-features-{dataset}", shuffle_partitions=48)
    t0 = time.time()
    staged_path: Path | None = None
    try:
        clean = spark.read.parquet(str(clean_path))
        sensors_meta = spark.read.parquet(str(sensors_path))
        rng = clean.agg(F.min("ts_utc").alias("lo"), F.max("ts_utc").alias("hi"),
                        F.min("date_local").alias("d0"), F.max("date_local").alias("d1")).first()
        b = compute_boundaries(rng.lo, rng.hi, spec.interval_seconds, spec.local_timezone, scfg)
        holidays = us_federal_holidays(range(rng.d0.year, rng.d1.year + 1))
        log.info("split boundaries (local): %s", b.to_dict()["local"])

        train_clean = clean.filter(train_mask(F.col("ts_utc"), b) & (F.col("quality") == QUALITY_OBSERVED))
        static = sensor_static(spark, train_clean, sensors_meta, sensor_ids, adj)
        static.coalesce(1).write.mode("overwrite").parquet(str(out_dir / "sensor_stats"))
        static = spark.read.parquet(str(out_dir / "sensor_stats"))
        profile = slot_profile(train_clean)
        profile.coalesce(4).write.mode("overwrite").parquet(str(out_dir / "slot_profile"))
        profile = spark.read.parquet(str(out_dir / "slot_profile"))
        log.info("train-only sensor_stats (%d sensors) and slot_profile (%d rows) written", static.count(), profile.count())

        nbr = neighbor_features(spark, clean, sensor_ids, adj, CONGESTION_SPEED_MPH, fcfg.neighbor_batch_rows)
        full = assemble(clean, nbr, static, profile, b, holidays, spec.interval_seconds, spec.local_timezone, fcfg)

        # Stage everything once (with drop reasons) on the Linux disk scratch area, then derive model-ready rows.
        (SCRATCH_DIR / "staging").mkdir(parents=True, exist_ok=True)
        staged_path = Path(tempfile.mkdtemp(prefix=f"{dataset_slug(dataset)}_features_", dir=SCRATCH_DIR / "staging"))
        cols = KEY_COLUMNS + FEATURE_COLUMNS + [LABEL] + AUX_TARGETS + ["split", "drop_reason"]
        casts = {c: F.col(c).cast("float") for c in FEATURE_COLUMNS + ["speed_next"]}
        (full.select(*[casts.get(c, F.col(c)) for c in cols])
             .write.mode("overwrite").partitionBy("split").parquet(str(staged_path)))
        staged = spark.read.parquet(str(staged_path))
        dropped = {r.drop_reason: r["count"] for r in staged.filter(F.col("drop_reason").isNotNull())
                   .groupBy("drop_reason").count().orderBy(F.desc("count")).collect()}
        total = staged.count()

        features_path = out_dir / "features"
        log.info("Writing %s", features_path)
        (staged.filter(F.col("drop_reason").isNull()).drop("drop_reason")
               .withColumn(LABEL, F.col(LABEL).cast("int"))
               .repartition("split").sortWithinPartitions("ts_utc", "sensor_id")
               .write.mode("overwrite").partitionBy("split").parquet(str(features_path)))
        features = spark.read.parquet(str(features_path))
        nulls = features.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in FEATURE_COLUMNS + [LABEL]]).first().asDict()
        stats = split_stats(features)
        kept = sum(s["rows"] for s in stats.values())

        ff = static.agg(F.min("ff_speed").alias("min"), F.expr("percentile_approx(ff_speed, 0.5)").alias("median"),
                        F.max("ff_speed").alias("max")).first().asDict()
        rep = {
            "dataset": dataset, "spark_version": spark.version,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "features_path": str(features_path), "feature_columns": FEATURE_COLUMNS, "label": LABEL,
            "label_definition": f"observed speed at t+{HORIZON_MINUTES}min < {CONGESTION_SPEED_MPH} mph",
            "congestion_threshold_mph": CONGESTION_SPEED_MPH, "free_flow_threshold_mph": FREE_FLOW_SPEED_MPH,
            "horizon_minutes": HORIZON_MINUTES, "boundaries": b.to_dict(), "split_config": asdict(scfg),
            "feature_config": asdict(fcfg), "holidays": [str(h) for h in holidays],
            "splits": stats, "total_rows": total, "kept_rows": kept, "dropped_rows": dropped,
            "null_counts_in_output": {k: v for k, v in nulls.items() if v},
            "ff_speed": ff,
            "threshold_sensitivity": threshold_sensitivity(train_clean, static),
            "episodes": congestion_episodes(train_clean, spec.interval_seconds),
        }
        rep["duration_seconds"] = round(time.time() - t0, 1)
    finally:
        if staged_path is not None:
            shutil.rmtree(staged_path, ignore_errors=True)
        if own:
            spark.stop()

    meta = {k: rep[k] for k in ("dataset", "feature_columns", "label", "label_definition", "congestion_threshold_mph",
                                "horizon_minutes", "boundaries", "split_config", "generated_at_utc")}
    meta.update(features_path=str(features_path), aux_targets=AUX_TARGETS, key_columns=KEY_COLUMNS,
                feature_groups={n: g for n, (g, _) in FEATURES.items()},
                class_balance={s: v["positive_rate"] for s, v in rep["splits"].items()})
    (out_dir / "features_meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    report_dir.mkdir(parents=True, exist_ok=True)
    stem = report_dir / f"{dataset_slug(dataset)}_feature_report"
    stem.with_suffix(".json").write_text(json.dumps(rep, indent=2, default=str) + "\n")
    stem.with_suffix(".md").write_text(render_markdown(rep))
    for name, s in rep["splits"].items():
        log.info("%-5s %s -> %s  rows %s  positive %.2f%%  persistence F1 %.3f", name, s["first_local"], s["last_local"],
                 f"{s['rows']:,}", 100 * s["positive_rate"], s["persistence"]["f1"])
    log.info("dropped %s | kept %s of %s | report %s.{json,md}", dropped, f"{kept:,}", f"{total:,}", stem)
    if rep["null_counts_in_output"]:
        raise SystemExit(f"Nulls in model-ready features: {rep['null_counts_in_output']}")
    return rep


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    d = SplitConfig()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dataset", default="pems-bay", choices=sorted(SPECS))
    p.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    p.add_argument("--processed-dir", type=Path, default=PROCESSED_DIR)
    p.add_argument("--report-dir", type=Path, default=PREPROCESSING_REPORT_DIR)
    p.add_argument("--train-frac", type=float, default=d.train_frac)
    p.add_argument("--val-frac", type=float, default=d.val_frac)
    p.add_argument("--purge-minutes", type=int, default=d.purge_minutes)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    a = parse_args(argv)
    run(a.dataset, a.raw_dir, a.processed_dir, a.report_dir,
        SplitConfig(a.train_frac, a.val_frac, a.purge_minutes), FeatureConfig())
    return 0


if __name__ == "__main__":
    sys.exit(main())
