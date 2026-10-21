"""Phase 3 tests: cleaning rules, feature correctness, no look-ahead, chronological splits, train-only statistics.

Run:  pytest -q tests/test_features.py
"""
from __future__ import annotations

import dataclasses
import json
import pickle
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from src.common.spark import get_spark
from src.preprocessing import build_features as bf
from src.preprocessing.clean_traffic import (
    ISSUE_ABSENT, ISSUE_NON_POSITIVE, ISSUE_OUT_OF_RANGE, ISSUE_STUCK, CleaningConfig, clean,
)
from src.preprocessing.download_dataset import MANIFEST_NAME, file_hashes
from src.preprocessing.sources import SPECS
from src.preprocessing.splits import SplitConfig, compute_boundaries, in_purge_gap

SPEC = SPECS["pems-bay"]
LONG_SCHEMA = "ts_local_raw timestamp, sensor_id int, speed_raw double"


@pytest.fixture(scope="module")
def spark():
    s = get_spark("trafficsense-tests-features", driver_memory="1g", shuffle_partitions=4)
    yield s
    s.stop()


def _slots(start: datetime, n: int, skip: set | None = None):
    out, t = [], start
    while len(out) < n:
        if not skip or t not in skip:
            out.append(t)
        t += timedelta(minutes=5)
    return out


# ------------------------------------------------------------------------------------------ cleaning
def test_clean_handles_dst_duplicates_offgrid_invalid_stuck_and_ffill(spark):
    start = datetime(2017, 3, 12, 1, 0)
    dst_hole = {datetime(2017, 3, 12, 2, 0) + timedelta(minutes=5 * i) for i in range(12)}
    local = _slots(start, 30, skip=dst_hole)          # 01:00..01:55, 03:00..04:25 local = 30 contiguous UTC slots
    rng = np.random.default_rng(0)
    rows = []
    for i, t in enumerate(local):
        rows.append((t, 1, float(60 + rng.normal())))
        if i == 24:                                    # sensor 2 lacks one slot entirely
            continue
        v = float(55 + rng.normal())
        if i == 3:
            v = 0.0                                    # non-positive
        if i == 4:
            v = 150.0                                  # out of range
        if 5 <= i <= 9:
            v = None if i != 5 else 0.0                # 0.0 then 4 nulls -> 7-slot invalid stretch (3..9)
        if 10 <= i <= 21:
            v = 20.0                                   # 12 identical congested readings (1 h) -> stuck
        if 22 <= i <= 29:
            v = 30.0 if i <= 28 else 31.0              # short congested plateau (broken by the absent slot) -> kept
        rows.append((t, 2, v))
    rows.append((local[1], 2, 0.0))                    # duplicate of a slot: invalid copy must lose
    rows.append((local[2] + timedelta(minutes=2), 3, 64.0))   # off-grid, only reading of sensor 3

    long = spark.createDataFrame(rows, LONG_SCHEMA)
    out, stats = clean(long, spark, SPEC, CleaningConfig(), ["1", "2", "3"])
    pdf = out.toPandas().sort_values(["sensor_id", "ts_utc"]).reset_index(drop=True)

    assert stats["grid_slots"] == 30                   # DST hole does not create slots in UTC
    assert stats["duplicate_rows"] == 1 and stats["off_grid_timestamps"] == 1
    assert len(pdf) == 3 * 30 and pdf.groupby("sensor_id").ts_utc.nunique().eq(30).all()
    assert (pdf.ts_utc.diff().dropna()[pdf.sensor_id.diff().dropna() == 0] == timedelta(minutes=5)).all()

    s1 = pdf[pdf.sensor_id == 1]
    assert s1.issue.isna().all() and (s1.quality == "observed").all()

    s2 = pdf[pdf.sensor_id == 2].reset_index(drop=True)
    assert s2.loc[1, "quality"] == "observed"          # duplicate resolved to the valid copy
    assert s2.loc[3, "issue"] == ISSUE_NON_POSITIVE and s2.loc[4, "issue"] == ISSUE_OUT_OF_RANGE
    # slots 3..9 invalid: 3,4,5 forward-filled from slot 2 (limit 3), 6..9 missing
    assert list(s2.loc[3:5, "quality"]) == ["imputed"] * 3
    assert s2.loc[3, "speed_mph"] == pytest.approx(s2.loc[2, "speed_mph"])
    assert list(s2.loc[6:9, "quality"]) == ["missing"] * 4
    stuck_rows = s2.loc[10:21]
    assert (stuck_rows.issue == ISSUE_STUCK).all() and (stuck_rows.quality == "missing").all()
    assert s2.loc[24, "issue"] == ISSUE_ABSENT and s2.loc[24, "quality"] == "imputed"
    assert s2.loc[22:29].drop(index=24).issue.isna().all()   # short congested plateau is kept

    s3 = pdf[pdf.sensor_id == 3].reset_index(drop=True)
    assert s3.loc[2, "speed_raw"] == 64.0 and s3.loc[2, "quality"] == "observed"
    assert (s3.drop(index=2).issue == ISSUE_ABSENT).all()


# ------------------------------------------------------------------------------------------ features
def _clean_frame(spark, speeds: dict[int, list], start=datetime(2017, 1, 2, 8, 0)):
    rows = []
    for sid, values in speeds.items():
        for i, v in enumerate(values):
            utc = start + timedelta(minutes=5 * i)
            loc = utc - timedelta(hours=8)
            q = "observed" if v is not None else "missing"
            if isinstance(v, tuple):
                v, q = v
            rows.append(Row(sensor_id=sid, ts_utc=utc, ts_local=loc, date_local=loc.date(), quality=q,
                            speed_mph=None if v is None else float(v)))
    return spark.createDataFrame(rows)


def test_lags_rolling_and_target_are_correct(spark):
    vals = [60, 50, 40, 30, 20, (25, "imputed"), 50, 60]
    df = _clean_frame(spark, {7: vals}).withColumnRenamed("speed_mph", "speed")
    out = bf.add_target(bf.add_history(df), 300, "America/Los_Angeles").orderBy("ts_utc").toPandas()
    assert list(out.speed_lag_5m[1:4]) == [60, 50, 40]
    assert out.speed_lag_15m[3] == 60 and np.isnan(out.speed_lag_15m[2])
    assert out.roll_mean_15m[2] == pytest.approx(50)              # (60+50+40)/3
    assert out.roll_min_60m[4] == 20 and out.delta_5m[1] == -10
    assert list(out.congested_now) == [0, 0, 0, 1, 1, 1, 0, 0]
    assert out.congested_frac_60m[4] == pytest.approx(2 / 5)
    assert out.label[2] == 1 and out.speed_next[2] == 30          # next slot 30 mph -> congested
    assert np.isnan(out.label[4])                                 # next value was imputed -> no target
    assert out.label[0] == 0 and out.level_next[0] == "slowed"    # 50 mph -> slowed
    assert np.isnan(out.label[7])                                 # no future reading


def test_features_never_look_ahead(spark):
    rng = np.random.default_rng(1)
    base = list(np.round(rng.uniform(20, 70, 60), 1))
    future_changed = base[:30] + list(np.round(rng.uniform(20, 70, 30), 1))
    cols = [c for c in bf.FEATURES if c in {"speed", "congested_now", "is_imputed"} or c.startswith(("speed_lag", "delta", "roll_", "congested_frac", "imputed_count"))]

    def feats(values):
        df = _clean_frame(spark, {1: values}).withColumnRenamed("speed_mph", "speed")
        return bf.add_history(df).orderBy("ts_utc").toPandas()[cols].iloc[:30]

    a, b = feats(base), feats(future_changed)
    assert a.equals(b), "features at t changed when only values after t changed"


def test_neighbor_features_weighted_mean(spark):
    # sensor order [10, 11, 12]; 10 -> 11 (w=1.0), 12 -> 11 (w=0.5), 11 -> 12 (w=0.8)
    A = np.zeros((3, 3), dtype=np.float32)
    A[0, 1], A[2, 1], A[1, 2] = 1.0, 0.5, 0.8
    np.fill_diagonal(A, 1.0)                                      # self loops must be ignored
    clean_df = _clean_frame(spark, {10: [60, None], 11: [30, 40], 12: [20, 50]})
    out = {(r.sensor_id, r.ts_utc.minute): r for r in bf.neighbor_features(spark, clean_df, ["10", "11", "12"], A).collect()}
    t0, t1 = 0, 5
    assert out[(11, t0)].nbr_up_speed == pytest.approx((60 * 1.0 + 20 * 0.5) / 1.5)
    assert out[(11, t0)].nbr_up_congested_frac == pytest.approx(0.5 / 1.5)
    assert out[(11, t1)].nbr_up_speed == pytest.approx(50)        # sensor 10 missing -> renormalised
    assert out[(10, t0)].nbr_down_speed == pytest.approx(30)
    assert out[(12, t0)].nbr_up_speed == pytest.approx(30)
    assert out[(10, t0)].nbr_up_speed is None or np.isnan(out[(10, t0)].nbr_up_speed)   # no upstream


def test_us_federal_holidays_2017():
    h = set(bf.us_federal_holidays([2017]))
    for d in ["2017-01-02", "2017-01-16", "2017-02-20", "2017-05-29", "2017-07-04", "2017-09-04",
              "2017-10-09", "2017-11-10", "2017-11-23", "2017-12-25"]:
        assert date.fromisoformat(d) in h
    assert len(h) == 10


# ------------------------------------------------------------------------------------------ splits
def test_split_boundaries_pems_bay_range():
    b = compute_boundaries(datetime(2017, 1, 1, 8), datetime(2017, 7, 1, 6, 55), 300, "America/Los_Angeles", SplitConfig())
    d = b.to_dict()
    assert d["local"]["val_start"].endswith("00:00") and d["local"]["test_start"].endswith("00:00")
    assert d["local"]["start"] == "2017-01-01 00:00" and d["local"]["end"] == "2017-07-01 00:00"
    total = (b.end - b.start).total_seconds()
    assert abs((b.val_start - b.start).total_seconds() / total - 0.7) < 0.01
    assert abs((b.test_start - b.start).total_seconds() / total - 0.8) < 0.01


def test_purge_gap_marks_rows_before_boundaries(spark):
    b = compute_boundaries(datetime(2017, 1, 1, 8), datetime(2017, 1, 31, 7, 55), 300, "America/Los_Angeles",
                           SplitConfig(purge_minutes=10))
    ts = [b.val_start - timedelta(minutes=15), b.val_start - timedelta(minutes=10), b.val_start - timedelta(minutes=5),
          b.val_start, b.test_start - timedelta(minutes=5)]
    df = spark.createDataFrame([(t,) for t in ts], "ts_utc timestamp")
    assert [r[0] for r in df.select(in_purge_gap(F.col("ts_utc"), b)).collect()] == [False, True, True, False, True]


# ------------------------------------------------------------------------------------------ end-to-end
def _synthetic_raw_and_clean(spark, root, test_speed_offset: float = 0.0):
    """3 sensors x 28 days of clean data + adjacency/manifest, mimicking the real layout."""
    raw = root / "raw" / "pems_bay"
    proc = root / "processed" / "pems_bay"
    raw.mkdir(parents=True, exist_ok=True)
    sensor_ids = ["101", "102", "103"]
    A = np.array([[1, 0.9, 0], [0, 1, 0.7], [0, 0, 1]], dtype=np.float32)
    adj_path = raw / "adj.pkl"
    if not adj_path.exists():
        adj_path.write_bytes(pickle.dumps((sensor_ids, {s: i for i, s in enumerate(sensor_ids)}, A), protocol=2))
        meta = {"role": "adjacency", "size": adj_path.stat().st_size, **file_hashes(adj_path)}
        (raw / MANIFEST_NAME).write_text(json.dumps({"files": {"adj.pkl": meta}, "adjacency_pkl": "adj.pkl"}))

    start, n = datetime(2017, 1, 1, 8, 0), 28 * 288
    rng = np.random.default_rng(42)
    test_from = datetime(2017, 1, 24, 8, 0)
    rows = []
    for k, sid in enumerate(sensor_ids):
        for i in range(n):
            utc = start + timedelta(minutes=5 * i)
            loc = utc - timedelta(hours=8)
            peak = 40 * np.exp(-((loc.hour + loc.minute / 60 - 8) ** 2) / 1.5) if loc.weekday() < 5 else 0
            v = 66 - peak * (0.6 + 0.2 * k) + rng.normal(0, 1.5)
            if utc >= test_from:
                v += test_speed_offset
            rows.append(Row(sensor_id=int(sid), ts_utc=utc, ts_local=loc, speed_raw=float(v), speed_mph=float(v),
                            quality="observed", issue=None, date_local=loc.date(), month=loc.strftime("%Y-%m")))
    schema = ("sensor_id int, ts_utc timestamp, ts_local timestamp, speed_raw float, speed_mph float, quality string, "
              "issue string, date_local date, month string")
    spark.createDataFrame(rows, schema).write.mode("overwrite").partitionBy("month").parquet(str(proc / "clean"))
    spark.createDataFrame([(101, 37.30, -121.90), (102, 37.31, -121.91), (103, 37.32, -121.92)],
                          "sensor_id int, latitude double, longitude double").write.mode("overwrite").parquet(str(proc / "sensors"))
    return root / "raw", root / "processed"


def test_build_features_end_to_end_and_train_only_statistics(spark, tmp_path):
    raw_dir, proc_dir = _synthetic_raw_and_clean(spark, tmp_path)
    rep = bf.run("pems-bay", raw_dir, proc_dir, tmp_path / "reports", SplitConfig(purge_minutes=60), bf.FeatureConfig(), spark=spark)
    feats = spark.read.parquet(rep["features_path"])
    pdf = feats.toPandas()

    assert set(pdf.split) == {"train", "val", "test"}
    assert pdf[bf.FEATURE_COLUMNS + ["label"]].notna().all().all()
    assert set(pdf.label.unique()) <= {0, 1} and pdf.label.sum() > 0
    order = pdf.groupby("split").ts_utc.agg(["min", "max"])
    assert order.loc["train", "max"] < order.loc["val", "min"] and order.loc["val", "max"] < order.loc["test", "min"]
    b = rep["boundaries"]["utc"]
    val_start = datetime.fromisoformat(b["val_start"])
    assert order.loc["train", "max"] < val_start - timedelta(minutes=60)          # purge gap respected
    assert pdf.ts_utc.min() >= datetime(2017, 1, 8, 8, 0) - timedelta(minutes=5)  # 7-day warm-up dropped
    assert rep["dropped_rows"].get("warmup_7d", 0) > 0 and rep["dropped_rows"].get("purge_gap", 0) > 0
    meta = json.loads((proc_dir / "pems_bay" / "features_meta.json").read_text())
    assert meta["feature_columns"] == bf.FEATURE_COLUMNS

    stats1 = spark.read.parquet(str(proc_dir / "pems_bay" / "sensor_stats")).orderBy("sensor_id").toPandas()
    prof1 = spark.read.parquet(str(proc_dir / "pems_bay" / "slot_profile")).orderBy("sensor_id", "day_of_week", "slot_of_day").toPandas()
    train1 = pdf[pdf.split == "train"].sort_values(["sensor_id", "ts_utc"]).reset_index(drop=True)

    # Leave-one-out profile: a training row's hist_* excludes its own target; validation rows use the profile as is.
    prof = prof1.set_index(["sensor_id", "day_of_week", "slot_of_day"])
    def profile_rows(rows):
        nxt = pd.to_datetime(rows.ts_local) + pd.Timedelta(minutes=5)
        return prof.loc[list(zip(rows.sensor_id, nxt.dt.dayofweek + 1, nxt.dt.hour * 12 + nxt.dt.minute // 5))]
    early = train1[train1.ts_utc < val_start - timedelta(hours=2)].head(3000)
    p = profile_rows(early)
    np.testing.assert_allclose(early.hist_congested_rate_next.to_numpy(),
                               (p.hist_congested_sum.to_numpy() - early.label.to_numpy()) / (p.hist_n.to_numpy() - 1), rtol=1e-5)
    np.testing.assert_allclose(early.hist_speed_next.to_numpy(),
                               (p.hist_speed_sum.to_numpy() - early.speed_next.to_numpy()) / (p.hist_n.to_numpy() - 1), rtol=1e-4)
    vrows = pdf[pdf.split == "val"].head(1000)
    np.testing.assert_allclose(vrows.hist_congested_rate_next.to_numpy(), profile_rows(vrows).hist_congested_rate.to_numpy(), rtol=1e-5)

    # Change only the test period drastically: nothing learned from training may change.
    raw_dir, proc_dir = _synthetic_raw_and_clean(spark, tmp_path, test_speed_offset=-30.0)
    bf.run("pems-bay", raw_dir, proc_dir, tmp_path / "reports", SplitConfig(purge_minutes=60), bf.FeatureConfig(), spark=spark)
    stats2 = spark.read.parquet(str(proc_dir / "pems_bay" / "sensor_stats")).orderBy("sensor_id").toPandas()
    prof2 = spark.read.parquet(str(proc_dir / "pems_bay" / "slot_profile")).orderBy("sensor_id", "day_of_week", "slot_of_day").toPandas()
    feats2 = spark.read.parquet(str(proc_dir / "pems_bay" / "features")).toPandas()
    train2 = feats2[feats2.split == "train"].sort_values(["sensor_id", "ts_utc"]).reset_index(drop=True)

    assert stats1.equals(stats2), "sensor statistics depend on non-training data"
    assert prof1.equals(prof2), "slot profile depends on non-training data"
    assert train1[bf.FEATURE_COLUMNS + ["label"]].equals(train2[bf.FEATURE_COLUMNS + ["label"]]), "training rows changed"
    assert feats2[feats2.split == "test"].label.mean() > pdf[pdf.split == "test"].label.mean()
