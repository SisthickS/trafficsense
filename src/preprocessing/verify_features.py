"""Independent verification of Phase 3 outputs (reads only). Exit code 1 if any check fails.

Usage (project root, env sourced):
    python -m src.preprocessing.verify_features
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, RAW_DIR, processed_dataset_dir, raw_dataset_dir
from src.common.spark import get_spark
from src.preprocessing.build_features import FEATURE_COLUMNS, LABEL
from src.preprocessing.download_dataset import load_verified_manifest
from src.preprocessing.sources import SPECS


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dataset", default="pems-bay", choices=sorted(SPECS))
    a = p.parse_args(argv)
    spec = SPECS[a.dataset]
    base = processed_dataset_dir(a.dataset, PROCESSED_DIR)
    results: list[tuple[str, bool, str]] = []
    check = lambda name, ok, info="": results.append((name, bool(ok), info))

    ds_raw = raw_dataset_dir(a.dataset, RAW_DIR)
    try:
        manifest = load_verified_manifest(ds_raw)
        check("raw files match MANIFEST sha256", True)
        writable = [n for n in manifest["files"] if os.access(ds_raw / n, os.W_OK)]
        check("raw files read-only", not writable, ", ".join(writable))
    except SystemExit as exc:
        check("raw files match MANIFEST sha256", False, str(exc))

    spark = get_spark("trafficsense-verify-features")
    try:
        clean = spark.read.parquet(str(base / "clean"))
        c = clean.agg(F.count("*").alias("rows"), F.countDistinct("sensor_id").alias("sensors"),
                      F.countDistinct("ts_utc").alias("ts"), F.countDistinct("sensor_id", "ts_utc").alias("keys"),
                      F.min("ts_utc").alias("lo"), F.max("ts_utc").alias("hi"),
                      F.sum((F.col("quality") == "observed").cast("int")).alias("observed")).first()
        slots = int((c.hi - c.lo).total_seconds() // spec.interval_seconds) + 1
        check("clean: complete sensor x UTC grid", c.rows == c.keys == c.sensors * slots == c.sensors * c.ts,
              f"{c.rows:,} rows = {c.sensors} sensors x {slots:,} slots")
        check("clean: observed share >= 99%", c.observed / c.rows >= 0.99, f"{c.observed / c.rows:.4%}")
        bad_obs = clean.filter((F.col("quality") == "observed") & (F.col("speed_mph") != F.col("speed_raw"))).count()
        check("clean: observed speeds equal raw values", bad_obs == 0, f"{bad_obs} mismatches")

        meta = json.loads((base / "features_meta.json").read_text())
        check("features_meta lists the current feature set", meta["feature_columns"] == FEATURE_COLUMNS,
              f"{len(meta['feature_columns'])} features")
        feats = spark.read.parquet(str(base / "features"))
        nulls = feats.agg(*[F.sum(F.col(x).isNull().cast("int")).alias(x) for x in FEATURE_COLUMNS + [LABEL]]).first().asDict()
        check("features: no nulls in features/label", not any(nulls.values()), str({k: v for k, v in nulls.items() if v}))
        labels = {r[0] for r in feats.select(LABEL).distinct().collect()}
        check("features: label is binary {0,1}", labels == {0, 1}, str(sorted(labels)))
        dup = feats.groupBy("sensor_id", "ts_utc").count().filter("count > 1").count()
        check("features: one row per (sensor, ts)", dup == 0, f"{dup} duplicates")

        s = {r.split: r for r in feats.groupBy("split").agg(
            F.min("ts_utc").alias("lo"), F.max("ts_utc").alias("hi"), F.count("*").alias("n"),
            F.avg(LABEL).alias("pos")).collect()}
        b = meta["boundaries"]["utc"]
        purge = timedelta(minutes=meta["boundaries"]["purge_minutes"])
        vs, ts_ = datetime.fromisoformat(b["val_start"]), datetime.fromisoformat(b["test_start"])
        check("splits: train < val < test (no overlap)", s["train"].hi < s["val"].lo and s["val"].hi < s["test"].lo,
              f"train..{s['train'].hi} | val {s['val'].lo}..{s['val'].hi} | test {s['test'].lo}..")
        check("splits: purge gap before each boundary", s["train"].hi < vs - purge + timedelta(seconds=1)
              and s["val"].hi < ts_ - purge + timedelta(seconds=1), f"purge {purge}")
        check("splits: rows match boundaries", s["val"].lo >= vs and s["test"].lo >= ts_)
        check("splits: every split has both classes", all(0 < r.pos < 1 for r in s.values()),
              ", ".join(f"{k} {v.pos:.2%}" for k, v in s.items()))
        # label must equal (next observed clean speed < threshold) - recompute independently on a sample of sensors
        sample = [r.sensor_id for r in feats.select("sensor_id").distinct().orderBy("sensor_id").limit(5).collect()]
        nxt = (clean.filter(F.col("sensor_id").isin(sample) & (F.col("quality") == "observed"))
                    .select("sensor_id", (F.col("ts_utc") - F.expr(f"INTERVAL {spec.interval_seconds} SECONDS")).alias("ts_utc"),
                            (F.col("speed_mph") < meta["congestion_threshold_mph"]).cast("int").alias("expected")))
        mism = feats.filter(F.col("sensor_id").isin(sample)).join(nxt, ["sensor_id", "ts_utc"], "left") \
                    .filter(F.col("expected").isNull() | (F.col("expected") != F.col(LABEL))).count()
        check("label = next observed speed < threshold (5 sensors recomputed)", mism == 0, f"{mism} mismatches")
        lag_bad = (feats.filter(F.col("sensor_id").isin(sample)).alias("f")
                   .join(clean.select("sensor_id", (F.col("ts_utc") + F.expr("INTERVAL 5 MINUTES")).alias("ts_utc"),
                                      F.col("speed_mph").alias("prev")), ["sensor_id", "ts_utc"])
                   .filter(F.abs(F.col("speed_lag_5m") - F.col("prev")) > 1e-4).count())
        check("speed_lag_5m = clean speed 5 min earlier (5 sensors)", lag_bad == 0, f"{lag_bad} mismatches")
        prof = spark.read.parquet(str(base / "slot_profile"))
        purge_end = F.lit(vs - purge)
        nxt = F.from_utc_timestamp(F.col("ts_utc") + F.expr(f"INTERVAL {spec.interval_seconds} SECONDS"), spec.local_timezone)
        loo = (feats.filter((F.col("split") == "train") & F.col("sensor_id").isin(sample))
                    .withColumn("day_of_week", (((F.dayofweek(nxt) + 5) % 7) + 1).cast("int"))
                    .withColumn("slot_of_day", (F.hour(nxt) * 12 + F.floor(F.minute(nxt) / 5)).cast("int"))
                    .join(prof, ["sensor_id", "day_of_week", "slot_of_day"])
                    .withColumn("in_profile", F.col("ts_utc") + F.expr(f"INTERVAL {spec.interval_seconds} SECONDS") < purge_end)
                    .withColumn("expected", F.when(F.col("in_profile"), (F.col("hist_congested_sum") - F.col(LABEL)) / (F.col("hist_n") - 1))
                                             .otherwise(F.col("hist_congested_rate"))))
        loo_bad = loo.filter(F.abs(F.col("hist_congested_rate_next") - F.col("expected")) > 1e-5).count()
        check("training hist_* profile is leave-one-out (5 sensors)", loo_bad == 0, f"{loo_bad} mismatches")
        for name in ("sensor_stats", "slot_profile"):
            check(f"{name}/ present", (base / name).exists())
    finally:
        spark.stop()

    width = max(len(n) for n, _, _ in results)
    for name, ok, info in results:
        print(f"  [{'OK' if ok else 'FAIL':4}] {name:<{width}}  {info}")
    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
