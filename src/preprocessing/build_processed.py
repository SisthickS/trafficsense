"""Build cleaned, Spark-friendly Parquet from the validated raw dataset.

    data/raw/<dataset>/            (immutable input, never modified)
      -> data/processed/<dataset>/readings/   long format, partitioned by month
      -> data/processed/<dataset>/sensors/    one row per sensor (location + quality stats)
      -> data/processed/<dataset>/_build_info.json

Cleaning in this phase is deliberately minimal and lossless:
  * wide -> long unpivot (one row per sensor reading)
  * timestamp string -> timestamp; sensor id -> int
  * missing readings (null / NaN / <= 0 mph) -> speed_mph = null, is_missing = true
No imputation, smoothing or feature engineering here (that is Phase 5).

Usage (project root, env sourced):
    python -m src.preprocessing.build_processed
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, RAW_DIR, VALIDATION_REPORT_DIR, dataset_slug, processed_dataset_dir, raw_dataset_dir
from src.common.spark import get_spark
from src.preprocessing.download_dataset import load_verified_manifest
from src.preprocessing.raw_reader import CORRUPT_COL, load_locations, load_wide, read_header, to_long
from src.preprocessing.sources import SPECS

log = logging.getLogger("build_processed")


def require_validation(report_path: Path, manifest: dict) -> dict:
    """Refuse to build unless validation ran on exactly these raw files and did not FAIL."""
    if not report_path.exists():
        raise SystemExit(f"Validation report {report_path} not found. Run validate_dataset first.")
    report = json.loads(report_path.read_text())
    if report.get("overall_status") == "FAIL":
        raise SystemExit(f"Validation status is FAIL ({report_path}); fix the raw data before building.")
    c02 = next((c for c in report["checks"] if c["id"] == "C02"), None)
    validated = {f["file"]: f["sha256"] for f in (c02 or {}).get("details", {}).get("files", [])}
    current = {name: meta["sha256"] for name, meta in manifest["files"].items()}
    if validated != current:
        raise SystemExit("Raw files changed since validation (checksums differ). Re-run validate_dataset.")
    return report


def build(dataset: str, raw_dir: Path, processed_dir: Path, report_dir: Path, skip_validation: bool = False,
          spark=None) -> dict:
    spec = SPECS[dataset]
    ds_raw = raw_dataset_dir(dataset, raw_dir)
    out_dir = processed_dataset_dir(dataset, processed_dir)
    manifest = load_verified_manifest(ds_raw)
    csv_path = ds_raw / manifest["readings_csv"]

    report = None if skip_validation else require_validation(
        report_dir / f"{dataset_slug(dataset)}_validation_report.json", manifest)

    own_spark = spark is None
    spark = spark or get_spark(f"trafficsense-build-{dataset}")
    t0 = time.time()
    try:
        sensors = read_header(csv_path)[1:]
        wide = load_wide(spark, csv_path, sensors).drop(CORRUPT_COL)
        speed = F.col("speed_mph")
        is_missing = speed.isNull() | F.isnan(speed) | (speed <= 0)
        readings = (
            to_long(wide, sensors, spec)
            .withColumn("is_missing", is_missing)
            .withColumn("speed_mph", F.when(is_missing, F.lit(None).cast("double")).otherwise(speed))
            .withColumn("date", F.to_date("ts"))
            .withColumn("month", F.date_format("ts", "yyyy-MM"))
            .select("ts", "sensor_id", "speed_mph", "is_missing", "date", "month")
        )
        readings_path = out_dir / "readings"
        log.info("Writing %s", readings_path)
        (readings.repartition("month").sortWithinPartitions("sensor_id", "ts")
         .write.mode("overwrite").partitionBy("month").parquet(str(readings_path)))

        written = spark.read.parquet(str(readings_path))
        stats = written.agg(F.count("*").alias("n"), F.sum(F.col("is_missing").cast("int")).alias("missing"),
                            F.countDistinct("sensor_id").alias("sensors"),
                            F.date_format(F.min("ts"), "yyyy-MM-dd HH:mm:ss").alias("min_ts"),
                            F.date_format(F.max("ts"), "yyyy-MM-dd HH:mm:ss").alias("max_ts")).first()
        expected = (report or {}).get("profile", {}).get("sensor_readings")
        if expected is not None and stats.n != expected:
            raise SystemExit(f"Row count mismatch after write: {stats.n:,} != validated {expected:,}")

        order = spark.createDataFrame([(int(s), i) for i, s in enumerate(sensors)], "sensor_id int, column_index int")
        per_sensor = written.groupBy("sensor_id").agg(
            F.count("*").alias("n_readings"), F.sum(F.col("is_missing").cast("int")).alias("n_missing"))
        sensor_df = order.join(per_sensor, "sensor_id", "left")
        if manifest.get("locations_csv"):
            sensor_df = sensor_df.join(load_locations(spark, ds_raw / manifest["locations_csv"], spec), "sensor_id", "left")
        sensor_df = sensor_df.withColumn("missing_rate", F.col("n_missing") / F.col("n_readings")).orderBy("column_index")
        sensors_path = out_dir / "sensors"
        sensor_df.coalesce(1).write.mode("overwrite").parquet(str(sensors_path))
        n_sensor_rows = spark.read.parquet(str(sensors_path)).count()

        months = sorted(r.month for r in written.select("month").distinct().collect())
        info = {
            "dataset": dataset,
            "built_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "spark_version": spark.version,
            "source_manifest": {n: m["sha256"] for n, m in manifest["files"].items()},
            "validation_status": (report or {}).get("overall_status", "SKIPPED"),
            "readings": {"path": str(readings_path), "rows": stats.n, "missing": stats.missing,
                         "sensors": stats.sensors, "min_ts": stats.min_ts, "max_ts": stats.max_ts,
                         "partitions": months, "schema": written.schema.simpleString()},
            "sensors": {"path": str(sensors_path), "rows": n_sensor_rows},
            "cleaning_rules": ["speed null/NaN/<=0 -> speed_mph null, is_missing true", "no imputation"],
            "duration_seconds": round(time.time() - t0, 1),
        }
        (out_dir / "_build_info.json").write_text(json.dumps(info, indent=2) + "\n")
        log.info("readings: %s rows (%s missing), %s sensors, %s -> %s, partitions %s",
                 f"{stats.n:,}", f"{stats.missing:,}", stats.sensors, stats.min_ts, stats.max_ts, months)
        log.info("sensors : %d rows  |  build info: %s", n_sensor_rows, out_dir / "_build_info.json")
        return info
    finally:
        if own_spark:
            spark.stop()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dataset", default="pems-bay", choices=sorted(SPECS))
    p.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    p.add_argument("--processed-dir", type=Path, default=PROCESSED_DIR)
    p.add_argument("--report-dir", type=Path, default=VALIDATION_REPORT_DIR)
    p.add_argument("--skip-validation-check", action="store_true", help="build even without a validation report")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    args = parse_args(argv)
    build(args.dataset, args.raw_dir, args.processed_dir, args.report_dir, args.skip_validation_check)
    return 0


if __name__ == "__main__":
    sys.exit(main())
