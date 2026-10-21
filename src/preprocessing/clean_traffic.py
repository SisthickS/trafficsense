"""Clean the raw wide-format PEMS-BAY readings into a complete, long-format, quality-flagged Parquet dataset.

    data/raw/<dataset>/<readings>.csv   (immutable, checksum-verified, validated in Phase 2)
      -> data/processed/<dataset>/clean/  (Parquet, partitioned by local month)
      -> reports/preprocessing/<dataset>_cleaning_report.{json,md}

Steps (each counted in the cleaning report):
  1. Wide -> long unpivot: one row per (sensor, timestamp).
  2. Timestamp anomalies: unparseable timestamps dropped; off-grid timestamps floored to the 5-minute grid;
     naive local wall-clock converted to true UTC instants. This removes the DST artefact (the "missing"
     02:00-02:55 hour on 2017-03-12 never existed, so in real time the series is continuous).
  3. Duplicate (sensor, ts_utc) rows collapsed (e.g. the repeated local hour when DST ends): keep the valid,
     on-grid reading.
  4. Re-index to the complete sensor x UTC grid, so every later window/lag is an exact time offset;
     inserted rows are flagged `absent_row`.
  5. Invalid measurements -> null with a reason: null/NaN, <= 0 mph (DCRNN encodes gaps as 0.0),
     > max plausible speed, and stuck detectors (identical readings for >= 6 h, or identical congested
     readings for >= 1 h - real congestion fluctuates).
  6. Causal gap filling: forward-fill the last valid reading for at most 15 minutes (never uses the future,
     identical to what a live stream can do). Longer gaps stay null and are flagged `missing`.

Usage (project root, env sourced):
    python -m src.preprocessing.clean_traffic
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, RAW_DIR, REPORTS_DIR, VALIDATION_REPORT_DIR, dataset_slug, processed_dataset_dir, raw_dataset_dir
from src.common.spark import get_spark
from src.preprocessing.build_processed import require_validation
from src.preprocessing.congestion import CONGESTION_SPEED_MPH
from src.preprocessing.download_dataset import load_verified_manifest
from src.preprocessing.raw_reader import CORRUPT_COL, load_wide, read_header, to_long
from src.preprocessing.sources import SPECS, DatasetSpec

log = logging.getLogger("clean_traffic")
PREPROCESSING_REPORT_DIR = REPORTS_DIR / "preprocessing"

QUALITY_OBSERVED, QUALITY_IMPUTED, QUALITY_MISSING = "observed", "imputed", "missing"
ISSUE_NULL, ISSUE_NON_POSITIVE, ISSUE_OUT_OF_RANGE = "null_value", "non_positive", "out_of_range"
ISSUE_STUCK, ISSUE_ABSENT = "stuck_sensor", "absent_row"
CLEAN_COLUMNS = ["sensor_id", "ts_utc", "ts_local", "speed_raw", "speed_mph", "quality", "issue", "date_local", "month"]


@dataclass(frozen=True)
class CleaningConfig:
    max_speed_mph: float = 100.0
    stuck_run_slots: int = 72              # identical readings for >= 6 h
    stuck_congested_run_slots: int = 12    # identical readings below the congestion threshold for >= 1 h
    congestion_speed_mph: float = CONGESTION_SPEED_MPH
    ffill_limit_slots: int = 3             # forward-fill at most 15 min


def load_raw_long(spark: SparkSession, dataset: str, raw_dir: Path, spec: DatasetSpec) -> tuple[DataFrame, list[str]]:
    """Checksum-verified raw wide CSV -> long rows (ts_local_raw, sensor_id, speed_raw)."""
    ds_raw = raw_dataset_dir(dataset, raw_dir)
    manifest = load_verified_manifest(ds_raw)
    csv_path = ds_raw / manifest["readings_csv"]
    sensors = read_header(csv_path)[1:]
    wide = load_wide(spark, csv_path, sensors).drop(CORRUPT_COL)
    long = to_long(wide, sensors, spec).select(
        F.col("ts").alias("ts_local_raw"), "sensor_id", F.col("speed_mph").alias("speed_raw"))
    return long, sensors


def clean(long: DataFrame, spark: SparkSession, spec: DatasetSpec, cfg: CleaningConfig,
          sensor_ids: list[str]) -> tuple[DataFrame, dict]:
    """Return (clean DataFrame with CLEAN_COLUMNS, input statistics)."""
    step, tz = spec.interval_seconds, spec.local_timezone
    epoch = F.col("ts_local_raw").cast("long")
    snapped = F.timestamp_seconds(F.floor(epoch / step) * step)

    df = (long.filter(F.col("ts_local_raw").isNotNull())
              .withColumn("ts_local_snapped", snapped)
              .withColumn("ts_utc", F.to_utc_timestamp("ts_local_snapped", tz)))

    in_stats = long.agg(
        F.count("*").alias("input_rows"),
        F.sum(F.col("ts_local_raw").isNull().cast("int")).alias("unparseable_timestamps"),
        F.sum((F.col("ts_local_raw").isNotNull() & (epoch % step != 0)).cast("int")).alias("off_grid_timestamps"),
    ).first().asDict()
    bounds = df.agg(F.min("ts_utc").alias("lo"), F.max("ts_utc").alias("hi"),
                    F.countDistinct("sensor_id", "ts_utc").alias("distinct_keys")).first()
    in_stats["duplicate_rows"] = in_stats["input_rows"] - in_stats["unparseable_timestamps"] - bounds.distinct_keys

    speed = F.col("speed_raw")
    value_issue = (F.when(speed.isNull() | F.isnan(speed), ISSUE_NULL)
                    .when(speed <= 0, ISSUE_NON_POSITIVE)
                    .when(speed > cfg.max_speed_mph, ISSUE_OUT_OF_RANGE))
    df = df.withColumn("issue", value_issue)

    dedup_order = Window.partitionBy("sensor_id", "ts_utc").orderBy(
        F.col("issue").isNull().desc(),
        (F.col("ts_local_raw") == F.col("ts_local_snapped")).desc(),
        F.col("ts_local_raw").desc())
    df = df.withColumn("_rn", F.row_number().over(dedup_order)).filter("_rn = 1").drop("_rn", "ts_local_snapped")

    n_slots = int((bounds.hi - bounds.lo).total_seconds() // step) + 1
    lo_epoch = int(bounds.lo.replace(tzinfo=timezone.utc).timestamp())
    grid = spark.range(n_slots).select(F.timestamp_seconds(F.lit(lo_epoch) + F.col("id") * step).alias("ts_utc"))
    sensors = spark.createDataFrame([(int(s),) for s in sensor_ids], "sensor_id int")
    df = (sensors.crossJoin(grid).join(df, ["sensor_id", "ts_utc"], "left")
              .withColumn("issue", F.when(F.col("ts_local_raw").isNull(), ISSUE_ABSENT).otherwise(F.col("issue"))))
    in_stats.update(grid_slots=n_slots, sensors=len(sensor_ids), expected_rows=n_slots * len(sensor_ids),
                    first_ts_utc=str(bounds.lo), last_ts_utc=str(bounds.hi))

    # Stuck-detector runs: consecutive identical valid readings per sensor.
    w = Window.partitionBy("sensor_id").orderBy("ts_utc")
    df = df.withColumn("_v", F.when(F.col("issue").isNull(), F.col("speed_raw")))
    prev = F.lag("_v").over(w)
    df = (df.withColumn("_new_run", F.when(F.col("_v").isNull() | prev.isNull() | (F.col("_v") != prev), 1).otherwise(0))
            .withColumn("_run_id", F.sum("_new_run").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
            .withColumn("_run_len", F.count("_v").over(Window.partitionBy("sensor_id", "_run_id"))))
    stuck = F.col("_v").isNotNull() & (
        (F.col("_run_len") >= cfg.stuck_run_slots)
        | ((F.col("_run_len") >= cfg.stuck_congested_run_slots) & (F.col("_v") < cfg.congestion_speed_mph)))
    df = (df.withColumn("issue", F.when(stuck, ISSUE_STUCK).otherwise(F.col("issue")))
            .withColumn("_valid", F.when(F.col("issue").isNull(), F.col("speed_raw"))))

    # Causal forward fill, bounded.
    last_valid = F.last("_valid", ignorenulls=True).over(w.rowsBetween(-cfg.ffill_limit_slots, Window.currentRow))
    df = df.withColumn("speed_mph", F.coalesce(F.col("_valid"), last_valid))
    quality = (F.when(F.col("_valid").isNotNull(), QUALITY_OBSERVED)
                .when(F.col("speed_mph").isNotNull(), QUALITY_IMPUTED)
                .otherwise(QUALITY_MISSING))
    ts_local = F.from_utc_timestamp("ts_utc", tz)
    out = df.select(
        F.col("sensor_id").cast("int"),
        "ts_utc",
        ts_local.alias("ts_local"),
        F.col("speed_raw").cast("float"),
        F.col("speed_mph").cast("float"),
        quality.alias("quality"),
        "issue",
        F.to_date(ts_local).alias("date_local"),
        F.date_format(ts_local, "yyyy-MM").alias("month"),
    )
    return out, in_stats


def summarize(clean_df: DataFrame, in_stats: dict, cfg: CleaningConfig, step: int) -> dict:
    t = clean_df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("ts_utc").alias("distinct_ts"),
        F.countDistinct("sensor_id").alias("sensors"),
        F.min("ts_utc").alias("lo"), F.max("ts_utc").alias("hi"),
    ).first()
    issues = {r.issue: r["count"] for r in clean_df.groupBy("issue").count().collect() if r.issue is not None}
    quality = {r.quality: r["count"] for r in clean_df.groupBy("quality").count().collect()}
    by_issue_quality = [r.asDict() for r in clean_df.filter(F.col("issue").isNotNull())
                        .groupBy("issue", "quality").count().orderBy("issue", "quality").collect()]
    per_sensor = clean_df.groupBy("sensor_id").agg(
        F.avg((F.col("quality") == QUALITY_OBSERVED).cast("double")).alias("observed_share"),
        F.sum((F.col("quality") == QUALITY_MISSING).cast("int")).alias("missing"))
    ps = per_sensor.agg(F.min("observed_share").alias("min_obs"), F.max("missing").alias("max_missing"),
                        F.sum((F.col("missing") > 0).cast("int")).alias("sensors_with_missing")).first()
    worst = [r.asDict() for r in per_sensor.orderBy("observed_share").limit(5).collect()]
    span_slots = int((t.hi - t.lo).total_seconds() // step) + 1
    return {
        "config": asdict(cfg),
        "input": in_stats,
        "output": {
            "rows": t.rows, "sensors": t.sensors, "distinct_ts_utc": t.distinct_ts,
            "first_ts_utc": str(t.lo), "last_ts_utc": str(t.hi), "grid_complete": t.rows == t.sensors * span_slots == in_stats["expected_rows"],
            "quality_counts": quality, "issue_counts": issues, "issue_by_final_quality": by_issue_quality,
            "min_sensor_observed_share": round(ps.min_obs, 6), "max_sensor_missing_after_fill": ps.max_missing,
            "sensors_with_missing_after_fill": ps.sensors_with_missing, "worst_sensors": worst,
        },
    }


def render_markdown(dataset: str, rep: dict) -> str:
    i, o = rep["input"], rep["output"]
    rows = o["rows"]
    pct = lambda n: f"{n / rows:.4%}" if rows else "-"
    L = [f"# Cleaning Report — {dataset.upper()}", "",
         f"- Generated (UTC): {rep['generated_at_utc']} · runtime {rep['duration_seconds']} s",
         f"- Output: `{rep['output_path']}` (Parquet, partitioned by local `month`)",
         f"- Complete sensor × time grid: **{'yes' if o['grid_complete'] else 'NO'}** — "
         f"{o['sensors']} sensors × {o['distinct_ts_utc']:,} UTC slots = {rows:,} rows "
         f"({o['first_ts_utc']} → {o['last_ts_utc']} UTC)", "",
         "## 1. Timestamp handling", "", "| Step | Rows |", "|---|---|",
         f"| Input rows (wide → long) | {i['input_rows']:,} |",
         f"| Unparseable timestamps (dropped) | {i['unparseable_timestamps']:,} |",
         f"| Off-grid timestamps (floored to 5 min) | {i['off_grid_timestamps']:,} |",
         f"| Duplicate (sensor, UTC) rows collapsed | {i['duplicate_rows']:,} |",
         f"| Rows inserted to complete the grid (`absent_row`) | {o['issue_counts'].get(ISSUE_ABSENT, 0):,} |", "",
         "Local wall-clock timestamps were converted to UTC (`America/Los_Angeles` → UTC). The 12 \"missing\" "
         "local slots on 2017-03-12 02:00–02:55 never existed (DST spring-forward); in UTC the series is "
         "continuous, so no rows are needed for them.", "",
         "## 2. Invalid measurements", "", "| Issue | Rows | Share | Rule |", "|---|---|---|---|"]
    rules = {
        ISSUE_NULL: "null / NaN",
        ISSUE_NON_POSITIVE: "speed ≤ 0 mph (DCRNN stores missing readings as 0.0)",
        ISSUE_OUT_OF_RANGE: f"speed > {rep['config']['max_speed_mph']:.0f} mph",
        ISSUE_STUCK: (f"identical readings ≥ {rep['config']['stuck_run_slots']} slots "
                      f"({rep['config']['stuck_run_slots'] * 5 / 60:.0f} h), or identical readings "
                      f"< {rep['config']['congestion_speed_mph']:.0f} mph for ≥ {rep['config']['stuck_congested_run_slots']} slots (1 h)"),
        ISSUE_ABSENT: "row missing from the source",
    }
    for issue, rule in rules.items():
        n = o["issue_counts"].get(issue, 0)
        L.append(f"| `{issue}` | {n:,} | {pct(n)} | {rule} |")
    L += ["", "## 3. Gap filling (causal)", "",
          f"Forward-fill the last valid reading for at most {rep['config']['ffill_limit_slots']} slots "
          f"({rep['config']['ffill_limit_slots'] * 5} min). This uses only past data, exactly what the live stream can do.", "",
          "| Final quality | Rows | Share |", "|---|---|---|"]
    for q in (QUALITY_OBSERVED, QUALITY_IMPUTED, QUALITY_MISSING):
        n = o["quality_counts"].get(q, 0)
        L.append(f"| `{q}` | {n:,} | {pct(n)} |")
    L += ["", "| Issue → final quality | Rows |", "|---|---|"]
    L += [f"| `{r['issue']}` → `{r['quality']}` | {r['count']:,} |" for r in o["issue_by_final_quality"]]
    L += ["", f"Lowest per-sensor observed share: **{o['min_sensor_observed_share']:.3%}** · sensors with "
          f"unfilled gaps: {o['sensors_with_missing_after_fill']} · max unfilled readings for one sensor: "
          f"{o['max_sensor_missing_after_fill']:,}", "",
          "| Worst sensors | Observed share | Unfilled |", "|---|---|---|"]
    L += [f"| {w['sensor_id']} | {w['observed_share']:.3%} | {w['missing']:,} |" for w in o["worst_sensors"]]
    L += ["", "## 4. Output schema", "", "| Column | Type | Description |", "|---|---|---|",
          "| `sensor_id` | int | PeMS station id |",
          "| `ts_utc` | timestamp | Interval start, true UTC instant (use for ordering, lags, windows) |",
          "| `ts_local` | timestamp | Interval start, local Pacific wall-clock (use for hour/day features) |",
          "| `speed_raw` | float | Raw reading as published (null for inserted rows) |",
          "| `speed_mph` | float | Cleaned speed: valid reading, else forward-filled ≤ 15 min, else null |",
          "| `quality` | string | `observed` / `imputed` / `missing` |",
          "| `issue` | string | Why the raw value was not used (null when valid) |",
          "| `date_local`, `month` | date, string | Local calendar date; partition column |", ""]
    return "\n".join(L)


def run(dataset: str, raw_dir: Path, processed_dir: Path, validation_dir: Path, report_dir: Path,
        cfg: CleaningConfig, spark: SparkSession | None = None, skip_validation: bool = False) -> dict:
    spec = SPECS[dataset]
    ds_raw = raw_dataset_dir(dataset, raw_dir)
    if not skip_validation:
        require_validation(validation_dir / f"{dataset_slug(dataset)}_validation_report.json",
                           load_verified_manifest(ds_raw))
    out_path = processed_dataset_dir(dataset, processed_dir) / "clean"
    own = spark is None
    spark = spark or get_spark(f"trafficsense-clean-{dataset}", shuffle_partitions=24)
    t0 = time.time()
    try:
        long, sensors = load_raw_long(spark, dataset, raw_dir, spec)
        clean_df, in_stats = clean(long, spark, spec, cfg, sensors)
        log.info("Writing %s", out_path)
        (clean_df.repartition("month").sortWithinPartitions("sensor_id", "ts_utc")
         .write.mode("overwrite").partitionBy("month").parquet(str(out_path)))
        written = spark.read.parquet(str(out_path))
        rep = summarize(written, in_stats, cfg, spec.interval_seconds)
        rep.update(dataset=dataset, output_path=str(out_path), spark_version=spark.version,
                   generated_at_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   duration_seconds=round(time.time() - t0, 1))
    finally:
        if own:
            spark.stop()
    report_dir.mkdir(parents=True, exist_ok=True)
    stem = report_dir / f"{dataset_slug(dataset)}_cleaning_report"
    stem.with_suffix(".json").write_text(json.dumps(rep, indent=2, default=str) + "\n")
    stem.with_suffix(".md").write_text(render_markdown(dataset, rep))
    o = rep["output"]
    log.info("clean rows %s | grid complete %s | quality %s | issues %s", f"{o['rows']:,}", o["grid_complete"],
             o["quality_counts"], o["issue_counts"])
    log.info("report: %s.{json,md}", stem)
    if not o["grid_complete"]:
        raise SystemExit("Clean output is not a complete sensor x time grid.")
    return rep


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    d = CleaningConfig()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dataset", default="pems-bay", choices=sorted(SPECS))
    p.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    p.add_argument("--processed-dir", type=Path, default=PROCESSED_DIR)
    p.add_argument("--validation-dir", type=Path, default=VALIDATION_REPORT_DIR)
    p.add_argument("--report-dir", type=Path, default=PREPROCESSING_REPORT_DIR)
    p.add_argument("--max-speed", type=float, default=d.max_speed_mph)
    p.add_argument("--stuck-run-slots", type=int, default=d.stuck_run_slots)
    p.add_argument("--stuck-congested-run-slots", type=int, default=d.stuck_congested_run_slots)
    p.add_argument("--ffill-limit-slots", type=int, default=d.ffill_limit_slots)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    a = parse_args(argv)
    cfg = CleaningConfig(max_speed_mph=a.max_speed, stuck_run_slots=a.stuck_run_slots,
                         stuck_congested_run_slots=a.stuck_congested_run_slots, ffill_limit_slots=a.ffill_limit_slots)
    run(a.dataset, a.raw_dir, a.processed_dir, a.validation_dir, a.report_dir, cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
