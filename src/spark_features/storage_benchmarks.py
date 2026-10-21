"""Storage benchmarks: CSV vs Parquet on the cleaned PEMS-BAY readings.

Cases (all files written to the Linux-disk scratch area, so the comparison measures formats, not the 9p mount):
  W  write time, size on disk, file count: csv, csv+gzip, parquet+snappy, parquet+zstd, parquet partitioned by month
  R1 full scan of all columns (noop sink): csv with inferSchema, csv with explicit schema, csv.gz, parquet snappy/zstd
  R2 2-column aggregation: column pruning (bytes actually read)
  R3 selective filter (one sensor, congested readings): Parquet predicate pushdown on vs off, vs CSV
  R4 partition pruning: one month from a month-partitioned dataset vs a flat dataset
  R5 storage location: identical Parquet on the Windows drive (/mnt/c, 9p) vs the WSL ext4 disk

Usage (project root, env sourced):
    python -m src.spark_features.storage_benchmarks [--quick] [--repeats 3]
"""
from __future__ import annotations

import argparse
import shutil
import sys

from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, processed_dataset_dir
from src.spark_features.common import (
    BENCH_SCRATCH, QUICK_MONTH, SparkMetrics, bar_chart, base_args, bench_spark, dir_size, environment, explain_text,
    flat, fmt_bytes, load_clean, log, md_table, measure, noop, setup_logging, write_report,
)
from src.spark_features.dataframe_sql import CLEAN_SCHEMA, CSV_SCHEMA

COLUMNS = [f.name for f in CLEAN_SCHEMA.fields]
FORMATS = {
    "csv": ("CSV", lambda w: w.option("header", True).csv),
    "csv_gzip": ("CSV + gzip", lambda w: w.option("header", True).option("compression", "gzip").csv),
    "parquet_snappy": ("Parquet + snappy", lambda w: w.option("compression", "snappy").parquet),
    "parquet_zstd": ("Parquet + zstd", lambda w: w.option("compression", "zstd").parquet),
    "parquet_by_month": ("Parquet, partitionBy(month)", lambda w: w.partitionBy("month").option("compression", "snappy").parquet),
}


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    base_args(p)
    a = p.parse_args(argv)
    repeats = 1 if a.quick else a.repeats
    root = BENCH_SCRATCH / "storage"
    spark = bench_spark("trafficsense-storage-benchmarks")
    m = SparkMetrics(spark)
    paths = {k: root / k for k in FORMATS}
    R = {}
    try:
        src = load_clean(spark, quick=a.quick).select(*COLUMNS).persist(StorageLevel.MEMORY_AND_DISK)
        n_rows = src.count()
        log.info("source cached: %s rows, %d partitions", f"{n_rows:,}", src.rdd.getNumPartitions())

        # W: writes (single run each: a write is long and its output must not be overwritten mid-measurement)
        writes = []
        for key, (label, writer) in FORMATS.items():
            shutil.rmtree(paths[key], ignore_errors=True)
            df = src if key != "csv" and key != "csv_gzip" else src.drop("month")
            res = measure(spark, f"write_{key}", lambda df=df, key=key, writer=writer:
                          writer(df.write.mode("overwrite"))(str(paths[key])), repeats=1, warmup=0, metrics=m)
            size, files = dir_size(paths[key])
            writes.append(flat(res, format=label, key=key, size_bytes=size, files=files,
                               bytes_per_row=round(size / n_rows, 1)))
        csv_size = next(w["size_bytes"] for w in writes if w["key"] == "csv")
        for w in writes:
            w["vs_csv_pct"] = 100 * w["size_bytes"] / csv_size
        R["writes"] = writes

        readers = {
            "csv_inferSchema": lambda: spark.read.option("header", True).option("inferSchema", True).csv(str(paths["csv"])),
            "csv_explicit_schema": lambda: spark.read.option("header", True).schema(CSV_SCHEMA).csv(str(paths["csv"])),
            "csv_gzip_explicit_schema": lambda: spark.read.option("header", True).schema(CSV_SCHEMA).csv(str(paths["csv_gzip"])),
            "parquet_snappy": lambda: spark.read.parquet(str(paths["parquet_snappy"])),
            "parquet_zstd": lambda: spark.read.parquet(str(paths["parquet_zstd"])),
        }

        # R1: full scan
        R["full_scan"] = [flat(measure(spark, f"scan_{k}", lambda r=r: noop(r()), repeats, 1, m), reader=k)
                          for k, r in readers.items()]

        # R2: column pruning - average speed per sensor touches 2 of 8 columns
        def agg(df):
            return df.groupBy("sensor_id").agg(F.avg("speed_mph")).collect()
        R["column_pruning"] = [flat(measure(spark, f"agg2cols_{k}", lambda r=readers[k]: agg(r()), repeats, 1, m), reader=k)
                               for k in ("csv_explicit_schema", "parquet_snappy")]

        # R3: predicate pushdown - one sensor's congested readings
        sensor = src.select(F.min("sensor_id")).first()[0]
        pred = (F.col("sensor_id") == sensor) & (F.col("speed_mph") < 35)
        pushed = explain_text(readers["parquet_snappy"]().filter(pred), "formatted")
        pushed_line = next((l.strip() for l in pushed.splitlines() if "PushedFilters" in l), "")
        rows = [flat(measure(spark, "filter_csv_explicit_schema", lambda: readers["csv_explicit_schema"]().filter(pred).count(),
                             repeats, 1, m), reader="csv_explicit_schema", pushdown="n/a (CSV)")]
        rows.append(flat(measure(spark, "filter_parquet_pushdown_on", lambda: readers["parquet_snappy"]().filter(pred).count(),
                                 repeats, 1, m), reader="parquet_snappy", pushdown="on"))
        spark.conf.set("spark.sql.parquet.filterPushdown", "false")
        rows.append(flat(measure(spark, "filter_parquet_pushdown_off", lambda: readers["parquet_snappy"]().filter(pred).count(),
                                 repeats, 1, m), reader="parquet_snappy", pushdown="off"))
        spark.conf.set("spark.sql.parquet.filterPushdown", "true")
        R["predicate_pushdown"] = {"sensor_id": sensor, "pushed_filters": pushed_line, "rows": rows,
                                   "matching_rows": rows[1].get("result")}

        # R4: partition pruning
        months = sorted(r.month for r in src.select("month").distinct().collect())
        target = months[len(months) // 2]
        by_month = spark.read.parquet(str(paths["parquet_by_month"])).filter(F.col("month") == target)
        part_line = next((l.strip() for l in explain_text(by_month, "formatted").splitlines() if "PartitionFilters" in l), "")
        R["partition_pruning"] = {
            "month": target, "months_available": len(months), "partition_filters": part_line,
            "rows": [flat(measure(spark, "month_filter_flat_parquet", lambda: noop(readers["parquet_snappy"]()
                                  .filter(F.col("month") == target)), repeats, 1, m), layout="flat parquet (filter on column)"),
                     flat(measure(spark, "month_filter_partitioned_parquet", lambda: noop(spark.read.parquet(
                         str(paths["parquet_by_month"])).filter(F.col("month") == target)), repeats, 1, m),
                         layout="partitionBy(month) (partition pruning)")]}

        # R5: storage location (same layout: partitioned by month, snappy)
        mnt_path = processed_dataset_dir("pems-bay", PROCESSED_DIR) / "clean"
        mnt_reader = (lambda: spark.read.parquet(str(mnt_path)).filter(F.col("month") == QUICK_MONTH)) if a.quick \
            else (lambda: spark.read.parquet(str(mnt_path)))
        R["location"] = [
            flat(measure(spark, "scan_parquet_windows_drive_9p", lambda: noop(mnt_reader()), repeats, 1, m),
                 location=f"Windows drive via 9p ({mnt_path})"),
            flat(measure(spark, "scan_parquet_wsl_ext4", lambda: noop(spark.read.parquet(str(paths["parquet_by_month"]))),
                         repeats, 1, m), location=f"WSL ext4 ({paths['parquet_by_month']})"),
        ]
        src.unpersist()
        env = environment(spark)
    finally:
        spark.stop()

    data_desc = f"cleaned PEMS-BAY readings, {'month ' + QUICK_MONTH if a.quick else 'all 6 months'}, {n_rows:,} rows × {len(COLUMNS)} columns"
    report_dir = a.report_dir
    w = {r["key"]: r for r in R["writes"]}
    fs = {r["reader"]: r for r in R["full_scan"]}
    cp = {r["reader"]: r for r in R["column_pruning"]}
    pp = {r["pushdown"]: r for r in R["predicate_pushdown"]["rows"]}
    pr = R["partition_pruning"]["rows"]
    loc = R["location"]
    bar_chart(report_dir / "storage_size.png", [w[k]["format"] for k in w], [w[k]["size_bytes"] / 2**20 for k in w],
              f"Size on disk ({n_rows:,} rows)", "MB")
    bar_chart(report_dir / "storage_full_scan.png", list(fs), [fs[k]["seconds_median"] for k in fs],
              "Full scan time (median)", "seconds")

    headline = [
        f"Size: Parquet+snappy is {w['parquet_snappy']['vs_csv_pct']:.1f}% of CSV ({fmt_bytes(w['parquet_snappy']['size_bytes'])} vs "
        f"{fmt_bytes(w['csv']['size_bytes'])}); zstd {w['parquet_zstd']['vs_csv_pct']:.1f}%; gzip-CSV {w['csv_gzip']['vs_csv_pct']:.1f}%.",
        f"Full scan: Parquet {fs['parquet_snappy']['seconds_median']:.2f}s vs CSV {fs['csv_explicit_schema']['seconds_median']:.2f}s with "
        f"an explicit schema ({fs['csv_explicit_schema']['seconds_median'] / fs['parquet_snappy']['seconds_median']:.1f}× faster) and "
        f"{fs['csv_inferSchema']['seconds_median']:.2f}s with inferSchema (CSV read twice: {fmt_bytes(fs['csv_inferSchema']['inputBytes'])}).",
        f"Column pruning: a 2-column aggregate read {fmt_bytes(cp['parquet_snappy']['inputBytes'])} from Parquet vs "
        f"{fmt_bytes(cp['csv_explicit_schema']['inputBytes'])} from CSV "
        f"({cp['csv_explicit_schema']['seconds_median'] / cp['parquet_snappy']['seconds_median']:.1f}× faster).",
        f"Predicate pushdown (sensor {R['predicate_pushdown']['sensor_id']}, speed < 35): Parquet {pp['on']['seconds_median']:.2f}s with "
        f"pushdown vs {pp['off']['seconds_median']:.2f}s without, CSV {pp['n/a (CSV)']['seconds_median']:.2f}s; input "
        f"{fmt_bytes(pp['on']['inputBytes'])} vs {fmt_bytes(pp['off']['inputBytes'])}.",
        f"Partition pruning (month {R['partition_pruning']['month']}): {pr[1]['seconds_median']:.2f}s / {fmt_bytes(pr[1]['inputBytes'])} "
        f"/ {pr[1]['numTasks']} tasks with partitionBy(month) vs {pr[0]['seconds_median']:.2f}s / {fmt_bytes(pr[0]['inputBytes'])} "
        f"/ {pr[0]['numTasks']} tasks on a flat layout.",
        f"Location: the same Parquet scans in {loc[1]['seconds_median']:.2f}s on WSL ext4 vs {loc[0]['seconds_median']:.2f}s on the "
        f"Windows drive (9p) — {loc[0]['seconds_median'] / loc[1]['seconds_median']:.1f}× slower across the VM boundary.",
    ]
    cols_time = [("seconds_median", "Median s"), ("seconds_min", "Min s"), ("inputBytes", "Bytes read"),
                 ("inputRecords", "Records read"), ("numTasks", "Tasks")]
    body = "\n".join([
        f"Methodology: {repeats} measured run(s) after 1 warm-up per read case (writes: 1 run). Source data cached in memory "
        "before writing, so write times exclude reading the source. Reads use the `noop` sink (full materialisation, no "
        "output). Byte/record/task counts come from Spark's REST API for the measured run. Files are on the WSL ext4 "
        "scratch disk, and the OS page cache is warm for every format.", "",
        "## W. Write cost and size on disk", "",
        md_table(R["writes"], [("format", "Format"), ("seconds_median", "Write s"), ("size_bytes", "Size"), ("vs_csv_pct", "% of CSV"),
                               ("bytes_per_row", "Bytes/row"), ("files", "Files")]),
        "", "![size](storage_size.png)", "",
        "## R1. Full scan (all columns)", "", md_table(R["full_scan"], [("reader", "Reader")] + cols_time),
        "", "![scan](storage_full_scan.png)", "",
        "`inferSchema` makes Spark read the whole CSV once to guess types, then again to load it, which is why its bytes read double.", "",
        "## R2. Column pruning (`avg(speed_mph)` by `sensor_id`)", "", md_table(R["column_pruning"], [("reader", "Reader")] + cols_time),
        "", "Parquet stores each column separately, so only `sensor_id` and `speed_mph` are read. CSV must parse every byte of every row.", "",
        "## R3. Predicate pushdown", "", f"Filter: `sensor_id = {R['predicate_pushdown']['sensor_id']} AND speed_mph < 35` "
        f"→ {R['predicate_pushdown']['matching_rows']} rows.", "", f"Plan evidence: `{R['predicate_pushdown']['pushed_filters']}`", "",
        md_table(R["predicate_pushdown"]["rows"], [("reader", "Reader"), ("pushdown", "Pushdown")] + cols_time),
        "", "With pushdown, the Parquet reader uses row-group/page min-max statistics to skip data and filters inside the "
        "vectorized reader. With it off, every row is decoded and passed to a separate Filter operator.", "",
        "## R4. Partition pruning", "", f"Query: all columns of month `{R['partition_pruning']['month']}` "
        f"({R['partition_pruning']['months_available']} months available).", "",
        f"Plan evidence: `{R['partition_pruning']['partition_filters']}`", "",
        md_table(pr, [("layout", "Layout")] + cols_time), "",
        (f"Here both layouts read about the same bytes ({fmt_bytes(pr[0]['inputBytes'])} vs {fmt_bytes(pr[1]['inputBytes'])}): the flat "
         "files were written from month-ordered input, so Parquet min/max statistics on `month` already skip the other "
         "months' row groups. Partition pruning still wins because the other months' files are never opened or "
         f"scheduled ({pr[1]['numTasks']} vs {pr[0]['numTasks']} tasks), and it works regardless of how rows are ordered."
         if pr[0]["inputBytes"] and abs(pr[0]["inputBytes"] - pr[1]["inputBytes"]) / pr[0]["inputBytes"] < 0.2 else
         "Partition pruning removes non-matching directories at planning time, before any file is opened."), "",
        "## R5. Storage location", "", md_table(loc, [("location", "Location")] + cols_time), "",
        "This is why scratch data, the Python venv and Spark spill files live on the WSL disk, while the project "
        "deliverables stay on the Windows drive.", "",
    ])
    path = write_report("storage_benchmarks", "Storage benchmarks: CSV vs Parquet", env, body,
                        {**R, "data_description": data_desc, "repeats": repeats}, headline, report_dir)
    shutil.rmtree(root, ignore_errors=True)
    log.info("report: %s", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
