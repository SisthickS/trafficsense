"""Partitioning benchmarks on the cleaned PEMS-BAY readings.

Cases:
  P1 repartition(n) vs coalesce(n) going DOWN from 200 partitions: time, shuffle bytes, partition balance
  P2 going UP: coalesce cannot increase the partition count, repartition can
  P3 spark.sql.shuffle.partitions sweep for a groupBy, with AQE off, then AQE on (partition coalescing)
  P4 pre-partitioning by key: repartition("sensor_id") + cache once, then three sensor-keyed operations reuse
     the partitioning (no further Exchange), vs. each operation shuffling on its own
  P5 output layout: partitionBy("month") vs partitionBy("sensor_id") - the small-files problem

Usage (project root, env sourced):
    python -m src.spark_features.partition_benchmarks [--quick] [--repeats 3]
"""
from __future__ import annotations

import argparse
import shutil
import statistics
import sys

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.spark_features.common import (
    BENCH_SCRATCH, QUICK_MONTH, SparkMetrics, bar_chart, base_args, bench_spark, dir_size, environment, flat, fmt_bytes,
    load_clean, log, md_table, measure, noop, plan_nodes, setup_logging, write_report,
)

SHUFFLE_SWEEP = [12, 48, 200, 1000]


def balance(df: DataFrame) -> dict:
    counts = [r["count"] for r in df.groupBy(F.spark_partition_id().alias("pid")).count().collect()]
    n = df.rdd.getNumPartitions()
    counts += [0] * (n - len(counts))
    mean = statistics.mean(counts)
    return {"partitions": n, "min_rows": min(counts), "max_rows": max(counts),
            "max_over_mean_x": max(counts) / mean if mean else 0.0,
            "cv_pct": 100 * statistics.pstdev(counts) / mean if mean else 0.0}


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    base_args(p)
    a = p.parse_args(argv)
    repeats = 1 if a.quick else a.repeats
    root = BENCH_SCRATCH / "partitions"
    spark = bench_spark("trafficsense-partition-benchmarks", shuffle_partitions=48)
    m = SparkMetrics(spark)
    cores = spark.sparkContext.defaultParallelism
    R = {}
    try:
        src = load_clean(spark, quick=a.quick).select("sensor_id", "ts_utc", "date_local", "speed_mph", "quality", "month")
        n_rows = src.count()

        # P1 / P2: start from 200 cached partitions so both variants see identical input
        base200 = src.repartition(200).persist(StorageLevel.MEMORY_AND_DISK)
        base200.count()
        p1 = []
        for label, fn in (("coalesce", lambda: base200.coalesce(cores)), ("repartition", lambda: base200.repartition(cores))):
            res = measure(spark, f"{label}_200_to_{cores}", lambda fn=fn: noop(fn()), repeats, 1, m)
            p1.append(flat(res, op=f"{label}({cores})", **balance(fn()), exchange="Exchange" in plan_nodes(fn())))
        R["down"] = {"from": 200, "to": cores, "rows": p1}
        R["up"] = {"from": 200, "requested": 400,
                   "coalesce_result": base200.coalesce(400).rdd.getNumPartitions(),
                   "repartition_result": base200.repartition(400).rdd.getNumPartitions()}
        base200.unpersist()

        # P3: shuffle partitions sweep, AQE off then on
        def daily_agg():
            noop(src.groupBy("sensor_id", "date_local").agg(F.avg("speed_mph").alias("avg"), F.count(F.lit(1)).alias("n")))
        sweep = []
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        for n in SHUFFLE_SWEEP:
            spark.conf.set("spark.sql.shuffle.partitions", str(n))
            sweep.append(flat(measure(spark, f"groupBy_shuffle_partitions_{n}_aqe_off", daily_agg, repeats, 1, m),
                              shuffle_partitions=n, aqe="off"))
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_SWEEP[-1]))
        sweep.append(flat(measure(spark, f"groupBy_shuffle_partitions_{SHUFFLE_SWEEP[-1]}_aqe_on", daily_agg, repeats, 1, m),
                          shuffle_partitions=SHUFFLE_SWEEP[-1], aqe="on"))
        spark.conf.set("spark.sql.shuffle.partitions", "48")
        R["shuffle_sweep"] = sweep

        # P4: pre-partitioning by key
        w = Window.partitionBy("sensor_id").orderBy("ts_utc")
        ops = {
            "avg_by_sensor": lambda d: d.groupBy("sensor_id").agg(F.avg("speed_mph")),
            "max_drop_by_sensor": lambda d: d.groupBy("sensor_id").agg(F.min("speed_mph"), F.max("speed_mph")),
            "lag_window_by_sensor": lambda d: d.withColumn("prev", F.lag("speed_mph").over(w)).filter(F.col("prev") - F.col("speed_mph") > 20),
        }
        plain = [flat(measure(spark, f"plain_{k}", lambda f=f: noop(f(src)), repeats, 1, m), op=k,
                      exchanges=plan_nodes(f(src)).count("Exchange")) for k, f in ops.items()]
        keyed = src.repartition("sensor_id").persist(StorageLevel.MEMORY_AND_DISK)
        prep = measure(spark, "prepartition_by_sensor_and_cache", keyed.count, 1, 0, m)
        pre = [flat(measure(spark, f"prepartitioned_{k}", lambda f=f: noop(f(keyed)), repeats, 1, m), op=k,
                    exchanges=plan_nodes(f(keyed)).count("Exchange")) for k, f in ops.items()]
        keyed.unpersist()
        R["prepartition"] = {"plain": plain, "prepartitioned": pre, "one_time_cost": flat(prep, op="repartition(sensor_id)+cache")}

        # P5: output layout / small files
        layout = []
        for col in ("month", "sensor_id"):
            out = root / f"by_{col}"
            shutil.rmtree(out, ignore_errors=True)
            wres = measure(spark, f"write_partitionBy_{col}", lambda col=col, out=out:
                           src.write.mode("overwrite").partitionBy(col).parquet(str(out)), 1, 0, m)
            size, files = dir_size(out)
            rres = measure(spark, f"scan_partitionBy_{col}", lambda out=out: noop(spark.read.parquet(str(out))), repeats, 1, m)
            layout.append({"layout": f"partitionBy({col})", "write_s": wres["seconds_median"], "scan_s": rres["seconds_median"],
                           "files": files, "size_bytes": size, "avg_file_bytes": size // max(files, 1),
                           "scan_tasks": rres["metrics"]["numTasks"]})
        R["layout"] = layout
        env = environment(spark)
    finally:
        spark.stop()
        shutil.rmtree(root, ignore_errors=True)

    data_desc = f"cleaned PEMS-BAY readings, {'month ' + QUICK_MONTH if a.quick else 'all 6 months'}, {n_rows:,} rows"
    d = {r["op"].split("(")[0]: r for r in R["down"]["rows"]}
    sw = R["shuffle_sweep"]
    best = min(sw[:-1], key=lambda r: r["seconds_median"])
    worst = max(sw[:-1], key=lambda r: r["seconds_median"])
    plain_t = sum(r["seconds_median"] for r in R["prepartition"]["plain"])
    pre_t = sum(r["seconds_median"] for r in R["prepartition"]["prepartitioned"])
    plain_x = sum(r["exchanges"] for r in R["prepartition"]["plain"])
    pre_x = sum(r["exchanges"] for r in R["prepartition"]["prepartitioned"])
    lay = {r["layout"]: r for r in R["layout"]}
    bar_chart(a.report_dir / "partition_shuffle_sweep.png", [f"{r['shuffle_partitions']}\nAQE {r['aqe']}" for r in sw],
              [r["seconds_median"] for r in sw], "groupBy(sensor, day): spark.sql.shuffle.partitions", "seconds",
              [r["numTasks"] for r in sw], "tasks")
    headline = [
        f"coalesce(200→{cores}) took {d['coalesce']['seconds_median']:.2f}s with {fmt_bytes(d['coalesce']['shuffleWriteBytes'])} "
        f"shuffled; repartition({cores}) took {d['repartition']['seconds_median']:.2f}s and shuffled "
        f"{fmt_bytes(d['repartition']['shuffleWriteBytes'])}. Partition balance (max/mean): coalesce "
        f"{d['coalesce']['max_over_mean_x']:.2f}×, repartition {d['repartition']['max_over_mean_x']:.2f}×.",
        f"Increasing partitions: coalesce(400) left {R['up']['coalesce_result']} partitions; repartition(400) gave "
        f"{R['up']['repartition_result']}.",
        f"Shuffle partitions (AQE off): best {best['shuffle_partitions']} → {best['seconds_median']:.2f}s, worst "
        f"{worst['shuffle_partitions']} → {worst['seconds_median']:.2f}s ({worst['numTasks']:,} tasks). With AQE on, "
        f"{SHUFFLE_SWEEP[-1]} requested partitions ran as {sw[-1]['numTasks']:,} tasks in {sw[-1]['seconds_median']:.2f}s.",
        f"Pre-partitioning by sensor: three sensor-keyed operations needed {plain_x} Exchange(s) and {plain_t:.2f}s on the "
        f"raw data, vs {pre_x} Exchange(s) and {pre_t:.2f}s after a one-time repartition+cache "
        f"({R['prepartition']['one_time_cost']['seconds_median']:.2f}s).",
        f"Small files: partitionBy(sensor_id) wrote {lay['partitionBy(sensor_id)']['files']:,} files "
        f"(avg {fmt_bytes(lay['partitionBy(sensor_id)']['avg_file_bytes'])}) vs {lay['partitionBy(month)']['files']:,} for "
        f"partitionBy(month); the full scan took {lay['partitionBy(sensor_id)']['scan_s']:.2f}s vs {lay['partitionBy(month)']['scan_s']:.2f}s.",
    ]
    cols = [("seconds_median", "Median s"), ("shuffleWriteBytes", "Shuffle write"), ("shuffleReadBytes", "Shuffle read"),
            ("numTasks", "Tasks")]
    body = "\n".join([
        f"Methodology: {repeats} measured run(s) after 1 warm-up, `noop` sink, metrics from the Spark REST API. "
        f"{cores} cores (`local[*]`).", "",
        f"## P1. Reducing partitions: 200 → {cores}", "",
        md_table(R["down"]["rows"], [("op", "Operation")] + cols + [("exchange", "Exchange in plan"), ("min_rows", "Min rows/part"),
                                                                    ("max_rows", "Max rows/part"), ("max_over_mean_x", "Max/mean"),
                                                                    ("cv_pct", "CV")]),
        "", "`coalesce` merges existing partitions on the same executor, a narrow dependency with no shuffle, but the "
        "merged partitions can be uneven. `repartition` does a full shuffle (round-robin) and produces evenly sized partitions.", "",
        "## P2. Increasing partitions: 200 → 400", "",
        f"`coalesce(400)` → **{R['up']['coalesce_result']}** partitions (it can only merge). "
        f"`repartition(400)` → **{R['up']['repartition_result']}** partitions.", "",
        "## P3. `spark.sql.shuffle.partitions` for `groupBy(sensor_id, date_local)`", "",
        md_table(sw, [("shuffle_partitions", "Shuffle partitions"), ("aqe", "AQE")] + cols),
        "", "![sweep](partition_shuffle_sweep.png)", "",
        "Too few partitions under-use the cores and make large tasks. Too many add scheduling overhead per tiny task. "
        "Adaptive Query Execution (AQE) merges small post-shuffle partitions at runtime (`AQEShuffleRead`), so a "
        "generous setting costs little.", "",
        "## P4. Pre-partitioning by key (`repartition(\"sensor_id\")` + cache)", "",
        md_table(R["prepartition"]["plain"] + R["prepartition"]["prepartitioned"],
                 [("case", "Case"), ("exchanges", "Exchanges in plan")] + cols),
        "", f"One-time cost of `repartition(\"sensor_id\").persist()`: {R['prepartition']['one_time_cost']['seconds_median']:.2f}s, "
        f"{fmt_bytes(R['prepartition']['one_time_cost']['shuffleWriteBytes'])} shuffled. When the data is already hash-partitioned "
        "by the key, later `groupBy`/`Window` on that key need no `Exchange`.", "",
        "## P5. Output layout and the small-files problem", "",
        md_table(R["layout"], [("layout", "Layout"), ("files", "Files"), ("size_bytes", "Size"), ("avg_file_bytes", "Avg file"),
                               ("write_s", "Write s"), ("scan_s", "Full scan s"), ("scan_tasks", "Scan tasks")]),
        "", "Partitioning by a high-cardinality column creates many tiny files, which slows writes, file listing and "
        "scans. Choose partition columns with low cardinality that match common filters (here `month`).", "",
    ])
    path = write_report("partition_benchmarks", "Partitioning benchmarks", env, body,
                        {**R, "data_description": data_desc, "repeats": repeats}, headline, a.report_dir)
    log.info("report: %s", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
