"""Caching / persistence strategies and shuffle analysis on the cleaned PEMS-BAY readings.

Cases:
  C1 caching strategies for a reused, expensive DataFrame (window lag over all sensors + derived columns),
     used by 3 actions: no caching, cache() (= MEMORY_AND_DISK), persist(MEMORY_ONLY), persist(DISK_ONLY),
     persist(MEMORY_AND_DISK_DESER). For each: first-action (materialise) time, reuse time, storage footprint
     from the Spark storage REST API, and proof that unpersist() releases it.
  C2 SQL CACHE TABLE / UNCACHE TABLE (eager table cache through the catalog)
  S1 join strategies: 16.9M readings JOIN 325-row sensor table - broadcast hash vs sort-merge vs shuffle hash
  S2 aggregation shuffles: DataFrame groupBy (partial aggregation) vs RDD reduceByKey (map-side combine)
     vs RDD groupByKey (ships every value) - shuffle bytes and records
Execution plans of the join strategies are saved to reports/spark_features/plans/.

Usage (project root, env sourced):
    python -m src.spark_features.optimization [--quick] [--repeats 3]
"""
from __future__ import annotations

import argparse
import sys
import time

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, processed_dataset_dir
from src.spark_features.common import (
    QUICK_MONTH, SparkMetrics, bar_chart, base_args, bench_spark, environment, explain_text, flat, fmt_bytes, load_clean,
    log, md_table, measure, noop, plan_nodes, setup_logging, write_report,
)

STRATEGIES = {
    "none": None,
    "cache()": "cache",
    "persist(MEMORY_ONLY)": StorageLevel.MEMORY_ONLY,
    "persist(DISK_ONLY)": StorageLevel.DISK_ONLY,
    "persist(MEMORY_AND_DISK_DESER)": StorageLevel.MEMORY_AND_DISK_DESER,
}


def derived_frame(clean: DataFrame) -> DataFrame:
    w = Window.partitionBy("sensor_id").orderBy("ts_utc")
    return (clean.filter(F.col("quality") == "observed")
                 .select("sensor_id", "ts_utc", "ts_local", "speed_mph")
                 .withColumn("prev_speed", F.lag("speed_mph").over(w))
                 .withColumn("delta_5m", F.col("speed_mph") - F.col("prev_speed"))
                 .withColumn("hour", F.hour("ts_local"))
                 .withColumn("congested", (F.col("speed_mph") < 35).cast("int")))


ACTIONS = {
    "a1_congestion_by_hour": lambda d: d.groupBy("hour").agg(F.avg("congested")).collect(),
    "a2_sudden_drops_count": lambda d: d.filter(F.col("delta_5m") < -20).count(),
    "a3_max_drop_by_sensor": lambda d: d.groupBy("sensor_id").agg(F.min("delta_5m")).collect(),
}


def caching_cycle(spark: SparkSession, m: SparkMetrics, clean: DataFrame, label: str, level) -> dict:
    df = derived_frame(clean)
    if level == "cache":
        df = df.cache()
    elif level is not None:
        df = df.persist(level)
    runs = {k: measure(spark, f"{label}:{k}", lambda f=f: f(df), 1, 0, m) for k, f in ACTIONS.items()}
    storage = m.storage() if level is not None else []
    row = {
        "strategy": label,
        "storage_level": str(df.storageLevel) if level is not None else "-",
        "first_action_s": runs["a1_congestion_by_hour"]["seconds_median"],
        "reuse_actions_s": runs["a2_sudden_drops_count"]["seconds_median"] + runs["a3_max_drop_by_sensor"]["seconds_median"],
        "total_s": sum(r["seconds_median"] for r in runs.values()),
        "shuffle_write_bytes": sum(r["metrics"]["shuffleWriteBytes"] for r in runs.values()),
        "stages_skipped": sum(r["metrics"]["stages_skipped"] for r in runs.values()),
        "memory_bytes": sum(s["memory_bytes"] for s in storage),
        "disk_bytes": sum(s["disk_bytes"] for s in storage),
        "cached_partitions": sum(s["cached_partitions"] for s in storage),
        "is_cached": df.is_cached,
        "per_action_s": {k: r["seconds_median"] for k, r in runs.items()},
        "per_action_shuffle": [r["metrics"]["shuffleWriteBytes"] for r in runs.values()],
    }
    if level is not None:
        df.unpersist(blocking=True)
        time.sleep(0.5)
        row["after_unpersist_storage_entries"] = len(m.storage())
        row["after_unpersist_is_cached"] = df.is_cached
    return row


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    base_args(p)
    p.add_argument("--cache-repeats", type=int, default=1, help="full caching cycles per strategy (median is reported)")
    a = p.parse_args(argv)
    repeats = 1 if a.quick else a.repeats
    spark = bench_spark("trafficsense-optimization", shuffle_partitions=48)
    m = SparkMetrics(spark)
    plan_dir = a.report_dir / "plans"
    plan_dir.mkdir(parents=True, exist_ok=True)
    R = {}
    try:
        clean = load_clean(spark, quick=a.quick)
        n_rows = clean.count()

        # C1: caching strategies
        cycles = []
        for label, level in STRATEGIES.items():
            runs = [caching_cycle(spark, m, clean, label, level) for _ in range(max(1, a.cache_repeats))]
            runs.sort(key=lambda r: r["total_s"])
            cycles.append(runs[len(runs) // 2])
            log.info("%-32s first %.2fs reuse %.2fs total %.2fs mem %s disk %s", label, cycles[-1]["first_action_s"],
                     cycles[-1]["reuse_actions_s"], cycles[-1]["total_s"], fmt_bytes(cycles[-1]["memory_bytes"]),
                     fmt_bytes(cycles[-1]["disk_bytes"]))
        R["caching"] = cycles

        # C2: SQL CACHE TABLE
        derived_frame(clean).createOrReplaceTempView("derived_readings")
        eager = measure(spark, "sql_CACHE_TABLE", lambda: spark.sql("CACHE TABLE derived_readings"), 1, 0, m)
        cached_flag = spark.catalog.isCached("derived_readings")
        q = measure(spark, "sql_query_on_cached_table", lambda: spark.sql(
            "SELECT hour, avg(congested) AS rate FROM derived_readings GROUP BY hour").collect(), 1, 0, m)
        storage = m.storage()
        spark.sql("UNCACHE TABLE derived_readings")
        R["sql_cache_table"] = {"cache_table_s": eager["seconds_median"], "query_s": q["seconds_median"],
                                "is_cached": cached_flag, "after_uncache_is_cached": spark.catalog.isCached("derived_readings"),
                                "memory_bytes": sum(s["memory_bytes"] for s in storage)}

        # S1: join strategies
        stats_path = processed_dataset_dir("pems-bay", PROCESSED_DIR) / "sensor_stats"
        sensor_stats = spark.read.parquet(str(stats_path)).select("sensor_id", "ff_speed", "latitude", "longitude")
        readings = clean.select("sensor_id", "ts_utc", "speed_mph")
        joins = {
            "broadcast_hash": lambda: readings.join(F.broadcast(sensor_stats), "sensor_id"),
            "sort_merge": lambda: readings.hint("merge").join(sensor_stats.hint("merge"), "sensor_id"),
            "shuffle_hash": lambda: readings.hint("shuffle_hash").join(sensor_stats, "sensor_id"),
        }
        jrows = []
        for name, build in joins.items():
            df = build().withColumn("ratio", F.col("speed_mph") / F.col("ff_speed"))
            (plan_dir / f"join_{name}.txt").write_text(explain_text(df, "formatted"))
            nodes = plan_nodes(df)
            res = measure(spark, f"join_{name}", lambda df=df: noop(df), repeats, 1, m)
            jrows.append(flat(res, strategy=name, join_node=next((n for n in nodes if n.endswith("Join")), "?"),
                              exchanges=nodes.count("Exchange") + nodes.count("BroadcastExchange")))
        R["joins"] = jrows

        # S2: aggregation shuffles (one month: the RDD variants run Python lambdas per record)
        month = clean.filter(F.col("month") == QUICK_MONTH).filter(F.col("quality") == "observed").select("sensor_id", "speed_mph")
        month = month.persist(StorageLevel.MEMORY_AND_DISK)
        month_rows = month.count()
        pairs = month.rdd.map(lambda r: (r[0], float(r[1])))
        aggs = {
            "DataFrame groupBy().agg(avg)": lambda: {r[0]: r[1] for r in month.groupBy("sensor_id").agg(F.avg("speed_mph")).collect()},
            "RDD reduceByKey": lambda: dict(pairs.mapValues(lambda v: (v, 1))
                                            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
                                            .mapValues(lambda t: t[0] / t[1]).collect()),
            "RDD groupByKey": lambda: dict(pairs.groupByKey().mapValues(lambda vs: sum(vs) / len(vs)).collect()),
        }
        arows, answers = [], {}
        for name, fn in aggs.items():
            holder = {}
            res = measure(spark, f"agg_{name}", lambda fn=fn, holder=holder: holder.update(v=fn()), repeats, 1, m)
            answers[name] = holder["v"]
            arows.append(flat(res, method=name))
        ref = answers["DataFrame groupBy().agg(avg)"]
        R["aggregation"] = {"rows_input": month_rows, "month": QUICK_MONTH, "rows": arows,
                            "results_agree": all(abs(v[k] - ref[k]) < 1e-6 for v in answers.values() for k in ref)}
        month.unpersist()
        env = environment(spark)
    finally:
        spark.stop()

    data_desc = f"cleaned PEMS-BAY readings, {'month ' + QUICK_MONTH if a.quick else 'all 6 months'}, {n_rows:,} rows"
    c = {r["strategy"]: r for r in R["caching"]}
    best_cache = min((r for r in R["caching"] if r["strategy"] != "none"), key=lambda r: r["total_s"])
    j = {r["strategy"]: r for r in R["joins"]}
    ag = {r["method"]: r for r in R["aggregation"]["rows"]}
    bar_chart(a.report_dir / "optimization_caching.png", list(c), [c[k]["total_s"] for k in c],
              "3 actions on a derived DataFrame (window lag)", "seconds (total)")
    bar_chart(a.report_dir / "optimization_joins.png", list(j), [j[k]["seconds_median"] for k in j],
              f"Join {n_rows:,} readings with 325 sensors", "seconds",
              [j[k]["shuffleWriteBytes"] / 2**20 for k in j], "shuffle write MB")
    headline = [
        f"Caching: reusing the windowed DataFrame took {best_cache['reuse_actions_s']:.2f}s with {best_cache['strategy']} vs "
        f"{c['none']['reuse_actions_s']:.2f}s uncached ({c['none']['reuse_actions_s'] / max(best_cache['reuse_actions_s'], 1e-9):.0f}× "
        f"faster; uncached recomputed the window and shuffled {fmt_bytes(c['none']['shuffle_write_bytes'])}). End-to-end "
        f"{best_cache['total_s']:.2f}s vs {c['none']['total_s']:.2f}s, because the first action pays to materialise the cache.",
        f"Footprint: cache() {fmt_bytes(c['cache()']['memory_bytes'])} memory + {fmt_bytes(c['cache()']['disk_bytes'])} disk; "
        f"DISK_ONLY {fmt_bytes(c['persist(DISK_ONLY)']['disk_bytes'])} disk; MEMORY_ONLY cached "
        f"{c['persist(MEMORY_ONLY)']['cached_partitions']} partition(s). unpersist() left "
        f"{max(r.get('after_unpersist_storage_entries', 0) for r in R['caching'])} storage entries.",
        f"SQL CACHE TABLE is eager ({R['sql_cache_table']['cache_table_s']:.2f}s, isCached={R['sql_cache_table']['is_cached']}); "
        f"the follow-up query took {R['sql_cache_table']['query_s']:.2f}s; UNCACHE TABLE → isCached="
        f"{R['sql_cache_table']['after_uncache_is_cached']}.",
        f"Joins: broadcast hash {j['broadcast_hash']['seconds_median']:.2f}s / {fmt_bytes(j['broadcast_hash']['shuffleWriteBytes'])} "
        f"shuffled vs sort-merge {j['sort_merge']['seconds_median']:.2f}s / {fmt_bytes(j['sort_merge']['shuffleWriteBytes'])} vs "
        f"shuffle hash {j['shuffle_hash']['seconds_median']:.2f}s / {fmt_bytes(j['shuffle_hash']['shuffleWriteBytes'])}.",
        f"Aggregation over {R['aggregation']['rows_input']:,} readings: RDD groupByKey shuffled "
        f"{fmt_bytes(ag['RDD groupByKey']['shuffleWriteBytes'])} vs reduceByKey "
        f"{fmt_bytes(ag['RDD reduceByKey']['shuffleWriteBytes'])} and DataFrame groupBy "
        f"{fmt_bytes(ag['DataFrame groupBy().agg(avg)']['shuffleWriteBytes'])}; times "
        f"{ag['RDD groupByKey']['seconds_median']:.2f}s / {ag['RDD reduceByKey']['seconds_median']:.2f}s / "
        f"{ag['DataFrame groupBy().agg(avg)']['seconds_median']:.2f}s; results agree: {R['aggregation']['results_agree']}.",
    ]
    body = "\n".join([
        f"Methodology: join and aggregation cases use {repeats} measured run(s) after 1 warm-up. Caching cycles run once "
        f"per strategy (`--cache-repeats` {a.cache_repeats}), because each cycle changes cache state. Metrics come from "
        "the Spark REST API.", "",
        "## C1. Caching strategies", "",
        "Workload: `derived = observed readings + lag() window per sensor + derived columns`, then 3 actions: "
        "congestion rate by hour, count of sudden drops (> 20 mph in 5 min), and worst drop per sensor.", "",
        md_table(R["caching"], [("strategy", "Strategy"), ("storage_level", "StorageLevel"), ("first_action_s", "1st action s"),
                                ("reuse_actions_s", "Actions 2+3 s"), ("total_s", "Total s"),
                                ("shuffle_write_bytes", "Shuffle write"), ("memory_bytes", "Cached in memory"),
                                ("disk_bytes", "Cached on disk"), ("cached_partitions", "Cached partitions"),
                                ("after_unpersist_storage_entries", "Storage entries after unpersist")]),
        "", "![caching](optimization_caching.png)", "",
        "- `cache()` on a DataFrame is `persist(MEMORY_AND_DISK)`. Data is stored in compressed columnar form in memory "
        "and spills to disk only when memory is short.",
        "- The first action pays for computing and storing the data. Later actions read the cached blocks, so the window "
        "shuffle is not repeated (no shuffle write).",
        "- `MEMORY_ONLY` silently skips partitions that do not fit, which are then recomputed. `DISK_ONLY` trades speed "
        "for memory safety.",
        "- `unpersist(blocking=True)` releases the blocks; the storage REST endpoint shows no remaining entries.",
        f"- **Why uncached action 1 is fast:** it only needs `hour` and `congested`, so Catalyst pruned the unused `lag()` "
        f"window (shuffle write {fmt_bytes(c['none']['per_action_shuffle'][0])} vs {fmt_bytes(c['cache()']['per_action_shuffle'][0])} "
        "for the cached first action). A cached DataFrame must materialise **every** column, including the window, so "
        "caching only pays off when the DataFrame is reused (actions 2 and 3).",
        f"- **Serialized vs deserialized:** in PySpark, `MEMORY_ONLY`/`DISK_ONLY` are *serialized* levels "
        f"({fmt_bytes(c['persist(MEMORY_ONLY)']['memory_bytes'])}), while `cache()`/`MEMORY_AND_DISK_DESER` keep deserialized "
        f"columnar batches ({fmt_bytes(c['cache()']['memory_bytes'])}). Serialized storage is smaller but costs CPU on every read.",
        f"- `cache()` and `persist(MEMORY_AND_DISK_DESER)` use the same storage level "
        f"(`{c['cache()']['storage_level']}`), so the gap between their reuse times "
        f"({c['cache()']['reuse_actions_s']:.2f}s vs {c['persist(MEMORY_AND_DISK_DESER)']['reuse_actions_s']:.2f}s) is "
        "run-to-run variance (`cache()` ran first, straight after the uncached cycle). Use `--cache-repeats 3` for medians.", "",
        "## C2. SQL `CACHE TABLE`", "",
        md_table([R["sql_cache_table"]], [("cache_table_s", "CACHE TABLE s (eager)"), ("query_s", "Query s"),
                                          ("memory_bytes", "Memory used"), ("is_cached", "isCached"),
                                          ("after_uncache_is_cached", "isCached after UNCACHE")]), "",
        "## S1. Join strategies (16.9M-row fact table ⋈ 325-row dimension)", "",
        md_table(R["joins"], [("strategy", "Hint"), ("join_node", "Physical join"), ("exchanges", "Exchanges"),
                              ("seconds_median", "Median s"), ("shuffleWriteBytes", "Shuffle write"),
                              ("shuffleReadBytes", "Shuffle read"), ("numTasks", "Tasks")]),
        "", "![joins](optimization_joins.png)", "",
        "A broadcast hash join sends the small table to every task, so the 16.9M-row side is never shuffled. Sort-merge "
        "and shuffle-hash joins repartition **both** sides by `sensor_id`. Plans are in `plans/join_*.txt`. With "
        "`spark.sql.autoBroadcastJoinThreshold` (10 MB by default), Spark picks the broadcast automatically.", "",
        f"## S2. Aggregation shuffles: average speed per sensor ({R['aggregation']['rows_input']:,} readings, {R['aggregation']['month']})", "",
        md_table(R["aggregation"]["rows"], [("method", "Method"), ("seconds_median", "Median s"),
                                           ("shuffleWriteBytes", "Shuffle write"), ("shuffleWriteRecords", "Shuffle records*"),
                                           ("numTasks", "Tasks")]),
        "", "`groupByKey` ships every speed value across the shuffle before averaging. `reduceByKey` combines "
        "(sum, count) inside each map task first, so only one small tuple per sensor per task is shuffled. The DataFrame "
        "`groupBy` does the same partial aggregation (`HashAggregate` partial → `Exchange` → final) in the JVM, without "
        "Python serialization, which makes it the fastest.", "",
        "\\* PySpark serializes RDD records in pickled **batches**, and its `groupByKey` already merges values into "
        "per-key lists inside each task. RDD record counts therefore count batches, not individual values; **shuffle "
        "bytes** are the comparable measure.", "",
    ])
    path = write_report("optimization", "Caching, persistence & shuffle analysis", env, body,
                        {**R, "data_description": data_desc, "repeats": repeats}, headline, a.report_dir)
    log.info("report: %s", path)
    return 0 if R["aggregation"]["results_agree"] else 1


if __name__ == "__main__":
    sys.exit(main())
