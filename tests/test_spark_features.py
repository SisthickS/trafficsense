"""Phase 4 tests: the benchmark harness measures real Spark behaviour and plan comparison is sound.

Run:  pytest -q tests/test_spark_features.py
"""
from __future__ import annotations

import json

import pytest
from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.spark_features.common import (
    BENCH_CONF, SparkMetrics, md_table, measure, normalized_plan, noop, plan_nodes, write_report, write_summary,
)
from src.common.spark import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("trafficsense-tests-spark-features", driver_memory="1g", shuffle_partitions=4, extra_conf=BENCH_CONF)
    yield s
    s.stop()


@pytest.fixture(scope="module")
def data(spark):
    return (spark.range(200_000)
                 .select((F.col("id") % 50).cast("int").alias("sensor_id"),
                         (F.col("id") * 300).cast("timestamp").alias("ts_utc"),
                         (F.col("id") % 70 + 5).cast("float").alias("speed_mph")))


def test_measure_reports_shuffle_for_wide_and_none_for_narrow(spark, data):
    m = SparkMetrics(spark)
    wide = measure(spark, "wide", lambda: noop(data.groupBy("sensor_id").agg(F.avg("speed_mph"))), repeats=1, warmup=0, metrics=m)
    narrow = measure(spark, "narrow", lambda: noop(data.filter("speed_mph > 30").coalesce(1)), repeats=1, warmup=0, metrics=m)
    assert wide["metrics"]["jobs"] >= 1 and wide["metrics"]["shuffleWriteBytes"] > 0
    assert narrow["metrics"]["jobs"] >= 1 and narrow["metrics"]["shuffleWriteBytes"] == 0
    assert len(wide["seconds_all"]) == 1 and wide["seconds_median"] > 0


def test_transformations_are_lazy(spark, data):
    holder = {}
    res = measure(spark, "lazy", lambda: holder.update(df=data.filter("speed_mph < 35").groupBy("sensor_id").count()),
                  repeats=1, warmup=0)
    assert res["metrics"]["jobs"] == 0
    act = measure(spark, "action", lambda: holder["df"].count(), repeats=1, warmup=0)
    assert act["metrics"]["jobs"] >= 1 and act["result"] == 50


def test_dataframe_and_sql_produce_identical_plans(spark, data):
    data.createOrReplaceTempView("t")
    df = data.filter(F.col("speed_mph") < 35).groupBy("sensor_id").agg(F.count(F.lit(1)).alias("n")).orderBy("sensor_id")
    sql = spark.sql("SELECT sensor_id, count(1) AS n FROM t WHERE speed_mph < 35 GROUP BY sensor_id ORDER BY sensor_id")
    other = spark.sql("SELECT sensor_id, count(1) AS n FROM t WHERE speed_mph < 40 GROUP BY sensor_id ORDER BY sensor_id")
    assert normalized_plan(df, "optimized") == normalized_plan(sql, "optimized")
    assert normalized_plan(df, "physical") == normalized_plan(sql, "physical")
    assert normalized_plan(df, "optimized") != normalized_plan(other, "optimized")
    assert df.collect() == sql.collect()


def test_plan_nodes_detect_join_strategy_and_reused_partitioning(spark, data):
    dim = spark.createDataFrame([(i, float(i)) for i in range(50)], "sensor_id int, lat double")
    assert "BroadcastHashJoin" in plan_nodes(data.join(F.broadcast(dim), "sensor_id"))
    assert "SortMergeJoin" in plan_nodes(data.hint("merge").join(dim.hint("merge"), "sensor_id"))
    assert plan_nodes(data.groupBy("sensor_id").count()).count("Exchange") == 1
    keyed = data.repartition("sensor_id").persist(StorageLevel.MEMORY_AND_DISK)
    keyed.count()
    try:
        nodes = plan_nodes(keyed.groupBy("sensor_id").count())
        assert "InMemoryTableScan" in nodes and nodes.count("Exchange") == 0
    finally:
        keyed.unpersist()


def test_storage_metrics_and_unpersist(spark, data):
    m = SparkMetrics(spark)
    cached = data.persist(StorageLevel.MEMORY_ONLY)
    cached.count()
    entries = m.storage()
    assert entries and sum(e["memory_bytes"] for e in entries) > 0
    cached.unpersist(blocking=True)
    assert m.storage() == []


def test_reports_and_summary(tmp_path, spark):
    from src.spark_features.common import environment
    env = environment(spark)
    table = md_table([{"case": "a", "seconds_median": 1.234, "shuffleWriteBytes": 2048, "files": 3, "ok": True}],
                     [("case", "Case"), ("seconds_median", "s"), ("shuffleWriteBytes", "Shuffle"), ("files", "Files"), ("ok", "OK")])
    assert "| a | 1.23 | 2.0 KB | 3 | yes |" in table
    write_report("storage_benchmarks", "Storage", env, table, {"data_description": "x"}, ["finding 1"], tmp_path)
    summary = write_summary(tmp_path).read_text()
    assert "finding 1" in summary and "[Storage](storage_benchmarks.md)" in summary
    assert json.loads((tmp_path / "storage_benchmarks.json").read_text())["headline"] == ["finding 1"]
