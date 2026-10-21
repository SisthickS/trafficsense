"""PySpark DataFrame & Spark SQL demonstration on the cleaned PEMS-BAY data (16.9M readings).

Sections (each one runs real queries and records its evidence in reports/spark_features/dataframe_sql.md):
  1. Explicit schemas: StructType on Parquet; explicit schema vs schema inference on CSV (extra job + wrong types)
  2. Lazy transformations vs actions: a chain of transformations launches 0 jobs; narrow vs wide in the plan
  3. Actions: count, first, take, collect, show, describe, toPandas
  4. Joins: inner, left outer, left semi, left anti, self-join (2-hop road graph), broadcast hint
  5. Window functions: lag, lead, row_number, rank / dense_rank, running total, time-range rolling average
  6. Spark SQL: temporary and global temporary views, catalog listing, three DataFrame/SQL query pairs whose
     optimized logical AND physical plans are compared programmatically, explain("formatted") saved to files

Usage (project root, env sourced):
    python -m src.spark_features.dataframe_sql            # full data
    python -m src.spark_features.dataframe_sql --quick    # one month
"""
from __future__ import annotations

import argparse
import io
import shutil
import sys
from contextlib import redirect_stdout
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.paths import PROCESSED_DIR, RAW_DIR, processed_dataset_dir, raw_dataset_dir
from src.preprocessing.congestion import CONGESTION_SPEED_MPH
from src.preprocessing.download_dataset import load_verified_manifest
from src.preprocessing.raw_reader import safe_load_adjacency
from src.spark_features.common import (
    BENCH_REPORT_DIR, BENCH_SCRATCH, QUICK_MONTH, SparkMetrics, base_args, bench_spark, environment, explain_text,
    log, md_table, measure, normalized_plan, plan_nodes, setup_logging, write_report,
)

THR = CONGESTION_SPEED_MPH

CLEAN_SCHEMA = T.StructType([
    T.StructField("sensor_id", T.IntegerType(), False),
    T.StructField("ts_utc", T.TimestampType(), False),
    T.StructField("ts_local", T.TimestampType(), True),
    T.StructField("speed_raw", T.FloatType(), True),
    T.StructField("speed_mph", T.FloatType(), True),
    T.StructField("quality", T.StringType(), True),
    T.StructField("issue", T.StringType(), True),
    T.StructField("date_local", T.DateType(), True),
    T.StructField("month", T.StringType(), True),
])
CSV_SCHEMA = T.StructType([f for f in CLEAN_SCHEMA.fields if f.name != "month"])


def _show(df: DataFrame, n: int = 10) -> str:
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.show(n, truncate=False)
    return buf.getvalue().rstrip()


def _rows(df: DataFrame, n: int = 10) -> list[dict]:
    return [r.asDict() for r in df.limit(n).collect()]


def _cols(rows: list[dict]) -> list[tuple[str, str]]:
    return [(k, k) for k in rows[0]] if rows else []


# ------------------------------------------------------------------------------------------- 1. schemas
def section_schemas(spark: SparkSession, clean_path: Path, quick: bool, metrics: SparkMetrics) -> tuple[DataFrame, dict]:
    clean = spark.read.schema(CLEAN_SCHEMA).parquet(str(clean_path))
    if quick:
        clean = clean.filter(F.col("month") == QUICK_MONTH)
    assert [(f.name, f.dataType) for f in clean.schema.fields] == [(f.name, f.dataType) for f in CLEAN_SCHEMA.fields]

    # CSV: schema inference needs an extra pass over the file and guesses types; an explicit schema does not.
    csv_path = BENCH_SCRATCH / "dataframe_sql" / "clean_sample_csv"
    sample = clean.filter(F.col("month") == QUICK_MONTH).drop("month")
    sample.write.mode("overwrite").option("header", True).csv(str(csv_path))
    inferred = measure(spark, "csv_read_inferSchema", lambda: spark.read.option("header", True)
                       .option("inferSchema", True).csv(str(csv_path)).count(), repeats=1, warmup=0, metrics=metrics)
    explicit = measure(spark, "csv_read_explicit_schema", lambda: spark.read.option("header", True)
                       .schema(CSV_SCHEMA).csv(str(csv_path)).count(), repeats=1, warmup=0, metrics=metrics)
    inferred_schema = spark.read.option("header", True).option("inferSchema", True).csv(str(csv_path)).schema
    type_diff = [{"column": e.name, "explicit": e.dataType.simpleString(), "inferred": i.dataType.simpleString()}
                 for e, i in zip(CSV_SCHEMA.fields, inferred_schema.fields)]
    return clean, {
        "parquet_schema_ddl": clean.schema.simpleString(),
        "csv_rows": inferred["result"],
        "csv_infer": {"seconds": inferred["seconds_median"], "jobs": inferred["metrics"]["jobs"],
                      "input_bytes": inferred["metrics"]["inputBytes"]},
        "csv_explicit": {"seconds": explicit["seconds_median"], "jobs": explicit["metrics"]["jobs"],
                         "input_bytes": explicit["metrics"]["inputBytes"]},
        "type_diff": type_diff,
    }


# ------------------------------------------------------------------------------------------- 2/3. lazy + actions
def section_lazy_and_actions(spark: SparkSession, clean: DataFrame, metrics: SparkMetrics) -> dict:
    holder = {}

    def build_chain():
        holder["df"] = (clean.filter(F.col("quality") == "observed")                 # narrow
                             .withColumn("hour", F.hour("ts_local"))                  # narrow
                             .withColumn("congested", (F.col("speed_mph") < THR).cast("int"))
                             .select("sensor_id", "hour", "congested", "speed_mph")   # narrow
                             .groupBy("sensor_id", "hour")                            # wide (shuffle)
                             .agg(F.avg("congested").alias("congested_rate"), F.avg("speed_mph").alias("avg_speed"))
                             .orderBy(F.desc("congested_rate")))                      # wide (range shuffle)
        return None

    lazy = measure(spark, "build_transformation_chain", build_chain, repeats=1, warmup=0, metrics=metrics)
    chain = holder["df"]
    act = measure(spark, "action_count_on_chain", chain.count, repeats=1, warmup=0, metrics=metrics)
    nodes = plan_nodes(chain)

    actions = {}
    actions["count"] = clean.count()
    first = clean.filter(F.col("quality") == "observed").select("sensor_id", "ts_local", "speed_mph").first()
    actions["first"] = {k: (round(v, 2) if isinstance(v, float) else str(v)) for k, v in first.asDict().items()}
    actions["take_3"] = [{k: (round(v, 3) if isinstance(v, float) else v) for k, v in r.asDict().items()} for r in chain.take(3)]
    actions["collect_quality"] = {r.quality: r["count"] for r in clean.groupBy("quality").count().collect()}
    actions["describe"] = _show(clean.select("speed_mph").describe())
    actions["show_chain"] = _show(chain, 5)
    pdf = chain.limit(5).toPandas()
    actions["toPandas_shape"] = list(pdf.shape)
    return {
        "lazy_seconds": lazy["seconds_median"], "lazy_jobs": lazy["metrics"]["jobs"],
        "action_seconds": act["seconds_median"], "action_jobs": act["metrics"]["jobs"],
        "action_shuffle_bytes": act["metrics"]["shuffleWriteBytes"], "chain_rows": act["result"],
        "physical_nodes": nodes, "exchanges": nodes.count("Exchange"),
        "actions": actions,
    }


# ------------------------------------------------------------------------------------------- 4. joins
def section_joins(spark: SparkSession, clean: DataFrame, sensors: DataFrame, edges: DataFrame) -> dict:
    observed = clean.filter(F.col("quality") == "observed")
    per_sensor = observed.groupBy("sensor_id").agg(
        F.avg((F.col("speed_mph") < THR).cast("int")).alias("congested_rate"), F.count(F.lit(1)).alias("readings"))
    out = {}

    inner = (per_sensor.join(sensors, "sensor_id", "inner")
             .select("sensor_id", "latitude", "longitude", F.round(F.col("congested_rate") * 100, 2).alias("congested_pct"))
             .orderBy(F.desc("congested_pct")))
    out["inner"] = {"rows": inner.count(), "top": _rows(inner, 5), "nodes": plan_nodes(inner)}

    month_events = (observed.filter(F.col("speed_mph") < THR).groupBy("sensor_id", "month")
                    .agg(F.count(F.lit(1)).alias("congested_readings")))
    first_month = month_events.agg(F.min("month")).first()[0]
    left = (sensors.join(month_events.filter(F.col("month") == first_month), "sensor_id", "left")
            .withColumn("congested_readings", F.coalesce("congested_readings", F.lit(0))))
    out["left_outer"] = {"rows": left.count(), "sensors_with_zero": left.filter("congested_readings = 0").count(),
                         "month": first_month}

    ever = observed.filter(F.col("speed_mph") < THR).select("sensor_id")
    out["left_semi"] = {"rows": sensors.join(ever, "sensor_id", "left_semi").count()}
    anti = sensors.join(ever, "sensor_id", "left_anti")
    out["left_anti"] = {"rows": anti.count(), "examples": [r.sensor_id for r in anti.orderBy("sensor_id").limit(5).collect()]}

    e1, e2 = edges.alias("e1"), edges.alias("e2")
    two_hop = (e1.join(e2, F.col("e1.dst") == F.col("e2.src"))
               .filter(F.col("e1.src") != F.col("e2.dst"))
               .select(F.col("e1.src").alias("origin"), F.col("e2.dst").alias("two_hops_downstream")).distinct())
    out["self_join"] = {"edges": edges.count(), "two_hop_pairs": two_hop.count()}

    bjoin = clean.join(F.broadcast(sensors.select("sensor_id", "latitude")), "sensor_id")
    sjoin = clean.hint("merge").join(sensors.select("sensor_id", "latitude").hint("merge"), "sensor_id")
    out["broadcast_hint"] = {"join_node": next((n for n in plan_nodes(bjoin) if n.endswith("Join")), "?")}
    out["merge_hint"] = {"join_node": next((n for n in plan_nodes(sjoin) if n.endswith("Join")), "?")}
    return out


# ------------------------------------------------------------------------------------------- 5. windows
def section_windows(spark: SparkSession, clean: DataFrame) -> dict:
    obs = clean.filter(F.col("quality") == "observed").select("sensor_id", "ts_utc", "ts_local", "date_local", "speed_mph")
    w = Window.partitionBy("sensor_id").orderBy("ts_utc")
    with_lags = (obs.withColumn("prev_speed", F.lag("speed_mph", 1).over(w))
                    .withColumn("next_speed", F.lead("speed_mph", 1).over(w))
                    .withColumn("delta_5m", F.col("speed_mph") - F.col("prev_speed")))
    onsets = with_lags.filter((F.col("speed_mph") < THR) & (F.col("prev_speed") >= THR))
    out = {"onsets_total": onsets.count()}

    sample_sensor = onsets.groupBy("sensor_id").count().orderBy(F.desc("count"), "sensor_id").first().sensor_id
    sample_day = (onsets.filter(F.col("sensor_id") == sample_sensor).groupBy("date_local").count()
                  .orderBy(F.desc("count"), "date_local").first().date_local)
    out["sample"] = {"sensor_id": sample_sensor, "date": str(sample_day)}
    day = with_lags.filter((F.col("sensor_id") == sample_sensor) & (F.col("date_local") == sample_day))
    out["lag_lead_rows"] = [{k: (round(v, 1) if isinstance(v, float) else str(v)) for k, v in r.asDict().items()}
                            for r in day.filter(F.col("prev_speed") >= THR).filter(F.col("speed_mph") < THR + 10)
                            .select("ts_local", "prev_speed", "speed_mph", "next_speed", "delta_5m").orderBy("ts_local").limit(6).collect()]

    w_day = Window.partitionBy("sensor_id", "date_local").orderBy("speed_mph", "ts_utc")
    slowest = (obs.withColumn("rn", F.row_number().over(w_day)).filter("rn <= 3")
               .filter((F.col("sensor_id") == sample_sensor) & (F.col("date_local") == sample_day))
               .select("rn", "ts_local", "speed_mph").orderBy("rn"))
    out["row_number_top3_slowest"] = [{k: (round(v, 1) if isinstance(v, float) else str(v)) for k, v in r.asDict().items()}
                                      for r in slowest.collect()]

    daily = (obs.groupBy("date_local", "sensor_id")
             .agg((F.sum((F.col("speed_mph") < THR).cast("int")) * 5).alias("congested_minutes")))
    w_rank = Window.partitionBy("date_local").orderBy(F.desc("congested_minutes"))
    ranked = (daily.withColumn("rank", F.rank().over(w_rank)).withColumn("dense_rank", F.dense_rank().over(w_rank))
              .filter(F.col("date_local") == sample_day).filter("rank <= 5").orderBy("rank", "sensor_id"))
    out["rank_dense_rank"] = [{k: str(v) for k, v in r.asDict().items()} for r in ranked.collect()]

    w_run = Window.partitionBy("sensor_id", "date_local").orderBy("ts_utc").rowsBetween(Window.unboundedPreceding, 0)
    w_30 = (Window.partitionBy("sensor_id").orderBy(F.col("ts_utc").cast("long")).rangeBetween(-30 * 60 + 1, 0))
    running = (obs.filter((F.col("sensor_id") == sample_sensor) & (F.col("date_local") == sample_day))
               .withColumn("cum_congested_min", F.sum((F.col("speed_mph") < THR).cast("int") * 5).over(w_run))
               .withColumn("roll_avg_30min", F.round(F.avg("speed_mph").over(w_30), 1))
               .select("ts_local", "speed_mph", "roll_avg_30min", "cum_congested_min").orderBy("ts_local"))
    rows = running.collect()
    out["running_and_range"] = [{k: (round(v, 1) if isinstance(v, float) else str(v)) for k, v in r.asDict().items()}
                                for r in rows[::max(1, len(rows) // 8)][:8]]
    out["daily_congested_minutes_sample_day"] = rows[-1].cum_congested_min if rows else 0
    return out


# ------------------------------------------------------------------------------------------- 6. SQL
def section_sql(spark: SparkSession, clean: DataFrame, sensors: DataFrame, plan_dir: Path) -> dict:
    clean.createOrReplaceTempView("clean")
    sensors.createOrReplaceTempView("sensors")
    sensors.createOrReplaceGlobalTempView("sensors_global")
    catalog = [{"name": t.name, "database": t.database, "temporary": t.isTemporary} for t in spark.catalog.listTables()]
    global_count = spark.sql("SELECT count(*) AS n FROM global_temp.sensors_global").first().n

    w = Window.partitionBy("sensor_id").orderBy("ts_utc")
    pairs = {
        "hourly_weekday_congestion": (
            clean.filter((F.col("quality") == "observed") & F.dayofweek("ts_local").between(2, 6))
                 .groupBy(F.hour("ts_local").alias("hour"))
                 .agg(F.round(F.avg((F.col("speed_mph") < 35).cast("int")) * 100, 2).alias("congested_pct"),
                      F.round(F.avg("speed_mph"), 2).alias("avg_speed"),
                      F.count(F.lit(1)).alias("readings"))
                 .orderBy("hour"),
            """SELECT hour(ts_local) AS hour,
                      round(avg(CAST(speed_mph < 35 AS INT)) * 100, 2) AS congested_pct,
                      round(avg(speed_mph), 2) AS avg_speed,
                      count(1) AS readings
               FROM clean
               WHERE quality = 'observed' AND dayofweek(ts_local) BETWEEN 2 AND 6
               GROUP BY hour(ts_local)
               ORDER BY hour"""),
        "congestion_onsets_window": (
            clean.select("sensor_id", "ts_utc", "speed_mph")
                 .withColumn("prev_speed", F.lag("speed_mph").over(w))
                 .filter((F.col("speed_mph") < 35) & (F.col("prev_speed") >= 35))
                 .groupBy("sensor_id").agg(F.count(F.lit(1)).alias("onsets"))
                 .orderBy(F.desc("onsets"), "sensor_id").limit(10),
            """SELECT sensor_id, count(1) AS onsets
               FROM (SELECT sensor_id, ts_utc, speed_mph,
                            LAG(speed_mph) OVER (PARTITION BY sensor_id ORDER BY ts_utc) AS prev_speed
                     FROM clean)
               WHERE speed_mph < 35 AND prev_speed >= 35
               GROUP BY sensor_id
               ORDER BY onsets DESC, sensor_id
               LIMIT 10"""),
        "most_congested_sensors_join": (
            clean.filter(F.col("quality") == "observed")
                 .groupBy("sensor_id").agg(F.avg((F.col("speed_mph") < 35).cast("int")).alias("rate"))
                 .join(sensors, "sensor_id")
                 .select("sensor_id", "latitude", "longitude", F.round(F.col("rate") * 100, 2).alias("congested_pct"))
                 .orderBy(F.desc("congested_pct"), "sensor_id").limit(5),
            """SELECT sensor_id, latitude, longitude, round(rate * 100, 2) AS congested_pct
               FROM (SELECT sensor_id, avg(CAST(speed_mph < 35 AS INT)) AS rate
                     FROM clean WHERE quality = 'observed' GROUP BY sensor_id) c
               JOIN sensors USING (sensor_id)
               ORDER BY congested_pct DESC, sensor_id
               LIMIT 5"""),
    }
    plan_dir.mkdir(parents=True, exist_ok=True)
    results = {}
    for name, (df, sql) in pairs.items():
        sdf = spark.sql(sql)
        same_opt = normalized_plan(df, "optimized") == normalized_plan(sdf, "optimized")
        same_phys = normalized_plan(df, "physical") == normalized_plan(sdf, "physical")
        rows_df, rows_sql = df.collect(), sdf.collect()
        (plan_dir / f"{name}__dataframe.txt").write_text(explain_text(df, "formatted"))
        (plan_dir / f"{name}__sql.txt").write_text(explain_text(sdf, "formatted"))
        results[name] = {"same_optimized_plan": same_opt, "same_physical_plan": same_phys,
                         "same_result": rows_df == rows_sql, "rows": len(rows_df),
                         "preview": [r.asDict() for r in rows_sql[:5]], "sql": " ".join(sql.split())}
        if not same_opt:
            (plan_dir / f"{name}__optimized_diff.txt").write_text(
                "DATAFRAME\n" + normalized_plan(df, "optimized") + "\n\nSQL\n" + normalized_plan(sdf, "optimized"))
        log.info("%-30s optimized plan equal: %s | physical plan equal: %s | same result: %s",
                 name, same_opt, same_phys, rows_df == rows_sql)

    q = pairs["hourly_weekday_congestion"][0]
    (plan_dir / "hourly_weekday_congestion__all_modes.txt").write_text(
        "\n\n".join(f"===== explain(mode='{m}') =====\n{explain_text(q, m)}" for m in ("simple", "extended", "cost", "codegen")))
    return {"catalog": catalog, "global_temp_rows": global_count, "pairs": results}


# ------------------------------------------------------------------------------------------- report
def render(r: dict, plan_dir_rel: str) -> str:
    s, lz, j, w, q = r["schemas"], r["lazy"], r["joins"], r["windows"], r["sql"]
    L = ["## 1. Explicit schemas", "",
         f"Parquet is read with an explicit `StructType`: `{s['parquet_schema_ddl']}`", "",
         f"CSV sample ({s['csv_rows']:,} rows, one month) read two ways:", "",
         md_table([{"mode": "inferSchema=true", "time_s": s["csv_infer"]["seconds"], "jobs": s["csv_infer"]["jobs"],
                    "input_bytes": s["csv_infer"]["input_bytes"]},
                   {"mode": "explicit schema", "time_s": s["csv_explicit"]["seconds"], "jobs": s["csv_explicit"]["jobs"],
                    "input_bytes": s["csv_explicit"]["input_bytes"]}],
                  [("mode", "Read mode"), ("time_s", "Seconds (read + count)"), ("jobs", "Spark jobs"), ("input_bytes", "Bytes read")]),
         "", "Types chosen by inference vs the explicit schema:", "",
         md_table(s["type_diff"], [("column", "Column"), ("explicit", "Explicit"), ("inferred", "Inferred")]),
         "", "## 2. Lazy evaluation: transformations vs actions", "",
         md_table([{"step": "build chain: filter → withColumn ×2 → select → groupBy/agg → orderBy", "time_s": lz["lazy_seconds"],
                    "jobs": lz["lazy_jobs"]},
                   {"step": "action: count()", "time_s": lz["action_seconds"], "jobs": lz["action_jobs"]}],
                  [("step", "Step"), ("time_s", "Seconds"), ("jobs", "Spark jobs launched")]),
         "", f"Physical operators: `{' → '.join(lz['physical_nodes'])}`. "
         f"`filter`/`withColumn`/`select` are **narrow** (pipelined into one stage). `groupBy` and `orderBy` are **wide**; "
         f"each adds an `Exchange` (shuffle). The plan has {lz['exchanges']} `Exchange` node(s).", "",
         "## 3. Actions", "",
         f"- `count()` → {lz['actions']['count']:,}", f"- `first()` → `{lz['actions']['first']}`",
         f"- `take(3)` → `{lz['actions']['take_3']}`", f"- `collect()` (quality counts) → `{lz['actions']['collect_quality']}`",
         f"- `toPandas()` shape of top-5 → `{lz['actions']['toPandas_shape']}`", "",
         "`describe()`:", "```", lz["actions"]["describe"], "```", "`show(5)` of the chain:", "```", lz["actions"]["show_chain"], "```",
         "", "## 4. Joins", "",
         md_table([
             {"join": "inner (per-sensor congestion ⋈ sensors)", "rows": j["inner"]["rows"],
              "note": f"physical: {', '.join(n for n in j['inner']['nodes'] if n.endswith('Join'))}"},
             {"join": f"left outer (sensors ⟕ congestion in {j['left_outer']['month']})", "rows": j["left_outer"]["rows"],
              "note": f"{j['left_outer']['sensors_with_zero']} sensors had no congestion → null → coalesce(0)"},
             {"join": "left semi (sensors ever congested)", "rows": j["left_semi"]["rows"], "note": "keeps left rows that match"},
             {"join": "left anti (sensors never congested)", "rows": j["left_anti"]["rows"],
              "note": f"e.g. {j['left_anti']['examples']}"},
             {"join": "self-join on road-graph edges (2 hops downstream)", "rows": j["self_join"]["two_hop_pairs"],
              "note": f"from {j['self_join']['edges']:,} directed edges"},
             {"join": "broadcast() hint", "rows": None, "note": f"physical join: {j['broadcast_hint']['join_node']}"},
             {"join": "merge hint", "rows": None, "note": f"physical join: {j['merge_hint']['join_node']}"},
         ], [("join", "Join"), ("rows", "Rows"), ("note", "Evidence")]),
         "", "Top 5 most congested sensors (inner join):", "", md_table(j["inner"]["top"], _cols(j["inner"]["top"])),
         "", "## 5. Window functions", "",
         f"Congestion **onsets** (speed drops below {THR:.0f} mph after being ≥ {THR:.0f} mph, found with `lag`): "
         f"**{w['onsets_total']:,}**. Sample: sensor {w['sample']['sensor_id']} on {w['sample']['date']}.", "",
         "`lag` / `lead` around onset:", "", md_table(w["lag_lead_rows"], _cols(w["lag_lead_rows"])),
         "", "`row_number()`: 3 slowest readings of the day:", "", md_table(w["row_number_top3_slowest"], _cols(w["row_number_top3_slowest"])),
         "", "`rank()` / `dense_rank()` of sensors by congested minutes that day:", "",
         md_table(w["rank_dense_rank"], _cols(w["rank_dense_rank"])),
         "", "Running total (`rowsBetween(unboundedPreceding, 0)`) and 30-minute **time-range** window "
         "(`rangeBetween` on epoch seconds), sampled through the day:", "",
         md_table(w["running_and_range"], _cols(w["running_and_range"])),
         "", "## 6. Spark SQL", "",
         f"Catalog: `{q['catalog']}` · global temp view `global_temp.sensors_global` rows: {q['global_temp_rows']}", "",
         md_table([{"query": k, "same_optimized_plan": v["same_optimized_plan"], "same_physical_plan": v["same_physical_plan"],
                    "same_result": v["same_result"], "rows": v["rows"],
                    "plans": f"[DF]({plan_dir_rel}/{k}__dataframe.txt) · [SQL]({plan_dir_rel}/{k}__sql.txt)"}
                   for k, v in q["pairs"].items()],
                  [("query", "Query"), ("same_optimized_plan", "Same optimized plan"), ("same_physical_plan", "Same physical plan"),
                   ("same_result", "Same result"), ("rows", "Rows"), ("plans", "explain('formatted')")]),
         "", "The DataFrame API and SQL are two front-ends to the **same Catalyst optimizer**. After analysis and "
         "optimization each pair produces an identical plan (expression IDs removed before comparing), so neither is faster.", "",
         f"All explain modes (simple, extended, cost, codegen) for the first query: [{plan_dir_rel}/hourly_weekday_congestion__all_modes.txt]"
         f"({plan_dir_rel}/hourly_weekday_congestion__all_modes.txt)", ""]
    for k, v in q["pairs"].items():
        L += [f"**{k}**: `{v['sql']}`", "", md_table(v["preview"], _cols(v["preview"])), ""]
    return "\n".join(L)


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    base_args(p)
    a = p.parse_args(argv)
    spark = bench_spark("trafficsense-dataframe-sql")
    metrics = SparkMetrics(spark)
    base = processed_dataset_dir("pems-bay", PROCESSED_DIR)
    plan_dir = a.report_dir / "plans"
    try:
        clean, schemas = section_schemas(spark, base / "clean", a.quick, metrics)
        sensors = spark.read.parquet(str(base / "sensors")).select("sensor_id", "latitude", "longitude")
        ds_raw = raw_dataset_dir("pems-bay", RAW_DIR)
        ids, _, adj = safe_load_adjacency(ds_raw / load_verified_manifest(ds_raw)["adjacency_pkl"])
        edges = spark.createDataFrame([(int(ids[i]), int(ids[j]), float(adj[i, j])) for i in range(len(ids))
                                       for j in range(len(ids)) if i != j and adj[i, j] > 0],
                                      "src int, dst int, weight double")
        log.info("1. schemas done; 2-3. lazy evaluation and actions")
        lazy = section_lazy_and_actions(spark, clean, metrics)
        log.info("4. joins")
        joins = section_joins(spark, clean, sensors, edges)
        log.info("5. window functions")
        windows = section_windows(spark, clean)
        log.info("6. Spark SQL and plan comparison")
        sql = section_sql(spark, clean, sensors, plan_dir)
        env = environment(spark)
    finally:
        spark.stop()
        shutil.rmtree(BENCH_SCRATCH / "dataframe_sql", ignore_errors=True)

    r = {"schemas": schemas, "lazy": lazy, "joins": joins, "windows": windows, "sql": sql,
         "data_description": f"cleaned PEMS-BAY readings ({'month ' + QUICK_MONTH if a.quick else 'all 6 months'}, "
                             f"{lazy['actions']['count']:,} rows)"}
    pairs = sql["pairs"]
    headline = [
        f"Schema inference on CSV cost {schemas['csv_infer']['jobs']} Spark jobs vs {schemas['csv_explicit']['jobs']} with an "
        f"explicit schema ({schemas['csv_infer']['seconds']:.1f}s vs {schemas['csv_explicit']['seconds']:.1f}s), and it guessed "
        f"different types for {sum(d['explicit'] != d['inferred'] for d in schemas['type_diff'])} column(s).",
        f"Lazy evaluation: building a 6-step transformation chain launched {lazy['lazy_jobs']} jobs in "
        f"{lazy['lazy_seconds'] * 1000:.0f} ms; the count() action launched {lazy['action_jobs']} job(s) and took {lazy['action_seconds']:.1f}s.",
        f"Joins: broadcast hint → {joins['broadcast_hint']['join_node']}, merge hint → {joins['merge_hint']['join_node']}; "
        f"{joins['left_anti']['rows']} sensor(s) were never congested (left anti), {joins['left_semi']['rows']} were (left semi).",
        f"Window functions found {windows['onsets_total']:,} congestion onsets with lag(); row_number, rank/dense_rank, "
        "running totals and 30-min rangeBetween windows demonstrated.",
        f"DataFrame vs SQL: {sum(v['same_optimized_plan'] for v in pairs.values())}/{len(pairs)} query pairs have identical "
        f"optimized plans, {sum(v['same_physical_plan'] for v in pairs.values())}/{len(pairs)} identical physical plans, "
        f"{sum(v['same_result'] for v in pairs.values())}/{len(pairs)} identical results.",
    ]
    path = write_report("dataframe_sql", "DataFrame API & Spark SQL demonstration", env, render(r, "plans"), r, headline, a.report_dir)
    log.info("report: %s", path)
    ok = all(v["same_result"] for v in pairs.values()) and lazy["lazy_jobs"] == 0
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
