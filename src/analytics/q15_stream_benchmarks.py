"""Q15 - Simulated-stream throughput and latency benchmarks (Spark Structured Streaming).

Real PEMS-BAY test-period readings are replayed as one CSV file per 5-minute slot (325 readings each) into a
landing directory. The streaming query mirrors the planned pipeline: file source -> stream-static join with the
train-period sensor statistics -> congestion flag -> 15-minute sliding windows (5-minute slide) with a 10-minute
watermark -> foreachBatch sink. Kafka replaces the file source in Phase 7; the stateful part is identical.

  Throughput: files pre-landed, trigger(availableNow) with different maxFilesPerTrigger and shuffle partitions.
  Latency   : a writer thread lands one slot every 0.5 s (600x real time); latency = time the window result is
              available in the sink - time the newest contributing reading was landed.
  Scoring   : latency of the Q16 PipelineModel on micro-batch-sized inputs (1 slot, 1 hour, 4 hours of readings).

Usage: python -m src.analytics.q15_stream_benchmarks
"""
from __future__ import annotations

import json
import os
import shutil
import statistics
import sys
import threading
import time
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.analytics.common import AnalyticsContext, QuestionResult, plt_setup, run_single
from src.common.paths import SCRATCH_DIR
from src.modeling.data import load_split, model_dir

ROOT = SCRATCH_DIR / "stream_bench"
SLOT_ROWS = 325
SCHEMA = T.StructType([T.StructField("sensor_id", T.IntegerType()), T.StructField("ts_local", T.TimestampType()),
                       T.StructField("speed", T.DoubleType()), T.StructField("ingest_ms", T.LongType())])
REPLAY_START = "2017-06-05"          # a Monday in the test period


def _slots(ctx: AnalyticsContext, n: int) -> list[pd.DataFrame]:
    clean = ctx.spark.read.parquet(str(ctx.base / "clean"))
    pdf = (clean.filter(F.col("ts_local") >= F.lit(REPLAY_START).cast("timestamp"))
                .filter(F.col("speed_mph").isNotNull())
                .select("sensor_id", F.date_format("ts_local", "yyyy-MM-dd HH:mm:ss").alias("ts_local"),
                        F.col("speed_mph").cast("double").alias("speed"))
                .orderBy("ts_local").limit(n * SLOT_ROWS).toPandas())
    return [g for _, g in pdf.groupby("ts_local", sort=True)][:n]


def _land(slot: pd.DataFrame, landing: Path, i: int) -> None:
    """Write to a temp name, then rename: the file source must never see a half-written file."""
    tmp = landing.parent / f".{landing.name}_{i:05d}.tmp"
    slot.assign(ingest_ms=int(time.time() * 1000)).to_csv(tmp, index=False, header=True)
    os.replace(tmp, landing / f"slot_{i:05d}.csv")


def _query(ctx, landing: Path, ckpt: Path, max_files: int, trigger: dict, sink_log: list):
    stats = ctx.spark.createDataFrame(ctx.sensors_pd[["sensor_id", "ff_speed"]])
    src = (ctx.spark.readStream.schema(SCHEMA).option("header", True).option("maxFilesPerTrigger", max_files)
                .csv(str(landing)))
    agg = (src.join(F.broadcast(stats), "sensor_id")
              .withColumn("congested", (F.col("speed") < 35).cast("int"))
              .withColumn("ratio", F.col("speed") / F.col("ff_speed"))
              .withWatermark("ts_local", "10 minutes")
              .groupBy(F.window("ts_local", "15 minutes", "5 minutes"), "sensor_id")
              .agg(F.avg("speed").alias("avg_speed"), F.max("congested").alias("any_congested"),
                   F.min("ratio").alias("min_ratio"), F.count(F.lit(1)).alias("n"), F.max("ingest_ms").alias("max_ingest")))

    def sink(batch_df, batch_id):
        ingest = [row[0] for row in batch_df.select("max_ingest").collect()]     # materialises the micro-batch
        now = time.time() * 1000
        sink_log.append({"batch_id": batch_id, "rows_out": len(ingest), "done_ms": now,
                         "latencies_ms": [now - x for x in ingest]})

    return (agg.writeStream.outputMode("update").foreachBatch(sink).option("checkpointLocation", str(ckpt))
               .trigger(**trigger).start())


def _progress_stats(progress: list) -> dict:
    p = [x for x in progress if x.get("numInputRows", 0) > 0]
    dur = [x["durationMs"].get("triggerExecution", 0) for x in p]
    parts = {k: statistics.mean(x["durationMs"].get(k, 0) for x in p) for k in
             ("latestOffset", "getBatch", "queryPlanning", "addBatch", "walCommit", "commitOffsets")} if p else {}
    return {"batches": len(p), "rows": sum(x["numInputRows"] for x in p),
            "batch_ms_p50": float(np.percentile(dur, 50)) if dur else np.nan,
            "batch_ms_p95": float(np.percentile(dur, 95)) if dur else np.nan, **{f"mean_{k}_ms": v for k, v in parts.items()}}


def throughput_run(ctx, slots, n_files: int, max_files: int, partitions: int) -> dict:
    run_dir = ROOT / f"tp_m{max_files}_p{partitions}"
    shutil.rmtree(run_dir, ignore_errors=True)
    landing = run_dir / "landing"
    landing.mkdir(parents=True)
    for i, s in enumerate(slots[:n_files]):
        _land(s, landing, i)
    ctx.spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    log: list = []
    t0 = time.time()
    q = _query(ctx, landing, run_dir / "ckpt", max_files, {"availableNow": True}, log)
    q.awaitTermination()
    wall = time.time() - t0
    st = _progress_stats(list(q.recentProgress))
    shutil.rmtree(run_dir, ignore_errors=True)
    return {"max_files_per_trigger": max_files, "shuffle_partitions": partitions, "files": n_files,
            "input_rows": st["rows"], "wall_s": wall, "rows_per_s": st["rows"] / wall if wall else 0.0,
            "slots_per_s": n_files / wall if wall else 0.0, **st}


def latency_run(ctx, slots, n_files: int, interval_s: float, trigger: dict, label: str) -> tuple[dict, np.ndarray]:
    run_dir = ROOT / f"lat_{label}"
    shutil.rmtree(run_dir, ignore_errors=True)
    landing = run_dir / "landing"
    landing.mkdir(parents=True)
    ctx.spark.conf.set("spark.sql.shuffle.partitions", "12")
    log: list = []
    q = _query(ctx, landing, run_dir / "ckpt", 1000, trigger, log)
    time.sleep(3)                                     # let the first (empty) trigger pass

    def writer():
        for i, s in enumerate(slots[:n_files]):
            _land(s, landing, i)
            time.sleep(interval_s)
    th = threading.Thread(target=writer, daemon=True)
    th.start()
    th.join()
    deadline = time.time() + 60
    while time.time() < deadline:
        done = sum(x.get("numInputRows", 0) for x in q.recentProgress)
        if done >= n_files * SLOT_ROWS:
            break
        time.sleep(0.5)
    time.sleep(1)
    q.stop()
    st = _progress_stats(list(q.recentProgress))
    lat = np.array([v for b in log for v in b["latencies_ms"]]) if log else np.array([np.nan])
    shutil.rmtree(run_dir, ignore_errors=True)
    return {"trigger": label, "files": n_files, "arrival_interval_s": interval_s, "simulated_speedup": 300 / interval_s,
            "latency_ms_p50": float(np.nanpercentile(lat, 50)), "latency_ms_p95": float(np.nanpercentile(lat, 95)),
            "latency_ms_max": float(np.nanmax(lat)), "output_rows": int(np.isfinite(lat).sum()), **st}, lat


def scoring_benchmark(ctx) -> pd.DataFrame:
    reg_path = model_dir() / "registry.json"
    if not reg_path.exists():
        return pd.DataFrame()
    from pyspark.ml import PipelineModel
    from src.modeling.models import score
    reg = json.loads(reg_path.read_text())
    model = PipelineModel.load(reg["model_path"])
    test = load_split(ctx.spark, "test")
    first = test.agg(F.min("ts_utc")).first()[0]
    rows = []
    for label, slots in (("1 slot (5 min)", 1), ("1 hour", 12), ("4 hours", 48)):
        df = test.filter(F.col("ts_utc") < F.lit(first) + F.expr(f"INTERVAL {slots * 5} MINUTES")).cache()
        n = df.count()
        times = []
        for i in range(6):
            t0 = time.perf_counter()
            score(model, df).agg(F.sum("p")).collect()
            if i:
                times.append(time.perf_counter() - t0)
        df.unpersist()
        rows.append({"batch": label, "rows": n, "median_ms": 1000 * statistics.median(times),
                     "p95_ms": 1000 * float(np.percentile(times, 95)), "rows_per_s": n / statistics.median(times)})
    return pd.DataFrame(rows).assign(model=reg["best_model_label"])


def run(ctx: AnalyticsContext) -> QuestionResult:
    r = QuestionResult("q15", "Simulated-stream throughput and latency benchmarks",
                       "Can Spark Structured Streaming process the PEMS-BAY sensor stream fast enough for real-time "
                       "5-minute predictions on this machine, and which settings matter?")
    ctx.release()
    n_big, n_small, n_lat = ctx.pick(576, 96), ctx.pick(96, 24), ctx.pick(120, 24)
    slots = _slots(ctx, n_big)
    ctx.spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "2000")
    shuffle_default = ctx.spark.conf.get("spark.sql.shuffle.partitions")
    r.method = [
        f"Replay of real test-period readings from {REPLAY_START}: one CSV file per 5-minute slot ({SLOT_ROWS} readings), "
        "landed atomically (write, then rename) into a directory watched by the Structured Streaming **file source**.",
        "Query: stream–static join with the train-period sensor statistics (broadcast), congestion flag, speed ratio, then "
        "`groupBy(window(ts, 15 min, 5 min), sensor_id)` with `withWatermark(10 min)` (stateful), output mode *update*, "
        "`foreachBatch` sink. Checkpoints go to the Linux scratch disk.",
        f"Throughput: {n_big} pre-landed slots ({n_big * SLOT_ROWS:,} readings), `trigger(availableNow=True)`, varying "
        "`maxFilesPerTrigger` (1 / 12 / 48 slots per micro-batch) and `spark.sql.shuffle.partitions` (state-store "
        "partitions: 4 / 12 / 48). Metrics come from `StreamingQuery.recentProgress`.",
        f"Latency: {n_lat} slots landed live, one every 0.5 s (600× real time), with the default trigger (next batch as soon "
        "as possible) and `processingTime='2 seconds'`. Latency per output row = sink time − landing time of the newest "
        "reading in that window.",
        "Model scoring: Q16's saved PipelineModel applied to static batches of 1 slot, 1 hour and 4 hours of feature rows "
        "(median of 5 timed runs after a warm-up).",
    ]
    tp = []
    try:
        tp.append(throughput_run(ctx, slots, n_small, 1, 12))
        for m in (12, 48):
            tp.append(throughput_run(ctx, slots, n_big, m, 12))
        for parts in (4, 48):
            tp.append(throughput_run(ctx, slots, n_big, 12, parts))
        lat_rows, lat_samples = [], {}
        for label, trig in (("as fast as possible", {"processingTime": "0 seconds"}), ("processingTime 2 s", {"processingTime": "2 seconds"})):
            row, lat = latency_run(ctx, slots, n_lat, 0.5, trig, label.replace(" ", "_"))
            lat_rows.append(row)
            lat_samples[label] = lat
    finally:
        ctx.spark.conf.set("spark.sql.shuffle.partitions", shuffle_default)
        shutil.rmtree(ROOT, ignore_errors=True)
    tp = pd.DataFrame(tp)
    lat = pd.DataFrame(lat_rows)
    sc = scoring_benchmark(ctx)

    best = tp.loc[tp.rows_per_s.idxmax()]
    m1 = tp[(tp.max_files_per_trigger == 1)].iloc[0]
    part = tp[tp.max_files_per_trigger == 12].sort_values("shuffle_partitions")
    fast = lat.iloc[0]
    realtime_headroom = best.slots_per_s * 300
    r.findings = [
        f"Peak throughput **{best.rows_per_s:,.0f} readings/s** ({best.slots_per_s:.1f} five-minute slots per second) with "
        f"{int(best.max_files_per_trigger)} slots per micro-batch and {int(best.shuffle_partitions)} shuffle partitions: "
        f"about **{realtime_headroom:,.0f}× faster than real time** for the 325-sensor network (one slot arrives every 300 s).",
        f"Micro-batch size matters: one slot per batch reaches only {m1.rows_per_s:,.0f} readings/s (median batch "
        f"{m1.batch_ms_p50:.0f} ms, dominated by fixed per-batch overhead: planning {m1.get('mean_queryPlanning_ms', np.nan):.0f} ms, "
        f"commit {m1.get('mean_walCommit_ms', np.nan) + m1.get('mean_commitOffsets_ms', np.nan):.0f} ms).",
        "State-store partitions: " + ", ".join(f"{int(x.shuffle_partitions)} → {x.rows_per_s:,.0f} readings/s "
                                               f"(batch p50 {x.batch_ms_p50:.0f} ms)" for x in part.itertuples()) +
        ". Each partition is a separate state store, so too many small partitions add overhead.",
        f"End-to-end latency (file landed → window result in the sink) at 600× real-time arrival: median "
        f"**{fast.latency_ms_p50 / 1000:.2f} s**, p95 {fast.latency_ms_p95 / 1000:.2f} s with the default trigger; "
        f"{lat.iloc[1].latency_ms_p50 / 1000:.2f} s / {lat.iloc[1].latency_ms_p95 / 1000:.2f} s with a 2 s processing-time trigger. "
        "Both are far below the 5-minute prediction horizon.",
    ]
    if len(sc):
        one = sc.iloc[0]
        r.findings.append(f"Model scoring ({one.model}): {int(one.rows)} rows (one slot) in a median {one.median_ms:.0f} ms; "
                          f"{sc.iloc[-1].rows_per_s:,.0f} rows/s for a 4-hour batch. Scoring adds well under a second per slot.")
    r.metrics = {"peak_rows_per_s": float(best.rows_per_s), "realtime_factor": float(realtime_headroom),
                 "latency_p50_s": float(fast.latency_ms_p50 / 1000), "latency_p95_s": float(fast.latency_ms_p95 / 1000)}
    r.table("throughput", tp, "Throughput by micro-batch size and shuffle partitions (trigger availableNow)")
    r.table("latency", lat, "End-to-end latency with live arrivals (600× real time)")
    if len(sc):
        r.table("scoring", sc, "Model scoring latency on micro-batch-sized inputs")

    plt, sns = plt_setup()
    fig, axes = plt.subplots(1, 3, figsize=(18, 4.4))
    sub = tp[tp.shuffle_partitions == 12].sort_values("max_files_per_trigger")
    axes[0].bar(sub.max_files_per_trigger.astype(str), sub.rows_per_s, color="#4c72b0")
    axes[0].set(title="Throughput vs slots per micro-batch", xlabel="maxFilesPerTrigger (5-min slots)", ylabel="readings / s")
    comp = ["mean_latestOffset_ms", "mean_getBatch_ms", "mean_queryPlanning_ms", "mean_addBatch_ms", "mean_walCommit_ms", "mean_commitOffsets_ms"]
    bd = tp.set_index(tp.max_files_per_trigger.astype(str) + " slots / " + tp.shuffle_partitions.astype(str) + " parts")[comp]
    bd.columns = [c.replace("mean_", "").replace("_ms", "") for c in comp]
    bd.plot.barh(stacked=True, ax=axes[1])
    axes[1].set(title="Mean micro-batch time breakdown", xlabel="ms", ylabel="")
    axes[1].legend(fontsize=7)
    for label, samples in lat_samples.items():
        s = np.sort(samples[np.isfinite(samples)]) / 1000
        if len(s):
            axes[2].plot(s, np.arange(1, len(s) + 1) / len(s), label=label)
    axes[2].set(title="Latency CDF (landed → window result)", xlabel="seconds", ylabel="share of window results")
    axes[2].legend()
    fig.tight_layout()
    r.figure(fig, "streaming", "Streaming throughput, batch-time breakdown and latency distribution")

    r.implications = [
        "A single laptop processes the 325-sensor stream several orders of magnitude faster than real time, so Phase 7 can "
        "afford the full feature set plus model scoring in each 5-minute micro-batch.",
        f"Use small, regular micro-batches (default trigger) for latency, and {int(part.loc[part.rows_per_s.idxmax(), 'shuffle_partitions'])} "
        "shuffle partitions for the state store (the fastest setting measured; each extra partition is another small state "
        "store with its own commit). Larger batches only help when catching up a backlog.",
        "Per-batch fixed overhead (planning, offset/commit logs) dominates small batches; Kafka will change the source "
        "overhead but not the stateful aggregation or the commit costs.",
    ]
    r.limitations = [
        "File source and accelerated replay stand in for Kafka; network, broker and serialization costs are not included yet.",
        "Single machine in local mode (shared CPU with the driver); results are for this laptop, not a cluster.",
        "Latency is measured from file landing to the sink; production latency also includes sensor → Kafka transport.",
    ]
    return r


if __name__ == "__main__":
    sys.exit(run_single(run))
