"""Spark Structured Streaming application: Kafka -> parse -> watermark -> windows + 5-minute-ahead predictions -> MongoDB.

    python -m src.streaming.traffic_stream --run demo --trigger "processing:2 seconds"      # continuous
    python -m src.streaming.traffic_stream --run demo --trigger available-now               # drain the topic, then exit
    python -m src.streaming.traffic_stream --run demo --reset                               # wipe checkpoints + MongoDB db

Four queries share one SparkSession (each has its own checkpoint under <scratch>/streaming/checkpoints/<run>/):

  predictions     valid events -> watermark -> dropDuplicates -> foreachBatch(PredictionSink):
                  persist readings -> LiveFeatureEngine (event-time slot completeness) -> Phase 7 MLlib pipeline ->
                  upsert predictions + state document in MongoDB
  sensor_windows  valid events -> watermark -> dropDuplicates -> 15-min sliding windows (5-min slide) per sensor (append mode)
  network_windows same, over the whole network
  dead_letters    rejected events (malformed JSON, bad types, implausible values) with the reason

Exactly-once effect: the Kafka source + checkpoint make batches replayable (at-least-once sink), and every MongoDB write is an
idempotent upsert on a deterministic _id, so a re-run batch rewrites identical documents. Driver-side state (the feature
engine) is rebuilt after a restart from the historical store + the readings persisted in MongoDB (see PredictionSink.setup).
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import signal
import sys
import threading
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.spark import get_spark
from src.ml.config import ML_MODEL_DIR
from src.ml.model_io import ModelBundle, load_model, predict
from src.preprocessing.build_features import FEATURE_COLUMNS
from src.streaming import config as C
from src.streaming.events import dataset_time_range, parse_events, rejected, slot_start, valid
from src.streaming.live_features import LiveFeatureEngine
from src.streaming.mongo_store import MongoStore, reading_id
from src.streaming.reference import engine_inputs, history_before, to_slot
from src.streaming.streaming_metrics import attach_listener

log = logging.getLogger("trafficsense.stream")


# ------------------------------------------------------------------------------------------------ sinks
class PredictionSink:
    """foreachBatch target of the `predictions` query.

    Default (deterministic) mode finalizes a slot once every partition is `grace` slots past it. With `idle_seconds > 0`
    (real-time mode) a background thread additionally finalizes a slot as soon as `min_coverage` of the sensors reported and
    it has been quiet for `idle_seconds`: lower latency, but which events make it into a slot then depends on timing.
    """

    def __init__(self, store: MongoStore, bundle: ModelBundle, dataset: str, grace_slots: int, n_partitions: int,
                 idle_seconds: float = 0.0, min_coverage: float = 0.9):
        self.store, self.bundle, self.dataset = store, bundle, dataset
        self.grace, self.n_partitions = grace_slots, n_partitions
        self.idle_seconds, self.min_coverage = idle_seconds, min_coverage
        self.engine = None
        self.last_batch_id = -1
        self.spark = None
        self.lock = threading.RLock()
        self._inputs = engine_inputs(dataset)             # static inputs are loaded at startup, not inside the first micro-batch
        if idle_seconds > 0:
            threading.Thread(target=self._idle_loop, daemon=True, name="idle-finalizer").start()

    # -- real-time mode
    def _idle_loop(self) -> None:
        while True:
            time.sleep(0.5)
            with self.lock:
                if self.engine is None or self.spark is None:
                    continue
                try:
                    through = self.engine.settled_through(self.idle_seconds, self.min_coverage)
                    if through is None or through <= self.engine.final_slot:
                        continue
                    t0 = time.time()
                    finalized = self.engine.finalize(through)
                    feats = pd.concat([f for _, f in finalized], ignore_index=True)
                    n = self._score_and_store(self.spark, feats, -1, {}, "idle")
                    self.store.set_state(last_final_slot=int(self.engine.final_slot),
                                         partition_max={str(k): int(v) for k, v in self.engine.partition_max.items()})
                    log.info("idle-finalize: %d slots, %d predictions, %.2fs (coverage >= %.0f%%, quiet >= %.1fs)", len(finalized), n,
                             time.time() - t0, 100 * self.min_coverage, self.idle_seconds)
                except Exception:
                    log.exception("idle finalizer failed; state will be rebuilt from MongoDB")
                    self.engine = None

    # -- state bootstrap / recovery
    def setup(self, first_batch_pdf: pd.DataFrame) -> None:
        state = self.store.get_state()
        self.engine = LiveFeatureEngine(**self._inputs, grace_slots=self.grace, n_partitions=self.n_partitions)
        if state is None:
            first_slot = int(first_batch_pdf.slot.min())
            self.engine.load_history(history_before(first_slot, self.dataset), first_slot)
            self.store.set_state(first_slot=first_slot, last_final_slot=first_slot - 1, last_batch_id=-1, partition_max={})
            log.info("fresh start: stream begins at slot %d (%s)", first_slot, slot_start(first_slot))
        else:
            t0 = time.time()
            readings = self.store.readings_from(int(state["first_slot"]))
            self.engine.restore(history_before(int(state["first_slot"]), self.dataset), int(state["first_slot"]), readings,
                                int(state["last_final_slot"]), state.get("partition_max", {}))
            self.last_batch_id = int(state.get("last_batch_id", -1))
            log.info("RECOVERED state from MongoDB in %.1fs: last finalized slot %d (%s), %d persisted readings, %d pending slots, "
                     "last committed batch %d", time.time() - t0, self.engine.final_slot, slot_start(self.engine.final_slot),
                     len(readings), len(self.engine.pending), self.last_batch_id)

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        with self.lock:
            self.spark = batch_df.sparkSession
            try:
                self._process(batch_df, batch_id)
            except BaseException:
                self.engine = None        # in-memory state may be half-updated: force a rebuild from MongoDB, never reuse it
                raise

    def _process(self, batch_df: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        tm: dict[str, float] = {}
        lap = lambda name, since: tm.__setitem__(name, round(time.time() - since, 2)) or time.time()
        pdf = batch_df.toPandas()
        if pdf.empty:
            return
        t = lap("collect", t0)
        pdf["slot"] = to_slot(pdf.event_ts)
        pdf["produced_at_ms"] = pdf.produced_at_ms.fillna(0).astype("int64")
        if self.engine is None:
            self.setup(pdf)
            t = lap("setup", t)
        replayed = batch_id <= self.last_batch_id          # batch re-delivered after a crash that happened after our commit
        fin0 = self.engine.final_slot

        accepted, late, stats = self.engine.ingest(pdf)
        if len(accepted):
            self.store.upsert(C.COL_READINGS, ({"_id": reading_id(r.sensor_id, r.slot), "sensor_id": int(r.sensor_id), "slot": int(r.slot),
                                                "event_ts": r.event_ts.to_pydatetime(), "speed_mph": float(r.speed_mph),
                                                "produced_at_ms": int(r.produced_at_ms), "partition": int(r.partition),
                                                "offset": int(r.offset)} for r in accepted.itertuples(index=False)))
        if len(late) and not replayed:
            self.store.upsert("late_events", ({"_id": reading_id(r.sensor_id, r.slot), "sensor_id": int(r.sensor_id), "slot": int(r.slot),
                                               "event_ts": r.event_ts.to_pydatetime(), "batch_id": int(batch_id)}
                                              for r in late.itertuples(index=False)))
        t = lap("persist_readings", t)
        through = self.engine.ready_through()
        finalized = self.engine.finalize(through) if through is not None else []
        t = lap("features", t)

        n_pred = 0
        if finalized:
            feats = pd.concat([f for _, f in finalized], ignore_index=True)
            n_pred = self._score_and_store(batch_df.sparkSession, feats, batch_id, tm, "grace")
            t = time.time()
        _maybe_crash(batch_id)                                   # test hook: crash after the output, before the commit
        self.store.set_state(last_batch_id=int(batch_id), last_final_slot=int(self.engine.final_slot),
                             partition_max={str(k): int(v) for k, v in self.engine.partition_max.items()})
        self.last_batch_id = max(self.last_batch_id, int(batch_id))
        secs = time.time() - t0
        self.store.upsert(C.COL_BATCH_LOG, [{"_id": int(batch_id), "batch_id": int(batch_id), "events": int(len(pdf)),
                                             "accepted": stats["accepted"], "late": 0 if replayed else stats["late"],
                                             "unknown_sensor": stats["unknown_sensor"],
                                             "slots_finalized": len(finalized), "predictions": n_pred, "replayed": bool(replayed),
                                             "final_slot_before": fin0, "final_slot_after": int(self.engine.final_slot),
                                             "seconds": round(secs, 3), "stage_seconds": tm, "at": datetime.utcnow()}])
        log.info("batch %d: %d events (%d late), %d slots finalized, %d predictions, %.2fs %s%s", batch_id, len(pdf), stats["late"],
                 len(finalized), n_pred, secs, tm, " [replayed after recovery]" if replayed else "")

    def _score_and_store(self, spark: SparkSession, feats: pd.DataFrame, batch_id: int, tm: dict, finalized_by: str) -> int:
        t = time.time()
        cols = ["sensor_id", "slot"] + FEATURE_COLUMNS
        feats = feats.assign(null_features=feats[FEATURE_COLUMNS].isna().sum(axis=1),
                             slot_ready_ms=feats.groupby("slot").produced_at_ms.transform("max"))
        sdf = spark.createDataFrame(feats[cols])
        scored = predict(self.bundle, sdf).select("sensor_id", "slot", "p", "alert").toPandas()
        scored = scored.merge(feats[["sensor_id", "slot", "ts_utc", "speed", "is_imputed", "produced_at_ms", "null_features", "coverage",
                                     "slot_ready_ms"]], on=["sensor_id", "slot"])
        tm["score"] = round(time.time() - t, 2)
        t = time.time()
        now = datetime.utcnow()
        now_ms = int(time.time() * 1000)
        docs = []
        for r in scored.itertuples(index=False):
            ts = r.ts_utc.to_pydatetime()
            docs.append({"_id": reading_id(r.sensor_id, r.slot), "sensor_id": int(r.sensor_id), "slot": int(r.slot), "event_ts": ts,
                         "target_ts": ts + timedelta(seconds=C.SLOT_SECONDS), "speed_mph": float(r.speed),
                         "probability": float(r.p), "alert": bool(r.alert), "threshold": self.bundle.threshold,
                         "model": self.bundle.metadata.get("label", "best"), "is_imputed": bool(r.is_imputed),
                         "null_features": int(r.null_features), "batch_id": int(batch_id), "processed_at": now,
                         "coverage": float(r.coverage), "finalized_by": finalized_by,
                         "produced_at_ms": int(r.produced_at_ms),
                         # latency since this sensor's own event was produced / since the LAST event of its slot was produced
                         "latency_ms": int(now_ms - r.produced_at_ms) if r.produced_at_ms else None,
                         "slot_latency_ms": int(now_ms - r.slot_ready_ms) if r.slot_ready_ms else None})
        n = self.store.upsert(C.COL_PREDICTIONS, docs)
        tm["store"] = round(time.time() - t, 2)
        return n


def _maybe_crash(batch_id: int) -> None:
    """Fault-injection hook for the recovery test: die once, after outputs were written but before the batch is committed."""
    target, marker = os.environ.get("TRAFFICSENSE_CRASH_AT_BATCH"), os.environ.get("TRAFFICSENSE_CRASH_MARKER")
    if target is not None and marker and str(batch_id) == target and not os.path.exists(marker):
        open(marker, "w").write(f"crashed at batch {batch_id}\n")
        log.error("FAULT INJECTION: simulated crash in batch %d after the output write, before the state commit", batch_id)
        os._exit(137)


def _window_docs(pdf: pd.DataFrame, per_sensor: bool) -> list[dict]:
    docs = []
    for r in pdf.itertuples(index=False):
        start = r.window_start.to_pydatetime()
        d = {"window_start": start, "window_end": r.window_end.to_pydatetime(), "n": int(r.n), "avg_speed": float(r.avg_speed),
             "min_speed": float(r.min_speed), "congested_share": float(r.congested_n) / float(r.n), "updated_at": datetime.utcnow()}
        if per_sensor:
            d.update(_id=f"{r.sensor_id}|{start:%Y%m%dT%H%M}", sensor_id=int(r.sensor_id), max_speed=float(r.max_speed))
        else:
            d.update(_id=f"{start:%Y%m%dT%H%M}", sensors=int(r.sensors))
        docs.append(d)
    return docs


def window_sink(store: MongoStore, collection: str, per_sensor: bool):
    def process(batch_df: DataFrame, batch_id: int) -> None:
        pdf = batch_df.toPandas()
        n = store.upsert(collection, _window_docs(pdf, per_sensor))
        if n:
            log.info("%s batch %d: %d windows written", collection, batch_id, n)
    return process


def dead_letter_sink(store: MongoStore):
    def process(batch_df: DataFrame, batch_id: int) -> None:
        pdf = batch_df.toPandas()
        n = store.upsert(C.COL_DEAD_LETTERS, ({"_id": f"{r.partition}|{r.offset}", "raw": (r.raw or "")[:500], "reason": r.reject_reason,
                                               "partition": int(r.partition), "offset": int(r.offset), "kafka_ts": r.kafka_ts.to_pydatetime(),
                                               "batch_id": int(batch_id)} for r in pdf.itertuples(index=False)))
        if n:
            log.info("dead_letters batch %d: %d rejected events (%s)", batch_id, n, dict(pdf.reject_reason.value_counts()))
    return process


# ------------------------------------------------------------------------------------------------ plumbing
def kafka_source(spark: SparkSession, args) -> DataFrame:
    reader = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", args.bootstrap).option("subscribe", args.topic)
              .option("startingOffsets", args.starting_offsets).option("failOnDataLoss", "true"))
    if args.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
    return reader.load()


def trigger_kwargs(spec: str) -> dict:
    if spec == "available-now":
        return {"availableNow": True}
    if spec.startswith("processing:"):
        return {"processingTime": spec.split(":", 1)[1].strip()}
    raise SystemExit(f"--trigger must be 'available-now' or 'processing:<interval>' (e.g. 'processing:2 seconds'), got {spec!r}")


def start_queries(spark: SparkSession, args, store: MongoStore, bundle: ModelBundle) -> list:
    parsed = parse_events(kafka_source(spark, args), dataset_time_range(args.dataset))
    good = valid(parsed).withWatermark("event_ts", C.WATERMARK).dropDuplicates(["sensor_id", "event_ts"])
    trig = trigger_kwargs(args.trigger)
    congested = (F.col("speed_mph") < 35).cast("int")

    def writer(df: DataFrame, name: str, fn, mode: str = "append"):
        return (df.writeStream.queryName(name).outputMode(mode).foreachBatch(fn)
                  .option("checkpointLocation", str(C.checkpoint_dir(args.run, name))).trigger(**trig).start())

    sink = PredictionSink(store, bundle, args.dataset, args.grace_slots, args.partitions, args.idle_finalize_seconds, args.min_coverage)
    qs = {"predictions": lambda: writer(good, "predictions", sink),
          "sensor_windows": lambda: writer(
              good.groupBy(F.window("event_ts", C.WINDOW, C.WINDOW_SLIDE).alias("w"), "sensor_id")
                  .agg(F.count("*").alias("n"), F.avg("speed_mph").alias("avg_speed"), F.min("speed_mph").alias("min_speed"),
                       F.max("speed_mph").alias("max_speed"), F.sum(congested).alias("congested_n"))
                  .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "sensor_id", "n", "avg_speed",
                          "min_speed", "max_speed", "congested_n"),
              "sensor_windows", window_sink(store, C.COL_SENSOR_WINDOWS, True)),
          "network_windows": lambda: writer(
              good.groupBy(F.window("event_ts", C.WINDOW, C.WINDOW_SLIDE).alias("w"))
                  .agg(F.count("*").alias("n"), F.avg("speed_mph").alias("avg_speed"), F.min("speed_mph").alias("min_speed"),
                       F.sum(congested).alias("congested_n"), F.approx_count_distinct("sensor_id").alias("sensors"))
                  .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "n", "avg_speed", "min_speed",
                          "congested_n", "sensors"),
              "network_windows", window_sink(store, C.COL_NETWORK_WINDOWS, False)),
          "dead_letters": lambda: writer(rejected(parsed), "dead_letters", dead_letter_sink(store))}
    return [qs[name]() for name in args.queries]


def reset_run(args, store: MongoStore) -> None:
    shutil.rmtree(C.CHECKPOINT_ROOT / args.run, ignore_errors=True)
    store.drop_all()
    log.warning("reset: removed checkpoints of run %r and dropped MongoDB database %r", args.run, args.db)


def check_consistency(args, store: MongoStore) -> None:
    """Checkpoint and MongoDB must be reset together; a mismatch would silently skip or duplicate data."""
    has_ckpt = (C.CHECKPOINT_ROOT / args.run / "predictions").exists()
    if "predictions" not in args.queries:
        return
    has_state = store.get_state() is not None
    if has_ckpt != has_state:
        raise SystemExit(f"inconsistent restart state for run {args.run!r}: checkpoint exists={has_ckpt}, MongoDB state exists={has_state}. "
                         "Re-run with --reset to start clean (or restore the missing half).")


def warm_up(spark: SparkSession, bundle: ModelBundle) -> float:
    """Score one dummy row so the first real micro-batch does not pay the one-off JVM/codegen cost of the 7-stage pipeline."""
    t0 = time.time()
    row = {c: 0.0 for c in FEATURE_COLUMNS} | {"sensor_id": 0, "slot": 0, "speed": 60.0, "hour": 12, "day_of_week": 3}
    predict(bundle, spark.createDataFrame(pd.DataFrame([row]))).select("p").collect()
    return time.time() - t0


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run", default="default", help="checkpoint namespace (one per independent deployment / test run)")
    p.add_argument("--db", default=C.MONGO_DB)
    p.add_argument("--topic", default=C.TOPIC)
    p.add_argument("--bootstrap", default=C.KAFKA_BOOTSTRAP)
    p.add_argument("--partitions", type=int, default=C.TOPIC_PARTITIONS, help="topic partitions (slot completeness needs all of them)")
    p.add_argument("--dataset", default="pems-bay")
    p.add_argument("--model", default=str(ML_MODEL_DIR / "best"), help="Phase 7 model bundle")
    p.add_argument("--trigger", default="processing:2 seconds", help="'available-now' or 'processing:<interval>'")
    p.add_argument("--max-offsets-per-trigger", type=int, default=0, help="cap on events per micro-batch (0 = unlimited)")
    p.add_argument("--starting-offsets", default="earliest", help="only used when the checkpoint is new")
    p.add_argument("--grace-slots", type=int, default=1, help="a slot is predicted once all partitions are this many slots past it")
    p.add_argument("--idle-finalize-seconds", type=float, default=0.0,
                   help="REAL-TIME MODE (0 = off, deterministic): also predict a slot once --min-coverage of the sensors reported and it has been "
                        "quiet this long, instead of waiting for the next slot's events. Lower latency; slot contents then depend on timing.")
    p.add_argument("--min-coverage", type=float, default=0.9, help="sensor share required by --idle-finalize-seconds")
    p.add_argument("--queries", nargs="+", default=["predictions", "sensor_windows", "network_windows", "dead_letters"],
                   choices=["predictions", "sensor_windows", "network_windows", "dead_letters"])
    p.add_argument("--duration", type=float, default=0, help="stop after N seconds (continuous triggers); 0 = run until stopped")
    p.add_argument("--reset", action="store_true", help="delete this run's checkpoints and drop the MongoDB database first")
    p.add_argument("--metrics-dir", default=str(C.METRICS_ROOT / "metrics"))
    p.add_argument("--driver-memory", default="3g")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    store = MongoStore(db=args.db)
    if args.reset:
        reset_run(args, store)
    check_consistency(args, store)
    store.ensure_indexes()
    spark = get_spark("trafficsense-stream", driver_memory=args.driver_memory, shuffle_partitions=C.STATE_SHUFFLE_PARTITIONS,
                      extra_conf={"spark.jars.packages": C.KAFKA_PACKAGE, "spark.sql.streaming.metricsEnabled": "true",
                                  "spark.sql.execution.arrow.pyspark.enabled": "true"})
    bundle = load_model(args.model)
    log.info("model: %s, alert threshold %.3f (warm-up %.1fs)", bundle.metadata.get("label"), bundle.threshold, warm_up(spark, bundle))
    attach_listener(spark, store, args.metrics_dir, args.run)
    queries = start_queries(spark, args, store, bundle)
    stopping = []
    signal.signal(signal.SIGTERM, lambda *_: stopping.append(1))
    signal.signal(signal.SIGINT, lambda *_: stopping.append(1))
    log.info("started %d queries: %s (run %r, trigger %s)", len(queries), [q.name for q in queries], args.run, args.trigger)
    t0 = time.time()
    try:
        while any(q.isActive for q in queries):
            if stopping or (args.duration and time.time() - t0 > args.duration):
                break
            for q in queries:
                if q.exception() is not None:
                    raise RuntimeError(f"query {q.name} failed: {q.exception()}")
            time.sleep(0.5)
        for q in queries:                                           # available-now: let every query finish its last batch
            if not stopping and not args.duration:
                q.awaitTermination()
            elif q.isActive:
                q.stop()
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()
        store.close()
    log.info("stopped cleanly after %.1fs", time.time() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
