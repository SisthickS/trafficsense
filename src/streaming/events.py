"""The event contract: JSON encoding (producer) and explicit-schema parsing + validation (Spark).

Kafka value (UTF-8 JSON, key = sensor_id as text):
    {"schema_version": 1, "source": "pems-bay", "sensor_id": 400001, "event_ts": "2017-06-05T07:00:00Z",
     "speed_mph": 67.9, "produced_at_ms": 1760962345123}

`event_ts` is the ORIGINAL measurement time (UTC) of the dataset: replaying never rewrites it. `produced_at_ms` is the
wall-clock time the producer sent the event and is only used to measure end-to-end latency.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.paths import PROCESSED_DIR, processed_dataset_dir
from src.streaming.config import MAX_SPEED_MPH, SCHEMA_VERSION, SLOT_SECONDS

EVENT_SCHEMA = T.StructType([
    T.StructField("schema_version", T.IntegerType(), True),
    T.StructField("source", T.StringType(), True),
    T.StructField("sensor_id", T.IntegerType(), True),
    T.StructField("event_ts", T.StringType(), True),
    T.StructField("speed_mph", T.DoubleType(), True),
    T.StructField("produced_at_ms", T.LongType(), True),
    T.StructField("_corrupt_record", T.StringType(), True),
])
EVENT_COLUMNS = ["sensor_id", "event_ts", "speed_mph", "produced_at_ms"]


def encode_event(sensor_id: int, event_ts: datetime, speed_mph: float, produced_at_ms: int, source: str = "pems-bay") -> bytes:
    return json.dumps({"schema_version": SCHEMA_VERSION, "source": source, "sensor_id": int(sensor_id),
                       "event_ts": event_ts.strftime("%Y-%m-%dT%H:%M:%SZ"), "speed_mph": round(float(speed_mph), 3),
                       "produced_at_ms": int(produced_at_ms)}, separators=(",", ":")).encode()


def slot_of(ts: datetime) -> int:
    """Absolute 5-minute slot number of a naive-UTC datetime."""
    return int((ts - datetime(1970, 1, 1)).total_seconds()) // SLOT_SECONDS


def slot_start(slot: int) -> datetime:
    return datetime(1970, 1, 1) + timedelta(seconds=slot * SLOT_SECONDS)


def dataset_time_range(dataset: str = "pems-bay") -> tuple[str, str]:
    """Plausible event-time range = the dataset's own span +/- 1 day (guards the watermark against absurd timestamps)."""
    meta = json.loads((processed_dataset_dir(dataset, PROCESSED_DIR) / "features_meta.json").read_text())["boundaries"]["utc"]
    lo = datetime.strptime(meta["start"], "%Y-%m-%d %H:%M:%S") - timedelta(days=1)
    hi = datetime.strptime(meta["end"], "%Y-%m-%d %H:%M:%S") + timedelta(days=1)
    return lo.strftime("%Y-%m-%d %H:%M:%S"), hi.strftime("%Y-%m-%d %H:%M:%S")


def parse_events(raw: DataFrame, time_range: tuple[str, str]) -> DataFrame:
    """Kafka source frame (key, value, partition, offset, timestamp) -> typed columns + `reject_reason`.

    Nothing is dropped here: valid rows have reject_reason = NULL, every other row keeps its raw payload and a reason,
    so malformed input is auditable (dead_letters) instead of silently disappearing.
    """
    parsed = (raw.select(F.col("value").cast("string").alias("raw"), F.col("partition"), F.col("offset"),
                         F.col("timestamp").alias("kafka_ts"))
                 .withColumn("e", F.from_json("raw", EVENT_SCHEMA, {"mode": "PERMISSIVE",
                                                                    "columnNameOfCorruptRecord": "_corrupt_record"})))
    e = parsed["e"]
    ts = F.to_timestamp(e["event_ts"])
    lo, hi = F.lit(time_range[0]).cast("timestamp"), F.lit(time_range[1]).cast("timestamp")
    speed = e["speed_mph"]
    reason = (F.when(e.isNull() | e["_corrupt_record"].isNotNull() | (e["sensor_id"].isNull() & e["event_ts"].isNull() & speed.isNull()),
                     "malformed_json")
               .when(e["schema_version"] != SCHEMA_VERSION, "unsupported_schema_version")
               .when(e["sensor_id"].isNull(), "missing_sensor_id")
               .when(ts.isNull(), "bad_event_ts")
               .when((ts < lo) | (ts > hi), "event_ts_out_of_range")
               .when(speed.isNull() | F.isnan(speed), "missing_speed")
               .when((speed < 0) | (speed > MAX_SPEED_MPH), "speed_out_of_range"))
    return parsed.select(
        "raw", "partition", "offset", "kafka_ts",
        e["sensor_id"].alias("sensor_id"), ts.alias("event_ts"), speed.alias("speed_mph"),
        e["produced_at_ms"].alias("produced_at_ms"), reason.alias("reject_reason"))


def valid(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("reject_reason").isNull()).select("sensor_id", "event_ts", "speed_mph", "produced_at_ms",
                                                                 "partition", "offset")


def rejected(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("reject_reason").isNotNull()).select("raw", "reject_reason", "partition", "offset", "kafka_ts")
