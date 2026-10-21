"""Endpoints, names and tuning constants shared by the producer, the streaming application and the tools.

Everything can be overridden with environment variables (TRAFFICSENSE_KAFKA, TRAFFICSENSE_MONGO_URI, ...), so the same
code runs against the docker-compose services (defaults) or another cluster.
"""
from __future__ import annotations

import os
from pathlib import Path

from src.common.paths import REPORTS_DIR, SCRATCH_DIR

KAFKA_BOOTSTRAP = os.environ.get("TRAFFICSENSE_KAFKA", "localhost:9092")
TOPIC = os.environ.get("TRAFFICSENSE_TOPIC", "traffic.speed.events")
TOPIC_PARTITIONS = 3
MONGO_URI = os.environ.get("TRAFFICSENSE_MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("TRAFFICSENSE_MONGO_DB", "trafficsense")

# Event time semantics
SLOT_SECONDS = 300                         # PEMS-BAY reports every 5 minutes
SCHEMA_VERSION = 1
WATERMARK = "10 minutes"                   # tolerated event-time lateness (2 slots)
WATERMARK_SLOTS = 2
WINDOW = "15 minutes"                      # sliding window length ...
WINDOW_SLIDE = "5 minutes"                 # ... and slide
MAX_SPEED_MPH = 120.0                      # plausibility bound used to reject corrupt readings
FORWARD_FILL_SLOTS = 3                     # identical to Phase 3 cleaning (<= 15 min, causal)
HISTORY_SLOTS = 7 * 288 + 1                # 7 days of context: lag 1 week of the target slot
STATE_SHUFFLE_PARTITIONS = 8               # fixed: changing it invalidates the state in existing checkpoints

# Spark Kafka source (must match the Spark version, see requirements.txt / setup_toolchain.sh)
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9"

CHECKPOINT_ROOT = SCRATCH_DIR / "streaming" / "checkpoints"     # local ext4 disk: rename-atomic, fast fsync
METRICS_ROOT = REPORTS_DIR / "streaming"


def checkpoint_dir(run: str, query: str) -> Path:
    return CHECKPOINT_ROOT / run / query


# MongoDB collections (all writes are idempotent upserts keyed by _id)
COL_READINGS = "readings"                  # every accepted event (hot store; used to rebuild state after a restart)
COL_PREDICTIONS = "traffic_predictions"  # 5-minute-ahead congestion probability per sensor and slot (read by the API)
COL_SENSOR_WINDOWS = "sensor_windows"      # 15-min sliding windows per sensor
COL_NETWORK_WINDOWS = "network_windows"    # 15-min sliding windows over the whole network
COL_DEAD_LETTERS = "dead_letters"          # malformed / implausible events with the reason
COL_STATE = "stream_state"                 # {_id: "state"}: last finalized slot, last batch id, partition progress
COL_BATCH_LOG = "batch_log"                # one document per prediction micro-batch
COL_METRICS = "stream_metrics"             # StreamingQueryProgress documents (streaming_metrics.py)
