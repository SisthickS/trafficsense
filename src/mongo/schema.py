"""Collection design: purpose, document shape, $jsonSchema validators and indexes - the single source of truth.

  sensor_metadata       one doc per sensor (_id = sensor_id). Analyst-editable fields (name, description, direction, lanes,
                        status, tags, coordinates) + read-only dataset `stats` refreshed by the loader. CRUD through the API.
  settings              analyst settings (_id = key): typed, validated values with defaults in the API registry. CRUD.
  congestion_hotspots   per-sensor hotspot class / persistence (Q5) and severity index CSBI (Q9). Derived by Spark, read-only.
  analysis_results      Q1-Q18: one `question` doc (findings, implications, metrics) + one `table` doc per result table. Read-only.
  model_evaluations     Phase 7 models and baselines: validation/test metrics, confusion matrices, costs, selection. Read-only.
  traffic_predictions   5-minute-ahead congestion probabilities written by the streaming job (_id = sensor|slot).
  stream_metrics        StreamingQueryProgress documents (+ run summaries) written by the progress listener.
  pipeline_runs         one doc per pipeline run / phase (analytics questions, training, evaluation, streaming, data loads).
  + streaming support: readings, sensor_windows, network_windows, dead_letters, late_events, stream_state, batch_log.

Validators protect the collections edited by humans (sensor_metadata, settings) at the database level, in addition to the
API's Pydantic validation; the high-rate streaming collections deliberately have none (write path latency, evolving fields).
`ensure_schema` is idempotent: it creates missing collections, refreshes validators (collMod) and creates missing indexes.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pymongo import ASCENDING as ASC
from pymongo import DESCENDING as DESC
from pymongo import GEOSPHERE, IndexModel
from pymongo.database import Database
from pymongo.errors import CollectionInvalid

from src.streaming import config as C

log = logging.getLogger("trafficsense.mongo")

SENSOR_METADATA = "sensor_metadata"
SETTINGS = "settings"
CONGESTION_HOTSPOTS = "congestion_hotspots"
ANALYSIS_RESULTS = "analysis_results"
MODEL_EVALUATIONS = "model_evaluations"
TRAFFIC_PREDICTIONS = C.COL_PREDICTIONS
STREAM_METRICS = C.COL_METRICS
PIPELINE_RUNS = "pipeline_runs"

SENSOR_STATUSES = ["active", "inactive", "maintenance", "decommissioned"]
DIRECTIONS = ["N", "S", "E", "W"]
RUN_STATUSES = ["succeeded", "failed", "running"]


@dataclass
class CollectionSpec:
    name: str
    description: str
    indexes: list[IndexModel] = field(default_factory=list)
    validator: dict | None = None
    level: str = "strict"


def _ix(keys: list[tuple], name: str, **kw) -> IndexModel:
    return IndexModel(keys, name=name, **kw)


def _schema(required: list[str], properties: dict) -> dict:
    return {"$jsonSchema": {"bsonType": "object", "required": required, "properties": properties}}


NUM = "number"
SPECS: list[CollectionSpec] = [
    CollectionSpec(
        SENSOR_METADATA, "Sensor registry: analyst-editable metadata + read-only dataset statistics",
        validator=_schema(
            ["sensor_id", "latitude", "longitude", "location", "status", "tags", "source", "created_at", "updated_at", "version"],
            {"sensor_id": {"bsonType": ["int", "long"], "minimum": 1},
             "name": {"bsonType": ["string", "null"], "maxLength": 120},
             "description": {"bsonType": ["string", "null"], "maxLength": 1000},
             "direction": {"enum": DIRECTIONS + [None]},
             "lanes": {"bsonType": ["int", "long", "null"], "minimum": 1, "maximum": 12},
             "status": {"enum": SENSOR_STATUSES},
             "tags": {"bsonType": "array", "maxItems": 20, "items": {"bsonType": "string", "maxLength": 32}},
             "latitude": {"bsonType": NUM, "minimum": -90, "maximum": 90},
             "longitude": {"bsonType": NUM, "minimum": -180, "maximum": 180},
             "location": {"bsonType": "object", "required": ["type", "coordinates"],
                          "properties": {"type": {"enum": ["Point"]}, "coordinates": {"bsonType": "array", "minItems": 2, "maxItems": 2}}},
             "source": {"enum": ["dataset", "api"]},
             "version": {"bsonType": ["int", "long"], "minimum": 1}}),
        indexes=[_ix([("sensor_id", ASC)], "sensor_id_unique", unique=True),
                 _ix([("status", ASC), ("sensor_id", ASC)], "status_sensor"),
                 _ix([("tags", ASC)], "tags"),
                 _ix([("source", ASC)], "source"),
                 _ix([("location", GEOSPHERE)], "location_2dsphere"),
                 _ix([("updated_at", DESC)], "updated_at_desc")]),
    CollectionSpec(
        SETTINGS, "Analyst settings (_id = key)",
        validator=_schema(["key", "value", "category", "created_at", "updated_at", "version"],
                          {"key": {"bsonType": "string", "pattern": r"^[a-z][a-z0-9_.-]{1,63}$"},
                           "category": {"bsonType": "string", "maxLength": 32},
                           "description": {"bsonType": ["string", "null"], "maxLength": 300},
                           "updated_by": {"bsonType": ["string", "null"], "maxLength": 64},
                           "version": {"bsonType": ["int", "long"], "minimum": 1}}),
        indexes=[_ix([("key", ASC)], "key_unique", unique=True), _ix([("category", ASC), ("key", ASC)], "category_key")]),
    CollectionSpec(
        CONGESTION_HOTSPOTS, "Hotspot class and severity per sensor (Spark Q5 + Q9)", level="moderate",
        validator=_schema(["sensor_id", "class", "rank"], {"sensor_id": {"bsonType": ["int", "long"]}, "class": {"bsonType": "string"},
                                                           "rank": {"bsonType": ["int", "long"], "minimum": 1}}),
        indexes=[_ix([("sensor_id", ASC)], "sensor_id_unique", unique=True), _ix([("rank", ASC)], "rank"),
                 _ix([("class", ASC), ("rank", ASC)], "class_rank"), _ix([("csbi_rank", ASC)], "csbi_rank"),
                 _ix([("persistence", DESC)], "persistence_desc"), _ix([("location", GEOSPHERE)], "location_2dsphere")]),
    CollectionSpec(
        ANALYSIS_RESULTS, "Q1-Q18 results: question documents and result tables", level="moderate",
        validator=_schema(["doc_type", "qid"], {"doc_type": {"enum": ["question", "table"]}, "qid": {"bsonType": "string"}}),
        indexes=[_ix([("doc_type", ASC), ("qid", ASC)], "type_qid"), _ix([("qid", ASC), ("name", ASC)], "qid_name")]),
    CollectionSpec(
        MODEL_EVALUATIONS, "Phase 7 model evaluations (models and baselines)", level="moderate",
        validator=_schema(["model", "label", "kind"], {"kind": {"enum": ["model", "baseline"]}}),
        indexes=[_ix([("model", ASC)], "model_unique", unique=True), _ix([("kind", ASC), ("is_best", DESC)], "kind_best")]),
    CollectionSpec(
        TRAFFIC_PREDICTIONS, "5-minute-ahead congestion predictions from the streaming job (_id = sensor|slot)",
        indexes=[_ix([("sensor_id", ASC), ("event_ts", DESC)], "sensor_event_ts"), _ix([("event_ts", DESC)], "event_ts_desc"),
                 _ix([("slot", DESC), ("probability", DESC)], "slot_probability"), _ix([("alert", ASC), ("slot", DESC)], "alert_slot")]),
    CollectionSpec(
        STREAM_METRICS, "StreamingQueryProgress documents and run summaries",
        indexes=[_ix([("query", ASC), ("timestamp", DESC)], "query_timestamp"), _ix([("run", ASC), ("query", ASC), ("batch_id", ASC)], "run_query_batch"),
                 _ix([("doc_type", ASC), ("run", ASC)], "type_run")]),
    CollectionSpec(
        PIPELINE_RUNS, "Pipeline run history", level="moderate",
        validator=_schema(["kind", "name", "status"], {"status": {"enum": RUN_STATUSES}}),
        indexes=[_ix([("kind", ASC), ("finished_at", DESC)], "kind_finished"), _ix([("status", ASC)], "status"), _ix([("name", ASC)], "name")]),
    # streaming support collections (indexes only)
    CollectionSpec(C.COL_READINGS, "Accepted events (state recovery)", indexes=[_ix([("slot", ASC)], "slot")]),
    CollectionSpec(C.COL_SENSOR_WINDOWS, "15-minute sliding windows per sensor",
                   indexes=[_ix([("sensor_id", ASC), ("window_start", DESC)], "sensor_window")]),
    CollectionSpec(C.COL_NETWORK_WINDOWS, "15-minute sliding windows, whole network", indexes=[_ix([("window_start", DESC)], "window_start_desc")]),
    CollectionSpec(C.COL_DEAD_LETTERS, "Rejected stream events", indexes=[_ix([("reason", ASC)], "reason")]),
    CollectionSpec("late_events", "Events rejected as late", indexes=[_ix([("slot", ASC)], "slot")]),
    CollectionSpec(C.COL_STATE, "Stream recovery state"),
    CollectionSpec(C.COL_BATCH_LOG, "Prediction micro-batch log"),
]
SPEC_BY_NAME = {s.name: s for s in SPECS}


def ensure_schema(db: Database) -> dict:
    """Create missing collections, refresh validators, create missing indexes. Safe to call on every start."""
    existing = set(db.list_collection_names())
    created, validators = [], []
    for spec in SPECS:
        if spec.name not in existing:
            try:
                kw = {"validator": spec.validator, "validationLevel": spec.level, "validationAction": "error"} if spec.validator else {}
                db.create_collection(spec.name, **kw)
                created.append(spec.name)
            except CollectionInvalid:                       # created concurrently by another process
                pass
        elif spec.validator:
            db.command("collMod", spec.name, validator=spec.validator, validationLevel=spec.level, validationAction="error")
        if spec.validator:
            validators.append(spec.name)
        if spec.indexes:
            db[spec.name].create_indexes(spec.indexes)
    report = {"collections": len(SPECS), "created": created, "validators": validators,
              "indexes": {s.name: sorted(i["name"] for i in db[s.name].list_indexes()) for s in SPECS if s.indexes}}
    log.info("schema ensured: %d collections (%d created), %d validators", len(SPECS), len(created), len(validators))
    return report
