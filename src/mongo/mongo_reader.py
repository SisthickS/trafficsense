"""Read side of the MongoDB layer: filters, pagination, geo queries and aggregations used by the API."""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any

from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database

from src.mongo import schema as S
from src.mongo.settings_defs import KNOWN
from src.streaming import config as C

EARTH_RADIUS_M = 6_378_100.0
SENSOR_SORTS = {"sensor_id", "name", "status", "updated_at", "created_at"}
HOTSPOT_SORTS = {"rank", "csbi_rank", "persistence", "mean_daily_congested_min", "csbi", "sensor_id"}


class MongoReader:
    def __init__(self, db: Database):
        self.db = db

    # ------------------------------------------------------------------------------------------------ generic
    def ping(self) -> None:
        self.db.client.admin.command("ping")

    def _page(self, collection: str, filt: dict, sort: list[tuple], limit: int, offset: int, projection: dict | None = None) -> tuple[list[dict], int]:
        col = self.db[collection]
        items = list(col.find(filt, projection).sort(sort).skip(offset).limit(limit))
        return items, col.count_documents(filt)

    def counts(self) -> dict[str, int]:
        names = [S.SENSOR_METADATA, S.SETTINGS, S.CONGESTION_HOTSPOTS, S.ANALYSIS_RESULTS, S.MODEL_EVALUATIONS, S.TRAFFIC_PREDICTIONS,
                 S.STREAM_METRICS, S.PIPELINE_RUNS]
        return {n: self.db[n].estimated_document_count() for n in names}

    # ------------------------------------------------------------------------------------------------ sensors
    def get_sensor(self, sensor_id: int) -> dict | None:
        return self.db[S.SENSOR_METADATA].find_one({"_id": sensor_id})

    def list_sensors(self, status: str | None = None, tag: str | None = None, source: str | None = None, q: str | None = None,
                     bbox: tuple[float, float, float, float] | None = None, sort: str = "sensor_id", descending: bool = False,
                     limit: int = 50, offset: int = 0) -> tuple[list[dict], int]:
        filt: dict[str, Any] = {}
        if status:
            filt["status"] = status
        if tag:
            filt["tags"] = tag
        if source:
            filt["source"] = source
        if q:
            rx = {"$regex": re.escape(q), "$options": "i"}
            filt["$or"] = [{"name": rx}, {"description": rx}] + ([{"sensor_id": int(q)}] if q.isdigit() else [])
        if bbox:
            min_lon, min_lat, max_lon, max_lat = bbox
            filt["location"] = {"$geoWithin": {"$box": [[min_lon, min_lat], [max_lon, max_lat]]}}
        sort_key = sort if sort in SENSOR_SORTS else "sensor_id"
        return self._page(S.SENSOR_METADATA, filt, [(sort_key, DESCENDING if descending else ASCENDING), ("sensor_id", ASCENDING)], limit, offset)

    def sensors_near(self, lat: float, lon: float, radius_m: float, limit: int = 50) -> list[dict]:
        """Nearest first ($geoNear uses the 2dsphere index and returns the distance in metres)."""
        return list(self.db[S.SENSOR_METADATA].aggregate([
            {"$geoNear": {"near": {"type": "Point", "coordinates": [lon, lat]}, "distanceField": "distance_m", "maxDistance": radius_m,
                          "spherical": True}},
            {"$limit": limit}]))

    def sensor_status_counts(self) -> dict[str, int]:
        return {r["_id"]: r["n"] for r in self.db[S.SENSOR_METADATA].aggregate([{"$group": {"_id": "$status", "n": {"$sum": 1}}}])}

    # ------------------------------------------------------------------------------------------------ settings
    def get_setting_doc(self, key: str) -> dict | None:
        return self.db[S.SETTINGS].find_one({"_id": key})

    def effective_setting(self, key: str) -> Any:
        """Stored value if the setting was changed, else the registry default."""
        doc = self.get_setting_doc(key)
        if doc is not None:
            return doc["value"]
        return KNOWN[key].default

    def effective_settings(self) -> list[dict]:
        stored = {d["_id"]: d for d in self.db[S.SETTINGS].find()}
        out = [self.describe_setting(k, stored.get(k)) for k in KNOWN]
        out += [self.describe_setting(k, d) for k, d in sorted(stored.items()) if k not in KNOWN]
        return out

    @staticmethod
    def describe_setting(key: str, doc: dict | None) -> dict:
        d = KNOWN.get(key)
        base = {"key": key, "known": d is not None, "category": d.category if d else (doc or {}).get("category", "custom"),
                "description": (doc or {}).get("description") or (d.description if d else None)}
        if d:
            base.update(type=d.type, default=d.default, minimum=d.minimum, maximum=d.maximum, choices=list(d.choices) if d.choices else None)
        else:
            base.update(type="json", default=None, minimum=None, maximum=None, choices=None)
        if doc is None:
            return {**base, "value": d.default, "is_default": True, "version": None, "created_at": None, "updated_at": None, "updated_by": None}
        return {**base, "value": doc["value"], "is_default": False, "version": doc["version"], "created_at": doc["created_at"],
                "updated_at": doc["updated_at"], "updated_by": doc.get("updated_by")}

    # ------------------------------------------------------------------------------------------------ hotspots
    def get_hotspot(self, sensor_id: int) -> dict | None:
        return self.db[S.CONGESTION_HOTSPOTS].find_one({"_id": sensor_id})

    def list_hotspots(self, cls: str | None = None, min_persistence: float | None = None, near: tuple[float, float, float] | None = None,
                      sort: str = "rank", descending: bool = False, limit: int = 50, offset: int = 0) -> tuple[list[dict], int]:
        filt: dict[str, Any] = {}
        if cls:
            filt["class"] = cls
        if min_persistence is not None:
            filt["persistence"] = {"$gte": min_persistence}
        if near:
            lat, lon, radius_m = near
            filt["location"] = {"$geoWithin": {"$centerSphere": [[lon, lat], radius_m / EARTH_RADIUS_M]}}
        sort_key = sort if sort in HOTSPOT_SORTS else "rank"
        return self._page(S.CONGESTION_HOTSPOTS, filt, [(sort_key, DESCENDING if descending else ASCENDING), ("sensor_id", ASCENDING)], limit, offset)

    def hotspot_classes(self) -> list[dict]:
        return [{"class": r["_id"], "sensors": r["n"], "mean_persistence": r["p"], "mean_daily_congested_min": r["m"]}
                for r in self.db[S.CONGESTION_HOTSPOTS].aggregate([
                    {"$group": {"_id": "$class", "n": {"$sum": 1}, "p": {"$avg": "$persistence"}, "m": {"$avg": "$mean_daily_congested_min"}}},
                    {"$sort": {"p": -1}}])]

    # ------------------------------------------------------------------------------------------------ predictions
    def latest_slot(self) -> int | None:
        d = self.db[S.TRAFFIC_PREDICTIONS].find_one({}, {"slot": 1}, sort=[("slot", DESCENDING)])
        return int(d["slot"]) if d else None

    def latest_predictions(self, slot: int | None = None, min_probability: float | None = None, limit: int = 50, offset: int = 0
                           ) -> tuple[list[dict], int, int | None]:
        slot = self.latest_slot() if slot is None else slot
        if slot is None:
            return [], 0, None
        filt: dict[str, Any] = {"slot": slot}
        if min_probability is not None:
            filt["probability"] = {"$gte": min_probability}
        items, total = self._page(S.TRAFFIC_PREDICTIONS, filt, [("probability", DESCENDING), ("sensor_id", ASCENDING)], limit, offset)
        return items, total, slot

    def sensor_predictions(self, sensor_id: int, since: datetime | None = None, until: datetime | None = None, limit: int = 100) -> list[dict]:
        filt: dict[str, Any] = {"sensor_id": sensor_id}
        if since or until:
            filt["event_ts"] = {**({"$gte": since} if since else {}), **({"$lte": until} if until else {})}
        return list(self.db[S.TRAFFIC_PREDICTIONS].find(filt).sort("event_ts", DESCENDING).limit(limit))

    def newest_prediction_time(self) -> datetime | None:
        d = self.db[S.TRAFFIC_PREDICTIONS].find_one({}, {"event_ts": 1}, sort=[("event_ts", DESCENDING)])
        return d["event_ts"] if d else None

    def prediction_slots(self, threshold: float) -> list[dict]:
        """Every slot with predictions (for playback): sensors, alerts at `threshold`, mean / max probability."""
        alert = {"$sum": {"$cond": [{"$gte": ["$probability", threshold]}, 1, 0]}}
        return [{"slot": r["_id"], "event_ts": r["event_ts"], "predictions": r["n"], "alerts": r["alerts"], "mean_probability": r["mean"],
                 "max_probability": r["max"]}
                for r in self.db[S.TRAFFIC_PREDICTIONS].aggregate([
                    {"$group": {"_id": "$slot", "event_ts": {"$first": "$event_ts"}, "n": {"$sum": 1}, "alerts": alert, "mean": {"$avg": "$probability"},
                                "max": {"$max": "$probability"}}}, {"$sort": {"_id": 1}}], allowDiskUse=True)]

    def database_info(self) -> dict:
        """Storage statistics and, per collection: documents, index names and whether a schema validator is installed."""
        stats = self.db.command("dbStats", scale=1024 * 1024)
        colls = {c["name"]: c for c in self.db.list_collections()}
        out = []
        for name in sorted(colls):
            col = self.db[name]
            out.append({"name": name, "documents": col.estimated_document_count(), "indexes": sorted(i["name"] for i in col.list_indexes()),
                        "validator": bool(colls[name].get("options", {}).get("validator"))})
        return {"database": self.db.name, "data_mb": round(stats["dataSize"], 2), "storage_mb": round(stats["storageSize"], 2),
                "index_mb": round(stats["indexSize"], 2), "collections": out, "server_version": self.db.client.server_info()["version"]}

    def prediction_overview(self, window_hours: int, threshold: float) -> dict:
        """Counts and a per-slot series over the `window_hours` before the newest prediction (replayed data is historic,
        so 'recent' is relative to the data, not to the wall clock)."""
        newest = self.newest_prediction_time()
        if newest is None:
            return {"newest_event_ts": None, "window_hours": window_hours, "threshold": threshold, "overall": None, "by_slot": []}
        since = newest - timedelta(hours=window_hours)
        alert = {"$sum": {"$cond": [{"$gte": ["$probability", threshold]}, 1, 0]}}
        res = next(self.db[S.TRAFFIC_PREDICTIONS].aggregate([
            {"$match": {"event_ts": {"$gt": since}}},
            {"$facet": {
                "overall": [{"$group": {"_id": None, "predictions": {"$sum": 1}, "sensors": {"$addToSet": "$sensor_id"},
                                        "mean_probability": {"$avg": "$probability"}, "max_probability": {"$max": "$probability"}, "alerts": alert}},
                            {"$project": {"_id": 0, "predictions": 1, "sensors": {"$size": "$sensors"}, "mean_probability": 1,
                                          "max_probability": 1, "alerts": 1}}],
                "by_slot": [{"$group": {"_id": "$slot", "event_ts": {"$first": "$event_ts"}, "predictions": {"$sum": 1},
                                        "mean_probability": {"$avg": "$probability"}, "alerts": alert}},
                            {"$sort": {"_id": 1}}, {"$project": {"_id": 0, "slot": "$_id", "event_ts": 1, "predictions": 1,
                                                                   "mean_probability": 1, "alerts": 1}}]}}],
            allowDiskUse=True))
        return {"newest_event_ts": newest, "window_hours": window_hours, "threshold": threshold,
                "overall": res["overall"][0] if res["overall"] else None, "by_slot": res["by_slot"]}

    # ------------------------------------------------------------------------------------------------ analytics
    def list_questions(self) -> list[dict]:
        docs = list(self.db[S.ANALYSIS_RESULTS].find({"doc_type": "question"}))
        return sorted(docs, key=lambda d: d.get("number", 0))

    def get_question(self, qid: str) -> dict | None:
        return self.db[S.ANALYSIS_RESULTS].find_one({"_id": qid, "doc_type": "question"})

    def list_tables(self, qid: str) -> list[dict]:
        return list(self.db[S.ANALYSIS_RESULTS].find({"doc_type": "table", "qid": qid}, {"rows": 0}).sort("name", ASCENDING))

    def get_table(self, qid: str, name: str, limit: int, offset: int) -> dict | None:
        return self.db[S.ANALYSIS_RESULTS].find_one({"_id": f"{qid}:{name}", "doc_type": "table"}, {"rows": {"$slice": [offset, limit]}})

    # ------------------------------------------------------------------------------------------------ models
    def list_evaluations(self, kind: str | None = None) -> list[dict]:
        filt = {"kind": kind} if kind else {}
        return list(self.db[S.MODEL_EVALUATIONS].find(filt).sort([("kind", DESCENDING), ("is_best", DESCENDING), ("model", ASCENDING)]))

    def get_evaluation(self, model: str) -> dict | None:
        return self.db[S.MODEL_EVALUATIONS].find_one({"model": model})

    def best_evaluation(self) -> dict | None:
        return self.db[S.MODEL_EVALUATIONS].find_one({"is_best": True})

    # ------------------------------------------------------------------------------------------------ streaming
    def stream_runs(self) -> list[dict]:
        return [{"run": r["_id"], "queries": sorted(r["queries"]), "batches": r["batches"], "first_at": r["first"], "last_at": r["last"]}
                for r in self.db[S.STREAM_METRICS].aggregate([
                    {"$match": {"doc_type": {"$ne": "summary"}}},
                    {"$group": {"_id": "$run", "queries": {"$addToSet": "$query"}, "batches": {"$sum": 1}, "first": {"$min": "$timestamp"},
                                "last": {"$max": "$timestamp"}}}, {"$sort": {"last": -1}}])]

    def stream_progress(self, run: str | None = None, query: str | None = None, limit: int = 100, offset: int = 0) -> tuple[list[dict], int]:
        filt: dict[str, Any] = {"doc_type": {"$ne": "summary"}}
        if run:
            filt["run"] = run
        if query:
            filt["query"] = query
        return self._page(S.STREAM_METRICS, filt, [("timestamp", DESCENDING)], limit, offset, {"progress": 0})

    def stream_summary(self, run: str | None = None) -> list[dict]:
        """Per run and query: batches, rows, rate and batch-duration percentiles, aggregated by MongoDB ($percentile)."""
        match: dict[str, Any] = {"doc_type": {"$ne": "summary"}}
        if run:
            match["run"] = run
        return [{"run": r["_id"]["run"], "query": r["_id"]["query"], "batches": r["batches"], "batches_with_data": r["with_data"],
                 "input_rows": r["rows"], "batch_ms_p50": r["pct"][0], "batch_ms_p95": r["pct"][1], "batch_ms_max": r["max"],
                 "mean_processed_rows_per_s": r["rps"], "first_at": r["first"], "last_at": r["last"]}
                for r in self.db[S.STREAM_METRICS].aggregate([
                    {"$match": match},
                    {"$group": {"_id": {"run": "$run", "query": "$query"}, "batches": {"$sum": 1},
                                "with_data": {"$sum": {"$cond": [{"$gt": ["$num_input_rows", 0]}, 1, 0]}}, "rows": {"$sum": "$num_input_rows"},
                                "pct": {"$percentile": {"input": "$trigger_ms", "p": [0.5, 0.95], "method": "approximate"}},
                                "max": {"$max": "$trigger_ms"}, "rps": {"$avg": "$processed_rows_per_second"},
                                "first": {"$min": "$timestamp"}, "last": {"$max": "$timestamp"}}},
                    {"$sort": {"_id.run": 1, "_id.query": 1}}], allowDiskUse=True)]

    def stream_run_reports(self, run: str | None = None) -> list[dict]:
        filt: dict[str, Any] = {"doc_type": "summary"}
        if run:
            filt["run"] = run
        return list(self.db[S.STREAM_METRICS].find(filt).sort("run", ASCENDING))

    def stream_state(self) -> dict | None:
        return self.db[C.COL_STATE].find_one({"_id": "state"})

    # ------------------------------------------------------------------------------------------------ pipeline runs
    def list_runs(self, kind: str | None = None, status: str | None = None, limit: int = 50, offset: int = 0) -> tuple[list[dict], int]:
        filt: dict[str, Any] = {}
        if kind:
            filt["kind"] = kind
        if status:
            filt["status"] = status
        return self._page(S.PIPELINE_RUNS, filt, [("finished_at", DESCENDING), ("name", ASCENDING)], limit, offset)

    def get_run(self, run_id: str) -> dict | None:
        return self.db[S.PIPELINE_RUNS].find_one({"_id": run_id})

    def latest_run_per_kind(self) -> list[dict]:
        return [{"kind": r["_id"], "runs": r["n"], "failed": r["failed"], "latest_name": r["name"], "latest_finished_at": r["at"],
                 "latest_status": r["status"]}
                for r in self.db[S.PIPELINE_RUNS].aggregate([
                    {"$sort": {"finished_at": -1}},
                    {"$group": {"_id": "$kind", "n": {"$sum": 1}, "failed": {"$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}},
                                "name": {"$first": "$name"}, "at": {"$first": "$finished_at"}, "status": {"$first": "$status"}}},
                    {"$sort": {"_id": 1}}])]
