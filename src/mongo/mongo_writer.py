"""Write side of the MongoDB layer.

  * sensor_metadata: create / replace / partial update / delete with optimistic concurrency (`version`)
  * settings:        create / put (upsert) / patch / delete with optimistic concurrency
  * loaders:         idempotent bulk upserts for the derived collections and the dataset sensor registry

Every mutation of an analyst-edited document increments `version` and sets `updated_at`. A caller may pass
`expected_version`; if the stored version differs the write is refused (VersionMismatchError) instead of silently
overwriting someone else's change (no lost updates).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Iterable

from pymongo import ReplaceOne, UpdateOne
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError
from pymongo.results import UpdateResult

from src.mongo import schema as S
from src.mongo.errors import ConflictError, NotFoundError, VersionMismatchError
from src.mongo.settings_defs import KNOWN, category_for

log = logging.getLogger("trafficsense.mongo")

EDITABLE_SENSOR_FIELDS = ("name", "description", "direction", "lanes", "status", "tags", "latitude", "longitude")
SENSOR_DEFAULTS: dict[str, Any] = {"name": None, "description": None, "direction": None, "lanes": None, "status": "active", "tags": []}


def utcnow() -> datetime:
    """Current UTC time truncated to milliseconds (MongoDB's datetime precision), so values returned by a write equal values read back."""
    now = datetime.now(timezone.utc)
    return now.replace(microsecond=now.microsecond // 1000 * 1000)


def point(lat: float, lon: float) -> dict:
    return {"type": "Point", "coordinates": [float(lon), float(lat)]}      # GeoJSON order: lon, lat


class MongoWriter:
    def __init__(self, db: Database):
        self.db = db

    # ------------------------------------------------------------------------------------------------ sensors
    def create_sensor(self, data: dict, source: str = "api") -> dict:
        now = utcnow()
        doc = {**SENSOR_DEFAULTS, **{k: v for k, v in data.items() if v is not None or k in SENSOR_DEFAULTS},
               "_id": int(data["sensor_id"]), "sensor_id": int(data["sensor_id"]), "location": point(data["latitude"], data["longitude"]),
               "tags": list(data.get("tags") or []), "stats": {}, "source": source, "created_at": now, "updated_at": now, "version": 1}
        try:
            self.db[S.SENSOR_METADATA].insert_one(doc)
        except DuplicateKeyError:
            raise ConflictError(f"sensor {doc['sensor_id']} already exists")
        return doc

    def update_sensor(self, sensor_id: int, changes: dict, expected_version: int | None = None, replace: bool = False) -> dict:
        """PATCH (changes only) or PUT (`replace=True`: every editable field is set; omitted optional ones reset to defaults)."""
        col = self.db[S.SENSOR_METADATA]
        for _ in range(3):                                         # retry when a concurrent writer wins between our read and write
            cur = col.find_one({"_id": sensor_id})
            if cur is None:
                raise NotFoundError("sensor", sensor_id)
            if expected_version is not None and cur["version"] != expected_version:
                raise VersionMismatchError("sensor", sensor_id, expected_version, cur["version"])
            new = {f: changes[f] for f in EDITABLE_SENSOR_FIELDS if f in changes}
            if replace:
                new = {**SENSOR_DEFAULTS, **{f: cur[f] for f in ("latitude", "longitude")}, **new}
            if "tags" in new:
                new["tags"] = list(new["tags"] or [])
            if "latitude" in new or "longitude" in new:
                new["location"] = point(new.get("latitude", cur["latitude"]), new.get("longitude", cur["longitude"]))
            res = col.find_one_and_update({"_id": sensor_id, "version": cur["version"]},
                                          {"$set": {**new, "updated_at": utcnow()}, "$inc": {"version": 1}}, return_document=True)
            if res is not None:
                return res
            if expected_version is not None:
                raise VersionMismatchError("sensor", sensor_id, expected_version)
        raise ConflictError(f"sensor {sensor_id} is being modified concurrently, retry")

    def delete_sensor(self, sensor_id: int, expected_version: int | None = None) -> None:
        filt: dict = {"_id": sensor_id}
        if expected_version is not None:
            filt["version"] = expected_version
        if self.db[S.SENSOR_METADATA].delete_one(filt).deleted_count:
            return
        cur = self.db[S.SENSOR_METADATA].find_one({"_id": sensor_id}, {"version": 1})
        if cur is None:
            raise NotFoundError("sensor", sensor_id)
        raise VersionMismatchError("sensor", sensor_id, expected_version, cur["version"])

    def upsert_dataset_sensors(self, rows: Iterable[dict]) -> dict:
        """Load/refresh the registry from the dataset. NEVER overwrites analyst edits: editable fields are insert-only,
        only the read-only `stats` sub-document is refreshed on existing sensors."""
        now, ops = utcnow(), []
        for r in rows:
            sid = int(r["sensor_id"])
            ops.append(UpdateOne(
                {"_id": sid},
                {"$set": {"stats": r["stats"], "stats_synced_at": now},
                 "$setOnInsert": {**SENSOR_DEFAULTS, "name": r.get("name") or f"Sensor {sid}", "sensor_id": sid,
                                  "latitude": float(r["latitude"]), "longitude": float(r["longitude"]),
                                  "location": point(r["latitude"], r["longitude"]), "source": "dataset",
                                  "created_at": now, "updated_at": now, "version": 1}}, upsert=True))
        res = self.db[S.SENSOR_METADATA].bulk_write(ops, ordered=False) if ops else None
        return {"rows": len(ops), "inserted": res.upserted_count if res else 0, "refreshed": res.modified_count if res else 0}

    # ------------------------------------------------------------------------------------------------ settings
    def _setting_fields(self, key: str, value: Any, description: str | None, updated_by: str | None) -> dict:
        d = KNOWN.get(key)
        return {"key": key, "value": value, "category": category_for(key), "updated_by": updated_by,
                "description": description if description is not None else (d.description if d else None)}

    def create_setting(self, key: str, value: Any, description: str | None = None, updated_by: str | None = None) -> dict:
        now = utcnow()
        doc = {"_id": key, **self._setting_fields(key, value, description, updated_by), "created_at": now, "updated_at": now, "version": 1}
        try:
            self.db[S.SETTINGS].insert_one(doc)
        except DuplicateKeyError:
            raise ConflictError(f"setting {key!r} already exists (use PUT or PATCH to change it)")
        return doc

    def put_setting(self, key: str, value: Any, description: str | None = None, updated_by: str | None = None,
                    expected_version: int | None = None) -> tuple[dict, bool]:
        """Create-or-replace. Returns (document, created)."""
        now = utcnow()
        filt: dict = {"_id": key}
        if expected_version is not None:
            filt["version"] = expected_version
        try:
            res: UpdateResult = self.db[S.SETTINGS].update_one(
                filt, {"$set": {**self._setting_fields(key, value, description, updated_by), "updated_at": now},
                       "$setOnInsert": {"created_at": now}, "$inc": {"version": 1}}, upsert=True)
        except DuplicateKeyError:                                  # filter had a stale version: the upsert tried to insert an existing _id
            cur = self.db[S.SETTINGS].find_one({"_id": key}, {"version": 1})
            raise VersionMismatchError("setting", key, expected_version, cur["version"] if cur else None)
        return self.db[S.SETTINGS].find_one({"_id": key}), res.upserted_id is not None

    def patch_setting(self, key: str, changes: dict, updated_by: str | None = None, expected_version: int | None = None) -> dict:
        filt: dict = {"_id": key}
        if expected_version is not None:
            filt["version"] = expected_version
        sets = {k: v for k, v in changes.items() if k in ("value", "description")}
        sets.update(updated_at=utcnow(), updated_by=updated_by)
        res = self.db[S.SETTINGS].find_one_and_update(filt, {"$set": sets, "$inc": {"version": 1}}, return_document=True)
        if res is None:
            cur = self.db[S.SETTINGS].find_one({"_id": key}, {"version": 1})
            if cur is None:
                raise NotFoundError("setting", key)
            raise VersionMismatchError("setting", key, expected_version, cur["version"])
        return res

    def delete_setting(self, key: str, expected_version: int | None = None) -> None:
        filt: dict = {"_id": key}
        if expected_version is not None:
            filt["version"] = expected_version
        if self.db[S.SETTINGS].delete_one(filt).deleted_count:
            return
        cur = self.db[S.SETTINGS].find_one({"_id": key}, {"version": 1})
        if cur is None:
            raise NotFoundError("setting", key)
        raise VersionMismatchError("setting", key, expected_version, cur["version"])

    # ------------------------------------------------------------------------------------------------ loaders
    def replace_documents(self, collection: str, docs: list[dict], stale_filter: dict | None = None) -> dict:
        """Idempotent bulk load keyed by `_id`. With `stale_filter`, documents matching it that are NOT in `docs` are removed
        (e.g. result tables of an earlier run), so the collection mirrors the latest outputs exactly."""
        col = self.db[collection]
        if docs:
            col.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs], ordered=False)
        removed = 0
        if stale_filter is not None:
            removed = col.delete_many({**stale_filter, "_id": {"$nin": [d["_id"] for d in docs]}}).deleted_count
        return {"collection": collection, "written": len(docs), "removed_stale": removed}

    def record_pipeline_run(self, doc: dict) -> dict:
        doc = {**doc, "recorded_at": utcnow()}
        self.db[S.PIPELINE_RUNS].replace_one({"_id": doc["_id"]}, doc, upsert=True)
        return doc
