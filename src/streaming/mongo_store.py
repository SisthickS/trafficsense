"""MongoDB access for the streaming application: idempotent upserts, state document, recovery reads.

Every document has a deterministic `_id` (sensor|slot, sensor|window-start, ...), so re-writing a micro-batch after a
crash overwrites the same documents: Spark's at-least-once foreachBatch becomes effectively-once output.
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable

from pymongo import ASCENDING, MongoClient, ReplaceOne
from pymongo.collection import Collection

from src.streaming import config as C


class MongoStore:
    def __init__(self, uri: str = C.MONGO_URI, db: str = C.MONGO_DB):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000, appname="trafficsense-stream")
        self.db = self.client[db]
        self.client.admin.command("ping")

    def col(self, name: str) -> Collection:
        return self.db[name]

    def ensure_indexes(self) -> None:
        """Collections, validators and indexes are defined once, in src/mongo/schema.py (shared with the API)."""
        from src.mongo.schema import ensure_schema
        ensure_schema(self.db)

    def upsert(self, name: str, docs: Iterable[dict]) -> int:
        ops = [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs]
        if ops:
            self.col(name).bulk_write(ops, ordered=False)
        return len(ops)

    def drop_all(self) -> None:
        for name in self.db.list_collection_names():
            self.db.drop_collection(name)

    # ---- state document
    def get_state(self) -> dict | None:
        return self.col(C.COL_STATE).find_one({"_id": "state"})

    def set_state(self, **fields) -> None:
        fields["updated_at"] = datetime.utcnow()
        self.col(C.COL_STATE).update_one({"_id": "state"}, {"$set": fields}, upsert=True)

    # ---- recovery reads
    def readings_from(self, first_slot: int) -> list[dict]:
        return list(self.col(C.COL_READINGS).find({"slot": {"$gte": first_slot}}, {"_id": 0, "sensor_id": 1, "slot": 1,
                                                                                  "speed_mph": 1, "produced_at_ms": 1})
                    .sort("slot", ASCENDING))

    def close(self) -> None:
        self.client.close()


def reading_id(sensor_id: int, slot: int) -> str:
    return f"{sensor_id}|{slot}"
