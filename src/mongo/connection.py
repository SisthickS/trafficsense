"""MongoDB connection. One tz-aware client per process (PyMongo clients are thread-safe and pool connections)."""
from __future__ import annotations

from datetime import timezone

from pymongo import MongoClient
from pymongo.database import Database

from src.streaming import config as C


def make_client(uri: str = C.MONGO_URI, timeout_ms: int = 3000, app: str = "trafficsense") -> MongoClient:
    """tz_aware=True: every datetime read back is UTC-aware (serialised with a 'Z' by the API).

    Connections are opened lazily, so constructing the client never fails; the first operation raises
    ServerSelectionTimeoutError after `timeout_ms` if the server is unreachable.
    """
    return MongoClient(uri, tz_aware=True, tzinfo=timezone.utc, serverSelectionTimeoutMS=timeout_ms,
                       connectTimeoutMS=timeout_ms, appname=app, retryWrites=True, maxPoolSize=50)


def get_db(uri: str = C.MONGO_URI, db: str = C.MONGO_DB, timeout_ms: int = 3000) -> Database:
    return make_client(uri, timeout_ms)[db]
