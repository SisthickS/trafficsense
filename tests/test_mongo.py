"""Phase 9 tests: MongoDB schema/validators/indexes, writer semantics (CRUD, versions, loaders) and reader queries.

Run:  pytest -q tests/test_mongo.py        (skips when MongoDB is not reachable: docker compose up -d --wait mongo)
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

import pytest
from pymongo.errors import WriteError

from src.mongo import schema as S
from src.mongo.connection import make_client
from src.mongo.errors import ConflictError, NotFoundError, VersionMismatchError
from src.mongo.mongo_reader import MongoReader
from src.mongo.mongo_writer import MongoWriter
from src.mongo.settings_defs import KNOWN, validate_key, validate_value

DB = "ts_mongo_tests"


@pytest.fixture(scope="module")
def db():
    client = make_client(timeout_ms=1500)
    try:
        client.admin.command("ping")
    except Exception:
        pytest.skip("MongoDB not reachable (docker compose up -d --wait mongo)")
    client.drop_database(DB)
    d = client[DB]
    S.ensure_schema(d)
    yield d
    client.drop_database(DB)
    client.close()


@pytest.fixture()
def clean(db):
    for n in (S.SENSOR_METADATA, S.SETTINGS, S.CONGESTION_HOTSPOTS, S.ANALYSIS_RESULTS):
        db[n].delete_many({})
    return db


def sensor(sid=1001, **kw):
    return {"sensor_id": sid, "latitude": 37.30, "longitude": -121.90, **kw}


# ------------------------------------------------------------------------------------------------ schema
def test_ensure_schema_is_idempotent_and_complete(db):
    again = S.ensure_schema(db)
    assert again["created"] == []                                            # second run creates nothing
    names = set(db.list_collection_names())
    for core in ("sensor_metadata", "analysis_results", "congestion_hotspots", "traffic_predictions", "model_evaluations",
                 "stream_metrics", "pipeline_runs", "settings"):
        assert core in names
    for spec in S.SPECS:
        if spec.indexes:
            have = {i["name"] for i in db[spec.name].list_indexes()}
            assert {i.document["name"] for i in spec.indexes} <= have, spec.name
    opts = db.list_collections(filter={"name": "sensor_metadata"}).next()["options"]
    assert "validator" in opts and opts["validationLevel"] == "strict"
    ix = {i["name"]: i for i in db[S.SENSOR_METADATA].list_indexes()}
    assert ix["sensor_id_unique"]["unique"] and ix["location_2dsphere"]["key"]["location"] == "2dsphere"


@pytest.mark.parametrize("bad", [{"latitude": 200}, {"longitude": -300}, {"status": "weird"}, {"lanes": 99}, {"tags": ["x"] * 21},
                                 {"direction": "Q"}, {"sensor_id": 0}, {"version": 0}])
def test_validator_rejects_invalid_sensor_documents(clean, bad):
    w = MongoWriter(clean)
    doc = w.create_sensor(sensor())
    clean[S.SENSOR_METADATA].delete_one({"_id": doc["_id"]})
    good = {**doc, "_id": 7, "sensor_id": 7}
    with pytest.raises(WriteError) as e:
        clean[S.SENSOR_METADATA].insert_one({**good, **bad})
    assert e.value.code == 121                                                # "Document failed validation"
    clean[S.SENSOR_METADATA].insert_one(good)                                  # the unmodified document is accepted


def test_validator_rejects_bad_setting_key(clean):
    with pytest.raises(WriteError):
        clean[S.SETTINGS].insert_one({"_id": "Bad Key", "key": "Bad Key", "value": 1, "category": "x", "version": 1,
                                      "created_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc)})


# ------------------------------------------------------------------------------------------------ sensors
def test_sensor_crud_versions_and_location(clean):
    w, r = MongoWriter(clean), MongoReader(clean)
    d = w.create_sensor(sensor(name="A", tags=["x", "y"]))
    assert d["version"] == 1 and d["source"] == "api" and d["location"] == {"type": "Point", "coordinates": [-121.90, 37.30]}
    with pytest.raises(ConflictError):
        w.create_sensor(sensor())
    u = w.update_sensor(1001, {"description": "d", "latitude": 37.5})
    assert u["version"] == 2 and u["location"]["coordinates"] == [-121.90, 37.5] and u["name"] == "A" and u["updated_at"] >= d["updated_at"]
    p = w.update_sensor(1001, {"name": "B", "status": "maintenance"}, expected_version=2, replace=True)         # PUT semantics
    assert p["version"] == 3 and p["description"] is None and p["tags"] == [] and p["status"] == "maintenance" and p["name"] == "B"
    assert (p["latitude"], p["longitude"]) == (37.5, -121.90)                                                  # coordinates kept when omitted
    with pytest.raises(VersionMismatchError) as e:
        w.update_sensor(1001, {"name": "C"}, expected_version=1)
    assert e.value.actual == 3
    assert r.get_sensor(1001)["name"] == "B"
    with pytest.raises(NotFoundError):
        w.update_sensor(999, {"name": "x"})
    with pytest.raises(VersionMismatchError):
        w.delete_sensor(1001, expected_version=1)
    w.delete_sensor(1001, expected_version=3)
    assert r.get_sensor(1001) is None
    with pytest.raises(NotFoundError):
        w.delete_sensor(1001)


def test_concurrent_writers_with_same_expected_version_exactly_one_wins(clean):
    w = MongoWriter(clean)
    w.create_sensor(sensor())
    outcomes, barrier = [], threading.Barrier(8)

    def attempt(i):
        barrier.wait()
        try:
            w.update_sensor(1001, {"description": f"writer {i}"}, expected_version=1)
            outcomes.append("ok")
        except VersionMismatchError:
            outcomes.append("conflict")

    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert outcomes.count("ok") == 1 and outcomes.count("conflict") == 7           # no lost update
    assert MongoReader(clean).get_sensor(1001)["version"] == 2


def test_dataset_reload_keeps_analyst_edits_but_refreshes_stats(clean):
    w, r = MongoWriter(clean), MongoReader(clean)
    rows = [{**sensor(1), "stats": {"ff_speed": 60.0}}, {**sensor(2, latitude=37.4), "stats": {"ff_speed": 65.0}}]
    assert w.upsert_dataset_sensors(rows)["inserted"] == 2
    assert r.get_sensor(1)["name"] == "Sensor 1" and r.get_sensor(1)["source"] == "dataset"
    w.update_sensor(1, {"name": "Edited", "status": "inactive", "tags": ["keep"]})
    res = w.upsert_dataset_sensors([{**sensor(1), "stats": {"ff_speed": 61.5}}, rows[1]])
    s = r.get_sensor(1)
    assert res["inserted"] == 0 and s["name"] == "Edited" and s["status"] == "inactive" and s["tags"] == ["keep"]
    assert s["stats"]["ff_speed"] == 61.5 and s["version"] == 2


def test_reader_filters_pagination_geo_and_search(clean):
    w, r = MongoWriter(clean), MongoReader(clean)
    for i, (lat, lon) in enumerate([(37.30, -121.90), (37.31, -121.90), (37.40, -121.80), (37.90, -122.20)], start=1):
        w.create_sensor(sensor(i, latitude=lat, longitude=lon, name=f"Alpha {i}" if i % 2 else f"Beta {i}", tags=["even"] if i % 2 == 0 else [],
                               status="maintenance" if i == 4 else "active", description="near the interchange" if i == 3 else None))
    items, total = r.list_sensors(limit=2, offset=1)
    assert total == 4 and [d["sensor_id"] for d in items] == [2, 3]
    assert r.list_sensors(status="maintenance")[1] == 1 and [d["sensor_id"] for d in r.list_sensors(tag="even")[0]] == [2, 4]
    assert [d["sensor_id"] for d in r.list_sensors(q="alpha")[0]] == [1, 3] and r.list_sensors(q="INTERCHANGE")[1] == 1 and r.list_sensors(q="2")[1] == 1
    assert r.list_sensors(bbox=(-122.0, 37.25, -121.85, 37.35))[1] == 2
    assert [d["sensor_id"] for d in r.list_sensors(sort="name", descending=True)[0]][0] == 4
    near = r.sensors_near(37.30, -121.90, 2000)
    assert [d["sensor_id"] for d in near] == [1, 2] and near[0]["distance_m"] < 1 < near[1]["distance_m"] < 2000


# ------------------------------------------------------------------------------------------------ settings
def test_settings_writer_semantics(clean):
    w, r = MongoWriter(clean), MongoReader(clean)
    d = w.create_setting("custom.note", {"a": 1}, "my note", "ana")
    assert d["version"] == 1 and d["category"] == "custom" and d["updated_by"] == "ana"
    with pytest.raises(ConflictError):
        w.create_setting("custom.note", 2)
    doc, created = w.put_setting("alert_threshold", 0.7, updated_by="ana")
    assert created and doc["version"] == 1 and doc["category"] == "alerts" and "validation-tuned" in doc["description"]
    doc, created = w.put_setting("alert_threshold", 0.8, expected_version=1)
    assert not created and doc["version"] == 2 and doc["value"] == 0.8 and doc["created_at"] < doc["updated_at"]
    with pytest.raises(VersionMismatchError):
        w.put_setting("alert_threshold", 0.9, expected_version=1)
    with pytest.raises(VersionMismatchError):
        w.patch_setting("alert_threshold", {"value": 0.5}, expected_version=1)
    assert w.patch_setting("alert_threshold", {"description": "tuned"})["version"] == 3
    with pytest.raises(NotFoundError):
        w.patch_setting("watchlist_size", {"value": 5})
    assert r.effective_setting("alert_threshold") == 0.8 and r.effective_setting("watchlist_size") == 10         # override vs default
    eff = {s["key"]: s for s in r.effective_settings()}
    assert eff["alert_threshold"]["is_default"] is False and eff["watchlist_size"]["is_default"] is True and eff["custom.note"]["known"] is False
    w.delete_setting("alert_threshold", expected_version=3)
    assert r.effective_setting("alert_threshold") == KNOWN["alert_threshold"].default
    with pytest.raises(NotFoundError):
        w.delete_setting("alert_threshold")


def test_setting_value_validation():
    assert validate_value("alert_threshold", 0.5) == 0.5 and isinstance(validate_value("watchlist_size", 12.0), int)
    for key, bad in [("alert_threshold", 1.5), ("alert_threshold", "0.5"), ("alert_threshold", True), ("alert_threshold", float("nan")),
                     ("watchlist_size", 2.5), ("watchlist_size", 0), ("sensor_status_filter", "gone"), ("custom.x", float("inf")),
                     ("custom.x", "x" * 9000)]:
        with pytest.raises(ValueError):
            validate_value(key, bad)
    assert validate_value("custom.x", {"nested": [1, 2, None]}) == {"nested": [1, 2, None]}
    assert validate_key("ok_key-1.x") == "ok_key-1.x"
    for bad_key in ("Bad", "1abc", "a", "has space", "x" * 70):
        with pytest.raises(ValueError):
            validate_key(bad_key)


# ------------------------------------------------------------------------------------------------ loaders
def test_replace_documents_is_idempotent_and_removes_stale(clean):
    w = MongoWriter(clean)
    docs = [{"_id": "q1", "doc_type": "question", "qid": "q1"}, {"_id": "q1:t", "doc_type": "table", "qid": "q1", "name": "t"}]
    assert w.replace_documents(S.ANALYSIS_RESULTS, docs, stale_filter={})["written"] == 2
    assert w.replace_documents(S.ANALYSIS_RESULTS, docs, stale_filter={})["removed_stale"] == 0
    res = w.replace_documents(S.ANALYSIS_RESULTS, docs[:1], stale_filter={})
    assert res["removed_stale"] == 1 and clean[S.ANALYSIS_RESULTS].count_documents({}) == 1
    r = MongoReader(clean)
    assert r.get_question("q1")["qid"] == "q1" and r.get_table("q1", "t", 5, 0) is None
