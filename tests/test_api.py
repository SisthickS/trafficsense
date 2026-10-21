"""Phase 9 tests: FastAPI endpoints against a real MongoDB (CRUD persistence, validation, errors, auth, settings effects).

Run:  pytest -q tests/test_api.py      (skips without MongoDB; the model-inference test also needs the Phase 7 model + features)
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.api.config import ApiSettings
from src.api.main import create_app
from src.mongo import schema as S
from src.mongo.connection import make_client
from src.mongo.mongo_writer import MongoWriter
from src.streaming import config as C

DB = "ts_api_tests"
T0 = datetime(2017, 6, 5, 12, 0, tzinfo=timezone.utc)
V1 = "/api/v1"


def cfg(**kw) -> ApiSettings:
    base = dict(env="test", mongo_db=DB, mongo_timeout_ms=1500, enable_model_inference=False)
    return ApiSettings(_env_file=None, **{**base, **kw})


@pytest.fixture(scope="module")
def mongo():
    client = make_client(timeout_ms=1500)
    try:
        client.admin.command("ping")
    except Exception:
        pytest.skip("MongoDB not reachable (docker compose up -d --wait mongo)")
    client.drop_database(DB)
    yield client
    client.drop_database(DB)
    client.close()


def seed(db) -> None:
    w, now = MongoWriter(db), datetime.now(timezone.utc)
    w.upsert_dataset_sensors([{"sensor_id": i, "latitude": 37.30 + i * 0.01, "longitude": -121.90, "stats": {"free_flow_speed_mph": 60.0 + i}} for i in range(1, 6)])
    w.replace_documents(S.CONGESTION_HOTSPOTS, [
        {"_id": i, "sensor_id": i, "rank": i, "class": "persistent hotspot" if i <= 3 else "occasional", "is_hotspot": i <= 3, "persistence": 1.0 - i * 0.15,
         "mean_daily_congested_min": 300.0 - 50 * i, "max_daily_congested_min": 400.0, "workdays": 126, "csbi": 0.9 - i * 0.1, "csbi_rank": i, "tti_peak": 2.0,
         "latitude": 37.30 + i * 0.01, "longitude": -121.90, "location": {"type": "Point", "coordinates": [-121.90, 37.30 + i * 0.01]}} for i in range(1, 6)])
    preds = []
    for slot_off in (0, 1):
        for i in range(1, 6):
            p = min(1.0, round(0.2 * i + 0.01 * slot_off, 3))                 # 0.2 .. 1.0 (+0.01 in the newer slot)
            ts = T0 + timedelta(minutes=5 * slot_off)
            preds.append({"_id": f"{i}|{4988880 + slot_off}", "sensor_id": i, "slot": 4988880 + slot_off, "event_ts": ts, "target_ts": ts + timedelta(minutes=5),
                          "speed_mph": 55.0 - 5 * i, "probability": p, "alert": p >= 0.686, "model": "Logistic regression", "is_imputed": False})
    w.replace_documents(S.TRAFFIC_PREDICTIONS, preds)
    w.replace_documents(S.ANALYSIS_RESULTS, [
        {"_id": "q5", "doc_type": "question", "qid": "q5", "number": 5, "title": "Hotspots", "question": "Which?", "findings": ["a", "b"], "implications": ["c"],
         "limitations": [], "metrics": {"hotspots": 3}, "tables": ["top"], "figures": ["q5_map.png"], "generated_at": now},
        {"_id": "q5:top", "doc_type": "table", "qid": "q5", "name": "top", "columns": ["sensor_id", "v"], "n_rows": 5,
         "rows": [{"sensor_id": i, "v": i * 1.5} for i in range(1, 6)]}])
    w.replace_documents(S.MODEL_EVALUATIONS, [
        {"_id": "phase7:logistic_regression", "model": "logistic_regression", "label": "Logistic regression", "kind": "model", "is_best": True,
         "test": {"f1_optimal": {"f1": 0.93}}},
        {"_id": "phase7:persistence", "model": "persistence", "label": "Persistence", "kind": "baseline", "is_best": False, "test": {"f1_optimal": {"f1": 0.92}}}])
    w.replace_documents(S.STREAM_METRICS, [
        {"_id": f"r1|predictions|x|{b}", "run": "r1", "query": "predictions", "batch_id": b, "timestamp": now - timedelta(seconds=10 - b), "num_input_rows": 100 * b,
         "processed_rows_per_second": 50.0 + b, "trigger_ms": 1000 + 100 * b, "progress": {"big": "payload"}} for b in range(4)] +
        [{"_id": "summary:r1", "doc_type": "summary", "run": "r1", "query": "__summary__", "end_to_end_latency": {"since_slot_complete": {"p50_s": 7.0}}, "timestamp": now}])
    w.replace_documents(S.PIPELINE_RUNS, [
        {"_id": "analytics:q5", "kind": "analytics", "name": "q5", "status": "succeeded", "finished_at": now, "duration_seconds": 5.4},
        {"_id": "ml:training", "kind": "ml_training", "name": "train_model", "status": "failed", "finished_at": now - timedelta(days=1), "duration_seconds": 100.0}])
    db[C.COL_STATE].replace_one({"_id": "state"}, {"_id": "state", "last_batch_id": 7, "last_final_slot": 4988881, "updated_at": now}, upsert=True)


@pytest.fixture(scope="module")
def client(mongo):
    app = create_app(cfg())
    with TestClient(app) as c:
        seed(app.state.db)
        yield c


@pytest.fixture()
def fresh_settings(client):
    client.app.state.db[S.SETTINGS].delete_many({})
    yield


def err(r, status, code):
    assert r.status_code == status, r.text
    e = r.json()["error"]
    assert e["code"] == code and e["request_id"] and e["message"]
    return e


# ------------------------------------------------------------------------------------------------ platform
def test_health_ready_status_and_openapi(client):
    assert client.get("/health").json()["status"] == "ok"
    assert client.get("/ready").json() == {"status": "ready", "database": DB}
    s = client.get(f"{V1}/status").json()
    assert s["counts"]["sensor_metadata"] == 5 and s["counts"]["traffic_predictions"] == 10 and s["write_protection"] == "open"
    paths = client.get("/openapi.json").json()["paths"]
    assert set(paths[f"{V1}/sensors/{{sensor_id}}"]) >= {"get", "put", "patch", "delete"} and "post" in paths[f"{V1}/sensors"]
    assert set(paths[f"{V1}/settings/{{key}}"]) >= {"get", "put", "patch", "delete"} and "post" in paths[f"{V1}/settings"]
    r = client.get("/nope", headers={"X-Request-ID": "abc123"})
    assert err(r, 404, "not_found")["request_id"] == "abc123" and r.headers["X-Request-ID"] == "abc123" and r.headers["X-Content-Type-Options"] == "nosniff"


# ------------------------------------------------------------------------------------------------ sensors CRUD
def test_sensor_full_lifecycle(client):
    body = {"sensor_id": 9001, "latitude": 37.5, "longitude": -122.0, "name": " Test Sensor ", "tags": ["Bridge", "bridge", "north-bound"], "lanes": 4, "direction": "N"}
    r = client.post(f"{V1}/sensors", json=body)
    assert r.status_code == 201 and r.headers["Location"] == f"{V1}/sensors/9001" and r.headers["ETag"] == '"1"'
    d = r.json()
    assert d["name"] == "Test Sensor" and d["tags"] == ["bridge", "north-bound"] and d["status"] == "active" and d["version"] == 1 and d["source"] == "api"
    assert client.get(f"{V1}/sensors/9001").json() == {**d, "hotspot": None}
    err(client.post(f"{V1}/sensors", json=body), 409, "conflict")

    r = client.patch(f"{V1}/sensors/9001", json={"status": "maintenance", "description": "closed lane"})
    assert r.status_code == 200 and r.json()["version"] == 2 and r.json()["name"] == "Test Sensor" and r.json()["status"] == "maintenance"
    r = client.patch(f"{V1}/sensors/9001", json={"name": None, "lanes": None})                 # null clears optional fields
    assert r.json()["name"] is None and r.json()["lanes"] is None and r.json()["version"] == 3

    r = client.put(f"{V1}/sensors/9001", json={"latitude": 37.6, "longitude": -122.1, "name": "Replaced"}, headers={"If-Match": '"3"'})
    d = r.json()
    assert r.status_code == 200 and d["version"] == 4 and d["description"] is None and d["tags"] == [] and d["status"] == "active" and d["latitude"] == 37.6

    err(client.patch(f"{V1}/sensors/9001", json={"name": "stale"}, headers={"If-Match": '"1"'}), 412, "precondition_failed")
    assert client.get(f"{V1}/sensors/9001").json()["name"] == "Replaced"
    err(client.delete(f"{V1}/sensors/9001", headers={"If-Match": '"2"'}), 412, "precondition_failed")
    err(client.delete(f"{V1}/sensors/9001", headers={"If-Match": "nonsense"}), 400, "invalid_if_match")
    assert client.delete(f"{V1}/sensors/9001", headers={"If-Match": '"4"'}).status_code == 204
    err(client.get(f"{V1}/sensors/9001"), 404, "not_found")
    err(client.delete(f"{V1}/sensors/9001"), 404, "not_found")
    err(client.patch(f"{V1}/sensors/9001", json={"name": "x"}), 404, "not_found")
    err(client.put(f"{V1}/sensors/9001", json={"latitude": 1, "longitude": 1}), 404, "not_found")


@pytest.mark.parametrize("payload,field", [
    ({"sensor_id": 9100, "latitude": 91, "longitude": 0}, "latitude"), ({"sensor_id": 9100, "latitude": 0, "longitude": -181}, "longitude"),
    ({"sensor_id": 0, "latitude": 0, "longitude": 0}, "sensor_id"), ({"sensor_id": 9100, "latitude": 0}, "longitude"),
    ({"sensor_id": 9100, "latitude": 0, "longitude": 0, "status": "broken"}, "status"), ({"sensor_id": 9100, "latitude": 0, "longitude": 0, "lanes": 0}, "lanes"),
    ({"sensor_id": 9100, "latitude": 0, "longitude": 0, "tags": ["Bad Tag!"]}, "tags"), ({"sensor_id": 9100, "latitude": 0, "longitude": 0, "tags": ["t"] * 21}, "tags"),
    ({"sensor_id": 9100, "latitude": 0, "longitude": 0, "name": ""}, "name"), ({"sensor_id": 9100, "latitude": 0, "longitude": 0, "name": "x" * 121}, "name"),
    ({"sensor_id": 9100, "latitude": 0, "longitude": 0, "version": 5}, "version"), ({"sensor_id": 9100, "latitude": 0, "longitude": 0, "direction": "up"}, "direction"),
    ({"sensor_id": "abc", "latitude": 0, "longitude": 0}, "sensor_id"), ({"sensor_id": 9100, "latitude": "NaN", "longitude": 0}, "latitude")])
def test_sensor_create_validation(client, payload, field):
    e = err(client.post(f"{V1}/sensors", json=payload), 422, "validation_error")
    assert any(field in d["field"] for d in e["details"]), e
    assert client.get(f"{V1}/sensors/9100").status_code == 404                  # nothing was written


def test_sensor_patch_validation(client):
    for body in ({}, {"status": None}, {"latitude": None}, {"tags": None}, {"sensor_id": 5}, {"stats": {"x": 1}}, {"lanes": 13}):
        err(client.patch(f"{V1}/sensors/1", json=body), 422, "validation_error")
    err(client.patch(f"{V1}/sensors/1", content=b"{not json", headers={"Content-Type": "application/json"}), 422, "validation_error")
    err(client.get(f"{V1}/sensors", params={"limit": 0}), 422, "validation_error")
    err(client.get(f"{V1}/sensors", params={"limit": 100000}), 422, "validation_error")
    err(client.get(f"{V1}/sensors", params={"bbox": "1,2,3"}), 422, "validation_error")
    err(client.get(f"{V1}/sensors", params={"status": "zzz"}), 422, "validation_error")


def test_sensor_list_filter_sort_page_and_near(client):
    r = client.get(f"{V1}/sensors", params={"limit": 2, "offset": 1}).json()
    assert r["total"] >= 5 and [s["sensor_id"] for s in r["items"]] == [2, 3] and r["limit"] == 2 and r["offset"] == 1
    for sid, body in ((9201, {"name": "Zulu bridge", "tags": ["audit"], "status": "inactive"}), (9202, {"name": "Alpha bridge", "tags": ["audit"]})):
        assert client.post(f"{V1}/sensors", json={"sensor_id": sid, "latitude": 38.0, "longitude": -122.5, **body}).status_code == 201
    try:
        assert [s["sensor_id"] for s in client.get(f"{V1}/sensors", params={"tag": "audit", "sort": "name"}).json()["items"]] == [9202, 9201]
        assert [s["sensor_id"] for s in client.get(f"{V1}/sensors", params={"tag": "audit", "status": "inactive"}).json()["items"]] == [9201]
        assert client.get(f"{V1}/sensors", params={"q": "ALPHA"}).json()["total"] == 1 and client.get(f"{V1}/sensors", params={"q": "9201"}).json()["total"] == 1
        assert client.get(f"{V1}/sensors", params={"source": "api", "bbox": "-123,37.9,-122,38.1"}).json()["total"] == 2
        near = client.get(f"{V1}/sensors/near", params={"lat": 37.31, "lon": -121.90, "radius_m": 1500}).json()
        assert [s["sensor_id"] for s in near][0] == 1 and near[0]["distance_m"] < 1 and all(s["distance_m"] <= 1500 for s in near)
    finally:
        for sid in (9201, 9202):
            client.delete(f"{V1}/sensors/{sid}")
    one = client.get(f"{V1}/sensors/1").json()
    assert one["hotspot"]["rank"] == 1 and one["hotspot"]["class"] == "persistent hotspot" and one["stats"]["free_flow_speed_mph"] == 61.0


def test_dataset_sensor_edit_survives_loader_rerun(client):
    db = client.app.state.db
    assert client.patch(f"{V1}/sensors/2", json={"name": "Edited by analyst", "tags": ["watch"]}).status_code == 200
    MongoWriter(db).upsert_dataset_sensors([{"sensor_id": 2, "latitude": 37.32, "longitude": -121.90, "stats": {"free_flow_speed_mph": 99.0}}])
    d = client.get(f"{V1}/sensors/2").json()
    assert d["name"] == "Edited by analyst" and d["tags"] == ["watch"] and d["stats"]["free_flow_speed_mph"] == 99.0
    assert client.put(f"{V1}/sensors/2", json={"latitude": 37.32, "longitude": -121.90, "name": "Sensor 2"}).status_code == 200   # restore


# ------------------------------------------------------------------------------------------------ settings CRUD and effects
def test_settings_defaults_and_listing(client, fresh_settings):
    s = {x["key"]: x for x in client.get(f"{V1}/settings").json()}
    assert s["alert_threshold"]["is_default"] and s["alert_threshold"]["value"] == 0.686 and s["alert_threshold"]["version"] is None
    assert s["alert_threshold"]["minimum"] == 0.01 and s["sensor_status_filter"]["choices"][0] == "active"
    assert client.get(f"{V1}/settings", params={"overridden_only": True}).json() == []
    assert {x["key"] for x in client.get(f"{V1}/settings", params={"category": "hotspots"}).json()} == {"watchlist_size", "hotspot_min_persistence"}
    assert client.get(f"{V1}/settings/watchlist_size").json()["value"] == 10
    err(client.get(f"{V1}/settings/no_such_key"), 404, "not_found")
    err(client.get(f"{V1}/settings/Bad%20Key"), 422, "validation_error")


def test_settings_lifecycle_known_key(client, fresh_settings):
    r = client.put(f"{V1}/settings/alert_threshold", json={"value": 0.5}, headers={"X-Updated-By": "ana"})
    assert r.status_code == 201 and r.json()["value"] == 0.5 and r.json()["version"] == 1 and r.json()["updated_by"] == "ana" and not r.json()["is_default"]
    r = client.put(f"{V1}/settings/alert_threshold", json={"value": 0.8}, headers={"If-Match": '"1"'})
    assert r.status_code == 200 and r.json()["version"] == 2 and r.headers["ETag"] == '"2"'
    err(client.put(f"{V1}/settings/alert_threshold", json={"value": 0.9}, headers={"If-Match": '"1"'}), 412, "precondition_failed")
    r = client.patch(f"{V1}/settings/alert_threshold", json={"description": "tuned in review"})
    assert r.json()["description"] == "tuned in review" and r.json()["value"] == 0.8 and r.json()["version"] == 3
    err(client.patch(f"{V1}/settings/watchlist_size", json={"value": 5}), 404, "not_found")
    err(client.post(f"{V1}/settings", json={"key": "alert_threshold", "value": 0.4}), 409, "conflict")
    assert client.delete(f"{V1}/settings/alert_threshold").status_code == 204
    again = client.get(f"{V1}/settings/alert_threshold").json()
    assert again["is_default"] and again["value"] == 0.686                                  # reverted to the default
    err(client.delete(f"{V1}/settings/alert_threshold"), 404, "not_found")


@pytest.mark.parametrize("key,value", [("alert_threshold", 1.5), ("alert_threshold", "high"), ("alert_threshold", True), ("alert_threshold", None),
                                       ("watchlist_size", 2.5), ("watchlist_size", 0), ("default_page_size", 501), ("sensor_status_filter", "gone"),
                                       ("custom.big", "x" * 9000)])
def test_settings_value_validation(client, fresh_settings, key, value):
    err(client.put(f"{V1}/settings/{key}", json={"value": value}), 422, "validation_error")
    assert client.get(f"{V1}/settings", params={"overridden_only": True}).json() == []


def test_custom_settings_crud(client, fresh_settings):
    r = client.post(f"{V1}/settings", json={"key": "custom.dashboard", "value": {"layers": ["hotspots", "alerts"], "zoom": 11}, "description": "map layout"})
    assert r.status_code == 201 and r.headers["Location"] == f"{V1}/settings/custom.dashboard" and r.json()["category"] == "custom" and not r.json()["known"]
    assert client.get(f"{V1}/settings/custom.dashboard").json()["value"]["layers"] == ["hotspots", "alerts"]
    assert client.patch(f"{V1}/settings/custom.dashboard", json={"value": [1, 2, 3]}).json()["value"] == [1, 2, 3]
    assert [s["key"] for s in client.get(f"{V1}/settings", params={"overridden_only": True}).json()] == ["custom.dashboard"]
    err(client.post(f"{V1}/settings", json={"key": "Bad Key", "value": 1}), 422, "validation_error")
    err(client.post(f"{V1}/settings", json={"key": "custom.x", "value": 1, "extra": 1}), 422, "validation_error")
    err(client.patch(f"{V1}/settings/custom.dashboard", json={}), 422, "validation_error")
    assert client.delete(f"{V1}/settings/custom.dashboard").status_code == 204
    err(client.get(f"{V1}/settings/custom.dashboard"), 404, "not_found")


def test_settings_change_api_behaviour(client, fresh_settings):
    latest = client.get(f"{V1}/predictions/latest").json()
    assert latest["slot"] == 4988881 and latest["threshold"] == 0.686 and [p["sensor_id"] for p in latest["items"]][:2] == [5, 4]
    assert [p["alert"] for p in latest["items"]] == [True, True, False, False, False] and len(latest["items"]) == 5
    client.put(f"{V1}/settings/alert_threshold", json={"value": 0.3})                    # lower threshold -> more alerts, same stored data
    low = client.get(f"{V1}/predictions/latest", params={"alerts_only": True}).json()
    assert low["threshold"] == 0.3 and low["total"] == 4 and all(p["alert"] for p in low["items"])
    assert low["items"][0]["stored_alert"] is True and any(not p["stored_alert"] for p in low["items"])
    assert client.get(f"{V1}/predictions/overview").json()["overall"]["alerts"] == 8      # 2 slots x 4 sensors >= 0.3
    client.put(f"{V1}/settings/default_page_size", json={"value": 2})
    assert len(client.get(f"{V1}/predictions/latest").json()["items"]) == 2 and client.get(f"{V1}/sensors").json()["limit"] == 2
    client.put(f"{V1}/settings/watchlist_size", json={"value": 2})
    client.put(f"{V1}/settings/hotspot_min_persistence", json={"value": 0.39})           # persistence = 1 - 0.15*rank: ranks 1..4 qualify
    w = client.get(f"{V1}/hotspots/watchlist").json()
    assert w["size"] == 2 and w["eligible_sensors"] == 4 and [h["sensor_id"] for h in w["items"]] == [1, 2]
    client.put(f"{V1}/settings/prediction_window_hours", json={"value": 1})
    assert client.get(f"{V1}/predictions/overview").json()["window_hours"] == 1


# ------------------------------------------------------------------------------------------------ read-only resources
def test_hotspots_predictions_analytics_models_streaming_pipeline(client, fresh_settings):
    h = client.get(f"{V1}/hotspots", params={"class": "persistent hotspot", "sort": "persistence", "order": "asc"}).json()
    assert h["total"] == 3 and [x["sensor_id"] for x in h["items"]] == [3, 2, 1] and h["items"][0]["hotspot_class"] == "persistent hotspot"
    assert client.get(f"{V1}/hotspots", params={"lat": 37.31, "lon": -121.90, "radius_m": 500}).json()["total"] == 1
    err(client.get(f"{V1}/hotspots", params={"lat": 37.3}), 422, "validation_error")
    assert {c["class"]: c["sensors"] for c in client.get(f"{V1}/hotspots/classes").json()} == {"persistent hotspot": 3, "occasional": 2}
    err(client.get(f"{V1}/hotspots/999"), 404, "not_found")

    hist = client.get(f"{V1}/predictions/sensors/3", params={"hours": 1}).json()
    assert [p["slot"] for p in hist] == [4988881, 4988880] and client.get(f"{V1}/predictions/sensors/777").json() == []
    ov = client.get(f"{V1}/predictions/overview", params={"window_hours": 2}).json()
    assert ov["overall"]["predictions"] == 10 and ov["overall"]["sensors"] == 5 and [s["slot"] for s in ov["by_slot"]] == [4988880, 4988881]

    qs = client.get(f"{V1}/analytics/questions").json()
    assert qs[0]["qid"] == "q5" and qs[0]["findings"] == 2
    q = client.get(f"{V1}/analytics/questions/q5").json()
    assert q["figure_urls"] == [f"{V1}/analytics/figures/q5_map.png"] and "_id" not in q
    t = client.get(f"{V1}/analytics/questions/q5/tables/top", params={"limit": 2, "offset": 3}).json()
    assert t["n_rows"] == 5 and [r["sensor_id"] for r in t["rows"]] == [4, 5]
    assert client.get(f"{V1}/analytics/questions/q5/tables").json()[0]["columns"] == ["sensor_id", "v"]
    err(client.get(f"{V1}/analytics/questions/q99"), 422, "validation_error")
    err(client.get(f"{V1}/analytics/questions/q9"), 404, "not_found")

    assert [m["model"] for m in client.get(f"{V1}/models/evaluations").json()] == ["logistic_regression", "persistence"]
    assert client.get(f"{V1}/models/evaluations/best").json()["model"] == "logistic_regression"
    assert client.get(f"{V1}/models/evaluations", params={"kind": "baseline"}).json()[0]["model"] == "persistence"
    err(client.get(f"{V1}/models/evaluations/xgboost"), 404, "not_found")

    assert client.get(f"{V1}/streaming/runs").json()[0]["run"] == "r1"
    s = client.get(f"{V1}/streaming/metrics/summary", params={"run": "r1"}).json()[0]
    assert s["query"] == "predictions" and s["batches"] == 4 and s["input_rows"] == 600 and s["batch_ms_max"] == 1300 and 1000 <= s["batch_ms_p50"] <= 1300
    pr = client.get(f"{V1}/streaming/metrics/progress", params={"run": "r1", "limit": 2}).json()
    assert pr["total"] == 4 and len(pr["items"]) == 2 and "progress" not in pr["items"][0]
    assert client.get(f"{V1}/streaming/reports").json()[0]["end_to_end_latency"]["since_slot_complete"]["p50_s"] == 7.0
    assert client.get(f"{V1}/streaming/health").json()["healthy"] is True and client.get(f"{V1}/streaming/health", params={"max_age_s": 0.000001}).json()["healthy"] is False

    assert {r["kind"]: r["failed"] for r in client.get(f"{V1}/pipeline/status").json()} == {"analytics": 0, "ml_training": 1}
    runs = client.get(f"{V1}/pipeline/runs", params={"status": "failed"}).json()
    assert runs["total"] == 1 and runs["items"][0]["name"] == "train_model"
    assert client.get(f"{V1}/pipeline/runs/analytics:q5").json()["duration_seconds"] == 5.4
    err(client.get(f"{V1}/pipeline/runs/nope"), 404, "not_found")


def test_figures_are_served_safely(mongo, tmp_path):
    figs = tmp_path / "figures" / "analytics"
    figs.mkdir(parents=True)
    (figs / "ok.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"0" * 32)
    (figs / "notes.txt").write_text("secret")
    (tmp_path / "secret.png").write_bytes(b"outside")
    with TestClient(create_app(cfg(reports_dir=tmp_path))) as c:
        r = c.get(f"{V1}/analytics/figures/ok.png")
        assert r.status_code == 200 and r.headers["content-type"] == "image/png" and r.content.startswith(b"\x89PNG")
        for bad in ("notes.txt", "missing.png", "..%2Fsecret.png", "..%5Csecret.png", "%2e%2e%2fsecret.png"):
            assert c.get(f"{V1}/analytics/figures/{bad}").status_code == 404, bad


# ------------------------------------------------------------------------------------------------ model inference (validation; real model below)
def _instance(**over):
    return {"sensor_id": 1, "features": {"speed": 55.0, "hour": 8, "day_of_week": 2, **over}}


def test_predict_validation_and_unavailable(client):
    err(client.post(f"{V1}/models/predict", json={"instances": [_instance()]}), 503, "model_unavailable")       # inference disabled in this app
    for body in ({"instances": []}, {"instances": [{"features": {"hour": 8}}]}, {"instances": [_instance(not_a_feature=1)]},
                 {"instances": [_instance(speed="fast")]}, {"instances": [_instance()], "threshold": 1.5}, {"instances": [_instance()], "extra": 1}, {}):
        err(client.post(f"{V1}/models/predict", json=body), 422, "validation_error")
    info = client.get(f"{V1}/models/info").json()
    assert info["loaded"] is False and (not info["available"] or info["algorithm"] in ("logistic_regression", "decision_tree", "random_forest"))


# ------------------------------------------------------------------------------------------------ security, resilience, persistence
def test_api_key_protects_writes_only(mongo):
    with TestClient(create_app(cfg(api_key="k" * 24))) as c:
        body = {"sensor_id": 9301, "latitude": 1.0, "longitude": 2.0}
        assert c.get(f"{V1}/sensors/1").status_code == 200 and c.get(f"{V1}/settings").status_code == 200                # reads stay open
        e = err(c.post(f"{V1}/sensors", json=body), 401, "unauthorized")
        assert "api" in e["message"].lower()
        r = c.post(f"{V1}/sensors", json=body, headers={"X-API-Key": "wrong"})
        assert r.status_code == 401 and r.headers["WWW-Authenticate"] == "ApiKey"
        for method, url, kw in (("put", "/sensors/1", {"json": {"latitude": 1, "longitude": 1}}), ("patch", "/sensors/1", {"json": {"name": "x"}}),
                                ("delete", "/sensors/1", {}), ("put", "/settings/alert_threshold", {"json": {"value": 0.5}}),
                                ("delete", "/settings/alert_threshold", {}), ("post", "/settings", {"json": {"key": "custom.k", "value": 1}})):
            assert getattr(c, method)(f"{V1}{url}", **kw).status_code == 401, (method, url)
        ok = {"X-API-Key": "k" * 24}
        assert c.post(f"{V1}/sensors", json=body, headers=ok).status_code == 201
        assert c.delete(f"{V1}/sensors/9301", headers=ok).status_code == 204
        assert c.get(f"{V1}/status").json()["write_protection"] == "api-key"
        assert "k" * 24 not in json.dumps(c.get(f"{V1}/status").json())


def test_production_requires_api_key_and_explicit_origins():
    with pytest.raises(ValueError, match="API_KEY"):
        ApiSettings(_env_file=None, env="production")
    with pytest.raises(ValueError, match="CORS"):
        ApiSettings(_env_file=None, env="production", api_key="k" * 20, cors_origins="*")
    assert ApiSettings(_env_file=None, env="production", api_key="k" * 20).cors_origin_list == ["http://localhost:3000", "http://127.0.0.1:3000"]


def test_database_down_gives_503_not_a_crash():
    with TestClient(create_app(cfg(mongo_uri="mongodb://127.0.0.1:1", mongo_timeout_ms=300))) as c:
        assert c.get("/health").status_code == 200                                              # liveness does not need the database
        err(c.get("/ready"), 503, "not_ready")
        r = c.get(f"{V1}/sensors")
        err(r, 503, "database_unavailable")
        assert r.headers["Retry-After"] == "5"
        err(c.post(f"{V1}/sensors", json={"sensor_id": 5, "latitude": 1, "longitude": 1}), 503, "database_unavailable")


def test_changes_survive_application_restart(mongo):
    """Create/update/delete through one application instance, shut it down, start a new one: the data is still there."""
    with TestClient(create_app(cfg())) as c1:
        c1.post(f"{V1}/sensors", json={"sensor_id": 9401, "latitude": 37.0, "longitude": -122.0, "name": "Persistent"})
        c1.post(f"{V1}/sensors", json={"sensor_id": 9402, "latitude": 37.0, "longitude": -122.0})
        c1.patch(f"{V1}/sensors/9401", json={"status": "maintenance", "tags": ["survives"]})
        c1.delete(f"{V1}/sensors/9402")
        c1.put(f"{V1}/settings/watchlist_size", json={"value": 7}, headers={"X-Updated-By": "ana"})
        c1.post(f"{V1}/settings", json={"key": "custom.persist", "value": {"a": [1, 2]}})
    with TestClient(create_app(cfg())) as c2:                                                   # a brand-new process-like instance
        s = c2.get(f"{V1}/sensors/9401").json()
        assert s["name"] == "Persistent" and s["status"] == "maintenance" and s["tags"] == ["survives"] and s["version"] == 2
        assert c2.get(f"{V1}/sensors/9402").status_code == 404
        w = c2.get(f"{V1}/settings/watchlist_size").json()
        assert w["value"] == 7 and w["updated_by"] == "ana" and not w["is_default"]
        assert c2.get(f"{V1}/settings/custom.persist").json()["value"] == {"a": [1, 2]}
        assert len(c2.get(f"{V1}/hotspots/watchlist").json()["items"]) == 3                       # the persisted setting drives behaviour (7 > 3 eligible)
        c2.delete(f"{V1}/sensors/9401")
        c2.delete(f"{V1}/settings/watchlist_size")
        c2.delete(f"{V1}/settings/custom.persist")


# ------------------------------------------------------------------------------------------------ real model
MODEL_DIR = Path(ApiSettings(_env_file=None).model_path)


@pytest.mark.skipif(not (MODEL_DIR / "bundle.json").exists(), reason="Phase 7 model not available")
def test_model_inference_matches_the_values_saved_at_training_time(mongo):
    """Score real Phase-3 feature rows through the API and compare with probabilities recorded in reference.json at save time."""
    ref = json.loads((MODEL_DIR / "reference.json").read_text())[:3]
    with TestClient(create_app(cfg(enable_model_inference=True))) as c:
        assert c.get(f"{V1}/models/info").json()["loaded"] is False
        for i, r in enumerate(ref):
            ex = c.get(f"{V1}/models/example", params={"sensor_id": int(r["sensor_id"]), "ts_utc": r["ts_utc"].replace(" ", "T") + "Z"})
            assert ex.status_code == 200, ex.text
            out = c.post(f"{V1}/models/predict", json={"instances": [ex.json()["instance"]], "threshold": 0.5})
            assert out.status_code == 200, out.text
            o = out.json()
            assert o["cold_start"] is (i == 0) and o["threshold_source"] == "request" and o["count"] == 1
            assert o["predictions"][0]["probability"] == pytest.approx(r["p"], abs=1e-9)
            assert o["predictions"][0]["alert"] is (r["p"] >= 0.5)
        assert c.get(f"{V1}/models/info").json()["loaded"] is True
        inst = _instance(speed=10.0, hour=8, day_of_week=2, speed_lag_5m=12.0, congested_now=1.0, speed_ratio_ff=0.15)
        both = c.post(f"{V1}/models/predict", json={"instances": [inst, _instance(speed=70.0, speed_ratio_ff=1.05, congested_now=0.0)]}).json()
        slow, free = both["predictions"]
        assert 0 <= free["probability"] <= 1 and slow["probability"] > free["probability"] and slow["imputed_features"] > 30           # nulls are imputed
        assert both["threshold_source"] == "default" and both["threshold"] == pytest.approx(0.686)


# ------------------------------------------------------------------------------------------------ dashboard support endpoints (Phase 10)
def test_prediction_slots_playback_database_info_and_performance(client, fresh_settings):
    s = client.get(f"{V1}/predictions/slots").json()
    assert s["threshold"] == 0.686 and [x["slot"] for x in s["slots"]] == [4988880, 4988881]
    assert s["slots"][1]["predictions"] == 5 and s["slots"][1]["alerts"] == 2 and s["slots"][0]["max_probability"] == 1.0
    old = client.get(f"{V1}/predictions/latest", params={"slot": 4988880}).json()                     # playback: an earlier slot
    assert old["slot"] == 4988880 and old["items"][0]["probability"] == 1.0 and len(old["items"]) == 5
    empty = client.get(f"{V1}/predictions/latest", params={"slot": 1}).json()
    assert empty["total"] == 0 and empty["items"] == [] and empty["slot"] == 1
    client.put(f"{V1}/settings/alert_threshold", json={"value": 0.3})
    assert client.get(f"{V1}/predictions/slots").json()["slots"][0]["alerts"] == 4                   # slot summaries follow the setting

    info = client.get(f"{V1}/status/database").json()
    names = {c["name"]: c for c in info["collections"]}
    assert info["database"] == DB and info["server_version"].startswith("7") and info["ping_ms"] >= 0
    assert names["sensor_metadata"]["validator"] and "location_2dsphere" in names["sensor_metadata"]["indexes"] and names["traffic_predictions"]["documents"] == 10
    assert {"sensor_metadata", "analysis_results", "congestion_hotspots", "traffic_predictions", "model_evaluations", "stream_metrics", "pipeline_runs", "settings"} <= set(names)

    db = client.app.state.db
    now = datetime.now(timezone.utc)
    MongoWriter(db).replace_documents(S.PIPELINE_RUNS, [
        {"_id": "benchmark:storage", "kind": "spark_benchmark", "name": "storage", "status": "succeeded", "finished_at": now, "summary": {"title": "Storage", "full_scan": [{"case": "scan_csv", "seconds_median": 23.0}]}},
        {"_id": "ml:training2", "kind": "ml_training", "name": "train_model", "status": "succeeded", "finished_at": now, "summary": {"candidates_table": [{"algorithm": "random_forest", "train_s": 97.0}]}}])
    try:
        p = client.get(f"{V1}/performance/overview").json()
        assert p["spark_benchmarks"]["storage"]["full_scan"][0]["seconds_median"] == 23.0 and p["analytics_jobs"][0] == {"qid": "q5", "seconds": 5.4, "status": "succeeded"}
        assert p["ml_training"]["summary"]["candidates_table"][0]["train_s"] == 97.0
        assert p["streaming"]["summary"][0]["query"] == "predictions" and p["streaming"]["reports"][0]["run"] == "r1"
    finally:
        db[S.PIPELINE_RUNS].delete_many({"_id": {"$in": ["benchmark:storage", "ml:training2"]}})
