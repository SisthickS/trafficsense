"""End-to-end integration tests for the whole platform: Next.js dashboard -> /backend proxy -> FastAPI -> MongoDB.

The test boots its OWN API (port 8011) and its OWN production-mode dashboard (port 3111) against the real `trafficsense`
database, so it does not depend on servers that are already running. It skips when MongoDB, the loaded data, Node.js or
a production build (`npm run build` in frontend/) is missing.

Run:  pytest -q tests/test_integration_e2e.py
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from src.api.config import ApiSettings
from src.mongo.connection import make_client

ROOT = Path(__file__).resolve().parents[1]
FRONT = ROOT / "frontend"
API_PORT, WEB_PORT = 8011, 3111
API, WEB = f"http://127.0.0.1:{API_PORT}", f"http://127.0.0.1:{WEB_PORT}"
PAGES = {"/": "Predict congestion", "/analytics": "Traffic Analytics", "/hotspots": "Congestion Hotspots", "/live": "Simulated Live Traffic",
         "/models": "Model Evaluation", "/performance": "Spark Performance", "/data": "Data Management", "/status": "Analysis History"}
SENSOR_A, SENSOR_B = 9910001, 9910002


def _wait(url: str, timeout: float = 90.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            if httpx.get(url, timeout=3).status_code < 500:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    raise TimeoutError(url)


class Stack:
    """Owns the two child processes so a test can stop and restart the API."""

    def __init__(self) -> None:
        self.api: subprocess.Popen | None = None
        self.web: subprocess.Popen | None = None
        self.env = {**os.environ, "TRAFFICSENSE_ENABLE_MODEL_INFERENCE": "false", "TRAFFICSENSE_API_URL": API, "NEXT_TELEMETRY_DISABLED": "1"}
        self.env.pop("TRAFFICSENSE_API_KEY", None)

    def start_api(self) -> None:
        self.api = subprocess.Popen([sys.executable, "-m", "uvicorn", "src.api.main:app", "--host", "127.0.0.1", "--port", str(API_PORT), "--log-level", "warning"],
                                    cwd=ROOT, env=self.env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        _wait(f"{API}/ready")

    def stop_api(self) -> None:
        if self.api and self.api.poll() is None:
            self.api.kill()  # SIGKILL: no graceful shutdown, like a crash
            self.api.wait(10)

    def start_web(self) -> None:
        node = shutil.which("node")
        self.web = subprocess.Popen([node, "node_modules/next/dist/bin/next", "start", "-p", str(WEB_PORT), "-H", "127.0.0.1"], cwd=FRONT, env=self.env,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        _wait(f"{WEB}/")

    def stop_all(self) -> None:
        for p in (self.web, self.api):
            if p and p.poll() is None:
                p.terminate()
                try:
                    p.wait(10)
                except subprocess.TimeoutExpired:
                    p.kill()


@pytest.fixture(scope="module")
def db():
    client = make_client(timeout_ms=1500)
    try:
        client.admin.command("ping")
    except Exception:
        pytest.skip("MongoDB not reachable (docker compose up -d --wait mongo)")
    database = client[ApiSettings(_env_file=None).mongo_db]
    if database["analysis_results"].count_documents({}) < 90 or database["sensor_metadata"].count_documents({}) < 300:
        pytest.skip("Mongo is not loaded (python -m src.mongo.load_outputs)")
    yield database
    database["sensor_metadata"].delete_many({"sensor_id": {"$in": [SENSOR_A, SENSOR_B]}})
    client.close()


@pytest.fixture(scope="module")
def stack(db):
    if not shutil.which("node") or not (FRONT / ".next" / "BUILD_ID").exists() or not (FRONT / "node_modules" / "next").exists():
        pytest.skip("Node.js / production build missing (cd frontend && npm ci && npm run build)")
    s = Stack()
    try:
        s.start_api()
        s.start_web()
        yield s
    finally:
        s.stop_all()


@pytest.fixture()
def web(stack):
    with httpx.Client(base_url=WEB, timeout=30) as c:
        yield c


def test_every_page_renders_with_the_persistent_navigation(web):
    for path, marker in PAGES.items():
        r = web.get(path)
        assert r.status_code == 200, path
        assert "TrafficSense" in r.text and "Project scope" in r.text, path
        assert marker in r.text, f"{path}: {marker!r} not found"
        for href in PAGES:
            assert f'href="{href}"' in r.text, f"nav link {href} missing on {path}"
    assert web.get("/does-not-exist").status_code == 404


def test_proxy_serves_the_same_live_data_as_the_api(web):
    direct = httpx.get(f"{API}/api/v1/status", timeout=10).json()
    via = web.get("/backend/api/v1/status").json()
    assert via["counts"] == direct["counts"]
    assert via["counts"]["analysis_results"] >= 90
    for path in ("/api/v1/analytics/questions/q5", "/api/v1/hotspots?limit=5", "/api/v1/models/evaluations", "/api/v1/performance/overview"):
        a, b = httpx.get(f"{API}{path}", timeout=20), web.get(f"/backend{path}")
        assert a.status_code == b.status_code == 200, path
        assert a.json() == b.json(), path
    a, b = httpx.get(f"{API}/api/v1/status/database", timeout=20).json(), web.get("/backend/api/v1/status/database").json()
    assert a["collections"] == b["collections"] and a["server_version"] == b["server_version"]   # ping_ms is a live measurement


def test_every_question_is_served_with_findings_and_tables(web):
    for i in range(1, 19):
        q = web.get(f"/backend/api/v1/analytics/questions/q{i}")
        assert q.status_code == 200, i
        doc = q.json()
        assert doc["findings"] and doc["tables"], f"q{i} has no findings/tables"
        t = web.get(f"/backend/api/v1/analytics/questions/q{i}/tables/{doc['tables'][0]}?limit=5")
        assert t.status_code == 200 and t.json()["rows"], f"q{i} first table empty"


def test_live_traffic_endpoints_return_scored_predictions(web):
    slots = web.get("/backend/api/v1/predictions/slots").json()
    assert slots["slots"] if "slots" in slots else slots
    latest = web.get("/backend/api/v1/predictions/latest?limit=5").json()
    assert latest["items"] and all(0 <= p["probability"] <= 1 for p in latest["items"])


def test_crud_through_the_proxy_is_persisted_in_mongodb(web, db):
    col = db["sensor_metadata"]
    col.delete_many({"sensor_id": {"$in": [SENSOR_A, SENSOR_B]}})
    body = {"sensor_id": SENSOR_A, "latitude": 37.6, "longitude": -122.0, "name": "e2e sensor", "tags": ["e2e"]}

    r = web.post("/backend/api/v1/sensors", json=body)
    assert r.status_code == 201 and r.json()["version"] == 1
    doc = col.find_one({"sensor_id": SENSOR_A})
    assert doc and doc["name"] == "e2e sensor" and doc["source"] == "api"

    assert web.post("/backend/api/v1/sensors", json=body).status_code == 409           # duplicate id

    r = web.patch(f"/backend/api/v1/sensors/{SENSOR_A}", json={"name": "renamed", "status": "maintenance"}, headers={"If-Match": '"1"', "X-Updated-By": "pytest"})
    assert r.status_code == 200 and r.json()["version"] == 2
    doc = col.find_one({"sensor_id": SENSOR_A})
    assert (doc["name"], doc["status"], doc["version"]) == ("renamed", "maintenance", 2)

    stale = web.patch(f"/backend/api/v1/sensors/{SENSOR_A}", json={"name": "stale"}, headers={"If-Match": '"1"'})
    assert stale.status_code == 412 and stale.json()["error"]["code"]
    assert col.find_one({"sensor_id": SENSOR_A})["name"] == "renamed"

    bad = web.post("/backend/api/v1/sensors", json={"sensor_id": SENSOR_B, "latitude": 999, "longitude": 0})
    assert bad.status_code == 422 and bad.json()["error"]["details"]
    assert col.count_documents({"sensor_id": SENSOR_B}) == 0

    assert web.delete(f"/backend/api/v1/sensors/{SENSOR_A}").status_code == 204
    assert col.count_documents({"sensor_id": SENSOR_A}) == 0
    assert web.get(f"/backend/api/v1/sensors/{SENSOR_A}").status_code == 404


def test_settings_persist_across_an_api_crash_and_restart(stack, web, db):
    key = "default_page_size"
    before = web.get(f"/backend/api/v1/settings/{key}").json()
    new = 75 if before["value"] != 75 else 80
    try:
        assert web.put(f"/backend/api/v1/settings/{key}", json={"value": new}).status_code in (200, 201)   # 201 when first stored
        assert db["settings"].find_one({"_id": key})["value"] == new

        stack.stop_api()
        down = web.get("/backend/api/v1/settings")
        assert down.status_code == 502 and down.json()["error"]["code"] == "api_unreachable"   # gateway degrades cleanly
        assert web.get("/analytics").status_code == 200                                       # pages still render (data loads client-side)

        stack.start_api()
        assert web.get(f"/backend/api/v1/settings/{key}").json()["value"] == new             # survived the crash
    finally:
        if stack.api is None or stack.api.poll() is not None:
            stack.start_api()
        if before.get("is_default"):
            web.delete(f"/backend/api/v1/settings/{key}")
        else:
            web.put(f"/backend/api/v1/settings/{key}", json={"value": before["value"]})
    assert web.get(f"/backend/api/v1/settings/{key}").json()["value"] == before["value"]
