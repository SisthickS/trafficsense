"""End-to-end verification of the API against the real MongoDB, with real server processes.

    python -m src.api.verify_api [--port 8765] [--db trafficsense]

  1. start `uvicorn src.api.main:app` as a separate process, check readiness and the loaded collections
  2. CRUD over HTTP: sensors (create / read / patch / put / delete, If-Match), settings (create / put / patch / delete / reset),
     an edit of a real dataset sensor, plus the error paths (422, 404, 409, 412)
  3. read the documents straight from MongoDB (independent client): the writes are really stored
  4. SIGKILL the server (no graceful shutdown), start a NEW process, and compare every document and behaviour change
  5. stop the MongoDB container (outage: the API must answer 503 and stay up), start it again (the API must recover by itself)
  6. re-run the data loader: analyst edits must survive it, dataset statistics must refresh
  7. score a stored streaming prediction through POST /models/predict (saved Spark model inside the API process)
  8. clean up everything this script created and write reports/api/verification.{md,json} + openapi.json
Exit code 1 if any check fails.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from src.analytics.common import df_to_md
from src.common.paths import PROJECT_ROOT, REPORTS_DIR, SCRATCH_DIR
from src.mongo.connection import make_client

OUT = REPORTS_DIR / "api"
LOG_DIR = SCRATCH_DIR / "api"
NEW, TMP = 9_000_001, 9_000_002
EDIT = 400001                                    # a real dataset sensor that the script edits and later restores
MONGO_CONTAINER = "trafficsense-mongo"

checks: list[dict] = []
transcript: list[dict] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    checks.append({"check": name, "ok": bool(ok), "detail": detail})
    print(f"  [{'OK' if ok else 'FAIL':4}] {name}  {detail}")
    return bool(ok)


class Http:
    """httpx client that records every call (method, path, status, ms) for the report."""

    def __init__(self, base: str, section: str = ""):
        self.c, self.section = httpx.Client(base_url=base, timeout=180), section

    def call(self, method: str, url: str, **kw) -> httpx.Response:
        t0 = time.perf_counter()
        r = self.c.request(method, url, **kw)
        transcript.append({"step": self.section, "request": f"{method} {url}", "status": r.status_code, "ms": round((time.perf_counter() - t0) * 1000, 1),
                           "note": (r.json().get("error", {}).get("code") if r.status_code >= 400 and "json" in r.headers.get("content-type", "") else "")})
        return r

    def get(self, url, **kw): return self.call("GET", url, **kw)
    def post(self, url, **kw): return self.call("POST", url, **kw)
    def put(self, url, **kw): return self.call("PUT", url, **kw)
    def patch(self, url, **kw): return self.call("PATCH", url, **kw)
    def delete(self, url, **kw): return self.call("DELETE", url, **kw)


def start_server(port: int, tag: str) -> subprocess.Popen:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = open(LOG_DIR / f"uvicorn_{tag}.log", "wb")
    return subprocess.Popen([sys.executable, "-m", "uvicorn", "src.api.main:app", "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
                            cwd=PROJECT_ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True, env={**os.environ})


def wait_ready(base: str, proc: subprocess.Popen, timeout: float = 90) -> float:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early (code {proc.returncode}); see {LOG_DIR}")
        try:
            if httpx.get(f"{base}/ready", timeout=2).status_code == 200:
                return time.time() - t0
        except httpx.HTTPError:
            pass
        time.sleep(0.25)
    raise TimeoutError("server did not become ready")


def hard_kill(proc: subprocess.Popen) -> None:
    os.killpg(proc.pid, signal.SIGKILL)
    proc.wait()


def stable(doc: dict, keys: tuple) -> dict:
    return {k: doc.get(k) for k in keys}


SENSOR_KEYS = ("sensor_id", "name", "description", "direction", "lanes", "status", "tags", "latitude", "longitude", "version", "updated_at", "created_at")
SETTING_KEYS = ("key", "value", "description", "version", "updated_at", "updated_by", "is_default")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--db", default=os.environ.get("TRAFFICSENSE_MONGO_DB", "trafficsense"))
    ap.add_argument("--skip-mongo-restart", action="store_true")
    ap.add_argument("--skip-inference", action="store_true")
    a = ap.parse_args(argv)
    base = f"http://127.0.0.1:{a.port}"
    os.environ["TRAFFICSENSE_MONGO_DB"] = a.db
    db = make_client()[a.db]
    t_start, procs = time.time(), []
    original: dict = {}
    try:
        # ------------------------------------------------------------------------------------------- 1. start + baseline
        print("1. start the API server (separate process)")
        p1 = start_server(a.port, "first"); procs.append(p1)
        check("server process starts and becomes ready", True, f"pid {p1.pid}, ready after {wait_ready(base, p1):.1f}s")
        h = Http(base, "1 baseline")
        st = h.get("/api/v1/status").json()
        counts = st["counts"]
        check("MongoDB holds the loaded collections", counts["sensor_metadata"] >= 325 and counts["congestion_hotspots"] == 325 and counts["analysis_results"] == 95
              and counts["model_evaluations"] == 5 and counts["traffic_predictions"] > 0 and counts["pipeline_runs"] >= 25, json.dumps(counts))
        original = h.get(f"/api/v1/sensors/{EDIT}").json()
        original.pop("hotspot", None)
        pre_settings = {s["key"] for s in h.get("/api/v1/settings", params={"overridden_only": True}).json()}
        db.sensor_metadata.delete_many({"_id": {"$in": [NEW, TMP]}})

        # ------------------------------------------------------------------------------------------- 2. CRUD over HTTP
        print("2. CRUD over HTTP")
        h = Http(base, "2 sensors")
        r = h.post("/api/v1/sensors", json={"sensor_id": NEW, "latitude": 37.4, "longitude": -122.0, "name": "Verification sensor", "tags": ["verify", "Temp"], "lanes": 3})
        created = r.json()
        check("CREATE sensor -> 201 + Location + ETag", r.status_code == 201 and r.headers["Location"].endswith(f"/{NEW}") and r.headers["ETag"] == '"1"', f"version {created.get('version')}")
        r = h.patch(f"/api/v1/sensors/{NEW}", json={"status": "maintenance", "description": "lane closure"})
        check("UPDATE (PATCH) changes only the given fields", r.status_code == 200 and r.json()["name"] == "Verification sensor" and r.json()["status"] == "maintenance"
              and r.json()["version"] == 2, f"version {r.json().get('version')}")
        r = h.put(f"/api/v1/sensors/{NEW}", json={"latitude": 37.41, "longitude": -122.01, "name": "Verification sensor (replaced)", "tags": ["verify"]}, headers={"If-Match": '"2"'})
        check("UPDATE (PUT + If-Match) replaces the editable fields", r.status_code == 200 and r.json()["version"] == 3 and r.json()["description"] is None
              and r.json()["status"] == "active", f"version {r.json().get('version')}, description reset")
        check("stale If-Match is refused (412)", h.patch(f"/api/v1/sensors/{NEW}", json={"name": "stale"}, headers={"If-Match": '"1"'}).status_code == 412)
        check("read back after the writes (GET)", h.get(f"/api/v1/sensors/{NEW}").json()["name"] == "Verification sensor (replaced)")
        h.post("/api/v1/sensors", json={"sensor_id": TMP, "latitude": 1.0, "longitude": 2.0})
        check("DELETE sensor -> 204, then GET and DELETE -> 404", h.delete(f"/api/v1/sensors/{TMP}").status_code == 204 and h.get(f"/api/v1/sensors/{TMP}").status_code == 404
              and h.delete(f"/api/v1/sensors/{TMP}").status_code == 404)
        check("validation errors are 422 with field details", (lambda r: r.status_code == 422 and r.json()["error"]["details"][0]["field"] == "body.latitude")(
            h.post("/api/v1/sensors", json={"sensor_id": 9_000_003, "latitude": 123, "longitude": 0})))
        check("duplicate create is 409", h.post("/api/v1/sensors", json={"sensor_id": NEW, "latitude": 1, "longitude": 1}).status_code == 409)
        r = h.patch(f"/api/v1/sensors/{EDIT}", json={"name": "I-880 N (analyst edit)", "tags": ["verify", "watch"], "status": "maintenance", "description": "edited by verify_api"})
        check("a real dataset sensor can be edited", r.status_code == 200 and r.json()["name"] == "I-880 N (analyst edit)" and r.json()["stats"] == original["stats"], f"sensor {EDIT}")

        h = Http(base, "2 settings")
        r = h.put("/api/v1/settings/alert_threshold", json={"value": 0.55}, headers={"X-Updated-By": "verify"})
        check("CREATE setting (PUT) -> 201", r.status_code == 201 and r.json()["value"] == 0.55 and r.json()["updated_by"] == "verify")
        check("UPDATE setting (PUT + If-Match) -> 200", h.put("/api/v1/settings/alert_threshold", json={"value": 0.6}, headers={"If-Match": '"1"'}).json()["version"] == 2)
        check("setting validation: out of range -> 422", h.put("/api/v1/settings/alert_threshold", json={"value": 7}).status_code == 422)
        h.put("/api/v1/settings/watchlist_size", json={"value": 5})
        check("CREATE custom setting (POST) -> 201", h.post("/api/v1/settings", json={"key": "custom.verify", "value": {"layers": ["hotspots"], "zoom": 11}}).status_code == 201)
        check("PATCH setting", h.patch("/api/v1/settings/custom.verify", json={"description": "persisted by verify_api"}).json()["version"] == 2)
        h.post("/api/v1/settings", json={"key": "custom.verify_tmp", "value": 1})
        check("DELETE setting -> 204, then 404", h.delete("/api/v1/settings/custom.verify_tmp").status_code == 204 and h.get("/api/v1/settings/custom.verify_tmp").status_code == 404)

        h = Http(base, "2 behaviour")
        before = {"watch": h.get("/api/v1/hotspots/watchlist").json(), "latest": h.get("/api/v1/predictions/latest", params={"alerts_only": True, "limit": 500}).json()}
        check("settings change API behaviour", len(before["watch"]["items"]) == 5 and before["latest"]["threshold"] == 0.6 and all(p["probability"] >= 0.6 for p in before["latest"]["items"]),
              f"watch-list size 5; {before['latest']['total']} sensors at/above 0.6")
        expected_sensors = {s: stable(h.get(f"/api/v1/sensors/{s}").json(), SENSOR_KEYS) for s in (NEW, EDIT)}
        expected_settings = {k: stable(h.get(f"/api/v1/settings/{k}").json(), SETTING_KEYS) for k in ("alert_threshold", "watchlist_size", "custom.verify")}

        # ------------------------------------------------------------------------------------------- 3. direct MongoDB read
        print("3. read the documents straight from MongoDB (independent client)")
        d = db.sensor_metadata.find_one({"_id": NEW})
        check("sensor document exists in MongoDB with the API's final values", d is not None and d["name"] == "Verification sensor (replaced)" and d["version"] == 3
              and d["location"]["coordinates"] == [-122.01, 37.41] and d["tags"] == ["verify"], f"version {d['version'] if d else None}, GeoJSON location updated")
        e = db.sensor_metadata.find_one({"_id": EDIT})
        check("edited dataset sensor stored; read-only stats untouched", e["name"] == "I-880 N (analyst edit)" and e["stats"] == original["stats"] and e["source"] == "dataset")
        s = db.settings.find_one({"_id": "alert_threshold"})
        check("setting document exists in MongoDB", s["value"] == 0.6 and s["version"] == 2 and s["category"] == "alerts" and db.settings.find_one({"_id": "custom.verify_tmp"}) is None)
        check("deleted sensor is gone from MongoDB", db.sensor_metadata.find_one({"_id": TMP}) is None)

        # ------------------------------------------------------------------------------------------- 4. crash + new process
        print("4. SIGKILL the server, start a new process")
        hard_kill(p1)
        try:
            httpx.get(f"{base}/health", timeout=2)
            gone = False
        except httpx.HTTPError:
            gone = True
        check("server process killed (SIGKILL, no graceful shutdown)", gone and p1.returncode == -signal.SIGKILL, f"exit code {p1.returncode}")
        p2 = start_server(a.port, "second"); procs.append(p2)
        check("a NEW server process starts", p2.pid != p1.pid, f"pid {p2.pid}, ready after {wait_ready(base, p2):.1f}s")
        h = Http(base, "4 after restart")
        got_s = {s: stable(h.get(f"/api/v1/sensors/{s}").json(), SENSOR_KEYS) for s in (NEW, EDIT)}
        check("sensor changes survived the restart (every field, version and timestamps identical)", got_s == expected_sensors, f"sensors {NEW} and {EDIT}")
        got_c = {k: stable(h.get(f"/api/v1/settings/{k}").json(), SETTING_KEYS) for k in expected_settings}
        check("setting changes survived the restart (value, version, updated_by, timestamps identical)", got_c == expected_settings, ", ".join(expected_settings))
        check("deletions survived the restart", h.get(f"/api/v1/sensors/{TMP}").status_code == 404 and h.get("/api/v1/settings/custom.verify_tmp").status_code == 404)
        after = {"watch": h.get("/api/v1/hotspots/watchlist").json(), "latest": h.get("/api/v1/predictions/latest", params={"alerts_only": True, "limit": 500}).json()}
        check("persisted settings still drive behaviour after the restart", len(after["watch"]["items"]) == 5 and after["latest"]["threshold"] == 0.6
              and after["latest"]["total"] == before["latest"]["total"], f"watch-list {len(after['watch']['items'])}, alerts {after['latest']['total']}")

        # ------------------------------------------------------------------------------------------- 5. MongoDB outage
        if not a.skip_mongo_restart:
            print("5. stop MongoDB (outage), then start it again")
            subprocess.run(["docker", "stop", MONGO_CONTAINER], check=True, capture_output=True)
            t_down = time.perf_counter()
            r_sensor = httpx.get(f"{base}/api/v1/sensors/{NEW}", timeout=30)
            waited = time.perf_counter() - t_down
            r_write = httpx.patch(f"{base}/api/v1/sensors/{NEW}", json={"name": "must not be written"}, timeout=30)
            check("during the outage reads and writes answer 503 database_unavailable (+ Retry-After), the process stays up",
                  r_sensor.status_code == 503 and r_sensor.json()["error"]["code"] == "database_unavailable" and r_write.status_code == 503
                  and r_sensor.headers.get("Retry-After") == "5" and httpx.get(f"{base}/health", timeout=5).status_code == 200
                  and httpx.get(f"{base}/ready", timeout=30).status_code == 503, f"503 after {waited:.1f}s (server selection timeout), /health 200, /ready 503")
            subprocess.run(["docker", "start", MONGO_CONTAINER], check=True, capture_output=True)
            t0, recovered_after = time.time(), None
            while time.time() - t0 < 90:
                try:
                    if httpx.get(f"{base}/api/v1/sensors/{NEW}", timeout=10).status_code == 200:
                        recovered_after = time.time() - t0
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(0.5)
            check("API recovers by itself when MongoDB is back (no API restart)", recovered_after is not None,
                  f"first 200 {recovered_after:.1f}s after `docker start`" if recovered_after else "did not recover")
            db = make_client()[a.db]
            h = Http(base, "5 after outage")
            check("data and failed-write protection after the outage (volume intact, the refused write left no trace)",
                  h.get(f"/api/v1/sensors/{NEW}").json()["version"] == 3 and h.get(f"/api/v1/sensors/{NEW}").json()["name"] == "Verification sensor (replaced)"
                  and h.get("/api/v1/settings/alert_threshold").json()["value"] == 0.6)

        # ------------------------------------------------------------------------------------------- 6. loader re-run
        print("6. re-run the data loader")
        stats_before = db.sensor_metadata.find_one({"_id": EDIT})["stats_synced_at"]
        run = subprocess.run([sys.executable, "-m", "src.mongo.load_outputs", "--only", "sensors", "hotspots", "pipeline"], cwd=PROJECT_ROOT, capture_output=True, text=True)
        h = Http(base, "6 loader")
        e2 = h.get(f"/api/v1/sensors/{EDIT}").json()
        check("loader re-run keeps analyst edits (name, tags, status, version) and refreshes dataset stats", run.returncode == 0 and e2["name"] == "I-880 N (analyst edit)"
              and e2["tags"] == ["verify", "watch"] and e2["status"] == "maintenance" and e2["version"] == expected_sensors[EDIT]["version"]
              and db.sensor_metadata.find_one({"_id": EDIT})["stats_synced_at"] > stats_before and h.get(f"/api/v1/sensors/{NEW}").status_code == 200,
              "stats_synced_at advanced; API-created sensor untouched")

        # ------------------------------------------------------------------------------------------- 7. inference
        if not a.skip_inference:
            print("7. score stored streaming predictions through POST /models/predict")
            h = Http(base, "7 inference")
            items = h.get("/api/v1/predictions/latest", params={"limit": 5}).json()["items"]
            rows = []
            for it in items:
                ex = h.get("/api/v1/models/example", params={"sensor_id": it["sensor_id"], "ts_utc": it["event_ts"]})
                if ex.status_code != 200:
                    continue
                r = h.post("/api/v1/models/predict", json={"instances": [ex.json()["instance"]]})
                if r.status_code == 200:
                    rows.append((it["sensor_id"], it["probability"], r.json()["predictions"][0]["probability"], r.json()["inference_ms"], r.json()["cold_start"]))
            worst = max((abs(x - y) for _, x, y, _, _ in rows), default=None)
            warm = [ms for *_, ms, cold in rows if not cold]
            info = h.get("/api/v1/models/info").json()
            check("API inference equals the probabilities the streaming job stored (same model, same features)", len(rows) >= 3 and worst is not None and worst < 1e-5,
                  (f"{len(rows)} sensors, max |dp| = {worst:.1e}; cold start (Spark + model load) {info['load_seconds']:.0f}s on the first call; "
                   f"warm latency {min(warm):.0f}-{max(warm):.0f} ms") if rows and warm else "no rows")

        # ------------------------------------------------------------------------------------------- 8. cleanup + report
        print("8. clean up and write the report")
        h = Http(base, "8 cleanup")
        openapi = h.get("/openapi.json").json()
        ops = sum(1 for p in openapi["paths"].values() for m in p if m in ("get", "post", "put", "patch", "delete"))
        h.delete(f"/api/v1/sensors/{NEW}")
        h.put(f"/api/v1/sensors/{EDIT}", json={"latitude": original["latitude"], "longitude": original["longitude"], "name": original["name"], "description": original["description"],
                                                "direction": original["direction"], "lanes": original["lanes"], "status": original["status"], "tags": original["tags"]})
        for k in ("alert_threshold", "watchlist_size", "custom.verify"):
            h.delete(f"/api/v1/settings/{k}")
        leftover = {s["key"] for s in h.get("/api/v1/settings", params={"overridden_only": True}).json()} - pre_settings
        back = h.get(f"/api/v1/sensors/{EDIT}").json()
        check("cleanup: created sensors/settings removed, edited sensor restored", h.get(f"/api/v1/sensors/{NEW}").status_code == 404 and not leftover
              and all(back[k] == original[k] for k in ("name", "description", "direction", "lanes", "status", "tags", "latitude", "longitude", "stats")))
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / "openapi.json").write_text(json.dumps(openapi, indent=2) + "\n")
        write_report(a, openapi, ops, time.time() - t_start)
    finally:
        for p in procs:
            if p.poll() is None:
                os.killpg(p.pid, signal.SIGTERM)
                try:
                    p.wait(10)
                except subprocess.TimeoutExpired:
                    os.killpg(p.pid, signal.SIGKILL)
    ok = all(c["ok"] for c in checks)
    print(f"\nAPI VERIFICATION {'PASSED' if ok else 'FAILED'}: {sum(c['ok'] for c in checks)}/{len(checks)} checks in {time.time() - t_start:.0f}s -> {OUT / 'verification.md'}")
    return 0 if ok else 1


def write_report(a, openapi: dict, ops: int, seconds: float) -> None:
    import pandas as pd
    tags = sorted({t for p in openapi["paths"].values() for m in p.values() if isinstance(m, dict) for t in m.get("tags", [])})
    ok = all(c["ok"] for c in checks)
    md = [f"# API verification - {'PASSED' if ok else 'FAILED'} ({sum(c['ok'] for c in checks)}/{len(checks)} checks)", "",
          f"Real server processes (`uvicorn src.api.main:app`) against the real MongoDB database `{a.db}`; {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC, {seconds:.0f} s. "
          f"OpenAPI: {len(openapi['paths'])} paths / {ops} operations in {len(tags)} groups ({', '.join(tags)}) - `reports/api/openapi.json`.", "",
          "## Checks", "", df_to_md(pd.DataFrame([{"check": c["check"], "result": "PASS" if c["ok"] else "FAIL", "evidence": c["detail"]} for c in checks]), max_rows=100), "",
          "## HTTP transcript (every call the script made)", "", df_to_md(pd.DataFrame(transcript), max_rows=200), "",
          "## How to reproduce", "", "```bash", "docker compose up -d --wait mongo", "python -m src.mongo.load_outputs          # once", "python -m src.api.verify_api", "```", ""]
    (OUT / "verification.md").write_text("\n".join(md), encoding="utf-8")
    (OUT / "verification.json").write_text(json.dumps({"passed": ok, "database": a.db, "seconds": round(seconds, 1), "checks": checks, "transcript": transcript,
                                                      "openapi": {"paths": len(openapi["paths"]), "operations": ops, "groups": tags}}, indent=2) + "\n")


if __name__ == "__main__":
    sys.exit(main())
