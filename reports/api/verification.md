# API verification - PASSED (34/34 checks)

Real server processes (`uvicorn src.api.main:app`) against the real MongoDB database `trafficsense`; 2026-10-21 04:34 UTC, 60 s. OpenAPI: 34 paths / 42 operations in 9 groups (analytics, health, hotspots, models, pipeline, predictions, sensors, settings, streaming) - `reports/api/openapi.json`.

## Checks

| check | result | evidence |
|---|---|---|
| server process starts and becomes ready | PASS | pid 2796, ready after 3.4s |
| MongoDB holds the loaded collections | PASS | {"sensor_metadata": 325, "settings": 0, "congestion_hotspots": 325, "analysis_results": 95, "model_evaluations": 5, "traffic_predictions": 23004, "stream_metrics": 521, "pipeline_runs": 30} |
| CREATE sensor -> 201 + Location + ETag | PASS | version 1 |
| UPDATE (PATCH) changes only the given fields | PASS | version 2 |
| UPDATE (PUT + If-Match) replaces the editable fields | PASS | version 3, description reset |
| stale If-Match is refused (412) | PASS |  |
| read back after the writes (GET) | PASS |  |
| DELETE sensor -> 204, then GET and DELETE -> 404 | PASS |  |
| validation errors are 422 with field details | PASS |  |
| duplicate create is 409 | PASS |  |
| a real dataset sensor can be edited | PASS | sensor 400001 |
| CREATE setting (PUT) -> 201 | PASS |  |
| UPDATE setting (PUT + If-Match) -> 200 | PASS |  |
| setting validation: out of range -> 422 | PASS |  |
| CREATE custom setting (POST) -> 201 | PASS |  |
| PATCH setting | PASS |  |
| DELETE setting -> 204, then 404 | PASS |  |
| settings change API behaviour | PASS | watch-list size 5; 2 sensors at/above 0.6 |
| sensor document exists in MongoDB with the API's final values | PASS | version 3, GeoJSON location updated |
| edited dataset sensor stored; read-only stats untouched | PASS |  |
| setting document exists in MongoDB | PASS |  |
| deleted sensor is gone from MongoDB | PASS |  |
| server process killed (SIGKILL, no graceful shutdown) | PASS | exit code -9 |
| a NEW server process starts | PASS | pid 2872, ready after 3.2s |
| sensor changes survived the restart (every field, version and timestamps identical) | PASS | sensors 9000001 and 400001 |
| setting changes survived the restart (value, version, updated_by, timestamps identical) | PASS | alert_threshold, watchlist_size, custom.verify |
| deletions survived the restart | PASS |  |
| persisted settings still drive behaviour after the restart | PASS | watch-list 5, alerts 2 |
| during the outage reads and writes answer 503 database_unavailable (+ Retry-After), the process stays up | PASS | 503 after 3.2s (server selection timeout), /health 200, /ready 503 |
| API recovers by itself when MongoDB is back (no API restart) | PASS | first 200 1.2s after `docker start` |
| data and failed-write protection after the outage (volume intact, the refused write left no trace) | PASS |  |
| loader re-run keeps analyst edits (name, tags, status, version) and refreshes dataset stats | PASS | stats_synced_at advanced; API-created sensor untouched |
| API inference equals the probabilities the streaming job stored (same model, same features) | PASS | 5 sensors, max |dp| = 2.3e-07; cold start (Spark + model load) 25s on the first call; warm latency 954-1236 ms |
| cleanup: created sensors/settings removed, edited sensor restored | PASS |  |

## HTTP transcript (every call the script made)

| step | request | status | ms | note |
|---|---|---|---|---|
| 1 baseline | GET /api/v1/status | 200 | 51.000 |  |
| 1 baseline | GET /api/v1/sensors/400001 | 200 | 48.200 |  |
| 1 baseline | GET /api/v1/settings | 200 | 47.000 |  |
| 2 sensors | POST /api/v1/sensors | 201 | 33.600 |  |
| 2 sensors | PATCH /api/v1/sensors/9000001 | 200 | 22.000 |  |
| 2 sensors | PUT /api/v1/sensors/9000001 | 200 | 23.500 |  |
| 2 sensors | PATCH /api/v1/sensors/9000001 | 412 | 6.700 | precondition_failed |
| 2 sensors | GET /api/v1/sensors/9000001 | 200 | 4.800 |  |
| 2 sensors | POST /api/v1/sensors | 201 | 10.700 |  |
| 2 sensors | DELETE /api/v1/sensors/9000002 | 204 | 19.500 |  |
| 2 sensors | GET /api/v1/sensors/9000002 | 404 | 2.900 | not_found |
| 2 sensors | DELETE /api/v1/sensors/9000002 | 404 | 6.900 | not_found |
| 2 sensors | POST /api/v1/sensors | 422 | 5.600 | validation_error |
| 2 sensors | POST /api/v1/sensors | 409 | 8.700 | conflict |
| 2 sensors | PATCH /api/v1/sensors/400001 | 200 | 7.900 |  |
| 2 settings | PUT /api/v1/settings/alert_threshold | 201 | 28.000 |  |
| 2 settings | PUT /api/v1/settings/alert_threshold | 200 | 5.700 |  |
| 2 settings | PUT /api/v1/settings/alert_threshold | 422 | 7.400 | validation_error |
| 2 settings | PUT /api/v1/settings/watchlist_size | 201 | 6.200 |  |
| 2 settings | POST /api/v1/settings | 201 | 19.400 |  |
| 2 settings | PATCH /api/v1/settings/custom.verify | 200 | 29.000 |  |
| 2 settings | POST /api/v1/settings | 201 | 5.300 |  |
| 2 settings | DELETE /api/v1/settings/custom.verify_tmp | 204 | 25.700 |  |
| 2 settings | GET /api/v1/settings/custom.verify_tmp | 404 | 29.300 | not_found |
| 2 behaviour | GET /api/v1/hotspots/watchlist | 200 | 47.000 |  |
| 2 behaviour | GET /api/v1/predictions/latest | 200 | 42.600 |  |
| 2 behaviour | GET /api/v1/sensors/9000001 | 200 | 7.200 |  |
| 2 behaviour | GET /api/v1/sensors/400001 | 200 | 4.000 |  |
| 2 behaviour | GET /api/v1/settings/alert_threshold | 200 | 2.700 |  |
| 2 behaviour | GET /api/v1/settings/watchlist_size | 200 | 4.700 |  |
| 2 behaviour | GET /api/v1/settings/custom.verify | 200 | 4.600 |  |
| 4 after restart | GET /api/v1/sensors/9000001 | 200 | 44.900 |  |
| 4 after restart | GET /api/v1/sensors/400001 | 200 | 7.100 |  |
| 4 after restart | GET /api/v1/settings/alert_threshold | 200 | 59.100 |  |
| 4 after restart | GET /api/v1/settings/watchlist_size | 200 | 13.100 |  |
| 4 after restart | GET /api/v1/settings/custom.verify | 200 | 11.300 |  |
| 4 after restart | GET /api/v1/sensors/9000002 | 404 | 7.500 | not_found |
| 4 after restart | GET /api/v1/settings/custom.verify_tmp | 404 | 4.200 | not_found |
| 4 after restart | GET /api/v1/hotspots/watchlist | 200 | 32.900 |  |
| 4 after restart | GET /api/v1/predictions/latest | 200 | 34.000 |  |
| 5 after outage | GET /api/v1/sensors/9000001 | 200 | 7.000 |  |
| 5 after outage | GET /api/v1/sensors/9000001 | 200 | 7.300 |  |
| 5 after outage | GET /api/v1/settings/alert_threshold | 200 | 3.900 |  |
| 6 loader | GET /api/v1/sensors/400001 | 200 | 5.700 |  |
| 6 loader | GET /api/v1/sensors/9000001 | 200 | 3.700 |  |
| 7 inference | GET /api/v1/predictions/latest | 200 | 11.000 |  |
| 7 inference | GET /api/v1/models/example | 200 | 885.400 |  |
| 7 inference | POST /api/v1/models/predict | 200 | 28,776.100 |  |
| 7 inference | GET /api/v1/models/example | 200 | 1,118.200 |  |
| 7 inference | POST /api/v1/models/predict | 200 | 1,246.300 |  |
| 7 inference | GET /api/v1/models/example | 200 | 951.300 |  |
| 7 inference | POST /api/v1/models/predict | 200 | 1,140.300 |  |
| 7 inference | GET /api/v1/models/example | 200 | 1,003.200 |  |
| 7 inference | POST /api/v1/models/predict | 200 | 1,016.700 |  |
| 7 inference | GET /api/v1/models/example | 200 | 1,003.600 |  |
| 7 inference | POST /api/v1/models/predict | 200 | 962.600 |  |
| 7 inference | GET /api/v1/models/info | 200 | 28.100 |  |
| 8 cleanup | GET /openapi.json | 200 | 104.800 |  |
| 8 cleanup | DELETE /api/v1/sensors/9000001 | 204 | 23.600 |  |
| 8 cleanup | PUT /api/v1/sensors/400001 | 200 | 22.100 |  |
| 8 cleanup | DELETE /api/v1/settings/alert_threshold | 204 | 23.100 |  |
| 8 cleanup | DELETE /api/v1/settings/watchlist_size | 204 | 3.400 |  |
| 8 cleanup | DELETE /api/v1/settings/custom.verify | 204 | 3.000 |  |
| 8 cleanup | GET /api/v1/settings | 200 | 15.900 |  |
| 8 cleanup | GET /api/v1/sensors/400001 | 200 | 6.900 |  |
| 8 cleanup | GET /api/v1/sensors/9000001 | 404 | 3.500 | not_found |

## How to reproduce

```bash
docker compose up -d --wait mongo
python -m src.mongo.load_outputs          # once
python -m src.api.verify_api
```
