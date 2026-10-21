# TrafficSense — Requirements Traceability Matrix

Each requirement comes from the task text of one project phase. **Implementation** lists where it lives, **Evidence** where it is
demonstrated or tested (paths relative to the project root), **Status** whether it was verified (✔) or has a stated limitation (◐).
Frontend page paths refer to `frontend/src/app/<page>/page.tsx`.

## Phase 2–3 — Data acquisition, preparation, target, splits

| ID | Requirement | Implementation | Evidence | Status |
|---|---|---|---|---|
| R-01 | Acquire PEMS-BAY with provenance, keep raw data immutable | `src/preprocessing/download_dataset.py`, `sources.py` | `data/raw/pems_bay/MANIFEST.json` (sha256), `tests/test_preprocessing.py` | ✔ |
| R-02 | Validate the dataset (shape, ranges, duplicates, gaps, coordinates) | `src/preprocessing/validate_dataset.py` | `reports/validation/pems_bay_validation_report.md` | ✔ |
| R-03 | Clean data onto a complete UTC grid; flag invalid values | `src/preprocessing/clean_traffic.py` | `reports/preprocessing/pems_bay_cleaning_report.md`, `tests/test_features.py` | ✔ |
| R-04 | Leakage-safe features and a 5-minute-ahead congestion target | `src/preprocessing/build_features.py`, `congestion.py` | `reports/preprocessing/pems_bay_feature_report.md`, `src/preprocessing/verify_features.py` | ✔ |
| R-05 | Chronological train/val/test split with a purge gap | `src/preprocessing/splits.py` | `data/processed/pems_bay/features_meta.json`, `tests/test_features.py` | ✔ |

## Phase 4–5 — Spark core, Scala RDD, analytical questions Q1–Q9

| ID | Requirement | Implementation | Evidence | Status |
|---|---|---|---|---|
| R-06 | DataFrame API, Spark SQL, joins, windows, plan equivalence | `src/spark_features/dataframe_sql.py` | `reports/spark_features/dataframe_sql.md` | ✔ |
| R-07 | Storage benchmark CSV vs Parquet | `src/spark_features/storage_benchmarks.py` | `reports/spark_features/storage_benchmarks.md`; dashboard `/performance` | ✔ |
| R-08 | Partitioning, shuffle and small-file benchmarks | `src/spark_features/partition_benchmarks.py` | `reports/spark_features/partition_benchmarks.md`; `/performance` | ✔ |
| R-09 | Caching/persistence and join-strategy analysis | `src/spark_features/optimization.py` | `reports/spark_features/optimization.md`; `/performance` | ✔ |
| R-10 | Scala RDD module | `scala-rdd/` | `reports/scala_rdd/scala_rdd_report.md` | ✔ |
| R-11 | Q1–Q9: patterns, consistency, episodes/recovery, volatility, hotspots, road characteristics, propagation, peer performance, severity | `src/analytics/q1_…` – `q9_…` | `reports/analytics/q1..q9.md`, `tests/test_analytics.py` | ✔ |

## Phase 6 — Analytical questions Q10–Q18

| ID | Requirement | Implementation | Evidence | Status |
|---|---|---|---|---|
| R-12 | Q10 onset and state transitions | `src/analytics/q10_onset_transitions.py` | `reports/analytics/q10.md` | ✔ |
| R-13 | Q11 recovery performance across sensors | `q11_recovery_performance.py` | `reports/analytics/q11.md` | ✔ |
| R-14 | Q12 Pareto concentration of congestion burden | `q12_burden_concentration.py` | `reports/analytics/q12.md` | ✔ |
| R-15 | Q13 missing data and coverage bias | `q13_missing_coverage.py` | `reports/analytics/q13.md` | ✔ |
| R-16 | Q14 traffic vs sensor anomalies | `q14_anomalies.py` | `reports/analytics/q14.md` | ✔ |
| R-17 | Q15 simulated-stream throughput/latency benchmarks | `q15_stream_benchmarks.py` | `reports/analytics/q15.md` | ✔ |
| R-18 | Q16 five-minute-ahead prediction comparison | `q16_prediction_metrics.py` | `reports/analytics/q16.md` | ✔ |
| R-19 | Q17 feature-group contribution and ablation | `q17_feature_ablation.py` | `reports/analytics/q17.md` | ✔ |
| R-20 | Q18 generalisation across time and sensors | `q18_generalization.py` | `reports/analytics/q18.md` | ✔ |
| R-21 | Every question saves tables + figures, no duplicated logic, evidence-based insights and recommendations | `src/analytics/common.py`, `run_all.py` | `reports/analytics/*.json`, `reports/figures/analytics/` | ✔ |

## Phase 7 — MLlib pipeline

| ID | Requirement | Implementation | Evidence | Status |
|---|---|---|---|---|
| R-22 | 7-stage pipeline: imputer, StringIndexer, OneHotEncoder, scaler, VectorAssembler, estimator | `src/ml/pipeline.py` | `reports/ml/training.md` (stages), `tests/test_ml.py` | ✔ |
| R-23 | Logistic regression, decision tree, random forest, tuned chronologically | `src/ml/train_model.py` | `reports/ml/training.md` (11 candidates) | ✔ |
| R-24 | Precision, recall, F1, ROC-AUC, PR-AUC, confusion matrices | `src/ml/evaluate_model.py`, `src/modeling/evaluation.py` | `reports/ml/evaluation.md`; dashboard `/models` | ✔ |
| R-25 | Cost of missed congestion vs false alarms | `src/ml/costs.py` | `reports/ml/evaluation.md` (cost model, 59 % saving vs persistence); `/models` | ✔ |
| R-26 | Serialise the selected model and reload it for inference | `src/ml/model_io.py` | `reports/ml/training.md` (reload check); API `POST /api/v1/models/predict`; `tests/test_api.py` (inference parity) | ✔ |

## Phase 8 — Streaming

| ID | Requirement | Implementation | Evidence | Status |
|---|---|---|---|---|
| R-27 | Replay producer preserving event timestamps into Kafka at a configurable rate | `src/streaming/replay_events.py` | `reports/streaming/replay_*.json`, `tests/test_streaming.py` | ✔ |
| R-28 | Structured Streaming: explicit schemas, watermark, windows, live 5-min-ahead predictions, MongoDB sink | `src/streaming/traffic_stream.py`, `live_features.py`, `mongo_store.py` | `reports/streaming/parity.json` (identical to batch model), `accounting.json` | ✔ |
| R-29 | Checkpointing, query-progress monitoring, restart/recovery test | `src/streaming/streaming_metrics.py`, `recovery_test.py` | `reports/streaming/recovery_test.md` (kill mid-stream, 23,004 = 23,004 predictions, 0 lost, 0 duplicates); `/live`, `/performance` | ✔ |

## Phase 9 — MongoDB and FastAPI

| ID | Requirement | Implementation | Evidence | Status |
|---|---|---|---|---|
| R-30 | 8 collections with validators and indexes | `src/mongo/schema.py` | dashboard `/status` (collections, indexes, validators), `tests/test_mongo.py` | ✔ |
| R-31 | `mongo_writer.py` / `mongo_reader.py`, loader from Spark outputs | `src/mongo/` | `tests/test_mongo.py` | ✔ |
| R-32 | Modular FastAPI with schemas and routers, env-based config | `src/api/main.py`, `schemas.py`, `routers/`, `config.py` | `reports/api/openapi.json`, `tests/test_api.py` | ✔ |
| R-33 | Real CRUD for sensor metadata and analyst settings with validation and error handling | `routers/sensors.py`, `routers/settings.py` | `tests/test_api.py`, `reports/api/verification.md`; dashboard `/data` | ✔ |
| R-34 | Changes persist in MongoDB and survive restarts | `src/api/verify_api.py` | `reports/api/verification.md` (kill -9, Mongo outage); `tests/test_integration_e2e.py::test_settings_persist_across_an_api_crash_and_restart` | ✔ |
| R-35 | `.env.example` | — | Not created: the workspace's content policy blocks that file. All variables are documented in the README tables instead | ◐ |

## Phase 10 — Dashboard, integration, performance, review

| ID | Requirement | Implementation | Evidence | Status |
|---|---|---|---|---|
| R-36 | Professional responsive multi-page React/Next.js dashboard, lavender/white theme, persistent top nav, scope strip, hero + summary cards, pipeline workflow | `frontend/src/app/layout.tsx`, `components/Shell.tsx`, `app/page.tsx`, `globals.css` | `tests/test_integration_e2e.py::test_every_page_renders_with_the_persistent_navigation`; browser check, `reports/final/verification_report.md` | ✔ |
| R-37 | Dashboard landing page | `app/page.tsx` | browser check; e2e page test | ✔ |
| R-38 | Traffic Analytics: filters for all 18 questions, interactive Plotly charts | `app/analytics/page.tsx`, `lib/charts.ts`, `components/QuestionChart.tsx` | `frontend/tests/charts.test.ts` (18 questions × every chart spec vs real data); browser sweep Q1–Q18 | ✔ |
| R-39 | Congestion Hotspots page | `app/hotspots/page.tsx` | browser check; `tests/test_integration_e2e.py` (hotspot endpoint parity) | ✔ |
| R-40 | Simulated Live Traffic page | `app/live/page.tsx` | `test_live_traffic_endpoints_return_scored_predictions` | ✔ |
| R-41 | Model Evaluation (metrics, confusion matrices) | `app/models/page.tsx` | browser check (9 charts, 5 model rows) | ✔ |
| R-42 | Spark Performance page | `app/performance/page.tsx` | browser check (10 charts from live data) | ✔ |
| R-43 | Data Management with interactive CRUD tables and instant MongoDB persistence | `app/data/page.tsx`, `components/SensorsTab.tsx`, `SettingsTab.tsx` | `frontend/tests/data-status.test.tsx` (create → edit → status → delete verified through the API), `test_crud_through_the_proxy_is_persisted_in_mongodb`, browser CRUD | ✔ |
| R-44 | Analysis History & System Status | `app/status/page.tsx` | `frontend/tests/data-status.test.tsx` | ✔ |
| R-45 | All frontend data from FastAPI/MongoDB, no hard-coded primary data | `lib/api.ts`, `lib/hooks.ts`, `app/backend/[...path]/route.ts` | `tests/test_frontend_contract.py` (every endpoint literal exists in OpenAPI), `test_proxy_serves_the_same_live_data_as_the_api` | ✔ |
| R-46 | Requirements traceability matrix | this file | — | ✔ |
| R-47 | Automated integration test suite in `tests/` | `tests/test_frontend_contract.py`, `tests/test_integration_e2e.py`, `frontend/tests/*` | `scripts/run_phase10.sh`, `reports/final/logs/` | ✔ |
| R-48 | Performance benchmark summary | `scripts/benchmark_dashboard.py` | `reports/final/performance_summary.md`, `dashboard_benchmark.json` | ✔ |
| R-49 | Final review presentation outline | — | `reports/final/presentation_outline.md` | ✔ |
| R-50 | Final verification report | — | `reports/final/verification_report.md` | ✔ |

## Known limitations (not hidden requirements failures)

- No real-browser automation framework (Playwright/Chromium) could be installed in the WSL VM (missing system libraries, no sudo). Browser-level checks were
  run with the editor's integrated browser; automated UI tests run in jsdom (Plotly stubbed) against the live API.
- Maps are Plotly scatter plots of longitude/latitude, not tile maps (the lightweight cartesian Plotly bundle is used to keep the JS payload small).
- MongoDB runs unauthenticated on `127.0.0.1` (development setup). Write protection exists (`TRAFFICSENSE_API_KEY`) but is off by default.
- Analysis tables above 500 rows (e.g. Q7 edges, 2,369 rows) are shown as the first 500 rows, by the API's maximum page size.
