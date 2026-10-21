# TrafficSense — Final Review Presentation Outline

Target: 20 minutes + 5 minutes of questions, 14 slides. Every number below is taken from a report in `reports/` or from the live dashboard,
so it can be shown on the spot. "Live" marks a moment to switch from the slides to the running dashboard.

| # | Slide | Message and evidence | Time |
|---|---|---|---|
| 1 | **Problem and goal** | Predict congestion 5 minutes ahead for 325 Bay Area sensors; which roads and hours matter; can it run as a stream? | 1 min |
| 2 | **Data** | PEMS-BAY, 16,937,700 readings, 5-min speeds, Jan–Jun 2017. Raw data immutable (sha256 manifest); validation report. | 1 min |
| 3 | **Architecture** | One diagram: Spark (batch + MLlib) → Kafka → Structured Streaming → MongoDB (8 collections) → FastAPI → Next.js. The dashboard's 8-stage workflow card on the landing page *is* this diagram. | 2 min |
| 4 | **Preparation and leakage control** | Complete UTC grid, 41 leakage-safe features, 70/10/20 chronological split with 60-min purge; independent verifier. | 1.5 min |
| 5 | **Spark core and performance** | Parquet 6.8 % of CSV size and 20.9× faster full scan; caching 19× on reuse; coalesce 0.63 s vs repartition 3.91 s; broadcast join 0.90 s vs sort-merge 3.25 s. (`/performance`) | 2 min |
| 6 | **Scala RDD** | The RDD module and why DataFrames win for aggregation (reduceByKey 4.6 KB vs groupByKey 8.2 MB shuffled). | 1 min |
| 7 | **Analytics Q1–Q18** | Three headline findings: PM peak 1.4× AM peak; 148 persistent hotspots (45.5 % of sensors); states are sticky (congested dwell ≈ 64 min). **Live:** `/analytics`, change topic filter, highlight a sensor. | 3 min |
| 8 | **MLlib pipeline** | 7 stages; 11 chronologically-tuned candidates; best = logistic regression; pipeline reload verified. | 1.5 min |
| 9 | **Model results and cost** | Test F1 0.930 vs persistence 0.922, PR-AUC 0.975 vs 0.854; deployed cost-optimal threshold 0.686: recall 98.7 %, onset recall 83.6 % (persistence 0 %); 59 % cost saving (CI excludes 0). **Live:** `/models`, confusion matrices, "Try the saved model". | 2.5 min |
| 10 | **Streaming and fault tolerance** | Replay with duplicates, late and malformed events; p50 latency ≈ 9 s vs 300 s horizon; kill mid-stream → restart → 23,004 = 23,004 predictions, 0 lost, 0 duplicates. **Live:** `/live` playback and threshold slider. | 2 min |
| 11 | **MongoDB and API** | Schema validators + indexes; REST with If-Match versioning (412 on stale write), error envelope, optional API key. **Live:** `/status` collections table. | 1.5 min |
| 12 | **CRUD demo (live)** | `/data`: add sensor → edit → change status → refresh the page (persists) → delete; show the write log and the document in MongoDB; stop the API and restart it. | 2 min |
| 13 | **Verification** | 4 layers: pytest (unit, API, contract, end-to-end with crash/restart), vitest against the live API, browser sweeps of 8 pages and Q1–Q18, benchmark. Totals in `verification_report.md`. | 1.5 min |
| 14 | **Limitations and future work** | Speeds only (no flow/incidents), six months, no summer/holiday peaks; scatter maps; dev-mode Mongo without auth; next: weather/incident features, multi-step horizons, exactly-once sinks, tile maps, Playwright CI. | 1.5 min |

## Demo script (backup if the network or a service is down)

1. `docker compose up -d --wait mongo` → `uvicorn src.api.main:app --port 8000` → `cd frontend && npm run start`.
2. Open `/` (landing), `/analytics?q=q5`, `/hotspots`, `/live`, `/models`, `/performance`, `/data`, `/status`.
3. Fallback if a service is down: the dashboard shows an error panel with a Retry button instead of crashing — demonstrate by stopping the API (`/status` shows it offline), then restarting it.
4. Static evidence for every claim is in `reports/` (markdown + JSON + PNG).

## Likely questions and where the answer is

| Question | Answer / evidence |
|---|---|
| Why is persistence so strong (F1 0.922)? | Congestion is highly persistent (stay probability 0.92); the model's value is in onsets: 83.6 % onset recall vs 0 % (`reports/ml/evaluation.md`). |
| How is leakage prevented? | Train-only statistics, causal gap filling, chronological split + 60-minute purge, day-block bootstrap (`reports/preprocessing/`). |
| Why 0.686 as the alert threshold? | Chosen on validation by minimum expected cost at 10:1 miss:false-alarm; editable in Data Management → Analyst settings; the live page applies it immediately. |
| Is the stream really fault tolerant? | `reports/streaming/recovery_test.md`: killed mid-stream, restarted, identical output to an uninterrupted run. |
| Are the charts real data? | Every chart is built from Mongo documents through the API; `frontend/tests/charts.test.ts` fails if a chart spec no longer matches the stored tables. |
