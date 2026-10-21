# TrafficSense — Performance Benchmark Summary

All figures are measured on one development machine (Windows 11 host, WSL2 Ubuntu VM, Spark 3.5.9 in local mode, MongoDB 7 and Kafka in Docker),
so they show **relative** behaviour and order of magnitude, not production capacity. Sources: `reports/spark_features/`, `reports/ml/`,
`reports/streaming/`, `reports/final/dashboard_benchmark.json` (produced by `scripts/benchmark_dashboard.py`).

## 1. Batch processing (Spark, 16,937,700 readings)

| Topic | Result | Source |
|---|---|---|
| Storage size | Parquet+snappy **6.8 %** of CSV (96 MB vs 1.4 GB); zstd 5.1 % | `storage_benchmarks.md` |
| Full scan | Parquet 1.10 s vs CSV 23.0 s (**20.9×**); CSV with schema inference 70.3 s | same |
| Column pruning | 2-column aggregate: 21 MB read vs 1.4 GB (**15.1×** faster) | same |
| Partition pruning (1 month) | 0.49 s / 6 tasks vs 0.72 s / 13 tasks flat | same |
| `coalesce` vs `repartition` (200→12) | 0.63 s, 0 B shuffled vs 3.91 s, 318.8 MB shuffled | `partition_benchmarks.md` |
| Shuffle partitions (AQE off) | best 48 → 0.84 s; worst 1,000 → 2.58 s; AQE on collapses 1,000 → 13 tasks (0.61 s) | same |
| Small-file problem | `partitionBy(sensor_id)`: 1,950 files, scan 2.95 s vs 6 files, 0.98 s | same |
| Caching | reuse 0.57 s cached vs 11.10 s uncached (**19×**); cache costs 464 MB memory | `optimization.md` |
| Joins | broadcast hash 0.90 s / 0 B shuffle; sort-merge 3.25 s / 167 MB | same |
| Aggregation | `reduceByKey` shuffled 4.6 KB vs `groupByKey` 8.2 MB (same result) | same |

**Recommendation**: keep all intermediate data as partitioned Parquet (by month), never by sensor; broadcast the 325-row sensor table; persist reused windowed frames;
leave AQE on and `coalesce` rather than `repartition` when only reducing partitions.

## 2. Analytics jobs (Q1–Q18) and model training

| Item | Result | Source |
|---|---|---|
| Analytics jobs | 18 jobs, all `succeeded`; durations on dashboard `/status` ("Spark job duration per question") and `/performance` | MongoDB `pipeline_runs` |
| MLlib tuning | 11 candidates, total **17.3 min**: logistic regression 36–47 s each, decision tree 38–40 s, random forest 52–97 s (60 trees × depth 12 = 96.8 s) | `reports/ml/training.md` |
| Model quality / cost | logistic regression: test F1 0.930, PR-AUC 0.975; deployed threshold recall 98.7 %; **59 %** lower cost than persistence | `reports/ml/evaluation.md` |
| Cheapest model to serve | logistic regression is among the cheapest to train (36–47 s vs 38–40 s decision tree) and a single linear layer to score; random forest costs 1.1–2.7× the training time for a lower test F1 (0.926 vs 0.930) | `training.md`, `evaluation.md` |

## 3. Streaming (Kafka → Structured Streaming → MongoDB)

| Metric | Result | Source |
|---|---|---|
| Latency, producer → stored prediction (60× replay) | p50 **9.9 s**, p95 13.0 s, p99 13.0 s; 100 % within the 300 s prediction horizon | `reports/streaming/stream_metrics.md` |
| Micro-batch time (p50 / p95) | predictions 3.9 s / 6.1 s; windows 2.5 s / 3.8 s; dead letters 1.2 s / 1.8 s | same |
| Sustained throughput | ≈ 67–70 rows/s per query on the single-node setup (3,900 rows in 12 batches) | same |
| Back-pressure and state | max backlog 0 events; max state 981–2,925 rows; 0 rows dropped by the watermark in the live run | same |
| Fault tolerance | killed mid-stream (7,475 of 23,004 predictions written) → restart recovered state from MongoDB in 3.2 s → 23,004 = 23,004 predictions, 0 lost, 0 duplicates | `recovery_test.md` |

Bottleneck: the prediction query has the longest micro-batches (p50 3.9 s vs 2.5 s for the window queries), because it also builds features and scores the model. Scaling path: more Kafka
partitions and executors, larger micro-batches, or a lighter model export for scoring.

## 4. API and dashboard (measured by `scripts/benchmark_dashboard.py`, 30 samples per endpoint, warm)

| Endpoint | Direct p50 | Through dashboard proxy p50 | Payload |
|---|---|---|---|
| `/ready` | 4.4 ms | 16.4 ms | – |
| `/api/v1/status` | 22.7 ms | 28.3 ms | 1.5 KB |
| `/api/v1/sensors?limit=50` | 14.6 ms | 21.6 ms | 24.2 KB |
| `/api/v1/hotspots?limit=50` | 13.2 ms | 21.3 ms | 15.8 KB |
| `/api/v1/predictions/latest?limit=50` | 16.1 ms | 24.8 ms | 12.4 KB |
| `/api/v1/analytics/questions/q5` | 8.7 ms | 13.4 ms | 2.4 KB |
| `/api/v1/analytics/questions/q7/tables/edges?limit=500` | 22.2 ms | 28.1 ms | 84.9 KB |
| `/api/v1/performance/overview` | 26.9 ms | 35.9 ms | 42.1 KB |
| `/api/v1/status/database` | 33.3 ms | 46.9 ms | 1.7 KB |

- **Proxy overhead**: about +6 to +12 ms per call (extra Node hop); it removes CORS handling and keeps the API key on the server.
- **Page shell**: server-rendered HTML in 5–7 ms (11–25 KB); all data arrives client-side from the API, so a slow API never blocks navigation.
- **Concurrency** (8 workers × 25 requests of `/hotspots?limit=50`): direct 109 req/s (p95 100 ms); through the proxy 71 req/s (p95 170 ms). Run-to-run variation on this machine is large
  (earlier quiet and loaded runs gave 189 and 46 req/s direct), so treat this as "tens to a hundred requests per second on one worker", which is ample for an analyst dashboard.
- **JS payload**: 21 files, 2,190 KB raw / **709 KB gzip** in total; one chunk is the Plotly cartesian bundle (1,434 KB raw), loaded only on pages with charts (dynamic import, `ssr: false`).
- **First-call cost**: the `POST /models/predict` endpoint loads Spark + the model on first use (≈ 25 s cold, ≈ 1 s warm); `TRAFFICSENSE_PRELOAD_MODEL=true` moves that to start-up.
- Dashboard reads use paginated API calls (page size capped at 500 rows; analysis tables are fetched per chart, not all at once).

## 5. Findings and recommendations

1. Storage format and partitioning dominate batch performance (20× scan, 19× cache reuse); both are already applied to the pipeline.
2. The deployed model is the cheapest *and* the best on the cost metric, so there is no accuracy/latency trade-off to manage in streaming.
3. Streaming latency (≈ 10 s) is ~30× inside the 5-minute horizon; it includes the deliberate event-time grace wait for late events, which is a design choice rather than compute cost.
4. The dashboard is bound by API round-trips, not rendering. If concurrency matters: run several uvicorn workers, enable `TRAFFICSENSE_PRELOAD_MODEL`, and add HTTP caching for the immutable analysis tables.
5. Limitation: Spark runs in local mode on one machine; no cluster scaling experiments were done, and the benchmark machine is shared with the editor and Docker.
