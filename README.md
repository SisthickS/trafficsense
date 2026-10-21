# TrafficSense — Urban Traffic Intelligence and Congestion Prediction Using Apache Spark

23CSE352 Big Data Analytics project. Stack: Spark 3.5.9 (PySpark + Scala 2.12 RDDs), Spark MLlib,
Kafka (KRaft) + Structured Streaming, MongoDB, FastAPI, Next.js.

Dataset: **PEMS-BAY** — 325 Caltrans loop-detector sensors in the San Francisco Bay Area, average speed (mph)
every 5 minutes, 2017-01-01 → 2017-06-30 (52,116 timestamps × 325 sensors = **16,937,700 readings**).

## Quick start — run the app

The repository contains code, tests and reports only. The dataset, processed Parquet files and trained model (`data/`) are **not** stored in git (about 1 GB); the phase scripts below recreate them.

**Prerequisites:** Windows 11 with WSL2 Ubuntu (or any Linux), Docker with the compose plugin, about 8 GB RAM and 10 GB free disk, internet access for the first run.
The commands run inside WSL / Linux, from the project root.

```bash
git clone https://github.com/sisthicks/trafficsense.git && cd trafficsense

# 1. Toolchain (JDK 17, Spark 3.5, Python 3.11 venv, Node 22, sbt), no sudo
bash scripts/setup_toolchain.sh && source ~/.trafficsense_env.sh

# 2. Data and models: each script downloads / builds what the next one needs
bash scripts/run_phase2.sh     # download + validate PEMS-BAY
bash scripts/run_phase3.sh     # clean, features, chronological splits
bash scripts/run_phase4.sh     # Spark benchmarks
bash scripts/run_phase5.sh     # Scala RDD + analytics Q1-Q9
bash scripts/run_phase6.sh     # analytics Q10-Q18
bash scripts/run_phase7.sh     # MLlib training and evaluation (about 20 min)
bash scripts/run_phase8.sh     # Kafka replay + Structured Streaming predictions (about 25 min)

# 3. Database, API, tests (loads all results into MongoDB)
bash scripts/run_phase9.sh

# 4. Start the platform (three terminals)
docker compose up -d --wait mongo                          # MongoDB on 127.0.0.1:27017
uvicorn src.api.main:app --host 127.0.0.1 --port 8000      # FastAPI, docs at http://127.0.0.1:8000/docs
cd frontend && npm ci && npm run build && npm run start    # dashboard at http://localhost:3000
```

Open <http://localhost:3000>. Pages: Dashboard, Traffic Analytics (Q1-Q18), Congestion Hotspots, Simulated Live Traffic, Model Evaluation, Spark Performance, Data Management (CRUD), History & Status.

Verify everything with `bash scripts/run_phase10.sh` (build, all Python and frontend tests, benchmark). Final review material is in `reports/final/`.
Configuration is by environment variables (tables in the Phase 9 and Phase 10 sections below); the defaults work for local development.
Already-generated analysis reports are in `reports/`, so you can read the results without re-running the pipeline.

## Project layout

```
bdaa/
├── data/
│   ├── raw/pems_bay/          # IMMUTABLE: downloaded files (read-only) + MANIFEST.json (sha256 provenance)
│   └── processed/pems_bay/
│       ├── readings/          # Phase 2: lossless long-format staging (month-partitioned)
│       ├── sensors/           # Phase 2: sensor coordinates + quality stats
│       ├── clean/             # Phase 3: complete UTC grid, invalid values removed, quality flags
│       ├── features/          # Phase 3: model-ready rows, partitioned by split=train|val|test
│       ├── sensor_stats/      # Phase 3: train-only per-sensor stats (free-flow speed, congestion rate)
│       ├── slot_profile/      # Phase 3: train-only sensor × weekday × 5-min profile
│       └── features_meta.json # Feature list, label definition, split boundaries (read by MLlib)
├── notebooks/eda.ipynb        # Exploratory data analysis (Spark aggregates → plots)
├── reports/
│   ├── validation/            # Dataset Validation Report (.md for humans, .json for machines)
│   ├── preprocessing/         # Cleaning report + Feature & Target report
│   ├── spark_features/        # Phase 4: SUMMARY.md, 4 reports (.md/.json), charts, plans/ (explain output)
│   └── figures/eda/           # EDA figures (PNG)
├── scripts/
│   ├── setup_toolchain.sh     # One-time, sudo-free install: JDK 17, Spark, sbt, Python 3.11 venv, Node 22
│   ├── run_phase2.sh          # Phase 2 end-to-end runner + verification
│   ├── run_phase3.sh          # Phase 3 end-to-end runner + verification
│   └── run_phase4.sh          # Phase 4 demonstrations + benchmarks + verification
├── src/
│   ├── common/                # paths.py (all data locations), spark.py (SparkSession factory)
│   ├── preprocessing/
│   │   ├── sources.py         # Pinned source registry (URLs, checksums, expected shape)
│   │   ├── download_dataset.py
│   │   ├── raw_reader.py      # Shared raw CSV / pickle readers (validator + build use the same parsing)
│   │   ├── validate_dataset.py
│   │   ├── build_processed.py
│   │   ├── congestion.py      # Congestion threshold + target definition (single source of truth)
│   │   ├── splits.py          # Chronological split + purge gap
│   │   ├── clean_traffic.py   # Phase 3 cleaning
│   │   ├── build_features.py  # Phase 3 features, target, splits, reports
│   │   └── verify_features.py # Phase 3 independent verification
│   └── spark_features/
│       ├── common.py          # Benchmark harness: timing + Spark REST metrics, plan inspection, reports
│       ├── dataframe_sql.py   # DataFrame API, joins, windows, Spark SQL views, plan equivalence
│       ├── storage_benchmarks.py    # CSV vs Parquet: size, scans, pruning, pushdown, location
│       ├── partition_benchmarks.py  # repartition vs coalesce, shuffle partitions/AQE, pre-partitioning, small files
│       └── optimization.py    # cache/persist/unpersist strategies, join strategies, aggregation shuffles
├── tests/                     # test_preprocessing.py (P2), test_features.py (P3), test_spark_features.py (P4)
├── requirements.txt
└── pytest.ini
```

The project lives on the Windows drive (`C:\Users\ssis\Desktop\bdaa` = `/mnt/c/Users/ssis/Desktop/bdaa` in WSL).
The toolchain, the Python venv (`~/.venvs/trafficsense-py311`) and Spark scratch space
(`~/.cache/trafficsense`) live on the WSL Linux filesystem. WSL's `/tmp` is a 3.8 GB RAM disk and is too small
for Spark spill files.

## Setup (once, inside WSL Ubuntu)

```bash
cd /mnt/c/Users/ssis/Desktop/bdaa
bash scripts/setup_toolchain.sh        # idempotent; no sudo
source ~/.trafficsense_env.sh          # new terminals do this automatically; `ts` jumps to the project
```

## Phase 2 — dataset acquisition & validation

One command:

```bash
bash scripts/run_phase2.sh             # ≈ 4–5 min the first time (download ≈ 1 min)
```

Or step by step:

```bash
python -m src.preprocessing.download_dataset --inspect    # compare pinned checksums with the live source
python -m src.preprocessing.download_dataset              # download + verify → data/raw/pems_bay/
python -m src.preprocessing.validate_dataset              # PySpark checks → reports/validation/
python -m src.preprocessing.build_processed               # lossless Parquet → data/processed/pems_bay/
python -m pytest -q tests/test_preprocessing.py          # 7 Phase 2 tests
jupyter lab notebooks/eda.ipynb                           # or execute headless:
jupyter nbconvert --to notebook --execute --inplace notebooks/eda.ipynb
```

Alternatives: `--source huggingface` (original DCRNN HDF5 mirror, pinned revision) or `--dataset metr-la`.

### Sources

| Source | Use | Pinning |
|---|---|---|
| Zenodo 10.5281/zenodo.5724362 — `PEMS-BAY.csv`, `adj_mx_PEMS-BAY.pkl` | **Primary** | md5 + size from Zenodo |
| DCRNN GitHub `graph_sensor_locations_bay.csv` | Sensor coordinates | commit `602afd9` + sha256 |
| Hugging Face `jimmygao3218/PEMSBAY` (`PEMS-BAY.h5`) | Fallback | revision `c858397` + LFS sha256 |
| Hugging Face `witgaw/PEMS-BAY` | **Not used** — pre-windowed "augmented" samples (each reading duplicated ~24×); we build windows ourselves in Phase 5 | — |

Cite: Li et al., *Diffusion Convolutional Recurrent Neural Network*, ICLR 2018. Original data: Caltrans PeMS.

### Raw vs processed contract

* `data/raw/` is **never modified**: downloads are checksum-verified, set read-only, and recorded in
  `MANIFEST.json`. A raw file that no longer matches its checksum is refused (`--force` required).
* The validator only reads. The build refuses to run unless the validation report exists, is not `FAIL`,
  and was produced from exactly the current raw checksums.
* `data/processed/` holds only derived data: long format `ts, sensor_id, speed_mph, is_missing, date, month`
  (Parquet, snappy, partitioned by `month`). Missing readings (`0.0` in the raw file) become `speed_mph = null`,
  `is_missing = true`; no imputation happens in this phase.

### Validation checks (`reports/validation/pems_bay_validation_report.md`)

| ID | Check | Current result |
|---|---|---|
| C01–C02 | Manifest present; raw sha256 unchanged | PASS |
| C03–C05 | Header/schema, RDD line-structure scan, typed parse (PERMISSIVE) | PASS — 326 columns, 0 malformed |
| C06 | Record counts | PASS — 52,116 × 325 = 16,937,700 |
| C07 | Timestamps: parse, duplicates, order, 5-min grid, gaps | PASS — the only gap is the DST hour on 2017-03-12 |
| C08 | Missing values (null/NaN/zero/negative) | PASS — 521 zero readings (0.003%) |
| C09 | Speed distribution | PASS — 3.0–85.1 mph, median 65.3 |
| C10 | Flat-line runs ≥ 1 h (stuck or imputed sensors) | PASS — 0.70% of readings |
| C11 | Sensor coordinates | PASS — 325/325 in the Bay Area bounding box |
| C12 | Adjacency matrix (restricted unpickler) | PASS — 325×325, same sensor order |

### Verification

`run_phase2.sh` finishes with these checks. Run it again at any time to re-verify:

1. Raw files still match the `MANIFEST.json` sha256 values and are read-only.
2. The validation report is not `FAIL`.
3. Parquet row count equals the validated reading count (16,937,700).
4. Parquet is partitioned by month and nothing derived was written into `data/raw/`.
5. The EDA notebook ran with no error outputs and 10 figures exist.

## Phase 3 — cleaning, features, congestion target, chronological splits

```bash
bash scripts/run_phase3.sh             # ≈ 12 min; peak RAM ≈ 6.5 GB, so stop Docker containers first
```

Or step by step:

```bash
python -m src.preprocessing.clean_traffic      # raw wide CSV → clean/ + reports/preprocessing/pems_bay_cleaning_report.md
python -m src.preprocessing.build_features     # clean/ → features/ (+ sensor_stats/, slot_profile/, features_meta.json)
python -m pytest -q tests/test_features.py     # 8 Phase 3 tests
python -m src.preprocessing.verify_features    # independent checks on the written outputs
```

### Prediction target

**`label` = 1 if the observed speed of a sensor 5 minutes from now is below 35 mph.** The threshold is defined
once, in `src/preprocessing/congestion.py`:

* **Caltrans:** the congestion programme (HICOMP / Mobility Performance Report) counts freeway congestion as
  speeds of 35 mph or less for 15 minutes or more. PeMS reports delay against 35 and 60 mph.
* **Fits these sensors:** free-flow speed (train-period 85th percentile) is 62–72 mph on every sensor, so 35 mph
  is about 52% of free-flow, i.e. a breakdown of flow.
* **Episodes are sustained:** 96.8% of congested readings in the training period belong to episodes lasting at
  least 15 minutes, so a 5-minute label is not noisy.
* **Robust to the rule chosen:** a per-sensor "below 50% of free-flow" rule disagrees with 35 mph on only 0.28% of
  readings. The full sensitivity table is in the feature report.

### Cleaning (`clean_traffic.py`)

| Step | Result on PEMS-BAY |
|---|---|
| Local wall-clock → UTC; snap to the 5-min grid; collapse duplicates; complete the sensor × time grid | 16,937,700 rows, no gaps in UTC (the DST "gap" is not real time) |
| ≤ 0 mph → invalid | 521 |
| Stuck detector (identical for ≥ 6 h, or identical below 35 mph for ≥ 1 h) → invalid | 55,980 (0.33%) |
| Causal forward-fill ≤ 15 min | 1,531 imputed; 54,970 left missing (long stuck runs) |

### Features (`build_features.py`, 41 columns, full dictionary in the feature report)

temporal (hour, slot, ISO weekday, weekend, US holiday, sin/cos) · current speed and state · lags 5/10/15/30/60 min,
plus the target slot 1 day and 1 week earlier · deltas · rolling mean/std/min and congested share over
15/30/60 min · graph: adjacency-weighted upstream/downstream speed and congested share · sensor: lat/lon,
in/out degree, free-flow speed, speed ratio, congestion rate · profile: train-period mean speed and congestion
rate for this sensor at the target's weekday and slot.

### Chronological split (70 / 10 / 20 of time, boundaries at local midnight, 60-minute purge)

| Split | Local dates | Rows | Positive rate | Persistence F1 |
|---|---|---|---|---|
| train | 2017-01-08 → 2017-05-07 (first 7 days are lag warm-up) | 11,131,321 | 3.81% | 0.921 |
| val | 2017-05-08 → 2017-05-25 | 1,661,630 | 5.10% | 0.927 |
| test | 2017-05-26 → 2017-06-30 | 3,331,372 | 4.11% | 0.922 |

The persistence baseline ("the next 5 minutes look like now") already reaches F1 ≈ 0.92, because congestion
5 minutes ahead is highly autocorrelated. The MLlib phase must beat it, and should also report results on
**state-change rows** (`label != congested_now`), where persistence is always wrong.

### Leakage controls

* Every feature window ends at the current row. The clean grid is complete in UTC, so lag *k* is exactly *k* × 5 min.
* Targets come from **observed** readings only, never from forward-filled values.
* `ff_speed`, `sensor_congested_rate` and the `hist_*` profiles use **training rows only** (before the purge gap).
  A test proves that changing test-period data leaves every training feature and statistic unchanged.
* Splits are contiguous time blocks with no shuffling, and a 60-minute purge before each boundary.

### Verification (`verify_features.py`, run at the end of `run_phase3.sh`)

1. Raw files are unchanged and read-only.
2. Clean data is a complete sensor × UTC grid, at least 99% observed, and observed speeds equal the raw values.
3. `features_meta.json` matches the code. There are no nulls in features or label, the label is binary, and there
   is one row per (sensor, ts).
4. Splits are ordered with no overlap, respect the purge gap and boundaries, and each split contains both classes.
5. On 5 sensors, the label and `speed_lag_5m` are recomputed independently from `clean/` and must match exactly.

## Phase 4 — Spark core demonstrations & performance benchmarks

```bash
bash scripts/run_phase4.sh             # all 16.9M rows, 3 repeats after 1 warm-up (≈ 18–25 min)
bash scripts/run_phase4.sh --quick     # one month, 1 repeat (≈ 6 min)
```

Or step by step (each script writes `reports/spark_features/<name>.{md,json}` and refreshes `SUMMARY.md`):

```bash
python -m src.spark_features.dataframe_sql          # DataFrame API + Spark SQL + plan comparison
python -m src.spark_features.storage_benchmarks     # CSV vs Parquet
python -m src.spark_features.partition_benchmarks   # repartition / coalesce / shuffle partitions / AQE
python -m src.spark_features.optimization           # cache / persist / unpersist, join + aggregation shuffles
python -m pytest -q tests/test_spark_features.py    # 6 harness tests
```

**How the numbers are measured.** Each case runs in its own Spark job group. After the run, the harness reads
Spark's own task metrics from the monitoring REST API (`/api/v1/applications/<app>/jobs` and `/stages`): bytes
read, shuffle bytes and records, spill, tasks, and skipped stages. Times are the median of the measured repeats.
Benchmark files go to the WSL ext4 scratch disk and are deleted afterwards. Expect run-to-run variance of roughly
10–30% on a laptop. Ratios and byte counts are stable.

### Results on the full dataset (16,937,700 rows; details and charts in `reports/spark_features/SUMMARY.md`)

| Topic | Evidence |
|---|---|
| DataFrame vs SQL | 3/3 query pairs (aggregation, `LAG` window, join) have **identical** optimized and physical Catalyst plans and identical results; `explain("formatted")` saved in `plans/` |
| Lazy evaluation | A 6-step transformation chain launched **0 jobs**; `count()` launched 3 |
| Schema inference | CSV `inferSchema` = 2× the jobs and bytes read, guesses `double` for `float` columns |
| CSV vs Parquet | Parquet is **6.8%** of CSV's size (96 MB vs 1.4 GB). Full scan **1.1 s vs 23 s** (70 s with `inferSchema`). A 2-column aggregate reads 21 MB vs 1.4 GB |
| Pushdown / pruning | Predicate pushdown reads 2.4 MB instead of 21 MB. `partitionBy(month)` halves the tasks for a one-month query |
| coalesce vs repartition | 200→12 partitions: coalesce **0 B** shuffled (0.6 s) vs repartition 319 MB (3.9 s). Only repartition can increase the count |
| Shuffle partitions / AQE | 1000 partitions with AQE off: 1,012 tasks, 2.6 s. AQE on: **13 tasks, 0.6 s** |
| Pre-partitioning | `repartition("sensor_id")` + cache removes every later `Exchange`. Three keyed operations take 6.2 s → 2.6 s |
| Small files | `partitionBy(sensor_id)` writes 1,950 files of about 72 KB; scans 3× slower than `partitionBy(month)` |
| Caching | Reusing a windowed DataFrame: **0.57 s cached vs 11.1 s recomputed** (333 MB shuffle avoided). `unpersist()` frees all blocks. `MEMORY_ONLY`/`DISK_ONLY` are serialized (~30% smaller) |
| Join strategy | 16.9M ⋈ 325 rows: broadcast hash **0 B shuffled, 0.9 s** vs sort-merge 167 MB, 3.3 s |
| groupByKey vs reduceByKey | RDD `groupByKey` shuffles 8.2 MB vs 4.6 KB for `reduceByKey`. DataFrame `groupBy` is fastest (0.2 s vs 6–7 s, no Python serialization) |

`run_phase4.sh` ends with 14 checks. Examples: identical DataFrame/SQL plans, 0 jobs for the lazy chain, coalesce
shuffles 0 B, broadcast join shuffles 0 B, `unpersist()` releases everything, and scratch data is cleaned up.

## Phase 5 — Scala RDD demonstration & analytical questions Q1–Q9

```bash
bash scripts/run_phase5.sh              # sbt test+package, spark-submit Scala job, Q1–Q9, tests, 20 checks (≈ 6 min)
bash scripts/run_scala_rdd.sh           # Scala module only
python -m src.analytics.run_all         # Q1–Q9 in one Spark session (or --only q3 q7)
python -m src.analytics.q5_hotspots     # any single question
```

### Scala module (`scala-rdd/`, Scala 2.12.18, sbt 1.12.15, Spark 3.5.9 `provided`)

| File | Content |
|---|---|
| `build.sbt`, `project/build.properties` | Build; `sbt -batch test package` → 58 KB jar (`spark-submit` provides Spark at runtime) |
| `Model.scala` | `Obs` reading (compact primitives for Kryo), `TrafficState` (35/60 mph, same as PySpark), parsing |
| `Partitioners.scala` | `TrafficStatePartitioner` (one partition per state), `ObservationFrequencyPartitioner` (greedy LPT bin packing on per-sensor observation counts) |
| `TrafficRddDemo.scala` | `textFile` / `parallelize` → `flatMap` (1 wide line → 325 readings) → `map`/`filter` → pair RDDs (`reduceByKey`, `join`, `takeOrdered`) → `aggregateByKey(StatCounter)` / `combineByKey` / `histogram` → frequency profiles (`countByValue`) → custom partitioners (timed) → timed caching strategies → lineage + `checkpoint()` → `saveAsTextFile` |
| `ParsingAndPartitionersSpec.scala` | 6 ScalaTest tests (parsing, thresholds, partitioners, episode detection) |

Results (`reports/scala_rdd/scala_rdd_report.md`): 16,937,700 readings from 52,116 lines. The broadcast lookup and
the shuffle join give the same top 10. `combineByKey` and `StatCounter` means agree to 1e-13. The frequency
partitioner balances the skewed congestion workload perfectly (max/mean **1.000 vs 1.572** for hashing), and the
episode pass is 2.4× faster with identical results. The state partitioner removes the shuffle, but its 83% skew
makes it slower: a useful lesson. Deserialized cache vs re-parsing: 1.3× faster reuse at 2× the memory of Kryo
storage. `checkpoint()` truncates the lineage to a `ReliableCheckpointRDD`.

### Analytical questions (`src/analytics/`, reports in `reports/analytics/`, figures in `reports/figures/analytics/`)

Each question writes `qN.md` (question, method, computed findings, tables, figures, implications, limitations),
`qN.json`, CSV tables in `tables/`, and PNG figures. `SUMMARY.md` collects the key findings. Every comparative
statement in the text is derived from the computed numbers.

| Q | Topic | Headline (full data) |
|---|---|---|
| Q1 | Daily & weekly patterns | Working-day PM peak 17:00 (25.3% congested), AM peak 08:00 (17.6%); working days 18.6× weekends |
| Q2 | Recurring-profile consistency | 69% of congestion is recurrent; daily profiles correlate 0.96 with each sensor's average |
| Q3 | Episode duration & recovery | Median episode 30 min; episodes ≥ 1 h are 34% of episodes but 80% of congested minutes; median 50 min to free flow |
| Q4 | Speed volatility | Volatility peaks at 18:00; sudden changes 3.7× more frequent in the PM peak than at night; ρ = 0.67 with congestion |
| Q5 | Persistent hotspots | 148 sensors are congested ≥ 1 h on most working days; month-to-month rank ρ 0.94–0.97 |
| Q6 | Road characteristics | Negative result: the derived characteristics explain ≤ 2% of the variance (η²); congestion is site-specific |
| Q7 | Propagation | An onset raises a neighbour's congestion probability 3.7–3.9× within 15 min; no dominant direction at 5-min resolution |
| Q8 | Peers | 25 local bottlenecks (e.g. 402117: −23.7 mph vs peers); 7 of 10 faster-than-peer sensors lie directly downstream of one |
| Q9 | Severity & burden | CSBI (frequency, intensity, duration, PTI) is robust (ρ ≥ 0.98 when dropping any component); network peak TTI 1.44 |

## Phase 7 — MLlib pipeline: chronological tuning, evaluation, cost analysis, serialization (`src/ml/`)

```bash
bash scripts/run_phase7.sh                 # tests + train/tune + evaluate + verification (~25-40 min; --quick ≈ 12 min)
python -m src.ml.train_model [--quick] [--train-fraction 0.08] [--cost-fn 10 --cost-fp 1]
python -m src.ml.evaluate_model [--cost-fn X --cost-fp Y] [--bootstrap 1000]   # re-tune costs without retraining
python -m pytest -q tests/test_ml.py       # 16 tests (pipeline, imputation, costs, model I/O)
```

| Module | Role |
|---|---|
| `pipeline.py` | 7 stages: `Imputer` (median) → `StringIndexer` → `OneHotEncoder` → `VectorAssembler` (numeric) → `StandardScaler` → `VectorAssembler` (scaled + one-hot) → estimator (logistic regression / decision tree / random forest, class-weighted); hyper-parameter grids |
| `train_model.py` | Fits each candidate on the TRAIN period and scores it on the following VALIDATION period (chronological, no random folds); best per algorithm by validation PR-AUC; optimal model = lowest validation cost |
| `evaluate_model.py` | Test-period precision, recall, F1, ROC-AUC, PR-AUC, confusion matrices, cost of misses vs false alarms, cost-ratio sensitivity, day-block bootstrap vs persistence; reloads the saved model |
| `costs.py` | Cost model (default missed congestion : false alarm = 10 : 1), cost-optimal thresholds, sensitivity, bootstrap |
| `model_io.py` | `save_model` / `load_model` / `predict` / `verify_reload` (bundle = Spark `PipelineModel` + `bundle.json` + reference probabilities) |

Outputs: `data/models/pems_bay/ml_pipeline/{logistic_regression,decision_tree,random_forest,best}/`, `reports/ml/training.md`,
`reports/ml/evaluation.md` (+ `.json`, `tables/`), `reports/figures/ml/`. Thresholds (F1-optimal and cost-optimal) are chosen on
validation only and applied unchanged to test. Reuses `src/modeling` (data access, class weights, histogram metrics) from Phase 6.

Inference from the saved model:

```python
from src.ml.model_io import load_model, predict
bundle = load_model("data/models/pems_bay/ml_pipeline/best")
scored = predict(bundle, features_df)      # adds p = P(congested in 5 min) and alert = p >= bundle.threshold
```

Results (full data, 8% training sample): the validation-selected model is logistic regression. On the test period it reaches
F1 0.930 / PR-AUC 0.975 / ROC-AUC 0.999 at its F1-optimal threshold vs persistence F1 0.922 / PR-AUC 0.854; at the deployed
cost-optimal threshold recall is 98.7% (precision 81.6%) and the expected cost at 10:1 is 59% lower than persistence
(95% bootstrap CI excludes zero). Decision tree and random forest are within ~4% of its cost; the historical slot profile is worse than persistence.

## Phase 8 — simulated real-time streaming: Kafka → Spark Structured Streaming → MongoDB (`src/streaming/`)

```
 clean PEMS-BAY ──► replay_events.py ──► Kafka topic traffic.speed.events (3 partitions, key = sensor_id)
 (observed readings,   idempotent producer,            │
  chronological)       --speedup / --events-per-sec     ▼
                                          traffic_stream.py  (Spark Structured Streaming, 4 queries, own checkpoints)
                       parse (explicit schema) → validate → watermark 10 min → dropDuplicates
                         ├─ predictions     LiveFeatureEngine (41 Phase-3 features) → Phase 7 MLlib pipeline → P(congested in 5 min)
                         ├─ sensor_windows  15-min sliding windows (5-min slide) per sensor
                         ├─ network_windows 15-min sliding windows over the network
                         └─ dead_letters    rejected payloads with the reason
                                                       ▼
                       MongoDB db `trafficsense`: traffic_predictions · sensor_windows · network_windows · readings · late_events · dead_letters ·
                       stream_state · batch_log · stream_metrics        (all writes = upserts on a deterministic _id)
```

```bash
docker compose up -d --wait                         # Kafka (KRaft) + MongoDB, 127.0.0.1 only
bash scripts/run_phase8.sh                          # tests, parity, recovery test, 2 live runs, verification (~16 min); --skip-recovery/--skip-live/--skip-tests

# manual use (three terminals)
python -m src.streaming.traffic_stream --run demo --trigger "processing:2 seconds"                  # add --idle-finalize-seconds 1.5 for real-time mode
python -m src.streaming.replay_events --start 2017-06-05T12:00:00Z --hours 6 --speedup 60 --recreate-topic   # 60x real time; --speedup 0 = unthrottled
python -m src.streaming.streaming_metrics report --db trafficsense --run demo                       # throughput, batch duration, backlog, latency
python -m src.streaming.streaming_metrics health --db trafficsense --max-age 60                     # exit 1 if no batch committed recently
python -m src.streaming.recovery_test                                                                # documented restart / fault-tolerance test
python -m pytest -q tests/test_streaming.py                                                          # 14 tests (Kafka not needed; MongoDB tests skip if down)
docker compose down                                 # free ~1.3 GB before the batch phases (-v also deletes the volumes)
```

| Module | Role |
|---|---|
| `replay_events.py` | Producer: reads `clean/` (observed readings only), keeps the original `event_ts`, sends in event-time order at `--speedup X` (event seconds per wall second; 1 = real time, 0 = max) or `--events-per-sec N`; idempotent producer (`acks=all`); optional seeded fault injection (`--duplicate-fraction`, `--late-fraction`, `--malformed-fraction`) |
| `events.py` | Event contract: JSON schema v1, `EVENT_SCHEMA` (explicit `StructType`), `parse_events` → typed columns + `reject_reason` (malformed JSON, bad types, unknown schema version, event time outside the dataset span ±1 day, speed outside 0–120 mph) |
| `traffic_stream.py` | The Spark application: queries, watermark, windows, scoring, MongoDB sinks, checkpoints, restart logic |
| `live_features.py`, `reference.py` | Incremental computation of the 41 Phase-3 features from the event stream (7-day ring buffer, bounded causal forward-fill, shared neighbour kernel `neighbor_stats`), history/static inputs |
| `streaming_metrics.py` | `StreamingQueryListener` (every `StreamingQueryProgress` → JSONL + MongoDB), report (throughput, batch phases, Kafka backlog, state size, latency), health check |
| `recovery_test.py`, `verify_stream.py` | Fault-tolerance test; stream-vs-batch parity and fault accounting |

**Semantics.** Event time is the dataset's own timestamp. A slot (5 min) is *finalized* when every Kafka partition has delivered events at least
`--grace-slots` (default 1) slots later; an event is *late* when its slot is that far behind the newest slot of its partition. Both rules use event time
per partition only, so results are identical however events are split into micro-batches (this is what makes the recovery test an equality test).
`--idle-finalize-seconds N` (opt-in) additionally predicts a slot once ≥ 90% of the sensors reported and it has been quiet for N s: use it for a
real-time feed (otherwise a prediction waits for the next slot's first events, one slot period = 5 min); slot contents then depend on timing. The history needed by
lag/rolling features (7 days) comes from the Phase 3 `clean/` table (the stand-in for a production historical store) and is always older than the streamed events.

**Checkpointing and recovery.** Each query has its own checkpoint (`~/.cache/trafficsense/streaming/checkpoints/<run>/<query>`, local ext4): Kafka offsets, commits,
de-duplication and window state. The sink is at-least-once, made effectively-once by idempotent upserts. The driver-side feature state is rebuilt on start from the
history plus the `readings` persisted in MongoDB, guided by the `stream_state` document (`RECOVERED state …` in the log). Checkpoint and MongoDB must be reset together
(`--reset`); a mismatch aborts with an explanation instead of silently skipping or duplicating data.

### Kafka and MongoDB configuration guidance
| Topic | Setting used | Why / production note |
|---|---|---|
| Broker | `apache/kafka:3.9.1`, KRaft single node, `KAFKA_HEAP_OPTS=-Xmx768m`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` | No ZooKeeper; topics are created explicitly so partitions and retention are never accidental. Production: ≥ 3 brokers, replication factor 3, `min.insync.replicas=2` |
| Listeners | `PLAINTEXT://0.0.0.0:9092`, advertised `localhost:9092`, port bound to 127.0.0.1 | Clients run on the WSL host. For clients in other containers add a second listener with the container-network name. Add TLS/SASL outside a laptop |
| Topic | 3 partitions, key = `sensor_id`, `retention.ms` 7 d | Same sensor → same partition → per-sensor order. Partitions bound the Spark read parallelism; `--partitions` must equal the topic's count (slot completeness waits for every partition) |
| Record timestamps | wall-clock `CreateTime` (not the 2017 event time) | The original time is in the payload (`event_ts`). Setting record timestamps to 2017 would let time-based retention delete the replay immediately |
| Producer | idempotent, `acks=all`, `linger.ms=5`, `lz4`, 2,000-message batches | No loss, no duplicates or reordering on retry; ~15,000 msg/s unthrottled on this machine |
| Spark source | `subscribe`, `startingOffsets=earliest` (new checkpoints only), `maxOffsetsPerTrigger` optional, `failOnDataLoss=true` | Offsets live in the Spark checkpoint, not in Kafka consumer groups. `maxOffsetsPerTrigger` bounds catch-up batches. Connector `spark-sql-kafka-0-10_2.12:3.5.9` must equal the Spark version (resolved through `spark.jars.packages`, cached in `~/.ivy2`) |
| Spark | `spark.sql.shuffle.partitions=8` (fixed), checkpoints on ext4 | The state-store partition count is frozen into a checkpoint: changing it requires a new checkpoint. `/mnt/c` is too slow and not rename-atomic for checkpoints |
| MongoDB | `mongo:7.0`, WiredTiger cache 0.5 GB, no auth (127.0.0.1 only) | Deterministic `_id`s give idempotent upserts; indexes on (sensor_id, event_ts) and event_ts. Production: auth + TLS, replica set, and a TTL index on `readings` (the recovery reads it from `first_slot`) |

### Verification evidence (`scripts/run_phase8.sh`, all checks passed; details in `reports/streaming/`)
| Check | Result |
|---|---|
| Stream = batch (6 h from Monday 05:00 local, 23,004 predictions, rush hour) | max \|Δp\| 5.3e-7 vs the Phase 3 features scored by the same model; 22,893/22,893 comparable rows, 2,953 alerts identical; every one of the 41 features equals Phase 3 (unit test) |
| Windows | 22,356 sensor + 69 network windows equal a pandas aggregation exactly |
| Faults injected (246 duplicates, 232 late, 235 malformed) | 235/235 dead-lettered with a reason; duplicates removed; 228 late rejected (rest within tolerance); accepted + late = unique events |
| Recovery test: `kill -9` after batch 3 (7,475 of 23,004 predictions stored) and a crash after the output write but before the commit (2,275 documents written) | After restart both runs are identical to the uninterrupted run: same predictions (max \|Δp\| 0), readings, late events, dead letters, windows, final state; the interrupted batch was rewritten with identical values; state recovered from MongoDB in ~1 s (29 checks) |
| Live run at 60× real time (2 s trigger) | no backlog; micro-batches 1–4 s; deterministic mode: latency p50 9.2 s after warm-up (one slot period of 5 s is waiting for the next slot); real-time mode: p50 5.5 s, p95 9.4 s; all far inside the 5-minute horizon |
| Capacity | One slot (325 events) is scored in ≈ 1–1.5 s (Spark scoring dominates); a 300× replay (≈ 330 events/s) builds a backlog, i.e. the pipeline sustains roughly 230× real time with four concurrent queries on this laptop |

Limitations: the last slot(s) of a finite replay stay pending until later events arrive (grace); one driver holds the feature state (scale-out would key it by sensor,
e.g. `applyInPandasWithState`); the historical store is the Phase 3 parquet, not a live database; `readings` grows unbounded without a TTL.
(Phase 9 renamed the predictions collection to `traffic_predictions`; index definitions now live in `src/mongo/schema.py`.)

## Phase 9 — MongoDB design, FastAPI backend with live CRUD (`src/mongo/`, `src/api/`)

```bash
docker compose up -d --wait mongo                       # MongoDB 7.0 (image pulled on first use), 127.0.0.1:27017, named volume `mongo-data`
python -m src.mongo.load_outputs                        # schema + load Spark analytics / hotspots / models / stream metrics / run history (idempotent)
uvicorn src.api.main:app --host 127.0.0.1 --port 8000   # or: python -m src.api.main        docs: http://127.0.0.1:8000/docs
bash scripts/run_phase9.sh                              # tests + end-to-end verification (~2.5 min); --seed-predictions fills traffic_predictions via Phase 8
python -m src.api.verify_api                            # only the real-process verification (kill -9, MongoDB outage, loader re-run, inference)
python -m pytest -q tests/test_mongo.py tests/test_api.py      # 17 + 40 tests against a real MongoDB
```

### Collections (`src/mongo/schema.py`: one place for validators and indexes; `ensure_schema` is idempotent and runs at API / stream / loader start)
| Collection | Documents | Written by | Key indexes |
|---|---|---|---|
| `sensor_metadata` | one per sensor (`_id` = sensor_id): editable name, description, direction, lanes, status, tags, coordinates + GeoJSON `location`; read-only `stats`; `version`, timestamps | **API (CRUD)**, loader (insert-only for editable fields) | unique sensor_id, 2dsphere location, (status, sensor_id), tags, source |
| `settings` | one per key (`_id` = key): typed value, category, description, `updated_by`, `version` | **API (CRUD)** | unique key, (category, key) |
| `congestion_hotspots` | per sensor: hotspot class, persistence, CSBI severity (Spark Q5 + Q9), location | loader | unique sensor_id, rank, (class, rank), csbi_rank, 2dsphere |
| `analysis_results` | 18 `question` docs (findings, implications, metrics) + 77 `table` docs (rows) | loader | (doc_type, qid), (qid, name) |
| `model_evaluations` | 3 models + 2 baselines: validation/test metrics, confusion matrices, cost, bootstrap vs persistence | loader | unique model, (kind, is_best) |
| `traffic_predictions` | 5-min-ahead probability per sensor and slot (`_id` = sensor\|slot) | streaming job | (sensor_id, event_ts), event_ts, (slot, probability), (alert, slot) |
| `stream_metrics` | StreamingQueryProgress documents + run summaries | progress listener, loader | (query, timestamp), (run, query, batch_id) |
| `pipeline_runs` | analytics questions, training, evaluation, streaming checks, data loads | loader | (kind, finished_at), status, name |

`$jsonSchema` validators on the human-edited collections (`sensor_metadata`, `settings`) reject bad documents even if something bypasses the API; the high-rate
streaming collections have none on purpose. `mongo_writer.py` (create/replace/update/delete with optimistic concurrency, idempotent loaders) and `mongo_reader.py`
(filters, pagination, `$geoNear`/`$geoWithin`, aggregations incl. `$percentile`) are the only code that touches the documents; routers never build queries.

### API structure
```
src/api/main.py          app factory, lifespan (connect, ensure_schema, optional model preload), middleware (request id), CORS, routers
src/api/config.py        TRAFFICSENSE_* environment / .env settings (see below);   errors.py   one error envelope + DB-error mapping
src/api/schemas.py       Pydantic models (strict: unknown fields rejected);        deps.py     db access, pagination, If-Match, API key, X-Updated-By
src/api/inference.py     lazy Spark PipelineModel loading + scoring;               serialize.py  document -> response model
src/api/routers/         health · sensors (CRUD) · settings (CRUD) · hotspots · predictions · analytics · models · streaming · pipeline
src/api/verify_api.py    real-process verification
```
| Resource | Endpoints (prefix `/api/v1`) |
|---|---|
| **sensors** | `GET /sensors` (status, tag, source, q, bbox, sort, order, limit, offset) · `GET /sensors/near` · `GET /sensors/{id}` (+ hotspot summary, ETag) · **`POST /sensors`** (201, Location) · **`PUT /sensors/{id}`** (replace) · **`PATCH /sensors/{id}`** · **`DELETE /sensors/{id}`** (204) |
| **settings** | `GET /settings` (defaults + overrides) · `GET /settings/{key}` · **`POST /settings`** (409 if exists) · **`PUT /settings/{key}`** (201/200) · **`PATCH /settings/{key}`** · **`DELETE /settings/{key}`** (known keys revert to the default) |
| hotspots | `GET /hotspots` · `/hotspots/classes` · `/hotspots/watchlist` (size and persistence cut-off come from the settings) · `/hotspots/{sensor_id}` |
| predictions | `GET /predictions/latest` (`alerts_only`) · `/predictions/overview` · `/predictions/sensors/{id}` — `alert` uses the `alert_threshold` setting |
| analytics | `GET /analytics/questions` · `/questions/{q1..q18}` · `/questions/{qid}/tables[/{name}]` (paginated rows) · `/analytics/figures/{png}` |
| models | `GET /models/evaluations[/{model}\|/best]` · `/models/info` · `/models/example` · **`POST /models/predict`** (saved Spark model; ≤ 1,000 rows; `speed` required, other features imputed) |
| streaming / pipeline | `GET /streaming/runs · /metrics/summary · /metrics/progress · /reports · /health` · `GET /pipeline/status · /runs · /runs/{id}` |
| ops | `GET /health` (liveness) · `/ready` (MongoDB + schema) · `/api/v1/status` · `/docs` · `/openapi.json` |

Conventions: every error is `{"error": {"code", "message", "details", "request_id"}}` (422 with per-field `details`, 404, 409, 412, 401, 503 with `Retry-After`, 500 without internals);
`PUT/PATCH/DELETE` honour `If-Match: "<version>"` (412 on a stale version; no lost updates), `X-Updated-By` is stored as `updated_by`; when `TRAFFICSENSE_API_KEY` is set every write needs
`X-API-Key` (constant-time compare; reads stay open). Dataset-derived `stats` are read-only; re-running the loader never overwrites analyst edits.
Settings (known keys are typed and bounded, custom keys take any JSON ≤ 8 KB): `alert_threshold` (0.686), `watchlist_size` (10), `hotspot_min_persistence` (0.5),
`prediction_window_hours` (6), `default_page_size` (50), `dashboard_refresh_seconds` (10), `sensor_status_filter` (active).

### Configuration (environment variables or `.env` at the project root; real variables win; `.env` is git-ignored)
| Variable | Default | Meaning |
|---|---|---|
| `TRAFFICSENSE_ENV` | `development` | `production` requires an API key (≥ 16 chars) and explicit CORS origins |
| `TRAFFICSENSE_HOST` / `_PORT` / `_LOG_LEVEL` | `127.0.0.1` / `8000` / `INFO` | used by `python -m src.api.main` |
| `TRAFFICSENSE_MONGO_URI` / `_MONGO_DB` / `_MONGO_TIMEOUT_MS` | `mongodb://localhost:27017` / `trafficsense` / `3000` | shared with the streaming job; the timeout is how long a request waits before answering 503. With authentication use `mongodb://user:pass@host:27017/?authSource=admin&tls=true` |
| `TRAFFICSENSE_API_KEY` | empty (writes open) | `python -c "import secrets; print(secrets.token_urlsafe(32))"` |
| `TRAFFICSENSE_CORS_ORIGINS` | `http://localhost:3000,http://127.0.0.1:3000` | comma-separated dashboard origins |
| `TRAFFICSENSE_DOCS_ENABLED` / `_MAX_PAGE_SIZE` | `true` / `500` | hide `/docs` in production; upper bound for `limit` |
| `TRAFFICSENSE_ENABLE_MODEL_INFERENCE` / `_PRELOAD_MODEL` | `true` / `false` | `PRELOAD_MODEL=true` loads Spark + the model in the background at startup (otherwise the first prediction pays ~25–50 s) |
| `TRAFFICSENSE_MODEL_PATH` / `_SPARK_DRIVER_MEMORY` / `_MAX_PREDICT_INSTANCES` | Phase 7 `best` / `1g` / `1000` | model bundle, memory of the inference JVM, request cap |

The table above is the reference for `.env`: create `.env` with only the variables you change (one `NAME=value` per line, `#` for comments). A ready-made `.env.example`
could not be written in this environment (blocked by an organization content policy), so it is not part of the repository.

### Verification (`scripts/run_phase9.sh`, `reports/api/verification.md`: 71 tests and 34 end-to-end checks passed)
| Scenario | Evidence |
|---|---|
| Live CRUD over HTTP against the real database | create (201 + Location + ETag), PATCH, PUT with `If-Match`, delete (204 then 404), duplicate (409), stale version (412), invalid body (422 with field), an edit of real sensor 400001; settings create/put/patch/delete; settings change behaviour (watch-list size 5, alert threshold 0.6) |
| Documents are really in MongoDB | read with an independent client: version 3, GeoJSON location updated, deleted documents gone, dataset `stats` untouched |
| `kill -9` of the API, new process | every sensor and setting field, version and timestamp identical; deletions persisted; persisted settings still drive the watch-list and alerts |
| MongoDB stopped and started | 503 `database_unavailable` (+ `Retry-After`) after the 3 s server-selection timeout, process and `/health` stay up, `/ready` 503; the API recovers 1.2 s after `docker start` without a restart; data intact |
| Loader re-run | analyst edits (name, tags, status, version) kept; `stats_synced_at` refreshed |
| Saved-model inference | `POST /models/predict` equals the probabilities stored by the streaming job (5 sensors, max \|Δp\| 2.3e-7) and the values recorded when the model was saved; cold start 25 s, warm 1.0–1.2 s per call |

Limitations: inference runs the Spark pipeline in the API process (fine for dashboards and spot checks; export the model for high-throughput serving); `GET /models/example` scans the feature
parquet (~3 s); authentication is a single shared API key (add per-user auth/roles before multi-user use); MongoDB runs without auth in `docker-compose.yml` and is bound to 127.0.0.1;
`traffic_predictions` and `readings` grow without a TTL.

## Phase 10 — Next.js dashboard, integration tests, final review material (`frontend/`, `tests/`, `reports/final/`)

```bash
# once (WSL): dependencies and production build
cd frontend && npm ci && npm run build && cd ..
# three terminals (or let scripts/run_phase10.sh start what is missing)
docker compose up -d --wait mongo                        # MongoDB
uvicorn src.api.main:app --host 127.0.0.1 --port 8000    # FastAPI
cd frontend && npm run start                             # dashboard on http://localhost:3000  (npm run dev for development)
# verification
bash scripts/run_phase10.sh                              # build + pytest (all) + vitest + benchmark, logs in reports/final/logs/
pytest -q tests/test_frontend_contract.py tests/test_integration_e2e.py   # contract + end-to-end (starts its own API and dashboard)
cd frontend && npm test                                  # vitest against the live API
python scripts/benchmark_dashboard.py                    # latency / throughput / bundle size -> reports/final/dashboard_benchmark.json
```

### Frontend architecture (`frontend/src/`)

| Path | Role |
|---|---|
| `app/layout.tsx`, `components/Shell.tsx` | persistent top navigation (8 pages), project-scope strip (live from `/status`, Q13, Q1), footer, toast provider |
| `app/{page,analytics,hotspots,live,models,performance,data,status}/page.tsx` | the 8 pages: landing, Traffic Analytics (Q1–Q18), Congestion Hotspots, Simulated Live Traffic, Model Evaluation, Spark Performance, Data Management (CRUD), Analysis History & System Status |
| `app/backend/[...path]/route.ts` | server-side gateway to FastAPI (`/backend/*` → `TRAFFICSENSE_API_URL`): no CORS, adds `TRAFFICSENSE_API_KEY` to requests, forwards `If-Match`, answers 502 `api_unreachable` when the API is down |
| `lib/api.ts`, `lib/hooks.ts` | one fetch client (error envelope → `ApiError`, `If-Match` versioning) and an SWR hook (`useApi`) with polling |
| `lib/charts.ts` | declarative chart specification for every question (`QUESTION_CHARTS`) + `buildFigure`; data comes from the Mongo analysis tables |
| `components/ui.tsx`, `PlotChart.tsx`, `QuestionChart.tsx`, `SensorsTab.tsx`, `SettingsTab.tsx` | design-system pieces, lazy Plotly wrapper, per-chart data loading, the two CRUD editors |

| Variable (frontend) | Default | Purpose |
|---|---|---|
| `TRAFFICSENSE_API_URL` | `http://127.0.0.1:8000` | where the `/backend` gateway forwards (server-side only) |
| `TRAFFICSENSE_API_KEY` | empty | must equal the API's key when write protection is on; never sent to the browser |
| `NEXT_PUBLIC_API_BASE` | `/backend` | set to an API URL to call the API directly from the browser (then add the dashboard origin to `TRAFFICSENSE_CORS_ORIGINS`) |

Final-review documents: `reports/final/traceability_matrix.md`, `performance_summary.md`, `presentation_outline.md`, `verification_report.md`.
Verification result: 130 Python tests and 29 frontend tests pass; all 8 pages and Q1–Q18 were checked in a browser against live data (details in the verification report).
