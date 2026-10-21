# Dataset Validation Report — PEMS-BAY

- **Overall status:** ✅ **PASS** (12 pass, 0 warn, 0 fail)
- **Generated (UTC):** 2026-10-20T08:07:52+00:00  ·  **Spark:** 3.5.9  ·  **Runtime:** 57.8 s
- **Raw directory:** `/mnt/c/Users/ssis/Desktop/bdaa/data/raw/pems_bay` (read-only, verified against MANIFEST.json)

## 1. Check summary

| ID | Check | Status | Result |
|---|---|---|---|
| C01 | Manifest & files present | ✅ PASS | 3/3 manifest files present |
| C02 | Raw data integrity | ✅ PASS | raw files unchanged since download (sha256 match) |
| C03 | Header / schema | ✅ PASS | 1 timestamp column + 325 numeric, unique sensor columns |
| C04 | Line structure (RDD scan) | ✅ PASS | 52,117 lines, 0 with a field count other than 326 |
| C05 | Typed parse (explicit schema, PERMISSIVE) | ✅ PASS | 0 malformed rows out of 52,116 |
| C06 | Record counts | ✅ PASS | 52,116 timestamps x 325 sensors = 16,937,700 readings (reference: 52,116 timestamps) |
| C07 | Timestamps | ✅ PASS | 2017-01-01 00:00:00 -> 2017-06-30 23:55:00 (181.0 days), 5-min grid of 52,128 slots, 12 missing slot(s) in 1 gap(s) - all explained by DST |
| C08 | Missing values | ✅ PASS | 521 of 16,937,700 readings missing (0.003%): 0 null/NaN, 521 zero-speed, 0 negative |
| C09 | Speed value distribution | ✅ PASS | valid speeds 3.0-85.1 mph, median 65.3, mean 62.62, 0 above 100 mph |
| C10 | Flat-line runs (stuck/imputed sensors) | ✅ PASS | 3,090 runs >= 60 min of identical speed across 184 sensors, covering 117,820 readings (0.696%) |
| C11 | Sensor locations | ✅ PASS | 325 sensors with coordinates, 0 reading-sensors lack coordinates, 0 outside expected bounding box |
| C12 | Adjacency matrix (graph) | ✅ PASS | 325x325 float32, sensor order matches CSV header |

## 2. Dataset profile

| Metric | Value |
|---|---|
| Sensors | 325 |
| Timestamps (rows) | 52,116 |
| Sensor readings (long format) | 16,937,700 |
| First timestamp | 2017-01-01 00:00:00 |
| Last timestamp | 2017-06-30 23:55:00 |
| Span (days) | 181.0 |
| Sampling interval (min) | 5 |
| Missing rate | 0.003% |
| Readings in ≥1 h flat-line runs | 0.696% |
| Raw CSV size (MB) | 85.7 |

## 3. Timestamp gaps

| Start | End | Missing slots | Minutes | Cause |
|---|---|---|---|---|
| 2017-03-12 02:00:00 | 2017-03-12 02:55:00 | 12 | 60 | DST spring-forward (local time does not exist) |

Interval histogram (seconds → count): `{'300': 52114, '3900': 1}`

## 4. Missing values

Rule: missing := null OR NaN OR speed == 0 OR speed < 0 (DCRNN encodes gaps as 0.0)

Sensors by missing rate: `{'(0,1%]': 270, '0%': 55}` · timestamps where every sensor is missing: **0**

| Sensor | Readings | Null/NaN | Zero | Missing rate |
|---|---|---|---|---|
| 404370 | 52,116 | 0 | 4 | 0.008% |
| 404444 | 52,116 | 0 | 4 | 0.008% |
| 404461 | 52,116 | 0 | 4 | 0.008% |
| 405613 | 52,116 | 0 | 4 | 0.008% |
| 407364 | 52,116 | 0 | 4 | 0.008% |
| 407373 | 52,116 | 0 | 4 | 0.008% |
| 408907 | 52,116 | 0 | 4 | 0.008% |
| 409526 | 52,116 | 0 | 4 | 0.008% |
| 400174 | 52,116 | 0 | 3 | 0.006% |
| 400292 | 52,116 | 0 | 3 | 0.006% |

## 5. Speed distribution (valid readings, mph)

| n | min | p01 | p05 | p25 | p50 | p75 | p95 | p99 | max | mean | std |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 16,937,179 | 3.0 | 19.6 | 40.0 | 62.2 | 65.3 | 67.5 | 69.8 | 71.8 | 85.1 | 62.621 | 9.588 |

### Longest flat-line runs (identical consecutive readings)

| Sensor | Start | Length (slots) | Hours | Speed (mph) |
|---|---|---|---|---|
| 407190 | 2017-03-12 16:00:00 | 384 | 32.0 | 69.0 |
| 407180 | 2017-05-31 17:05:00 | 371 | 30.9 | 69.0 |
| 407206 | 2017-05-31 17:05:00 | 371 | 30.9 | 69.0 |
| 400519 | 2017-02-16 00:00:00 | 288 | 24.0 | 68.1 |
| 400100 | 2017-04-20 00:00:00 | 288 | 24.0 | 68.1 |

## 6. Data dictionary

**Raw (`data/raw/`, wide CSV, immutable)**

| Column | Type | Description |
|---|---|---|
| *(blank header)* | string `yyyy-MM-dd HH:mm:ss` | 5-minute interval start, naive local Pacific time |
| `<sensor_id>` × N | double | Average speed (mph) at loop-detector station; `0.0` encodes a missing reading |

**Processed (`data/processed/<dataset>/readings/`, Parquet, partitioned by `month`)**

| Column | Type | Description |
|---|---|---|
| `ts` | timestamp | Interval start (local wall-clock, stored verbatim with session tz UTC) |
| `sensor_id` | int | Caltrans PeMS / LA Metro station id |
| `speed_mph` | double (nullable) | Cleaned speed; `null` where the raw reading is missing |
| `is_missing` | boolean | Raw reading was null/NaN/≤0 |
| `date`, `month` | date, string | Convenience/partition columns |

**Sensors (`data/processed/<dataset>/sensors/`)**: `sensor_id`, `column_index` (graph order), `latitude`, `longitude`, `n_readings`, `n_missing`, `missing_rate`.

## 7. Provenance

| File | Role | Bytes | SHA-256 | Verified |
|---|---|---|---|---|
| `PEMS-BAY.csv` | readings | 85,718,439 | `c39dc8f5f3a7f1e9…` | yes |
| `adj_mx_PEMS-BAY.pkl` | adjacency | 1,681,480 | `116275f5704d0d49…` | yes |
| `graph_sensor_locations_bay.csv` | sensor_locations | 9,750 | `276ee01059610774…` | yes |

Source: zenodo — https://doi.org/10.5281/zenodo.5724362 · retrieved 2026-10-20T08:07:48+00:00

## 8. Sources considered but not used

- **huggingface:witgaw/PEMS-BAY** — Pre-windowed ('augmented') samples: 12 input + 12 target steps per row, each reading duplicated ~24x with normalised time-of-day features. Not raw data; TrafficSense builds these windows itself with Spark window functions in Phase 5.

## 9. Full check details

See the JSON report alongside this file for every metric.
