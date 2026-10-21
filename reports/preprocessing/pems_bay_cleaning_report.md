# Cleaning Report — PEMS-BAY

- Generated (UTC): 2026-10-20T13:12:51+00:00 · runtime 125.0 s
- Output: `/mnt/c/Users/ssis/Desktop/bdaa/data/processed/pems_bay/clean` (Parquet, partitioned by local `month`)
- Complete sensor × time grid: **yes** — 325 sensors × 52,116 UTC slots = 16,937,700 rows (2017-01-01 08:00:00 → 2017-07-01 06:55:00 UTC)

## 1. Timestamp handling

| Step | Rows |
|---|---|
| Input rows (wide → long) | 16,937,700 |
| Unparseable timestamps (dropped) | 0 |
| Off-grid timestamps (floored to 5 min) | 0 |
| Duplicate (sensor, UTC) rows collapsed | 0 |
| Rows inserted to complete the grid (`absent_row`) | 0 |

Local wall-clock timestamps were converted to UTC (`America/Los_Angeles` → UTC). The 12 "missing" local slots on 2017-03-12 02:00–02:55 never existed (DST spring-forward); in UTC the series is continuous, so no rows are needed for them.

## 2. Invalid measurements

| Issue | Rows | Share | Rule |
|---|---|---|---|
| `null_value` | 0 | 0.0000% | null / NaN |
| `non_positive` | 521 | 0.0031% | speed ≤ 0 mph (DCRNN stores missing readings as 0.0) |
| `out_of_range` | 0 | 0.0000% | speed > 100 mph |
| `stuck_sensor` | 55,980 | 0.3305% | identical readings ≥ 72 slots (6 h), or identical readings < 35 mph for ≥ 12 slots (1 h) |
| `absent_row` | 0 | 0.0000% | row missing from the source |

## 3. Gap filling (causal)

Forward-fill the last valid reading for at most 3 slots (15 min). This uses only past data, exactly what the live stream can do.

| Final quality | Rows | Share |
|---|---|---|
| `observed` | 16,881,199 | 99.6664% |
| `imputed` | 1,531 | 0.0090% |
| `missing` | 54,970 | 0.3245% |

| Issue → final quality | Rows |
|---|---|
| `non_positive` → `imputed` | 521 |
| `stuck_sensor` → `imputed` | 1,010 |
| `stuck_sensor` → `missing` | 54,970 |

Lowest per-sensor observed share: **93.438%** · sensors with unfilled gaps: 108 · max unfilled readings for one sensor: 3,346

| Worst sensors | Observed share | Unfilled |
|---|---|---|
| 402366 | 93.438% | 3,346 |
| 407190 | 95.809% | 2,161 |
| 400649 | 96.366% | 1,839 |
| 409525 | 96.776% | 1,657 |
| 400715 | 96.915% | 1,557 |

## 4. Output schema

| Column | Type | Description |
|---|---|---|
| `sensor_id` | int | PeMS station id |
| `ts_utc` | timestamp | Interval start, true UTC instant (use for ordering, lags, windows) |
| `ts_local` | timestamp | Interval start, local Pacific wall-clock (use for hour/day features) |
| `speed_raw` | float | Raw reading as published (null for inserted rows) |
| `speed_mph` | float | Cleaned speed: valid reading, else forward-filled ≤ 15 min, else null |
| `quality` | string | `observed` / `imputed` / `missing` |
| `issue` | string | Why the raw value was not used (null when valid) |
| `date_local`, `month` | date, string | Local calendar date; partition column |
