# Feature & Target Report — PEMS-BAY

- Generated (UTC): 2026-10-20T13:22:16+00:00 · runtime 563.3 s · Spark 3.5.9
- Output: `/mnt/c/Users/ssis/Desktop/bdaa/data/processed/pems_bay/features` (Parquet, partitioned by `split`) · 41 features

## 1. Prediction target

**`label` = 1 if the observed speed of the sensor at t + 5 min is below 35 mph, else 0.** Features use only information available at t.

### Why 35 mph

- Caltrans State Highway Congestion Monitoring Program (HICOMP) / Mobility Performance Report: congestion = freeway speeds of 35 mph or less lasting 15 minutes or longer on a typical weekday (https://dot.ca.gov/programs/traffic-operations/mpr).
- Caltrans PeMS performance measures: vehicle-hours of delay computed against 35 mph and 60 mph thresholds.
- Free-flow speed (train-period 85th percentile) ranges 62.2–72.2 mph (median 67.8); all sensors are freeway mainline, so one absolute threshold is meaningful and 35 mph ≈ 52% of typical free-flow (flow breakdown).
- Duration: Caltrans counts congestion lasting ≥ 15 min. In the training period **96.8%** of congested readings belong to episodes ≥ 15 min (median episode 30 min, 35,146 episodes), so the 5-minute label tracks sustained congestion rather than noise.

### Sensitivity (training period, observed readings)

| Rule | Positive rate | Disagreement with 35 mph |
|---|---|---|
| speed < 25 mph | 1.83% |  |
| speed < 30 mph | 2.75% |  |
| **speed < 35 mph** ✔ | 3.74% |  |
| speed < 40 mph | 4.76% |  |
| speed < 45 mph | 5.82% |  |
| speed < 50 mph | 7.10% |  |
| speed < 60 mph | 16.40% |  |
| speed < 50% of sensor free-flow | 3.48% | 0.28% |
| speed < 60% of sensor free-flow | 4.83% | 1.09% |
| speed < 70% of sensor free-flow | 6.27% | 2.54% |

Descriptive levels for the dashboard (`level_next`): **free** >= 60 mph (no delay by the PeMS 60 mph threshold); **slowed** 35-60 mph (delayed but flowing); **congested** < 35 mph (Caltrans congestion threshold).

## 2. Chronological split

Fractions 70% / 10% / 20% of the time axis, boundaries snapped to local midnight; the last 60 min before each boundary are purged.

| Split | From (local) | To (local) | Rows | Positives | Positive rate | Persistence F1 |
|---|---|---|---|---|---|---|
| train | 2017-01-07 23:55 | 2017-05-07 22:55 | 11,131,321 | 424,032 | 3.81% | 0.921 |
| val | 2017-05-08 00:00 | 2017-05-25 22:55 | 1,661,630 | 84,769 | 5.10% | 0.927 |
| test | 2017-05-26 00:00 | 2017-06-30 23:50 | 3,331,372 | 136,859 | 4.11% | 0.922 |

Boundaries (local): train starts 2017-01-01 00:00, val starts 2017-05-08 00:00, test starts 2017-05-26 00:00, data ends 2017-07-01 00:00 (exclusive).

**Persistence baseline** = predict that the next slot has the same state as now. Any model must beat this F1 on validation/test to be useful.

## 3. Rows not used for modelling

| Reason | Rows |
|---|---|
| `warmup_7d` | 652,978 |
| `feature_gap` | 95,568 |
| `current_speed_missing` | 54,898 |
| `purge_gap` | 7,800 |
| `target_not_observed` | 2,133 |
| **kept** | **16,124,323** of 16,937,700 |

## 4. Leakage controls

| Control | How |
|---|---|
| No look-ahead in features | All lags/rolling windows use frames ending at the current row; the grid is complete in UTC, so row offsets are exact time offsets |
| Target from real observations | `label` uses the next **observed** reading; forward-filled values are never targets |
| Train-only statistics | `ff_speed`, `sensor_congested_rate`, `hist_*` profiles use training rows before the purge gap only |
| Leave-one-out profiles | For training rows, `hist_speed_next` / `hist_congested_rate_next` exclude the row's own target reading ((sum − own value) / (n − 1)); otherwise models learn to trust a profile that secretly contains the label |
| Chronological split | Contiguous time blocks, no shuffling; the model never sees the future during training |
| Purge gap | Last 60 min before each boundary dropped, so no target crosses a split |
| Same code for streaming | Feature functions are importable; `sensor_stats/` and `slot_profile/` are reused by the streaming job |

`sensor_congested_rate` and `ff_speed` also include each training row once among ~35,000 readings per sensor; this in-sample effect is negligible, so no leave-one-out is applied to them.

## 5. Feature dictionary

| # | Feature | Group | Description |
|---|---|---|---|
| 1 | `hour` | temporal | Local hour of day (0-23) |
| 2 | `slot_of_day` | temporal | Local 5-minute slot of the day (0-287) |
| 3 | `day_of_week` | temporal | ISO day of week, Monday=1 ... Sunday=7 (local) |
| 4 | `is_weekend` | temporal | 1 on Saturday/Sunday |
| 5 | `is_holiday` | temporal | 1 on US federal holidays (observed dates) |
| 6 | `tod_sin` | temporal | sin(2*pi*slot_of_day/288) - cyclical time of day |
| 7 | `tod_cos` | temporal | cos(2*pi*slot_of_day/288) |
| 8 | `dow_sin` | temporal | sin(2*pi*(day_of_week-1)/7) - cyclical day of week |
| 9 | `dow_cos` | temporal | cos(2*pi*(day_of_week-1)/7) |
| 10 | `speed` | current | Cleaned speed at t (mph) |
| 11 | `is_imputed` | current | 1 if the speed at t was forward-filled |
| 12 | `congested_now` | current | 1 if speed at t < 35 mph |
| 13 | `speed_lag_5m` | lag | Speed at t-5 min |
| 14 | `speed_lag_10m` | lag | Speed at t-10 min |
| 15 | `speed_lag_15m` | lag | Speed at t-15 min |
| 16 | `speed_lag_30m` | lag | Speed at t-30 min |
| 17 | `speed_lag_60m` | lag | Speed at t-60 min |
| 18 | `speed_target_slot_1d` | lag | Speed 24 h before the target slot (t+5 min-24 h) |
| 19 | `speed_target_slot_1w` | lag | Speed 7 days before the target slot (t+5 min-7 d) |
| 20 | `delta_5m` | lag | speed - speed_lag_5m (mph per 5 min) |
| 21 | `delta_15m` | lag | speed - speed_lag_15m |
| 22 | `roll_mean_15m` | rolling | Mean speed over (t-15 min, t] |
| 23 | `roll_mean_30m` | rolling | Mean speed over (t-30 min, t] |
| 24 | `roll_mean_60m` | rolling | Mean speed over (t-60 min, t] |
| 25 | `roll_std_60m` | rolling | Std-dev of speed over (t-60 min, t] |
| 26 | `roll_min_60m` | rolling | Minimum speed over (t-60 min, t] |
| 27 | `congested_frac_60m` | rolling | Share of the last 12 slots below 35 mph |
| 28 | `imputed_count_60m` | rolling | Forward-filled readings in the last 12 slots (data quality) |
| 29 | `nbr_up_speed` | graph | Adjacency-weighted mean speed of upstream sensors at t (own speed if none) |
| 30 | `nbr_down_speed` | graph | Adjacency-weighted mean speed of downstream sensors at t (own speed if none) |
| 31 | `nbr_up_congested_frac` | graph | Adjacency-weighted share of upstream sensors below 35 mph at t |
| 32 | `nbr_down_congested_frac` | graph | Adjacency-weighted share of downstream sensors below 35 mph at t |
| 33 | `latitude` | sensor | Sensor latitude |
| 34 | `longitude` | sensor | Sensor longitude |
| 35 | `in_degree` | sensor | Number of upstream neighbours in the road graph |
| 36 | `out_degree` | sensor | Number of downstream neighbours in the road graph |
| 37 | `ff_speed` | sensor | Free-flow speed: 85th percentile of observed speed (train period only) |
| 38 | `speed_ratio_ff` | current | Current speed / sensor free-flow speed (current state normalised per sensor) |
| 39 | `sensor_congested_rate` | sensor | Share of observed readings below 35 mph (train period only) |
| 40 | `hist_speed_next` | profile | Mean train-period speed for this sensor at the target's day-of-week and slot (leave-one-out for training rows) |
| 41 | `hist_congested_rate_next` | profile | Train-period congestion rate for this sensor at the target's day-of-week and slot (leave-one-out for training rows) |

Other columns: keys `sensor_id`, `ts_utc`, `ts_local`, `date_local`; target `label`; auxiliary targets `speed_next` (regression) and `level_next` (3-level); partition `split`.
