# DataFrame API & Spark SQL demonstration

_Spark 3.5.9 · local[*] (12 cores) · driver 3g · 7.6 GB RAM · AQE true · 2026-10-20T11:29:11+00:00_

Data: cleaned PEMS-BAY readings (all 6 months, 16,937,700 rows)

## Key findings

- Schema inference on CSV cost 4 Spark jobs vs 2 with an explicit schema (9.7s vs 0.8s), and it guessed different types for 2 column(s).
- Lazy evaluation: building a 6-step transformation chain launched 0 jobs in 121 ms; the count() action launched 3 job(s) and took 1.5s.
- Joins: broadcast hint → BroadcastHashJoin, merge hint → SortMergeJoin; 6 sensor(s) were never congested (left anti), 319 were (left semi).
- Window functions found 52,010 congestion onsets with lag(); row_number, rank/dense_rank, running totals and 30-min rangeBetween windows demonstrated.
- DataFrame vs SQL: 3/3 query pairs have identical optimized plans, 3/3 identical physical plans, 3/3 identical results.

## 1. Explicit schemas

Parquet is read with an explicit `StructType`: `struct<sensor_id:int,ts_utc:timestamp,ts_local:timestamp,speed_raw:float,speed_mph:float,quality:string,issue:string,date_local:date,month:string>`

CSV sample (2,901,600 rows, one month) read two ways:

| Read mode | Seconds (read + count) | Spark jobs | Bytes read |
|---|---|---|---|
| inferSchema=true | 9.74 | 4 | 493.4 MB |
| explicit schema | 0.82 | 2 | 246.6 MB |

Types chosen by inference vs the explicit schema:

| Column | Explicit | Inferred |
|---|---|---|
| sensor_id | int | int |
| ts_utc | timestamp | timestamp |
| ts_local | timestamp | timestamp |
| speed_raw | float | double |
| speed_mph | float | double |
| quality | string | string |
| issue | string | string |
| date_local | date | date |

## 2. Lazy evaluation: transformations vs actions

| Step | Seconds | Spark jobs launched |
|---|---|---|
| build chain: filter → withColumn ×2 → select → groupBy/agg → orderBy | 0.12 | 0 |
| action: count() | 1.47 | 3 |

Physical operators: `Sort → Exchange → HashAggregate → Exchange → HashAggregate → Project → Filter → Scan parquet`. `filter`/`withColumn`/`select` are **narrow** (pipelined into one stage). `groupBy` and `orderBy` are **wide**; each adds an `Exchange` (shuffle). The plan has 2 `Exchange` node(s).

## 3. Actions

- `count()` → 16,937,700
- `first()` → `{'sensor_id': '400172', 'ts_local': '2017-06-01 00:00:00', 'speed_mph': 67.9}`
- `take(3)` → `[{'sensor_id': 400586, 'hour': 17, 'congested_rate': 0.711, 'avg_speed': 32.013}, {'sensor_id': 400895, 'hour': 17, 'congested_rate': 0.709, 'avg_speed': 29.549}, {'sensor_id': 400586, 'hour': 16, 'congested_rate': 0.707, 'avg_speed': 32.36}]`
- `collect()` (quality counts) → `{'missing': 54970, 'observed': 16881199, 'imputed': 1531}`
- `toPandas()` shape of top-5 → `[5, 4]`

`describe()`:
```
+-------+------------------+
|summary|speed_mph         |
+-------+------------------+
|count  |16882730          |
|mean   |62.611642116279434|
|stddev |9.591030172545086 |
|min    |3.0               |
|max    |85.1              |
+-------+------------------+
```
`show(5)` of the chain:
```
+---------+----+------------------+------------------+
|sensor_id|hour|congested_rate    |avg_speed         |
+---------+----+------------------+------------------+
|400586   |17  |0.7108655616942909|32.013351740977605|
|400895   |17  |0.7087962962962963|29.548564789471804|
|400586   |16  |0.7071823204419889|32.35953042388621 |
|401811   |17  |0.7067219152854513|34.85727439488715 |
|400172   |17  |0.7053406998158379|32.91984344758162 |
+---------+----+------------------+------------------+
only showing top 5 rows
```

## 4. Joins

| Join | Rows | Evidence |
|---|---|---|
| inner (per-sensor congestion ⋈ sensors) | 325 | physical: BroadcastHashJoin |
| left outer (sensors ⟕ congestion in 2017-01) | 325 | 32 sensors had no congestion → null → coalesce(0) |
| left semi (sensors ever congested) | 319 | keeps left rows that match |
| left anti (sensors never congested) | 6 | e.g. [400160, 400804, 400842, 401391, 401908] |
| self-join on road-graph edges (2 hops downstream) | 4,709 | from 2,369 directed edges |
| broadcast() hint | - | physical join: BroadcastHashJoin |
| merge hint | - | physical join: SortMergeJoin |

Top 5 most congested sensors (inner join):

| sensor_id | latitude | longitude | congested_pct |
|---|---|---|---|
| 400,586 | 37.377 | -121.943 | 17.9% |
| 400,172 | 37.376 | -121.940 | 17.8% |
| 400,895 | 37.381 | -121.961 | 17.8% |
| 400,069 | 37.386 | -121.977 | 14.6% |
| 400,971 | 37.369 | -121.901 | 14.0% |

## 5. Window functions

Congestion **onsets** (speed drops below 35 mph after being ≥ 35 mph, found with `lag`): **52,010**. Sample: sensor 407361 on 2017-05-07.

`lag` / `lead` around onset:

| ts_local | prev_speed | speed_mph | next_speed | delta_5m |
|---|---|---|---|---|
| 2017-05-07 10:00:00 | 55.300 | 42.300 | 40.300 | -13.000 |
| 2017-05-07 10:05:00 | 42.300 | 40.300 | 34.600 | -2.000 |
| 2017-05-07 10:10:00 | 40.300 | 34.600 | 32.400 | -5.700 |
| 2017-05-07 10:45:00 | 35.100 | 36.400 | 36.500 | 1.300 |
| 2017-05-07 10:50:00 | 36.400 | 36.500 | 35.800 | 0.100 |
| 2017-05-07 10:55:00 | 36.500 | 35.800 | 33.500 | -0.700 |

`row_number()`: 3 slowest readings of the day:

| rn | ts_local | speed_mph |
|---|---|---|
| 1 | 2017-05-07 18:05:00 | 25.200 |
| 2 | 2017-05-07 13:10:00 | 28.700 |
| 3 | 2017-05-07 13:25:00 | 28.800 |

`rank()` / `dense_rank()` of sensors by congested minutes that day:

| date_local | sensor_id | congested_minutes | rank | dense_rank |
|---|---|---|---|---|
| 2017-05-07 | 407361 | 350 | 1 | 1 |
| 2017-05-07 | 407339 | 170 | 2 | 2 |
| 2017-05-07 | 407155 | 55 | 3 | 3 |
| 2017-05-07 | 407157 | 45 | 4 | 4 |
| 2017-05-07 | 407153 | 40 | 5 | 5 |

Running total (`rowsBetween(unboundedPreceding, 0)`) and 30-minute **time-range** window (`rangeBetween` on epoch seconds), sampled through the day:

| ts_local | speed_mph | roll_avg_30min | cum_congested_min |
|---|---|---|---|
| 2017-05-07 00:00:00 | 67.400 | 67.400 | 0 |
| 2017-05-07 03:00:00 | 68.000 | 68.000 | 0 |
| 2017-05-07 06:00:00 | 67.800 | 66.800 | 0 |
| 2017-05-07 09:00:00 | 61.500 | 65.700 | 0 |
| 2017-05-07 12:00:00 | 32.100 | 33.600 | 80 |
| 2017-05-07 15:00:00 | 36.900 | 36.200 | 205 |
| 2017-05-07 18:00:00 | 35.100 | 34.200 | 255 |
| 2017-05-07 21:00:00 | 60.200 | 55.800 | 350 |

## 6. Spark SQL

Catalog: `[{'name': 'clean', 'database': None, 'temporary': True}, {'name': 'sensors', 'database': None, 'temporary': True}]` · global temp view `global_temp.sensors_global` rows: 325

| Query | Same optimized plan | Same physical plan | Same result | Rows | explain('formatted') |
|---|---|---|---|---|---|
| hourly_weekday_congestion | yes | yes | yes | 24 | [DF](plans/hourly_weekday_congestion__dataframe.txt) · [SQL](plans/hourly_weekday_congestion__sql.txt) |
| congestion_onsets_window | yes | yes | yes | 10 | [DF](plans/congestion_onsets_window__dataframe.txt) · [SQL](plans/congestion_onsets_window__sql.txt) |
| most_congested_sensors_join | yes | yes | yes | 5 | [DF](plans/most_congested_sensors_join__dataframe.txt) · [SQL](plans/most_congested_sensors_join__sql.txt) |

The DataFrame API and SQL are two front-ends to the **same Catalyst optimizer**. After analysis and optimization each pair produces an identical plan (expression IDs removed before comparing), so neither is faster.

All explain modes (simple, extended, cost, codegen) for the first query: [plans/hourly_weekday_congestion__all_modes.txt](plans/hourly_weekday_congestion__all_modes.txt)

**hourly_weekday_congestion**: `SELECT hour(ts_local) AS hour, round(avg(CAST(speed_mph < 35 AS INT)) * 100, 2) AS congested_pct, round(avg(speed_mph), 2) AS avg_speed, count(1) AS readings FROM clean WHERE quality = 'observed' AND dayofweek(ts_local) BETWEEN 2 AND 6 GROUP BY hour(ts_local) ORDER BY hour`

| hour | congested_pct | avg_speed | readings |
|---|---|---|---|
| 0 | 0.0% | 66.580 | 506,077 |
| 1 | 0.0% | 66.360 | 505,697 |
| 2 | 0.1% | 66.260 | 505,962 |
| 3 | 0.0% | 66.260 | 505,916 |
| 4 | 0.0% | 66.660 | 505,873 |

**congestion_onsets_window**: `SELECT sensor_id, count(1) AS onsets FROM (SELECT sensor_id, ts_utc, speed_mph, LAG(speed_mph) OVER (PARTITION BY sensor_id ORDER BY ts_utc) AS prev_speed FROM clean) WHERE speed_mph < 35 AND prev_speed >= 35 GROUP BY sensor_id ORDER BY onsets DESC, sensor_id LIMIT 10`

| sensor_id | onsets |
|---|---|
| 407,361 | 682 |
| 407,360 | 609 |
| 400,971 | 582 |
| 400,158 | 551 |
| 407,339 | 537 |

**most_congested_sensors_join**: `SELECT sensor_id, latitude, longitude, round(rate * 100, 2) AS congested_pct FROM (SELECT sensor_id, avg(CAST(speed_mph < 35 AS INT)) AS rate FROM clean WHERE quality = 'observed' GROUP BY sensor_id) c JOIN sensors USING (sensor_id) ORDER BY congested_pct DESC, sensor_id LIMIT 5`

| sensor_id | latitude | longitude | congested_pct |
|---|---|---|---|
| 400,586 | 37.377 | -121.943 | 17.9% |
| 400,172 | 37.376 | -121.940 | 17.8% |
| 400,895 | 37.381 | -121.961 | 17.8% |
| 400,069 | 37.386 | -121.977 | 14.6% |
| 400,971 | 37.369 | -121.901 | 14.0% |

