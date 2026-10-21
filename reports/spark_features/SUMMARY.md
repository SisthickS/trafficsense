# Phase 4 — Spark Core Demonstrations & Benchmarks (summary)

Generated from the JSON results in this folder. Full tables, plans and charts are in the linked reports.

## [DataFrame API & Spark SQL demonstration](dataframe_sql.md)

_run 2026-10-20T11:29:11+00:00 · data: cleaned PEMS-BAY readings (all 6 months, 16,937,700 rows)_

- Schema inference on CSV cost 4 Spark jobs vs 2 with an explicit schema (9.7s vs 0.8s), and it guessed different types for 2 column(s).
- Lazy evaluation: building a 6-step transformation chain launched 0 jobs in 121 ms; the count() action launched 3 job(s) and took 1.5s.
- Joins: broadcast hint → BroadcastHashJoin, merge hint → SortMergeJoin; 6 sensor(s) were never congested (left anti), 319 were (left semi).
- Window functions found 52,010 congestion onsets with lag(); row_number, rank/dense_rank, running totals and 30-min rangeBetween windows demonstrated.
- DataFrame vs SQL: 3/3 query pairs have identical optimized plans, 3/3 identical physical plans, 3/3 identical results.

## [Storage benchmarks: CSV vs Parquet](storage_benchmarks.md)

_run 2026-10-20T11:42:38+00:00 · data: cleaned PEMS-BAY readings, all 6 months, 16,937,700 rows × 9 columns_

- Size: Parquet+snappy is 6.8% of CSV (96.0 MB vs 1.4 GB); zstd 5.1%; gzip-CSV 9.5%.
- Full scan: Parquet 1.10s vs CSV 23.02s with an explicit schema (20.9× faster) and 70.34s with inferSchema (CSV read twice: 2.8 GB).
- Column pruning: a 2-column aggregate read 21.1 MB from Parquet vs 1.4 GB from CSV (15.1× faster).
- Predicate pushdown (sensor 400001, speed < 35): Parquet 0.24s with pushdown vs 0.37s without, CSV 4.23s; input 2.4 MB vs 21.1 MB.
- Partition pruning (month 2017-04): 0.49s / 16.7 MB / 6 tasks with partitionBy(month) vs 0.72s / 16.6 MB / 13 tasks on a flat layout.
- Location: the same Parquet scans in 1.52s on WSL ext4 vs 2.61s on the Windows drive (9p) — 1.7× slower across the VM boundary.

## [Partitioning benchmarks](partition_benchmarks.md)

_run 2026-10-20T11:22:00+00:00 · data: cleaned PEMS-BAY readings, all 6 months, 16,937,700 rows_

- coalesce(200→12) took 0.63s with 0 B shuffled; repartition(12) took 3.91s and shuffled 318.8 MB. Partition balance (max/mean): coalesce 1.02×, repartition 1.00×.
- Increasing partitions: coalesce(400) left 200 partitions; repartition(400) gave 400.
- Shuffle partitions (AQE off): best 48 → 0.84s, worst 1000 → 2.58s (1,012 tasks). With AQE on, 1000 requested partitions ran as 13 tasks in 0.61s.
- Pre-partitioning by sensor: three sensor-keyed operations needed 3 Exchange(s) and 6.15s on the raw data, vs 0 Exchange(s) and 2.64s after a one-time repartition+cache (7.75s).
- Small files: partitionBy(sensor_id) wrote 1,950 files (avg 71.7 KB) vs 6 for partitionBy(month); the full scan took 2.95s vs 0.98s.

## [Caching, persistence & shuffle analysis](optimization.md)

_run 2026-10-20T11:32:09+00:00 · data: cleaned PEMS-BAY readings, all 6 months, 16,937,700 rows_

- Caching: reusing the windowed DataFrame took 0.57s with persist(MEMORY_AND_DISK_DESER) vs 11.10s uncached (19× faster; uncached recomputed the window and shuffled 333.7 MB). End-to-end 10.76s vs 14.45s, because the first action pays to materialise the cache.
- Footprint: cache() 464.0 MB memory + 0 B disk; DISK_ONLY 324.7 MB disk; MEMORY_ONLY cached 48 partition(s). unpersist() left 0 storage entries.
- SQL CACHE TABLE is eager (9.71s, isCached=True); the follow-up query took 0.49s; UNCACHE TABLE → isCached=False.
- Joins: broadcast hash 0.90s / 0 B shuffled vs sort-merge 3.25s / 167.2 MB vs shuffle hash 3.48s / 167.2 MB.
- Aggregation over 2,890,110 readings: RDD groupByKey shuffled 8.2 MB vs reduceByKey 4.6 KB and DataFrame groupBy 7.6 KB; times 7.24s / 6.41s / 0.20s; results agree: True.
