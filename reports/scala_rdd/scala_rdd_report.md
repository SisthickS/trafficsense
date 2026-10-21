# Scala RDD demonstration — PEMS-BAY (raw wide CSV)

Spark 3.5.9 · Scala 2.12.18 · master `local[*]` · default parallelism 12 · partitions used 12 · Kryo serializer · input `/mnt/c/Users/ssis/Desktop/bdaa/data/raw/pems_bay/PEMS-BAY.csv`

## 1. RDD creation

| RDD | Created with | Partitions | Records |
|---|---|---|---|
| raw lines | sc.textFile(csv, minPartitions) | 12 | 52,117 |
| data lines | mapPartitionsWithIndex (drop header in partition 0) | 12 | 52,116 |
| sensor locations (pair RDD) | sc.textFile + flatMap(parseLocation) | 2 | 325 |
| hours 0-23 | sc.parallelize(0 until 24, 4) | 4 | 24 |

The header lists **325 sensors**. Their id array is shared with tasks as a **broadcast variable** (sent once per executor instead of once per task).

## 2. Transformations: flatMap, map, filter (+ accumulators)

| Step | Operation | Records |
|---|---|---|
| wide lines | textFile | 52,116 |
| readings | flatMap: 1 line → 325 Obs | 16,937,700 |
| valid readings | filter(speed > 0) | 16,937,179 |
| zero readings | accumulator inside foreach (action) | 521 |
| malformed lines | accumulator inside flatMap | 0 |

Materialising and caching the readings (`persist(MEMORY_AND_DISK_SER)`, Kryo) took **3.5 s**. Accumulators updated inside an *action* (`foreach`) are counted exactly once. Inside a *transformation* they can over-count if a partition is recomputed; here the readings are cached, so each partition is computed once.

## 3. Pair RDDs: reduceByKey, mapValues, join, takeOrdered

Congestion rate (share of valid readings < 35 mph) by hour, from `map → reduceByKey → mapValues`:

| Hour | 00 | 03 | 06 | 09 | 12 | 15 | 18 | 21 |
|---|---|---|---|---|---|---|---|---|
| weekday | 0.0% | 0.0% | 2.2% | 12.7% | 0.4% | 11.2% | 18.0% | 0.1% |
| weekend | 0.0% | 0.0% | 0.0% | 0.2% | 0.7% | 0.8% | 0.4% | 0.1% |

Weekday peak hour: **17:00** with **24.6%** of readings congested.

Top 10 most congested sensors (`reduceByKey` counts, `join` with readings and with the locations pair RDD, `takeOrdered`):

| Sensor | Congested share | Latitude | Longitude |
|---|---|---|---|
| 400586 | 17.91% | 37.377 | -121.943 |
| 400172 | 17.78% | 37.376 | -121.940 |
| 400895 | 17.69% | 37.381 | -121.961 |
| 400069 | 14.58% | 37.386 | -121.977 |
| 400971 | 14.03% | 37.369 | -121.901 |
| 400973 | 13.95% | 37.418 | -121.972 |
| 400104 | 13.72% | 37.419 | -121.955 |
| 400372 | 13.20% | 37.385 | -121.974 |
| 400394 | 13.17% | 37.364 | -121.903 |
| 403414 | 12.67% | 37.315 | -121.914 |

Map-side lookup with a broadcast location map gives the same top 10 without a shuffle join: **true**.

## 4. Distributed aggregations: aggregateByKey(StatCounter), combineByKey, stats(), histogram()

Network: n = 16,937,179, mean 62.62 mph, stdev 9.59, min 3.0, max 85.1.

| Speed bucket (mph) | Readings |
|---|---|
| 0–10 | 14,136 |
| 10–20 | 160,500 |
| 20–30 | 317,883 |
| 30–40 | 351,335 |
| 40–50 | 400,768 |
| 50–60 | 1,588,056 |
| 60–70 | 13,379,154 |
| 70–80 | 724,410 |
| 80–90 | 937 |

Five slowest sensors by mean speed (`aggregateByKey` with a `StatCounter` per key, one pass, map-side combined):

| Sensor | Mean | Stdev | Min | Max | Readings |
|---|---|---|---|---|---|
| 400970 | 53.945 | 12.718 | 10.100 | 79.700 | 52,115 |
| 400971 | 54.776 | 14.273 | 7.600 | 81.000 | 52,114 |
| 400895 | 55.762 | 18.024 | 9.500 | 73.800 | 52,114 |
| 400172 | 56.004 | 16.335 | 8.300 | 79.000 | 52,114 |
| 407373 | 56.058 | 8.290 | 13.700 | 73.900 | 52,112 |

`combineByKey` (sum, count) means agree with `aggregateByKey` StatCounter means: max |difference| = 1.42e-13 mph.

## 5. Frequency profiles

| Traffic state | Readings | Share |
|---|---|---|
| free | 14,104,501 | 83.27% |
| slowed | 2,166,077 | 12.79% |
| congested | 666,601 | 3.94% |
| missing | 521 | 0.00% |

Per-sensor traffic-state profile (`((sensor, state), 1) → reduceByKey → per-sensor Map`): sensors by share of congested readings:

| Congested share | Sensors |
|---|---|
| 0% | 6 |
| (0, 1%] | 91 |
| (1, 5%] | 125 |
| (5, 10%] | 76 |
| > 10% | 27 |

Observation frequency: valid readings per sensor range 52,112 – 52,116 (of 52,116 timestamps).

| Hour | free | slowed | congested |
|---|---|---|---|
| 00:00 | 98.5% | 1.4% | 0.0% |
| 03:00 | 98.0% | 2.0% | 0.0% |
| 06:00 | 83.7% | 14.7% | 1.6% |
| 09:00 | 68.7% | 22.1% | 9.2% |
| 12:00 | 84.7% | 14.8% | 0.5% |
| 15:00 | 67.3% | 24.4% | 8.2% |
| 18:00 | 62.7% | 24.2% | 13.1% |
| 21:00 | 97.2% | 2.7% | 0.1% |

Co-congestion: in 26,906 of 52,116 timestamps (51.6%) at least one sensor is congested; on those, on average 24.8 sensors at once (max 127).

## 6. Custom partitioners

**TrafficStatePartitioner**: one partition per traffic state, via `partitionBy` on `(state, speed)`.

| Partition | State | Records | Mean speed (computed per partition, no shuffle) |
|---|---|---|---|
| 0 | free | 14,104,501 | 65.897 |
| 1 | slowed | 2,166,077 | 53.040 |
| 2 | congested | 666,601 | 24.457 |
| 3 | missing | 521 | 0.000 |

Every partition holds exactly one state: **true**. Because `mapValues` keeps the partitioner, a later `reduceByKey` is a **narrow** dependency (ShuffleDependency: false, 3.01 s) instead of a new shuffle on unpartitioned data (ShuffleDependency: true, 1.37 s).

**Avoiding the shuffle did not make it faster here.** The state partitioner creates only 4 partitions and the largest holds 83% of all readings, so one task does most of the work while the other cores idle. The shuffled version runs 12 map tasks with map-side combining. A partitioner must also **balance** the data, which is what the frequency-aware partitioner below does.

**ObservationFrequencyPartitioner**: the workload is the **666601** congested readings, keyed by sensor and very skewed (per sensor: min 0, max 9333). Greedy bin packing on those frequencies vs `HashPartitioner(sensorId)`, followed by per-partition episode detection (group by sensor, sort by time):

| Partitioner | Partitions | Min records | Max records | Max / mean | Episode pass (s) | Episodes | Longest (min) |
|---|---|---|---|---|---|---|---|
| HashPartitioner | 12 | 32,178 | 87,352 | 1.572 | 0.395 | 52,012 | 980 |
| ObservationFrequencyPartitioner | 12 | 55,545 | 55,554 | 1.000 | 0.181 | 52,012 | 980 |

A stage finishes when its slowest task does, so the largest partition sets the runtime. Both partitioners find the same episodes: **true**.

## 7. Persistence, lineage and checkpointing (timed)

Workload: hourly congestion rate over all 16.9M readings, run 3 times on a freshly built RDD per strategy. Run 1 includes parsing (and storing, when cached); runs 2–3 measure reuse.

| Strategy | Run 1 (s) | Run 2 (s) | Run 3 (s) | Cached size (MB) |
|---|---|---|---|---|
| none (re-parse CSV) | 1.217 | 1.378 | 1.217 | - |
| MEMORY_ONLY (deserialized objects) | 2.473 | 0.704 | 0.510 | 581.489 |
| MEMORY_AND_DISK_SER (Kryo bytes) | 3.646 | 1.455 | 1.066 | 274.602 |

Reuse with **MEMORY_ONLY (deserialized objects)** takes 0.61 s vs 1.30 s re-parsing (**2.1× faster**). Deserialized objects are fastest to reuse but largest. Kryo-serialized storage is about 2–3× smaller but must be deserialized on every access.

Lineage (`toDebugString`) of per-sensor-day congestion counts **before** checkpointing:

```
(12) ShuffledRDD[115] at reduceByKey at TrafficRddDemo.scala:306 []
 +-(12) MapPartitionsRDD[114] at map at TrafficRddDemo.scala:306 []
    |   MapPartitionsRDD[8] at filter at TrafficRddDemo.scala:125 []
    |   MapPartitionsRDD[7] at flatMap at TrafficRddDemo.scala:116 []
    |       CachedPartitions: 12; MemorySize: 274.6 MiB; DiskSize: 0.0 B
    |   MapPartitionsRDD[2] at mapPartitionsWithIndex at TrafficRddDemo.scala:99 []
    |   /mnt/c/Users/ssis/Desktop/bdaa/data/raw/pems_bay/PEMS-BAY.csv MapPartitionsRDD[1] at textFile at TrafficRddDemo.scala:95 []
    |   /mnt/c/Users/ssis/Desktop/bdaa/data/raw/pems_bay/PEMS-BAY.csv HadoopRDD[0] at textFile at TrafficRddDemo.scala:95 []
```

After `checkpoint()` (reliable, written to `/home/wsluser/.cache/trafficsense/scala-rdd/checkpoints`), the first action computed and saved 58,825 records in 2.69 s. isCheckpointed = **true**. The lineage is now cut at the checkpoint, so recovery after a failure re-reads the checkpoint instead of re-parsing the CSV:

```
(12) ShuffledRDD[115] at reduceByKey at TrafficRddDemo.scala:306 [Disk Memory Serialized 1x Replicated]
 |        CachedPartitions: 12; MemorySize: 1206.4 KiB; DiskSize: 0.0 B
 |   ReliableCheckpointRDD[116] at count at TrafficRddDemo.scala:310 [Disk Memory Serialized 1x Replicated]
```

## 8. Output

Per-sensor statistics and traffic-state profiles were written with `sortByKey → coalesce(1) → saveAsTextFile` to `/mnt/c/Users/ssis/Desktop/bdaa/reports/scala_rdd/sensor_profiles_csv/part-00000`. Columns: `sensor,valid,mean,stdev,min,max,free,slowed,congested,missing`.

Total runtime: 75.9 s.
