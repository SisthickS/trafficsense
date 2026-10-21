# Restart / recovery test - PASSED

Replay: 6 h of PEMS-BAY from 2017-06-05T12:00:00Z (23,321 unique events; 23,802 messages incl. 246 duplicates, 232 late events, 235 malformed payloads), micro-batches of 2,000 events, 3 partitions, trigger availableNow. Total 6.4 min.

## Why recovery works

- **Spark checkpoint** (offsets + commits per query, state store of the windowed aggregations / de-duplication): after a crash the query re-plans the batch that was not committed from the checkpointed offsets; Kafka retains the events.
- **Idempotent sink**: every MongoDB document has a deterministic `_id` (sensor|slot, sensor|window start, partition|offset), so a re-run batch rewrites the same documents (effectively-once output on top of at-least-once delivery).
- **Driver-side feature state** (7-day speed buffer, pending slots) lives in memory only. On start it is rebuilt from the historical store plus the readings already persisted in MongoDB; the `stream_state` document records the last finalized slot and per-partition progress.
- **Deterministic semantics**: lateness and slot completeness depend on event time per partition, never on micro-batch boundaries, so a different batch layout after a restart yields identical results.

## Reference run

Uninterrupted run: 23,004 predictions in 12 batches (96.7 s).

## Scenario: kill -9

SIGKILL of the whole process group right after batch 3 was committed; 7,475 of 23,004 predictions were stored. Checkpoint at kill: offsets [0, 1, 2, 3], commits [0, 1, 2]. Restart took 72.2 s; final commits [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].

| check | result | evidence |
|---|---|---|
| same number of predictions | PASS | 23,004 vs reference 23,004 |
| same prediction ids (nothing lost, nothing extra) | PASS | missing 0, extra 0 |
| probabilities identical | PASS | max |dp| = 0.0e+00 |
| alerts identical | PASS | 2,974 alerts |
| no duplicate predictions | PASS | _id = sensor|slot; upserts are idempotent |
| same readings | PASS | 23,093 vs reference 23,093 |
| same late | PASS | 228 vs reference 228 |
| same dead | PASS | 235 vs reference 235 |
| same sensor_windows | PASS | 22,356 vs reference 22,356 |
| same network_windows | PASS | 69 vs reference 69 |
| same final state | PASS | last finalized slot 4988950 vs 4988950 |
| killed mid-stream (partial output before the kill) | PASS | 7,475 of 23,004 predictions |
| restart recovered driver state from MongoDB | PASS | RECOVERED state from MongoDB in 3.2s: last finalized slot 4988902 (2017-06-05 13:50:00), 7759 persisted readings, 2 pending slots, last committed batch 3 |
| restart finished cleanly | PASS | exit code 0 |

## Scenario: injected crash

Fault hook: the process exits (137) in batch 4 after 2,275 prediction documents were written and before the state/commit. Checkpoint at crash: offsets [0, 1, 2, 3, 4], commits [0, 1, 2, 3] (an offsets entry without a commit = the in-flight batch). Restart took 63.8 s.

| check | result | evidence |
|---|---|---|
| same number of predictions | PASS | 23,004 vs reference 23,004 |
| same prediction ids (nothing lost, nothing extra) | PASS | missing 0, extra 0 |
| probabilities identical | PASS | max |dp| = 0.0e+00 |
| alerts identical | PASS | 2,974 alerts |
| no duplicate predictions | PASS | _id = sensor|slot; upserts are idempotent |
| same readings | PASS | 23,093 vs reference 23,093 |
| same late | PASS | 228 vs reference 228 |
| same dead | PASS | 235 vs reference 235 |
| same sensor_windows | PASS | 22,356 vs reference 22,356 |
| same network_windows | PASS | 69 vs reference 69 |
| same final state | PASS | last finalized slot 4988950 vs 4988950 |
| crash happened after output write, before commit | PASS | exit 137, 2,275 documents of batch 4 already written, last committed batch 3 |
| interrupted batch recomputed idempotently | PASS | 2,275/2,275 documents rewritten by the restart with identical probabilities |
| restart recovered driver state from MongoDB | PASS | RECOVERED state from MongoDB in 1.0s: last finalized slot 4988902 (2017-06-05 13:50:00), 9704 persisted readings, 8 pending slots, last committed batch 3 |
| restart finished cleanly | PASS | exit code 0 |

## Reproduce

```bash
docker compose up -d --wait
python -m src.streaming.recovery_test
```

Logs of every run: `~/.cache/trafficsense/streaming/logs/`. The fault hook is `TRAFFICSENSE_CRASH_AT_BATCH` / `TRAFFICSENSE_CRASH_MARKER` (see `_maybe_crash` in `traffic_stream.py`); it is inert unless both variables are set.
