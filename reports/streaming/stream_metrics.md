# Streaming metrics - run `live`

Source: StreamingQueryProgress events of every micro-batch (80 records in 1 file(s); `runs_seen` > 1 means the application was restarted).

## Per query

| query | batches_with_data | input_rows | runs_seen | rows_per_s_overall | batch_ms_p50 | batch_ms_p95 | max_backlog_events | final_backlog_events | max_state_rows | rows_dropped_by_watermark |
|---|---|---|---|---|---|---|---|---|---|---|
| dead_letters | 12 | 3,900 | 1 | 238.532 | 1,217.500 | 1,793.850 | 0 | 0 | 0 | 0 |
| network_windows | 12 | 3,900 | 1 | 69.865 | 2,543.500 | 3,957.650 | 0 | 0 | 981 | 0 |
| predictions | 12 | 3,900 | 1 | 67.445 | 3,886.500 | 6,147.900 | 0 | 0 | 975 | 0 |
| sensor_windows | 12 | 3,900 | 1 | 69.121 | 2,505.500 | 3,801.050 | 0 | 0 | 2,925 | 0 |

## End-to-end latency (producer send -> prediction stored in MongoDB)

`since sensor event` includes the deliberate wait for the slot to complete (event-time grace); `since slot complete` is the latency from the LAST event of the slot to its predictions (in deterministic mode this still contains the wait for the next slot's first events, one slot period = 5 s at 60x, 300 s at real time); `after warmup` skips the first 4 slots (JVM/model warm-up, initial backlog).

| measured from | predictions | p50_s | p95_s | p99_s | max_s | mean_s | share_under_5_min_horizon |
|---|---|---|---|---|---|---|---|
| since sensor event | 3,575 | 9.935 | 12.958 | 12.961 | 12.963 | 10.271 | 1.000 |
| since slot complete | 3,575 | 9.932 | 12.941 | 12.941 | 12.941 | 10.265 | 1.000 |
| since slot complete after warmup | 2,275 | 9.177 | 10.286 | 10.286 | 10.286 | 9.251 | 1.000 |

Slots finalized by: {'grace': 3575}

## MongoDB document counts

| readings | predictions | sensor_windows | network_windows | dead_letters | late_events | batch_log |
|---|---|---|---|---|---|---|
| 3,900 | 3,575 | 2,925 | 9 | 0 | 0 | 12 |

![metrics](stream_metrics.png)
