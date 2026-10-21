"""Phase 8 tests: event contract, live feature engine (gap fill, lateness, determinism, recovery), producer, metrics, MongoDB.

Run:  pytest -q tests/test_streaming.py          (Kafka is not needed; MongoDB tests skip when it is unreachable)
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from src.common.paths import PROCESSED_DIR, processed_dataset_dir
from src.preprocessing.build_features import FEATURE_COLUMNS, neighbor_stats
from src.streaming import config as C
from src.streaming import recovery_test, replay_events, streaming_metrics
from src.streaming.events import encode_event, parse_events, rejected, slot_of, slot_start, valid
from src.streaming.live_features import LiveFeatureEngine

IDS = [10, 11, 12, 13]
FIRST = 4_988_880                      # a Monday, 12:00 UTC


def make_engine(grace=1, partitions=1) -> LiveFeatureEngine:
    A = np.array([[0, .5, .2, 0], [.5, 0, .5, .2], [.2, .5, 0, .5], [0, .2, .5, 0]], dtype=float)
    static = pd.DataFrame({"sensor_id": IDS, "latitude": 37.0, "longitude": -122.0, "in_degree": 2, "out_degree": 2,
                           "ff_speed": 65.0, "sensor_congested_rate": 0.05})
    profile = pd.DataFrame({"sensor_id": IDS, "day_of_week": 1, "slot_of_day": 61, "hist_speed": 55.0, "hist_congested_rate": 0.1})
    e = LiveFeatureEngine(IDS, A, static, profile, grace_slots=grace, n_partitions=partitions)
    hist = pd.DataFrame([(s, t, 60.0, "observed") for t in range(FIRST - 40, FIRST) for s in IDS], columns=["sensor_id", "slot", "speed_mph", "quality"])
    e.load_history(hist, FIRST)
    return e


def events(rows) -> pd.DataFrame:
    """rows: (sensor, slot_offset, speed, partition, offset)"""
    return pd.DataFrame([{"sensor_id": s, "slot": FIRST + k, "speed_mph": v, "produced_at_ms": 1, "partition": p, "offset": o}
                         for s, k, v, p, o in rows])


# ------------------------------------------------------------------------------------------------ event contract
@pytest.fixture(scope="module")
def spark():
    from src.common.spark import get_spark
    s = get_spark("trafficsense-tests-streaming", driver_memory="1g", shuffle_partitions=4)
    yield s
    s.stop()


def test_encode_event_and_slots():
    ts = datetime(2017, 6, 5, 12, 5)
    d = json.loads(encode_event(400001, ts, 67.93456, 123456))
    assert d == {"schema_version": 1, "source": "pems-bay", "sensor_id": 400001, "event_ts": "2017-06-05T12:05:00Z",
                 "speed_mph": 67.935, "produced_at_ms": 123456}
    assert slot_of(ts) == FIRST + 1 and slot_start(FIRST) == datetime(2017, 6, 5, 12, 0)


def test_parse_events_explicit_schema_and_rejection_reasons(spark):
    ok = encode_event(400001, datetime(2017, 6, 5, 12, 0), 60.0, 1).decode()
    base = {"schema_version": 1, "sensor_id": 1, "event_ts": "2017-06-05T12:00:00Z", "speed_mph": 50.0, "produced_at_ms": 1}
    cases = {"valid": ok, "malformed_json": "{oops", "unsupported_schema_version": json.dumps({**base, "schema_version": 2}),
             "missing_sensor_id": json.dumps({k: v for k, v in base.items() if k != "sensor_id"}),
             "bad_event_ts": json.dumps({**base, "event_ts": "not a date"}),
             "event_ts_out_of_range": json.dumps({**base, "event_ts": "2099-01-01T00:00:00Z"}),
             "missing_speed": json.dumps({**base, "speed_mph": None}), "speed_out_of_range": json.dumps({**base, "speed_mph": 500.0})}
    raw = spark.createDataFrame([(k.encode(), v.encode(), i % 3, i, datetime(2026, 1, 1)) for i, (k, v) in enumerate(cases.items())],
                                "key binary, value binary, partition int, offset long, timestamp timestamp")
    parsed_df = parse_events(raw, ("2016-12-31 00:00:00", "2017-07-02 00:00:00"))
    parsed = parsed_df.toPandas()
    reasons = sorted(r for r in parsed.reject_reason if r)
    assert reasons == sorted(k for k in cases if k != "valid"), reasons
    assert valid(parsed_df).count() == 1 and rejected(parsed_df).count() == len(cases) - 1
    v = parsed[parsed.reject_reason.isna()].iloc[0]
    assert v.sensor_id == 400001 and v.speed_mph == 60.0 and pd.Timestamp(v.event_ts) == pd.Timestamp("2017-06-05 12:00:00")


# ------------------------------------------------------------------------------------------------ live feature engine
def test_neighbor_kernel_matches_naive_loop():
    rng = np.random.default_rng(1)
    A = rng.random((6, 6)) * (rng.random((6, 6)) > 0.5)
    np.fill_diagonal(A, 0)
    S = rng.uniform(10, 70, (5, 6))
    S[rng.random(S.shape) < 0.2] = np.nan
    up, dn, upc, dnc = neighbor_stats(S, A, 35.0)
    for r in range(5):
        for j in range(6):
            w, m = A[:, j], ~np.isnan(S[r])
            den = (w * m).sum()
            exp_up = (w[m] * S[r][m]).sum() / den if den > 0 else np.nan
            exp_c = (w[m] * (S[r][m] < 35)).sum() / den if den > 0 else np.nan
            assert (np.isnan(up[r, j]) and np.isnan(exp_up)) or up[r, j] == pytest.approx(exp_up)
            assert (np.isnan(upc[r, j]) and np.isnan(exp_c)) or upc[r, j] == pytest.approx(exp_c)


def test_forward_fill_is_causal_and_bounded_to_three_slots():
    e = make_engine()
    rows = [(s, k, 55.0, 0, k * 10 + s) for k in range(8) for s in IDS if not (s == 10 and 1 <= k <= 6)]   # sensor 10 silent in slots 1..6
    e.ingest(events(rows))
    out = dict(e.finalize(FIRST + 7))
    cur = lambda k: out[FIRST + k].set_index("sensor_id")
    assert cur(0).loc[10, "is_imputed"] == 0
    for k in (1, 2, 3):                                           # forward-filled: speed carried over, flagged imputed
        assert cur(k).loc[10, "speed"] == pytest.approx(55.0) and cur(k).loc[10, "is_imputed"] == 1
    assert 10 not in cur(4).index and 10 not in cur(6).index      # > 15 minutes: no cleaned speed -> no prediction row
    assert cur(7).loc[10, "is_imputed"] == 0 and cur(7).loc[10, "imputed_count_60m"] >= 4


def test_lateness_is_event_time_per_partition():
    e = make_engine(grace=1, partitions=1)
    acc, late, st = e.ingest(events([(10, 0, 50, 0, 0), (11, 2, 50, 0, 1), (12, 1, 50, 0, 2), (13, 2, 50, 0, 3)]))
    # slot 1 arrives after slot 2 was seen in the partition -> late (<= max - grace); slot 2 is on time
    assert sorted(late.sensor_id) == [12] and sorted(acc.sensor_id) == [10, 11, 13] and st["late"] == 1
    assert e.ready_through() == FIRST + 1


def test_slot_completeness_needs_every_partition():
    e = make_engine(grace=1, partitions=2)
    e.ingest(events([(10, 0, 50, 0, 0), (10, 3, 50, 0, 1)]))
    assert e.ready_through() is None                                  # partition 1 has not been heard from
    e.ingest(events([(11, 1, 50, 1, 0)]))
    assert e.ready_through() == FIRST + 0                             # min(max slots) = 1, minus grace


def test_results_do_not_depend_on_micro_batch_boundaries():
    rng = np.random.default_rng(7)
    rows, off = [], {0: 0, 1: 0}
    for k in range(25):
        for s in IDS:
            if rng.random() < 0.9:
                p = s % 2
                rows.append((s, k, float(rng.uniform(15, 70)), p, off[p]))
                off[p] += 1
                if rng.random() < 0.05:                                   # delayed duplicate-slot delivery 3 slots later = late
                    rows.append((s, k - 3 if k > 3 else k, 40.0, p, off[p]))
                    off[p] += 1
    ev = events(rows)

    def run(chunk):
        e, feats, late = make_engine(1, 2), [], 0
        for i in range(0, len(ev), chunk):
            _, l, _ = e.ingest(ev.iloc[i:i + chunk])
            late += len(l)
            t = e.ready_through()
            if t is not None:
                feats += [f for _, f in e.finalize(t)]
        return pd.concat(feats).sort_values(["slot", "sensor_id"]).reset_index(drop=True), late

    (a, la), (b, lb) = run(5), run(1000)
    assert la == lb and la > 0
    pd.testing.assert_frame_equal(a[FEATURE_COLUMNS], b[FEATURE_COLUMNS])


def test_restore_rebuilds_identical_state():
    live = make_engine()
    rows = [(s, k, 40.0 + k + s % 3, 0, k * 10 + s) for k in range(12) for s in IDS if (s, k) != (11, 4)]
    live.ingest(events(rows))
    live.finalize(FIRST + 9)
    persisted = [{"sensor_id": s, "slot": FIRST + k, "speed_mph": v, "produced_at_ms": 1} for s, k, v, *_ in rows]
    restored = LiveFeatureEngine(IDS, live.A, pd.DataFrame({"sensor_id": IDS, "latitude": 37.0, "longitude": -122.0, "in_degree": 2, "out_degree": 2,
                                                            "ff_speed": 65.0, "sensor_congested_rate": 0.05}),
                                 pd.DataFrame({"sensor_id": IDS, "day_of_week": 1, "slot_of_day": 61, "hist_speed": 55.0, "hist_congested_rate": 0.1}))
    hist = pd.DataFrame([(s, t, 60.0, "observed") for t in range(FIRST - 40, FIRST) for s in IDS], columns=["sensor_id", "slot", "speed_mph", "quality"])
    restored.restore(hist, FIRST, persisted, FIRST + 9, live.partition_max)
    assert restored.final_slot == live.final_slot and set(restored.pending) == set(live.pending)
    a = live.finalize(FIRST + 11)
    b = restored.finalize(FIRST + 11)
    for (ta, fa), (tb, fb) in zip(a, b):
        assert ta == tb
        pd.testing.assert_frame_equal(fa[FEATURE_COLUMNS], fb[FEATURE_COLUMNS])


def test_settled_slot_for_realtime_mode():
    e = make_engine()
    e.ingest(events([(s, 0, 50, 0, s) for s in IDS[:3]]))             # 3 of 4 sensors = 75% coverage
    assert e.settled_through(0.0, 0.9) is None                        # not enough sensors
    assert e.settled_through(0.0, 0.7) == FIRST
    assert e.settled_through(60.0, 0.7) is None                       # not quiet long enough


@pytest.mark.skipif(not (processed_dataset_dir("pems-bay", PROCESSED_DIR) / "features").exists(), reason="Phase 3 outputs not available")
def test_live_features_equal_phase3_features_on_real_data():
    from src.streaming.reference import build_engine, clean_rows, history_before
    start, end = datetime(2017, 6, 5, 12), datetime(2017, 6, 5, 14)
    ev = clean_rows(start, end, observed_only=True)
    eng = build_engine(grace_slots=1, n_partitions=1)
    first = int(ev.slot.min())
    eng.load_history(history_before(first), first)
    eng.ingest(ev.assign(partition=0, offset=range(len(ev)), produced_at_ms=0))
    live = pd.concat([f for _, f in eng.finalize(int(ev.slot.max()))], ignore_index=True)
    ref = pd.read_parquet(processed_dataset_dir("pems-bay", PROCESSED_DIR) / "features", columns=["sensor_id", "ts_utc"] + FEATURE_COLUMNS)
    ref = ref[(ref.ts_utc >= pd.Timestamp(start)) & (ref.ts_utc < pd.Timestamp(end))]
    m = live.merge(ref, on=["sensor_id", "ts_utc"], suffixes=("", "_ref"))
    assert len(m) > 0.95 * len(ref)
    for c in FEATURE_COLUMNS:
        a, b = m[c].astype(float), m[c + "_ref"].astype(float)
        assert (a.isna() == b.isna()).all(), c
        assert np.nanmax(np.abs(a - b)) < 1e-4, c


# ------------------------------------------------------------------------------------------------ producer
def test_producer_stream_is_chronological_and_fault_injection_is_deterministic():
    start = pd.Timestamp("2017-06-05 12:00")
    ev = pd.DataFrame([(s, start + pd.Timedelta(minutes=5 * k), FIRST + k, 50.0) for k in range(30) for s in IDS],
                      columns=["sensor_id", "ts_utc", "slot", "speed_mph"])
    args = SimpleNamespace(seed=3, late_fraction=0.1, late_slots=4, duplicate_fraction=0.1, malformed_fraction=0.1)
    a, b = replay_events.build_stream(ev, args), replay_events.build_stream(ev, args)
    assert [(x[0], x[1], x[3]) for x in a] == [(x[0], x[1], x[3]) for x in b]
    kinds = [x[3] for x in a]
    assert kinds.count(0) + kinds.count(2) == len(ev) and kinds.count(1) > 0 and kinds.count(3) > 0
    normal_slots = [x[0] for x in a if x[3] == 0]
    assert normal_slots == sorted(normal_slots)
    clean = replay_events.build_stream(ev, SimpleNamespace(seed=3, late_fraction=0, late_slots=4, duplicate_fraction=0, malformed_fraction=0))
    assert len(clean) == len(ev) and [x[0] for x in clean] == sorted(x[0] for x in clean)


# ------------------------------------------------------------------------------------------------ metrics & recovery helpers
def test_progress_summary_and_offsets(tmp_path):
    def prog(batch, rows, ms, end, latest, ts):
        return {"run": "r", "name": "predictions", "batchId": batch, "runId": "a", "timestamp": ts, "numInputRows": rows,
                "inputRowsPerSecond": 10.0, "processedRowsPerSecond": 100.0, "durationMs": {"triggerExecution": ms, "addBatch": ms - 10},
                "sources": [{"endOffset": {"t": {"0": end, "1": end}}, "latestOffset": {"t": {"0": latest, "1": latest}}}],
                "stateOperators": [{"numRowsTotal": 5, "memoryUsedBytes": 2048, "numRowsDroppedByWatermark": 1}],
                "eventTime": {"watermark": "2017-06-05T12:00:00.000Z"}}
    f = tmp_path / "progress_r.jsonl"
    f.write_text("\n".join(json.dumps(p) for p in [prog(0, 100, 1000, 100, 400, "2026-01-01T00:00:00.000Z"),
                                                    prog(1, 300, 2000, 400, 400, "2026-01-01T00:00:10.000Z")]))
    s = streaming_metrics.summarize(streaming_metrics.load_progress([f]))["predictions"]
    assert s["input_rows"] == 400 and s["batches_with_data"] == 2 and s["max_backlog_events"] == 600 and s["final_backlog_events"] == 0
    assert s["max_state_rows"] == 5 and s["rows_dropped_by_watermark"] == 2 and s["batch_ms_max"] == 2000
    assert streaming_metrics._offset_total({"t": {"0": 3, "1": 4}}) == 7 and streaming_metrics._offset_total(None) is None


def test_recovery_compare_detects_differences():
    def side(prob=0.5, extra=None):
        p = pd.DataFrame({"_id": ["a|1", "b|1"], "probability": [prob, 0.1], "alert": [True, False]})
        if extra:
            p = pd.concat([p, pd.DataFrame({"_id": [extra], "probability": [0.2], "alert": [False]})])
        w = pd.DataFrame({"_id": ["w1"], "n": [3], "avg_speed": [50.0], "min_speed": [40.0], "congested_share": [0.0]})
        return {"predictions": p, "readings": {"r1"}, "late": set(), "dead": {"d1"}, "sensor_windows": w, "network_windows": w,
                "state": {"last_final_slot": 7}}
    assert all(c["ok"] for c in recovery_test.compare(side(), side()).values())
    bad = recovery_test.compare(side(), side(prob=0.6, extra="c|1"))
    assert not bad["probabilities identical"]["ok"] and not bad["same prediction ids (nothing lost, nothing extra)"]["ok"]


# ------------------------------------------------------------------------------------------------ MongoDB
@pytest.fixture()
def store():
    from src.streaming.mongo_store import MongoStore
    try:
        s = MongoStore(db="ts_unit_tests")
    except Exception:
        pytest.skip("MongoDB not reachable (docker compose up -d)")
    s.drop_all()
    yield s
    s.drop_all()
    s.close()


def test_upserts_are_idempotent_and_state_roundtrips(store):
    docs = [{"_id": "a|1", "p": 0.1}, {"_id": "b|1", "p": 0.2}]
    store.upsert(C.COL_PREDICTIONS, docs)
    store.upsert(C.COL_PREDICTIONS, docs + [{"_id": "a|1", "p": 0.9}])      # rewrite after a crash: same ids, last write wins
    assert store.col(C.COL_PREDICTIONS).count_documents({}) == 2
    assert store.col(C.COL_PREDICTIONS).find_one({"_id": "a|1"})["p"] == 0.9
    assert store.get_state() is None
    store.set_state(first_slot=1, last_final_slot=5, last_batch_id=2, partition_max={"0": 6})
    assert store.get_state()["last_final_slot"] == 5
    store.upsert(C.COL_READINGS, [{"_id": f"{s}|{k}", "sensor_id": s, "slot": k, "speed_mph": 50.0, "produced_at_ms": 1} for s in (1, 2) for k in (3, 4)])
    assert [d["slot"] for d in store.readings_from(4)] == [4, 4]
    assert streaming_metrics.health(store, 60)["healthy"] is True
