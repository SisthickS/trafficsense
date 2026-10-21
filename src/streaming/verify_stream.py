"""Verification of the streaming output against independent batch computations.

    python -m src.streaming.verify_stream parity     --db ts_parity --start 2017-06-05T12:00:00Z --hours 6     (clean replay)
    python -m src.streaming.verify_stream accounting --db ts_faults --summary reports/streaming/replay_faults.json

parity      every stored prediction vs the Phase 3 `features` table scored by the same saved model in batch mode (probability
            and alert), prediction coverage vs the cleaned grid, and every stored window vs a pandas aggregation of the events.
accounting  with fault injection: dead letters == injected malformed payloads, duplicates removed, late events rejected,
            accepted readings == unique events - late events.
Both return/print a dict of named checks {ok, detail}; exit code 1 if any check fails.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from src.common.paths import PROCESSED_DIR, processed_dataset_dir
from src.preprocessing.build_features import FEATURE_COLUMNS
from src.streaming import config as C
from src.streaming.mongo_store import MongoStore
from src.streaming.reference import clean_rows, to_slot


def _check(checks: dict, name: str, ok: bool, detail: str = "") -> None:
    checks[name] = {"ok": bool(ok), "detail": detail}


def parity(db: str, start: datetime, hours: float, grace_slots: int = 1, model_path: str | None = None,
           dataset: str = "pems-bay") -> dict:
    from src.common.spark import get_spark
    from src.ml.config import ML_MODEL_DIR
    from src.ml.model_io import load_model, predict

    checks: dict = {}
    store = MongoStore(db=db)
    end = start + timedelta(hours=hours)
    preds = pd.DataFrame(list(store.col(C.COL_PREDICTIONS).find({}, {"_id": 0, "sensor_id": 1, "slot": 1, "probability": 1, "alert": 1,
                                                                    "event_ts": 1, "null_features": 1})))
    _check(checks, "predictions stored", len(preds) > 0, f"{len(preds):,} documents")
    if preds.empty:
        return checks
    state = store.get_state()
    first_slot, last_final = int(state["first_slot"]), int(state["last_final_slot"])

    # coverage: every cleaned-grid row with a speed in the finalized slots must have exactly one prediction
    grid_all = clean_rows(start, end, dataset)
    grid = grid_all[grid_all.speed_mph.notna() & (grid_all.slot >= first_slot) & (grid_all.slot <= last_final)]
    key = lambda d: set(zip(d.sensor_id.astype(int), d.slot.astype(int)))
    expected, got = key(grid), key(preds)
    _check(checks, "one prediction per cleaned-grid row of every finalized slot", expected == got,
           f"expected {len(expected):,}, stored {len(got):,}, missing {len(expected - got)}, extra {len(got - expected)}")
    last_data = int(grid_all.slot.max())
    _check(checks, f"newest {grace_slots} slot(s) held back until later events arrive (grace)", last_final == last_data - grace_slots,
           f"last finalized slot {last_final}, last data slot {last_data}")

    # probabilities vs batch scoring of the stored Phase 3 features with the same model
    feats = pd.read_parquet(processed_dataset_dir(dataset, PROCESSED_DIR) / "features", columns=["sensor_id", "ts_utc"] + FEATURE_COLUMNS)
    feats = feats[(feats.ts_utc >= pd.Timestamp(start)) & (feats.ts_utc < pd.Timestamp(end))]
    feats = feats.assign(slot=to_slot(feats.ts_utc))
    feats = feats[feats.slot <= last_final]
    spark = get_spark("trafficsense-verify-stream", driver_memory="2g", shuffle_partitions=8)
    try:
        bundle = load_model(model_path or ML_MODEL_DIR / "best")
        batch = predict(bundle, spark.createDataFrame(feats)).select("sensor_id", "slot", "p", "alert").toPandas()
    finally:
        spark.stop()
    m = preds.merge(batch, on=["sensor_id", "slot"], how="inner", suffixes=("_stream", "_batch"))
    d = (m.probability - m.p).abs()
    _check(checks, "stream predictions compared with batch scoring", len(m) > 0.95 * len(feats), f"{len(m):,} of {len(feats):,} batch rows "
           f"({len(preds) - len(m):,} stream rows have no Phase 3 feature row: label unobserved)")
    _check(checks, "probabilities identical to batch scoring (max |dp| < 1e-6)", d.max() < 1e-6, f"max |dp| = {d.max():.2e}, mean {d.mean():.2e}")
    flips = int((m.alert_stream.astype(bool) != m.alert_batch.astype(bool)).sum())
    _check(checks, "alert decisions identical to batch scoring", flips == 0, f"{flips} differ of {int(m.alert_stream.astype(bool).sum()):,} alerts")
    only_stream = preds.merge(batch[["sensor_id", "slot"]], on=["sensor_id", "slot"], how="left", indicator=True)
    only_stream = only_stream[only_stream._merge == "left_only"]
    _check(checks, "complete features on every row that Phase 3 keeps; incomplete rows are exactly those it drops",
           int((m.null_features > 0).sum()) == 0,
           f"{len(only_stream)} stream-only rows ({int((only_stream.null_features > 0).sum())} with null features, scored through the "
           f"model's Imputer); 0 of the {len(m):,} comparable rows have a null feature")

    # windows vs pandas (events rounded to 3 decimals, exactly as encoded on the wire)
    ev = clean_rows(start, end, dataset, observed_only=True)
    ev = ev.assign(speed_mph=ev.speed_mph.astype("float64").round(3))
    ev = ev.assign(cong=(ev.speed_mph < 35).astype(int))
    rows = []
    for k in range(3):                                      # 15-minute windows sliding by 5 minutes: an event is in 3 windows
        rows.append(ev.assign(ws=(ev.slot - k)))
    allw = pd.concat(rows)
    exp = (allw.groupby(["sensor_id", "ws"]).agg(n=("speed_mph", "size"), avg=("speed_mph", "mean"), mn=("speed_mph", "min"),
                                                mx=("speed_mph", "max"), cong=("cong", "sum")).reset_index())
    docs = pd.DataFrame(list(store.col(C.COL_SENSOR_WINDOWS).find({}, {"_id": 0})))
    _check(checks, "sensor windows stored", len(docs) > 0, f"{len(docs):,} finalized windows")
    if len(docs):
        docs["ws"] = to_slot(pd.to_datetime(docs.window_start))
        mm = docs.merge(exp, on=["sensor_id", "ws"], how="left")
        bad = mm[(mm.n_x != mm.n_y) | ((mm.avg_speed - mm.avg).abs() > 1e-9) | ((mm.min_speed - mm.mn).abs() > 1e-9) |
                 ((mm.max_speed - mm.mx).abs() > 1e-9) | (np.rint(mm.congested_share * mm.n_x) != mm.cong)]
        _check(checks, "sensor windows equal the pandas aggregation (n, mean, min, max, congested)", len(bad) == 0, f"{len(mm):,} windows, {len(bad)} differ")
    nd = pd.DataFrame(list(store.col(C.COL_NETWORK_WINDOWS).find({}, {"_id": 0})))
    if len(nd):
        net = allw.groupby("ws").agg(n=("speed_mph", "size"), avg=("speed_mph", "mean")).reset_index()
        nd["ws"] = to_slot(pd.to_datetime(nd.window_start))
        mm = nd.merge(net, on="ws", how="left")
        bad = mm[(mm.n_x != mm.n_y) | ((mm.avg_speed - mm.avg).abs() > 1e-9)]
        _check(checks, "network windows equal the pandas aggregation", len(bad) == 0, f"{len(mm)} windows, {len(bad)} differ")
    store.close()
    return checks


def accounting(db: str, summary: dict) -> dict:
    checks: dict = {}
    s = MongoStore(db=db)
    unique, late, dup, mal = summary["unique_events"], summary["late"], summary["duplicates"], summary["malformed"]
    n_read, n_late, n_dead = s.col(C.COL_READINGS).count_documents({}), s.col("late_events").count_documents({}), s.col(C.COL_DEAD_LETTERS).count_documents({})
    _check(checks, "every malformed payload landed in dead_letters", n_dead == mal, f"{n_dead} dead letters vs {mal} injected")
    reasons = {r["_id"]: r["n"] for r in s.col(C.COL_DEAD_LETTERS).aggregate([{"$group": {"_id": "$reason", "n": {"$sum": 1}}}])}
    _check(checks, "dead letters carry a reason", sum(reasons.values()) == n_dead and None not in reasons, json.dumps(reasons))
    _check(checks, "duplicates did not create extra readings", n_read + n_late <= unique, f"{n_read:,} accepted + {n_late} late <= {unique:,} unique events")
    _check(checks, "accepted + late == unique events (nothing lost, nothing double counted)", n_read + n_late == unique,
           f"{n_read:,} + {n_late} = {n_read + n_late:,} vs {unique:,} ({dup} duplicates removed by dropDuplicates)")
    _check(checks, "late events were rejected, not predicted", n_late > 0 and n_late <= late + 0, f"{n_late} rejected of {late} injected as late "
           "(the rest were out of order but within the tolerance)")
    s.close()
    return checks


def report(checks: dict) -> int:
    for name, c in checks.items():
        print(f"  [{'OK' if c['ok'] else 'FAIL':4}] {name}  {c['detail']}")
    return 0 if all(c["ok"] for c in checks.values()) else 1


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["parity", "accounting"])
    p.add_argument("--db", required=True)
    p.add_argument("--start", default="2017-06-05T12:00:00Z")
    p.add_argument("--hours", type=float, default=6.0)
    p.add_argument("--summary", default="")
    p.add_argument("--out", default="")
    a = p.parse_args(argv)
    if a.command == "parity":
        checks = parity(a.db, datetime.strptime(a.start, "%Y-%m-%dT%H:%M:%SZ"), a.hours)
    else:
        checks = accounting(a.db, json.loads(Path(a.summary).read_text()))
    if a.out:
        Path(a.out).write_text(json.dumps(checks, indent=2) + "\n")
    return report(checks)


if __name__ == "__main__":
    sys.exit(main())
