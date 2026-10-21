"""Query-progress monitoring for the streaming application.

  * ProgressListener   - a StreamingQueryListener that appends every StreamingQueryProgress (all queries) to a JSONL file
                         and to MongoDB (`stream_metrics`), so a crashed or restarted run keeps its history.
  * summarize()        - throughput, batch duration percentiles, trigger-phase breakdown, backlog (Kafka lag), state size and
                         watermark progress per query, from the JSONL files.
  * end_to_end_latency - producer send time -> prediction written (from the `predictions` collection).
  * CLI                - `report` writes reports/streaming/stream_metrics.{md,json,png}; `health` exits non-zero when the
                         application has not committed a batch recently (cron / container health check).

    python -m src.streaming.streaming_metrics report --db trafficsense --run demo
    python -m src.streaming.streaming_metrics health --db trafficsense --max-age 60
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql.streaming import StreamingQueryListener

from src.analytics.common import df_to_md
from src.streaming import config as C

log = logging.getLogger("trafficsense.metrics")


# ------------------------------------------------------------------------------------------------ listener
class ProgressListener(StreamingQueryListener):
    def __init__(self, store, path: Path, run: str):
        super().__init__()
        self.store, self.path, self.run = store, Path(path), run
        self.lock = threading.Lock()

    def onQueryStarted(self, event):
        log.info("query started: %s (id %s, run id %s)", event.name, event.id, event.runId)

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        p["run"] = self.run
        line = json.dumps(p)
        with self.lock:
            with self.path.open("a") as f:
                f.write(line + "\n")
        try:
            self.store.upsert(C.COL_METRICS, [{"_id": f"{self.run}|{p['name']}|{p['runId']}|{p['batchId']}", "query": p["name"], "run": self.run,
                                               "batch_id": p["batchId"], "timestamp": pd.Timestamp(p["timestamp"]).to_pydatetime().replace(tzinfo=None),
                                               "num_input_rows": p.get("numInputRows", 0), "input_rows_per_second": p.get("inputRowsPerSecond"),
                                               "processed_rows_per_second": p.get("processedRowsPerSecond"),
                                               "trigger_ms": p.get("durationMs", {}).get("triggerExecution"), "progress": p}])
        except Exception as exc:                                  # monitoring must never break the query
            log.warning("could not store progress in MongoDB: %s", exc)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        if event.exception:
            log.error("query %s terminated with an exception: %s", event.id, event.exception)
        else:
            log.info("query %s terminated", event.id)


def attach_listener(spark, store, metrics_dir: str | Path, run: str) -> ProgressListener:
    d = Path(metrics_dir)
    d.mkdir(parents=True, exist_ok=True)
    listener = ProgressListener(store, d / f"progress_{run}.jsonl", run)
    spark.streams.addListener(listener)
    return listener


# ------------------------------------------------------------------------------------------------ analysis
def _offset_total(offsets) -> int | None:
    if not offsets:
        return None
    d = json.loads(offsets) if isinstance(offsets, str) else offsets
    return int(sum(v for topic in d.values() for v in topic.values()))


def load_progress(paths: list[Path]) -> pd.DataFrame:
    rows = []
    for path in paths:
        for line in Path(path).read_text().splitlines():
            if not line.strip():
                continue
            p = json.loads(line)
            src = (p.get("sources") or [{}])[0]
            st = (p.get("stateOperators") or [{}])
            d = p.get("durationMs", {})
            rows.append({"run": p.get("run"), "query": p["name"], "batch_id": p["batchId"], "run_id": p["runId"],
                         "timestamp": pd.Timestamp(p["timestamp"]), "rows": p.get("numInputRows", 0),
                         "input_rps": p.get("inputRowsPerSecond"), "processed_rps": p.get("processedRowsPerSecond"),
                         "trigger_ms": d.get("triggerExecution", 0), "add_batch_ms": d.get("addBatch", 0),
                         "latest_offset_ms": d.get("latestOffset", 0), "wal_ms": d.get("walCommit", 0),
                         "planning_ms": d.get("queryPlanning", 0), "commit_ms": d.get("commitOffsets", 0),
                         "end_offset_total": _offset_total(src.get("endOffset")), "latest_offset_total": _offset_total(src.get("latestOffset")),
                         "state_rows": sum(s.get("numRowsTotal", 0) for s in st), "state_bytes": sum(s.get("memoryUsedBytes", 0) for s in st),
                         "dropped_by_watermark": sum(s.get("numRowsDroppedByWatermark", 0) for s in st),
                         "watermark": (p.get("eventTime") or {}).get("watermark"), "event_max": (p.get("eventTime") or {}).get("max")})
    return pd.DataFrame(rows).sort_values(["query", "timestamp"]).reset_index(drop=True) if rows else pd.DataFrame()


def summarize(df: pd.DataFrame) -> dict:
    out = {}
    for q, g in df.groupby("query"):
        busy = g[g.rows > 0]
        wall = max((g.timestamp.max() - g.timestamp.min()).total_seconds(), 1e-9)
        lag = (g.latest_offset_total - g.end_offset_total).dropna()
        out[q] = {
            "progress_events": int(len(g)), "batches_with_data": int(len(busy)), "input_rows": int(g.rows.sum()),
            "runs_seen": int(g.run_id.nunique()), "first_batch": int(g.batch_id.min()), "last_batch": int(g.batch_id.max()),
            "rows_per_s_overall": float(g.rows.sum() / max(g.trigger_ms.sum() / 1000, 1e-9)),
            "rows_per_s_wall": float(g.rows.sum() / wall),
            "processed_rows_per_s_p50": float(busy.processed_rps.median()) if len(busy) else None,
            "batch_ms_p50": float(busy.trigger_ms.median()) if len(busy) else None,
            "batch_ms_p95": float(busy.trigger_ms.quantile(0.95)) if len(busy) else None,
            "batch_ms_max": float(busy.trigger_ms.max()) if len(busy) else None,
            "mean_add_batch_ms": float(busy.add_batch_ms.mean()) if len(busy) else None,
            "mean_wal_commit_ms": float(busy.wal_ms.mean()) if len(busy) else None,
            "mean_planning_ms": float(busy.planning_ms.mean()) if len(busy) else None,
            "max_backlog_events": int(lag.max()) if len(lag) else None, "final_backlog_events": int(lag.iloc[-1]) if len(lag) else None,
            "max_state_rows": int(g.state_rows.max()), "max_state_kib": float(g.state_bytes.max() / 1024),
            "rows_dropped_by_watermark": int(g.dropped_by_watermark.sum()),
            "final_watermark": str(g.watermark.dropna().iloc[-1]) if g.watermark.notna().any() else None,
        }
    return out


def end_to_end_latency(store) -> dict:
    docs = list(store.col(C.COL_PREDICTIONS).find({}, {"latency_ms": 1, "slot_latency_ms": 1, "finalized_by": 1, "slot": 1, "_id": 0}))
    out = {}
    first = min((d["slot"] for d in docs), default=0)

    def stats(a: np.ndarray) -> dict:
        return {"predictions": int(len(a)), "p50_s": float(np.percentile(a, 50)), "p95_s": float(np.percentile(a, 95)),
                "p99_s": float(np.percentile(a, 99)), "max_s": float(a.max()), "mean_s": float(a.mean()),
                "share_under_5_min_horizon": float((a < 300).mean())}

    for key, label in (("latency_ms", "since_sensor_event"), ("slot_latency_ms", "since_slot_complete")):
        a = np.asarray([d[key] for d in docs if d.get(key) is not None], dtype=float) / 1000
        if len(a):
            out[label] = stats(a)
    steady = np.asarray([d["slot_latency_ms"] for d in docs if d.get("slot_latency_ms") is not None and d["slot"] >= first + 4], dtype=float) / 1000
    if len(steady):
        out["since_slot_complete_after_warmup"] = stats(steady)          # excludes the first 4 slots (JVM / model warm-up, backlog)
    if docs:
        out["finalized_by"] = {k: int(v) for k, v in pd.Series([d.get("finalized_by") for d in docs]).value_counts().items()}
    return out


def mongo_counts(store) -> dict:
    return {name: store.col(name).estimated_document_count() if name != C.COL_DEAD_LETTERS else store.col(name).count_documents({})
            for name in (C.COL_READINGS, C.COL_PREDICTIONS, C.COL_SENSOR_WINDOWS, C.COL_NETWORK_WINDOWS, C.COL_DEAD_LETTERS, "late_events",
                         C.COL_BATCH_LOG)}


def health(store, max_age_s: float) -> dict:
    st = store.get_state()
    if st is None:
        return {"healthy": False, "reason": "no state document (application never committed a batch)"}
    age = (datetime.utcnow() - st["updated_at"]).total_seconds()
    return {"healthy": age <= max_age_s, "seconds_since_last_commit": round(age, 1), "last_batch_id": st.get("last_batch_id"),
            "last_final_slot": st.get("last_final_slot")}


# ------------------------------------------------------------------------------------------------ report
def write_report(store, run: str, metrics_dir: Path, out_dir: Path) -> dict:
    paths = [p for p in [Path(metrics_dir) / f"progress_{run}.jsonl"] if p.exists()]
    if not paths:
        raise SystemExit(f"no progress files for run {run!r} in {metrics_dir}")
    df = load_progress(paths)
    summary, e2e, counts = summarize(df), end_to_end_latency(store), mongo_counts(store)
    out_dir.mkdir(parents=True, exist_ok=True)

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(2, 2, figsize=(14, 8))
    for q, g in df[df.rows > 0].groupby("query"):
        t = (g.timestamp - df.timestamp.min()).dt.total_seconds()
        ax[0, 0].plot(t, g.processed_rps, marker=".", label=q)
        ax[0, 1].plot(t, g.trigger_ms / 1000, marker=".", label=q)
    ax[0, 0].set(title="Processing rate per micro-batch", xlabel="seconds since first batch", ylabel="events / s")
    ax[0, 1].set(title="Micro-batch duration", xlabel="seconds since first batch", ylabel="seconds")
    ax[0, 0].legend()
    pred = df[(df["query"] == "predictions")].dropna(subset=["latest_offset_total", "end_offset_total"])
    if len(pred):
        ax[1, 0].plot((pred.timestamp - df.timestamp.min()).dt.total_seconds(), pred.latest_offset_total - pred.end_offset_total)
    ax[1, 0].set(title="Kafka backlog of the predictions query (latest - processed offset)", xlabel="seconds since first batch", ylabel="events")
    docs = list(store.col(C.COL_PREDICTIONS).find({}, {"latency_ms": 1, "slot_latency_ms": 1, "_id": 0}))
    for key, label in (("latency_ms", "since sensor event"), ("slot_latency_ms", "since slot complete")):
        vals = np.sort([d[key] / 1000 for d in docs if d.get(key) is not None])
        if len(vals):
            ax[1, 1].step(vals, np.arange(1, len(vals) + 1) / len(vals), where="post", label=label)
    if docs:
        ax[1, 1].legend(loc="lower right")
    ax[1, 1].set(title="Latency: producer send -> prediction stored (CDF; horizon = 300 s)", xlabel="seconds", ylabel="share of predictions", ylim=(0, 1.02))
    fig.tight_layout()
    fig.savefig(out_dir / "stream_metrics.png", dpi=120)
    plt.close(fig)

    payload = {"run": run, "generated_at": datetime.utcnow().isoformat(timespec="seconds"), "queries": summary,
               "end_to_end_latency": e2e, "mongo_counts": counts}
    (out_dir / "stream_metrics.json").write_text(json.dumps(payload, indent=2) + "\n")
    t = pd.DataFrame(summary).T.rename_axis("query").reset_index()
    for c in t.columns[1:]:                                   # integer-valued metrics print as integers
        s = pd.to_numeric(t[c], errors="coerce")
        if s.notna().any() and (s.dropna() % 1 == 0).all():
            t[c] = [None if pd.isna(x) else int(x) for x in s]
    cols = ["query", "batches_with_data", "input_rows", "runs_seen", "rows_per_s_overall", "batch_ms_p50", "batch_ms_p95", "max_backlog_events",
            "final_backlog_events", "max_state_rows", "rows_dropped_by_watermark"]
    lat_rows = [{"measured from": k.replace("_", " "), **v} for k, v in e2e.items() if isinstance(v, dict) and "p50_s" in v]
    md = [f"# Streaming metrics - run `{run}`", "", "Source: StreamingQueryProgress events of every micro-batch "
          f"({len(df)} records in {len(paths)} file(s); `runs_seen` > 1 means the application was restarted).", "",
          "## Per query", "", df_to_md(t[cols]), "",
          "## End-to-end latency (producer send -> prediction stored in MongoDB)", "",
          "`since sensor event` includes the deliberate wait for the slot to complete (event-time grace); `since slot complete` is the latency "
          "from the LAST event of the slot to its predictions (in deterministic mode this still contains the wait for the next slot's first "
          "events, one slot period = 5 s at 60x, 300 s at real time); `after warmup` skips the first 4 slots (JVM/model warm-up, initial backlog).", "",
          df_to_md(pd.DataFrame(lat_rows)) if lat_rows else "_no predictions_", "",
          f"Slots finalized by: {e2e.get('finalized_by')}", "",
          "## MongoDB document counts", "", df_to_md(pd.DataFrame([counts])), "",
          "![metrics](stream_metrics.png)", ""]
    (out_dir / "stream_metrics.md").write_text("\n".join(md), encoding="utf-8")
    return payload


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["report", "health"])
    p.add_argument("--db", default=C.MONGO_DB)
    p.add_argument("--run", default="default")
    p.add_argument("--metrics-dir", default=str(C.METRICS_ROOT / "metrics"))
    p.add_argument("--out", default=str(C.METRICS_ROOT))
    p.add_argument("--max-age", type=float, default=60.0)
    a = p.parse_args(argv)
    from src.streaming.mongo_store import MongoStore
    store = MongoStore(db=a.db)
    if a.command == "health":
        h = health(store, a.max_age)
        print(json.dumps(h))
        return 0 if h["healthy"] else 1
    payload = write_report(store, a.run, Path(a.metrics_dir), Path(a.out))
    print(json.dumps({"queries": list(payload["queries"]), "e2e": payload["end_to_end_latency"]}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
