"""Load pipeline outputs into MongoDB (idempotent: safe to re-run; analyst edits to sensors/settings are never overwritten).

    python -m src.mongo.load_outputs                       # everything
    python -m src.mongo.load_outputs --only sensors hotspots
    python -m src.mongo.load_outputs --db trafficsense --uri mongodb://localhost:27017

  sensors    data/processed/<ds>/{sensor_stats,sensors}     -> sensor_metadata  (editable fields insert-only; `stats` refreshed)
  hotspots   reports/analytics/tables/q5_*, q9_*            -> congestion_hotspots
  analytics  reports/analytics/q1..q18.json + tables/*.csv  -> analysis_results (question + table documents)
  models     reports/ml/{evaluation,training}.json          -> model_evaluations
  streaming  reports/streaming/metrics/*.jsonl, *.json      -> stream_metrics (progress documents + run summaries)
  pipeline   run_status.json, training/evaluation/recovery  -> pipeline_runs
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from src.common.paths import PROCESSED_DIR, REPORTS_DIR, processed_dataset_dir
from src.mongo import schema as S
from src.mongo.connection import make_client
from src.mongo.mongo_writer import MongoWriter, point, utcnow
from src.streaming import config as C

log = logging.getLogger("trafficsense.load")
ANALYTICS_DIR = REPORTS_DIR / "analytics"
ML_DIR = REPORTS_DIR / "ml"
STREAM_DIR = REPORTS_DIR / "streaming"
ALGORITHMS = ["logistic_regression", "decision_tree", "random_forest"]
BASELINES = ["persistence", "historical_profile"]
STAGES = ["sensors", "hotspots", "analytics", "models", "streaming", "pipeline"]


def clean(v):
    """Make any structure BSON/JSON safe: numpy -> python, NaN/Inf -> None, Timestamps -> aware datetimes."""
    if isinstance(v, dict):
        return {str(k).replace(".", "_").lstrip("$"): clean(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [clean(x) for x in v]
    if isinstance(v, np.generic):
        v = v.item()
    if isinstance(v, float):
        return None if (math.isnan(v) or math.isinf(v)) else v
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime().astimezone(timezone.utc) if v.tzinfo else v.to_pydatetime().replace(tzinfo=timezone.utc)
    if v is pd.NaT:
        return None
    return v


def mtime(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def read_json(path: Path):
    return json.loads(path.read_text()) if path.exists() else None


# ------------------------------------------------------------------------------------------------ stages
def load_sensors(w: MongoWriter, dataset: str) -> dict:
    base = processed_dataset_dir(dataset, PROCESSED_DIR)
    stats = pd.read_parquet(base / "sensor_stats")
    extra = pd.read_parquet(base / "sensors")[["sensor_id", "n_readings", "missing_rate"]]
    df = stats.merge(extra, on="sensor_id", how="left")
    rows = [{"sensor_id": int(r.sensor_id), "latitude": float(r.latitude), "longitude": float(r.longitude),
             "stats": clean({"in_degree": r.in_degree, "out_degree": r.out_degree, "free_flow_speed_mph": r.ff_speed,
                             "congested_rate": r.sensor_congested_rate, "train_observed_readings": r.train_observed_readings,
                             "n_readings": r.n_readings, "missing_rate": r.missing_rate})} for r in df.itertuples()]
    return w.upsert_dataset_sensors(rows)


def load_hotspots(w: MongoWriter, dataset: str) -> dict:
    t = ANALYTICS_DIR / "tables"
    q5 = pd.read_csv(t / "q5_all_sensors.csv")
    q9 = pd.read_csv(t / "q9_all_sensors.csv").rename(columns={"rank": "csbi_rank"})
    m = q5.merge(q9.drop(columns=["latitude", "longitude"]), on="sensor_id", how="left", suffixes=("", "_q9"))
    now = utcnow()
    docs = []
    for r in m.to_dict("records"):
        sid = int(r["sensor_id"])
        docs.append(clean({
            "_id": sid, "sensor_id": sid, "rank": int(r["rank"]), "class": r["class"], "is_hotspot": r["class"] == "persistent hotspot",
            "persistence": r["persistence"], "persistence_30": r["persistence_30"], "persistence_120": r["persistence_120"],
            "mean_daily_congested_min": r["mean_daily_congested_min"], "max_daily_congested_min": r["max_daily_congested_min"],
            "workdays": r["workdays"], "csbi": r.get("csbi"), "csbi_rank": r.get("csbi_rank"), "tti_peak": r.get("tti_peak"),
            "frequency": r.get("frequency"), "intensity": r.get("intensity"), "congested_hours_per_workday": r.get("congested_hours_per_workday"),
            "latitude": r["latitude"], "longitude": r["longitude"], "location": point(r["latitude"], r["longitude"]),
            "sources": ["q5_persistent_hotspots", "q9_severity_burden"], "loaded_at": now}))
    return w.replace_documents(S.CONGESTION_HOTSPOTS, docs, stale_filter={})


def load_analytics(w: MongoWriter, dataset: str) -> dict:
    now, questions, tables = utcnow(), [], []
    for n in range(1, 19):
        p = ANALYTICS_DIR / f"q{n}.json"
        if not p.exists():
            log.warning("missing %s", p)
            continue
        d = json.loads(p.read_text())
        questions.append(clean({
            "_id": d["qid"], "doc_type": "question", "qid": d["qid"], "number": n, "title": d["title"], "question": d["question"],
            "findings": d["findings"], "implications": d["implications"], "limitations": d.get("limitations", []), "metrics": d.get("metrics", {}),
            "tables": sorted(d["tables"]), "figures": [Path(f).name for f in d["figures"]], "seconds": d.get("seconds"),
            "report": f"reports/analytics/{d['qid']}.md", "generated_at": mtime(p), "loaded_at": now}))
        for name, rel in d["tables"].items():
            df = pd.read_csv(ANALYTICS_DIR / rel)
            cols = [str(c).replace(".", "_").lstrip("$") for c in df.columns]
            df.columns = cols
            tables.append(clean({"_id": f"{d['qid']}:{name}", "doc_type": "table", "qid": d["qid"], "name": name, "columns": cols,
                                 "n_rows": int(len(df)), "rows": df.astype(object).where(df.notna(), None).to_dict("records"),
                                 "source": rel, "generated_at": mtime(ANALYTICS_DIR / rel), "loaded_at": now}))
    res = w.replace_documents(S.ANALYSIS_RESULTS, questions + tables, stale_filter={})
    return {**res, "questions": len(questions), "tables": len(tables)}


def load_models(w: MongoWriter, dataset: str) -> dict:
    ev, tr = read_json(ML_DIR / "evaluation.json"), read_json(ML_DIR / "training.json")
    if not ev or not tr:
        raise FileNotFoundError("reports/ml/evaluation.json / training.json missing - run scripts/run_phase7.sh")
    sel, best, cm = tr["selection"], ev["best_algorithm"], ev["cost_model"]
    evaluated_at, now = mtime(ML_DIR / "evaluation.json"), utcnow()
    tm = {(r["model"], r["point"]): r for r in ev["test_metrics"] if r["split"] == "test"}
    cf = {(r["model"], r["point"]): r for r in ev["confusion_matrices"] if r["split"] == "test"}
    cost = {r["model"]: r for r in ev["cost_bootstrap"]}
    f1b = {r["model"]: r for r in ev["f1_bootstrap"]}

    def point_doc(m: str, p: str) -> dict:
        r, c = tm[(m, p)], cf[(m, p)]
        return {"threshold": r["threshold"], "precision": r["precision"], "recall": r["recall"], "f1": r["f1"], "accuracy": r["accuracy"],
                "pr_auc": r["pr_auc"], "roc_auc": r["roc_auc"], "onset_recall": r["onset_recall"], "false_alarm_rate": r["false_alarm_rate"],
                "miss_rate": r["miss_rate"], "cost": r["cost"], "cost_per_1000": r["cost_per_1000"],
                "confusion": {k: c[k] for k in ("tp", "fp", "fn", "tn")}}
    docs = []
    for m in BASELINES + ALGORITHMS:
        a = sel["algorithms"].get(m)
        docs.append(clean({
            "_id": f"phase7:{m}", "model": m, "label": tm[(m, "f1")]["label"], "kind": "model" if a else "baseline", "is_best": m == best,
            "params": a["params"] if a else None, "operating_threshold": a["threshold"] if a else None,
            "model_path": a["path"] if a else None, "train_fraction": sel["train_fraction"] if a else None,
            "validation": {"pr_auc": a["pr_auc"], "roc_auc": a["roc_auc"], "f1_point": a["f1_point"], "cost_point": a["cost_point"]} if a else None,
            "test": {"f1_optimal": point_doc(m, "f1"), "cost_optimal": point_doc(m, "cost")},
            "cost_vs_persistence": cost.get(m), "f1_vs_persistence": f1b.get(m), "cost_model": cm,
            "evaluated_at": evaluated_at, "loaded_at": now, "source": "reports/ml/evaluation.json"}))
    return w.replace_documents(S.MODEL_EVALUATIONS, docs, stale_filter={})


def load_streaming(w: MongoWriter, dataset: str) -> dict:
    docs, now = [], utcnow()
    for f in sorted((STREAM_DIR / "metrics").glob("progress_*.jsonl")):
        for line in f.read_text().splitlines():
            if not line.strip():
                continue
            p = json.loads(line)
            ts = pd.Timestamp(p["timestamp"]).to_pydatetime()
            docs.append(clean({"_id": f"{p['run']}|{p['name']}|{p['runId']}|{p['batchId']}", "query": p["name"], "run": p["run"], "batch_id": p["batchId"],
                               "timestamp": ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc), "num_input_rows": p.get("numInputRows", 0),
                               "input_rows_per_second": p.get("inputRowsPerSecond"), "processed_rows_per_second": p.get("processedRowsPerSecond"),
                               "trigger_ms": p.get("durationMs", {}).get("triggerExecution"), "progress": p}))
    n_progress = len(docs)
    for f in [STREAM_DIR / "stream_metrics.json", STREAM_DIR / "live_rt" / "stream_metrics.json"]:
        d = read_json(f)
        if d:
            docs.append(clean({"_id": f"summary:{d['run']}", "doc_type": "summary", "run": d["run"], "query": "__summary__", "queries": d["queries"],
                               "end_to_end_latency": d["end_to_end_latency"], "mongo_counts": d["mongo_counts"], "generated_at": d["generated_at"],
                               "loaded_at": now, "timestamp": mtime(f)}))
    res = w.replace_documents(S.STREAM_METRICS, docs)
    return {**res, "progress_docs": n_progress, "summaries": len(docs) - n_progress}


def _run(rid: str, kind: str, name: str, ok: bool, finished: datetime, seconds: float | None, summary: dict, artifacts: list[str]) -> dict:
    return clean({"_id": rid, "kind": kind, "name": name, "status": "succeeded" if ok else "failed", "finished_at": finished,
                  "duration_seconds": seconds, "summary": summary, "artifacts": artifacts})


def load_pipeline(w: MongoWriter, dataset: str) -> dict:
    runs = []
    st = read_json(ANALYTICS_DIR / "run_status.json") or {}
    for qid, s in st.items():
        f = ANALYTICS_DIR / f"{qid}.json"
        runs.append(_run(f"analytics:{qid}", "analytics", qid, bool(s.get("ok")), mtime(f) if f.exists() else utcnow(), s.get("seconds"),
                         {k: s[k] for k in ("tables", "figures", "findings", "error") if k in s}, [f"reports/analytics/{qid}.md", f"reports/analytics/{qid}.json"]))
    tr, ev = read_json(ML_DIR / "training.json"), read_json(ML_DIR / "evaluation.json")
    if tr:
        runs.append(_run("ml:training", "ml_training", "train_model", bool(tr["selection"]["reload_check"]["ok"]), mtime(ML_DIR / "training.json"),
                         tr.get("seconds"), {"best_algorithm": tr["selection"]["best_algorithm"], "candidates": tr["selection"]["grid_candidates"],
                                             "train_rows": tr["selection"]["train_sample"]["rows"], "cost_model": tr["selection"]["cost_model"],
                                             "quick": tr.get("quick", False), "candidates_table": tr["candidates"]},
                         ["reports/ml/training.md", "data/models/pems_bay/ml_pipeline/best"]))
    for short in ("partition", "storage", "optimization"):                                   # Phase 4 Spark benchmarks
        name = f"{short}_benchmarks" if short != "optimization" else "optimization"
        f = REPORTS_DIR / "spark_features" / f"{name}.json"
        if f.exists():
            runs.append(_run(f"benchmark:{short}", "spark_benchmark", short, True, mtime(f), None, read_json(f),
                             [f"reports/spark_features/{name}.md", f"reports/spark_features/{name}.json"]))
    if ev:
        runs.append(_run("ml:evaluation", "ml_evaluation", "evaluate_model", bool(ev["reload_check"]["ok"]), mtime(ML_DIR / "evaluation.json"), None,
                         {"best_algorithm": ev["best_algorithm"], "findings": len(ev["findings"]), "max_abs_probability_diff": ev["reload_check"]["max_abs_probability_diff"]},
                         ["reports/ml/evaluation.md"]))
    rec = read_json(STREAM_DIR / "recovery_test.json")
    if rec:
        runs.append(_run("streaming:recovery_test", "streaming", "recovery_test", bool(rec["passed"]), mtime(STREAM_DIR / "recovery_test.json"), rec.get("seconds"),
                         {"scenarios": [k for k in rec["scenarios"] if k != "reference"],
                          "checks": sum(len(s["checks"]) for k, s in rec["scenarios"].items() if k != "reference")}, ["reports/streaming/recovery_test.md"]))
    for name in ("parity", "accounting"):
        d = read_json(STREAM_DIR / f"{name}.json")
        if d:
            runs.append(_run(f"streaming:{name}", "streaming", f"verify_{name}", all(c["ok"] for c in d.values()), mtime(STREAM_DIR / f"{name}.json"), None,
                             {"checks": len(d)}, [f"reports/streaming/{name}.json"]))
    for rid, f in (("live", STREAM_DIR / "stream_metrics.json"), ("live_rt", STREAM_DIR / "live_rt" / "stream_metrics.json")):
        d = read_json(f)
        if d:
            lat = d["end_to_end_latency"].get("since_slot_complete", {})
            runs.append(_run(f"streaming:{rid}", "streaming", f"live_{rid}", True, mtime(f), None,
                             {"queries": list(d["queries"]), "latency_p50_s": lat.get("p50_s"), "latency_p95_s": lat.get("p95_s")}, [str(f.relative_to(REPORTS_DIR.parent))]))
    for name, f in (("clean", REPORTS_DIR / "preprocessing" / "pems_bay_cleaning_report.json"), ("features", REPORTS_DIR / "preprocessing" / "pems_bay_feature_report.json")):
        if f.exists():
            runs.append(_run(f"preprocessing:{name}", "preprocessing", name, True, mtime(f), None, {}, [str(f.relative_to(REPORTS_DIR.parent))]))
    for r in runs:
        w.record_pipeline_run(r)
    return {"collection": S.PIPELINE_RUNS, "written": len(runs)}


LOADERS = {"sensors": load_sensors, "hotspots": load_hotspots, "analytics": load_analytics, "models": load_models,
           "streaming": load_streaming, "pipeline": load_pipeline}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", nargs="*", choices=STAGES, help="subset of stages (default: all)")
    ap.add_argument("--uri", default=C.MONGO_URI)
    ap.add_argument("--db", default=C.MONGO_DB)
    ap.add_argument("--dataset", default="pems-bay")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    db = make_client(a.uri)[a.db]
    S.ensure_schema(db)
    w, started, results, failed = MongoWriter(db), utcnow(), {}, []
    for stage in a.only or STAGES:
        try:
            results[stage] = LOADERS[stage](w, a.dataset)
            log.info("%-9s %s", stage, results[stage])
        except Exception as exc:                                       # keep loading the other stages
            failed.append(stage)
            results[stage] = {"error": f"{type(exc).__name__}: {exc}"}
            log.error("%-9s FAILED: %s", stage, exc)
    finished = utcnow()
    w.record_pipeline_run({"_id": f"data_load:{finished:%Y%m%dT%H%M%S}", "kind": "data_load", "name": "load_outputs", "status": "failed" if failed else "succeeded",
                           "started_at": started, "finished_at": finished, "duration_seconds": (finished - started).total_seconds(),
                           "summary": clean(results), "artifacts": []})
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
