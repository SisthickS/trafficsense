"""Performance overview for the dashboard: Spark benchmarks (Phase 4), analytics job durations, MLlib training times and
streaming throughput / latency - all read from MongoDB (loaded by `python -m src.mongo.load_outputs`)."""
from __future__ import annotations

from fastapi import APIRouter

from src.api.deps import Reader
from src.api.routers import ERRORS
from src.api.serialize import public, public_list

router = APIRouter(prefix="/api/v1/performance", tags=["performance"], responses={k: ERRORS[k] for k in (422, 503)})


@router.get("/overview", summary="Spark benchmarks, analytics durations, training times, streaming throughput and latency")
def overview(reader: Reader):
    benchmarks = {d["name"]: public(d)["summary"] | {"finished_at": d["finished_at"]} for d in reader.list_runs(kind="spark_benchmark", limit=20)[0]}
    analytics = sorted(({"qid": d["name"], "seconds": d.get("duration_seconds"), "status": d["status"], **d.get("summary", {})}
                        for d in reader.list_runs(kind="analytics", limit=50)[0]), key=lambda r: int(r["qid"][1:]))
    training = next(iter(reader.list_runs(kind="ml_training", limit=1)[0]), None)
    return {"spark_benchmarks": benchmarks, "analytics_jobs": analytics,
            "ml_training": public(training) if training else None,
            "streaming": {"summary": reader.stream_summary(), "reports": public_list(reader.stream_run_reports())}}
