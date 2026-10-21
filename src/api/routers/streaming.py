"""Streaming job observability: query-progress metrics (`stream_metrics`), run summaries and a liveness probe - read-only."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Query

from src.api.deps import Paging, Reader
from src.api.routers import ERRORS
from src.api.serialize import public_list
from src.streaming import config as C

router = APIRouter(prefix="/api/v1/streaming", tags=["streaming"], responses={k: ERRORS[k] for k in (422, 503)})
Run = Annotated[str | None, Query(max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")]


@router.get("/runs", summary="Streaming runs that reported progress")
def runs(reader: Reader):
    return reader.stream_runs()


@router.get("/metrics/summary", summary="Per run and query: batches, rows, batch-duration p50/p95/max (MongoDB aggregation)")
def summary(reader: Reader, run: Run = None):
    return reader.stream_summary(run)


@router.get("/metrics/progress", summary="Raw StreamingQueryProgress records, newest first")
def progress(reader: Reader, page: Paging, run: Run = None, query: Annotated[str | None, Query(pattern=r"^[a-z_]{1,32}$")] = None):
    items, total = reader.stream_progress(run, query, page.limit, page.offset)
    return {"total": total, "limit": page.limit, "offset": page.offset, "items": public_list(items)}


@router.get("/reports", summary="Run reports: end-to-end latency, throughput, document counts")
def reports(reader: Reader, run: Run = None):
    return public_list(reader.stream_run_reports(run))


@router.get("/health", summary="Is the streaming job committing batches? (age of the last committed batch)")
def health(reader: Reader, max_age_s: Annotated[float, Query(gt=0, le=86400)] = 60.0):
    st = reader.stream_state()
    if st is None:
        return {"healthy": False, "reason": "no stream state document: the streaming job has never committed a batch"}
    age = (datetime.now(timezone.utc) - st["updated_at"]).total_seconds()
    return {"healthy": age <= max_age_s, "seconds_since_last_commit": round(age, 1), "max_age_s": max_age_s, "last_batch_id": st.get("last_batch_id"),
            "last_finalized_slot": st.get("last_final_slot"), "newest_prediction_event_ts": reader.newest_prediction_time(),
            "dead_letters": reader.db[C.COL_DEAD_LETTERS].estimated_document_count(), "late_events": reader.db["late_events"].estimated_document_count()}
