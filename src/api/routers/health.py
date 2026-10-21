"""Liveness, readiness and an overview of what the API serves."""
from __future__ import annotations

import time

from fastapi import APIRouter, Request

from src.api import __version__
from src.api.deps import Reader, Settings
from src.api.errors import ApiError
from src.mongo.schema import ensure_schema

router = APIRouter(tags=["health"])


@router.get("/health", summary="Liveness: the process is up (does not touch the database)")
def health(request: Request, settings: Settings):
    return {"status": "ok", "version": __version__, "env": settings.env, "uptime_s": round(time.time() - request.app.state.started_at, 1)}


@router.get("/ready", summary="Readiness: MongoDB reachable and schema (collections, validators, indexes) in place")
def ready(request: Request, reader: Reader):
    try:
        reader.ping()
        if not request.app.state.schema_ready:                  # MongoDB was down at startup: finish the setup now
            ensure_schema(reader.db)
            request.app.state.schema_ready = True
    except Exception as exc:
        raise ApiError(503, f"not ready: {type(exc).__name__}", code="not_ready")
    return {"status": "ready", "database": reader.db.name}


@router.get("/api/v1/status/database", summary="MongoDB: server version, storage, and per collection documents / indexes / validator")
def database(reader: Reader):
    t0 = time.perf_counter()
    reader.ping()
    info = reader.database_info()
    info["ping_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return info


@router.get("/api/v1/status", summary="Overview: document counts, sensor statuses, model and streaming state")
def overview(request: Request, reader: Reader, settings: Settings):
    models = request.app.state.models
    newest = reader.newest_prediction_time()
    return {"version": __version__, "env": settings.env, "database": reader.db.name, "counts": reader.counts(),
            "sensors_by_status": reader.sensor_status_counts(), "newest_prediction_event_ts": newest,
            "write_protection": "api-key" if settings.api_key else "open", "model": {"loaded": models.loaded, "enabled": settings.enable_model_inference},
            "pipeline": reader.latest_run_per_kind()}
