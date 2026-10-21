"""TrafficSense API application.

    uvicorn src.api.main:app --host 127.0.0.1 --port 8000            # configuration from TRAFFICSENSE_* / .env
    uvicorn src.api.main:app --reload                                  # development
    python -m src.api.main                                             # same, using TRAFFICSENSE_HOST / _PORT
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from src.api import __version__
from src.api.config import ApiSettings, get_settings
from src.api.errors import install_error_handlers
from src.api.inference import ModelService
from src.api.routers import analytics, health, hotspots, models, performance, pipeline, predictions, sensors, settings, streaming
from src.mongo.connection import make_client
from src.mongo.mongo_reader import MongoReader
from src.mongo.mongo_writer import MongoWriter
from src.mongo.schema import ensure_schema

log = logging.getLogger("trafficsense.api")

DESCRIPTION = """
REST backend for TrafficSense: sensor registry and analyst settings (full CRUD), Spark analytics results (Q1-Q18), congestion hotspots,
5-minute-ahead predictions from the streaming job, model evaluations and live inference with the saved model, streaming metrics and pipeline runs.
All data lives in MongoDB. Errors use one envelope: `{"error": {"code", "message", "details", "request_id"}}`.
Writes (POST/PUT/PATCH/DELETE) require `X-API-Key` when the server has an API key; PUT/PATCH/DELETE honour `If-Match: "<version>"`.
"""


def create_app(config: ApiSettings | None = None) -> FastAPI:
    cfg = config or get_settings()
    logging.basicConfig(level=cfg.log_level, format="%(asctime)s %(levelname)-7s %(name)s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("pymongo").setLevel(logging.WARNING)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = make_client(cfg.mongo_uri, cfg.mongo_timeout_ms, app="trafficsense-api")
        db = client[cfg.mongo_db]
        app.state.settings, app.state.client, app.state.db = cfg, client, db
        app.state.reader, app.state.writer, app.state.models = MongoReader(db), MongoWriter(db), ModelService(cfg)
        app.state.started_at, app.state.schema_ready = time.time(), False
        try:
            ensure_schema(db)
            app.state.schema_ready = True
        except Exception as exc:                                # start anyway: /ready reports 503 and retries the setup
            log.error("MongoDB not ready at startup (%s: %s); the API will retry on /ready and per request", type(exc).__name__, exc)
        log.info("TrafficSense API %s started (env=%s, db=%s, writes=%s)", __version__, cfg.env, cfg.mongo_db, "api-key" if cfg.api_key else "open")
        if cfg.preload_model and cfg.enable_model_inference:
            def preload():
                try:
                    app.state.models.warm_up()
                except Exception as exc:                        # inference stays available lazily; /models/predict reports the error
                    log.error("model preload failed: %s: %s", type(exc).__name__, exc)
            threading.Thread(target=preload, name="model-preload", daemon=True).start()
        yield
        app.state.models.close()
        client.close()

    app = FastAPI(title="TrafficSense API", version=__version__, description=DESCRIPTION, lifespan=lifespan,
                  docs_url="/docs" if cfg.docs_enabled else None, redoc_url="/redoc" if cfg.docs_enabled else None,
                  openapi_url="/openapi.json" if cfg.docs_enabled else None)
    app.add_middleware(CORSMiddleware, allow_origins=cfg.cors_origin_list, allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
                       allow_headers=["Content-Type", "X-API-Key", "If-Match", "X-Updated-By"], expose_headers=["ETag", "Location", "X-Request-ID"])

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request.state.request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:16]
        t0 = time.perf_counter()
        response = await call_next(request)
        ms = (time.perf_counter() - t0) * 1000
        response.headers["X-Request-ID"] = request.state.request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        log.info("%s %s -> %d (%.1f ms) rid=%s", request.method, request.url.path, response.status_code, ms, request.state.request_id)
        return response

    install_error_handlers(app)
    for module in (health, sensors, settings, hotspots, predictions, analytics, models, streaming, pipeline, performance):
        app.include_router(module.router)

    @app.get("/", include_in_schema=False)
    def root():
        return {"name": "TrafficSense API", "version": __version__, "docs": "/docs" if cfg.docs_enabled else None, "health": "/health", "ready": "/ready"}

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn
    s = get_settings()
    uvicorn.run("src.api.main:app", host=s.host, port=s.port, log_level=s.log_level.lower())
