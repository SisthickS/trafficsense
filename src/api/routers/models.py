"""Model evaluations (Phase 7, from MongoDB) and live inference with the saved best model."""
from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Path, Query, Request

from src.api.deps import Reader
from src.api.errors import ApiError
from src.api.inference import InferenceUnavailable, ModelService
from src.api.routers import ERRORS
from src.api.schemas import PredictRequest, PredictResponse, PredictionResult
from src.api.serialize import public
from src.mongo.errors import NotFoundError
from src.preprocessing.build_features import FEATURE_COLUMNS, LABEL

router = APIRouter(prefix="/api/v1/models", tags=["models"], responses={k: ERRORS[k] for k in (422, 503)})


def service(request: Request) -> ModelService:
    return request.app.state.models


@router.get("/evaluations", summary="Test-period evaluation of every model and baseline (precision, recall, F1, ROC-AUC, PR-AUC, confusion, cost)")
def list_evaluations(reader: Reader, kind: Annotated[str | None, Query(pattern="^(model|baseline)$")] = None):
    return [public(d) for d in reader.list_evaluations(kind)]


@router.get("/evaluations/best", responses={404: ERRORS[404]}, summary="The model selected by lowest validation cost")
def best(reader: Reader):
    doc = reader.best_evaluation()
    if doc is None:
        raise NotFoundError("evaluation", "best")
    return public(doc)


@router.get("/evaluations/{model}", responses={404: ERRORS[404]})
def get_evaluation(model: Annotated[str, Path(pattern=r"^[a-z_]{1,40}$")], reader: Reader):
    doc = reader.get_evaluation(model)
    if doc is None:
        raise NotFoundError("evaluation", model)
    return public(doc)


@router.get("/info", summary="Saved model bundle metadata and inference state (does not load Spark)")
def info(request: Request):
    return service(request).info()


@router.get("/example", summary="A real feature row (test period) to copy into POST /models/predict")
def example(sensor_id: Annotated[int | None, Query(ge=1)] = None,
            ts_utc: Annotated[datetime | None, Query(description="Exact reading time (UTC), e.g. 2017-06-05T12:05:00Z")] = None):
    import pyarrow as pa
    import pyarrow.dataset as ds
    from src.modeling.data import features_path
    path = features_path()
    if not path.exists():
        raise ApiError(503, f"feature table not found at {path}", code="features_unavailable")
    dataset = ds.dataset(str(path), format="parquet", partitioning="hive")
    flt = ds.field("split") == "test" if ts_utc is None else ds.field("split").isin(["train", "val", "test"])
    if sensor_id is not None:
        flt = flt & (ds.field("sensor_id") == sensor_id)
    if ts_utc is not None:
        stamp = ts_utc.astimezone(timezone.utc).replace(tzinfo=None) if ts_utc.tzinfo else ts_utc
        flt = flt & (ds.field("ts_utc") == pa.scalar(stamp, type=dataset.schema.field("ts_utc").type))
    tbl = dataset.scanner(filter=flt, columns=["sensor_id", "ts_utc", LABEL] + FEATURE_COLUMNS).head(1)
    if tbl.num_rows == 0:
        raise NotFoundError("feature row", f"sensor={sensor_id} ts_utc={ts_utc}")
    row = tbl.to_pylist()[0]
    clean = lambda v: None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v
    return {"sensor_id": row["sensor_id"], "ts_utc": row["ts_utc"], "actual_label": row[LABEL],
            "instance": {"sensor_id": row["sensor_id"], "features": {c: clean(row[c]) for c in FEATURE_COLUMNS}}}


@router.post("/predict", response_model=PredictResponse, responses={503: ERRORS[503]},
             summary="Score feature rows with the saved best model (Spark PipelineModel, loaded on first use)")
def predict(body: PredictRequest, request: Request, reader: Reader):
    svc, settings = service(request), request.app.state.settings
    if len(body.instances) > settings.max_predict_instances:
        raise ApiError(413, f"at most {settings.max_predict_instances} instances per request", code="payload_too_large")
    if body.threshold is not None:
        threshold, source = body.threshold, "request"
    elif (doc := reader.get_setting_doc("alert_threshold")) is not None:
        threshold, source = float(doc["value"]), "settings"
    else:
        threshold, source = svc.default_threshold() or float(reader.effective_setting("alert_threshold")), "default"
    rows = [i.features.model_dump() for i in body.instances]
    t0 = time.perf_counter()
    try:
        probs, cold = svc.predict(rows)
    except InferenceUnavailable as exc:
        raise ApiError(503, str(exc), code="model_unavailable")
    ms = (time.perf_counter() - t0) * 1000
    results = [PredictionResult(index=i, sensor_id=inst.sensor_id, probability=p, alert=p >= threshold,
                                imputed_features=sum(v is None for v in rows[i].values()))
               for i, (inst, p) in enumerate(zip(body.instances, probs))]
    return PredictResponse(model=svc.model_label(), threshold=threshold, threshold_source=source, count=len(results),
                           inference_ms=round(ms, 1), cold_start=cold, predictions=results)
