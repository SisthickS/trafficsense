"""5-minute-ahead congestion predictions produced by the streaming job (`traffic_predictions`) - read-only.

`alert` is evaluated with the analyst setting `alert_threshold`, so changing the setting changes the alerts immediately,
without touching the stored predictions (`stored_alert` keeps the flag computed by the stream with the model default).
"""
from __future__ import annotations

from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Query

from src.api.deps import Paging, Reader
from src.api.routers import ERRORS
from src.api.schemas import LatestPredictions, PredictionOut
from src.api.serialize import prediction_out

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"], responses={k: ERRORS[k] for k in (422, 503)})


@router.get("/latest", response_model=LatestPredictions, summary="One slot (default: the newest): probability of congestion in the next 5 minutes per sensor")
def latest(reader: Reader, page: Paging,
           slot: Annotated[int | None, Query(ge=0, description="5-minute slot number (see /predictions/slots); default: newest")] = None,
           min_probability: Annotated[float | None, Query(ge=0, le=1)] = None,
           alerts_only: Annotated[bool, Query(description="Only sensors at/above the effective alert threshold")] = False):
    threshold = float(reader.effective_setting("alert_threshold"))
    floor = max(min_probability or 0.0, threshold if alerts_only else 0.0) or None
    items, total, slot = reader.latest_predictions(slot=slot, min_probability=floor, limit=page.limit, offset=page.offset)
    return LatestPredictions(slot=slot, event_ts=items[0]["event_ts"] if items else None, threshold=threshold, total=total,
                             limit=page.limit, offset=page.offset, items=[prediction_out(d, threshold) for d in items])


@router.get("/slots", summary="Every slot with predictions (sensors, alerts at the effective threshold, mean/max probability) - drives playback")
def slots(reader: Reader):
    threshold = float(reader.effective_setting("alert_threshold"))
    return {"threshold": threshold, "slots": reader.prediction_slots(threshold)}


@router.get("/overview", summary="Counts and a per-slot series over the look-back window (relative to the newest prediction)")
def overview(reader: Reader,
             window_hours: Annotated[int | None, Query(ge=1, le=168, description="Default: analyst setting `prediction_window_hours`")] = None):
    hours = window_hours or int(reader.effective_setting("prediction_window_hours"))
    return reader.prediction_overview(hours, float(reader.effective_setting("alert_threshold")))


@router.get("/sensors/{sensor_id}", response_model=list[PredictionOut], summary="Prediction history of one sensor, newest first")
def sensor_history(sensor_id: int, reader: Reader,
                   hours: Annotated[int | None, Query(ge=1, le=168, description="Look-back from the newest prediction; default: setting `prediction_window_hours`")] = None,
                   limit: Annotated[int, Query(ge=1, le=1000)] = 100):
    newest = reader.newest_prediction_time()
    if newest is None:
        return []
    threshold = float(reader.effective_setting("alert_threshold"))
    since = newest - timedelta(hours=hours or int(reader.effective_setting("prediction_window_hours")))
    return [prediction_out(d, threshold) for d in reader.sensor_predictions(sensor_id, since=since, limit=limit)]
