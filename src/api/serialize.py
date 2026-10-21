"""Document -> response-model conversion (the only place that knows the MongoDB document layout)."""
from __future__ import annotations

from typing import Any

from src.api.schemas import HotspotOut, PredictionOut, SensorNearOut, SensorOut


def public(doc: dict | None) -> dict | None:
    """Drop MongoDB's `_id` (always a natural key that is also present as a normal field)."""
    return None if doc is None else {k: v for k, v in doc.items() if k != "_id"}


def public_list(docs: list[dict]) -> list[dict]:
    return [public(d) for d in docs]


def hotspot_summary(h: dict | None) -> dict[str, Any] | None:
    if h is None:
        return None
    return {k: h.get(k) for k in ("rank", "class", "is_hotspot", "persistence", "mean_daily_congested_min", "csbi", "csbi_rank", "tti_peak")}


def sensor_out(doc: dict, hotspot: dict | None = None) -> SensorOut:
    return SensorOut.model_validate({**doc, "hotspot": hotspot_summary(hotspot)})


def sensor_near_out(doc: dict) -> SensorNearOut:
    return SensorNearOut.model_validate(doc)


def hotspot_out(doc: dict) -> HotspotOut:
    return HotspotOut.model_validate({**doc, "hotspot_class": doc["class"]})


def prediction_out(doc: dict, threshold: float) -> PredictionOut:
    return PredictionOut.model_validate({**doc, "alert": doc["probability"] >= threshold, "stored_alert": bool(doc.get("alert"))})
