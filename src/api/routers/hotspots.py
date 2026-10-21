"""Congestion hotspots (Spark Q5 persistence + Q9 severity index) - read-only."""
from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Query

from src.api.deps import Paging, Reader
from src.api.errors import ApiError
from src.api.routers import ERRORS
from src.api.schemas import HotspotOut, PageOut
from src.api.serialize import hotspot_out
from src.mongo.errors import NotFoundError

router = APIRouter(prefix="/api/v1/hotspots", tags=["hotspots"], responses={k: ERRORS[k] for k in (422, 503)})


@router.get("", response_model=PageOut[HotspotOut], summary="List sensors by hotspot rank (filter by class, persistence, location)")
def list_hotspots(reader: Reader, page: Paging,
                  hotspot_class: Annotated[str | None, Query(alias="class", description="e.g. `persistent hotspot` (see /hotspots/classes)")] = None,
                  min_persistence: Annotated[float | None, Query(ge=0, le=1)] = None,
                  lat: Annotated[float | None, Query(ge=-90, le=90)] = None, lon: Annotated[float | None, Query(ge=-180, le=180)] = None,
                  radius_m: Annotated[float | None, Query(gt=0, le=50_000)] = None,
                  sort: Literal["rank", "csbi_rank", "persistence", "mean_daily_congested_min", "csbi", "sensor_id"] = "rank",
                  order: Literal["asc", "desc"] = "asc"):
    geo = (lat, lon, radius_m)
    if any(v is not None for v in geo) and not all(v is not None for v in geo):
        raise ApiError(422, "lat, lon and radius_m must be given together", code="validation_error")
    items, total = reader.list_hotspots(hotspot_class, min_persistence, geo if all(v is not None for v in geo) else None, sort,
                                        order == "desc", page.limit, page.offset)
    return PageOut[HotspotOut](items=[hotspot_out(d) for d in items], total=total, limit=page.limit, offset=page.offset)


@router.get("/classes", summary="Hotspot classes with sensor counts")
def hotspot_classes(reader: Reader):
    return reader.hotspot_classes()


@router.get("/watchlist", summary="Dashboard watch-list (size and persistence cut-off come from the analyst settings)")
def watchlist(reader: Reader):
    size = int(reader.effective_setting("watchlist_size"))
    min_p = float(reader.effective_setting("hotspot_min_persistence"))
    items, total = reader.list_hotspots(min_persistence=min_p, limit=size)
    return {"size": size, "min_persistence": min_p, "eligible_sensors": total, "items": [hotspot_out(d) for d in items]}


@router.get("/{sensor_id}", response_model=HotspotOut, responses={404: ERRORS[404]})
def get_hotspot(sensor_id: int, reader: Reader):
    doc = reader.get_hotspot(sensor_id)
    if doc is None:
        raise NotFoundError("hotspot record for sensor", sensor_id)
    return hotspot_out(doc)
