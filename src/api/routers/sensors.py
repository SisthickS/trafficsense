"""Sensor metadata: full CRUD (Create / Read / Update (PUT, PATCH) / Delete) plus filtered, paginated and geo reads."""
from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Query, Response

from src.api.deps import IfMatch, Paging, Reader, WriteAccess, Writer
from src.api.errors import ApiError
from src.api.routers import ERRORS
from src.api.schemas import PageOut, SensorCreate, SensorNearOut, SensorOut, SensorReplace, SensorStatus, SensorUpdate
from src.api.serialize import sensor_near_out, sensor_out
from src.mongo.errors import NotFoundError

router = APIRouter(prefix="/api/v1/sensors", tags=["sensors"], responses={k: ERRORS[k] for k in (422, 503)})


def _etag(response: Response, doc: dict) -> None:
    response.headers["ETag"] = f'"{doc["version"]}"'


def _bbox(value: str | None) -> tuple[float, float, float, float] | None:
    if value is None:
        return None
    try:
        min_lon, min_lat, max_lon, max_lat = (float(x) for x in value.split(","))
    except ValueError:
        raise ApiError(422, "bbox must be 'min_lon,min_lat,max_lon,max_lat'", code="validation_error",
                       details=[{"field": "query.bbox", "message": "expected four comma-separated numbers", "type": "value_error"}])
    if not (-180 <= min_lon < max_lon <= 180 and -90 <= min_lat < max_lat <= 90):
        raise ApiError(422, "bbox out of range or min >= max", code="validation_error",
                       details=[{"field": "query.bbox", "message": "longitudes in [-180,180], latitudes in [-90,90], min < max", "type": "value_error"}])
    return min_lon, min_lat, max_lon, max_lat


@router.get("", response_model=PageOut[SensorOut], summary="List sensors (filter, search, sort, paginate)")
def list_sensors(reader: Reader, page: Paging,
                 status: SensorStatus | None = None,
                 tag: Annotated[str | None, Query(max_length=32, description="Sensors carrying this tag")] = None,
                 source: Literal["dataset", "api"] | None = None,
                 q: Annotated[str | None, Query(min_length=1, max_length=64, description="Case-insensitive match on name/description, or exact sensor id")] = None,
                 bbox: Annotated[str | None, Query(description="min_lon,min_lat,max_lon,max_lat")] = None,
                 sort: Literal["sensor_id", "name", "status", "updated_at", "created_at"] = "sensor_id",
                 order: Literal["asc", "desc"] = "asc"):
    items, total = reader.list_sensors(status, tag, source, q, _bbox(bbox), sort, order == "desc", page.limit, page.offset)
    return PageOut[SensorOut](items=[sensor_out(d) for d in items], total=total, limit=page.limit, offset=page.offset)


@router.get("/near", response_model=list[SensorNearOut], summary="Sensors within a radius, nearest first")
def sensors_near(reader: Reader, lat: Annotated[float, Query(ge=-90, le=90)], lon: Annotated[float, Query(ge=-180, le=180)],
                 radius_m: Annotated[float, Query(gt=0, le=50_000)] = 2000.0, limit: Annotated[int, Query(ge=1, le=200)] = 20):
    return [sensor_near_out(d) for d in reader.sensors_near(lat, lon, radius_m, limit)]


@router.get("/{sensor_id}", response_model=SensorOut, responses={404: ERRORS[404]}, summary="Get one sensor (with its hotspot summary)")
def get_sensor(sensor_id: int, reader: Reader, response: Response):
    doc = reader.get_sensor(sensor_id)
    if doc is None:
        raise NotFoundError("sensor", sensor_id)
    _etag(response, doc)
    return sensor_out(doc, reader.get_hotspot(sensor_id))


@router.post("", response_model=SensorOut, status_code=201, dependencies=[WriteAccess], responses={k: ERRORS[k] for k in (401, 409)},
             summary="Create a sensor")
def create_sensor(body: SensorCreate, writer: Writer, response: Response):
    doc = writer.create_sensor(body.model_dump())
    response.headers["Location"] = f"/api/v1/sensors/{doc['sensor_id']}"
    _etag(response, doc)
    return sensor_out(doc)


@router.put("/{sensor_id}", response_model=SensorOut, dependencies=[WriteAccess], responses={k: ERRORS[k] for k in (401, 404, 412)},
            summary="Replace a sensor's editable fields (omitted optional fields reset to defaults)")
def replace_sensor(sensor_id: int, body: SensorReplace, writer: Writer, response: Response, version: IfMatch):
    doc = writer.update_sensor(sensor_id, body.model_dump(), expected_version=version, replace=True)
    _etag(response, doc)
    return sensor_out(doc)


@router.patch("/{sensor_id}", response_model=SensorOut, dependencies=[WriteAccess], responses={k: ERRORS[k] for k in (401, 404, 412)},
              summary="Update only the fields present in the body")
def update_sensor(sensor_id: int, body: SensorUpdate, writer: Writer, response: Response, version: IfMatch):
    doc = writer.update_sensor(sensor_id, body.model_dump(exclude_unset=True), expected_version=version)
    _etag(response, doc)
    return sensor_out(doc)


@router.delete("/{sensor_id}", status_code=204, dependencies=[WriteAccess], responses={k: ERRORS[k] for k in (401, 404, 412)},
               summary="Delete a sensor")
def delete_sensor(sensor_id: int, writer: Writer, version: IfMatch):
    writer.delete_sensor(sensor_id, expected_version=version)
    return Response(status_code=204)
