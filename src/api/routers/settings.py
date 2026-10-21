"""Analyst settings: full CRUD with typed validation for known keys, free-form JSON for custom keys.

A known key always has a value: when no override is stored, GET returns the registry default (`is_default: true`).
DELETE removes the override (known keys revert to the default; custom keys disappear).
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Path, Query, Response

from src.api.deps import Actor, IfMatch, Reader, WriteAccess, Writer
from src.api.errors import ApiError
from src.api.routers import ERRORS
from src.api.schemas import SettingCreate, SettingOut, SettingPatch, SettingPut
from src.mongo.errors import NotFoundError
from src.mongo.settings_defs import KEY_PATTERN, KNOWN, validate_value

router = APIRouter(prefix="/api/v1/settings", tags=["settings"], responses={k: ERRORS[k] for k in (422, 503)})
Key = Annotated[str, Path(pattern=KEY_PATTERN.pattern, description="Setting key (lower-case slug)")]


def _checked(key: str, value):
    try:
        return validate_value(key, value)
    except ValueError as exc:
        raise ApiError(422, str(exc), code="validation_error", details=[{"field": "body.value", "message": str(exc), "type": "value_error"}])


def _out(reader: Reader, key: str, doc: dict | None) -> SettingOut:
    return SettingOut.model_validate(reader.describe_setting(key, doc))


@router.get("", response_model=list[SettingOut], summary="All settings (known keys with defaults + stored custom keys)")
def list_settings(reader: Reader, category: Annotated[str | None, Query(max_length=32)] = None,
                  overridden_only: Annotated[bool, Query(description="Only settings with a stored override")] = False):
    out = [SettingOut.model_validate(s) for s in reader.effective_settings()]
    if category:
        out = [s for s in out if s.category == category]
    return [s for s in out if not (overridden_only and s.is_default)]


@router.get("/{key}", response_model=SettingOut, responses={404: ERRORS[404]}, summary="Get one setting (default if not overridden)")
def get_setting(key: Key, reader: Reader, response: Response):
    doc = reader.get_setting_doc(key)
    if doc is None and key not in KNOWN:
        raise NotFoundError("setting", key)
    if doc is not None:
        response.headers["ETag"] = f'"{doc["version"]}"'
    return _out(reader, key, doc)


@router.post("", response_model=SettingOut, status_code=201, dependencies=[WriteAccess], responses={k: ERRORS[k] for k in (401, 409)},
             summary="Create a setting (409 if it already exists)")
def create_setting(body: SettingCreate, reader: Reader, writer: Writer, response: Response, who: Actor):
    value = _checked(body.key, body.value)
    doc = writer.create_setting(body.key, value, body.description, who)
    response.headers["Location"] = f"/api/v1/settings/{body.key}"
    response.headers["ETag"] = f'"{doc["version"]}"'
    return _out(reader, body.key, doc)


@router.put("/{key}", response_model=SettingOut, dependencies=[WriteAccess], responses={k: ERRORS[k] for k in (401, 412)},
            summary="Create or replace a setting (201 when created, 200 when replaced)")
def put_setting(key: Key, body: SettingPut, reader: Reader, writer: Writer, response: Response, version: IfMatch, who: Actor):
    value = _checked(key, body.value)
    doc, created = writer.put_setting(key, value, body.description, who, expected_version=version)
    response.status_code = 201 if created else 200
    response.headers["ETag"] = f'"{doc["version"]}"'
    return _out(reader, key, doc)


@router.patch("/{key}", response_model=SettingOut, dependencies=[WriteAccess], responses={k: ERRORS[k] for k in (401, 404, 412)},
              summary="Change value and/or description of a stored setting")
def patch_setting(key: Key, body: SettingPatch, reader: Reader, writer: Writer, response: Response, version: IfMatch, who: Actor):
    changes = body.model_dump(exclude_unset=True)
    if "value" in changes:
        changes["value"] = _checked(key, changes["value"])
    doc = writer.patch_setting(key, changes, who, expected_version=version)
    response.headers["ETag"] = f'"{doc["version"]}"'
    return _out(reader, key, doc)


@router.delete("/{key}", status_code=204, dependencies=[WriteAccess], responses={k: ERRORS[k] for k in (401, 404, 412)},
               summary="Delete the stored setting (known keys revert to their default)")
def delete_setting(key: Key, writer: Writer, version: IfMatch):
    writer.delete_setting(key, expected_version=version)
    return Response(status_code=204)
