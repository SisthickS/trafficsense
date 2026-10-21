"""Request dependencies: database access, pagination, optimistic-concurrency header, write authorisation, audit header."""
from __future__ import annotations

import re
import secrets
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Header, Query, Request

from src.api.config import ApiSettings
from src.api.errors import ApiError
from src.mongo.mongo_reader import MongoReader
from src.mongo.mongo_writer import MongoWriter


def settings_dep(request: Request) -> ApiSettings:
    return request.app.state.settings


def reader_dep(request: Request) -> MongoReader:
    return request.app.state.reader


def writer_dep(request: Request) -> MongoWriter:
    return request.app.state.writer


Settings = Annotated[ApiSettings, Depends(settings_dep)]
Reader = Annotated[MongoReader, Depends(reader_dep)]
Writer = Annotated[MongoWriter, Depends(writer_dep)]


def require_write(settings: Settings, x_api_key: Annotated[str | None, Header(description="Required for writes when the server has an API key")] = None) -> None:
    """Writes need `X-API-Key` when TRAFFICSENSE_API_KEY is configured (constant-time comparison)."""
    if settings.api_key is None:
        return
    if not x_api_key or not secrets.compare_digest(x_api_key.encode(), settings.api_key.get_secret_value().encode()):
        raise ApiError(401, "missing or invalid API key", headers={"WWW-Authenticate": "ApiKey"})


WriteAccess = Depends(require_write)


def actor(x_updated_by: Annotated[str | None, Header(max_length=64, description="Optional analyst name stored as `updated_by`")] = None) -> str | None:
    return x_updated_by.strip() or None if x_updated_by else None


Actor = Annotated[str | None, Depends(actor)]


def if_match(if_match: Annotated[str | None, Header(alias="If-Match", description='Version the client last saw, e.g. `"3"` (optimistic concurrency)')] = None) -> int | None:
    if if_match is None or if_match.strip() == "*":
        return None
    m = re.fullmatch(r'\s*(?:W/)?"?(\d{1,9})"?\s*', if_match)
    if not m:
        raise ApiError(400, 'If-Match must be a version number such as "3"', code="invalid_if_match")
    return int(m.group(1))


IfMatch = Annotated[int | None, Depends(if_match)]


@dataclass
class Page:
    limit: int
    offset: int


def pagination(settings: Settings, reader: Reader,
               limit: Annotated[int | None, Query(ge=1, description="Page size (default: analyst setting `default_page_size`)")] = None,
               offset: Annotated[int, Query(ge=0)] = 0) -> Page:
    if limit is None:
        limit = int(reader.effective_setting("default_page_size"))
    if limit > settings.max_page_size:
        raise ApiError(422, f"limit must be <= {settings.max_page_size}", code="validation_error",
                       details=[{"field": "query.limit", "message": f"must be <= {settings.max_page_size}", "type": "less_than_equal"}])
    return Page(limit, offset)


Paging = Annotated[Page, Depends(pagination)]
