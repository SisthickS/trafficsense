"""Uniform error envelope and the mapping from database / domain errors to HTTP responses.

    {"error": {"code": "not_found", "message": "sensor 5 not found", "details": [...], "request_id": "..."}}
"""
from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pymongo.errors import ConnectionFailure, PyMongoError, WriteError
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.mongo.errors import ConflictError, NotFoundError, VersionMismatchError

log = logging.getLogger("trafficsense.api")

CODES = {400: "bad_request", 401: "unauthorized", 403: "forbidden", 404: "not_found", 405: "method_not_allowed", 409: "conflict",
         412: "precondition_failed", 413: "payload_too_large", 422: "validation_error", 429: "too_many_requests", 500: "internal_error",
         503: "unavailable"}


class ApiError(Exception):
    def __init__(self, status: int, message: str, code: str | None = None, details: list | None = None, headers: dict | None = None):
        super().__init__(message)
        self.status, self.message, self.code, self.details, self.headers = status, message, code or CODES.get(status, "error"), details, headers


def envelope(request: Request, status: int, code: str, message: str, details: list | None = None, headers: dict | None = None) -> JSONResponse:
    body = {"error": {"code": code, "message": message, "request_id": getattr(request.state, "request_id", None)}}
    if details:
        body["error"]["details"] = details
    return JSONResponse(body, status_code=status, headers=headers)


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api(request: Request, exc: ApiError):
        return envelope(request, exc.status, exc.code, exc.message, exc.details, exc.headers)

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException):
        return envelope(request, exc.status_code, CODES.get(exc.status_code, "error"), str(exc.detail), headers=getattr(exc, "headers", None))

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError):
        details = [{"field": ".".join(str(p) for p in e["loc"]), "message": e["msg"], "type": e["type"]} for e in exc.errors()]
        return envelope(request, 422, "validation_error", "request validation failed", details)

    @app.exception_handler(NotFoundError)
    async def _nf(request: Request, exc: NotFoundError):
        return envelope(request, 404, "not_found", str(exc))

    @app.exception_handler(ConflictError)
    async def _conflict(request: Request, exc: ConflictError):
        return envelope(request, 409, "conflict", str(exc))

    @app.exception_handler(VersionMismatchError)
    async def _version(request: Request, exc: VersionMismatchError):
        return envelope(request, 412, "precondition_failed", str(exc), [{"expected_version": exc.expected, "current_version": exc.actual}])

    @app.exception_handler(WriteError)
    async def _write(request: Request, exc: WriteError):
        if exc.code == 121:                                  # $jsonSchema validator rejected the document
            return envelope(request, 422, "document_validation_failed", "the database rejected the document (schema validation)")
        log.exception("database write error")
        return envelope(request, 500, "database_error", "database write failed")

    @app.exception_handler(PyMongoError)
    async def _mongo(request: Request, exc: PyMongoError):
        log.error("database error: %s: %s", type(exc).__name__, exc)
        if isinstance(exc, ConnectionFailure):               # includes ServerSelectionTimeoutError and AutoReconnect
            return envelope(request, 503, "database_unavailable", "the database is unavailable, try again shortly", headers={"Retry-After": "5"})
        return envelope(request, 500, "database_error", "database operation failed")

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception):
        log.exception("unhandled error (request %s)", getattr(request.state, "request_id", None))
        return envelope(request, 500, "internal_error", "internal server error")
