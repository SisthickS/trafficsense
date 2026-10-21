"""Routers. Shared OpenAPI documentation of the error envelope."""
from src.api.schemas import ErrorResponse

ERRORS = {
    401: {"model": ErrorResponse, "description": "Missing or invalid API key (writes, when a key is configured)"},
    404: {"model": ErrorResponse, "description": "Resource not found"},
    409: {"model": ErrorResponse, "description": "Resource already exists"},
    412: {"model": ErrorResponse, "description": "`If-Match` version does not match the stored version"},
    422: {"model": ErrorResponse, "description": "Validation error"},
    503: {"model": ErrorResponse, "description": "Database unavailable"},
}
