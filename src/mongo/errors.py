"""Domain errors raised by the writer/reader; the API maps them to HTTP status codes (404 / 409 / 412)."""
from __future__ import annotations


class MongoLayerError(Exception):
    """Base class."""


class NotFoundError(MongoLayerError):
    def __init__(self, what: str, key):
        super().__init__(f"{what} {key!r} not found")
        self.what, self.key = what, key


class ConflictError(MongoLayerError):
    """The document already exists (unique key violated)."""


class VersionMismatchError(MongoLayerError):
    """Optimistic-concurrency failure: the document changed since the caller read it."""

    def __init__(self, what: str, key, expected, actual=None):
        super().__init__(f"{what} {key!r} was modified (expected version {expected}, current {actual})")
        self.expected, self.actual = expected, actual
