"""Analyst settings: the registry of known keys (type, bounds, default) and value validation.

Known keys are typed and bounded and have a default, so a missing document means 'default'. Custom keys (any other
slug-like name) accept any JSON value up to 8 KB. Deleting a known key's document reverts it to the default.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{1,63}$")
MAX_CUSTOM_BYTES = 8192


@dataclass(frozen=True)
class SettingDef:
    key: str
    type: str                      # float | int | bool | string
    default: Any
    category: str
    description: str
    minimum: float | None = None
    maximum: float | None = None
    choices: tuple | None = None


KNOWN: dict[str, SettingDef] = {d.key: d for d in [
    SettingDef("alert_threshold", "float", 0.686, "alerts", "Probability at or above which a prediction is an alert "
               "(default = validation-tuned, cost-optimal threshold of the Phase 7 model)", 0.01, 0.99),
    SettingDef("watchlist_size", "int", 10, "hotspots", "Number of sensors on the hotspot watch-list", 1, 100),
    SettingDef("hotspot_min_persistence", "float", 0.5, "hotspots", "Minimum share of working days with >= 60 congested minutes "
               "for a sensor to appear on the watch-list", 0.0, 1.0),
    SettingDef("prediction_window_hours", "int", 6, "predictions", "Default look-back (relative to the newest prediction) for "
               "prediction summaries and per-sensor histories", 1, 168),
    SettingDef("default_page_size", "int", 50, "display", "Page size used by list endpoints when `limit` is omitted", 1, 500),
    SettingDef("dashboard_refresh_seconds", "int", 10, "display", "Dashboard auto-refresh interval", 2, 3600),
    SettingDef("sensor_status_filter", "string", "active", "display", "Default sensor status shown on the dashboard map",
               choices=("active", "inactive", "maintenance", "decommissioned", "all")),
]}


def category_for(key: str) -> str:
    return KNOWN[key].category if key in KNOWN else "custom"


def validate_key(key: str) -> str:
    if not isinstance(key, str) or not KEY_PATTERN.match(key):
        raise ValueError("key must match ^[a-z][a-z0-9_.-]{1,63}$ (lower-case letters, digits, _ . -)")
    return key


def validate_value(key: str, value: Any) -> Any:
    """Return the normalised value or raise ValueError with a message safe to show to the client."""
    d = KNOWN.get(key)
    if d is None:
        try:
            size = len(json.dumps(value, allow_nan=False))
        except (TypeError, ValueError):
            raise ValueError("value must be JSON-serialisable (no NaN/Infinity)")
        if size > MAX_CUSTOM_BYTES:
            raise ValueError(f"custom setting values are limited to {MAX_CUSTOM_BYTES} bytes of JSON")
        return value
    if d.type in ("int", "float"):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{key} must be a number")
        if d.type == "int":
            if isinstance(value, float) and not value.is_integer():
                raise ValueError(f"{key} must be an integer")
            value = int(value)
        else:
            value = float(value)
            if value != value or value in (float("inf"), float("-inf")):
                raise ValueError(f"{key} must be finite")
        if (d.minimum is not None and value < d.minimum) or (d.maximum is not None and value > d.maximum):
            raise ValueError(f"{key} must be between {d.minimum:g} and {d.maximum:g}")
        return value
    if d.type == "string":
        if not isinstance(value, str):
            raise ValueError(f"{key} must be a string")
        if d.choices and value not in d.choices:
            raise ValueError(f"{key} must be one of {', '.join(d.choices)}")
        return value
    if d.type == "bool":
        if not isinstance(value, bool):
            raise ValueError(f"{key} must be true or false")
        return value
    raise ValueError(f"unsupported setting type {d.type}")
