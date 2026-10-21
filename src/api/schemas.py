"""Pydantic request / response models (validation lives here; business rules in the Mongo layer)."""
from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Generic, Literal, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, create_model, field_validator, model_validator

from src.mongo.settings_defs import KEY_PATTERN
from src.preprocessing.build_features import FEATURE_COLUMNS

T = TypeVar("T")

SensorStatus = Literal["active", "inactive", "maintenance", "decommissioned"]
Direction = Literal["N", "S", "E", "W"]
Tag = Annotated[str, StringConstraints(strip_whitespace=True, to_lower=True, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$")]   # stored lower-case
Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=120)]
Description = Annotated[str, StringConstraints(strip_whitespace=True, max_length=1000)]
Lat = Annotated[float, Field(ge=-90, le=90, allow_inf_nan=False)]
Lon = Annotated[float, Field(ge=-180, le=180, allow_inf_nan=False)]
SensorId = Annotated[int, Field(ge=1, le=9_999_999_999)]

STRICT = ConfigDict(extra="forbid")


class ErrorBody(BaseModel):
    code: str
    message: str
    request_id: str | None = None
    details: list[dict[str, Any]] | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody


class PageOut(BaseModel, Generic[T]):
    items: list[T]
    total: int
    limit: int
    offset: int


# ------------------------------------------------------------------------------------------------ sensors
def _dedupe(tags: list[str] | None) -> list[str] | None:
    return None if tags is None else list(dict.fromkeys(tags))


class SensorCreate(BaseModel):
    model_config = STRICT
    sensor_id: SensorId
    latitude: Lat
    longitude: Lon
    name: Optional[Name] = None
    description: Optional[Description] = None
    direction: Optional[Direction] = None
    lanes: Optional[Annotated[int, Field(ge=1, le=12)]] = None
    status: SensorStatus = "active"
    tags: Annotated[list[Tag], Field(max_length=20)] = Field(default_factory=list)

    _tags = field_validator("tags")(_dedupe)


class SensorReplace(BaseModel):
    """PUT: full replacement of the editable fields; omitted optional fields reset to their defaults."""
    model_config = STRICT
    latitude: Lat
    longitude: Lon
    name: Optional[Name] = None
    description: Optional[Description] = None
    direction: Optional[Direction] = None
    lanes: Optional[Annotated[int, Field(ge=1, le=12)]] = None
    status: SensorStatus = "active"
    tags: Annotated[list[Tag], Field(max_length=20)] = Field(default_factory=list)

    _tags = field_validator("tags")(_dedupe)


class SensorUpdate(BaseModel):
    """PATCH: only the fields present in the body change; `null` clears name / description / direction / lanes."""
    model_config = STRICT
    latitude: Optional[Lat] = None
    longitude: Optional[Lon] = None
    name: Optional[Name] = None
    description: Optional[Description] = None
    direction: Optional[Direction] = None
    lanes: Optional[Annotated[int, Field(ge=1, le=12)]] = None
    status: Optional[SensorStatus] = None
    tags: Optional[Annotated[list[Tag], Field(max_length=20)]] = None

    _tags = field_validator("tags")(_dedupe)

    @model_validator(mode="after")
    def _check(self) -> "SensorUpdate":
        if not self.model_fields_set:
            raise ValueError("provide at least one field to update")
        for f in ("latitude", "longitude", "status", "tags"):
            if f in self.model_fields_set and getattr(self, f) is None:
                raise ValueError(f"{f} cannot be null")
        return self


class SensorOut(BaseModel):
    model_config = ConfigDict(extra="ignore")
    sensor_id: int
    name: str | None
    description: str | None
    direction: str | None
    lanes: int | None
    status: str
    tags: list[str]
    latitude: float
    longitude: float
    stats: dict[str, Any] = Field(default_factory=dict, description="Read-only dataset statistics (refreshed by the loader)")
    source: str
    created_at: datetime
    updated_at: datetime
    version: int
    hotspot: dict[str, Any] | None = Field(None, description="Q5/Q9 hotspot summary (single-sensor reads only)")


class SensorNearOut(SensorOut):
    distance_m: float


# ------------------------------------------------------------------------------------------------ settings
class SettingCreate(BaseModel):
    model_config = STRICT
    key: Annotated[str, Field(pattern=KEY_PATTERN.pattern)]
    value: Any
    description: Optional[Annotated[str, StringConstraints(strip_whitespace=True, max_length=300)]] = None


class SettingPut(BaseModel):
    model_config = STRICT
    value: Any
    description: Optional[Annotated[str, StringConstraints(strip_whitespace=True, max_length=300)]] = None


class SettingPatch(BaseModel):
    model_config = STRICT
    value: Any = None
    description: Optional[Annotated[str, StringConstraints(strip_whitespace=True, max_length=300)]] = None

    @model_validator(mode="after")
    def _check(self) -> "SettingPatch":
        if not self.model_fields_set:
            raise ValueError("provide value and/or description")
        return self


class SettingOut(BaseModel):
    key: str
    value: Any
    known: bool
    category: str
    description: str | None
    type: str
    default: Any = None
    minimum: float | None = None
    maximum: float | None = None
    choices: list[str] | None = None
    is_default: bool = Field(description="True when no override is stored (the value is the registry default)")
    version: int | None
    created_at: datetime | None
    updated_at: datetime | None
    updated_by: str | None


# ------------------------------------------------------------------------------------------------ hotspots & predictions
class HotspotOut(BaseModel):
    sensor_id: int
    rank: int
    hotspot_class: str
    is_hotspot: bool
    persistence: float | None
    mean_daily_congested_min: float | None
    max_daily_congested_min: float | None
    workdays: int | None
    csbi: float | None
    csbi_rank: int | None
    tti_peak: float | None
    latitude: float
    longitude: float


class PredictionOut(BaseModel):
    sensor_id: int
    slot: int
    event_ts: datetime
    target_ts: datetime
    speed_mph: float
    probability: float
    alert: bool = Field(description="probability >= the effective alert threshold (analyst setting `alert_threshold`)")
    stored_alert: bool = Field(description="alert flag stored by the streaming job (model default threshold)")
    model: str | None
    is_imputed: bool | None = None


class LatestPredictions(BaseModel):
    slot: int | None
    event_ts: datetime | None
    threshold: float
    total: int
    limit: int
    offset: int
    items: list[PredictionOut]


# ------------------------------------------------------------------------------------------------ model inference
_features: dict[str, Any] = {c: (Optional[float], None) for c in FEATURE_COLUMNS}
FeatureValues = create_model("FeatureValues", __config__=ConfigDict(extra="forbid"), **_features)
FeatureValues.__doc__ = "The 41 Phase-3 features (all optional; missing values are median-imputed by the model, `speed` is required)."


class PredictInstance(BaseModel):
    model_config = STRICT
    sensor_id: Optional[SensorId] = None
    features: FeatureValues  # type: ignore[valid-type]

    @field_validator("features")
    @classmethod
    def _need_speed(cls, v):
        if v.speed is None:
            raise ValueError("features.speed is required")
        return v


class PredictRequest(BaseModel):
    model_config = STRICT
    instances: Annotated[list[PredictInstance], Field(min_length=1)]
    threshold: Optional[Annotated[float, Field(ge=0.01, le=0.99)]] = Field(None, description="Overrides the analyst setting for this call")


class PredictionResult(BaseModel):
    index: int
    sensor_id: int | None
    probability: float
    alert: bool
    imputed_features: int = Field(description="Number of null features the pipeline's Imputer filled")


class PredictResponse(BaseModel):
    model: str
    threshold: float
    threshold_source: Literal["request", "settings", "default"]
    count: int
    inference_ms: float
    cold_start: bool = Field(description="True when this call had to load Spark and the model")
    predictions: list[PredictionResult]
