"""Environment-based configuration (12-factor): TRAFFICSENSE_* variables, optionally from a `.env` file at the project root.

See `.env.example` for every variable. Real environment variables override the file. Secrets (API key) are never logged.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.common.paths import PROJECT_ROOT, REPORTS_DIR
from src.ml.config import ML_MODEL_DIR


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TRAFFICSENSE_", env_file=PROJECT_ROOT / ".env", env_file_encoding="utf-8", extra="ignore")

    env: Literal["development", "test", "production"] = "development"
    host: str = "127.0.0.1"
    port: int = Field(8000, ge=1, le=65535)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "trafficsense"
    mongo_timeout_ms: int = Field(3000, ge=100, le=60000)

    api_key: SecretStr | None = None                     # when set, every write (POST/PUT/PATCH/DELETE) needs `X-API-Key`
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"
    docs_enabled: bool = True                            # /docs, /redoc, /openapi.json
    max_page_size: int = Field(500, ge=1, le=5000)

    enable_model_inference: bool = True                  # POST /models/predict loads the Spark model lazily on first use
    preload_model: bool = False                          # load Spark + the model in a background thread at startup (avoids a ~30-60 s first request)
    model_path: Path = ML_MODEL_DIR / "best"
    spark_driver_memory: str = "1g"
    max_predict_instances: int = Field(1000, ge=1, le=10000)

    reports_dir: Path = REPORTS_DIR                      # figures served by /analytics/figures

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @model_validator(mode="after")
    def _production_guard(self) -> "ApiSettings":
        if self.env == "production":
            if self.api_key is None or len(self.api_key.get_secret_value()) < 16:
                raise ValueError("TRAFFICSENSE_API_KEY (>= 16 characters) is required when TRAFFICSENSE_ENV=production")
            if "*" in self.cors_origin_list:
                raise ValueError("TRAFFICSENSE_CORS_ORIGINS must list explicit origins in production")
        return self


@lru_cache
def get_settings() -> ApiSettings:
    return ApiSettings()
