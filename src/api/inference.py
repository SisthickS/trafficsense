"""Saved-model inference for the API: the Phase 7 Spark PipelineModel, loaded lazily on the first prediction request.

Spark and the model are loaded once (guarded by a lock) in a small local session; scoring runs the same 7-stage pipeline as
training/streaming, so probabilities are identical to the batch and streaming paths. A cold start takes several seconds; a
warm call on a handful of rows ~0.3-1 s. For high-throughput serving, export the model to a lighter runtime.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path

from pyspark.sql import types as T

from src.api.config import ApiSettings
from src.preprocessing.build_features import FEATURE_COLUMNS

log = logging.getLogger("trafficsense.api")


class InferenceUnavailable(RuntimeError):
    pass


class ModelService:
    def __init__(self, settings: ApiSettings):
        self.settings = settings
        self._lock = threading.Lock()
        self._bundle = None
        self._spark = None
        self.load_seconds: float | None = None
        self.predictions_served = 0

    @property
    def loaded(self) -> bool:
        return self._bundle is not None

    def info(self) -> dict:
        """Bundle metadata read from disk (does not start Spark)."""
        meta_file = Path(self.settings.model_path) / "bundle.json"
        if not meta_file.exists():
            return {"available": False, "path": str(self.settings.model_path), "loaded": False}
        meta = json.loads(meta_file.read_text())
        return {"available": True, "loaded": self.loaded, "path": str(self.settings.model_path), "algorithm": meta.get("algorithm"),
                "label": meta.get("label"), "params": meta.get("params"), "default_threshold": meta.get("threshold"),
                "threshold_rule": meta.get("threshold_rule"), "stages": meta.get("stages"), "spark_version": meta.get("spark_version"),
                "saved_at": meta.get("saved_at"), "load_seconds": self.load_seconds, "predictions_served": self.predictions_served,
                "features": FEATURE_COLUMNS}

    def default_threshold(self) -> float | None:
        meta_file = Path(self.settings.model_path) / "bundle.json"
        return float(json.loads(meta_file.read_text())["threshold"]) if meta_file.exists() else None

    def _ensure_loaded(self) -> bool:
        """Returns True when this call performed the (cold) load."""
        if self._bundle is not None:
            return False
        with self._lock:
            if self._bundle is not None:
                return False
            if not self.settings.enable_model_inference:
                raise InferenceUnavailable("model inference is disabled (TRAFFICSENSE_ENABLE_MODEL_INFERENCE=false)")
            if not (Path(self.settings.model_path) / "bundle.json").exists():
                raise InferenceUnavailable(f"no saved model at {self.settings.model_path} (run scripts/run_phase7.sh)")
            t0 = time.time()
            os.environ.setdefault("SPARK_MASTER", "local[2]")
            from src.common.spark import get_spark
            from src.ml.model_io import load_model
            self._spark = get_spark("trafficsense-api", driver_memory=self.settings.spark_driver_memory, shuffle_partitions=4,
                                    extra_conf={"spark.ui.enabled": "false"})
            self._bundle = load_model(self.settings.model_path)
            self.load_seconds = round(time.time() - t0, 2)
            log.info("model loaded: %s in %.1fs", self._bundle.metadata.get("label"), self.load_seconds)
            return True

    def predict(self, rows: list[dict]) -> tuple[list[float], bool]:
        """rows: dicts with the 41 feature keys (None allowed). Returns (probabilities in input order, cold_start)."""
        cold = self._ensure_loaded()
        from src.ml.model_io import predict
        schema = T.StructType([T.StructField("row_id", T.IntegerType(), False)] + [T.StructField(c, T.DoubleType(), True) for c in FEATURE_COLUMNS])
        data = [(i, *[None if r.get(c) is None else float(r[c]) for c in FEATURE_COLUMNS]) for i, r in enumerate(rows)]
        with self._lock:                                        # one Spark job at a time keeps memory predictable
            df = self._spark.createDataFrame(data, schema)
            out = predict(self._bundle, df).select("row_id", "p").orderBy("row_id").collect()
            self.predictions_served += len(out)
        return [float(r["p"]) for r in out], cold

    def warm_up(self) -> None:
        """Load Spark and the model and score one dummy row (JIT / codegen), so the first real request is fast."""
        self.predict([{"speed": 60.0, "hour": 12.0, "day_of_week": 3.0}])
        self.predictions_served -= 1

    def model_label(self) -> str:
        return (self._bundle.metadata.get("label") if self._bundle else self.info().get("label")) or "model"

    def close(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = self._bundle = None
