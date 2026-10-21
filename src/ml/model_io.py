"""Model serialization: save a fitted PipelineModel with its metadata, reload it, verify, and run inference.

Bundle layout (a directory):
    model/            Spark PipelineModel (stages/, metadata/) - written with PipelineModel.write()
    bundle.json       algorithm, hyper-parameters, operating thresholds, feature schema, stage classes, metrics
    reference.json    probabilities of a deterministic row sample, recorded at save time (used by verify_reload)

Downstream inference:
    bundle = load_model(path)
    scored = predict(bundle, features_df)        # adds probability `p` and the 0/1 alert `alert`
"""
from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyspark
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.ml.pipeline import CATEGORICAL, N_STAGES, NUMERIC, prepare
from src.preprocessing.build_features import LABEL

KEYS = ["sensor_id", "ts_utc"]
REFERENCE_MODULO = 10_000


@dataclass
class ModelBundle:
    model: PipelineModel
    metadata: dict
    path: Path

    @property
    def threshold(self) -> float:
        return float(self.metadata["threshold"])


def _probability(model: PipelineModel, df: DataFrame, keep: list[str]) -> DataFrame:
    return model.transform(prepare(df)).select(*keep, vector_to_array("probability")[1].alias("p"))


def _reference_rows(df: DataFrame, modulo: int = REFERENCE_MODULO) -> DataFrame:
    """Deterministic, hash-selected row sample (~1 in `modulo`) so the same rows can be re-scored after reload."""
    return df.filter(F.abs(F.hash(*[F.col(k).cast("string") for k in KEYS])) % modulo == 0)


def _keyed(df: DataFrame) -> DataFrame:
    return df.select(F.col("sensor_id").cast("string").alias("sensor_id"), F.col("ts_utc").cast("string").alias("ts_utc"), "p")


def save_model(model: PipelineModel, path: Path, metadata: dict, reference: DataFrame | None = None,
               reference_modulo: int = REFERENCE_MODULO) -> Path:
    path = Path(path)
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    model.write().overwrite().save(str(path / "model"))
    meta = {
        **metadata,
        "stages": [type(s).__name__ for s in model.stages],
        "numeric_features": NUMERIC, "categorical_features": CATEGORICAL, "target_column": LABEL,
        "reference_modulo": reference_modulo,
        "spark_version": pyspark.__version__, "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (path / "bundle.json").write_text(json.dumps(meta, indent=2, default=str) + "\n")
    if reference is not None:
        rows = _keyed(_probability(model, _reference_rows(reference, reference_modulo), KEYS)).toPandas()
        (path / "reference.json").write_text(rows.to_json(orient="records"))
    return path


def load_model(path: Path) -> ModelBundle:
    path = Path(path)
    if not (path / "bundle.json").exists() or not (path / "model" / "metadata").exists():
        raise FileNotFoundError(f"{path} is not a saved model bundle (missing bundle.json or model/metadata)")
    meta = json.loads((path / "bundle.json").read_text())
    return ModelBundle(PipelineModel.load(str(path / "model")), meta, path)


def update_metadata(path: Path, **updates) -> None:
    f = Path(path) / "bundle.json"
    meta = json.loads(f.read_text())
    meta.update(updates)
    f.write_text(json.dumps(meta, indent=2, default=str) + "\n")


def predict(bundle: ModelBundle, df: DataFrame, threshold: float | None = None) -> DataFrame:
    """Score a Phase-3 feature frame: original columns + `p` (probability of congestion at t+5 min) + `alert`."""
    t = bundle.threshold if threshold is None else threshold
    scored = bundle.model.transform(prepare(df)).withColumn("p", vector_to_array("probability")[1])
    drop = [c for c in scored.columns if c.endswith(("_imp", "_idx", "_ohe")) or c in
            ("numeric_vec", "numeric_scaled", "features", "rawPrediction", "probability", "prediction")]
    return scored.drop(*drop).withColumn("alert", (F.col("p") >= F.lit(t)).cast("int"))


def verify_reload(path: Path, df: DataFrame, tolerance: float = 1e-9) -> dict:
    """Reload the saved bundle and prove it reproduces the probabilities recorded at save time."""
    bundle = load_model(path)
    checks = {}
    checks["stage_count"] = len(bundle.model.stages) == N_STAGES
    checks["stage_types_match_metadata"] = [type(s).__name__ for s in bundle.model.stages] == bundle.metadata["stages"]
    ref_file = Path(path) / "reference.json"
    if not ref_file.exists():
        return {"ok": False, "checks": checks, "error": "reference.json missing"}
    import pandas as pd
    expected = pd.read_json(ref_file, orient="records", dtype={"sensor_id": str, "ts_utc": str})
    modulo = int(bundle.metadata.get("reference_modulo", REFERENCE_MODULO))
    got = _keyed(_probability(bundle.model, _reference_rows(df, modulo), KEYS)).toPandas()
    merged = expected.merge(got, on=KEYS, suffixes=("_saved", "_reloaded"))
    max_diff = float((merged.p_saved - merged.p_reloaded).abs().max()) if len(merged) else float("nan")
    checks["reference_rows_found"] = len(merged) == len(expected) > 0
    checks["probabilities_identical"] = bool(len(merged)) and max_diff <= tolerance
    scored = predict(bundle, _reference_rows(df, modulo))
    checks["predict_adds_alert"] = {"p", "alert"} <= set(scored.columns) and scored.filter(F.col("p").isNull()).count() == 0
    return {"ok": all(checks.values()), "checks": checks, "reference_rows": int(len(expected)),
            "max_abs_probability_diff": max_diff, "threshold": bundle.threshold, "stages": bundle.metadata["stages"]}
