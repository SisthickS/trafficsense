"""Model specifications, training and scoring (Spark MLlib)."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Callable

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import DecisionTreeClassifier, GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.modeling.data import EVAL_COLUMNS, WEIGHT
from src.preprocessing.build_features import LABEL

PROB = "p"


@dataclass(frozen=True)
class ModelSpec:
    name: str
    label: str
    build: Callable[[], object]
    params: dict


def model_specs(quick: bool = False) -> dict[str, ModelSpec]:
    trees = 20 if quick else 60
    iters = 20 if quick else 60
    return {
        "logistic_regression": ModelSpec(
            "logistic_regression", "Logistic regression",
            lambda: LogisticRegression(labelCol=LABEL, weightCol=WEIGHT, maxIter=60, regParam=1e-3, elasticNetParam=0.0,
                                       standardization=True),
            {"maxIter": 60, "regParam": 1e-3, "standardization": True}),
        "decision_tree": ModelSpec(
            "decision_tree", "Decision tree",
            lambda: DecisionTreeClassifier(labelCol=LABEL, weightCol=WEIGHT, maxDepth=10, minInstancesPerNode=50, seed=42),
            {"maxDepth": 10, "minInstancesPerNode": 50}),
        "random_forest": ModelSpec(
            "random_forest", "Random forest",
            lambda: RandomForestClassifier(labelCol=LABEL, weightCol=WEIGHT, numTrees=trees, maxDepth=10, minInstancesPerNode=50,
                                           subsamplingRate=0.7, featureSubsetStrategy="sqrt", seed=42),
            {"numTrees": trees, "maxDepth": 10, "subsamplingRate": 0.7, "featureSubsetStrategy": "sqrt"}),
        "gbt": ModelSpec(
            "gbt", "Gradient-boosted trees",
            lambda: GBTClassifier(labelCol=LABEL, weightCol=WEIGHT, maxIter=iters, maxDepth=6, stepSize=0.1,
                                  subsamplingRate=0.8, seed=42),
            {"maxIter": iters, "maxDepth": 6, "stepSize": 0.1, "subsamplingRate": 0.8}),
    }


def compact_gbt(quick: bool = False) -> ModelSpec:
    """Smaller GBT used for the many re-trainings of Q17 (ablation) and Q18 (sensor hold-out)."""
    iters = 15 if quick else 30
    return ModelSpec("gbt_compact", "Gradient-boosted trees (compact)",
                     lambda: GBTClassifier(labelCol=LABEL, weightCol=WEIGHT, maxIter=iters, maxDepth=5, stepSize=0.15,
                                           subsamplingRate=0.8, seed=42),
                     {"maxIter": iters, "maxDepth": 5, "stepSize": 0.15, "subsamplingRate": 0.8})


def fit(spec: ModelSpec, train: DataFrame, feature_cols: list[str]) -> tuple[PipelineModel, float]:
    pipe = Pipeline(stages=[VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="error"),
                            spec.build()])
    t0 = time.time()
    model = pipe.fit(train)
    return model, time.time() - t0


def score(model: PipelineModel, df: DataFrame, keep: list[str] | None = None) -> DataFrame:
    keep = keep or [c for c in EVAL_COLUMNS if c in df.columns]
    return model.transform(df).select(*keep, vector_to_array("probability")[1].alias(PROB))


BASELINES: dict[str, tuple[str, str]] = {
    "persistence": ("Persistence (next = now)", "congested_now"),
    "historical_profile": ("Historical slot profile", "hist_congested_rate_next"),
}


def score_baseline(name: str, df: DataFrame, keep: list[str] | None = None) -> DataFrame:
    keep = keep or [c for c in EVAL_COLUMNS if c in df.columns]
    return df.select(*keep, F.col(BASELINES[name][1]).cast("double").alias(PROB))


def importances(model: PipelineModel, feature_cols: list[str]) -> dict[str, float]:
    est = model.stages[-1]
    if hasattr(est, "featureImportances"):
        imp = est.featureImportances.toArray()
        return {c: float(v) for c, v in zip(feature_cols, imp)}
    return {}


def save_json(path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n")
