"""The 7-stage MLlib pipeline and the hyper-parameter grids.

  1 Imputer            median imputation of the numeric features (medians learned on the training sample only)
  2 StringIndexer      hour_cat, dow_cat, speed_state -> index columns (unseen / missing -> extra "keep" bucket)
  3 OneHotEncoder      index columns -> one-hot vectors
  4 VectorAssembler    imputed numeric columns -> one vector
  5 StandardScaler     zero-mean / unit-variance numeric vector
  6 VectorAssembler    scaled numeric vector + one-hot vectors -> `features`
  7 Estimator          LogisticRegression | DecisionTreeClassifier | RandomForestClassifier (class-weighted)

`prepare()` derives the three string categoricals from raw feature columns; it is a pure row-wise function, so it is
applied identically at training, evaluation and inference time (model_io.predict calls it).
"""
from __future__ import annotations

from itertools import product

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import Imputer, OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.modeling.data import WEIGHT
from src.preprocessing.build_features import FEATURE_COLUMNS, LABEL
from src.preprocessing.congestion import CONGESTION_SPEED_MPH, FREE_FLOW_SPEED_MPH

CATEGORICAL = ["hour_cat", "dow_cat", "speed_state"]
# hour / day_of_week are represented by their categorical form (plus the cyclical sin/cos features).
NUMERIC = [c for c in FEATURE_COLUMNS if c not in ("hour", "day_of_week")]
N_STAGES = 7
SEED = 42


def prepare(df: DataFrame) -> DataFrame:
    """Add the string categorical columns (null stays null so the indexer can bucket it as 'unknown')."""
    speed = F.col("speed")
    return (df
            .withColumn("hour_cat", F.when(F.col("hour").isNotNull(), F.format_string("h%02d", F.col("hour").cast("int"))))
            .withColumn("dow_cat", F.when(F.col("day_of_week").isNotNull(), F.format_string("d%d", F.col("day_of_week").cast("int"))))
            .withColumn("speed_state", F.when(speed.isNull() | F.isnan(speed), F.lit(None).cast("string"))
                                        .when(speed < CONGESTION_SPEED_MPH, "congested")
                                        .when(speed < FREE_FLOW_SPEED_MPH, "slowed")
                                        .otherwise("free")))


def build_pipeline(estimator) -> Pipeline:
    stages = [
        Imputer(strategy="median", inputCols=NUMERIC, outputCols=[f"{c}_imp" for c in NUMERIC]),
        StringIndexer(inputCols=CATEGORICAL, outputCols=[f"{c}_idx" for c in CATEGORICAL], handleInvalid="keep"),
        OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAL], outputCols=[f"{c}_ohe" for c in CATEGORICAL],
                      handleInvalid="keep", dropLast=True),
        VectorAssembler(inputCols=[f"{c}_imp" for c in NUMERIC], outputCol="numeric_vec", handleInvalid="error"),
        StandardScaler(inputCol="numeric_vec", outputCol="numeric_scaled", withMean=True, withStd=True),
        VectorAssembler(inputCols=["numeric_scaled"] + [f"{c}_ohe" for c in CATEGORICAL], outputCol="features",
                        handleInvalid="error"),
        estimator,
    ]
    assert len(stages) == N_STAGES
    return Pipeline(stages=stages)


def make_estimator(algo: str, params: dict):
    common = dict(labelCol=LABEL, featuresCol="features", weightCol=WEIGHT)
    if algo == "logistic_regression":
        # inputs are already standardized by stage 5
        return LogisticRegression(**common, maxIter=params.get("maxIter", 60), regParam=params["regParam"],
                                  elasticNetParam=params.get("elasticNetParam", 0.0), standardization=False)
    if algo == "decision_tree":
        return DecisionTreeClassifier(**common, maxDepth=params["maxDepth"],
                                      minInstancesPerNode=params["minInstancesPerNode"], seed=SEED)
    if algo == "random_forest":
        return RandomForestClassifier(**common, numTrees=params["numTrees"], maxDepth=params["maxDepth"],
                                      minInstancesPerNode=params.get("minInstancesPerNode", 50),
                                      subsamplingRate=0.7, featureSubsetStrategy="sqrt", seed=SEED)
    raise ValueError(f"unknown algorithm {algo!r}")


def _grid(**axes) -> list[dict]:
    return [dict(zip(axes, values)) for values in product(*axes.values())]


def param_grids(quick: bool = False) -> dict[str, list[dict]]:
    if quick:
        return {
            "logistic_regression": _grid(regParam=[1e-3, 1e-2]),
            "decision_tree": _grid(maxDepth=[6, 10], minInstancesPerNode=[50]),
            "random_forest": _grid(numTrees=[15], maxDepth=[8, 10]),
        }
    return {
        "logistic_regression": _grid(regParam=[1e-4, 1e-3, 1e-2]),
        "decision_tree": _grid(maxDepth=[6, 10], minInstancesPerNode=[50, 200]),
        "random_forest": _grid(numTrees=[30, 60], maxDepth=[8, 12]),
    }
