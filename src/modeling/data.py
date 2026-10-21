"""Data access for modeling: feature splits, feature groups, class-weighted training samples, artifact paths."""
from __future__ import annotations

from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.paths import DATA_DIR, PROCESSED_DIR, dataset_slug, processed_dataset_dir
from src.preprocessing.build_features import FEATURE_COLUMNS, FEATURES, LABEL

FEATURE_GROUPS: dict[str, list[str]] = {}
for _name, (_group, _desc) in FEATURES.items():
    FEATURE_GROUPS.setdefault(_group, []).append(_name)

# Columns kept next to the probability in scored data (for segmenting metrics without re-reading features).
EVAL_COLUMNS = ["sensor_id", "ts_utc", "ts_local", "date_local", LABEL, "congested_now", "hist_congested_rate_next",
                "hour", "day_of_week", "is_weekend", "is_holiday"]
WEIGHT = "w"


def features_path(dataset: str = "pems-bay") -> Path:
    return processed_dataset_dir(dataset, PROCESSED_DIR) / "features"


def model_dir(dataset: str = "pems-bay") -> Path:
    return DATA_DIR / "models" / dataset_slug(dataset)


def predictions_dir(dataset: str = "pems-bay") -> Path:
    return processed_dataset_dir(dataset, PROCESSED_DIR) / "predictions"


def load_split(spark: SparkSession, split: str, columns: list[str] | None = None, dataset: str = "pems-bay") -> DataFrame:
    path = features_path(dataset)
    if not path.exists():
        raise SystemExit(f"{path} not found - run scripts/run_phase3.sh first.")
    df = spark.read.parquet(str(path)).filter(F.col("split") == split)
    cols = columns or (EVAL_COLUMNS + [c for c in FEATURE_COLUMNS if c not in EVAL_COLUMNS])
    return df.select(*dict.fromkeys(cols))


def cached_split(spark: SparkSession, split: str, cache: dict, dataset: str = "pems-bay") -> DataFrame:
    """Validation/test frames are scored by several models; keep them cached once per run."""
    key = f"split:{split}"
    if key not in cache:
        df = load_split(spark, split, dataset=dataset).persist(StorageLevel.MEMORY_AND_DISK)
        df.count()
        cache[key] = df
    return cache[key]


def with_class_weights(df: DataFrame) -> tuple[DataFrame, dict]:
    """Add weight column w so both classes carry equal total weight (n / (2 * n_class))."""
    counts = {r[LABEL]: r["count"] for r in df.groupBy(LABEL).count().collect()}
    n, n_pos = sum(counts.values()), counts.get(1, 0)
    n_neg = n - n_pos
    w_pos, w_neg = n / (2.0 * max(n_pos, 1)), n / (2.0 * max(n_neg, 1))
    out = df.withColumn(WEIGHT, F.when(F.col(LABEL) == 1, F.lit(w_pos)).otherwise(F.lit(w_neg)))
    return out, {"rows": n, "positives": n_pos, "positive_rate": n_pos / n if n else 0.0, "w_pos": w_pos, "w_neg": w_neg}


def training_sample(spark: SparkSession, fraction: float, seed: int = 42, row_filter: Column | None = None,
                    dataset: str = "pems-bay") -> tuple[DataFrame, dict]:
    """Random sample of the TRAINING split (time-ordered split is preserved: no val/test rows), class-weighted, cached."""
    df = load_split(spark, "train", dataset=dataset)
    if row_filter is not None:
        df = df.filter(row_filter)
    df = df.sample(withReplacement=False, fraction=fraction, seed=seed)
    df, info = with_class_weights(df)
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    info.update(fraction=fraction, seed=seed)
    return df, info


def sensor_holdout(sensor_col: str = "sensor_id", share: float = 0.2, seed: int = 7) -> Column:
    """Deterministic pseudo-random sensor split: True for the held-out ~share of sensors."""
    return (F.abs(F.hash(F.col(sensor_col), F.lit(seed))) % 1000) < int(share * 1000)
