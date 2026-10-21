"""Shared SparkSession factory tuned for a single WSL2 machine."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from src.common.paths import SCRATCH_DIR


def _ensure_java_env() -> None:
    # Allows `python -m ...` and notebooks to work even if ~/.trafficsense_env.sh was not sourced.
    jdk = Path.home() / "opt" / "jdk-17"
    if "JAVA_HOME" not in os.environ and jdk.exists():
        os.environ["JAVA_HOME"] = str(jdk)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark(
    app_name: str,
    driver_memory: str | None = None,
    shuffle_partitions: int = 16,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Create (or reuse) a local SparkSession.

    Timestamps in PEMS-BAY / METR-LA are naive Pacific wall-clock strings. The session time zone is
    fixed to UTC so Spark stores them verbatim and never shifts them across DST boundaries.
    """
    _ensure_java_env()
    local_dir = SCRATCH_DIR / "spark-local"
    local_dir.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName(app_name)
        .config("spark.driver.memory", driver_memory or os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
        .config("spark.local.dir", str(local_dir))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, str(value))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
