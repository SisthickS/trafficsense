"""Readers for the raw wide-format traffic files. Shared by validation and the Parquet build so both
interpret the raw data identically.

Raw layout (DCRNN CSV export):
    ,400001,400017,...          <- header: blank index label + one column per sensor id
    2017-01-01 00:00:00,71.4,...  <- naive local (Pacific) timestamp, speed in mph per sensor
"""
from __future__ import annotations

import pickle
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.preprocessing.sources import DatasetSpec

CORRUPT_COL = "_corrupt_record"
TS_RAW_COL = "timestamp"


def read_header(csv_path: Path) -> list[str]:
    with open(csv_path, "r", encoding="utf-8", newline="") as fh:
        return fh.readline().rstrip("\r\n").split(",")


def wide_schema(sensor_ids: list[str], with_corrupt: bool = True) -> T.StructType:
    fields = [T.StructField(TS_RAW_COL, T.StringType(), True)]
    fields += [T.StructField(sid, T.DoubleType(), True) for sid in sensor_ids]
    if with_corrupt:
        fields.append(T.StructField(CORRUPT_COL, T.StringType(), True))
    return T.StructType(fields)


def _quiet_csv_header_checker(spark: SparkSession) -> None:
    # The raw header's first cell is blank while our schema names it "timestamp"; Spark logs a long
    # warning listing all 326 columns. The header is validated explicitly (check C03), so mute it.
    try:
        jvm = spark.sparkContext._jvm
        jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
            "org.apache.spark.sql.catalyst.csv.CSVHeaderChecker", jvm.org.apache.logging.log4j.Level.ERROR)
    except Exception:
        pass


def load_wide(spark: SparkSession, csv_path: Path, sensor_ids: list[str]) -> DataFrame:
    """Read the wide CSV with an explicit schema (no inference pass over 326 columns).

    PERMISSIVE mode keeps malformed rows and stores their text in `_corrupt_record`.
    """
    _quiet_csv_header_checker(spark)
    return spark.read.csv(
        str(csv_path),
        header=True,
        schema=wide_schema(sensor_ids),
        mode="PERMISSIVE",
        columnNameOfCorruptRecord=CORRUPT_COL,
        enforceSchema=True,
    )


def to_long(wide: DataFrame, sensor_ids: list[str], spec: DatasetSpec) -> DataFrame:
    """Unpivot wide -> long: (ts, sensor_id, speed_mph). One row per sensor reading."""
    return (
        wide.select(TS_RAW_COL, *[F.col(f"`{s}`") for s in sensor_ids])
        .unpivot([TS_RAW_COL], sensor_ids, "sensor_id", "speed_mph")
        .select(
            F.to_timestamp(TS_RAW_COL, spec.timestamp_format).alias("ts"),
            F.col("sensor_id").cast(T.IntegerType()).alias("sensor_id"),
            F.col("speed_mph").cast(T.DoubleType()).alias("speed_mph"),
        )
    )


def load_locations(spark: SparkSession, path: Path, spec: DatasetSpec) -> DataFrame:
    if spec.locations_has_header:
        df = spark.read.csv(str(path), header=True, inferSchema=False)
        df = df.select("sensor_id", "latitude", "longitude")
    else:
        df = spark.read.csv(str(path), header=False).toDF("sensor_id", "latitude", "longitude")
    return df.select(
        F.col("sensor_id").cast(T.IntegerType()).alias("sensor_id"),
        F.col("latitude").cast(T.DoubleType()).alias("latitude"),
        F.col("longitude").cast(T.DoubleType()).alias("longitude"),
    )


class _RestrictedUnpickler(pickle.Unpickler):
    """Only lets the pickle rebuild plain numpy arrays and builtins, never arbitrary callables.

    The DCRNN adjacency pickles come from third parties; an unrestricted pickle.load could execute code.
    """

    _ALLOWED = {
        ("numpy", "ndarray"),
        ("numpy", "dtype"),
        ("numpy.core.multiarray", "_reconstruct"),
        ("numpy._core.multiarray", "_reconstruct"),
        ("numpy.core.multiarray", "scalar"),
        ("numpy._core.multiarray", "scalar"),
        ("copy_reg", "_reconstructor"),
        ("copyreg", "_reconstructor"),
        ("__builtin__", "object"),
        ("builtins", "object"),
        # Protocol-2 pickles written by Python 3 store raw bytes as _codecs.encode(str, "latin1").
        ("_codecs", "encode"),
    }

    def find_class(self, module: str, name: str):
        if (module, name) in self._ALLOWED:
            return super().find_class(module, name)
        raise pickle.UnpicklingError(f"blocked global in pickle: {module}.{name}")


def safe_load_adjacency(path: Path):
    """Return (sensor_ids: list[str], sensor_id_to_index: dict, adj_mx: numpy.ndarray)."""
    with open(path, "rb") as fh:
        obj = _RestrictedUnpickler(fh, encoding="latin1").load()
    sensor_ids, id_to_ind, adj = obj
    sensor_ids = [s.decode() if isinstance(s, bytes) else str(s) for s in sensor_ids]
    return sensor_ids, id_to_ind, adj
