"""Loading of the static/historical inputs of the streaming application and of the replay events (pandas/pyarrow, no JVM)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from src.common.paths import PROCESSED_DIR, RAW_DIR, processed_dataset_dir, raw_dataset_dir
from src.preprocessing.download_dataset import load_verified_manifest
from src.preprocessing.raw_reader import safe_load_adjacency
from src.streaming.config import HISTORY_SLOTS, SLOT_SECONDS
from src.streaming.live_features import LiveFeatureEngine

EPOCH = pd.Timestamp("1970-01-01")


def to_slot(ts: pd.Series) -> np.ndarray:
    return ((ts - EPOCH) // pd.Timedelta(seconds=SLOT_SECONDS)).to_numpy().astype(np.int64)


def meta(dataset: str = "pems-bay") -> dict:
    return json.loads((processed_dataset_dir(dataset, PROCESSED_DIR) / "features_meta.json").read_text())


def clean_rows(start: datetime, end: datetime, dataset: str = "pems-bay", observed_only: bool = False) -> pd.DataFrame:
    """Cleaned grid rows with `start <= ts_utc < end` as (sensor_id, ts_utc, slot, speed_mph, quality)."""
    base = processed_dataset_dir(dataset, PROCESSED_DIR) / "clean"
    months, cur = [], datetime(start.year, start.month, 1) - timedelta(days=1)
    while cur < end:                                           # months whose UTC range may overlap (local-time partitions)
        months.append(cur.strftime("%Y-%m"))
        cur = datetime(cur.year + (cur.month == 12), cur.month % 12 + 1, 1)
    months = sorted(set(months) | {start.strftime("%Y-%m"), (end - timedelta(seconds=1)).strftime("%Y-%m")})
    existing = [m for m in months if (base / f"month={m}").exists()]
    df = pd.read_parquet(base, columns=["sensor_id", "ts_utc", "speed_mph", "quality"], filters=[("month", "in", existing)])
    df = df[(df.ts_utc >= pd.Timestamp(start)) & (df.ts_utc < pd.Timestamp(end))]
    if observed_only:
        df = df[(df.quality == "observed") & df.speed_mph.notna()]
    df = df.assign(slot=to_slot(df.ts_utc)).sort_values(["slot", "sensor_id"], kind="stable").reset_index(drop=True)
    return df


def history_before(first_slot: int, dataset: str = "pems-bay") -> pd.DataFrame:
    """Cleaned history for the HISTORY_SLOTS slots preceding the first streamed slot (the 'historical store')."""
    first = datetime(1970, 1, 1) + timedelta(seconds=first_slot * SLOT_SECONDS)
    return clean_rows(first - timedelta(seconds=HISTORY_SLOTS * SLOT_SECONDS), first, dataset)


def engine_inputs(dataset: str = "pems-bay") -> dict:
    base = processed_dataset_dir(dataset, PROCESSED_DIR)
    ds_raw = raw_dataset_dir(dataset, RAW_DIR)
    ids, _, adj = safe_load_adjacency(ds_raw / load_verified_manifest(ds_raw)["adjacency_pkl"])
    return {"sensor_ids": [int(s) for s in ids], "adjacency": np.array(adj, dtype=np.float64),
            "static": pd.read_parquet(base / "sensor_stats"), "profile": pd.read_parquet(base / "slot_profile"),
            "timezone_name": meta(dataset)["boundaries"]["local_timezone"]}


def build_engine(dataset: str = "pems-bay", grace_slots: int = 1, n_partitions: int = 1) -> LiveFeatureEngine:
    return LiveFeatureEngine(**engine_inputs(dataset), grace_slots=grace_slots, n_partitions=n_partitions)
