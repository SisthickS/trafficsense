"""Chronological train / validation / test splitting with a purge gap (no random shuffling).

The time axis is split by fraction (default 70 / 10 / 20, the DCRNN convention for PEMS-BAY) and each
boundary is snapped to local midnight, so a split never cuts a day in half. Rows whose prediction target
would fall on the other side of a boundary are removed (purge gap >= forecast horizon).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from pyspark.sql import Column
from pyspark.sql import functions as F

SPLITS = ("train", "val", "test")


@dataclass(frozen=True)
class SplitConfig:
    train_frac: float = 0.70
    val_frac: float = 0.10
    purge_minutes: int = 60

    def __post_init__(self):
        if not (0 < self.train_frac < 1 and 0 < self.val_frac < 1 and self.train_frac + self.val_frac < 1):
            raise ValueError("train_frac and val_frac must be in (0, 1) and sum to < 1")


@dataclass(frozen=True)
class SplitBoundaries:
    """All instants are naive UTC datetimes (Spark session time zone is UTC)."""
    start: datetime
    val_start: datetime
    test_start: datetime
    end: datetime              # exclusive
    purge: timedelta
    local_timezone: str

    def local(self, instant: datetime) -> str:
        tz = ZoneInfo(self.local_timezone)
        return instant.replace(tzinfo=timezone.utc).astimezone(tz).strftime("%Y-%m-%d %H:%M")

    def to_dict(self) -> dict:
        fmt = "%Y-%m-%d %H:%M:%S"
        return {
            "utc": {k: getattr(self, k).strftime(fmt) for k in ("start", "val_start", "test_start", "end")},
            "local": {k: self.local(getattr(self, k)) for k in ("start", "val_start", "test_start", "end")},
            "purge_minutes": int(self.purge.total_seconds() // 60),
            "local_timezone": self.local_timezone,
        }


def _local_midnight_utc(day: date, tz: ZoneInfo) -> datetime:
    return datetime.combine(day, time()).replace(tzinfo=tz).astimezone(timezone.utc).replace(tzinfo=None)


def compute_boundaries(first_utc: datetime, last_utc: datetime, step_seconds: int, local_timezone: str,
                       cfg: SplitConfig = SplitConfig()) -> SplitBoundaries:
    tz = ZoneInfo(local_timezone)
    end = last_utc + timedelta(seconds=step_seconds)
    total = end - first_utc

    def snapped(frac: float) -> datetime:
        raw = (first_utc + total * frac).replace(tzinfo=timezone.utc).astimezone(tz)
        nearest = raw.date() + timedelta(days=1) if raw.hour >= 12 else raw.date()
        return _local_midnight_utc(nearest, tz)

    val_start = snapped(cfg.train_frac)
    test_start = snapped(cfg.train_frac + cfg.val_frac)
    if not first_utc < val_start < test_start < end:
        raise ValueError(f"degenerate split boundaries: {first_utc} {val_start} {test_start} {end}")
    return SplitBoundaries(first_utc, val_start, test_start, end, timedelta(minutes=cfg.purge_minutes), local_timezone)


def split_column(ts_utc: Column, b: SplitBoundaries) -> Column:
    return (F.when(ts_utc < F.lit(b.val_start), "train")
             .when(ts_utc < F.lit(b.test_start), "val")
             .otherwise("test"))


def in_purge_gap(ts_utc: Column, b: SplitBoundaries) -> Column:
    """True for rows in the last `purge` window before a boundary (their targets cross into the next split)."""
    return (((ts_utc >= F.lit(b.val_start - b.purge)) & (ts_utc < F.lit(b.val_start)))
            | ((ts_utc >= F.lit(b.test_start - b.purge)) & (ts_utc < F.lit(b.test_start))))


def train_mask(ts_utc: Column, b: SplitBoundaries) -> Column:
    """Rows allowed to contribute to statistics learned from data (training period minus purge)."""
    return ts_utc < F.lit(b.val_start - b.purge)
