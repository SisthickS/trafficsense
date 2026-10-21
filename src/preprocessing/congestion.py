"""Congestion definition: the single source of truth for batch features, MLlib, streaming and the API.

Threshold (documented in reports/preprocessing/<dataset>_feature_report.md):
  * Caltrans' congestion monitoring programme (HICOMP, now the Mobility Performance Report,
    https://dot.ca.gov/programs/traffic-operations/mpr) counts a freeway as congested when speeds are
    35 mph or less for 15 minutes or longer on a typical weekday. Caltrans PeMS also reports delay against
    35 mph and 60 mph speed thresholds.
  * Every PEMS-BAY sensor is freeway mainline with a free-flow speed (85th percentile) of 62-72 mph, so
    35 mph is roughly half of free-flow: a breakdown of flow, not just heavy-but-moving traffic.
Prediction target: congested(t + 5 min) = observed speed at the next 5-minute slot < 35 mph.
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

CONGESTION_SPEED_MPH = 35.0
FREE_FLOW_SPEED_MPH = 60.0
HORIZON_MINUTES = 5

LEVEL_FREE, LEVEL_SLOWED, LEVEL_CONGESTED = "free", "slowed", "congested"
LEVELS = {
    LEVEL_FREE: f">= {FREE_FLOW_SPEED_MPH:.0f} mph (no delay by the PeMS 60 mph threshold)",
    LEVEL_SLOWED: f"{CONGESTION_SPEED_MPH:.0f}-{FREE_FLOW_SPEED_MPH:.0f} mph (delayed but flowing)",
    LEVEL_CONGESTED: f"< {CONGESTION_SPEED_MPH:.0f} mph (Caltrans congestion threshold)",
}

SOURCES = [
    "Caltrans State Highway Congestion Monitoring Program (HICOMP) / Mobility Performance Report: congestion = "
    "freeway speeds of 35 mph or less lasting 15 minutes or longer on a typical weekday "
    "(https://dot.ca.gov/programs/traffic-operations/mpr).",
    "Caltrans PeMS performance measures: vehicle-hours of delay computed against 35 mph and 60 mph thresholds.",
]


def is_congested(speed: Column, threshold: float = CONGESTION_SPEED_MPH) -> Column:
    """1 if speed < threshold, 0 if speed >= threshold, null if speed is null."""
    return F.when(speed.isNull(), F.lit(None)).otherwise((speed < F.lit(threshold)).cast("int"))


def congestion_level(speed: Column) -> Column:
    return (F.when(speed.isNull(), F.lit(None).cast("string"))
             .when(speed < CONGESTION_SPEED_MPH, LEVEL_CONGESTED)
             .when(speed < FREE_FLOW_SPEED_MPH, LEVEL_SLOWED)
             .otherwise(LEVEL_FREE))
