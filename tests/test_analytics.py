"""Phase 5 tests: analytics helpers and the numeric cores of Q3 (episodes) and Q7 (lagged association).

Run:  pytest -q tests/test_analytics.py
"""
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import Row

from src.analytics import q7_propagation as q7
from src.analytics.common import AnalyticsContext, df_to_md, eta_squared, hhmm, minmax, spearman
from src.common.spark import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("trafficsense-tests-analytics", driver_memory="1g", shuffle_partitions=4)
    yield s
    s.stop()


def test_helpers():
    assert hhmm(69) == "17:15"
    assert list(minmax(pd.Series([2.0, 4.0, 6.0]))) == [0.0, 0.5, 1.0]
    assert spearman([1, 2, 3, 4], [10, 20, 30, 40]) == pytest.approx(1.0)
    assert spearman([1, 2, 3, 4], [4, 3, 2, 1]) == pytest.approx(-1.0)
    df = pd.DataFrame({"g": ["a", "a", "b", "b"], "v": [1.0, 1.0, 3.0, 3.0]})
    assert eta_squared(df, "g", "v") == pytest.approx(1.0)
    md = df_to_md(pd.DataFrame({"x": [1, 2], "y": [0.5, float("nan")]}))
    assert "| 1 | 0.500 |" in md and "| 2 | - |" in md


def test_lagged_corr_recovers_known_lag():
    rng = np.random.default_rng(0)
    T = 3000
    up = rng.normal(size=T + 10)
    down = np.roll(up, 2) + 0.1 * rng.normal(size=T + 10)        # downstream copies upstream 2 steps later
    Z = np.stack([up, down], axis=1)[5:T + 5].astype(np.float32)
    rows = np.arange(20, T - 20)
    R = q7._lagged_corr(Z, rows, np.array([0]), np.array([1]))
    assert q7.LAGS[np.nanargmax(R[:, 0])] == 2
    assert R[q7.LAGS == 2][0, 0] > 0.95 and abs(R[q7.LAGS == 0][0, 0]) < 0.1


def test_conditional_probability_after_onset():
    T = 200
    C = np.zeros((T, 2), dtype=np.float32)
    C[100:110, 0] = 1                  # onset of sensor 0 at t=100
    C[103:115, 1] = 1                  # sensor 1 congested from t=103
    prev = np.vstack([np.full((1, 2), np.nan, dtype=np.float32), C[:-1]])
    O = ((C == 1) & (prev == 0)).astype(np.float32)
    rows = np.arange(20, 180)
    p = q7._conditional(C, O, rows, trig=np.array([0]), resp=np.array([1]))
    lag = dict(zip(q7.LAGS.tolist(), p))
    assert lag[0] == 0 and lag[3] == 1 and lag[-1] == 0


def test_episodes_from_grid(spark):
    start = datetime(2017, 3, 6, 16, 0)                         # Monday 08:00 local (UTC-8)
    speeds = [65, 64, 30, 25, 20, 32, 50, 58, 61, 66, 33, None, 34, 65]
    rows = [Row(sensor_id=1, ts_utc=start + timedelta(minutes=5 * i), ts_local=start - timedelta(hours=8) + timedelta(minutes=5 * i),
                speed=None if v is None else float(v), is_workday=True, dow=1) for i, v in enumerate(speeds)]
    ctx = AnalyticsContext(spark)
    ctx._cache["grid"] = spark.createDataFrame(rows)
    ep = ctx.episodes().sort_values("start_utc").reset_index(drop=True)
    assert list(ep.duration_min) == [20, 5, 5]                 # 30,25,20,32 | 33 | (missing breaks) 34
    first = ep.iloc[0]
    assert first.min_speed == 20 and first.speed_at_start == 30 and first.speed_at_end == 32
    assert first.speed_15m_before is None or np.isnan(first.speed_15m_before) or first.speed_15m_before == 65
    assert first.recovery_min == pytest.approx(10.0)          # end 08:25 -> +5 = 08:30; free (61) at 08:40
    assert first.start_hour == 8
