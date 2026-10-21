"""Phase 6 tests: modeling layer (histogram evaluation, transition metrics, bootstrap, weights, hold-out split).

Run:  pytest -q tests/test_modeling.py
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import functions as F

from src.common.spark import get_spark
from src.modeling import evaluation as ev
from src.modeling.data import sensor_holdout, with_class_weights


@pytest.fixture(scope="module")
def spark():
    s = get_spark("trafficsense-tests-modeling", driver_memory="1g", shuffle_partitions=4)
    yield s
    s.stop()


def _hist(rows):
    """rows: (congested_now, label, bucket, n[, date])"""
    df = pd.DataFrame(rows, columns=["congested_now", "label", "bucket", "n", "date_local"][: len(rows[0])])
    if "date_local" not in df:
        df["date_local"] = date(2017, 6, 1)
    return df


def test_perfect_classifier():
    h = _hist([(0, 0, 0, 900), (1, 1, ev.BINS - 1, 100)])
    c = ev.curve(h)
    pr, roc = ev.auc(c)
    assert pr == pytest.approx(1.0) and roc == pytest.approx(1.0)
    m = ev.metrics(h, ev.best_bucket(c))
    assert m["f1"] == pytest.approx(1.0) and m["precision"] == 1 and m["recall"] == 1


def test_random_classifier_auc():
    rng = np.random.default_rng(0)
    b = rng.integers(0, ev.BINS, size=20000)
    y = rng.random(20000) < 0.1
    h = pd.DataFrame({"congested_now": 0, "label": y.astype(int), "bucket": b, "n": 1, "date_local": date(2017, 6, 1)})
    pr, roc = ev.auc(ev.curve(h))
    assert abs(roc - 0.5) < 0.03 and abs(pr - 0.1) < 0.02


def test_transition_metrics():
    hi, lo = ev.BINS - 1, 0
    h = _hist([
        (0, 1, hi, 30), (0, 1, lo, 70),     # 100 onsets, 30 predicted
        (1, 0, lo, 40), (1, 0, hi, 10),     # 50 clearances, 40 predicted correctly
        (1, 1, hi, 200),                    # staying congested, predicted
        (0, 0, lo, 990), (0, 0, hi, 10),    # stable free, 10 false alarms
    ])
    m = ev.metrics(h, ev.BINS // 2)
    assert m["onset_recall"] == pytest.approx(0.30)
    assert m["clearance_accuracy"] == pytest.approx(0.80)
    assert m["transition_accuracy"] == pytest.approx((30 + 40) / 150)
    assert m["false_alarm_rate"] == pytest.approx(10 / 1000)
    assert m["recall"] == pytest.approx(230 / 300)


def test_persistence_scores_zero_on_transitions():
    # persistence: p = congested_now
    h = _hist([(0, 1, 0, 50), (1, 0, ev.BINS - 1, 20), (1, 1, ev.BINS - 1, 100), (0, 0, 0, 800)])
    m = ev.metrics(h, ev.BINS // 2)
    assert m["onset_recall"] == 0 and m["clearance_accuracy"] == 0 and m["transition_accuracy"] == 0


def test_bootstrap_identical_models_have_zero_delta():
    daily = pd.DataFrame({"date_local": pd.date_range("2017-06-01", periods=20).date,
                          "tp": np.arange(20) + 10, "fp": 5, "fn": 7, "tn": 100})
    out = ev.bootstrap_f1({"a": daily, "b": daily.copy()}, "a", n_boot=200).set_index("model")
    assert out.loc["b", "delta_vs_a"] == 0 and out.loc["b", "delta_ci_low"] == 0 and out.loc["b", "delta_ci_high"] == 0
    assert out.loc["a", "f1_ci_low"] <= out.loc["a", "f1"] <= out.loc["a", "f1_ci_high"]


def test_metrics_by_multiple_keys():
    h = _hist([(0, 1, ev.BINS - 1, 5, date(2017, 6, 1)), (0, 0, 0, 5, date(2017, 6, 2))])
    h["split"] = ["val", "test"]
    out = ev.metrics_by(h, ev.BINS // 2, ["split", "date_local"])
    assert set(out.columns) >= {"split", "date_local", "f1"} and len(out) == 2


def test_histogram_and_class_weights(spark):
    rows = [(i % 7, i % 10 == 0, i / 1000.0) for i in range(1000)]
    df = spark.createDataFrame(rows, "congested_now int, label boolean, p double").withColumn("label", F.col("label").cast("int")) \
              .withColumn("date_local", F.lit("2017-06-01").cast("date"))
    h = ev.histogram(df)
    assert h.n.sum() == 1000 and h.bucket.between(0, ev.BINS - 1).all()
    w, info = with_class_weights(df)
    tot = {r.label: r.w for r in w.groupBy("label").agg(F.sum("w").alias("w")).collect()}
    assert tot[0] == pytest.approx(tot[1]) and info["positives"] == 100


def test_sensor_holdout_is_deterministic_and_about_20pct(spark):
    df = spark.range(400000, 400400).withColumnRenamed("id", "sensor_id")
    a = df.filter(sensor_holdout()).count()
    b = df.filter(sensor_holdout()).count()
    assert a == b and 40 <= a <= 120
