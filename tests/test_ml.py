"""Phase 7 tests: 7-stage pipeline (imputation of missing values, categoricals), cost analysis, model serialization.

Run:  pytest -q tests/test_ml.py
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
from pyspark.ml.feature import (Imputer, ImputerModel, OneHotEncoder, OneHotEncoderModel, StandardScaler, StandardScalerModel,
                                StringIndexer, StringIndexerModel, VectorAssembler)
from pyspark.sql import functions as F

from src.common.spark import get_spark
from src.ml import costs
from src.ml.model_io import load_model, predict, save_model, update_metadata, verify_reload
from src.ml.pipeline import N_STAGES, NUMERIC, build_pipeline, make_estimator, param_grids, prepare
from src.modeling import evaluation as ev
from src.modeling.data import with_class_weights
from src.preprocessing.build_features import FEATURE_COLUMNS, LABEL


@pytest.fixture(scope="module")
def spark():
    s = get_spark("trafficsense-tests-ml", driver_memory="1g", shuffle_partitions=4)
    yield s
    s.stop()


@pytest.fixture(scope="module")
def frame(spark):
    rng = np.random.default_rng(0)
    n = 600
    pdf = pd.DataFrame({c: rng.normal(size=n) for c in FEATURE_COLUMNS})
    pdf["speed"] = rng.uniform(15, 75, n)
    pdf["hour"] = rng.integers(0, 24, n)
    pdf["day_of_week"] = rng.integers(1, 8, n)
    pdf["sensor_id"] = [f"s{i % 5}" for i in range(n)]
    pdf["ts_utc"] = pd.date_range("2017-03-01", periods=n, freq="5min")
    pdf[LABEL] = (pdf.speed < 35).astype(int)
    pdf["congested_now"] = (pdf.speed < 35).astype(int)
    df = spark.createDataFrame(pdf)
    df, _ = with_class_weights(df)
    return df


def test_pipeline_has_seven_stages_in_order(spark):
    pipe = build_pipeline(make_estimator("logistic_regression", {"regParam": 0.01}))
    kinds = [type(s) for s in pipe.getStages()]
    assert len(kinds) == N_STAGES == 7
    assert kinds[:6] == [Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, VectorAssembler]
    assert type(pipe.getStages()[-1]).__name__ == "LogisticRegression"


@pytest.mark.parametrize("algo,params", [
    ("logistic_regression", {"regParam": 0.01}),
    ("decision_tree", {"maxDepth": 4, "minInstancesPerNode": 5}),
    ("random_forest", {"numTrees": 5, "maxDepth": 4}),
])
def test_all_three_estimators_fit_and_predict(frame, algo, params):
    model = build_pipeline(make_estimator(algo, params)).fit(prepare(frame))
    assert [type(s) for s in model.stages[:5]] == [ImputerModel, StringIndexerModel, OneHotEncoderModel,
                                                   VectorAssembler, StandardScalerModel]
    out = model.transform(prepare(frame)).select("probability").collect()
    assert len(out) == 600 and all(abs(sum(r.probability) - 1) < 1e-9 for r in out)


def test_prepare_derives_categoricals_and_keeps_nulls(spark):
    df = spark.createDataFrame([(20.0, 7, 1), (50.0, 17, 7), (70.0, None, 3), (None, 3, None), (float("nan"), 4, 2)],
                               "speed double, hour int, day_of_week int")
    rows = prepare(df).collect()
    r = {x.speed: x for x in rows if x.speed is not None}
    assert (r[20.0].speed_state, r[50.0].speed_state, r[70.0].speed_state) == ("congested", "slowed", "free")
    assert r[20.0].hour_cat == "h07" and r[50.0].dow_cat == "d7" and r[70.0].hour_cat is None
    nulls = [x for x in rows if x.speed is None][0]
    assert nulls.speed_state is None and nulls.dow_cat is None
    assert [x for x in rows if x.speed is not None and x.speed != x.speed][0].speed_state is None   # NaN counts as missing


def test_imputer_repairs_missing_numeric_values(spark, frame):
    pdf = frame.toPandas()
    median_speed_ratio = pdf["speed_ratio_ff"].median()
    pdf.loc[pdf.index[:50], "speed_ratio_ff"] = np.nan
    pdf.loc[pdf.index[50:80], "latitude"] = np.nan
    dirty = spark.createDataFrame(pdf)
    model = build_pipeline(make_estimator("logistic_regression", {"regParam": 0.01})).fit(prepare(dirty))
    out = model.transform(prepare(dirty))
    assert out.filter(F.col("speed_ratio_ff_imp").isNull() | F.isnan("speed_ratio_ff_imp")).count() == 0
    filled = out.select("speed_ratio_ff", "speed_ratio_ff_imp").filter(F.isnan("speed_ratio_ff") | F.col("speed_ratio_ff").isNull()).first()
    assert filled.speed_ratio_ff_imp == pytest.approx(median_speed_ratio, abs=0.1)
    # a row with a missing value at inference time is scored, not rejected
    assert out.count() == 600


def test_unseen_and_missing_categories_are_scored(spark, frame):
    model = build_pipeline(make_estimator("decision_tree", {"maxDepth": 3, "minInstancesPerNode": 5})).fit(prepare(frame))
    odd = frame.limit(5).withColumn("hour", F.lit(None).cast("int")).withColumn("day_of_week", F.lit(9))
    assert model.transform(prepare(odd)).count() == 5


def test_grids_cover_each_algorithm():
    for quick in (True, False):
        g = param_grids(quick)
        assert set(g) == {"logistic_regression", "decision_tree", "random_forest"} and all(g.values())
    assert len(NUMERIC) == len(FEATURE_COLUMNS) - 2


# ---------------------------------------------------------------------------------------------------- serialization
def test_save_load_roundtrip_and_verification(spark, frame, tmp_path):
    model = build_pipeline(make_estimator("random_forest", {"numTrees": 5, "maxDepth": 4})).fit(prepare(frame))
    path = save_model(model, tmp_path / "bundle", {"algorithm": "random_forest", "threshold": 0.4}, reference=frame, reference_modulo=5)
    assert (path / "bundle.json").exists() and (path / "reference.json").exists() and (path / "model" / "metadata").exists()
    check = verify_reload(path, frame)
    assert check["ok"], check
    assert check["reference_rows"] > 20 and check["max_abs_probability_diff"] <= 1e-9
    bundle = load_model(path)
    assert bundle.threshold == 0.4 and len(bundle.model.stages) == 7
    scored = predict(bundle, frame)
    assert {"p", "alert"} <= set(scored.columns) and not {"features", "numeric_vec", "rawPrediction"} & set(scored.columns)
    agg = scored.agg(F.min("p"), F.max("p"), F.sum("alert")).first()
    assert 0 <= agg[0] <= agg[1] <= 1 and agg[2] > 0
    assert predict(bundle, frame, threshold=1.01).agg(F.sum("alert")).first()[0] == 0


def test_verify_reload_detects_tampering(spark, frame, tmp_path):
    model = build_pipeline(make_estimator("decision_tree", {"maxDepth": 3, "minInstancesPerNode": 5})).fit(prepare(frame))
    path = save_model(model, tmp_path / "b", {"threshold": 0.5}, reference=frame, reference_modulo=5)
    ref = json.loads((path / "reference.json").read_text())
    for r in ref:
        r["p"] = 1 - r["p"] if r["p"] != 0.5 else 0.1
    (path / "reference.json").write_text(json.dumps(ref))
    assert not verify_reload(path, frame)["ok"]


def test_load_rejects_non_bundle(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_model(tmp_path)


def test_update_metadata(spark, frame, tmp_path):
    model = build_pipeline(make_estimator("logistic_regression", {"regParam": 0.01})).fit(prepare(frame))
    path = save_model(model, tmp_path / "m", {"threshold": 0.5})
    update_metadata(path, role="best")
    assert load_model(path).metadata["role"] == "best"


# ---------------------------------------------------------------------------------------------------- costs
def _hist():
    # 100 congested rows: 90 scored high, 10 low; 900 free rows: 90 scored high, 810 low
    rows = [(0, 1, 450, 90), (0, 1, 50, 10), (0, 0, 450, 90), (0, 0, 50, 810)]
    return pd.DataFrame(rows, columns=["congested_now", "label", "bucket", "n"]).assign(date_local="2017-06-01")


def test_cost_model_basics():
    cm = costs.CostModel(fn=10, fp=1)
    assert cm.ratio == 10 and cm.breakeven_probability == pytest.approx(1 / 11) and cm.cost(2, 3) == 23


def test_cost_optimal_threshold_moves_with_ratio():
    c = ev.curve(_hist())
    cheap_miss = costs.best_cost_bucket(c, costs.CostModel(fn=1, fp=1))
    dear_miss = costs.best_cost_bucket(c, costs.CostModel(fn=100, fp=1))
    assert c.threshold[dear_miss] <= c.threshold[cheap_miss]
    op = costs.operating_point(c, costs.best_cost_bucket(c, costs.CostModel(fn=10, fp=1)), costs.CostModel(fn=10, fp=1))
    assert op["tp"] + op["fn"] == 100 and op["fp"] + op["tn"] == 900
    assert op["cost"] == 10 * op["fn"] + op["fp"] and op["cost_per_1000"] == pytest.approx(op["cost"])


def test_reference_costs_and_profile():
    cm = costs.CostModel(fn=10, fp=1)
    p = costs.profile(_hist(), cm)
    ref = costs.reference_costs(p["curve"], cm)
    assert ref["never_alert_cost"] == 1000 and ref["always_alert_cost"] == 900
    assert p["cost_point"]["cost"] <= min(ref["never_alert_cost"], ref["always_alert_cost"])
    assert 0 < p["roc_auc"] <= 1 and 0 < p["pr_auc"] <= 1


def test_sensitivity_and_bootstrap():
    c = ev.curve(_hist())
    s = costs.sensitivity(c, c, [1, 5, 25])
    assert list(s.fn_fp_ratio) == [1, 5, 25] and s.cost_per_1000.is_monotonic_increasing
    days = [f"2017-06-{d:02d}" for d in range(1, 11)]
    good = pd.DataFrame({"date_local": days, "tp": 9, "fp": 1, "fn": 1, "tn": 89})
    bad = pd.DataFrame({"date_local": days, "tp": 5, "fp": 5, "fn": 5, "tn": 85})
    b = costs.bootstrap_cost_saving({"persistence": bad, "model": good}, "persistence", costs.CostModel(10, 1), n_boot=200)
    row = b.set_index("model").loc["model"]
    assert row.saving_vs_persistence > 0 and row.saving_ci_low > 0 and row.share_boot_cheaper == 1.0
