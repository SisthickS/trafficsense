"""Phase 2 tests: validation catches planted defects, processed build is lossless, raw data stays intact.

Run:  pytest -q tests/test_preprocessing.py
"""
from __future__ import annotations

import dataclasses
import json
import os
import pickle
import stat
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

from src.common.spark import get_spark
from src.preprocessing import validate_dataset as vd
from src.preprocessing.build_processed import build
from src.preprocessing.download_dataset import MANIFEST_NAME, file_hashes
from src.preprocessing.raw_reader import safe_load_adjacency
from src.preprocessing.sources import SPECS

SENSORS = ["1001", "1002", "1003"]
START = datetime(2017, 3, 12, 0, 0)
END = datetime(2017, 3, 12, 4, 0)
DST_HOLE = {START.replace(hour=2) + timedelta(minutes=5 * i) for i in range(12)}  # 02:00-02:55 never existed
DATA_GAP = {START.replace(hour=3, minute=30)}                                      # genuine missing slot


@pytest.fixture(scope="session")
def spark():
    s = get_spark("trafficsense-tests", driver_memory="1g", shuffle_partitions=2)
    yield s
    s.stop()


def _timestamps() -> list[datetime]:
    out, t = [], START
    while t <= END:
        if t not in DST_HOLE and t not in DATA_GAP:
            out.append(t)
        t += timedelta(minutes=5)
    return out


@pytest.fixture()
def raw_env(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    ds_dir = raw_dir / "pems_bay"
    ds_dir.mkdir(parents=True)
    rows = [",".join([""] + SENSORS)]
    for i, t in enumerate(_timestamps()):
        s1 = f"{60 + (i % 7) * 0.5:.1f}"
        s2 = "0.0" if i == 3 else "" if i == 5 else f"{55 + (i % 5):.1f}"  # one zero, one blank
        s3 = "65.0"                                                       # stuck sensor (flat line)
        rows.append(f"{t:%Y-%m-%d %H:%M:%S},{s1},{s2},{s3}")
    (ds_dir / "PEMS-BAY.csv").write_text("\n".join(rows) + "\n")
    (ds_dir / "locs.csv").write_text("1001,37.36,-121.90\n1002,37.25,-121.94\n1003,37.35,-121.90\n")

    files = {}
    for name, role in (("PEMS-BAY.csv", "readings"), ("locs.csv", "sensor_locations")):
        p = ds_dir / name
        files[name] = {"role": role, "url": None, "derived_from": None, "size": p.stat().st_size, **file_hashes(p)}
        p.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    (ds_dir / MANIFEST_NAME).write_text(json.dumps({
        "dataset": "pems-bay", "source": "synthetic", "landing_page": "n/a", "retrieved_at_utc": "n/a",
        "readings_csv": "PEMS-BAY.csv", "locations_csv": "locs.csv", "adjacency_pkl": None, "files": files}))
    spec = dataclasses.replace(SPECS["pems-bay"], n_sensors=3, expected_rows=len(_timestamps()))
    return {"raw_dir": raw_dir, "ds_dir": ds_dir, "spec": spec, "tmp": tmp_path}


def _checks(report: vd.Report) -> dict:
    return {c["id"]: c for c in report.checks}


def test_validation_detects_planted_defects(spark, raw_env):
    report = vd.validate("pems-bay", raw_env["raw_dir"], spark, raw_env["spec"])
    c = _checks(report)

    assert c["C02"]["status"] == vd.PASS
    assert c["C06"]["details"]["sensor_readings"] == len(_timestamps()) * 3

    gaps = c["C07"]["details"]["gaps"]
    assert {(g["start"], g["missing_slots"], g["cause"].split()[0]) for g in gaps} == {
        ("2017-03-12 02:00:00", 12, "DST"), ("2017-03-12 03:30:00", 1, "missing")}
    assert c["C07"]["status"] == vd.WARN  # the non-DST gap must be surfaced

    m = c["C08"]["details"]
    assert (m["nulls_or_nan"], m["zero_speed"], m["missing"]) == (1, 1, 2)

    flat = c["C10"]["details"]
    assert flat["sensors_affected"] >= 1
    assert any(r["sensor_id"] == 1003 for r in flat["longest_runs"])

    assert c["C11"]["status"] == vd.PASS
    assert c["C12"]["status"] == vd.SKIP
    assert report.overall == vd.WARN


def test_tampered_raw_file_fails_integrity(spark, raw_env):
    csv = raw_env["ds_dir"] / "PEMS-BAY.csv"
    csv.chmod(csv.stat().st_mode | stat.S_IWUSR)
    with open(csv, "a") as fh:
        fh.write("2017-03-12 04:05:00,1,2,3\n")
    report = vd.validate("pems-bay", raw_env["raw_dir"], spark, raw_env["spec"])
    assert _checks(report)["C02"]["status"] == vd.FAIL
    assert report.overall == vd.FAIL


def test_build_is_lossless_and_preserves_raw(spark, raw_env):
    report = vd.validate("pems-bay", raw_env["raw_dir"], spark, raw_env["spec"])
    report_dir = raw_env["tmp"] / "reports"
    vd.write_reports(report, report_dir)
    raw_csv = raw_env["ds_dir"] / "PEMS-BAY.csv"
    before = file_hashes(raw_csv)["sha256"]

    processed_dir = raw_env["tmp"] / "processed"
    info = build("pems-bay", raw_env["raw_dir"], processed_dir, report_dir, spark=spark)

    assert info["readings"]["rows"] == len(_timestamps()) * 3
    assert info["readings"]["missing"] == 2
    df = spark.read.parquet(info["readings"]["path"])
    assert df.filter("is_missing AND speed_mph IS NOT NULL").count() == 0
    assert df.filter("NOT is_missing AND speed_mph IS NULL").count() == 0
    assert {f.name for f in df.schema.fields} == {"ts", "sensor_id", "speed_mph", "is_missing", "date", "month"}
    sensors = spark.read.parquet(info["sensors"]["path"]).orderBy("column_index").collect()
    assert [r.sensor_id for r in sensors] == [int(s) for s in SENSORS]
    assert sensors[0].latitude == pytest.approx(37.36)

    assert file_hashes(raw_csv)["sha256"] == before
    assert not os.access(raw_csv, os.W_OK)


def test_build_refuses_without_validation_report(spark, raw_env):
    with pytest.raises(SystemExit, match="Validation report"):
        build("pems-bay", raw_env["raw_dir"], raw_env["tmp"] / "processed", raw_env["tmp"] / "none", spark=spark)


class _Exploit:
    def __reduce__(self):
        return (os.system, ("echo pwned",))


def test_restricted_unpickler_blocks_code_execution(tmp_path):
    bad = tmp_path / "bad.pkl"
    bad.write_bytes(pickle.dumps(_Exploit()))
    with pytest.raises(pickle.UnpicklingError, match="blocked"):
        safe_load_adjacency(bad)


def test_restricted_unpickler_loads_numpy_adjacency(tmp_path):
    good = tmp_path / "adj.pkl"
    good.write_bytes(pickle.dumps((["1", "2"], {"1": 0, "2": 1}, np.eye(2, dtype=np.float32)), protocol=2))
    ids, mapping, adj = safe_load_adjacency(good)
    assert ids == ["1", "2"] and mapping["2"] == 1 and adj.shape == (2, 2)


def test_dst_nonexistent_time_detection():
    assert vd._nonexistent_local(datetime(2017, 3, 12, 2, 30), "America/Los_Angeles") is True
    assert vd._nonexistent_local(datetime(2017, 3, 12, 3, 30), "America/Los_Angeles") is False
