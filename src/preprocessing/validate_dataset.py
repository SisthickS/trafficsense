"""PySpark validation of the raw dataset in data/raw/<dataset>/ -> structured Dataset Validation Report.

Read-only: never writes to data/raw or data/processed. Outputs:
    reports/validation/<dataset>_validation_report.json   (machine-readable, consumed by build_processed)
    reports/validation/<dataset>_validation_report.md     (human-readable, for the review)

Usage (project root, env sourced):
    python -m src.preprocessing.validate_dataset                 # PEMS-BAY
    python -m src.preprocessing.validate_dataset --dataset metr-la

Exit code: 0 when overall status is PASS/WARN, 1 when any check FAILs.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.paths import RAW_DIR, VALIDATION_REPORT_DIR, dataset_slug, raw_dataset_dir
from src.common.spark import get_spark
from src.preprocessing.download_dataset import MANIFEST_NAME, file_hashes
from src.preprocessing.raw_reader import (
    CORRUPT_COL,
    TS_RAW_COL,
    load_locations,
    load_wide,
    read_header,
    safe_load_adjacency,
    to_long,
)
from src.preprocessing.sources import REJECTED_SOURCES, SPECS, DatasetSpec

log = logging.getLogger("validate_dataset")
PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIP"
TS_OUT_FMT = "yyyy-MM-dd HH:mm:ss"
PY_TS_FMT = "%Y-%m-%d %H:%M:%S"
MISSING_WARN, MISSING_FAIL = 0.05, 0.20


class Report:
    def __init__(self, dataset: str, spec: DatasetSpec, raw_dir: Path):
        self.checks: list[dict] = []
        self.profile: dict = {}
        self.meta = {
            "dataset": dataset,
            "raw_dir": str(raw_dir),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "spec": {k: v for k, v in spec.__dict__.items()},
        }
        self._t0 = time.time()

    def add(self, cid: str, name: str, status: str, summary: str, **details) -> None:
        self.checks.append({"id": cid, "name": name, "status": status, "summary": summary, "details": details})
        log.info("[%s] %s %s - %s", status, cid, name, summary)

    @property
    def overall(self) -> str:
        statuses = {c["status"] for c in self.checks}
        return FAIL if FAIL in statuses else WARN if WARN in statuses else PASS

    def to_dict(self) -> dict:
        return {
            **self.meta,
            "overall_status": self.overall,
            "duration_seconds": round(time.time() - self._t0, 1),
            "status_counts": {s: sum(c["status"] == s for c in self.checks) for s in (PASS, WARN, FAIL, SKIP)},
            "profile": self.profile,
            "checks": self.checks,
            "rejected_sources": REJECTED_SOURCES,
        }


def _nonexistent_local(dt: datetime, tzname: str) -> bool | None:
    """True if `dt` never occurred on local clocks (skipped by a DST spring-forward)."""
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(tzname)
    except Exception:
        return None
    roundtrip = dt.replace(tzinfo=tz).astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None)
    return roundtrip != dt


def _group_gaps(missing: list[datetime], step: timedelta, tzname: str) -> list[dict]:
    gaps: list[dict] = []
    for dt in missing:
        if gaps and dt - gaps[-1]["_end"] == step:
            gaps[-1]["_end"] = dt
            gaps[-1]["_slots"].append(dt)
        else:
            gaps.append({"_start": dt, "_end": dt, "_slots": [dt]})
    out = []
    for g in gaps:
        dst = [_nonexistent_local(s, tzname) for s in g["_slots"]]
        cause = "DST spring-forward (local time does not exist)" if dst and all(dst) else "missing data"
        out.append({
            "start": g["_start"].strftime(PY_TS_FMT),
            "end": g["_end"].strftime(PY_TS_FMT),
            "missing_slots": len(g["_slots"]),
            "minutes": len(g["_slots"]) * int(step.total_seconds() // 60),
            "cause": cause,
        })
    return out


# --------------------------------------------------------------------------------------------- checks
def check_manifest_and_integrity(report: Report, ds_dir: Path) -> dict | None:
    manifest_path = ds_dir / MANIFEST_NAME
    if not manifest_path.exists():
        report.add("C01", "Manifest & files present", FAIL, f"{manifest_path} not found - run download_dataset first")
        return None
    manifest = json.loads(manifest_path.read_text())
    files = manifest.get("files", {})
    missing = [n for n in files if not (ds_dir / n).exists()]
    report.add("C01", "Manifest & files present", FAIL if missing else PASS,
               f"{len(files) - len(missing)}/{len(files)} manifest files present",
               source=manifest.get("source"), landing_page=manifest.get("landing_page"),
               retrieved_at_utc=manifest.get("retrieved_at_utc"), missing=missing)
    if missing:
        return None

    mismatched, writable, rows = [], [], []
    for name, meta in files.items():
        path = ds_dir / name
        hashes = file_hashes(path)
        ok = hashes["sha256"] == meta["sha256"] and path.stat().st_size == meta["size"]
        if not ok:
            mismatched.append(name)
        if os.access(path, os.W_OK):
            writable.append(name)
        rows.append({"file": name, "role": meta["role"], "size": meta["size"], "sha256": meta["sha256"],
                     "verified": ok, "derived_from": meta.get("derived_from")})
    status = FAIL if mismatched else WARN if writable else PASS
    summary = ("raw files unchanged since download (sha256 match)" if not mismatched
               else f"raw files modified since download: {mismatched}")
    if writable and not mismatched:
        summary += f"; but writable (expected read-only): {writable}"
    report.add("C02", "Raw data integrity", status, summary, files=rows)
    return manifest


def check_header(report: Report, header: list[str], spec: DatasetSpec) -> list[str]:
    sensors = header[1:]
    non_numeric = [s for s in sensors if not s.isdigit()]
    dupes = sorted({s for s in sensors if sensors.count(s) > 1})
    problems = []
    if header[0] not in ("", TS_RAW_COL, "timestamp", "Unnamed: 0"):
        problems.append(f"unexpected first column label '{header[0]}'")
    if non_numeric:
        problems.append(f"{len(non_numeric)} non-numeric sensor ids")
    if dupes:
        problems.append(f"duplicate sensor ids {dupes[:10]}")
    count_ok = len(sensors) == spec.n_sensors
    if not count_ok:
        problems.append(f"{len(sensors)} sensor columns, expected {spec.n_sensors}")
    report.add("C03", "Header / schema", FAIL if (non_numeric or dupes) else WARN if problems else PASS,
               "; ".join(problems) or f"1 timestamp column + {len(sensors)} numeric, unique sensor columns",
               n_columns=len(header), n_sensor_columns=len(sensors), first_column_label=header[0],
               first_sensors=sensors[:5], last_sensors=sensors[-5:],
               inferred_schema={"timestamp": "string -> timestamp (yyyy-MM-dd HH:mm:ss)",
                                "<sensor_id> x N": "double (speed, mph)"})
    return sensors


def check_line_structure(report: Report, spark: SparkSession, csv_path: Path, n_fields: int) -> None:
    """RDD pass over raw lines: every line must have exactly n_fields comma-separated fields."""
    lines = spark.sparkContext.textFile(str(csv_path))
    dist = lines.map(lambda line: (line.count(",") + 1) if line.strip() else 0).countByValue()
    total = sum(dist.values())
    bad = total - dist.get(n_fields, 0)
    report.add("C04", "Line structure (RDD scan)", FAIL if bad else PASS,
               f"{total:,} lines, {bad:,} with a field count other than {n_fields}",
               total_lines=total, rdd_partitions=lines.getNumPartitions(),
               field_count_distribution={str(k): v for k, v in sorted(dist.items())})


def check_parse_and_counts(report: Report, wide: DataFrame, spec: DatasetSpec, n_sensors: int) -> int:
    n_rows = wide.count()
    corrupt = wide.filter(F.col(CORRUPT_COL).isNotNull()).count()
    report.add("C05", "Typed parse (explicit schema, PERMISSIVE)", FAIL if corrupt else PASS,
               f"{corrupt:,} malformed rows out of {n_rows:,}", malformed_rows=corrupt,
               sample_malformed=[r[0][:120] for r in wide.filter(F.col(CORRUPT_COL).isNotNull())
                                 .select(CORRUPT_COL).limit(3).collect()])
    readings = n_rows * n_sensors
    status = PASS if n_rows == spec.expected_rows else WARN
    report.add("C06", "Record counts", status,
               f"{n_rows:,} timestamps x {n_sensors} sensors = {readings:,} readings "
               f"(reference: {spec.expected_rows:,} timestamps)",
               timestamp_rows=n_rows, sensors=n_sensors, sensor_readings=readings,
               expected_timestamp_rows=spec.expected_rows)
    return n_rows


def check_timestamps(report: Report, spark: SparkSession, wide: DataFrame, spec: DatasetSpec) -> dict:
    step = spec.interval_seconds
    ts = wide.select(
        F.monotonically_increasing_id().alias("row_id"),
        F.col(TS_RAW_COL),
        F.to_timestamp(TS_RAW_COL, spec.timestamp_format).alias("ts"),
    ).cache()
    unparseable = ts.filter(F.col("ts").isNull()).count()
    valid = ts.filter(F.col("ts").isNotNull())
    agg = valid.agg(
        F.count("*").alias("n"),
        F.countDistinct("ts").alias("n_distinct"),
        F.min(F.col("ts").cast("long")).alias("min_epoch"),
        F.max(F.col("ts").cast("long")).alias("max_epoch"),
        F.date_format(F.min("ts"), TS_OUT_FMT).alias("min_ts"),
        F.date_format(F.max("ts"), TS_OUT_FMT).alias("max_ts"),
    ).first()
    duplicates = agg.n - agg.n_distinct

    # Ordering + interval histogram in file order (window over the 52k-row timestamp column only).
    seq = valid.withColumn("delta_s", F.col("ts").cast("long") - F.lag(F.col("ts").cast("long")).over(Window.orderBy("row_id")))
    out_of_order = seq.filter(F.col("delta_s") <= 0).count()
    interval_hist = {str(r.delta_s): r["count"] for r in
                     seq.filter(F.col("delta_s").isNotNull()).groupBy("delta_s").count()
                     .orderBy(F.desc("count")).limit(10).collect()}
    off_grid = valid.filter(((F.col("ts").cast("long") - F.lit(agg.min_epoch)) % step) != 0).count()

    # Expected regular grid min..max at `step`, anti-joined with observed timestamps -> missing slots.
    n_slots = (agg.max_epoch - agg.min_epoch) // step + 1
    grid = spark.range(0, n_slots).select(F.timestamp_seconds(F.lit(agg.min_epoch) + F.col("id") * step).alias("ts"))
    missing_df = grid.join(valid.select("ts").distinct(), "ts", "left_anti")
    n_missing = missing_df.count()
    missing = [datetime.strptime(r.ts_str, PY_TS_FMT) for r in
               missing_df.select(F.date_format("ts", TS_OUT_FMT).alias("ts_str")).orderBy("ts_str").limit(20_000).collect()]
    gaps = _group_gaps(missing, timedelta(seconds=step), spec.local_timezone)
    non_dst = [g for g in gaps if not g["cause"].startswith("DST")]
    ts.unpersist()

    days = (agg.max_epoch - agg.min_epoch) / 86400 + step / 86400
    hard = unparseable or duplicates or off_grid or out_of_order
    status = FAIL if hard else WARN if non_dst else PASS
    summary = (f"{agg.min_ts} -> {agg.max_ts} ({days:.1f} days), {step // 60}-min grid of {n_slots:,} slots, "
               f"{n_missing} missing slot(s) in {len(gaps)} gap(s)")
    if gaps and not non_dst:
        summary += " - all explained by DST"
    report.add("C07", "Timestamps", status, summary,
               min_ts=agg.min_ts, max_ts=agg.max_ts, span_days=round(days, 2),
               unparseable=unparseable, duplicates=duplicates, out_of_order=out_of_order, off_grid=off_grid,
               expected_grid_slots=n_slots, missing_slots=n_missing, gaps=gaps[:50],
               interval_histogram_seconds=interval_hist,
               timezone_note=f"naive local wall-clock ({spec.local_timezone}); parsed with Spark session tz UTC "
                             "so values are stored verbatim")
    return {"min_ts": agg.min_ts, "max_ts": agg.max_ts, "span_days": round(days, 2), "gaps": gaps}


def check_missing_values(report: Report, long: DataFrame, spec: DatasetSpec, n_rows: int, n_sensors: int) -> dict:
    speed = F.col("speed_mph")
    flags = long.select(
        "ts", "sensor_id",
        (speed.isNull() | F.isnan(speed)).cast("int").alias("is_null"),
        (speed == 0).cast("int").alias("is_zero"),
        (speed < 0).cast("int").alias("is_negative"),
        (speed > spec.max_plausible_speed).cast("int").alias("is_too_high"),
    ).withColumn("is_missing", F.greatest("is_null", "is_zero", "is_negative"))

    per_sensor = flags.groupBy("sensor_id").agg(
        F.count("*").alias("n"),
        F.sum("is_null").alias("nulls"),
        F.sum("is_zero").alias("zeros"),
        F.sum("is_negative").alias("negatives"),
        F.sum("is_too_high").alias("too_high"),
        F.sum("is_missing").alias("missing"),
    ).withColumn("missing_rate", F.col("missing") / F.col("n")).cache()

    t = per_sensor.agg(*[F.sum(c).alias(c) for c in ("n", "nulls", "zeros", "negatives", "too_high", "missing")]).first()
    rate = t.missing / t.n if t.n else 0.0
    top = [r.asDict() for r in per_sensor.orderBy(F.desc("missing_rate"), "sensor_id").limit(10).collect()]
    for r in top:
        r["missing_rate"] = round(r["missing_rate"], 5)
    fully_missing = per_sensor.filter(F.col("missing") == F.col("n")).count()
    over_20 = per_sensor.filter(F.col("missing_rate") > 0.20).count()
    sensors_with_any = per_sensor.filter(F.col("missing") > 0).count()
    rate_hist = {r.bucket: r["count"] for r in per_sensor.select(
        F.when(F.col("missing_rate") == 0, "0%").when(F.col("missing_rate") <= 0.01, "(0,1%]")
         .when(F.col("missing_rate") <= 0.05, "(1,5%]").when(F.col("missing_rate") <= 0.20, "(5,20%]")
         .otherwise(">20%").alias("bucket")).groupBy("bucket").count().collect()}
    all_down = flags.groupBy("ts").agg(F.sum("is_missing").alias("m")).filter(F.col("m") == n_sensors).count()
    per_sensor.unpersist()

    status = FAIL if rate > MISSING_FAIL or t.n != n_rows * n_sensors else WARN if rate > MISSING_WARN else PASS
    report.add("C08", "Missing values", status,
               f"{t.missing:,} of {t.n:,} readings missing ({rate:.3%}): {t.nulls:,} null/NaN, "
               f"{t.zeros:,} zero-speed, {t.negatives:,} negative",
               readings=t.n, missing=t.missing, missing_rate=round(rate, 6), nulls_or_nan=t.nulls,
               zero_speed=t.zeros, negative=t.negatives, above_plausible_max=t.too_high,
               plausible_max_mph=spec.max_plausible_speed,
               sensors_with_any_missing=sensors_with_any, sensors_fully_missing=fully_missing,
               sensors_over_20pct_missing=over_20, sensor_missing_rate_histogram=rate_hist,
               timestamps_all_sensors_missing=all_down, top10_sensors_by_missing_rate=top,
               rule="missing := null OR NaN OR speed == 0 OR speed < 0 (DCRNN encodes gaps as 0.0)")
    return {"missing_rate": round(rate, 6), "missing_readings": t.missing, "zero_speed": t.zeros,
            "timestamps_all_sensors_missing": all_down}


def check_value_distribution(report: Report, long: DataFrame, spec: DatasetSpec) -> dict:
    speed = F.col("speed_mph")
    valid = long.filter(speed.isNotNull() & ~F.isnan(speed) & (speed > 0))
    s = valid.agg(F.count("*").alias("n"), F.min(speed).alias("min"), F.max(speed).alias("max"),
                  F.mean(speed).alias("mean"), F.stddev(speed).alias("stddev")).first()
    probs = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
    q = valid.approxQuantile("speed_mph", probs, 0.001)
    quantiles = {f"p{int(p * 100):02d}": round(v, 2) for p, v in zip(probs, q)}
    too_high = valid.filter(speed > spec.max_plausible_speed).count()
    stats = {"n_valid": s.n, "min": s.min, "max": s.max, "mean": round(s.mean, 3), "stddev": round(s.stddev, 3), **quantiles}
    report.add("C09", "Speed value distribution", WARN if too_high else PASS,
               f"valid speeds {s.min:.1f}-{s.max:.1f} mph, median {quantiles['p50']}, mean {s.mean:.2f}, "
               f"{too_high:,} above {spec.max_plausible_speed:.0f} mph", **stats, above_plausible_max=too_high)
    return stats


def check_flatlines(report: Report, long: DataFrame, spec: DatasetSpec, min_run_minutes: int = 60) -> dict:
    """Detect stuck / imputed sensors: runs of identical consecutive readings (Spark window functions).

    PeMS imputes detector outages upstream, so true missingness often appears as flat lines, not zeros.
    """
    min_slots = max(2, min_run_minutes * 60 // spec.interval_seconds)
    w = Window.partitionBy("sensor_id").orderBy("ts")
    prev = F.lag("speed_mph").over(w)
    runs = (
        long.select("sensor_id", "ts", "speed_mph")
        .withColumn("new_run", F.when(prev.isNull() | (F.col("speed_mph") != prev), 1).otherwise(0))
        .withColumn("run_id", F.sum("new_run").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        .groupBy("sensor_id", "run_id")
        .agg(F.count("*").alias("length"), F.first("speed_mph").alias("speed_mph"),
             F.date_format(F.min("ts"), TS_OUT_FMT).alias("start"))
        .filter(F.col("length") >= min_slots)
        .cache()
    )
    agg = runs.agg(F.count("*").alias("n_runs"), F.sum("length").alias("readings"),
                   F.countDistinct("sensor_id").alias("sensors")).first()
    total = long.count()
    flat_readings = agg.readings or 0
    share = flat_readings / total if total else 0.0
    longest = [{**r.asDict(), "hours": round(r.length * spec.interval_seconds / 3600, 1)}
               for r in runs.orderBy(F.desc("length")).limit(5).collect()]
    runs.unpersist()
    status = PASS if share < 0.01 else WARN
    report.add("C10", "Flat-line runs (stuck/imputed sensors)", status,
               f"{agg.n_runs:,} runs >= {min_run_minutes} min of identical speed across {agg.sensors} sensors, "
               f"covering {flat_readings:,} readings ({share:.3%})",
               min_run_slots=min_slots, runs=agg.n_runs, sensors_affected=agg.sensors,
               readings_in_runs=flat_readings, share=round(share, 6), longest_runs=longest)
    return {"flatline_share": round(share, 6)}


def check_locations(report: Report, spark: SparkSession, path: Path, spec: DatasetSpec, sensors: list[str]) -> None:
    loc = load_locations(spark, path, spec).cache()
    n = loc.count()
    n_ids = loc.select("sensor_id").distinct().count()
    nulls = loc.filter(F.col("sensor_id").isNull() | F.col("latitude").isNull() | F.col("longitude").isNull()).count()
    (lat0, lat1), (lon0, lon1) = spec.lat_range, spec.lon_range
    out_of_box = loc.filter(~F.col("latitude").between(lat0, lat1) | ~F.col("longitude").between(lon0, lon1)).count()
    b = loc.agg(F.min("latitude"), F.max("latitude"), F.min("longitude"), F.max("longitude")).first()
    loc_ids = {r[0] for r in loc.select("sensor_id").collect()}
    hdr_ids = {int(s) for s in sensors if s.isdigit()}
    no_location = sorted(hdr_ids - loc_ids)
    no_readings = sorted(loc_ids - hdr_ids)
    loc.unpersist()
    status = FAIL if (nulls or n != n_ids) else WARN if (no_location or no_readings or out_of_box) else PASS
    report.add("C11", "Sensor locations", status,
               f"{n} sensors with coordinates, {len(no_location)} reading-sensors lack coordinates, "
               f"{out_of_box} outside expected bounding box",
               rows=n, distinct_ids=n_ids, null_fields=nulls, out_of_bbox=out_of_box,
               bbox_observed={"lat": [b[0], b[1]], "lon": [b[2], b[3]]},
               bbox_expected={"lat": list(spec.lat_range), "lon": list(spec.lon_range)},
               sensors_without_location=no_location[:20], locations_without_readings=no_readings[:20])


def check_adjacency(report: Report, path: Path, sensors: list[str]) -> None:
    try:
        ids, _, adj = safe_load_adjacency(path)
    except Exception as exc:
        report.add("C12", "Adjacency matrix (graph)", FAIL, f"could not load safely: {exc}")
        return
    same_order = ids == sensors
    same_set = set(ids) == set(sensors)
    square = adj.ndim == 2 and adj.shape[0] == adj.shape[1] == len(ids)
    status = PASS if (same_order and square) else WARN if (same_set and square) else FAIL
    report.add("C12", "Adjacency matrix (graph)", status,
               f"{adj.shape[0]}x{adj.shape[1]} {adj.dtype}, sensor order "
               f"{'matches' if same_order else 'differs from'} CSV header",
               shape=list(adj.shape), dtype=str(adj.dtype), min=float(adj.min()), max=float(adj.max()),
               nonzero_edges=int((adj > 0).sum()), self_loops=int((adj.diagonal() > 0).sum()),
               same_sensor_set=same_set, same_order=same_order,
               loader="restricted unpickler (numpy arrays/builtins only)")


# --------------------------------------------------------------------------------------------- output
def write_reports(report: Report, report_dir: Path) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    data = report.to_dict()
    stem = f"{dataset_slug(data['dataset'])}_validation_report"
    json_path = report_dir / f"{stem}.json"
    md_path = report_dir / f"{stem}.md"
    json_path.write_text(json.dumps(data, indent=2, default=str) + "\n")
    md_path.write_text(render_markdown(data))
    return json_path, md_path


def render_markdown(d: dict) -> str:
    icon = {PASS: "✅", WARN: "⚠️", FAIL: "❌", SKIP: "⏭️"}
    p = d["profile"]
    by_id = {c["id"]: c for c in d["checks"]}
    L = [f"# Dataset Validation Report — {d['dataset'].upper()}", "",
         f"- **Overall status:** {icon[d['overall_status']]} **{d['overall_status']}** "
         f"({d['status_counts']['PASS']} pass, {d['status_counts']['WARN']} warn, {d['status_counts']['FAIL']} fail)",
         f"- **Generated (UTC):** {d['generated_at_utc']}  ·  **Spark:** {p.get('spark_version', '?')}  ·  "
         f"**Runtime:** {d['duration_seconds']} s",
         f"- **Raw directory:** `{d['raw_dir']}` (read-only, verified against MANIFEST.json)", "",
         "## 1. Check summary", "", "| ID | Check | Status | Result |", "|---|---|---|---|"]
    for c in d["checks"]:
        L.append(f"| {c['id']} | {c['name']} | {icon[c['status']]} {c['status']} | {c['summary']} |")

    if p:
        L += ["", "## 2. Dataset profile", "", "| Metric | Value |", "|---|---|"]
        for k, label in [("sensors", "Sensors"), ("timestamp_rows", "Timestamps (rows)"),
                         ("sensor_readings", "Sensor readings (long format)"), ("min_ts", "First timestamp"),
                         ("max_ts", "Last timestamp"), ("span_days", "Span (days)"),
                         ("interval_minutes", "Sampling interval (min)"), ("missing_rate", "Missing rate"),
                         ("flatline_share", "Readings in ≥1 h flat-line runs"),
                         ("raw_csv_mb", "Raw CSV size (MB)")]:
            if k in p:
                v = f"{p[k]:.3%}" if k in ("missing_rate", "flatline_share") else f"{p[k]:,}" if isinstance(p[k], int) else p[k]
                L.append(f"| {label} | {v} |")

    c7 = by_id.get("C07")
    if c7 and c7["details"].get("gaps"):
        L += ["", "## 3. Timestamp gaps", "", "| Start | End | Missing slots | Minutes | Cause |", "|---|---|---|---|---|"]
        for g in c7["details"]["gaps"]:
            L.append(f"| {g['start']} | {g['end']} | {g['missing_slots']} | {g['minutes']} | {g['cause']} |")
        L.append(f"\nInterval histogram (seconds → count): `{c7['details']['interval_histogram_seconds']}`")

    c8 = by_id.get("C08")
    if c8 and "top10_sensors_by_missing_rate" in c8["details"]:
        det = c8["details"]
        L += ["", "## 4. Missing values", "", f"Rule: {det['rule']}", "",
              f"Sensors by missing rate: `{det['sensor_missing_rate_histogram']}` · "
              f"timestamps where every sensor is missing: **{det['timestamps_all_sensors_missing']}**", "",
              "| Sensor | Readings | Null/NaN | Zero | Missing rate |", "|---|---|---|---|---|"]
        for r in det["top10_sensors_by_missing_rate"]:
            L.append(f"| {r['sensor_id']} | {r['n']:,} | {r['nulls']:,} | {r['zeros']:,} | {r['missing_rate']:.3%} |")

    c9 = by_id.get("C09")
    if c9:
        det = c9["details"]
        L += ["", "## 5. Speed distribution (valid readings, mph)", "",
              "| n | min | p01 | p05 | p25 | p50 | p75 | p95 | p99 | max | mean | std |",
              "|---|---|---|---|---|---|---|---|---|---|---|---|",
              f"| {det['n_valid']:,} | {det['min']} | {det['p01']} | {det['p05']} | {det['p25']} | {det['p50']} | "
              f"{det['p75']} | {det['p95']} | {det['p99']} | {det['max']} | {det['mean']} | {det['stddev']} |"]

    c10 = by_id.get("C10")
    if c10 and c10["details"].get("longest_runs"):
        L += ["", "### Longest flat-line runs (identical consecutive readings)", "",
              "| Sensor | Start | Length (slots) | Hours | Speed (mph) |", "|---|---|---|---|---|"]
        for r in c10["details"]["longest_runs"]:
            L.append(f"| {r['sensor_id']} | {r['start']} | {r['length']:,} | {r['hours']} | {r['speed_mph']} |")

    L += ["", "## 6. Data dictionary", "", "**Raw (`data/raw/`, wide CSV, immutable)**", "",
          "| Column | Type | Description |", "|---|---|---|",
          "| *(blank header)* | string `yyyy-MM-dd HH:mm:ss` | 5-minute interval start, naive local Pacific time |",
          "| `<sensor_id>` × N | double | Average speed (mph) at loop-detector station; `0.0` encodes a missing reading |",
          "", "**Processed (`data/processed/<dataset>/readings/`, Parquet, partitioned by `month`)**", "",
          "| Column | Type | Description |", "|---|---|---|",
          "| `ts` | timestamp | Interval start (local wall-clock, stored verbatim with session tz UTC) |",
          "| `sensor_id` | int | Caltrans PeMS / LA Metro station id |",
          "| `speed_mph` | double (nullable) | Cleaned speed; `null` where the raw reading is missing |",
          "| `is_missing` | boolean | Raw reading was null/NaN/≤0 |",
          "| `date`, `month` | date, string | Convenience/partition columns |",
          "", "**Sensors (`data/processed/<dataset>/sensors/`)**: `sensor_id`, `column_index` (graph order), "
          "`latitude`, `longitude`, `n_readings`, `n_missing`, `missing_rate`."]

    c2 = by_id.get("C02")
    if c2:
        L += ["", "## 7. Provenance", "", "| File | Role | Bytes | SHA-256 | Verified |", "|---|---|---|---|---|"]
        for f in c2["details"]["files"]:
            L.append(f"| `{f['file']}` | {f['role']} | {f['size']:,} | `{f['sha256'][:16]}…` | {'yes' if f['verified'] else 'NO'} |")
        c1 = by_id["C01"]["details"]
        L.append(f"\nSource: {c1.get('source')} — {c1.get('landing_page')} · retrieved {c1.get('retrieved_at_utc')}")

    L += ["", "## 8. Sources considered but not used", ""]
    L += [f"- **{k}** — {v}" for k, v in d["rejected_sources"].items()]
    L += ["", "## 9. Full check details", "", "See the JSON report alongside this file for every metric.", ""]
    return "\n".join(L)


# --------------------------------------------------------------------------------------------- driver
def validate(dataset: str, raw_dir: Path, spark: SparkSession, spec: DatasetSpec | None = None) -> Report:
    spec = spec or SPECS[dataset]
    ds_dir = raw_dataset_dir(dataset, raw_dir)
    report = Report(dataset, spec, ds_dir)
    report.profile["spark_version"] = spark.version

    manifest = check_manifest_and_integrity(report, ds_dir)
    if manifest is None:
        return report
    csv_path = ds_dir / manifest["readings_csv"]
    report.profile["raw_csv_mb"] = round(csv_path.stat().st_size / 1e6, 1)

    header = read_header(csv_path)
    sensors = check_header(report, header, spec)
    check_line_structure(report, spark, csv_path, len(header))

    wide = load_wide(spark, csv_path, sensors).cache()
    n_rows = check_parse_and_counts(report, wide, spec, len(sensors))
    ts_info = check_timestamps(report, spark, wide, spec)

    long = to_long(wide.drop(CORRUPT_COL), sensors, spec).cache()
    miss = check_missing_values(report, long, spec, n_rows, len(sensors))
    check_value_distribution(report, long, spec)
    flat = check_flatlines(report, long, spec)
    long.unpersist()
    wide.unpersist()

    if manifest.get("locations_csv"):
        check_locations(report, spark, ds_dir / manifest["locations_csv"], spec, sensors)
    else:
        report.add("C11", "Sensor locations", SKIP, "no locations file in manifest")
    if manifest.get("adjacency_pkl"):
        check_adjacency(report, ds_dir / manifest["adjacency_pkl"], sensors)
    else:
        report.add("C12", "Adjacency matrix (graph)", SKIP, "no adjacency file in manifest")

    report.profile.update({
        "sensors": len(sensors), "timestamp_rows": n_rows, "sensor_readings": n_rows * len(sensors),
        "interval_minutes": spec.interval_seconds // 60, **{k: ts_info[k] for k in ("min_ts", "max_ts", "span_days")},
        "missing_rate": miss["missing_rate"], "flatline_share": flat["flatline_share"],
    })
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dataset", default="pems-bay", choices=sorted(SPECS))
    p.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    p.add_argument("--report-dir", type=Path, default=VALIDATION_REPORT_DIR)
    p.add_argument("--driver-memory", default=None, help="e.g. 3g (default from SPARK_DRIVER_MEMORY or 3g)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    args = parse_args(argv)
    spark = get_spark(f"trafficsense-validate-{args.dataset}", driver_memory=args.driver_memory)
    try:
        report = validate(args.dataset, args.raw_dir, spark)
        json_path, md_path = write_reports(report, args.report_dir)
    finally:
        spark.stop()
    log.info("Overall: %s  |  %s  |  %s", report.overall, json_path, md_path)
    return 1 if report.overall == FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
