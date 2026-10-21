"""Shared context and result writer for the analytical questions.

AnalyticsContext caches, once per Spark session:
  grid  - complete sensor x 5-minute grid from data/processed/<ds>/clean with calendar columns
          (speed may be null where the reading was missing; `observed` marks real readings)
  obs   - grid filtered to observed readings (used for rates and distributions)
  sensors_pd - per-sensor metadata (lat/lon, train-period free-flow speed, graph degrees)
  adjacency  - (sensor ids, matrix) from the raw DCRNN pickle
  episodes() - congestion episodes (shared by Q3 and Q9)

QuestionResult renders one question to
  reports/analytics/qN.md (+ .json), tables -> reports/analytics/tables/qN_*.csv,
  figures -> reports/figures/analytics/qN_*.png, and refreshes reports/analytics/SUMMARY.md.
"""
from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, RAW_DIR, REPORTS_DIR, processed_dataset_dir, raw_dataset_dir
from src.common.spark import get_spark
from src.preprocessing.build_features import us_federal_holidays
from src.preprocessing.congestion import CONGESTION_SPEED_MPH, FREE_FLOW_SPEED_MPH
from src.preprocessing.download_dataset import load_verified_manifest
from src.preprocessing.raw_reader import safe_load_adjacency

log = logging.getLogger("analytics")

ANALYTICS_DIR = REPORTS_DIR / "analytics"
TABLE_DIR = ANALYTICS_DIR / "tables"
FIG_DIR = REPORTS_DIR / "figures" / "analytics"
FIG_REL = "../figures/analytics"          # relative link from reports/analytics/*.md
THR = CONGESTION_SPEED_MPH
FREE = FREE_FLOW_SPEED_MPH
AM_PEAK = (6, 10)                          # [start, end) local hours
PM_PEAK = (15, 19)
DOW_NAMES = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}


# ------------------------------------------------------------------------------------------- context
class AnalyticsContext:
    def __init__(self, spark: SparkSession, dataset: str = "pems-bay", quick: bool = False):
        self.spark = spark
        self.dataset = dataset
        self.quick = quick           # smaller samples / fewer files for fast end-to-end checks
        self.base = processed_dataset_dir(dataset, PROCESSED_DIR)
        self._cache: dict = {}

    def pick(self, full, quick):
        """Return the full-run or quick-run value of a size parameter."""
        return quick if self.quick else full

    @property
    def grid(self) -> DataFrame:
        if "grid" not in self._cache:
            clean = self.spark.read.parquet(str(self.base / "clean"))
            rng = clean.agg(F.min("date_local"), F.max("date_local")).first()
            holidays = us_federal_holidays(range(rng[0].year, rng[1].year + 1))
            iso_dow = ((F.dayofweek("ts_local") + 5) % 7) + 1
            g = (clean.select("sensor_id", "ts_utc", "ts_local", "date_local", "month", "quality", "issue", "speed_raw",
                              F.col("speed_mph").alias("speed"))
                 .withColumn("hour", F.hour("ts_local"))
                 .withColumn("slot", (F.hour("ts_local") * 12 + F.floor(F.minute("ts_local") / 5)).cast("int"))
                 .withColumn("slot15", (F.hour("ts_local") * 4 + F.floor(F.minute("ts_local") / 15)).cast("int"))
                 .withColumn("dow", iso_dow.cast("int"))
                 .withColumn("is_weekend", F.col("dow") >= 6)
                 .withColumn("is_holiday", F.col("date_local").isin(holidays))
                 .withColumn("is_workday", ~F.col("is_weekend") & ~F.col("is_holiday"))
                 .withColumn("observed", F.col("quality") == "observed")
                 .withColumn("congested", F.when(F.col("observed"), (F.col("speed") < THR).cast("int"))))
            g = g.persist(StorageLevel.MEMORY_AND_DISK)
            n = g.count()
            log.info("grid cached: %s rows", f"{n:,}")
            self._cache.update(grid=g, holidays=holidays, n_rows=n)
        return self._cache["grid"]

    @property
    def obs(self) -> DataFrame:
        return self.grid.filter(F.col("observed"))

    @property
    def holidays(self) -> list:
        self.grid
        return self._cache["holidays"]

    @property
    def sensors_pd(self) -> pd.DataFrame:
        if "sensors_pd" not in self._cache:
            self._cache["sensors_pd"] = self.spark.read.parquet(str(self.base / "sensor_stats")).toPandas()
        return self._cache["sensors_pd"].copy()

    @property
    def adjacency(self) -> tuple[list[str], np.ndarray]:
        if "adjacency" not in self._cache:
            ds_raw = raw_dataset_dir(self.dataset, RAW_DIR)
            ids, _, adj = safe_load_adjacency(ds_raw / load_verified_manifest(ds_raw)["adjacency_pkl"])
            A = np.array(adj, dtype=np.float64)
            np.fill_diagonal(A, 0.0)
            self._cache["adjacency"] = (ids, A)
        return self._cache["adjacency"]

    def edges_pd(self) -> pd.DataFrame:
        ids, A = self.adjacency
        src, dst = np.nonzero(A)
        return pd.DataFrame({"src": [int(ids[i]) for i in src], "dst": [int(ids[j]) for j in dst],
                             "src_idx": src, "dst_idx": dst, "weight": A[src, dst]})

    def episodes(self) -> pd.DataFrame:
        """Congestion episodes: maximal runs of consecutive congested 5-min slots per sensor.

        A missing reading ends an episode. For each episode: start/end (UTC + local), duration, min/mean speed,
        speed 15 min before the start and 15 min after the end, and the first time after the end at which the
        sensor is back at free flow (>= 60 mph).
        """
        if "episodes" in self._cache:
            return self._cache["episodes"]
        g = self.grid
        w = Window.partitionBy("sensor_id").orderBy("ts_utc")
        w_desc = Window.partitionBy("sensor_id").orderBy(F.col("ts_utc").desc()).rowsBetween(Window.unboundedPreceding, 0)
        state = F.coalesce((F.col("speed") < THR).cast("int"), F.lit(-1))
        prev_state = F.lag("st").over(w)
        df = (g.select("sensor_id", "ts_utc", "ts_local", "speed", "is_workday", "dow")
               .withColumn("st", state)
               .withColumn("_new", F.when(prev_state.isNull() | (F.col("st") != prev_state), 1).otherwise(0))
               .withColumn("run_id", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0)))
               .withColumn("speed_before", F.lag("speed", 3).over(w))
               .withColumn("speed_after", F.lead("speed", 3).over(w))
               .withColumn("next_free_ts", F.last(F.when(F.col("speed") >= FREE, F.col("ts_utc")), ignorenulls=True).over(w_desc)))
        ep = (df.filter(F.col("st") == 1).groupBy("sensor_id", "run_id")
                .agg(F.min("ts_utc").alias("start_utc"), F.max("ts_utc").alias("end_utc"),
                     F.min("ts_local").alias("start_local"), F.count(F.lit(1)).alias("slots"),
                     F.min("speed").alias("min_speed"), F.avg("speed").alias("mean_speed"),
                     F.min_by("speed", "ts_utc").alias("speed_at_start"), F.max_by("speed", "ts_utc").alias("speed_at_end"),
                     F.min_by("speed_before", "ts_utc").alias("speed_15m_before"),
                     F.max_by("speed_after", "ts_utc").alias("speed_15m_after"),
                     F.max_by("next_free_ts", "ts_utc").alias("next_free_utc"),
                     F.first("is_workday").alias("is_workday"), F.first("dow").alias("dow"))
                .drop("run_id"))
        pdf = ep.toPandas()
        pdf["duration_min"] = pdf["slots"] * 5
        pdf["start_hour"] = pd.to_datetime(pdf["start_local"]).dt.hour
        rec = pd.to_datetime(pdf["next_free_utc"]) - (pd.to_datetime(pdf["end_utc"]) + pd.Timedelta(minutes=5))
        pdf["recovery_min"] = (rec.dt.total_seconds() / 60).clip(lower=0)
        pdf["onset_drop_mph"] = pdf["speed_15m_before"] - pdf["speed_at_start"]
        pdf["recovery_rise_mph"] = pdf["speed_15m_after"] - pdf["speed_at_end"]
        self._cache["episodes"] = pdf
        log.info("episodes: %s", f"{len(pdf):,}")
        return pdf

    def release(self) -> None:
        """Free the cached grid (it is rebuilt on next access); keeps small pandas caches."""
        grid = self._cache.pop("grid", None)
        if grid is not None:
            grid.unpersist()


def start_spark(app: str = "trafficsense-analytics") -> SparkSession:
    return get_spark(app, shuffle_partitions=48, extra_conf={"spark.sql.execution.arrow.pyspark.enabled": "true"})


# ------------------------------------------------------------------------------------------- results
def df_to_md(df: pd.DataFrame, max_rows: int = 12, floatfmt: int = 3) -> str:
    def fmt(v):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return "-"
        if isinstance(v, (bool, np.bool_)):
            return "yes" if v else "no"
        if isinstance(v, (int, np.integer)):
            return f"{int(v):,}"
        if isinstance(v, (float, np.floating)):
            return "-" if np.isnan(v) else (f"{v:,.{floatfmt}f}" if abs(v) < 1e6 else f"{v:,.0f}")
        if isinstance(v, (pd.Timestamp, datetime)):
            return pd.Timestamp(v).strftime("%Y-%m-%d %H:%M")
        return str(v)
    d = df.head(max_rows)
    lines = ["| " + " | ".join(str(c) for c in d.columns) + " |", "|" + "---|" * len(d.columns)]
    lines += ["| " + " | ".join(fmt(v) for v in row) + " |" for row in d.itertuples(index=False)]
    if len(df) > max_rows:
        lines.append(f"\n_{max_rows} of {len(df):,} rows shown; full table in the CSV._")
    return "\n".join(lines)


def save_fig(fig, name: str) -> str:
    import matplotlib.pyplot as plt
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    path = FIG_DIR / f"{name}.png"
    fig.savefig(path, dpi=130, bbox_inches="tight")
    plt.close(fig)
    return path.name


@dataclass
class QuestionResult:
    qid: str
    title: str
    question: str
    method: list[str] = field(default_factory=list)
    findings: list[str] = field(default_factory=list)
    implications: list[str] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)
    tables: dict = field(default_factory=dict)       # name -> (DataFrame, caption)
    figures: list = field(default_factory=list)      # (file name, caption)
    metrics: dict = field(default_factory=dict)
    seconds: float = 0.0

    def table(self, name: str, df: pd.DataFrame, caption: str) -> pd.DataFrame:
        self.tables[name] = (df, caption)
        return df

    def figure(self, fig, name: str, caption: str) -> None:
        self.figures.append((save_fig(fig, f"{self.qid}_{name}"), caption))

    def write(self, out_dir: Path = ANALYTICS_DIR) -> Path:
        out_dir.mkdir(parents=True, exist_ok=True)
        TABLE_DIR.mkdir(parents=True, exist_ok=True)
        L = [f"# {self.qid.upper()} — {self.title}", "", f"**Question.** {self.question}", "", "## Method", ""]
        L += [f"- {m}" for m in self.method] + ["", "## Key findings", ""] + [f"- {f}" for f in self.findings]
        L += ["", "## Tables", ""]
        for name, (df, caption) in self.tables.items():
            csv = TABLE_DIR / f"{self.qid}_{name}.csv"
            df.to_csv(csv, index=False)
            L += [f"**{caption}** ([CSV](tables/{csv.name}))", "", df_to_md(df), ""]
        L += ["## Figures", ""]
        for file_name, caption in self.figures:
            L += [f"**{caption}**", "", f"![{caption}]({FIG_REL}/{file_name})", ""]
        L += ["## Interpretation & implications for TrafficSense", ""] + [f"- {i}" for i in self.implications]
        L += ["", "## Limitations", ""] + [f"- {x}" for x in self.limitations]
        L += ["", f"_Computed in {self.seconds:.1f} s · {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC._", ""]
        path = out_dir / f"{self.qid}.md"
        path.write_text("\n".join(L))
        payload = {"qid": self.qid, "title": self.title, "question": self.question, "findings": self.findings,
                   "implications": self.implications, "limitations": self.limitations, "metrics": self.metrics,
                   "tables": {k: f"tables/{self.qid}_{k}.csv" for k in self.tables},
                   "figures": [f"{FIG_REL}/{f}" for f, _ in self.figures], "seconds": round(self.seconds, 1)}
        (out_dir / f"{self.qid}.json").write_text(json.dumps(payload, indent=2, default=_json_default) + "\n")
        write_summary(out_dir)
        return path


def _json_default(o):
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.floating):
        return None if np.isnan(o) else float(o)
    if isinstance(o, np.bool_):
        return bool(o)
    return str(o)


def write_summary(out_dir: Path = ANALYTICS_DIR) -> Path:
    items = []
    for p in sorted(out_dir.glob("q*.json"), key=lambda p: int(p.stem[1:]) if p.stem[1:].isdigit() else 99):
        try:
            items.append(json.loads(p.read_text()))
        except json.JSONDecodeError:
            continue
    L = ["# TrafficSense — Analytical Questions (summary)", "",
         "Each question has its own report with method, computed tables (CSV), figures, interpretation and limitations. "
         "Data: cleaned PEMS-BAY readings, 325 sensors, 2017-01-01 → 2017-06-30 (local time).", ""]
    for d in items:
        L += [f"## [{d['qid'].upper()} — {d['title']}]({d['qid']}.md)", "", f"_{d['question']}_", ""]
        L += [f"- {f}" for f in d["findings"][:4]] + [""]
    path = out_dir / "SUMMARY.md"
    path.write_text("\n".join(L))
    return path


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("matplotlib").setLevel(logging.WARNING)


def run_single(run: Callable[[AnalyticsContext], QuestionResult]) -> int:
    """Entry point used by each qN module: python -m src.analytics.qN_... [--quick]"""
    import sys
    setup_logging()
    spark = start_spark()
    ctx = AnalyticsContext(spark, quick="--quick" in sys.argv[1:])
    try:
        t0 = time.time()
        res = run(ctx)
        res.seconds = time.time() - t0
        log.info("%s written: %s", res.qid, res.write())
    finally:
        ctx.release()
        spark.stop()
    return 0


def pct(x: float, digits: int = 1) -> str:
    return f"{100 * x:.{digits}f}%"


def hhmm(slot15: int) -> str:
    return f"{int(slot15) // 4:02d}:{(int(slot15) % 4) * 15:02d}"


def plt_setup():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns
    sns.set_theme(style="whitegrid", context="notebook")
    return plt, sns


def minmax(s: pd.Series) -> pd.Series:
    lo, hi = s.min(), s.max()
    return (s - lo) / (hi - lo) if hi > lo else s * 0.0


def spearman(a, b) -> float:
    """Spearman rank correlation without scipy (DataFrame.corr has its own implementation; Series.corr imports scipy)."""
    d = pd.concat([pd.Series(a).reset_index(drop=True), pd.Series(b).reset_index(drop=True)], axis=1)
    return float(d.corr(method="spearman").iloc[0, 1])


def eta_squared(df: pd.DataFrame, group: str, value: str) -> float:
    """Share of variance in `value` explained by `group` (one-way ANOVA effect size)."""
    d = df[[group, value]].dropna()
    grand = d[value].mean()
    ss_between = sum(len(g) * (g[value].mean() - grand) ** 2 for _, g in d.groupby(group))
    ss_total = ((d[value] - grand) ** 2).sum()
    return float(ss_between / ss_total) if ss_total > 0 else 0.0
