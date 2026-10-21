"""Shared benchmark harness: wall-clock timing plus real Spark task metrics from the Spark UI REST API.

Every measured run gets its own Spark job group. After the run, the harness queries
    <spark-ui>/api/v1/applications/<app>/jobs            -> jobs in that group
    <spark-ui>/api/v1/applications/<app>/stages/<id>     -> per-stage task metrics
and sums the metrics of the COMPLETED stages (skipped stages, e.g. reused shuffle files, contribute 0).
Reported metrics are therefore what Spark actually did, not estimates.
"""
from __future__ import annotations

import json
import logging
import os
import platform
import re
import statistics
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, REPORTS_DIR, SCRATCH_DIR, processed_dataset_dir
from src.common.spark import get_spark

log = logging.getLogger("spark_features")

BENCH_REPORT_DIR = REPORTS_DIR / "spark_features"
BENCH_SCRATCH = SCRATCH_DIR / "bench"
QUICK_MONTH = "2017-01"
METRIC_KEYS = ("numTasks", "inputBytes", "inputRecords", "outputBytes", "shuffleReadBytes", "shuffleReadRecords",
               "shuffleWriteBytes", "shuffleWriteRecords", "memoryBytesSpilled", "diskBytesSpilled", "executorRunTime")
BENCH_CONF = {
    "spark.ui.retainedJobs": "10000",
    "spark.ui.retainedStages": "20000",
    "spark.sql.ui.retainedExecutions": "2000",
}


def bench_spark(app: str, shuffle_partitions: int = 48, extra: dict | None = None) -> SparkSession:
    return get_spark(app, shuffle_partitions=shuffle_partitions, extra_conf={**BENCH_CONF, **(extra or {})})


# ------------------------------------------------------------------------------------------- metrics
class SparkMetrics:
    def __init__(self, spark: SparkSession):
        sc = spark.sparkContext
        self.sc = sc
        self.base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"

    def _get(self, path: str):
        with urllib.request.urlopen(self.base + path, timeout=30) as resp:
            return json.load(resp)

    def group_metrics(self, group: str, timeout: float = 15.0) -> dict:
        deadline = time.time() + timeout
        jobs: list = []
        while time.time() < deadline:
            jobs = [j for j in self._get("/jobs") if j.get("jobGroup") == group]
            if jobs and all(j["status"] != "RUNNING" for j in jobs):
                break
            time.sleep(0.2)
        totals = {k: 0 for k in METRIC_KEYS}
        stages_done = stages_skipped = 0
        for sid in sorted({s for j in jobs for s in j["stageIds"]}):
            for attempt in self._get(f"/stages/{sid}"):
                if attempt["status"] == "SKIPPED":
                    stages_skipped += 1
                    continue
                if attempt["status"] != "COMPLETE":
                    continue
                stages_done += 1
                for k in METRIC_KEYS:
                    totals[k] += attempt.get("numCompleteTasks" if k == "numTasks" else k, 0) or 0
        totals.update(jobs=len(jobs), stages=stages_done, stages_skipped=stages_skipped)
        return totals

    def storage(self) -> list[dict]:
        return [{"name": r["name"].strip()[:80], "storage_level": r["storageLevel"], "memory_bytes": r["memoryUsed"],
                 "disk_bytes": r["diskUsed"], "cached_partitions": r["numCachedPartitions"], "partitions": r["numPartitions"]}
                for r in self._get("/storage/rdd")]


def measure(spark: SparkSession, name: str, fn: Callable[[], Any], repeats: int = 3, warmup: int = 1,
            metrics: SparkMetrics | None = None, before_each: Callable[[], None] | None = None) -> dict:
    """Run fn (warmup + repeats) times; return timings and the Spark metrics of the last measured run."""
    metrics = metrics or SparkMetrics(spark)
    times, group, result = [], None, None
    for i in range(warmup + repeats):
        if before_each:
            before_each()
        group = f"{re.sub(r'[^A-Za-z0-9_.-]', '_', name)}-{i}-{uuid.uuid4().hex[:6]}"
        spark.sparkContext.setJobGroup(group, name)
        t0 = time.perf_counter()
        result = fn()
        elapsed = time.perf_counter() - t0
        if i >= warmup:
            times.append(elapsed)
    spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
    out = {
        "name": name,
        "seconds_median": round(statistics.median(times), 3),
        "seconds_min": round(min(times), 3),
        "seconds_all": [round(t, 3) for t in times],
        "repeats": repeats, "warmup": warmup,
        "metrics": metrics.group_metrics(group),
    }
    if isinstance(result, (int, float, str, bool)) or result is None:
        out["result"] = result
    log.info("%-48s median %7.2fs  shuffle W %9s  R %9s  input %9s  tasks %5d", name, out["seconds_median"],
             fmt_bytes(out["metrics"]["shuffleWriteBytes"]), fmt_bytes(out["metrics"]["shuffleReadBytes"]),
             fmt_bytes(out["metrics"]["inputBytes"]), out["metrics"]["numTasks"])
    return out


def noop(df: DataFrame) -> None:
    """Fully materialise a DataFrame without writing output (the 'noop' sink)."""
    df.write.format("noop").mode("overwrite").save()


# ------------------------------------------------------------------------------------------- plans
_EXPR_ID = re.compile(r"#\d+L?")
_PLAN_ID = re.compile(r"plan_id=\d+|\[id=#?\d+\]")


def normalized_plan(df: DataFrame, which: str = "optimized") -> str:
    """Catalyst plan text with expression IDs removed, so plans of equivalent queries can be compared."""
    qe = df._jdf.queryExecution()
    plan = {"analyzed": qe.analyzed, "optimized": qe.optimizedPlan, "physical": qe.executedPlan}[which]()
    return _PLAN_ID.sub("", _EXPR_ID.sub("", plan.toString())).strip()


def explain_text(df: DataFrame, mode: str = "formatted") -> str:
    """Capture df.explain(mode) as text (PySpark prints it to stdout)."""
    return df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)


def plan_nodes(df: DataFrame) -> list[str]:
    """Operator names of the physical plan, found by walking the plan tree in the JVM.

    Cached data appears as a single InMemoryTableScan: the plan that built the cache (with its own
    Exchange) is not counted, because it is not re-executed. Under AQE the current plan is used.
    """
    out: list[str] = []

    def walk(node) -> None:
        name = node.nodeName()
        if name == "AdaptiveSparkPlan":
            walk(node.executedPlan())
            return
        if name.startswith(("ShuffleQueryStage", "BroadcastQueryStage")):
            walk(node.plan())
            return
        if not name.startswith(("WholeStageCodegen", "InputAdapter")):
            out.append(re.sub(r"\s*\(\d+\)$", "", name).strip())
        children = node.children()
        for i in range(children.size()):
            walk(children.apply(i))

    walk(df._jdf.queryExecution().executedPlan())
    return out


# ------------------------------------------------------------------------------------------- data
def load_clean(spark: SparkSession, dataset: str = "pems-bay", quick: bool = False) -> DataFrame:
    path = processed_dataset_dir(dataset, PROCESSED_DIR) / "clean"
    if not path.exists():
        raise SystemExit(f"{path} not found - run scripts/run_phase3.sh first.")
    df = spark.read.parquet(str(path))
    return df.filter(F.col("month") == QUICK_MONTH) if quick else df


def dir_size(path: Path) -> tuple[int, int]:
    """(bytes, data files) for a Spark output directory (ignores _SUCCESS/.crc)."""
    total = files = 0
    for p in Path(path).rglob("*"):
        if p.is_file() and not p.name.startswith(("_", ".")):
            total += p.stat().st_size
            files += 1
    return total, files


def environment(spark: SparkSession) -> dict:
    mem_kb = 0
    try:
        mem_kb = int(next(l for l in open("/proc/meminfo") if l.startswith("MemTotal")).split()[1])
    except Exception:
        pass
    conf = spark.sparkContext.getConf()
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "spark_version": spark.version, "python": platform.python_version(), "master": spark.sparkContext.master,
        "default_parallelism": spark.sparkContext.defaultParallelism, "cpu_count": os.cpu_count(),
        "ram_gb": round(mem_kb / 1024 / 1024, 1), "driver_memory": conf.get("spark.driver.memory"),
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "aqe_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
        "scratch_dir": str(BENCH_SCRATCH), "platform": platform.platform(),
    }


# ------------------------------------------------------------------------------------------- reporting
def fmt_bytes(n: float | int | None) -> str:
    if n is None:
        return "-"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


BYTE_KEYS = {"inputBytes", "outputBytes", "shuffleWriteBytes", "shuffleReadBytes", "diskBytesSpilled", "memoryBytesSpilled"}


def md_table(rows: list[dict], columns: list[tuple[str, str]]) -> str:
    """columns: (key, header). Values are formatted by key naming conventions."""
    def fmt(key: str, v):
        if v is None:
            return "-"
        if key.endswith("_bytes") or key in BYTE_KEYS:
            return fmt_bytes(v)
        if key.endswith(("_s", "seconds_median", "seconds_min")):
            return f"{v:.2f}"
        if key.endswith("_x"):
            return f"{v:.1f}×"
        if key.endswith("_pct"):
            return f"{v:.1f}%"
        if isinstance(v, bool):
            return "yes" if v else "no"
        if isinstance(v, float):
            return f"{v:,.3f}"
        if isinstance(v, int):
            return f"{v:,}"
        return str(v)
    head = "| " + " | ".join(h for _, h in columns) + " |\n|" + "---|" * len(columns)
    body = "\n".join("| " + " | ".join(fmt(k, r.get(k)) for k, _ in columns) + " |" for r in rows)
    return head + "\n" + body


def flat(result: dict, **extra) -> dict:
    """Flatten a measure() result into one table row."""
    return {"case": result["name"], "seconds_median": result["seconds_median"], "seconds_min": result["seconds_min"],
            **{k: result["metrics"].get(k) for k in METRIC_KEYS}, "result": result.get("result"), **extra}


def bar_chart(path: Path, labels: list[str], values: list[float], title: str, ylabel: str,
              secondary: list[float] | None = None, secondary_label: str | None = None) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(max(6, 1.1 * len(labels) + 2), 3.8))
    bars = ax.bar(labels, values, color="#4c72b0")
    ax.bar_label(bars, fmt="%.2f", fontsize=8)
    ax.set(title=title, ylabel=ylabel)
    ax.tick_params(axis="x", rotation=20)
    if secondary is not None:
        ax2 = ax.twinx()
        ax2.plot(labels, secondary, color="#c44e52", marker="o")
        ax2.set_ylabel(secondary_label or "", color="#c44e52")
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120)
    plt.close(fig)


def write_report(name: str, title: str, env: dict, body_md: str, data: dict, headline: list[str],
                 report_dir: Path = BENCH_REPORT_DIR) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    payload = {"name": name, "title": title, "environment": env, "headline": headline, **data}
    (report_dir / f"{name}.json").write_text(json.dumps(payload, indent=2, default=str) + "\n")
    env_line = (f"Spark {env['spark_version']} · {env['master']} ({env['default_parallelism']} cores) · driver "
                f"{env['driver_memory']} · {env['ram_gb']} GB RAM · AQE {env['aqe_enabled']} · {env['generated_at_utc']}")
    md = [f"# {title}", "", f"_{env_line}_", "", f"Data: {data.get('data_description', 'n/a')}", "",
          "## Key findings", ""] + [f"- {h}" for h in headline] + ["", body_md, ""]
    path = report_dir / f"{name}.md"
    path.write_text("\n".join(md))
    write_summary(report_dir)
    return path


def write_summary(report_dir: Path = BENCH_REPORT_DIR) -> Path:
    order = ["dataframe_sql", "storage_benchmarks", "partition_benchmarks", "optimization"]
    found = {}
    for p in report_dir.glob("*.json"):
        try:
            d = json.loads(p.read_text())
        except json.JSONDecodeError:
            continue
        if isinstance(d, dict) and "headline" in d and "title" in d:
            found[p.stem] = d
    lines = ["# Phase 4 — Spark Core Demonstrations & Benchmarks (summary)", "",
             "Generated from the JSON results in this folder. Full tables, plans and charts are in the linked reports.", ""]
    for name in order + sorted(set(found) - set(order)):
        if name not in found:
            continue
        r = found[name]
        lines += [f"## [{r['title']}]({name}.md)", "", f"_run {r['environment']['generated_at_utc']} · "
                  f"data: {r.get('data_description', 'n/a')}_", ""] + [f"- {h}" for h in r["headline"]] + [""]
    path = report_dir / "SUMMARY.md"
    path.write_text("\n".join(lines))
    return path


def base_args(parser) -> None:
    parser.add_argument("--quick", action="store_true", help=f"use one month ({QUICK_MONTH}) and 1 repeat")
    parser.add_argument("--repeats", type=int, default=3, help="measured repeats per case (after 1 warm-up)")
    parser.add_argument("--report-dir", type=Path, default=BENCH_REPORT_DIR)


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
