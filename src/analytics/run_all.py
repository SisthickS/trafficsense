"""Run analytical questions in one Spark session (shared cached grid and episodes).

Usage (project root, env sourced):
    python -m src.analytics.run_all                 # Q1-Q18 (Q16 before Q15/Q17/Q18: they use its model)
    python -m src.analytics.run_all --only q3 q9    # selected questions
    python -m src.analytics.run_all --quick         # small samples for Q15-Q18 (end-to-end check)
"""
from __future__ import annotations

import argparse
import importlib
import json
import sys
import time
import traceback

from src.analytics.common import ANALYTICS_DIR, AnalyticsContext, log, setup_logging, start_spark, write_summary

QUESTIONS = {
    "q1": "q1_daily_weekly_patterns",
    "q2": "q2_profile_consistency",
    "q3": "q3_episodes_recovery",
    "q4": "q4_speed_volatility",
    "q5": "q5_hotspots",
    "q6": "q6_road_characteristics",
    "q7": "q7_propagation",
    "q8": "q8_peer_performance",
    "q9": "q9_severity_burden",
    "q10": "q10_onset_transitions",
    "q11": "q11_recovery_performance",
    "q12": "q12_burden_concentration",
    "q13": "q13_missing_coverage",
    "q14": "q14_anomalies",
    "q16": "q16_prediction_metrics",     # trains + saves the model used by q15, q17, q18
    "q17": "q17_feature_ablation",
    "q18": "q18_generalization",
    "q15": "q15_stream_benchmarks",
}
DESCRIPTIVE = [f"q{i}" for i in range(1, 15)]


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--only", nargs="*", choices=sorted(QUESTIONS, key=lambda q: int(q[1:])), help="subset of questions")
    p.add_argument("--quick", action="store_true", help="small samples / fewer stream files (modeling + streaming questions)")
    a = p.parse_args(argv)
    selected = [q for q in QUESTIONS if q in a.only] if a.only else list(QUESTIONS)
    spark = start_spark("trafficsense-analytics")
    ctx = AnalyticsContext(spark, quick=a.quick)
    status = {}
    try:
        for qid in selected:
            t0 = time.time()
            try:
                mod = importlib.import_module(f"src.analytics.{QUESTIONS[qid]}")
                res = mod.run(ctx)
                res.seconds = time.time() - t0
                res.write()
                status[qid] = {"ok": True, "seconds": round(res.seconds, 1), "tables": len(res.tables),
                               "figures": len(res.figures), "findings": len(res.findings)}
                log.info("%s done in %.1fs - %d tables, %d figures", qid, res.seconds, len(res.tables), len(res.figures))
            except Exception as exc:
                status[qid] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
                log.error("%s FAILED: %s\n%s", qid, exc, traceback.format_exc())
    finally:
        ctx.release()
        spark.stop()
    write_summary()
    status_path = ANALYTICS_DIR / "run_status.json"
    merged = json.loads(status_path.read_text()) if (a.only and status_path.exists()) else {}
    merged.update(status)
    status_path.write_text(json.dumps(dict(sorted(merged.items(), key=lambda kv: int(kv[0][1:]))), indent=2) + "\n")
    failed = [q for q, s in status.items() if not s["ok"]]
    log.info("finished: %d ok, %d failed %s", len(status) - len(failed), len(failed), failed or "")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
