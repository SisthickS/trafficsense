"""Documented restart / recovery test: the streaming application must survive crashes without losing or duplicating results.

    python -m src.streaming.recovery_test [--hours 6] [--max-offsets 2000] [--kill-after-batch 3] [--inject-batch 4]

Procedure (all runs read the SAME topic content, so their outputs must be identical):
  0. Replay N hours (with seeded duplicates, late events and malformed payloads) into a fresh topic.
  1. REFERENCE   uninterrupted run to completion (trigger availableNow, small micro-batches).
  2. KILL -9     start a fresh run, SIGKILL the whole process as soon as batch K is committed, restart it with the
                 same command and the same checkpoint, let it finish.
  3. INJECTED    start a fresh run with a fault hook that kills the process AFTER the predictions of batch B were written
                 to MongoDB but BEFORE the batch is committed (the worst moment for an at-least-once sink), restart.
Pass criteria for scenarios 2 and 3, compared with the reference: identical prediction set and probabilities (no loss, no
duplicates), identical readings / late events / dead letters / sensor + network windows, the restart reports that it
RECOVERED state from MongoDB, and in scenario 3 the interrupted batch was recomputed to identical documents.
Writes reports/streaming/recovery_test.{md,json}.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from src.analytics.common import df_to_md
from src.streaming import config as C
from src.streaming import replay_events
from src.streaming.mongo_store import MongoStore

LOG_DIR = C.CHECKPOINT_ROOT.parent / "logs"


def stream_cmd(run: str, db: str, max_offsets: int) -> list[str]:
    return [sys.executable, "-m", "src.streaming.traffic_stream", "--run", run, "--db", db, "--trigger", "available-now",
            "--max-offsets-per-trigger", str(max_offsets)]


def launch(cmd: list[str], log_name: str, env: dict | None = None) -> subprocess.Popen:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    f = open(LOG_DIR / log_name, "ab")
    return subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, env={**os.environ, **(env or {})}, start_new_session=True)


def wait_for(cond, proc: subprocess.Popen | None, timeout: float, what: str, poll: float = 0.1) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if cond():
            return True
        if proc is not None and proc.poll() is not None:
            return cond()
        time.sleep(poll)
    raise TimeoutError(f"timed out waiting for {what}")


def checkpoint_snapshot(run: str, query: str = "predictions") -> dict:
    base = C.checkpoint_dir(run, query)
    ids = lambda d: sorted(int(p.name) for p in (base / d).glob("*") if p.name.isdigit()) if (base / d).exists() else []
    return {"offsets": ids("offsets"), "commits": ids("commits")}


def collect(db: str) -> dict:
    s = MongoStore(db=db)
    out = {
        "predictions": pd.DataFrame(list(s.col(C.COL_PREDICTIONS).find({}, {"processed_at": 0, "latency_ms": 0, "batch_id": 0, "produced_at_ms": 0}))),
        "readings": set(d["_id"] for d in s.col(C.COL_READINGS).find({}, {"_id": 1})),
        "late": set(d["_id"] for d in s.col("late_events").find({}, {"_id": 1})),
        "dead": set(d["_id"] for d in s.col(C.COL_DEAD_LETTERS).find({}, {"_id": 1})),
        "sensor_windows": pd.DataFrame(list(s.col(C.COL_SENSOR_WINDOWS).find({}, {"updated_at": 0}))),
        "network_windows": pd.DataFrame(list(s.col(C.COL_NETWORK_WINDOWS).find({}, {"updated_at": 0}))),
        "state": s.get_state(),
        "batches": list(s.col(C.COL_BATCH_LOG).find({}, {"_id": 0}).sort("batch_id", 1)),
    }
    s.close()
    return out


def compare(ref: dict, other: dict) -> dict:
    checks = {}

    def check(name, ok, detail=""):
        checks[name] = {"ok": bool(ok), "detail": detail}

    rp, op = ref["predictions"].set_index("_id").sort_index(), other["predictions"].set_index("_id").sort_index()
    check("same number of predictions", len(rp) == len(op), f"{len(op):,} vs reference {len(rp):,}")
    check("same prediction ids (nothing lost, nothing extra)", set(rp.index) == set(op.index),
          f"missing {len(set(rp.index) - set(op.index))}, extra {len(set(op.index) - set(rp.index))}")
    common = rp.index.intersection(op.index)
    d = (rp.loc[common, "probability"] - op.loc[common, "probability"]).abs()
    check("probabilities identical", bool(d.max() < 1e-12) if len(d) else False, f"max |dp| = {d.max():.1e}")
    check("alerts identical", bool((rp.loc[common, "alert"] == op.loc[common, "alert"]).all()), f"{int(op.alert.sum()):,} alerts")
    check("no duplicate predictions", bool(op.index.is_unique), "_id = sensor|slot; upserts are idempotent")
    for name in ("readings", "late", "dead"):
        check(f"same {name}", ref[name] == other[name], f"{len(other[name]):,} vs reference {len(ref[name]):,}")
    for name in ("sensor_windows", "network_windows"):
        a, b = ref[name].set_index("_id").sort_index(), other[name].set_index("_id").sort_index()
        same = set(a.index) == set(b.index) and all(
            np.allclose(a.loc[b.index, c].astype(float), b[c].astype(float), atol=1e-9, rtol=0) for c in ("n", "avg_speed", "min_speed", "congested_share"))
        check(f"same {name}", same, f"{len(b):,} vs reference {len(a):,}")
    check("same final state", ref["state"]["last_final_slot"] == other["state"]["last_final_slot"],
          f"last finalized slot {other['state']['last_final_slot']} vs {ref['state']['last_final_slot']}")
    return checks


def fresh(run: str, db: str) -> None:
    """Remove every trace of an earlier run (MongoDB + checkpoints) BEFORE launching, so polling never sees stale state."""
    s = MongoStore(db=db)
    s.drop_all()
    s.close()
    shutil.rmtree(C.CHECKPOINT_ROOT / run, ignore_errors=True)


def log_has(log_name: str, needle: str) -> list[str]:
    p = LOG_DIR / log_name
    return [l for l in p.read_text(errors="replace").splitlines() if needle in l] if p.exists() else []


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2017-06-05T12:00:00Z")
    ap.add_argument("--hours", type=float, default=6.0)
    ap.add_argument("--max-offsets", type=int, default=2000, help="events per micro-batch")
    ap.add_argument("--kill-after-batch", type=int, default=3)
    ap.add_argument("--inject-batch", type=int, default=4)
    ap.add_argument("--out", default=str(C.METRICS_ROOT))
    a = ap.parse_args(argv)
    t_start = time.time()
    shutil.rmtree(LOG_DIR, ignore_errors=True)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    print("0. replaying events into a fresh topic (duplicates, late events and malformed payloads injected)")
    summary = replay_events.run(replay_events.parse_args(
        ["--start", a.start, "--hours", str(a.hours), "--speedup", "0", "--recreate-topic", "--duplicate-fraction", "0.01",
         "--late-fraction", "0.01", "--late-slots", "4", "--malformed-fraction", "0.01", "--summary-file", str(out / "recovery_replay.json")]))
    print(f"   {summary['messages_sent']:,} messages ({summary['unique_events']:,} unique events)")
    scenarios, checks_all = {}, {}

    print("1. REFERENCE: uninterrupted run")
    t0 = time.time()
    fresh("rec_ref", "ts_rec_ref")
    p = launch(stream_cmd("rec_ref", "ts_rec_ref", a.max_offsets) + ["--reset"], "ref.log")
    rc = p.wait()
    ref = collect("ts_rec_ref")
    scenarios["reference"] = {"exit_code": rc, "seconds": round(time.time() - t0, 1), "predictions": len(ref["predictions"]),
                              "batches": len(ref["batches"]), "checkpoint": checkpoint_snapshot("rec_ref")}
    print(f"   exit {rc}, {len(ref['predictions']):,} predictions in {len(ref['batches'])} batches ({scenarios['reference']['seconds']}s)")
    if rc != 0 or not len(ref["predictions"]):
        print("   reference run failed - see", LOG_DIR / "ref.log")
        return 2

    # ---------------------------------------------------------------- scenario 2: SIGKILL
    print(f"2. KILL -9 once batch {a.kill_after_batch} is committed, then restart")
    run, db = "rec_kill", "ts_rec_kill"
    fresh(run, db)
    store = MongoStore(db=db)
    p = launch(stream_cmd(run, db, a.max_offsets) + ["--reset"], "kill_1.log")
    wait_for(lambda: ((store.get_state() or {}).get("last_batch_id", -1) >= a.kill_after_batch), p, 300, f"batch {a.kill_after_batch}")
    os.killpg(p.pid, signal.SIGKILL)
    p.wait()
    time.sleep(1)
    n_at_kill = store.col(C.COL_PREDICTIONS).count_documents({})
    ck_kill = checkpoint_snapshot(run)
    state_kill = store.get_state()
    print(f"   killed (exit {p.returncode}) with {n_at_kill:,} of {len(ref['predictions']):,} predictions stored, "
          f"last committed batch {state_kill['last_batch_id']}, checkpoint offsets {ck_kill['offsets'][-3:]} commits {ck_kill['commits'][-3:]}")
    store.close()
    t0 = time.time()
    p = launch(stream_cmd(run, db, a.max_offsets), "kill_2.log")
    rc = p.wait()
    res = collect(db)
    checks = compare(ref, res)
    recovered = log_has("kill_2.log", "RECOVERED state")
    checks["killed mid-stream (partial output before the kill)"] = {"ok": 0 < n_at_kill < len(ref["predictions"]),
                                                                    "detail": f"{n_at_kill:,} of {len(ref['predictions']):,} predictions"}
    checks["restart recovered driver state from MongoDB"] = {"ok": bool(recovered), "detail": recovered[0].split("trafficsense.stream ")[-1] if recovered else ""}
    checks["restart finished cleanly"] = {"ok": rc == 0, "detail": f"exit code {rc}"}
    scenarios["kill -9"] = {"killed_after_committed_batch": state_kill["last_batch_id"], "predictions_at_kill": n_at_kill,
                            "checkpoint_at_kill": ck_kill, "restart_seconds": round(time.time() - t0, 1), "checkpoint_final": checkpoint_snapshot(run),
                            "restart_log": recovered, "checks": checks}
    checks_all["kill -9"] = checks
    print("   " + ("PASS" if all(c["ok"] for c in checks.values()) else "FAIL"))

    # ---------------------------------------------------------------- scenario 3: crash between output write and commit
    print(f"3. INJECTED crash in batch {a.inject_batch}: after the output write, before the commit; then restart")
    run, db = "rec_inject", "ts_rec_inject"
    fresh(run, db)
    marker = LOG_DIR / "crash.marker"
    marker.unlink(missing_ok=True)
    env = {"TRAFFICSENSE_CRASH_AT_BATCH": str(a.inject_batch), "TRAFFICSENSE_CRASH_MARKER": str(marker)}
    p = launch(stream_cmd(run, db, a.max_offsets) + ["--reset"], "inject_1.log", env)
    rc1 = p.wait()
    store = MongoStore(db=db)
    state = store.get_state()
    crashed = list(store.col(C.COL_PREDICTIONS).find({"batch_id": a.inject_batch}, {"_id": 1, "probability": 1, "processed_at": 1}))
    ck_inj = checkpoint_snapshot(run)
    store.close()
    print(f"   crashed (exit {rc1}); batch {a.inject_batch} wrote {len(crashed):,} predictions but state still says last committed batch "
          f"{state['last_batch_id']}; checkpoint commits end at {ck_inj['commits'][-1:]} while offsets end at {ck_inj['offsets'][-1:]}")
    t0 = time.time()
    p = launch(stream_cmd(run, db, a.max_offsets), "inject_2.log", env)
    rc2 = p.wait()
    res = collect(db)
    checks = compare(ref, res)
    store = MongoStore(db=db)
    after = {d["_id"]: d for d in store.col(C.COL_PREDICTIONS).find({"_id": {"$in": [c["_id"] for c in crashed]}}, {"probability": 1, "processed_at": 1})}
    store.close()
    rewritten = sum(1 for c in crashed if c["_id"] in after and after[c["_id"]]["processed_at"] > c["processed_at"])
    same = all(abs(after[c["_id"]]["probability"] - c["probability"]) < 1e-12 for c in crashed if c["_id"] in after)
    checks["crash happened after output write, before commit"] = {
        "ok": rc1 == 137 and marker.exists() and len(crashed) > 0 and state["last_batch_id"] == a.inject_batch - 1,
        "detail": f"exit {rc1}, {len(crashed):,} documents of batch {a.inject_batch} already written, last committed batch {state['last_batch_id']}"}
    checks["interrupted batch recomputed idempotently"] = {"ok": rewritten == len(crashed) and same,
                                                           "detail": f"{rewritten:,}/{len(crashed):,} documents rewritten by the restart with identical probabilities"}
    recovered = log_has("inject_2.log", "RECOVERED state")
    checks["restart recovered driver state from MongoDB"] = {"ok": bool(recovered), "detail": recovered[0].split("trafficsense.stream ")[-1] if recovered else ""}
    checks["restart finished cleanly"] = {"ok": rc2 == 0, "detail": f"exit code {rc2}"}
    scenarios["injected crash"] = {"crash_batch": a.inject_batch, "exit_code_first_run": rc1, "documents_written_before_crash": len(crashed),
                                   "checkpoint_at_crash": ck_inj, "restart_seconds": round(time.time() - t0, 1),
                                   "checkpoint_final": checkpoint_snapshot(run), "restart_log": recovered, "checks": checks}
    checks_all["injected crash"] = checks
    print("   " + ("PASS" if all(c["ok"] for c in checks.values()) else "FAIL"))

    ok = all(c["ok"] for cs in checks_all.values() for c in cs.values())
    write_report(out, summary, scenarios, ok, time.time() - t_start, a)
    print(f"\nRECOVERY TEST {'PASSED' if ok else 'FAILED'} in {time.time() - t_start:.0f}s -> {out / 'recovery_test.md'}")
    return 0 if ok else 1


def write_report(out: Path, summary: dict, scenarios: dict, ok: bool, seconds: float, a) -> None:
    ref = scenarios["reference"]
    md = [f"# Restart / recovery test - {'PASSED' if ok else 'FAILED'}", "",
          f"Replay: {summary['hours']:g} h of PEMS-BAY from {summary['start']} ({summary['unique_events']:,} unique events; {summary['messages_sent']:,} messages incl. "
          f"{summary['duplicates']} duplicates, {summary['late']} late events, {summary['malformed']} malformed payloads), micro-batches of "
          f"{a.max_offsets:,} events, {C.TOPIC_PARTITIONS} partitions, trigger availableNow. Total {seconds / 60:.1f} min.", "",
          "## Why recovery works", "",
          "- **Spark checkpoint** (offsets + commits per query, state store of the windowed aggregations / de-duplication): after a crash the query "
          "re-plans the batch that was not committed from the checkpointed offsets; Kafka retains the events.",
          "- **Idempotent sink**: every MongoDB document has a deterministic `_id` (sensor|slot, sensor|window start, partition|offset), so a re-run "
          "batch rewrites the same documents (effectively-once output on top of at-least-once delivery).",
          "- **Driver-side feature state** (7-day speed buffer, pending slots) lives in memory only. On start it is rebuilt from the historical store plus "
          "the readings already persisted in MongoDB; the `stream_state` document records the last finalized slot and per-partition progress.",
          "- **Deterministic semantics**: lateness and slot completeness depend on event time per partition, never on micro-batch boundaries, so a "
          "different batch layout after a restart yields identical results.", "",
          "## Reference run", "", f"Uninterrupted run: {ref['predictions']:,} predictions in {ref['batches']} batches ({ref['seconds']} s).", ""]
    for name in ("kill -9", "injected crash"):
        s = scenarios[name]
        md += [f"## Scenario: {name}", ""]
        if name == "kill -9":
            md += [f"SIGKILL of the whole process group right after batch {s['killed_after_committed_batch']} was committed; {s['predictions_at_kill']:,} of "
                   f"{ref['predictions']:,} predictions were stored. Checkpoint at kill: offsets {s['checkpoint_at_kill']['offsets']}, commits "
                   f"{s['checkpoint_at_kill']['commits']}. Restart took {s['restart_seconds']} s; final commits {s['checkpoint_final']['commits']}.", ""]
        else:
            md += [f"Fault hook: the process exits (137) in batch {s['crash_batch']} after {s['documents_written_before_crash']:,} prediction documents were written "
                   f"and before the state/commit. Checkpoint at crash: offsets {s['checkpoint_at_crash']['offsets']}, commits {s['checkpoint_at_crash']['commits']} "
                   f"(an offsets entry without a commit = the in-flight batch). Restart took {s['restart_seconds']} s.", ""]
        rows = pd.DataFrame([{"check": k, "result": "PASS" if v["ok"] else "FAIL", "evidence": v["detail"]} for k, v in s["checks"].items()])
        md += [df_to_md(rows, max_rows=40), ""]
    md += ["## Reproduce", "", "```bash", "docker compose up -d --wait", "python -m src.streaming.recovery_test", "```", "",
           "Logs of every run: `~/.cache/trafficsense/streaming/logs/`. The fault hook is `TRAFFICSENSE_CRASH_AT_BATCH` / `TRAFFICSENSE_CRASH_MARKER` "
           "(see `_maybe_crash` in `traffic_stream.py`); it is inert unless both variables are set.", ""]
    (out / "recovery_test.md").write_text("\n".join(md), encoding="utf-8")
    (out / "recovery_test.json").write_text(json.dumps({"passed": ok, "seconds": round(seconds, 1), "replay": summary, "scenarios": scenarios},
                                                       indent=2, default=str) + "\n")


if __name__ == "__main__":
    sys.exit(main())
