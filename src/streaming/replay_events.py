"""Event replay producer: streams the validated PEMS-BAY readings into Kafka in chronological order.

    python -m src.streaming.replay_events --start 2017-06-05T07:00:00Z --hours 6 --speedup 600

Source  : data/processed/<ds>/clean (Phase 3: validated, cleaned). Only OBSERVED readings are sent; slots that the
          cleaning pipeline forward-filled are simply absent from the stream, exactly like a sensor that did not report.
Order   : by event time, then sensor. Event timestamps are never rewritten (`event_ts` = original UTC measurement time).
Key     : sensor_id (per-sensor order is preserved inside a partition).
Rate    : --speedup X   X seconds of event time per wall-clock second (1 = real time, 300 = one 5-min slot per second,
                         0 = as fast as possible)
          --events-per-sec N   fixed event rate instead (overrides --speedup)
Faults  : optional, deterministic (--seed) fault injection for testing the stream application:
          --duplicate-fraction  identical re-sends             -> must be de-duplicated
          --late-fraction/--late-slots  events delivered N slots late -> beyond the watermark, must be rejected as late
          --malformed-fraction  garbage / implausible payloads -> must land in dead_letters
Delivery: idempotent producer, acks=all, so a retry never duplicates or reorders within a partition.
"""
from __future__ import annotations

import argparse
import heapq
import json
import random
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

from src.streaming import config as C
from src.streaming.events import encode_event
from src.streaming.kafka_tools import ensure_topic
from src.streaming.reference import clean_rows

MALFORMED_KINDS = ["not json {{{", '{"schema_version":1,"sensor_id":400001,"event_ts":"2017-06-05T07:00:0', '{"sensor_id":"abc","event_ts":"x"}',
                   "bad_speed", "bad_ts", "bad_range", "no_sensor", "wrong_version"]


def _malformed_payload(kind: str, rng: random.Random, sensor_ids: list[int], ts: datetime) -> bytes:
    base = {"schema_version": 1, "source": "pems-bay", "sensor_id": rng.choice(sensor_ids), "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "speed_mph": 55.0, "produced_at_ms": int(time.time() * 1000)}
    if kind == "bad_speed":
        base["speed_mph"] = rng.choice([-5.0, 999.0, None])
    elif kind == "bad_ts":
        base["event_ts"] = "yesterday-ish"
    elif kind == "bad_range":
        base["event_ts"] = "2099-01-01T00:00:00Z"
    elif kind == "no_sensor":
        del base["sensor_id"]
    elif kind == "wrong_version":
        base["schema_version"] = 99
    else:
        return kind.encode()
    return json.dumps(base).encode()


def build_stream(events, args) -> list[tuple[int, str, bytes | None, int]]:
    """-> ordered list of (slot, key, payload|None, kind) where payload None means 'encode at send time'.
    kind: 0 normal, 1 duplicate, 2 late, 3 malformed."""
    rng = random.Random(args.seed)
    ids = sorted(events.sensor_id.unique().tolist())
    out, delayed = [], []                       # delayed: heap of (release_slot, seq, item)
    seq = 0
    for r in events.itertuples(index=False):
        while delayed and delayed[0][0] <= r.slot:
            out.append(heapq.heappop(delayed)[2])
        item = (int(r.slot), str(r.sensor_id), (int(r.sensor_id), r.ts_utc.to_pydatetime(), float(r.speed_mph)), 0)
        x = rng.random()
        if x < args.late_fraction:
            seq += 1
            heapq.heappush(delayed, (r.slot + args.late_slots, seq, item[:3] + (2,)))
            continue
        out.append(item)
        if rng.random() < args.duplicate_fraction:
            out.append(item[:3] + (1,))
        if rng.random() < args.malformed_fraction:
            kind = rng.choice(MALFORMED_KINDS)
            out.append((int(r.slot), "malformed", _malformed_payload(kind, rng, ids, r.ts_utc.to_pydatetime()), 3))
    out.extend(x[2] for x in sorted(delayed))
    return out


def run(args) -> dict:
    start = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%SZ")
    end = start + timedelta(hours=args.hours)
    events = clean_rows(start, end, observed_only=True)
    if args.max_events:
        events = events.head(args.max_events)
    if events.empty:
        raise SystemExit(f"no events between {args.start} and {end:%Y-%m-%dT%H:%M:%SZ}")
    stream = build_stream(events, args)
    ensure_topic(args.topic, args.partitions, args.bootstrap, recreate=args.recreate_topic)

    from confluent_kafka import Producer
    errors: list[str] = []
    delivered = [0]

    def on_delivery(err, msg):
        if err is not None:
            errors.append(str(err))
        else:
            delivered[0] += 1

    producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 5,
                         "compression.type": "lz4", "batch.num.messages": 2000, "client.id": "trafficsense-replay"})
    stop = [False]
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__(0, True))
    t0, ts0 = time.time(), events.ts_utc.iloc[0].to_pydatetime()
    sent, kinds, payload_cache, last_slot = 0, {0: 0, 1: 0, 2: 0, 3: 0}, {}, None
    for slot, key, payload, kind in stream:
        if stop[0]:
            break
        if args.events_per_sec:
            delay = t0 + sent / args.events_per_sec - time.time()
        elif args.speedup > 0 and slot != last_slot:
            delay = t0 + ((slot * C.SLOT_SECONDS) - (ts0 - datetime(1970, 1, 1)).total_seconds()) / args.speedup - time.time()
        else:
            delay = 0
        if delay > 0:
            producer.poll(0)
            time.sleep(delay)
        last_slot = slot
        if isinstance(payload, tuple):
            sid, ts, speed = payload
            if kind == 1 and (sid, ts) in payload_cache:
                payload = payload_cache[(sid, ts)]               # byte-identical duplicate
            else:
                payload = encode_event(sid, ts, speed, int(time.time() * 1000))
                payload_cache = {(sid, ts): payload}             # only the latest key can be duplicated next
        while True:
            try:
                producer.produce(args.topic, value=payload, key=key, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
        sent += 1
        kinds[kind] += 1
        if sent % 5000 == 0:
            print(f"  sent {sent:,}/{len(stream):,} (event time {datetime(1970, 1, 1) + timedelta(seconds=slot * C.SLOT_SECONDS)})",
                  file=sys.stderr)
    producer.flush(60)
    wall = time.time() - t0
    summary = {"topic": args.topic, "start": args.start, "hours": args.hours, "unique_events": int(len(events)),
               "messages_sent": sent, "delivered": delivered[0], "delivery_errors": len(errors),
               "normal": kinds[0], "duplicates": kinds[1], "late": kinds[2], "malformed": kinds[3],
               "speedup": args.speedup, "events_per_sec_target": args.events_per_sec, "wall_seconds": round(wall, 2),
               "achieved_msgs_per_sec": round(sent / wall, 1) if wall else None,
               "achieved_speedup": round((events.slot.max() - events.slot.min() + 1) * C.SLOT_SECONDS / wall, 1) if wall else None,
               "first_event_ts": str(events.ts_utc.iloc[0]), "last_event_ts": str(events.ts_utc.iloc[-1]), "seed": args.seed}
    if args.summary_file:
        Path(args.summary_file).parent.mkdir(parents=True, exist_ok=True)
        Path(args.summary_file).write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", default="2017-06-05T12:00:00Z", help="first event time, UTC (default: Monday 2017-06-05 05:00 local = before the "
                                                                    "morning rush hour, in the test period)")
    p.add_argument("--hours", type=float, default=6.0, help="hours of event time to replay")
    p.add_argument("--speedup", type=float, default=300.0, help="event seconds per wall second (0 = unthrottled)")
    p.add_argument("--events-per-sec", type=float, default=0.0, help="fixed send rate (overrides --speedup)")
    p.add_argument("--topic", default=C.TOPIC)
    p.add_argument("--bootstrap", default=C.KAFKA_BOOTSTRAP)
    p.add_argument("--partitions", type=int, default=C.TOPIC_PARTITIONS)
    p.add_argument("--recreate-topic", action="store_true", help="delete and recreate the topic first (clean replay)")
    p.add_argument("--max-events", type=int, default=0)
    p.add_argument("--duplicate-fraction", type=float, default=0.0)
    p.add_argument("--late-fraction", type=float, default=0.0)
    p.add_argument("--late-slots", type=int, default=4, help="delay of late events in slots (watermark is 2 slots)")
    p.add_argument("--malformed-fraction", type=float, default=0.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--summary-file", default="")
    return p.parse_args(argv)


def main(argv=None) -> int:
    summary = run(parse_args(argv))
    print(json.dumps(summary, indent=2))
    return 0 if summary["delivery_errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
