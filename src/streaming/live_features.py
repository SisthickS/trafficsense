"""Live feature engine: the Phase 3 feature definitions computed incrementally from a stream of events.

State = a ring buffer of the last 7 days of CLEANED speeds (sensors x slots) with the 'observed' flag. Events are collected
per slot; a slot is FINALIZED (its row of the buffer is written and its features computed) only when every Kafka
partition has progressed past it (event-time completeness, see `ready_through`), so neighbour features see all sensors that
reported. Gap handling is the Phase 3 rule: causal forward-fill of the last valid reading for at most 3 slots (15 min).

Parity with the batch pipeline is by construction (same formulas, same neighbour kernel `neighbor_stats`) and verified on
real data by `src.streaming.verify_stream` (predictions of the stream vs the Phase 3 `features` table).
"""
from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src.preprocessing.build_features import FEATURE_COLUMNS, neighbor_stats, us_federal_holidays
from src.preprocessing.congestion import CONGESTION_SPEED_MPH
from src.streaming.config import FORWARD_FILL_SLOTS, HISTORY_SLOTS, SLOT_SECONDS
from src.streaming.events import slot_start

SLOTS_PER_DAY = 288
NEVER = -(10 ** 9)
STATIC_COLUMNS = ["latitude", "longitude", "in_degree", "out_degree", "ff_speed", "sensor_congested_rate"]


class LiveFeatureEngine:
    def __init__(self, sensor_ids: list[int], adjacency: np.ndarray, static: pd.DataFrame, profile: pd.DataFrame,
                 timezone_name: str = "America/Los_Angeles", grace_slots: int = 1, n_partitions: int = 1):
        self.ids = np.asarray(sensor_ids, dtype=np.int64)
        self.n = len(self.ids)
        self.index = {int(s): i for i, s in enumerate(self.ids)}
        self.A = np.array(adjacency, dtype=np.float64)
        np.fill_diagonal(self.A, 0.0)
        self.tz = ZoneInfo(timezone_name)
        self.holidays = set(us_federal_holidays(range(2000, 2036)))
        self.grace = grace_slots
        self.n_partitions = n_partitions
        st = static.set_index("sensor_id").reindex(self.ids)
        self.static = {c: st[c].to_numpy(dtype=np.float64) for c in STATIC_COLUMNS}
        # profile[sensor, day_of_week 1..7, slot_of_day] -> train-period speed / congestion rate (NaN if unseen)
        self.prof_speed = np.full((self.n, 8, SLOTS_PER_DAY), np.nan)
        self.prof_cong = np.full((self.n, 8, SLOTS_PER_DAY), np.nan)
        p = profile[profile.sensor_id.isin(self.index)]
        rows = p.sensor_id.map(self.index).to_numpy()
        self.prof_speed[rows, p.day_of_week.to_numpy(), p.slot_of_day.to_numpy()] = p.hist_speed.to_numpy()
        self.prof_cong[rows, p.day_of_week.to_numpy(), p.slot_of_day.to_numpy()] = p.hist_congested_rate.to_numpy()

        self.H = HISTORY_SLOTS
        self.S = np.full((self.H, self.n), np.nan, dtype=np.float32)
        self.OBS = np.zeros((self.H, self.n), dtype=bool)
        self.last_valid_slot = np.full(self.n, NEVER, dtype=np.int64)
        self.last_valid_speed = np.full(self.n, np.nan, dtype=np.float32)
        self.final_slot: int | None = None            # last finalized slot
        self.first_slot: int | None = None            # first slot of the stream (history ends right before it)
        self.pending: dict[int, dict[int, tuple[float, int]]] = {}
        self.last_arrival: dict[int, float] = {}          # wall-clock time of the newest accepted event per pending slot
        self.partition_max: dict[int, int] = {}

    # ------------------------------------------------------------------------------------------- state load
    def load_history(self, hist: pd.DataFrame, first_slot: int) -> None:
        """hist: cleaned grid rows (sensor_id, slot, speed_mph, quality) for slots < first_slot (up to HISTORY_SLOTS back)."""
        hist = hist[(hist.slot < first_slot) & hist.sensor_id.isin(self.index)]
        r = (hist.slot.to_numpy() % self.H).astype(np.int64)
        c = hist.sensor_id.map(self.index).to_numpy()
        self.S[r, c] = hist.speed_mph.to_numpy(dtype=np.float32)
        self.OBS[r, c] = (hist.quality == "observed").to_numpy()
        obs = hist[hist.quality == "observed"].sort_values("slot").groupby("sensor_id").tail(1)
        ci = obs.sensor_id.map(self.index).to_numpy()
        self.last_valid_slot[ci] = obs.slot.to_numpy()
        self.last_valid_speed[ci] = obs.speed_mph.to_numpy(dtype=np.float32)
        self.first_slot = first_slot
        self.final_slot = first_slot - 1

    def restore(self, hist: pd.DataFrame, first_slot: int, readings: list[dict], last_final: int, partition_max: dict) -> None:
        """Rebuild after a restart from the history + the readings already persisted in MongoDB."""
        self.load_history(hist, first_slot)
        by_slot: dict[int, dict] = {}
        for d in readings:
            by_slot.setdefault(int(d["slot"]), {})[self.index[int(d["sensor_id"])]] = (float(d["speed_mph"]), int(d.get("produced_at_ms") or 0))
        for t in range(first_slot, last_final + 1):
            self._advance(t, by_slot.pop(t, {}))
        self.pending = {t: v for t, v in by_slot.items() if t > last_final}
        self.last_arrival = {t: time.time() for t in self.pending}
        self.partition_max = {int(k): int(v) for k, v in partition_max.items()}

    # ------------------------------------------------------------------------------------------- ingest
    def ingest(self, events: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        """Classify and collect a micro-batch. events: sensor_id, slot, speed_mph, produced_at_ms, partition, offset.

        Lateness is decided in EVENT time per Kafka partition, independent of how events are split into micro-batches:
        an event is late when its slot is at least `grace` slots behind the newest slot already seen in its partition
        (arrival order = offset order). A slot is finalized once every partition is `grace` slots past it, so no
        on-time event can arrive for a finalized slot. Returns (accepted, late, counters).
        """
        events = events.sort_values(["partition", "offset"], kind="stable")
        late_mask = np.zeros(len(events), dtype=bool)
        slots = events.slot.to_numpy()
        parts = events.partition.to_numpy()
        for p in np.unique(parts):
            sel = np.nonzero(parts == p)[0]
            before = self.partition_max.get(int(p), NEVER)
            running = np.maximum.accumulate(np.concatenate([[before], slots[sel]]))[:-1]
            late_mask[sel] = slots[sel] <= running - self.grace
            self.partition_max[int(p)] = int(max(before, slots[sel].max()))
        if self.final_slot is not None:                         # only differs from the rule above when slots were finalized early
            late_mask |= slots <= self.final_slot
        known = events.sensor_id.map(self.index).notna().to_numpy()
        accepted = events[~late_mask & known]
        now = time.time()
        for r in accepted.itertuples(index=False):
            self.pending.setdefault(int(r.slot), {})[self.index[int(r.sensor_id)]] = (float(r.speed_mph), int(r.produced_at_ms or 0))
            self.last_arrival[int(r.slot)] = now
        return accepted, events[late_mask], {"accepted": int(len(accepted)), "late": int(late_mask.sum()), "unknown_sensor": int((~known).sum())}

    def ready_through(self) -> int | None:
        """Last slot that may be finalized: every partition has delivered events at least `grace` slots later."""
        if len(self.partition_max) < self.n_partitions:
            return None
        return min(self.partition_max.values()) - self.grace

    def settled_through(self, idle_seconds: float, min_coverage: float) -> int | None:
        """Real-time mode (opt-in, NOT deterministic): newest pending slot that has gone quiet for `idle_seconds` of wall-clock
        time with at least `min_coverage` of the sensors reported; everything before it is finalized with it."""
        now = time.time()
        for t in sorted(self.pending, reverse=True):
            if len(self.pending[t]) >= min_coverage * self.n and now - self.last_arrival.get(t, now) >= idle_seconds:
                return t
        return None

    def finalize(self, through: int) -> list[tuple[int, pd.DataFrame]]:
        """Advance the buffer slot by slot up to `through`; returns (slot, features) for each finalized slot."""
        out = []
        if self.final_slot is None:
            return out
        for t in range(self.final_slot + 1, through + 1):
            readings = self.pending.pop(t, {})
            self.last_arrival.pop(t, None)
            self._advance(t, readings)
            out.append((t, self.features(t, readings)))
        return out

    def _advance(self, t: int, readings: dict) -> None:
        r = t % self.H
        self.S[r, :] = np.nan
        self.OBS[r, :] = False
        for i, (speed, _) in readings.items():
            self.S[r, i] = np.float32(speed)
            self.OBS[r, i] = True
            self.last_valid_slot[i] = t
            self.last_valid_speed[i] = np.float32(speed)
        fill = (~self.OBS[r]) & (t - self.last_valid_slot <= FORWARD_FILL_SLOTS)
        self.S[r, fill] = self.last_valid_speed[fill]
        self.final_slot = t

    # ------------------------------------------------------------------------------------------- features
    def _row(self, t: int) -> np.ndarray:
        return self.S[t % self.H].astype(np.float64)

    def _window(self, t: int, n: int) -> tuple[np.ndarray, np.ndarray]:
        rows = [(t - j) % self.H for j in range(n)]
        return self.S[rows].astype(np.float64), self.OBS[rows]

    def features(self, t: int, readings: dict | None = None) -> pd.DataFrame:
        """Phase 3 feature row for every sensor with a cleaned speed at slot t (observed or forward-filled)."""
        cur = self._row(t)
        keep = ~np.isnan(cur)
        thr = CONGESTION_SPEED_MPH
        utc = slot_start(t)
        local = utc.replace(tzinfo=timezone.utc).astimezone(self.tz)
        slot_of_day = local.hour * 12 + local.minute // 5
        dow = local.isoweekday()
        nxt = (utc + timedelta(seconds=SLOT_SECONDS)).replace(tzinfo=timezone.utc).astimezone(self.tz)
        next_dow, next_slot = nxt.isoweekday(), nxt.hour * 12 + nxt.minute // 5
        two_pi = 2 * math.pi

        lag = lambda k: self._row(t - k)
        lag5, lag15 = lag(1), lag(3)
        W60, obs60 = self._window(t, 12)
        with np.errstate(invalid="ignore", divide="ignore"), _quiet():
            cnt = np.sum(~np.isnan(W60), axis=0)
            roll_std = np.where(cnt >= 2, np.nanstd(W60, axis=0, ddof=1), np.nan)
            cong60 = np.where(np.isnan(W60), np.nan, (W60 < thr).astype(np.float64))
            f = {
                "hour": np.full(self.n, local.hour), "slot_of_day": np.full(self.n, slot_of_day), "day_of_week": np.full(self.n, dow),
                "is_weekend": np.full(self.n, int(dow >= 6)), "is_holiday": np.full(self.n, int(local.date() in self.holidays)),
                "tod_sin": np.full(self.n, math.sin(two_pi * slot_of_day / SLOTS_PER_DAY)),
                "tod_cos": np.full(self.n, math.cos(two_pi * slot_of_day / SLOTS_PER_DAY)),
                "dow_sin": np.full(self.n, math.sin(two_pi * (dow - 1) / 7)), "dow_cos": np.full(self.n, math.cos(two_pi * (dow - 1) / 7)),
                "speed": cur, "is_imputed": (~self.OBS[t % self.H]).astype(int), "congested_now": (cur < thr).astype(int),
                "speed_lag_5m": lag5, "speed_lag_10m": lag(2), "speed_lag_15m": lag15, "speed_lag_30m": lag(6), "speed_lag_60m": lag(12),
                "speed_target_slot_1d": lag(SLOTS_PER_DAY - 1), "speed_target_slot_1w": lag(7 * SLOTS_PER_DAY - 1),
                "delta_5m": cur - lag5, "delta_15m": cur - lag15,
                "roll_mean_15m": np.nanmean(W60[:3], axis=0), "roll_mean_30m": np.nanmean(W60[:6], axis=0),
                "roll_mean_60m": np.nanmean(W60, axis=0), "roll_std_60m": roll_std, "roll_min_60m": np.nanmin(W60, axis=0),
                "congested_frac_60m": np.nanmean(cong60, axis=0), "imputed_count_60m": np.sum(~obs60, axis=0),
            }
            up, dn, up_c, dn_c = neighbor_stats(cur[None, :], self.A, thr)
            now_c = (cur < thr).astype(np.float64)
            f["nbr_up_speed"] = np.where(np.isnan(up[0]), cur, up[0])
            f["nbr_down_speed"] = np.where(np.isnan(dn[0]), cur, dn[0])
            f["nbr_up_congested_frac"] = np.where(np.isnan(up_c[0]), now_c, up_c[0])
            f["nbr_down_congested_frac"] = np.where(np.isnan(dn_c[0]), now_c, dn_c[0])
            for c in STATIC_COLUMNS:
                f[c] = self.static[c]
            f["speed_ratio_ff"] = cur / self.static["ff_speed"]
            f["hist_speed_next"] = self.prof_speed[:, next_dow, next_slot]
            f["hist_congested_rate_next"] = self.prof_cong[:, next_dow, next_slot]
        df = pd.DataFrame({c: f[c] for c in FEATURE_COLUMNS})
        df.insert(0, "sensor_id", self.ids)
        df.insert(1, "ts_utc", pd.Timestamp(utc))
        df.insert(2, "ts_local", pd.Timestamp(local.replace(tzinfo=None)))
        df.insert(3, "date_local", local.date())
        df["slot"] = t
        df["produced_at_ms"] = [(readings or {}).get(i, (0.0, 0))[1] for i in range(self.n)]
        df["coverage"] = len(readings or {}) / self.n
        return df[keep].reset_index(drop=True)


class _quiet:
    """Silence numpy's 'All-NaN slice' RuntimeWarnings (an all-null window is a legitimate null feature)."""
    def __enter__(self):
        import warnings
        self._cm = warnings.catch_warnings()
        self._cm.__enter__()
        warnings.simplefilter("ignore", RuntimeWarning)

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)
