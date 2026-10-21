"""Histogram-based evaluation: one Spark aggregation per (model, split) yields every metric.

scored DataFrame (label, congested_now, p, group columns)
  -> histogram(): counts per (groups, congested_now, label, probability bucket)   [Spark, one pass]
  -> curve(): precision / recall / F1 / FPR at every bucket threshold             [pandas]
  -> threshold tuned on VALIDATION (max F1), applied unchanged to TEST
  -> metrics(): precision, recall, F1, accuracy, PR-AUC (average precision), ROC-AUC, and transition metrics:
       onset recall      = recall on rows where congested_now = 0 and label = 1 (congestion starts)
       clearance accuracy = share predicted 0 where congested_now = 1 and label = 0 (congestion ends)
       transition accuracy = accuracy on all state-change rows (label != congested_now; persistence scores 0 here)
       false-alarm rate  = FPR on stable free-flowing rows (congested_now = 0 and label = 0)
  -> bootstrap_f1(): day-block bootstrap confidence intervals and differences vs a baseline
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.modeling.models import PROB
from src.preprocessing.build_features import LABEL

BINS = 500


def histogram(scored: DataFrame, groups: tuple[str, ...] = ("date_local",), prob: str = PROB) -> pd.DataFrame:
    bucket = F.least(F.floor(F.col(prob) * BINS), F.lit(BINS - 1)).cast("int")
    cols = list(groups) + ["congested_now", LABEL]
    return (scored.groupBy(*[F.col(c) for c in cols], bucket.alias("bucket")).count()
                  .toPandas().rename(columns={"count": "n"}))


def curve(hist: pd.DataFrame) -> pd.DataFrame:
    pos = np.bincount(hist.loc[hist[LABEL] == 1, "bucket"], weights=hist.loc[hist[LABEL] == 1, "n"], minlength=BINS)
    neg = np.bincount(hist.loc[hist[LABEL] == 0, "bucket"], weights=hist.loc[hist[LABEL] == 0, "n"], minlength=BINS)
    tp = pos[::-1].cumsum()[::-1]           # predicted positive when bucket >= b
    fp = neg[::-1].cumsum()[::-1]
    P, N = pos.sum(), neg.sum()
    with np.errstate(invalid="ignore", divide="ignore"):
        precision = np.where(tp + fp > 0, tp / (tp + fp), 1.0)
        recall = tp / P if P else np.zeros(BINS)
        f1 = np.where(precision + recall > 0, 2 * precision * recall / (precision + recall), 0.0)
        fpr = fp / N if N else np.zeros(BINS)
    return pd.DataFrame({"bucket": np.arange(BINS), "threshold": np.arange(BINS) / BINS, "tp": tp, "fp": fp,
                         "fn": P - tp, "tn": N - fp, "precision": precision, "recall": recall, "f1": f1, "fpr": fpr})


def auc(c: pd.DataFrame) -> tuple[float, float]:
    """(PR-AUC as average precision, ROC-AUC) from the bucketed curve (same binning idea as MLlib's numBins)."""
    r = np.append(c.recall.to_numpy(), 0.0)
    pr_auc = float(np.sum((r[:-1] - r[1:]) * c.precision.to_numpy()))
    fpr = np.concatenate([[1.0], c.fpr.to_numpy(), [0.0]])
    tpr = np.concatenate([[1.0], c.recall.to_numpy(), [0.0]])
    roc_auc = float(np.trapz(tpr[::-1], fpr[::-1]))
    return pr_auc, roc_auc


def best_bucket(c: pd.DataFrame) -> int:
    return int(c.loc[c.f1.idxmax(), "bucket"])


def _counts(h: pd.DataFrame, b: int) -> dict:
    pred = h.bucket >= b
    y = h[LABEL] == 1
    return {"tp": float(h.n[pred & y].sum()), "fp": float(h.n[pred & ~y].sum()),
            "fn": float(h.n[~pred & y].sum()), "tn": float(h.n[~pred & ~y].sum())}


def _prf(c: dict) -> dict:
    tp, fp, fn, tn = c["tp"], c["fp"], c["fn"], c["tn"]
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    n = tp + fp + fn + tn
    return {"precision": p, "recall": r, "f1": 2 * p * r / (p + r) if p + r else 0.0,
            "accuracy": (tp + tn) / n if n else 0.0, "rows": n, "positives": tp + fn}


def metrics(hist: pd.DataFrame, b: int, with_auc: bool = True) -> dict:
    out = _prf(_counts(hist, b))
    if with_auc:
        out["pr_auc"], out["roc_auc"] = auc(curve(hist))
    now = hist.congested_now == 1
    y = hist[LABEL] == 1
    onset = _counts(hist[~now & y], b)
    clear = _counts(hist[now & ~y], b)
    trans = _counts(hist[(now & ~y) | (~now & y)], b)
    stable_free = _counts(hist[~now & ~y], b)
    out.update(
        threshold=b / BINS,
        onset_rows=onset["tp"] + onset["fn"],
        onset_recall=onset["tp"] / (onset["tp"] + onset["fn"]) if onset["tp"] + onset["fn"] else np.nan,
        clearance_rows=clear["tn"] + clear["fp"],
        clearance_accuracy=clear["tn"] / (clear["tn"] + clear["fp"]) if clear["tn"] + clear["fp"] else np.nan,
        transition_accuracy=(trans["tp"] + trans["tn"]) / sum(trans.values()) if sum(trans.values()) else np.nan,
        false_alarm_rate=stable_free["fp"] / (stable_free["fp"] + stable_free["tn"]) if stable_free["fp"] + stable_free["tn"] else np.nan,
    )
    return out


def metrics_by(hist: pd.DataFrame, b: int, group, min_positives: int = 0) -> pd.DataFrame:
    """Metrics per segment; `group` is a column name or a list of column names."""
    keys = [group] if isinstance(group, str) else list(group)
    rows = []
    for key, h in hist.groupby(keys):
        key = key if isinstance(key, tuple) else (key,)
        m = metrics(h, b, with_auc=False)
        if m["positives"] >= min_positives:
            rows.append({**dict(zip(keys, key)), **m})
    return pd.DataFrame(rows)


def per_day(hist: pd.DataFrame, b: int, day_col: str = "date_local") -> pd.DataFrame:
    return pd.DataFrame([{day_col: d, **_counts(h, b)} for d, h in hist.groupby(day_col)])


def bootstrap_f1(daily: dict[str, pd.DataFrame], baseline: str, n_boot: int = 1000, seed: int = 42,
                 day_col: str = "date_local") -> pd.DataFrame:
    """Day-block bootstrap: resample test days with replacement (keeps within-day correlation)."""
    days = sorted(set.intersection(*[set(d[day_col]) for d in daily.values()]))
    mats = {m: d.set_index(day_col).loc[days, ["tp", "fp", "fn"]].to_numpy() for m, d in daily.items()}
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(days), size=(n_boot, len(days)))

    def f1(mat: np.ndarray, sample: np.ndarray) -> np.ndarray:
        s = mat[sample].sum(axis=1)          # (n_boot, 3)
        tp, fp, fn = s[:, 0], s[:, 1], s[:, 2]
        return 2 * tp / np.maximum(2 * tp + fp + fn, 1)

    base = f1(mats[baseline], idx)
    rows = []
    for m, mat in mats.items():
        f = f1(mat, idx)
        d = f - base
        point = f1(mat, np.arange(len(days))[None, :])[0]
        rows.append({"model": m, "f1": point, "f1_ci_low": np.percentile(f, 2.5), "f1_ci_high": np.percentile(f, 97.5),
                     "delta_vs_" + baseline: point - f1(mats[baseline], np.arange(len(days))[None, :])[0],
                     "delta_ci_low": np.percentile(d, 2.5), "delta_ci_high": np.percentile(d, 97.5),
                     "share_boot_better": float((d > 0).mean()), "days": len(days)})
    return pd.DataFrame(rows)


def calibration(hist: pd.DataFrame, n_bins: int = 10) -> tuple[pd.DataFrame, float, float]:
    """Reliability table, Brier score (bucket mid-points) and expected calibration error."""
    h = hist.copy()
    h["p_mid"] = (h.bucket + 0.5) / BINS
    h["bin"] = np.minimum((h.p_mid * n_bins).astype(int), n_bins - 1)
    h["y"] = h[LABEL]
    g = h.groupby("bin").apply(lambda x: pd.Series({"n": x.n.sum(), "mean_predicted": np.average(x.p_mid, weights=x.n),
                                                     "observed_rate": np.average(x.y, weights=x.n)}), include_groups=False)
    g = g.reset_index()
    brier = float(np.average((h.p_mid - h.y) ** 2, weights=h.n))
    ece = float(np.sum(g.n / g.n.sum() * (g.mean_predicted - g.observed_rate).abs()))
    return g, brier, ece
