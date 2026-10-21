"""Cost of missed congestion versus false alarms.

A missed congestion (false negative) means drivers / operators get no warning; a false alarm (false positive) is a
needless alert. Costs are in arbitrary units per event (default FN : FP = 10 : 1) and are configurable. An alert is
worth sending when P(congestion) > FP / (FP + FN); MLlib probabilities from class-weighted training are not calibrated,
so the operating threshold is tuned empirically on VALIDATION and applied unchanged to TEST.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class CostModel:
    fn: float = 10.0
    fp: float = 1.0

    @property
    def ratio(self) -> float:
        return self.fn / self.fp

    @property
    def breakeven_probability(self) -> float:
        return self.fp / (self.fp + self.fn)

    def cost(self, fn_count, fp_count):
        return self.fn * fn_count + self.fp * fp_count


def best_cost_bucket(c: pd.DataFrame, cm: CostModel) -> int:
    """Bucket whose threshold minimizes total cost on the curve (ties -> the higher threshold, fewer alerts)."""
    cost = cm.cost(c["fn"].to_numpy(), c["fp"].to_numpy())
    return int(len(cost) - 1 - np.argmin(cost[::-1]))


def operating_point(c: pd.DataFrame, bucket: int, cm: CostModel) -> dict:
    """Confusion matrix, rates and cost at one threshold of a curve (counts are exact for the bucket boundary)."""
    r = c.iloc[bucket]
    tp, fp, fn, tn = (float(r[k]) for k in ("tp", "fp", "fn", "tn"))
    n = tp + fp + fn + tn
    cost = cm.cost(fn, fp)
    return {"threshold": float(r["threshold"]), "tp": tp, "fp": fp, "fn": fn, "tn": tn, "rows": n,
            "precision": float(r["precision"]), "recall": float(r["recall"]), "f1": float(r["f1"]),
            "false_alarm_rate": fp / (fp + tn) if fp + tn else 0.0, "miss_rate": fn / (tp + fn) if tp + fn else 0.0,
            "cost": cost, "cost_per_1000": 1000 * cost / n if n else 0.0}


def profile(hist: pd.DataFrame, cm: CostModel) -> dict:
    """Curve, AUCs and the two operating points (F1-optimal, cost-optimal) of one scored histogram."""
    from src.modeling import evaluation as ev
    c = ev.curve(hist)
    pr_auc, roc_auc = ev.auc(c)
    b_f1, b_cost = ev.best_bucket(c), best_cost_bucket(c, cm)
    return {"curve": c, "pr_auc": pr_auc, "roc_auc": roc_auc, "bucket_f1": b_f1, "bucket_cost": b_cost,
            "f1_point": operating_point(c, b_f1, cm), "cost_point": operating_point(c, b_cost, cm)}


def reference_costs(c: pd.DataFrame, cm: CostModel) -> dict:
    """Trivial policies: never alert (miss every congestion) and always alert (false-alarm every free reading)."""
    pos, neg = float(c["tp"].iloc[0] + c["fn"].iloc[0]), float(c["fp"].iloc[0] + c["tn"].iloc[0])
    n = pos + neg
    return {"never_alert_cost": cm.fn * pos, "always_alert_cost": cm.fp * neg,
            "never_alert_cost_per_1000": 1000 * cm.fn * pos / n, "always_alert_cost_per_1000": 1000 * cm.fp * neg / n}


def sensitivity(c_val: pd.DataFrame, c_test: pd.DataFrame, ratios: list[float], fp_cost: float = 1.0) -> pd.DataFrame:
    """For each FN:FP ratio: threshold re-tuned on validation, cost measured on test."""
    rows = []
    for ratio in ratios:
        cm = CostModel(fn=ratio * fp_cost, fp=fp_cost)
        b = best_cost_bucket(c_val, cm)
        op = operating_point(c_test, b, cm)
        rows.append({"fn_fp_ratio": ratio, "threshold": op["threshold"], "cost_per_1000": op["cost_per_1000"],
                     "recall": op["recall"], "precision": op["precision"], "alerts_per_1000": 1000 * (op["tp"] + op["fp"]) / op["rows"]})
    return pd.DataFrame(rows)


def bootstrap_cost_saving(daily: dict[str, pd.DataFrame], baseline: str, cm: CostModel, n_boot: int = 1000,
                          seed: int = 42, day_col: str = "date_local") -> pd.DataFrame:
    """Day-block bootstrap of (baseline cost - model cost); positive = the model is cheaper than the baseline."""
    days = sorted(set.intersection(*[set(d[day_col]) for d in daily.values()]))
    cost = {m: cm.cost(d.set_index(day_col).loc[days, "fn"].to_numpy(), d.set_index(day_col).loc[days, "fp"].to_numpy())
            for m, d in daily.items()}
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(days), size=(n_boot, len(days)))
    base = cost[baseline][idx].sum(axis=1)
    rows = []
    for m, v in cost.items():
        saving = base - v[idx].sum(axis=1)
        point = cost[baseline].sum() - v.sum()
        rows.append({"model": m, "total_cost": float(v.sum()), "saving_vs_" + baseline: float(point),
                     "saving_pct": float(point / cost[baseline].sum()) if cost[baseline].sum() else np.nan,
                     "saving_ci_low": float(np.percentile(saving, 2.5)), "saving_ci_high": float(np.percentile(saving, 97.5)),
                     "share_boot_cheaper": float((saving > 0).mean()), "days": len(days)})
    return pd.DataFrame(rows)
