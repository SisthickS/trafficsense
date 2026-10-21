"""Small helpers shared by train_model.py and evaluate_model.py: tables, figures, JSON + Markdown reports."""
from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from src.analytics.common import df_to_md, plt_setup, setup_logging  # noqa: F401  (re-exported)
from src.ml.config import ML_FIG_DIR, ML_REPORT_DIR, ML_TABLE_DIR

log = logging.getLogger("trafficsense.ml")


def write_table(df: pd.DataFrame, name: str) -> str:
    ML_TABLE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(ML_TABLE_DIR / f"{name}.csv", index=False)
    return f"tables/{name}.csv"


def save_fig(fig, name: str) -> str:
    import matplotlib.pyplot as plt
    ML_FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(ML_FIG_DIR / f"{name}.png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    return f"../figures/ml/{name}.png"


def _json_default(o):
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.floating):
        return None if np.isnan(o) else float(o)
    if isinstance(o, np.bool_):
        return bool(o)
    return str(o)


def write_report(name: str, markdown: str, payload: dict) -> Path:
    ML_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    (ML_REPORT_DIR / f"{name}.json").write_text(json.dumps(payload, indent=2, default=_json_default) + "\n")
    path = ML_REPORT_DIR / f"{name}.md"
    path.write_text(markdown.rstrip() + "\n", encoding="utf-8")
    return path
