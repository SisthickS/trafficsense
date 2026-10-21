"""Locations and labels shared by train_model.py, evaluate_model.py and model_io.py."""
from __future__ import annotations

from src.common.paths import REPORTS_DIR
from src.modeling.data import model_dir

ML_MODEL_DIR = model_dir() / "ml_pipeline"          # one bundle per algorithm + best/ + selection.json
SELECTION_FILE = ML_MODEL_DIR / "selection.json"
ML_REPORT_DIR = REPORTS_DIR / "ml"
ML_TABLE_DIR = ML_REPORT_DIR / "tables"
ML_FIG_DIR = REPORTS_DIR / "figures" / "ml"

ALGORITHMS = {
    "logistic_regression": "Logistic regression",
    "decision_tree": "Decision tree",
    "random_forest": "Random forest",
}
