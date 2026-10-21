"""Canonical project paths. Every script resolves data locations from here."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.environ.get("TRAFFICSENSE_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
REPORTS_DIR = PROJECT_ROOT / "reports"
VALIDATION_REPORT_DIR = REPORTS_DIR / "validation"
FIGURES_DIR = REPORTS_DIR / "figures"
# Spark spill files and temporary staging. Must be on a real disk: WSL's /tmp is a small RAM-backed tmpfs.
SCRATCH_DIR = Path(os.environ.get("TRAFFICSENSE_SCRATCH_DIR", Path.home() / ".cache" / "trafficsense"))


def dataset_slug(dataset: str) -> str:
    """'pems-bay' -> 'pems_bay' (directory-safe name)."""
    return dataset.lower().replace("-", "_")


def raw_dataset_dir(dataset: str, raw_dir: Path = RAW_DIR) -> Path:
    return Path(raw_dir) / dataset_slug(dataset)


def processed_dataset_dir(dataset: str, processed_dir: Path = PROCESSED_DIR) -> Path:
    return Path(processed_dir) / dataset_slug(dataset)
