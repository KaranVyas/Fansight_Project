"""Utility helpers for loading and saving FanSight datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from fansight.config import DEFAULT_CONFIG, ProjectConfig


def load_csv(
    path: Path,
    parse_dates: Optional[Iterable[str]] = None,
    dtype: Optional[dict] = None,
) -> pd.DataFrame:
    """Load a CSV file into a dataframe with optional parsing."""

    if not path.exists():
        raise FileNotFoundError(f"Expected dataset at {path}")
    return pd.read_csv(path, parse_dates=list(parse_dates or ()), dtype=dtype)


def save_dataframe(
    df: pd.DataFrame,
    path: Path,
    create_dirs: bool = True,
) -> None:
    """Persist a dataframe as CSV."""

    if create_dirs:
        path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def get_processed_path(
    name: str,
    config: ProjectConfig = DEFAULT_CONFIG,
) -> Path:
    """Return a canonical processed-data path for a given dataset name."""

    return config.paths.processed / f"{name}.csv"
