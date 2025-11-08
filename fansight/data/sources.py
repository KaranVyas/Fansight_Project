"""Data access helpers for FanSight."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from fansight.config import DEFAULT_CONFIG, ProjectConfig
from fansight.data import schemas
from fansight.utils.io import load_csv


def _validate(df: pd.DataFrame, schema: schemas.TableSchema) -> None:
    diff = schema.validate(df.columns.tolist())
    if diff["missing"]:
        missing = ", ".join(diff["missing"])
        raise ValueError(f"{schema.name} missing columns: {missing}")


def load_games(
    path: Optional[Path] = None,
    *,
    config: ProjectConfig = DEFAULT_CONFIG,
) -> pd.DataFrame:
    """Load the core game-level dataset."""

    path = path or (config.paths.processed / "games.csv")
    df = load_csv(path, parse_dates=["game_date"])
    _validate(df, schemas.GAME_SCHEMA)
    return df


def load_fans(
    path: Optional[Path] = None,
    *,
    config: ProjectConfig = DEFAULT_CONFIG,
) -> pd.DataFrame:
    """Load the fan dimension table."""

    path = path or (config.paths.processed / "fans.csv")
    df = load_csv(path)
    _validate(df, schemas.FAN_SCHEMA)
    return df


def load_campaign_touches(
    path: Optional[Path] = None,
    *,
    config: ProjectConfig = DEFAULT_CONFIG,
) -> pd.DataFrame:
    """Load campaign-level touchpoints."""

    path = path or (config.paths.processed / "campaign_touches.csv")
    df = load_csv(path, parse_dates=["touch_date"])
    _validate(df, schemas.CAMPAIGN_SCHEMA)
    return df


def load_all(
    *,
    config: ProjectConfig = DEFAULT_CONFIG,
    overrides: Optional[Dict[str, Path]] = None,
) -> Dict[str, pd.DataFrame]:
    """Convenience helper to pull every dataset."""

    overrides = overrides or {}
    return {
        "games": load_games(overrides.get("games"), config=config),
        "fans": load_fans(overrides.get("fans"), config=config),
        "campaigns": load_campaign_touches(overrides.get("campaigns"), config=config),
    }
