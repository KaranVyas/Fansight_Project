"""Data engineering helpers that assemble the FanSight modeling table."""

from __future__ import annotations

import logging
from typing import Dict, Tuple, Union

import numpy as np
import pandas as pd

from fansight.config import DEFAULT_CONFIG, ProjectConfig, ensure_directories
from fansight.data import sources
from fansight.utils import io

LOGGER = logging.getLogger(__name__)


def _ensure_columns(
    df: pd.DataFrame, defaults: Dict[str, Union[float, int, str]]
) -> pd.DataFrame:
    """Add missing columns with default values."""

    for column, value in defaults.items():
        if column not in df.columns:
            df[column] = value
    return df


def _most_frequent(series: pd.Series, default: str = "unknown") -> str:
    if series.empty:
        return default
    mode = series.mode()
    if not mode.empty:
        return mode.iloc[0]
    return series.iloc[0]


def aggregate_campaign_touches(campaigns: pd.DataFrame) -> pd.DataFrame:
    """Aggregate campaign touches to a fan/game grain."""

    campaigns = campaigns.copy()
    defaults = {
        "game_id": np.nan,
        "campaign_spend": 0.0,
        "conversion": 0.0,
        "campaign_channel": "unknown",
        "promotion_flag": 0,
        "variant": "control",
    }
    campaigns = _ensure_columns(campaigns, defaults)

    campaigns["touch_date"] = pd.to_datetime(campaigns["touch_date"])
    campaigns["touch_count"] = 1

    grouped = (
        campaigns.groupby(["fan_id", "game_id"], dropna=False)
        .agg(
            {
                "touch_count": "sum",
                "campaign_spend": "sum",
                "conversion": "sum",
                "campaign_channel": lambda s: _most_frequent(s),
                "promotion_flag": "max",
                "variant": lambda s: _most_frequent(s, default="control"),
            }
        )
        .reset_index()
    )
    grouped.rename(
        columns={
            "touch_count": "touch_count_total",
            "campaign_spend": "campaign_spend",
            "conversion": "conversions",
        },
        inplace=True,
    )
    return grouped


def build_fan_game_dataset(
    games: pd.DataFrame,
    fans: pd.DataFrame,
    campaigns: pd.DataFrame,
    *,
    config: ProjectConfig = DEFAULT_CONFIG,
) -> pd.DataFrame:
    """Create the modeling table at a fan/game granularity."""

    ensure_directories(config)
    games = games.copy()
    fans = fans.copy()
    campaign_agg = aggregate_campaign_touches(campaigns)

    # Create every fan-game combination that has at least one touch
    merged = campaign_agg.merge(fans, on="fan_id", how="left")
    merged = merged.merge(
        games, on="game_id", how="left", suffixes=("_fan", "_game")
    )

    rename_map = {
        "campaign_channel_fan": "campaign_channel",
        "promotion_flag_fan": "promotion_flag",
        "home_team_game": "home_team",
    }
    merged.rename(columns=rename_map, inplace=True)
    if "home_team_fan" in merged.columns:
        merged.rename(columns={"home_team_fan": "home_team_preference"}, inplace=True)
    for redundant in ["campaign_channel_game", "promotion_flag_game"]:
        if redundant in merged.columns:
            merged.drop(columns=redundant, inplace=True)

    if merged["game_id"].isna().any():
        LOGGER.warning("Campaign touches reference unknown game_ids.")
        merged = merged.dropna(subset=["game_id"])

    merged["game_date"] = pd.to_datetime(merged["game_date"])
    merged = merged.sort_values(["fan_id", "game_date"]).reset_index(drop=True)

    # Fill time based helpers
    merged["touch_count_7d"] = (
        merged.groupby("fan_id")["touch_count_total"]
        .rolling(window=3, min_periods=1)
        .sum()
        .reset_index(level=0, drop=True)
    )

    merged["touch_count_30d"] = (
        merged.groupby("fan_id")["touch_count_total"]
        .rolling(window=5, min_periods=1)
        .sum()
        .reset_index(level=0, drop=True)
    )

    # Derive loyalty uplift proxy
    merged["loyalty_engagement"] = merged["loyalty_score"] * merged["engagement_score"]

    feature_cols = (
        config.features.categorical
        + config.features.numerical
        + [config.features.target]
    )
    metadata_cols = ["variant"]
    available_cols = [
        c for c in feature_cols + metadata_cols if c in merged.columns
    ]
    dataset = merged[available_cols].copy()
    dataset.dropna(subset=[config.features.target], inplace=True)

    return dataset


def build_and_save_dataset(
    name: str,
    *,
    config: ProjectConfig = DEFAULT_CONFIG,
) -> Tuple[pd.DataFrame, str]:
    """Full ETL routine that saves the processed dataset."""

    data_map = sources.load_all(config=config)
    dataset = build_fan_game_dataset(
        games=data_map["games"],
        fans=data_map["fans"],
        campaigns=data_map["campaigns"],
        config=config,
    )
    output_path = io.get_processed_path(name, config=config)
    io.save_dataframe(dataset, output_path)
    return dataset, str(output_path)
