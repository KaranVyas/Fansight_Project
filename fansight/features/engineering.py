"""Feature engineering utilities for FanSight."""

from __future__ import annotations

import logging
from typing import Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from fansight.config import DEFAULT_CONFIG, ProjectConfig

LOGGER = logging.getLogger(__name__)


def add_behavioral_features(df: pd.DataFrame) -> pd.DataFrame:
    """Derive behavioral metrics such as loyalty delta and price response."""

    df = df.copy()
    if "attendance" in df.columns and "capacity" in df.columns:
        df["sell_through_rate"] = df["attendance"] / df["capacity"].replace(0, np.nan)

    if {"loyalty_score", "avg_spend"}.issubset(df.columns):
        df["loyalty_value_ratio"] = df["lifetime_value"] / (df["loyalty_score"] + 1e-3)

    if {"ticket_price", "price_sensitivity"}.issubset(df.columns):
        df["price_alignment"] = df["ticket_price"] * (1 - df["price_sensitivity"])

    return df


def build_feature_pipeline(
    *,
    config: ProjectConfig = DEFAULT_CONFIG,
) -> ColumnTransformer:
    """Create a preprocessing pipeline for model-ready features."""

    categorical = [c for c in config.features.categorical]
    numeric = [n for n in config.features.numerical]

    cat_pipeline = Pipeline(
        steps=[
            ("impute", SimpleImputer(strategy="most_frequent")),
            ("encode", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    num_pipeline = Pipeline(
        steps=[
            ("impute", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
        ]
    )

    transformer = ColumnTransformer(
        transformers=[
            ("categorical", cat_pipeline, categorical),
            ("numerical", num_pipeline, numeric),
        ]
    )
    return transformer


def prepare_training_matrices(
    df: pd.DataFrame,
    *,
    config: ProjectConfig = DEFAULT_CONFIG,
) -> Tuple[pd.DataFrame, pd.Series]:
    """Return X and y for modeling."""

    df = add_behavioral_features(df)
    missing = [col for col in config.features.categorical + config.features.numerical if col not in df]
    if missing:
        LOGGER.warning("Dropping missing columns: %s", ", ".join(missing))
    X = df[[c for c in config.features.categorical + config.features.numerical if c in df]].copy()
    y = df[config.features.target].copy()
    return X, y
