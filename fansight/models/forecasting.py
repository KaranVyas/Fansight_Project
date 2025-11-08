"""Forecasting utilities for ticket sales and engagement."""

from __future__ import annotations

import joblib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

from fansight.config import DEFAULT_CONFIG, ProjectConfig
from fansight.features.engineering import build_feature_pipeline, prepare_training_matrices


@dataclass
class ForecastResults:
    model_path: Path
    mae: float
    r2: float


@dataclass
class AttendanceForecaster:
    """Wrapper that combines preprocessing and gradient boosting."""

    config: ProjectConfig = field(default_factory=lambda: DEFAULT_CONFIG)
    pipeline_: Optional[object] = None
    model_: Optional[GradientBoostingRegressor] = None

    def fit(self, dataset: pd.DataFrame) -> "AttendanceForecaster":
        X, y = prepare_training_matrices(dataset, config=self.config)
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=self.config.model.test_size,
            random_state=self.config.model.random_state,
        )

        self.pipeline_ = build_feature_pipeline(config=self.config)
        X_train_transformed = self.pipeline_.fit_transform(X_train)
        X_test_transformed = self.pipeline_.transform(X_test)

        self.model_ = GradientBoostingRegressor(
            n_estimators=self.config.model.n_estimators,
            max_depth=self.config.model.max_depth,
            random_state=self.config.model.random_state,
        )
        self.model_.fit(X_train_transformed, y_train)

        preds = self.model_.predict(X_test_transformed)
        mae = float(mean_absolute_error(y_test, preds))
        if len(y_test) >= 2:
            r2 = float(r2_score(y_test, preds))
        else:
            r2 = float("nan")
        self._latest_metrics = {"mae": mae, "r2": r2}
        return self

    def predict(self, df: pd.DataFrame) -> pd.Series:
        if self.pipeline_ is None or self.model_ is None:
            raise RuntimeError("Model not fit yet.")
        X, _ = prepare_training_matrices(df, config=self.config)
        transformed = self.pipeline_.transform(X)
        return pd.Series(self.model_.predict(transformed), index=df.index, name="attendance_pred")

    def save(self, path: Optional[Path] = None) -> Path:
        if self.pipeline_ is None or self.model_ is None:
            raise RuntimeError("Nothing to save.")
        path = path or (self.config.paths.artifacts / "attendance_model.joblib")
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump({"pipeline": self.pipeline_, "model": self.model_}, path)
        return path

    def evaluate(self) -> Tuple[float, float]:
        if not hasattr(self, "_latest_metrics"):
            raise RuntimeError("Run fit before evaluate.")
        return self._latest_metrics["mae"], self._latest_metrics["r2"]

    @classmethod
    def load(cls, path: Path) -> "AttendanceForecaster":
        payload = joblib.load(path)
        instance = cls()
        instance.pipeline_ = payload["pipeline"]
        instance.model_ = payload["model"]
        return instance
