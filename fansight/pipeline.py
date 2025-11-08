"""High-level orchestration for FanSight workflows."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import pandas as pd

from fansight import config
from fansight.data import etl, sources
from fansight.features import engineering, segmentation
from fansight.marketing import ab_testing
from fansight.models.forecasting import AttendanceForecaster
from fansight.reporting import dashboards

LOGGER = logging.getLogger(__name__)


@dataclass
class FanSightPipeline:
    """Bundle together ETL, modeling, segmentation, and reporting."""

    cfg: config.ProjectConfig = field(default_factory=lambda: config.DEFAULT_CONFIG)
    dataset_name: str = "fansight_master"
    dataset_: Optional[pd.DataFrame] = None
    model_: Optional[AttendanceForecaster] = None
    segment_result_: Optional[segmentation.SegmentResult] = None

    def run_etl(self) -> pd.DataFrame:
        LOGGER.info("Running FanSight ETL for dataset %s", self.dataset_name)
        dataset, path = etl.build_and_save_dataset(self.dataset_name, config=self.cfg)
        LOGGER.info("Saved processed dataset to %s", path)
        self.dataset_ = dataset
        return dataset

    def run_modeling(self) -> AttendanceForecaster:
        if self.dataset_ is None:
            raise RuntimeError("Call run_etl before modeling.")
        LOGGER.info("Training attendance forecaster.")
        model = AttendanceForecaster(config=self.cfg)
        model.fit(self.dataset_)
        model_path = model.save()
        mae, r2 = model.evaluate()
        LOGGER.info("Model saved to %s (MAE=%.2f, R2=%.3f)", model_path, mae, r2)
        self.model_ = model
        return model

    def run_segmentation(self, n_segments: Optional[int] = None) -> segmentation.SegmentResult:
        if self.dataset_ is None:
            raise RuntimeError("Dataset unavailable for segmentation.")
        LOGGER.info("Running segmentation.")
        result = segmentation.run_kmeans_segmentation(
            self.dataset_,
            n_segments=n_segments or self.cfg.model.segment_k,
        )
        self.segment_result_ = result
        LOGGER.info("Segmentation silhouette score: %.3f", result.silhouette)
        return result

    def run_ab_testing(
        self,
        *,
        variant_col: str = "variant",
        metric: str = "attendance",
    ) -> Optional[ab_testing.ABResult]:
        if self.dataset_ is None or variant_col not in self.dataset_:
            LOGGER.warning("Variant column missing; skipping A/B test.")
            return None
        LOGGER.info("Evaluating A/B experiment on %s.", metric)
        return ab_testing.run_ab_test(self.dataset_, metric=metric, variant_col=variant_col)

    def build_dashboard(self) -> Dict[str, Any]:
        if self.dataset_ is None:
            raise RuntimeError("No dataset loaded for dashboard creation.")
        preds = None
        feature_importances = None
        if self.model_ is not None:
            preds = self.model_.predict(self.dataset_)
        if self.model_ and hasattr(self.model_.model_, "feature_importances_"):
            feature_importances = {
                f"f_{i}": float(imp)
                for i, imp in enumerate(self.model_.model_.feature_importances_)
            }
        return dashboards.build_dashboard(
            self.dataset_,
            model_predictions=preds,
            feature_importances=feature_importances,
        )
