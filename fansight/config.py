"""Project-level configuration for the FanSight pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class DataPaths:
    """Holds canonical locations for all pipeline assets."""

    raw: Path = PROJECT_ROOT / "data" / "raw"
    processed: Path = PROJECT_ROOT / "data" / "processed"
    artifacts: Path = PROJECT_ROOT / "fansight_artifacts"
    cache: Path = PROJECT_ROOT / "fansight_artifacts" / "cache"


@dataclass(frozen=True)
class FeatureConfig:
    """Default feature views used across FanSight modules."""

    categorical: List[str] = field(
        default_factory=lambda: [
            "home_team",
            "visitor_team",
            "segment",
            "campaign_channel",
            "promotion_flag",
            "day_of_week",
            "month",
            "is_rivalry",
        ]
    )
    numerical: List[str] = field(
        default_factory=lambda: [
            "game_id",
            "fan_id",
            "ticket_price",
            "engagement_score",
            "loyalty_score",
            "tenure_days",
            "avg_spend",
            "lifetime_value",
            "price_sensitivity",
            "campaign_spend",
            "touch_count_7d",
            "touch_count_30d",
            "win_pct_home",
            "win_pct_visitor",
            "attendance_lag_1",
            "attendance_lag_3",
            "capacity",
        ]
    )
    target: str = "attendance"


@dataclass(frozen=True)
class ModelConfig:
    """Parameters shared across model training and evaluation."""

    test_size: float = 0.2
    random_state: int = 42
    n_estimators: int = 400
    max_depth: Optional[int] = 8
    forecast_horizon: int = 3
    segment_k: int = 6


@dataclass(frozen=True)
class ProjectConfig:
    """Aggregates all configuration dataclasses."""

    paths: DataPaths = field(default_factory=DataPaths)
    features: FeatureConfig = field(default_factory=FeatureConfig)
    model: ModelConfig = field(default_factory=ModelConfig)


DEFAULT_CONFIG = ProjectConfig()


def ensure_directories(config: ProjectConfig = DEFAULT_CONFIG) -> None:
    """Create folders required by downstream pipeline steps."""

    for path in [
        config.paths.artifacts,
        config.paths.cache,
        config.paths.processed,
    ]:
        path.mkdir(parents=True, exist_ok=True)
