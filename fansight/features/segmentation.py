"""Segmentation helpers built on top of scikit-learn clustering."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler


@dataclass
class SegmentResult:
    assignments: pd.Series
    model: KMeans
    silhouette: float


def run_kmeans_segmentation(
    df: pd.DataFrame,
    features: Optional[list[str]] = None,
    n_segments: int = 6,
    random_state: int = 18,
) -> SegmentResult:
    """Cluster fans based on behavioral fields."""

    if len(df) < 3:
        raise ValueError("Segmentation requires at least three records.")
    n_segments = max(2, min(n_segments, len(df) - 1))

    if features is None:
        features = [
            "loyalty_score",
            "engagement_score",
            "avg_spend",
            "price_sensitivity",
            "touch_count_30d",
        ]

    available = [f for f in features if f in df]
    if not available:
        raise ValueError("No overlapping features for segmentation.")

    matrix = df[available].fillna(df[available].median())
    scaled = StandardScaler().fit_transform(matrix)
    model = KMeans(n_clusters=n_segments, random_state=random_state, n_init="auto")
    labels = model.fit_predict(scaled)
    metric = silhouette_score(scaled, labels)
    return SegmentResult(assignments=pd.Series(labels, index=df.index, name="segment_id"), model=model, silhouette=metric)
