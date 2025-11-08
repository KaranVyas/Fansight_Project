"""Plotting helpers for FanSight insights."""

from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

try:  # pragma: no cover - optional dependency
    import plotly.graph_objects as go
except ImportError:  # pragma: no cover - optional dependency
    go = None


def kpi_card(title: str, value: float, delta: Optional[float] = None) -> go.Figure:
    """Create a single KPI card figure."""

    if go is None:  # pragma: no cover - optional dependency
        raise ImportError("Plotly is required for dashboard rendering. Install via `pip install plotly>=5.0`.")

    fig = go.Figure(
        go.Indicator(
            mode="number+delta" if delta is not None else "number",
            value=value,
            delta={"reference": value - delta} if delta is not None else None,
            title={"text": title},
        )
    )
    fig.update_layout(height=150, margin=dict(l=20, r=20, t=30, b=10))
    return fig


def driver_bar_chart(feature_importances: Dict[str, float]) -> go.Figure:
    """Render a sorted bar chart of feature importances."""

    if go is None:  # pragma: no cover - optional dependency
        raise ImportError("Plotly is required for dashboard rendering. Install via `pip install plotly>=5.0`.")

    items = sorted(feature_importances.items(), key=lambda kv: kv[1], reverse=True)
    labels, values = zip(*items) if items else ([], [])
    fig = go.Figure(go.Bar(x=list(labels), y=list(values)))
    fig.update_layout(
        title="Key Drivers",
        xaxis_title="Feature",
        yaxis_title="Importance",
        template="plotly_white",
    )
    return fig


def build_dashboard(
    df: pd.DataFrame,
    *,
    kpi_metric: str = "attendance",
    model_predictions: Optional[pd.Series] = None,
    feature_importances: Optional[Dict[str, float]] = None,
) -> Dict[str, go.Figure]:
    """Return a dictionary of Plotly figures for quick consumption."""

    figures = {}
    figures["kpi"] = kpi_card(
        title=f"Avg {kpi_metric.title()}",
        value=float(df[kpi_metric].mean()),
    )

    if model_predictions is not None:
        lift = float(model_predictions.mean() - df[kpi_metric].mean())
        figures["model_gap"] = kpi_card("Predicted Lift", model_predictions.mean(), delta=lift)

    if feature_importances:
        figures["drivers"] = driver_bar_chart(feature_importances)

    return figures
