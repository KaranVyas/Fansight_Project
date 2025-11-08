"""A/B test utilities for promotion analysis."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Tuple

import numpy as np
import pandas as pd
from scipy import stats


Variant = Literal["control", "treatment"]


@dataclass
class ABResult:
    lift: float
    ci_low: float
    ci_high: float
    p_value: float


def summarize_by_variant(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Return aggregate statistics for each variant."""

    summary = (
        df.groupby("variant")[metric]
        .agg(["mean", "std", "count"])
        .rename(columns={"mean": "avg", "std": "std_dev"})
    )
    return summary


def run_ab_test(
    df: pd.DataFrame,
    metric: str = "attendance",
    *,
    variant_col: str = "variant",
    alpha: float = 0.05,
) -> ABResult:
    """Calculate lift, confidence interval, and p-value for a binary test."""

    control = df[df[variant_col] == "control"][metric].values
    treatment = df[df[variant_col] == "treatment"][metric].values
    if len(control) == 0 or len(treatment) == 0:
        raise ValueError("Both control and treatment samples are required.")

    lift = treatment.mean() - control.mean()
    pooled_var = (
        np.var(control, ddof=1) / len(control) + np.var(treatment, ddof=1) / len(treatment)
    )
    se = np.sqrt(pooled_var)
    t_stat = lift / se
    dfree = len(control) + len(treatment) - 2
    p_value = 2 * (1 - stats.t.cdf(abs(t_stat), df=dfree))
    margin = stats.t.ppf(1 - alpha / 2, df=dfree) * se
    return ABResult(lift=lift, ci_low=lift - margin, ci_high=lift + margin, p_value=p_value)
