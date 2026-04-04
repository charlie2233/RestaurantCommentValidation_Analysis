"""Outlier and multivariate anomaly checks for syntheticness analysis."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from qsr_audit.validate.syntheticness_reporting import SyntheticnessSignal

UNIVARIATE_OUTLIER_THRESHOLD = 3.0
ISOLATION_FOREST_MIN_ROWS = 12


@dataclass(frozen=True)
class DerivedMetricFrame:
    """Derived metrics used by anomaly checks."""

    frame: pd.DataFrame
    feature_columns: tuple[str, ...]


def derive_metric_frame(core_brand_metrics: pd.DataFrame) -> DerivedMetricFrame:
    """Build a normalized metric frame for anomaly checks."""

    frame = core_brand_metrics.loc[
        :,
        [
            "brand_name",
            "row_number",
            "us_store_count_2024",
            "systemwide_revenue_usd_billions_2024",
            "average_unit_volume_usd_thousands",
            "fte_mid",
            "margin_mid_pct",
        ],
    ].copy()

    frame["implied_auv_k"] = (
        frame["systemwide_revenue_usd_billions_2024"].astype(float)
        * 1_000_000
        / frame["us_store_count_2024"].astype(float)
    )
    frame["revenue_per_store_usd_m"] = (
        frame["systemwide_revenue_usd_billions_2024"].astype(float)
        * 1000
        / frame["us_store_count_2024"].astype(float)
    )
    frame["recorded_auv_k"] = frame["average_unit_volume_usd_thousands"].astype(float)
    frame["fte_mid"] = frame["fte_mid"].astype(float)
    frame["margin_mid_pct"] = frame["margin_mid_pct"].astype(float)
    frame["store_count"] = frame["us_store_count_2024"].astype(float)
    frame["system_sales_b"] = frame["systemwide_revenue_usd_billions_2024"].astype(float)

    feature_columns = (
        "recorded_auv_k",
        "implied_auv_k",
        "revenue_per_store_usd_m",
        "fte_mid",
        "margin_mid_pct",
        "store_count",
        "system_sales_b",
    )
    return DerivedMetricFrame(frame=frame, feature_columns=feature_columns)


def analyze_univariate_outliers(core_brand_metrics: pd.DataFrame) -> list[SyntheticnessSignal]:
    """Flag brands that are extreme on one or more derived metrics."""

    derived = derive_metric_frame(core_brand_metrics)
    frame = derived.frame
    signals: list[SyntheticnessSignal] = []
    any_flagged = False

    for field_name in ("implied_auv_k", "revenue_per_store_usd_m", "fte_mid", "margin_mid_pct"):
        series = frame[field_name].astype(float)
        robust_z = _robust_z_scores(series)
        lower_fence, upper_fence = _iqr_fences(series)
        metric_flags = []

        for row_index, value in series.items():
            z_score = float(robust_z.loc[row_index])
            is_iqr_outlier = value < lower_fence or value > upper_fence
            is_z_outlier = abs(z_score) >= UNIVARIATE_OUTLIER_THRESHOLD
            if not is_iqr_outlier and not is_z_outlier:
                continue

            any_flagged = True
            brand_name = str(frame.loc[row_index, "brand_name"])
            row_number = int(frame.loc[row_index, "row_number"])
            strength = "moderate" if is_iqr_outlier and is_z_outlier else "weak"
            metric_flags.append(
                {
                    "brand_name": brand_name,
                    "row_number": row_number,
                    "value": float(value),
                    "robust_z": z_score,
                }
            )
            signals.append(
                SyntheticnessSignal(
                    signal_type="univariate_outlier",
                    title=f"{brand_name} stands out on {field_name}",
                    plain_english=(
                        f"{brand_name} is unusually extreme on `{field_name}` relative to the other Top 30 brands."
                    ),
                    strength=strength,
                    dataset="core_brand_metrics",
                    field_name=field_name,
                    method="robust z-score + IQR fence",
                    sample_size=len(series),
                    score=float(value),
                    z_score=z_score,
                    threshold=UNIVARIATE_OUTLIER_THRESHOLD,
                    observed=f"{value:.3f}",
                    expected=f"Within [{lower_fence:.3f}, {upper_fence:.3f}] or |robust z| < {UNIVARIATE_OUTLIER_THRESHOLD:.1f}",
                    interpretation=(
                        "An extreme value can reflect a true business outlier, a definition mismatch, or a synthetic placeholder."
                    ),
                    caveat=(
                        "Outliers are not inherently suspicious; they become more useful when several unrelated checks point in the same direction."
                    ),
                    details={"brand_name": brand_name, "row_number": row_number},
                )
            )

        if metric_flags:
            signals.append(
                SyntheticnessSignal(
                    signal_type="univariate_outlier_summary",
                    title=f"Outlier summary for {field_name}",
                    plain_english=(
                        f"{len(metric_flags)} brand(s) look unusually extreme on `{field_name}`."
                    ),
                    strength="weak",
                    dataset="core_brand_metrics",
                    field_name=field_name,
                    method="robust z-score + IQR fence",
                    sample_size=len(series),
                    details={"flagged_rows": metric_flags},
                )
            )

    if not any_flagged:
        signals.append(
            SyntheticnessSignal(
                signal_type="univariate_outlier_summary",
                title="Univariate outlier summary",
                plain_english=(
                    "No major univariate outliers were detected across implied AUV, revenue per store, FTE midpoint, and margin midpoint."
                ),
                strength="unknown",
                dataset="core_brand_metrics",
                method="robust z-score + IQR fence",
                sample_size=len(frame),
            )
        )

    return signals


def analyze_isolation_forest(
    core_brand_metrics: pd.DataFrame,
    *,
    random_state: int = 42,
) -> list[SyntheticnessSignal]:
    """Run an optional multivariate anomaly model."""

    derived = derive_metric_frame(core_brand_metrics)
    feature_frame = derived.frame.loc[:, derived.feature_columns].dropna().astype(float)
    if len(feature_frame) < ISOLATION_FOREST_MIN_ROWS or feature_frame.shape[1] < 3:
        return [
            SyntheticnessSignal(
                signal_type="isolation_forest",
                title="Isolation Forest multivariate check",
                plain_english=(
                    "Skipped the multivariate anomaly model because there were not enough complete rows or numeric features."
                ),
                strength="unknown",
                dataset="core_brand_metrics",
                method="Isolation Forest",
                sample_size=len(feature_frame),
                caveat=("Tree-based anomaly models become unstable when the sample is very small."),
            )
        ]

    contamination = min(0.15, max(0.05, 2 / len(feature_frame)))
    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=random_state,
    )
    labels = model.fit_predict(feature_frame)
    scores = -model.decision_function(feature_frame)
    flagged_index = feature_frame.index[labels == -1]

    if len(flagged_index) == 0:
        return [
            SyntheticnessSignal(
                signal_type="isolation_forest",
                title="Isolation Forest multivariate check",
                plain_english=(
                    "The multivariate anomaly model did not isolate any brands as unusually synthetic-looking on the selected metrics."
                ),
                strength="unknown",
                dataset="core_brand_metrics",
                method="Isolation Forest",
                sample_size=len(feature_frame),
                threshold=contamination,
            )
        ]

    summary_rows = []
    signals = [
        SyntheticnessSignal(
            signal_type="isolation_forest_summary",
            title="Isolation Forest multivariate summary",
            plain_english=(
                f"The multivariate anomaly model isolated {len(flagged_index)} brand(s) as unusual relative to the full Top 30 profile."
            ),
            strength="weak",
            dataset="core_brand_metrics",
            method="Isolation Forest",
            sample_size=len(feature_frame),
            threshold=contamination,
        )
    ]

    for row_index in flagged_index:
        brand_name = str(derived.frame.loc[row_index, "brand_name"])
        row_number = int(derived.frame.loc[row_index, "row_number"])
        anomaly_score = float(scores[feature_frame.index.get_loc(row_index)])
        strength = "moderate" if anomaly_score >= float(np.quantile(scores, 0.9)) else "weak"
        summary_rows.append(
            {
                "brand_name": brand_name,
                "row_number": row_number,
                "anomaly_score": anomaly_score,
            }
        )
        signals.append(
            SyntheticnessSignal(
                signal_type="isolation_forest",
                title=f"{brand_name} is unusual in the multivariate profile",
                plain_english=(
                    f"{brand_name} sits in a sparse part of the joint metric space when implied AUV, revenue per store, FTE midpoint, margin midpoint, store count, and system sales are considered together."
                ),
                strength=strength,
                dataset="core_brand_metrics",
                field_name="multivariate",
                method="Isolation Forest",
                sample_size=len(feature_frame),
                score=anomaly_score,
                threshold=contamination,
                interpretation=(
                    "This model is best treated as a ranking aid for analyst review, not as a proof engine."
                ),
                caveat=(
                    "Isolation Forest can flag genuine business leaders or unusual formats, especially in small datasets."
                ),
                details={"brand_name": brand_name, "row_number": row_number},
            )
        )

    signals[0] = SyntheticnessSignal(
        signal_type="isolation_forest_summary",
        title="Isolation Forest multivariate summary",
        plain_english=(
            f"The multivariate anomaly model isolated {len(flagged_index)} brand(s) as unusual relative to the full Top 30 profile."
        ),
        strength="weak",
        dataset="core_brand_metrics",
        method="Isolation Forest",
        sample_size=len(feature_frame),
        threshold=contamination,
        details={"flagged_rows": summary_rows},
    )
    return signals


def _robust_z_scores(series: pd.Series) -> pd.Series:
    median = float(series.median())
    mad = float((series - median).abs().median())
    if mad == 0:
        return pd.Series(np.zeros(len(series)), index=series.index, dtype=float)
    return 0.6745 * (series - median) / mad


def _iqr_fences(series: pd.Series) -> tuple[float, float]:
    q1 = float(series.quantile(0.25))
    q3 = float(series.quantile(0.75))
    iqr = q3 - q1
    return q1 - 1.5 * iqr, q3 + 1.5 * iqr


__all__ = [
    "DerivedMetricFrame",
    "ISOLATION_FOREST_MIN_ROWS",
    "UNIVARIATE_OUTLIER_THRESHOLD",
    "analyze_isolation_forest",
    "analyze_univariate_outliers",
    "derive_metric_frame",
]
