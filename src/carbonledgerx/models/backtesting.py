"""Walk-forward backtesting utilities for statistical annual forecasts."""

from __future__ import annotations

from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


NAIVE_MODEL_NAME = "naive_last_value"
TREND_MODEL_NAME = "linear_trend"
MODEL_NAMES = [NAIVE_MODEL_NAME, TREND_MODEL_NAME]
BACKTEST_TARGET_YEARS = [2022, 2023, 2024]
NOMINAL_INTERVAL_COVERAGE_PCT = 80.0
MIN_RELATIVE_INTERVAL_WIDTH_PCT = 8.0
INTERVAL_EXPANSION_PER_YEAR_PCT = 10.0


def build_company_forecast_backtest_results(
    *,
    history_annual: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build walk-forward annual backtest results for naive and trend models."""

    if history_annual is None:
        history_annual = _read_processed_table("company_emissions_history_annual.parquet")

    backtest_rows: list[dict[str, Any]] = []
    for _, company_history in history_annual.groupby("company_id", sort=True):
        sorted_history = company_history.sort_values("history_year", kind="stable").reset_index(drop=True)
        available_years = set(pd.to_numeric(sorted_history["history_year"], errors="coerce").dropna().astype(int))

        for actual_year in BACKTEST_TARGET_YEARS:
            if actual_year not in available_years:
                continue

            train_history = sorted_history.loc[sorted_history["history_year"] < actual_year].copy()
            test_history = sorted_history.loc[sorted_history["history_year"] == actual_year].copy()
            if train_history.shape[0] < 2 or test_history.empty:
                continue

            actual_total_mb_tco2e = float(pd.to_numeric(test_history.iloc[0]["total_mb_tco2e"], errors="coerce"))
            train_years = train_history["history_year"].astype(int).tolist()
            train_values = pd.to_numeric(train_history["total_mb_tco2e"], errors="coerce").astype(float).tolist()
            train_end_year = max(train_years)

            for model_name in MODEL_NAMES:
                predicted_total_mb_tco2e = predict_univariate_series(
                    years=train_years,
                    values=train_values,
                    target_year=actual_year,
                    model_name=model_name,
                )
                lower_bound_total_mb_tco2e, upper_bound_total_mb_tco2e = build_prediction_interval(
                    years=train_years,
                    values=train_values,
                    target_year=actual_year,
                    predicted_value=predicted_total_mb_tco2e,
                    model_name=model_name,
                )
                abs_error = abs(actual_total_mb_tco2e - predicted_total_mb_tco2e)
                ape_pct = (abs_error / actual_total_mb_tco2e * 100.0) if actual_total_mb_tco2e > 0 else 0.0

                backtest_rows.append(
                    {
                        "company_id": test_history.iloc[0]["company_id"],
                        "company_name": test_history.iloc[0]["company_name"],
                        "sector": test_history.iloc[0]["sector"],
                        "country": test_history.iloc[0]["country"],
                        "train_end_year": train_end_year,
                        "actual_year": actual_year,
                        "model_name": model_name,
                        "actual_total_mb_tco2e": round(actual_total_mb_tco2e, 3),
                        "predicted_total_mb_tco2e": round(predicted_total_mb_tco2e, 3),
                        "lower_bound_total_mb_tco2e": round(lower_bound_total_mb_tco2e, 3),
                        "upper_bound_total_mb_tco2e": round(upper_bound_total_mb_tco2e, 3),
                        "abs_error": round(abs_error, 3),
                        "ape_pct": round(ape_pct, 3),
                        "within_interval_flag": bool(
                            lower_bound_total_mb_tco2e <= actual_total_mb_tco2e <= upper_bound_total_mb_tco2e
                        ),
                    }
                )

    backtest_dataframe = pd.DataFrame(backtest_rows).convert_dtypes()
    selected_key_fields = [
        "company_id",
        "actual_year",
        "model_name",
        "actual_total_mb_tco2e",
        "predicted_total_mb_tco2e",
        "abs_error",
        "ape_pct",
        "within_interval_flag",
    ]
    assumptions = [
        "Backtests use walk-forward annual windows train-to-2021 predict-2022, train-to-2022 predict-2023, and train-to-2023 predict-2024.",
        "Two explainable models are compared for total market-based emissions: a naive last-value baseline and a linear annual trend model fit on each company's reconstructed history.",
        "Prediction intervals are heuristic and backtest-derived, using each model's in-sample residual spread plus a modest minimum relative width rather than claiming calibrated probabilistic coverage.",
    ]
    return ProcessedTableArtifact(
        output_name="company_forecast_backtest_results.parquet",
        dataframe=backtest_dataframe.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=["company_emissions_history_annual.parquet"],
    )


def select_best_models(backtest_results: pd.DataFrame) -> pd.DataFrame:
    """Select the best-performing model per company from walk-forward backtests."""

    aggregated = (
        backtest_results.groupby(
            ["company_id", "company_name", "sector", "country", "model_name"],
            as_index=False,
        )
        .agg(
            mean_ape_pct=("ape_pct", "mean"),
            mean_abs_error=("abs_error", "mean"),
            interval_coverage_pct=("within_interval_flag", lambda values: float(pd.Series(values).mean() * 100.0)),
            backtest_observations=("actual_year", "count"),
        )
        .copy()
    )
    aggregated["model_priority"] = aggregated["model_name"].map(
        {
            NAIVE_MODEL_NAME: 0,
            TREND_MODEL_NAME: 1,
        }
    )
    selected = (
        aggregated.sort_values(
            ["company_id", "mean_ape_pct", "mean_abs_error", "model_priority"],
            kind="stable",
        )
        .groupby("company_id", as_index=False)
        .first()
        .rename(columns={"model_name": "selected_model_name"})
    )
    return selected.drop(columns=["model_priority"]).reset_index(drop=True)


def predict_univariate_series(
    *,
    years: list[int],
    values: list[float],
    target_year: int,
    model_name: str,
) -> float:
    """Predict one future annual value for a univariate series."""

    cleaned_values = [max(0.0, float(value)) for value in values]
    if not cleaned_values:
        return 0.0

    if model_name == NAIVE_MODEL_NAME:
        return float(cleaned_values[-1])
    if model_name == TREND_MODEL_NAME:
        slope, intercept, reference_year = _fit_linear_trend(years=years, values=cleaned_values)
        x_target = target_year - reference_year
        return max(0.0, intercept + slope * x_target)

    raise ValueError(f"Unsupported model_name '{model_name}'.")


def build_prediction_interval(
    *,
    years: list[int],
    values: list[float],
    target_year: int,
    predicted_value: float,
    model_name: str,
) -> tuple[float, float]:
    """Build a heuristic prediction interval from in-sample model error spread."""

    abs_residuals = _training_abs_residuals(years=years, values=values, model_name=model_name)
    interval_radius = _interval_radius_from_error_spread(
        predicted_value=predicted_value,
        reference_values=values,
        abs_residuals=abs_residuals,
        horizon_steps=max(1, target_year - max(years)),
    )
    lower_bound = max(0.0, predicted_value - interval_radius)
    upper_bound = max(0.0, predicted_value + interval_radius)
    return (lower_bound, upper_bound)


def build_prediction_interval_from_backtest_metrics(
    *,
    predicted_value: float,
    mean_abs_error: float,
    mean_ape_pct: float,
    horizon_steps: int,
) -> tuple[float, float]:
    """Build a heuristic forecast interval from backtest summary metrics."""

    percentage_radius = abs(predicted_value) * max(MIN_RELATIVE_INTERVAL_WIDTH_PCT, mean_ape_pct) / 100.0
    absolute_radius = max(mean_abs_error, 0.0)
    expansion_multiplier = 1.0 + ((max(1, horizon_steps) - 1) * INTERVAL_EXPANSION_PER_YEAR_PCT / 100.0)
    interval_radius = max(percentage_radius, absolute_radius) * expansion_multiplier
    return (
        round(max(0.0, predicted_value - interval_radius), 3),
        round(max(0.0, predicted_value + interval_radius), 3),
    )


def _training_abs_residuals(
    *,
    years: list[int],
    values: list[float],
    model_name: str,
) -> list[float]:
    """Return absolute residuals for a fitted training series."""

    cleaned_values = [max(0.0, float(value)) for value in values]
    if len(cleaned_values) < 2:
        return []

    if model_name == NAIVE_MODEL_NAME:
        return [abs(cleaned_values[index] - cleaned_values[index - 1]) for index in range(1, len(cleaned_values))]

    if model_name == TREND_MODEL_NAME:
        slope, intercept, reference_year = _fit_linear_trend(years=years, values=cleaned_values)
        residuals: list[float] = []
        for year, actual_value in zip(years, cleaned_values, strict=True):
            predicted_value = max(0.0, intercept + slope * (year - reference_year))
            residuals.append(abs(actual_value - predicted_value))
        return residuals

    raise ValueError(f"Unsupported model_name '{model_name}'.")


def _interval_radius_from_error_spread(
    *,
    predicted_value: float,
    reference_values: list[float],
    abs_residuals: list[float],
    horizon_steps: int,
) -> float:
    """Convert residual spread into a simple, widening interval radius."""

    reference_level = max(
        1.0,
        abs(predicted_value),
        float(pd.Series(reference_values).mean()) if reference_values else 1.0,
        float(reference_values[-1]) if reference_values else 1.0,
    )
    empirical_error = float(pd.Series(abs_residuals).mean()) if abs_residuals else 0.0
    minimum_relative_error = reference_level * MIN_RELATIVE_INTERVAL_WIDTH_PCT / 100.0
    expansion_multiplier = 1.0 + ((max(1, horizon_steps) - 1) * INTERVAL_EXPANSION_PER_YEAR_PCT / 100.0)
    return max(empirical_error, minimum_relative_error) * expansion_multiplier


def _fit_linear_trend(*, years: list[int], values: list[float]) -> tuple[float, float, int]:
    """Fit a simple linear trend model on annual history values."""

    if len(years) != len(values):
        raise ValueError("years and values must have the same length.")

    reference_year = int(years[0])
    x_values = [year - reference_year for year in years]
    x_mean = sum(x_values) / len(x_values)
    y_mean = sum(values) / len(values)
    denominator = sum((x_value - x_mean) ** 2 for x_value in x_values)
    if denominator == 0:
        slope = 0.0
    else:
        slope = sum((x_value - x_mean) * (value - y_mean) for x_value, value in zip(x_values, values, strict=True)) / denominator
    intercept = y_mean - (slope * x_mean)
    return (slope, intercept, reference_year)


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
