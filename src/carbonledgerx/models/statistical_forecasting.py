"""Statistical annual forecasting from reconstructed company emissions history."""

from __future__ import annotations

from typing import Any

import pandas as pd

from carbonledgerx.models.backtesting import (
    TREND_MODEL_NAME,
    build_prediction_interval_from_backtest_metrics,
    predict_univariate_series,
    select_best_models,
)
from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


STATISTICAL_FORECAST_HORIZON_YEAR = 2030


def build_company_emissions_forecast_statistical(
    *,
    history_annual: pd.DataFrame | None = None,
    backtest_results: pd.DataFrame | None = None,
    baseline: pd.DataFrame | None = None,
    company_panel: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build the selected-model statistical forecast layer through 2030."""

    if history_annual is None:
        history_annual = _read_processed_table("company_emissions_history_annual.parquet")
    if backtest_results is None:
        backtest_results = _read_processed_table("company_forecast_backtest_results.parquet")
    if baseline is None:
        baseline = _read_processed_table("company_emissions_baseline.parquet")
    if company_panel is None:
        company_panel = _read_processed_table("company_synthetic_panel.parquet")

    selected_models = select_best_models(backtest_results)
    metadata = _build_company_metadata(
        history_annual=history_annual,
        baseline=baseline,
        company_panel=company_panel,
    )

    forecast_rows: list[dict[str, Any]] = []
    for company_id, company_history in history_annual.groupby("company_id", sort=True):
        sorted_history = company_history.sort_values("history_year", kind="stable").reset_index(drop=True)
        model_row = selected_models.loc[selected_models["company_id"] == company_id]
        if model_row.empty:
            raise ValueError(f"Missing selected model row for company_id '{company_id}'.")

        selection = model_row.iloc[0]
        metadata_row = metadata.loc[metadata["company_id"] == company_id]
        if metadata_row.empty:
            raise ValueError(f"Missing company metadata for company_id '{company_id}'.")

        meta = metadata_row.iloc[0]
        latest_history_year = int(pd.to_numeric(sorted_history["history_year"], errors="coerce").max())
        history_years = sorted_history["history_year"].astype(int).tolist()
        total_mb_values = pd.to_numeric(sorted_history["total_mb_tco2e"], errors="coerce").astype(float).tolist()
        scope1_values = pd.to_numeric(sorted_history["scope1_tco2e"], errors="coerce").astype(float).tolist()
        scope2_mb_values = pd.to_numeric(sorted_history["scope2_mb_tco2e"], errors="coerce").astype(float).tolist()

        for forecast_year in range(latest_history_year + 1, STATISTICAL_FORECAST_HORIZON_YEAR + 1):
            horizon_steps = forecast_year - latest_history_year
            forecast_total_mb_tco2e = predict_univariate_series(
                years=history_years,
                values=total_mb_values,
                target_year=forecast_year,
                model_name=str(selection["selected_model_name"]),
            )
            forecast_scope1_tco2e = predict_univariate_series(
                years=history_years,
                values=scope1_values,
                target_year=forecast_year,
                model_name=str(selection["selected_model_name"]),
            )
            forecast_scope2_mb_tco2e = predict_univariate_series(
                years=history_years,
                values=scope2_mb_values,
                target_year=forecast_year,
                model_name=str(selection["selected_model_name"]),
            )
            lower_bound_total_mb_tco2e, upper_bound_total_mb_tco2e = build_prediction_interval_from_backtest_metrics(
                predicted_value=forecast_total_mb_tco2e,
                mean_abs_error=float(selection["mean_abs_error"]),
                mean_ape_pct=float(selection["mean_ape_pct"]),
                horizon_steps=horizon_steps,
            )

            forecast_rows.append(
                {
                    "company_id": company_id,
                    "company_name": meta["company_name"],
                    "sector": meta["sector"],
                    "country": meta["country"],
                    "forecast_year": forecast_year,
                    "model_name": selection["selected_model_name"],
                    "forecast_total_mb_tco2e": round(forecast_total_mb_tco2e, 3),
                    "lower_bound_total_mb_tco2e": lower_bound_total_mb_tco2e,
                    "upper_bound_total_mb_tco2e": upper_bound_total_mb_tco2e,
                    "forecast_scope1_tco2e": round(forecast_scope1_tco2e, 3),
                    "forecast_scope2_mb_tco2e": round(forecast_scope2_mb_tco2e, 3),
                    "forecast_notes": _forecast_note(
                        model_name=str(selection["selected_model_name"]),
                        latest_history_year=latest_history_year,
                        forecast_year=forecast_year,
                        mean_ape_pct=float(selection["mean_ape_pct"]),
                        mean_abs_error=float(selection["mean_abs_error"]),
                    ),
                }
            )

    forecast_dataframe = pd.DataFrame(forecast_rows).convert_dtypes()
    selected_key_fields = [
        "company_id",
        "forecast_year",
        "model_name",
        "forecast_total_mb_tco2e",
        "lower_bound_total_mb_tco2e",
        "upper_bound_total_mb_tco2e",
    ]
    assumptions = [
        "The statistical layer forecasts total market-based emissions from the reconstructed annual history using the best backtested model per company.",
        "Each company selects between a naive last-value model and a simple linear annual trend model based on average walk-forward APE with MAE as the tiebreaker.",
        "Forecast intervals are widened from each company's backtest MAE and APE rather than presented as fully probabilistic confidence intervals.",
    ]
    return ProcessedTableArtifact(
        output_name="company_emissions_forecast_statistical.parquet",
        dataframe=forecast_dataframe.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[
            "company_emissions_history_annual.parquet",
            "company_forecast_backtest_results.parquet",
            "company_emissions_baseline.parquet",
            "company_synthetic_panel.parquet",
        ],
    )


def _build_company_metadata(
    *,
    history_annual: pd.DataFrame,
    baseline: pd.DataFrame,
    company_panel: pd.DataFrame,
) -> pd.DataFrame:
    """Build a stable company metadata table for forecast outputs."""

    history_metadata = history_annual.loc[:, ["company_id", "company_name", "sector", "country"]].drop_duplicates()
    baseline_metadata = baseline.loc[:, ["company_id", "company_name", "sector", "country"]].drop_duplicates()
    panel_metadata = company_panel.loc[:, ["company_id", "company_name", "sector", "country"]].drop_duplicates()

    metadata = history_metadata.merge(
        baseline_metadata,
        on="company_id",
        how="left",
        suffixes=("", "_baseline"),
        validate="one_to_one",
    ).merge(
        panel_metadata,
        on="company_id",
        how="left",
        suffixes=("", "_panel"),
        validate="one_to_one",
    )

    for column_name in ["company_name", "sector", "country"]:
        metadata[column_name] = metadata[column_name].fillna(metadata.get(f"{column_name}_baseline"))
        metadata[column_name] = metadata[column_name].fillna(metadata.get(f"{column_name}_panel"))

    keep_columns = ["company_id", "company_name", "sector", "country"]
    return metadata.loc[:, keep_columns].drop_duplicates().reset_index(drop=True)


def _forecast_note(
    *,
    model_name: str,
    latest_history_year: int,
    forecast_year: int,
    mean_ape_pct: float,
    mean_abs_error: float,
) -> str:
    """Build an auditable note for one statistical forecast row."""

    model_description = "naive carry-forward"
    if model_name == TREND_MODEL_NAME:
        model_description = "linear annual trend"

    return (
        f"Forecast extends reconstructed history from {latest_history_year} to {forecast_year} "
        f"using the selected {model_description} model; interval width is anchored to mean APE "
        f"{mean_ape_pct:.2f}% and mean absolute error {mean_abs_error:.1f} from walk-forward backtests."
    )


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
