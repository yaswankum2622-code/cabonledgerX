"""Deterministic annual emissions forecasting for synthetic companies."""

from __future__ import annotations

from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


FORECAST_METHOD = "deterministic_recursive_proxy_v1"
FORECAST_HORIZON_YEAR = 2030
BASE_UNCERTAINTY_PCT = 5.0
ANNUAL_UNCERTAINTY_EXPANSION_PCT = 1.0

SCOPE1_EFFICIENCY_PCT_BY_SECTOR = {
    "Utilities": 1.2,
    "Materials": 1.0,
    "Manufacturing": 1.8,
    "Logistics": 2.0,
    "Technology": 2.6,
    "Retail": 1.7,
    "Healthcare": 1.6,
    "Consumer Goods": 1.8,
}

GRID_DECARBONIZATION_PCT_BY_COUNTRY = {
    "France": 1.4,
    "Canada": 1.8,
    "Brazil": 1.8,
    "United States": 2.4,
    "United Kingdom": 2.8,
    "Germany": 2.5,
    "Japan": 2.2,
    "Australia": 2.3,
    "Singapore": 2.0,
    "India": 3.0,
}


def build_company_emissions_forecast(
    *,
    baseline: pd.DataFrame | None = None,
    company_panel: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build a deterministic annual forecast table from baseline year through 2030."""

    if baseline is None:
        baseline = _read_processed_table("company_emissions_baseline.parquet")
    if company_panel is None:
        company_panel = _read_processed_table("company_synthetic_panel.parquet")

    forecast_input = _build_forecast_input(
        baseline=baseline,
        company_panel=company_panel,
    )
    forecast_rows = [
        forecast_row
        for company_record in forecast_input.to_dict(orient="records")
        for forecast_row in _forecast_company(company_record)
    ]
    forecast_dataframe = pd.DataFrame(forecast_rows).convert_dtypes()

    selected_key_fields = [
        "company_id",
        "baseline_year",
        "forecast_year",
        "forecast_total_lb_tco2e",
        "forecast_total_mb_tco2e",
        "activity_growth_pct",
        "grid_decarbonization_pct",
        "mb_procurement_effect_pct",
    ]
    assumptions = [
        "Baseline-year emissions are copied directly from company_emissions_baseline and all later years are forecast recursively through 2030.",
        "Scope 1 uses sector-conditioned efficiency improvements with a small fleet-electrification bonus; Scope 2 LB uses simple country-level grid-decarbonization proxies.",
        "Scope 2 MB uses renewable-share procurement buckets, and uncertainty bands widen deterministically by 1 percentage point for each year beyond baseline.",
    ]
    return ProcessedTableArtifact(
        output_name="company_emissions_forecast.parquet",
        dataframe=forecast_dataframe.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=["company_emissions_baseline.parquet", "company_synthetic_panel.parquet"],
    )


def _build_forecast_input(
    *,
    baseline: pd.DataFrame,
    company_panel: pd.DataFrame,
) -> pd.DataFrame:
    """Join baseline emissions with synthetic panel forecast drivers."""

    panel_columns = [
        "company_id",
        "target_year",
        "target_reduction_pct",
        "annual_activity_growth_pct",
        "renewable_share_pct",
        "fleet_electrification_pct",
    ]
    panel_drivers = company_panel.loc[
        :,
        [column for column in panel_columns if column in company_panel.columns],
    ].copy()

    forecast_input = baseline.merge(
        panel_drivers,
        on="company_id",
        how="left",
        validate="one_to_one",
        suffixes=("", "_panel"),
    )
    numeric_columns = [
        "baseline_year",
        "current_scope1_tco2e",
        "current_scope2_lb_tco2e",
        "current_scope2_mb_tco2e",
        "annual_activity_growth_pct",
        "renewable_share_pct",
        "fleet_electrification_pct",
        "target_year",
        "target_reduction_pct",
    ]
    for column_name in numeric_columns:
        if column_name in forecast_input.columns:
            forecast_input[column_name] = pd.to_numeric(forecast_input[column_name], errors="coerce")

    return forecast_input


def _forecast_company(company_record: dict[str, Any]) -> list[dict[str, Any]]:
    """Build one company-level annual forecast path."""

    company_id = str(company_record["company_id"])
    baseline_year = int(company_record["baseline_year"])
    activity_growth_pct = float(company_record.get("annual_activity_growth_pct", 0.0) or 0.0)
    renewable_share_pct = float(company_record.get("renewable_share_pct", 0.0) or 0.0)
    fleet_electrification_pct = float(company_record.get("fleet_electrification_pct", 0.0) or 0.0)

    scope1_efficiency_pct = _scope1_efficiency_improvement_pct(
        sector=str(company_record["sector"]),
        fleet_electrification_pct=fleet_electrification_pct,
    )
    grid_decarbonization_pct = _grid_decarbonization_pct(str(company_record["country"]))
    mb_procurement_effect_pct = _market_based_procurement_effect_pct(renewable_share_pct)

    prior_scope1 = float(company_record["current_scope1_tco2e"])
    prior_scope2_lb = float(company_record["current_scope2_lb_tco2e"])
    prior_scope2_mb = float(company_record["current_scope2_mb_tco2e"])

    forecast_rows: list[dict[str, Any]] = []
    for forecast_year in range(baseline_year, FORECAST_HORIZON_YEAR + 1):
        if forecast_year == baseline_year:
            scope1_value = prior_scope1
            scope2_lb_value = prior_scope2_lb
            scope2_mb_value = prior_scope2_mb
        else:
            scope1_value = _apply_rate_step(
                prior_value=prior_scope1,
                positive_driver_pct=activity_growth_pct,
                reduction_driver_pct=scope1_efficiency_pct,
            )
            scope2_lb_value = _apply_rate_step(
                prior_value=prior_scope2_lb,
                positive_driver_pct=activity_growth_pct,
                reduction_driver_pct=grid_decarbonization_pct,
            )
            scope2_mb_value = _apply_rate_step(
                prior_value=prior_scope2_mb,
                positive_driver_pct=activity_growth_pct,
                reduction_driver_pct=mb_procurement_effect_pct,
            )
            prior_scope1 = scope1_value
            prior_scope2_lb = scope2_lb_value
            prior_scope2_mb = scope2_mb_value

        total_lb = scope1_value + scope2_lb_value
        total_mb = scope1_value + scope2_mb_value
        uncertainty_pct = BASE_UNCERTAINTY_PCT + (
            (forecast_year - baseline_year) * ANNUAL_UNCERTAINTY_EXPANSION_PCT
        )
        lower_lb, upper_lb = _uncertainty_band(total_lb, uncertainty_pct)
        lower_mb, upper_mb = _uncertainty_band(total_mb, uncertainty_pct)

        forecast_rows.append(
            {
                "company_id": company_id,
                "company_name": company_record["company_name"],
                "sector": company_record["sector"],
                "country": company_record["country"],
                "baseline_year": baseline_year,
                "forecast_year": forecast_year,
                "forecast_scope1_tco2e": round(scope1_value, 3),
                "forecast_scope2_lb_tco2e": round(scope2_lb_value, 3),
                "forecast_scope2_mb_tco2e": round(scope2_mb_value, 3),
                "forecast_total_lb_tco2e": round(total_lb, 3),
                "forecast_total_mb_tco2e": round(total_mb, 3),
                "lower_bound_total_lb_tco2e": lower_lb,
                "upper_bound_total_lb_tco2e": upper_lb,
                "lower_bound_total_mb_tco2e": lower_mb,
                "upper_bound_total_mb_tco2e": upper_mb,
                "forecast_method": FORECAST_METHOD,
                "activity_growth_pct": activity_growth_pct,
                "scope1_efficiency_improvement_pct": scope1_efficiency_pct,
                "grid_decarbonization_pct": grid_decarbonization_pct,
                "mb_procurement_effect_pct": mb_procurement_effect_pct,
                "forecast_notes": _forecast_notes(
                    forecast_year=forecast_year,
                    baseline_year=baseline_year,
                    sector=str(company_record["sector"]),
                    country=str(company_record["country"]),
                    renewable_share_pct=renewable_share_pct,
                ),
            }
        )

    return forecast_rows


def _scope1_efficiency_improvement_pct(*, sector: str, fleet_electrification_pct: float) -> float:
    """Return the annual scope 1 efficiency improvement percentage."""

    base_pct = SCOPE1_EFFICIENCY_PCT_BY_SECTOR.get(sector, 1.5)
    if fleet_electrification_pct >= 50:
        return round(base_pct + 0.4, 2)
    if fleet_electrification_pct >= 25:
        return round(base_pct + 0.2, 2)
    return round(base_pct, 2)


def _grid_decarbonization_pct(country: str) -> float:
    """Return the annual location-based grid-decarbonization percentage."""

    return float(GRID_DECARBONIZATION_PCT_BY_COUNTRY.get(country, 2.2))


def _market_based_procurement_effect_pct(renewable_share_pct: float) -> float:
    """Return the annual market-based procurement improvement percentage."""

    if renewable_share_pct >= 80:
        return 7.0
    if renewable_share_pct >= 60:
        return 5.5
    if renewable_share_pct >= 40:
        return 4.0
    if renewable_share_pct >= 20:
        return 2.5
    return 1.5


def _apply_rate_step(
    *,
    prior_value: float,
    positive_driver_pct: float,
    reduction_driver_pct: float,
) -> float:
    """Advance one forecast step with a simple transparent rate formula."""

    multiplier = max(0.0, 1.0 + (positive_driver_pct / 100.0) - (reduction_driver_pct / 100.0))
    return max(0.0, prior_value * multiplier)


def _uncertainty_band(value: float, uncertainty_pct: float) -> tuple[float, float]:
    """Return a simple non-negative deterministic uncertainty band."""

    lower = max(0.0, value * (1.0 - uncertainty_pct / 100.0))
    upper = max(0.0, value * (1.0 + uncertainty_pct / 100.0))
    return (round(lower, 3), round(upper, 3))


def _forecast_notes(
    *,
    forecast_year: int,
    baseline_year: int,
    sector: str,
    country: str,
    renewable_share_pct: float,
) -> str:
    """Build a concise, auditable note for one forecast row."""

    if forecast_year == baseline_year:
        return "Baseline-year row copied directly from company_emissions_baseline."

    procurement_bucket = "standard"
    if renewable_share_pct >= 80:
        procurement_bucket = "very_high_renewable"
    elif renewable_share_pct >= 60:
        procurement_bucket = "high_renewable"
    elif renewable_share_pct >= 40:
        procurement_bucket = "moderate_renewable"
    elif renewable_share_pct >= 20:
        procurement_bucket = "emerging_renewable"

    return (
        f"Recursive forecast from prior year using {sector} scope-1 efficiency rule, "
        f"{country} grid proxy, and {procurement_bucket} market-based procurement bucket."
    )


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
