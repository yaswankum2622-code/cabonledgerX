"""Historical annual emissions reconstruction for synthetic companies."""

from __future__ import annotations

from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


HISTORY_START_YEAR = 2015
HISTORY_END_YEAR = 2024
MIN_FACTOR_LB_MWH = 25.0

HISTORICAL_ACTIVITY_GROWTH_PCT_BY_SECTOR = {
    "Utilities": 1.4,
    "Materials": 1.8,
    "Manufacturing": 2.2,
    "Logistics": 3.0,
    "Technology": 4.2,
    "Retail": 2.4,
    "Healthcare": 2.0,
    "Consumer Goods": 2.5,
}

HISTORICAL_SCOPE1_EFFICIENCY_PCT_BY_SECTOR = {
    "Utilities": 0.9,
    "Materials": 0.8,
    "Manufacturing": 1.1,
    "Logistics": 1.3,
    "Technology": 1.6,
    "Retail": 1.0,
    "Healthcare": 0.9,
    "Consumer Goods": 1.0,
}

HISTORICAL_GRID_DECARBONIZATION_PCT_BY_REGION_TYPE = {
    "egrid_state_proxy": 1.8,
    "international_average_proxy_low_carbon": 1.4,
    "international_average_proxy_medium_grid": 1.0,
    "international_average_proxy_high_grid": 0.8,
    "international_average_proxy_standard": 1.0,
}

HISTORICAL_MB_PROCUREMENT_PCT_BY_SOURCE = {
    "market_based_proxy_high_renewable_procurement": 3.2,
    "market_based_proxy_partial_renewable_procurement": 2.0,
    "market_based_proxy_standard_procurement": 0.9,
}


def build_company_emissions_history_annual(
    *,
    company_panel: pd.DataFrame | None = None,
    activity_inputs: pd.DataFrame | None = None,
    calculated_emissions: pd.DataFrame | None = None,
    factor_mapping: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build an annual 2015-2024 emissions reconstruction for each company."""

    if company_panel is None:
        company_panel = _read_processed_table("company_synthetic_panel.parquet")
    if activity_inputs is None:
        activity_inputs = _read_processed_table("company_activity_inputs.parquet")
    if calculated_emissions is None:
        calculated_emissions = _read_processed_table("company_emissions_calculated.parquet")
    if factor_mapping is None:
        factor_mapping = _read_processed_table("company_factor_mapping.parquet")

    history_input = _build_history_input(
        company_panel=company_panel,
        activity_inputs=activity_inputs,
        calculated_emissions=calculated_emissions,
        factor_mapping=factor_mapping,
    )
    scope1_factor_coefficients = _derive_scope1_factor_coefficients(calculated_emissions)

    history_rows = [
        history_row
        for company_record in history_input.to_dict(orient="records")
        for history_row in _reconstruct_company_history(
            company_record=company_record,
            natural_gas_kg_per_mmbtu=scope1_factor_coefficients["natural_gas_kg_per_mmbtu"],
            diesel_kg_per_liter=scope1_factor_coefficients["diesel_kg_per_liter"],
        )
    ]
    history_dataframe = (
        pd.DataFrame(history_rows)
        .sort_values(["company_id", "history_year"], kind="stable")
        .reset_index(drop=True)
        .convert_dtypes()
    )

    selected_key_fields = [
        "company_id",
        "history_year",
        "total_lb_tco2e",
        "total_mb_tco2e",
        "historical_activity_growth_pct",
        "historical_grid_decarbonization_pct",
        "historical_mb_procurement_effect_pct",
    ]
    assumptions = [
        "The reconstruction is anchored to the current/base-year activity and activity-based calculated emissions, then extended backward to 2015 and forward to 2024 with deterministic annual rates.",
        "Electricity activity follows a sector-conditioned historical growth proxy, while natural gas and diesel use an adjusted fuel-growth proxy that nets sector growth against modest operational efficiency drift.",
        "Location-based and market-based scope 2 emissions use year-adjusted factor trends rather than pretending to use unavailable historical company utility procurement files.",
    ]
    return ProcessedTableArtifact(
        output_name="company_emissions_history_annual.parquet",
        dataframe=history_dataframe,
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[
            "company_synthetic_panel.parquet",
            "company_activity_inputs.parquet",
            "company_emissions_calculated.parquet",
            "company_factor_mapping.parquet",
        ],
    )


def _build_history_input(
    *,
    company_panel: pd.DataFrame,
    activity_inputs: pd.DataFrame,
    calculated_emissions: pd.DataFrame,
    factor_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Join current inputs needed for reconstruction."""

    panel_columns = [
        "company_id",
        "annual_activity_growth_pct",
        "renewable_share_pct",
        "fleet_electrification_pct",
    ]
    panel_drivers = company_panel.loc[
        :,
        [column for column in panel_columns if column in company_panel.columns],
    ].copy()

    factor_columns = [
        "company_id",
        "factor_region_type",
        "factor_region_key",
        "scope2_mb_factor_source",
        "mapping_notes",
    ]
    mapping_drivers = factor_mapping.loc[
        :,
        [column for column in factor_columns if column in factor_mapping.columns],
    ].copy()

    history_input = activity_inputs.merge(
        calculated_emissions,
        on=["company_id", "company_name", "sector", "country", "base_year"],
        how="left",
        validate="one_to_one",
        suffixes=("", "_calculated"),
    )
    history_input = history_input.merge(
        panel_drivers,
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    history_input = history_input.merge(
        mapping_drivers,
        on="company_id",
        how="left",
        validate="one_to_one",
    )

    numeric_columns = [
        "base_year",
        "electricity_mwh",
        "natural_gas_mmbtu",
        "diesel_liters",
        "calculated_scope1_tco2e",
        "calculated_scope2_lb_tco2e",
        "calculated_scope2_mb_tco2e",
        "calculated_total_lb_tco2e",
        "calculated_total_mb_tco2e",
        "scope2_lb_factor_value_lb_mwh",
        "scope2_mb_factor_value_lb_mwh",
        "annual_activity_growth_pct",
        "renewable_share_pct",
        "fleet_electrification_pct",
    ]
    for column_name in numeric_columns:
        if column_name in history_input.columns:
            history_input[column_name] = pd.to_numeric(history_input[column_name], errors="coerce")

    return history_input


def _derive_scope1_factor_coefficients(calculated_emissions: pd.DataFrame) -> dict[str, float]:
    """Derive fixed scope 1 factor coefficients from the calculated-emissions anchor."""

    natural_gas_mask = (
        pd.to_numeric(calculated_emissions["natural_gas_mmbtu"], errors="coerce").gt(0)
        & pd.to_numeric(calculated_emissions["natural_gas_scope1_tco2e"], errors="coerce").gt(0)
    )
    diesel_mask = (
        pd.to_numeric(calculated_emissions["diesel_liters"], errors="coerce").gt(0)
        & pd.to_numeric(calculated_emissions["diesel_scope1_tco2e"], errors="coerce").gt(0)
    )

    natural_gas_kg_per_mmbtu = (
        (
            pd.to_numeric(
                calculated_emissions.loc[natural_gas_mask, "natural_gas_scope1_tco2e"],
                errors="coerce",
            )
            * 1000.0
        )
        / pd.to_numeric(calculated_emissions.loc[natural_gas_mask, "natural_gas_mmbtu"], errors="coerce")
    ).median()
    diesel_kg_per_liter = (
        (
            pd.to_numeric(
                calculated_emissions.loc[diesel_mask, "diesel_scope1_tco2e"],
                errors="coerce",
            )
            * 1000.0
        )
        / pd.to_numeric(calculated_emissions.loc[diesel_mask, "diesel_liters"], errors="coerce")
    ).median()

    if pd.isna(natural_gas_kg_per_mmbtu) or pd.isna(diesel_kg_per_liter):
        raise ValueError("Could not derive stable scope 1 factor coefficients from company_emissions_calculated.")

    return {
        "natural_gas_kg_per_mmbtu": float(natural_gas_kg_per_mmbtu),
        "diesel_kg_per_liter": float(diesel_kg_per_liter),
    }


def _reconstruct_company_history(
    *,
    company_record: dict[str, Any],
    natural_gas_kg_per_mmbtu: float,
    diesel_kg_per_liter: float,
) -> list[dict[str, Any]]:
    """Reconstruct annual activity and emissions rows for one company."""

    base_year = int(_safe_float(company_record.get("base_year"), default=HISTORY_END_YEAR))
    sector = str(company_record["sector"])
    country = str(company_record["country"])
    factor_region_type = str(company_record.get("factor_region_type", "") or "international_average_proxy_standard")
    factor_region_key = str(company_record.get("factor_region_key", "") or country)

    historical_activity_growth_pct = _historical_activity_growth_pct(
        sector=sector,
        company_growth_pct=_safe_float(company_record.get("annual_activity_growth_pct")),
    )
    scope1_efficiency_pct = _historical_scope1_efficiency_pct(
        sector=sector,
        fleet_electrification_pct=_safe_float(company_record.get("fleet_electrification_pct")),
    )
    fuel_growth_pct = max(-4.5, historical_activity_growth_pct - scope1_efficiency_pct)
    historical_grid_decarbonization_pct = _historical_grid_decarbonization_pct(
        factor_region_type=factor_region_type,
    )
    historical_mb_procurement_effect_pct = _historical_mb_procurement_effect_pct(
        renewable_share_pct=_safe_float(company_record.get("renewable_share_pct")),
        scope2_mb_factor_source=str(company_record.get("scope2_mb_factor_source", "") or ""),
    )

    activity_growth_rate = historical_activity_growth_pct / 100.0
    fuel_growth_rate = fuel_growth_pct / 100.0
    grid_decarbonization_rate = historical_grid_decarbonization_pct / 100.0
    mb_procurement_rate = historical_mb_procurement_effect_pct / 100.0

    anchor_state = {
        "electricity_mwh": _safe_float(company_record.get("electricity_mwh")),
        "natural_gas_mmbtu": _safe_float(company_record.get("natural_gas_mmbtu")),
        "diesel_liters": _safe_float(company_record.get("diesel_liters")),
        "scope2_lb_factor_lb_mwh": _safe_float(company_record.get("scope2_lb_factor_value_lb_mwh")),
        "scope2_mb_factor_lb_mwh": _safe_float(company_record.get("scope2_mb_factor_value_lb_mwh")),
    }

    state_by_year: dict[int, dict[str, float]] = {base_year: anchor_state}

    for history_year in range(base_year - 1, HISTORY_START_YEAR - 1, -1):
        next_state = state_by_year[history_year + 1]
        prior_lb_factor = _backward_factor_step(
            next_state["scope2_lb_factor_lb_mwh"],
            annual_reduction_rate=grid_decarbonization_rate,
        )
        prior_mb_factor = min(
            prior_lb_factor,
            _backward_factor_step(
                next_state["scope2_mb_factor_lb_mwh"],
                annual_reduction_rate=mb_procurement_rate,
            ),
        )
        state_by_year[history_year] = {
            "electricity_mwh": _backward_activity_step(
                next_state["electricity_mwh"],
                annual_growth_rate=activity_growth_rate,
            ),
            "natural_gas_mmbtu": _backward_activity_step(
                next_state["natural_gas_mmbtu"],
                annual_growth_rate=fuel_growth_rate,
            ),
            "diesel_liters": _backward_activity_step(
                next_state["diesel_liters"],
                annual_growth_rate=fuel_growth_rate,
            ),
            "scope2_lb_factor_lb_mwh": prior_lb_factor,
            "scope2_mb_factor_lb_mwh": prior_mb_factor,
        }

    for history_year in range(base_year + 1, HISTORY_END_YEAR + 1):
        prior_state = state_by_year[history_year - 1]
        next_lb_factor = _forward_factor_step(
            prior_state["scope2_lb_factor_lb_mwh"],
            annual_reduction_rate=grid_decarbonization_rate,
        )
        next_mb_factor = min(
            next_lb_factor,
            _forward_factor_step(
                prior_state["scope2_mb_factor_lb_mwh"],
                annual_reduction_rate=mb_procurement_rate,
            ),
        )
        state_by_year[history_year] = {
            "electricity_mwh": _forward_activity_step(
                prior_state["electricity_mwh"],
                annual_growth_rate=activity_growth_rate,
            ),
            "natural_gas_mmbtu": _forward_activity_step(
                prior_state["natural_gas_mmbtu"],
                annual_growth_rate=fuel_growth_rate,
            ),
            "diesel_liters": _forward_activity_step(
                prior_state["diesel_liters"],
                annual_growth_rate=fuel_growth_rate,
            ),
            "scope2_lb_factor_lb_mwh": next_lb_factor,
            "scope2_mb_factor_lb_mwh": next_mb_factor,
        }

    history_rows: list[dict[str, Any]] = []
    for history_year in range(HISTORY_START_YEAR, HISTORY_END_YEAR + 1):
        year_state = state_by_year[history_year]
        scope1_tco2e = _calculate_scope1_total(
            natural_gas_mmbtu=year_state["natural_gas_mmbtu"],
            diesel_liters=year_state["diesel_liters"],
            natural_gas_kg_per_mmbtu=natural_gas_kg_per_mmbtu,
            diesel_kg_per_liter=diesel_kg_per_liter,
        )
        scope2_lb_tco2e = (
            year_state["electricity_mwh"] * year_state["scope2_lb_factor_lb_mwh"] / 2204.62262
        )
        scope2_mb_tco2e = (
            year_state["electricity_mwh"] * year_state["scope2_mb_factor_lb_mwh"] / 2204.62262
        )

        if history_year == base_year:
            scope1_tco2e = _safe_float(company_record.get("calculated_scope1_tco2e"), default=scope1_tco2e)
            scope2_lb_tco2e = _safe_float(
                company_record.get("calculated_scope2_lb_tco2e"),
                default=scope2_lb_tco2e,
            )
            scope2_mb_tco2e = _safe_float(
                company_record.get("calculated_scope2_mb_tco2e"),
                default=scope2_mb_tco2e,
            )

        total_lb_tco2e = scope1_tco2e + scope2_lb_tco2e
        total_mb_tco2e = scope1_tco2e + scope2_mb_tco2e
        reconstruction_note = _reconstruction_note(
            history_year=history_year,
            base_year=base_year,
            factor_region_type=factor_region_type,
            factor_region_key=factor_region_key,
            historical_activity_growth_pct=historical_activity_growth_pct,
            scope1_efficiency_pct=scope1_efficiency_pct,
            historical_grid_decarbonization_pct=historical_grid_decarbonization_pct,
            historical_mb_procurement_effect_pct=historical_mb_procurement_effect_pct,
        )

        history_rows.append(
            {
                "company_id": company_record["company_id"],
                "company_name": company_record["company_name"],
                "sector": sector,
                "country": country,
                "history_year": history_year,
                "base_year": base_year,
                "electricity_mwh": round(year_state["electricity_mwh"], 3),
                "natural_gas_mmbtu": round(year_state["natural_gas_mmbtu"], 3),
                "diesel_liters": round(year_state["diesel_liters"], 3),
                "scope1_tco2e": round(scope1_tco2e, 3),
                "scope2_lb_tco2e": round(scope2_lb_tco2e, 3),
                "scope2_mb_tco2e": round(scope2_mb_tco2e, 3),
                "total_lb_tco2e": round(total_lb_tco2e, 3),
                "total_mb_tco2e": round(total_mb_tco2e, 3),
                "historical_activity_growth_pct": round(historical_activity_growth_pct, 2),
                "historical_grid_decarbonization_pct": round(historical_grid_decarbonization_pct, 2),
                "historical_mb_procurement_effect_pct": round(
                    historical_mb_procurement_effect_pct,
                    2,
                ),
                "reconstruction_notes": reconstruction_note,
            }
        )

    return history_rows


def _historical_activity_growth_pct(*, sector: str, company_growth_pct: float) -> float:
    """Return a sector-conditioned historical activity growth assumption."""

    sector_anchor = HISTORICAL_ACTIVITY_GROWTH_PCT_BY_SECTOR.get(sector, 2.0)
    blended_growth_pct = (company_growth_pct * 0.65) + (sector_anchor * 0.35)
    return round(min(6.5, max(-1.5, blended_growth_pct)), 2)


def _historical_scope1_efficiency_pct(*, sector: str, fleet_electrification_pct: float) -> float:
    """Return a modest historical operational-efficiency drift for scope 1 fuels."""

    base_efficiency_pct = HISTORICAL_SCOPE1_EFFICIENCY_PCT_BY_SECTOR.get(sector, 1.0)
    electrification_bonus = min(0.35, fleet_electrification_pct / 200.0)
    return round(base_efficiency_pct + electrification_bonus, 2)


def _historical_grid_decarbonization_pct(*, factor_region_type: str) -> float:
    """Return a simple historical grid-decarbonization proxy."""

    return float(
        HISTORICAL_GRID_DECARBONIZATION_PCT_BY_REGION_TYPE.get(
            factor_region_type,
            HISTORICAL_GRID_DECARBONIZATION_PCT_BY_REGION_TYPE["international_average_proxy_standard"],
        )
    )


def _historical_mb_procurement_effect_pct(
    *,
    renewable_share_pct: float,
    scope2_mb_factor_source: str,
) -> float:
    """Return a simple historical market-based procurement improvement proxy."""

    source_anchor = HISTORICAL_MB_PROCUREMENT_PCT_BY_SOURCE.get(scope2_mb_factor_source, 1.2)
    renewable_bonus = 0.0
    if renewable_share_pct >= 70:
        renewable_bonus = 0.6
    elif renewable_share_pct >= 40:
        renewable_bonus = 0.3
    return round(source_anchor + renewable_bonus, 2)


def _backward_activity_step(next_value: float, *, annual_growth_rate: float) -> float:
    """Step one year backward for an activity quantity."""

    denominator = max(0.05, 1.0 + annual_growth_rate)
    return max(0.0, next_value / denominator)


def _forward_activity_step(prior_value: float, *, annual_growth_rate: float) -> float:
    """Step one year forward for an activity quantity."""

    multiplier = max(0.05, 1.0 + annual_growth_rate)
    return max(0.0, prior_value * multiplier)


def _backward_factor_step(next_factor_value: float, *, annual_reduction_rate: float) -> float:
    """Step one year backward for an emissions factor."""

    denominator = max(0.05, 1.0 - annual_reduction_rate)
    return max(MIN_FACTOR_LB_MWH, next_factor_value / denominator)


def _forward_factor_step(prior_factor_value: float, *, annual_reduction_rate: float) -> float:
    """Step one year forward for an emissions factor."""

    multiplier = max(0.05, 1.0 - annual_reduction_rate)
    return max(MIN_FACTOR_LB_MWH, prior_factor_value * multiplier)


def _calculate_scope1_total(
    *,
    natural_gas_mmbtu: float,
    diesel_liters: float,
    natural_gas_kg_per_mmbtu: float,
    diesel_kg_per_liter: float,
) -> float:
    """Calculate annual scope 1 emissions from fuel activity."""

    return max(
        0.0,
        ((natural_gas_mmbtu * natural_gas_kg_per_mmbtu) + (diesel_liters * diesel_kg_per_liter))
        / 1000.0,
    )


def _reconstruction_note(
    *,
    history_year: int,
    base_year: int,
    factor_region_type: str,
    factor_region_key: str,
    historical_activity_growth_pct: float,
    scope1_efficiency_pct: float,
    historical_grid_decarbonization_pct: float,
    historical_mb_procurement_effect_pct: float,
) -> str:
    """Build an auditable reconstruction note for one annual row."""

    if history_year == base_year:
        return (
            "Base-year anchor row copied from company_emissions_calculated, with activity and "
            f"factor references aligned to {factor_region_type}:{factor_region_key}."
        )

    direction = "backward-reconstructed"
    if history_year > base_year:
        direction = "forward-extended"

    return (
        f"{direction} from base year {base_year} using {historical_activity_growth_pct:.2f}% "
        f"activity growth, {scope1_efficiency_pct:.2f}% scope 1 efficiency drift, "
        f"{historical_grid_decarbonization_pct:.2f}% LB grid decarbonization, and "
        f"{historical_mb_procurement_effect_pct:.2f}% MB procurement improvement under "
        f"{factor_region_type}:{factor_region_key}."
    )


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    """Convert a value to float with a stable fallback."""

    converted = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(converted):
        return float(default)
    return float(converted)


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
