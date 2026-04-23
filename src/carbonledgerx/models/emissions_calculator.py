"""Activity-based emissions calculator for synthetic companies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


LB_PER_METRIC_TON = 2204.62262
MMBTU_TO_KWH = 293.07107


@dataclass(frozen=True, slots=True)
class Scope1FactorSet:
    """Resolved scope 1 factors used by the calculator."""

    natural_gas_factor_id: str
    natural_gas_kg_per_kwh: float
    natural_gas_kg_per_mmbtu: float
    diesel_factor_id: str
    diesel_kg_per_liter: float


def load_scope1_factor_set(defra_factors: pd.DataFrame) -> Scope1FactorSet:
    """Resolve the DEFRA factors used for natural gas and diesel calculations."""

    natural_gas_row = _select_defra_factor(
        defra_factors,
        scope="Scope 1",
        level_1="Fuels",
        level_2="Gaseous fuels",
        level_3="Natural gas",
        factor_unit="kWh (Net CV)",
        ghg_unit="kg CO2e",
    )
    diesel_row = _select_defra_factor(
        defra_factors,
        scope="Scope 1",
        level_1="Fuels",
        level_2="Liquid fuels",
        level_3="Diesel (average biofuel blend)",
        factor_unit="litres",
        ghg_unit="kg CO2e",
    )

    natural_gas_kg_per_kwh = float(natural_gas_row["factor_value"])
    return Scope1FactorSet(
        natural_gas_factor_id=str(natural_gas_row["factor_id"]),
        natural_gas_kg_per_kwh=natural_gas_kg_per_kwh,
        natural_gas_kg_per_mmbtu=natural_gas_kg_per_kwh * MMBTU_TO_KWH,
        diesel_factor_id=str(diesel_row["factor_id"]),
        diesel_kg_per_liter=float(diesel_row["factor_value"]),
    )


def build_company_emissions_calculated(
    *,
    activity_inputs: pd.DataFrame | None = None,
    factor_mapping: pd.DataFrame | None = None,
    defra_factors: pd.DataFrame | None = None,
    egrid_state_factors: pd.DataFrame | None = None,
    egrid_ba_factors: pd.DataFrame | None = None,
    prior_baseline: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build the activity-based company emissions calculation table."""

    if activity_inputs is None:
        activity_inputs = _read_processed_table("company_activity_inputs.parquet")
    if factor_mapping is None:
        factor_mapping = _read_processed_table("company_factor_mapping.parquet")
    if defra_factors is None:
        defra_factors = _read_processed_table("defra_emission_factors.parquet")
    if egrid_state_factors is None:
        egrid_state_factors = _read_processed_table("egrid_state_factors.parquet")
    if egrid_ba_factors is None:
        egrid_ba_factors = _read_processed_table("egrid_ba_factors.parquet")
    if prior_baseline is None:
        prior_baseline = _read_processed_table("company_emissions_baseline.parquet")

    scope1_factor_set = load_scope1_factor_set(defra_factors)
    latest_state_lookup = _latest_state_factor_lookup(egrid_state_factors)
    latest_ba_lookup = _latest_ba_factor_lookup(egrid_ba_factors)

    calculation_input = activity_inputs.merge(
        factor_mapping,
        on=["company_id", "company_name", "sector", "country"],
        how="left",
        validate="one_to_one",
    )
    calculation_input = calculation_input.merge(
        prior_baseline.loc[:, ["company_id", "current_total_mb_tco2e"]],
        on="company_id",
        how="left",
        validate="one_to_one",
    ).rename(columns={"current_total_mb_tco2e": "prior_baseline_total_mb_tco2e"})

    numeric_columns = [
        "base_year",
        "electricity_mwh",
        "natural_gas_mmbtu",
        "diesel_liters",
        "scope2_lb_factor_value_lb_mwh",
        "scope2_mb_reference_value_lb_mwh",
        "prior_baseline_total_mb_tco2e",
    ]
    for column_name in numeric_columns:
        if column_name in calculation_input.columns:
            calculation_input[column_name] = pd.to_numeric(
                calculation_input[column_name],
                errors="coerce",
            )

    calculation_rows = [
        _calculate_company_emissions_row(
            company_record=company_record,
            scope1_factor_set=scope1_factor_set,
            latest_state_lookup=latest_state_lookup,
            latest_ba_lookup=latest_ba_lookup,
        )
        for company_record in calculation_input.to_dict(orient="records")
    ]
    calculated_dataframe = pd.DataFrame(calculation_rows).convert_dtypes()

    selected_key_fields = [
        "company_id",
        "calculated_scope1_tco2e",
        "calculated_scope2_lb_tco2e",
        "calculated_scope2_mb_tco2e",
        "calculated_total_mb_tco2e",
        "delta_vs_prior_baseline_mb_pct",
    ]
    assumptions = [
        "Scope 1 uses DEFRA natural gas and diesel factors with explicit conversions from MMBtu to kWh Net CV and from litres directly to kg CO2e per litre.",
        "Scope 2 location-based emissions use the mapped grid factor reference; US state or BA references are refreshed from eGRID where available, and international mappings use the documented proxy factors already stored in company_factor_mapping.",
        "Scope 2 market-based emissions use the mapped market-based adjusted factor reference from company_factor_mapping rather than assuming a separate unavailable certificate dataset.",
    ]
    return ProcessedTableArtifact(
        output_name="company_emissions_calculated.parquet",
        dataframe=calculated_dataframe.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[
            "company_activity_inputs.parquet",
            "company_factor_mapping.parquet",
            "defra_emission_factors.parquet",
            "egrid_state_factors.parquet",
            "egrid_ba_factors.parquet",
            "company_emissions_baseline.parquet",
        ],
    )


def _calculate_company_emissions_row(
    *,
    company_record: dict[str, Any],
    scope1_factor_set: Scope1FactorSet,
    latest_state_lookup: dict[str, float],
    latest_ba_lookup: dict[str, float],
) -> dict[str, Any]:
    """Calculate activity-based scope 1 and scope 2 emissions for one company."""

    electricity_mwh = _safe_float(company_record.get("electricity_mwh"))
    natural_gas_mmbtu = _safe_float(company_record.get("natural_gas_mmbtu"))
    diesel_liters = _safe_float(company_record.get("diesel_liters"))

    scope2_lb_factor_value, scope2_lb_factor_reference, scope2_lb_note = _resolve_location_factor(
        company_record=company_record,
        latest_state_lookup=latest_state_lookup,
        latest_ba_lookup=latest_ba_lookup,
    )
    scope2_mb_factor_value, scope2_mb_factor_reference, scope2_mb_note = _resolve_market_factor(
        company_record=company_record,
    )

    natural_gas_scope1_tco2e = (
        natural_gas_mmbtu * scope1_factor_set.natural_gas_kg_per_mmbtu / 1000.0
    )
    diesel_scope1_tco2e = diesel_liters * scope1_factor_set.diesel_kg_per_liter / 1000.0
    calculated_scope1_tco2e = natural_gas_scope1_tco2e + diesel_scope1_tco2e
    calculated_scope2_lb_tco2e = electricity_mwh * scope2_lb_factor_value / LB_PER_METRIC_TON
    calculated_scope2_mb_tco2e = electricity_mwh * scope2_mb_factor_value / LB_PER_METRIC_TON
    calculated_total_lb_tco2e = calculated_scope1_tco2e + calculated_scope2_lb_tco2e
    calculated_total_mb_tco2e = calculated_scope1_tco2e + calculated_scope2_mb_tco2e

    prior_baseline_total_mb_tco2e = _safe_float(company_record.get("prior_baseline_total_mb_tco2e"))
    delta_vs_prior_baseline_mb_tco2e = calculated_total_mb_tco2e - prior_baseline_total_mb_tco2e
    if prior_baseline_total_mb_tco2e > 0:
        delta_vs_prior_baseline_mb_pct = (
            delta_vs_prior_baseline_mb_tco2e / prior_baseline_total_mb_tco2e
        ) * 100.0
    else:
        delta_vs_prior_baseline_mb_pct = 0.0

    return {
        "company_id": company_record["company_id"],
        "company_name": company_record["company_name"],
        "sector": company_record["sector"],
        "country": company_record["country"],
        "base_year": int(_safe_float(company_record.get("base_year"), default=0.0)),
        "electricity_mwh": round(electricity_mwh, 3),
        "natural_gas_mmbtu": round(natural_gas_mmbtu, 3),
        "diesel_liters": round(diesel_liters, 3),
        "natural_gas_scope1_tco2e": round(natural_gas_scope1_tco2e, 3),
        "diesel_scope1_tco2e": round(diesel_scope1_tco2e, 3),
        "calculated_scope1_tco2e": round(calculated_scope1_tco2e, 3),
        "calculated_scope2_lb_tco2e": round(calculated_scope2_lb_tco2e, 3),
        "calculated_scope2_mb_tco2e": round(calculated_scope2_mb_tco2e, 3),
        "calculated_total_lb_tco2e": round(calculated_total_lb_tco2e, 3),
        "calculated_total_mb_tco2e": round(calculated_total_mb_tco2e, 3),
        "scope1_factor_reference": (
            f"defra_emission_factors::{scope1_factor_set.natural_gas_factor_id}::Natural gas "
            "kWh (Net CV) + "
            f"defra_emission_factors::{scope1_factor_set.diesel_factor_id}::Diesel litres"
        ),
        "scope2_lb_factor_reference": scope2_lb_factor_reference,
        "scope2_mb_factor_reference": scope2_mb_factor_reference,
        "scope2_lb_factor_value_lb_mwh": round(scope2_lb_factor_value, 3),
        "scope2_mb_factor_value_lb_mwh": round(scope2_mb_factor_value, 3),
        "prior_baseline_total_mb_tco2e": round(prior_baseline_total_mb_tco2e, 3),
        "delta_vs_prior_baseline_mb_tco2e": round(delta_vs_prior_baseline_mb_tco2e, 3),
        "delta_vs_prior_baseline_mb_pct": round(delta_vs_prior_baseline_mb_pct, 3),
        "calculation_notes": (
            f"Natural gas converted from MMBtu to kWh Net CV using {MMBTU_TO_KWH:.5f} kWh/MMBtu "
            f"and DEFRA factor {scope1_factor_set.natural_gas_factor_id}; diesel uses DEFRA "
            f"factor {scope1_factor_set.diesel_factor_id}. {scope2_lb_note}. {scope2_mb_note}."
        ),
    }


def _select_defra_factor(
    defra_factors: pd.DataFrame,
    *,
    scope: str,
    level_1: str,
    level_2: str,
    level_3: str,
    factor_unit: str,
    ghg_unit: str,
) -> pd.Series:
    """Select one exact DEFRA factor row."""

    mask = (
        (defra_factors["scope"] == scope)
        & (defra_factors["level_1"] == level_1)
        & (defra_factors["level_2"] == level_2)
        & (defra_factors["level_3"] == level_3)
        & (defra_factors["factor_unit"] == factor_unit)
        & (defra_factors["ghg_unit"] == ghg_unit)
    )
    matches = defra_factors.loc[mask].copy()
    if matches.empty:
        raise ValueError(
            "Could not resolve DEFRA factor for "
            f"{scope=} {level_1=} {level_2=} {level_3=} {factor_unit=} {ghg_unit=}."
        )
    return matches.iloc[0]


def _latest_state_factor_lookup(egrid_state_factors: pd.DataFrame) -> dict[str, float]:
    """Return the latest state-level location-based factor lookup."""

    latest_year = int(pd.to_numeric(egrid_state_factors["year"], errors="coerce").max())
    latest = egrid_state_factors.loc[egrid_state_factors["year"] == latest_year].copy()
    latest["state_annual_co2_total_output_emission_rate_lb_mwh"] = pd.to_numeric(
        latest["state_annual_co2_total_output_emission_rate_lb_mwh"],
        errors="coerce",
    )
    latest = latest.loc[latest["state_annual_co2_total_output_emission_rate_lb_mwh"].notna()].copy()
    return latest.set_index("state_code")[
        "state_annual_co2_total_output_emission_rate_lb_mwh"
    ].to_dict()


def _latest_ba_factor_lookup(egrid_ba_factors: pd.DataFrame) -> dict[str, float]:
    """Return the latest balancing-authority location-based factor lookup."""

    latest_year = int(pd.to_numeric(egrid_ba_factors["year"], errors="coerce").max())
    latest = egrid_ba_factors.loc[egrid_ba_factors["year"] == latest_year].copy()
    latest["ba_annual_co2_total_output_emission_rate_lb_mwh"] = pd.to_numeric(
        latest["ba_annual_co2_total_output_emission_rate_lb_mwh"],
        errors="coerce",
    )
    latest = latest.loc[latest["ba_annual_co2_total_output_emission_rate_lb_mwh"].notna()].copy()
    return latest.set_index("ba_code")[
        "ba_annual_co2_total_output_emission_rate_lb_mwh"
    ].to_dict()


def _resolve_location_factor(
    *,
    company_record: dict[str, Any],
    latest_state_lookup: dict[str, float],
    latest_ba_lookup: dict[str, float],
) -> tuple[float, str, str]:
    """Resolve the location-based electricity factor for one company."""

    factor_source = str(company_record.get("scope2_lb_factor_source", "") or "")
    mapped_value = _safe_float(company_record.get("scope2_lb_factor_value_lb_mwh"))

    if factor_source.startswith("egrid_state_factors::"):
        parts = factor_source.split("::")
        state_code = parts[2] if len(parts) >= 3 else ""
        if state_code in latest_state_lookup:
            factor_value = latest_state_lookup[state_code]
            return (
                factor_value,
                factor_source,
                f"Scope 2 LB resolved from the latest eGRID state factor for {state_code}",
            )
        return (
            mapped_value,
            factor_source,
            f"Scope 2 LB fell back to the stored state proxy value because {state_code} was missing from the latest eGRID state table",
        )

    if factor_source.startswith("egrid_ba_factors::"):
        parts = factor_source.split("::")
        ba_code = parts[2] if len(parts) >= 3 else ""
        if ba_code in latest_ba_lookup:
            factor_value = latest_ba_lookup[ba_code]
            return (
                factor_value,
                factor_source,
                f"Scope 2 LB resolved from the latest eGRID balancing-authority factor for {ba_code}",
            )
        return (
            mapped_value,
            factor_source,
            f"Scope 2 LB fell back to the stored BA proxy value because {ba_code} was missing from the latest eGRID BA table",
        )

    return (
        mapped_value,
        factor_source,
        "Scope 2 LB uses the documented international or proxy factor already stored in company_factor_mapping",
    )


def _resolve_market_factor(company_record: dict[str, Any]) -> tuple[float, str, str]:
    """Resolve the market-based electricity factor for one company."""

    factor_reference = (
        f"{company_record.get('scope2_mb_factor_source')}::"
        f"{_safe_float(company_record.get('scope2_mb_reference_value_lb_mwh')):.3f}"
        "::lb_per_mwh"
    )
    return (
        _safe_float(company_record.get("scope2_mb_reference_value_lb_mwh")),
        factor_reference,
        "Scope 2 MB uses the mapped procurement-adjusted factor value stored in company_factor_mapping",
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
