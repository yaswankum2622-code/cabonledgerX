"""Synthetic activity-input generation for the emissions calculator layer."""

from __future__ import annotations

from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.models.emissions_calculator import LB_PER_METRIC_TON, Scope1FactorSet, load_scope1_factor_set
from carbonledgerx.models.synthetic_company_panel import SECTOR_CONFIG
from carbonledgerx.utils.paths import processed_data_path


ACTIVITY_PROXY_CONFIG = {
    "Utilities": {
        "scope1_gas_share": 0.78,
        "base_facilities": 8,
        "revenue_per_facility_m": 320.0,
        "avg_facility_sqft": 115_000.0,
        "base_fleet": 55,
        "revenue_per_fleet_unit_m": 95.0,
        "production_index_per_revenue_m": 0.95,
    },
    "Materials": {
        "scope1_gas_share": 0.65,
        "base_facilities": 5,
        "revenue_per_facility_m": 420.0,
        "avg_facility_sqft": 98_000.0,
        "base_fleet": 28,
        "revenue_per_fleet_unit_m": 120.0,
        "production_index_per_revenue_m": 1.10,
    },
    "Manufacturing": {
        "scope1_gas_share": 0.62,
        "base_facilities": 4,
        "revenue_per_facility_m": 460.0,
        "avg_facility_sqft": 88_000.0,
        "base_fleet": 24,
        "revenue_per_fleet_unit_m": 135.0,
        "production_index_per_revenue_m": 1.05,
    },
    "Logistics": {
        "scope1_gas_share": 0.20,
        "base_facilities": 7,
        "revenue_per_facility_m": 240.0,
        "avg_facility_sqft": 72_000.0,
        "base_fleet": 65,
        "revenue_per_fleet_unit_m": 28.0,
        "production_index_per_revenue_m": 0.75,
    },
    "Technology": {
        "scope1_gas_share": 0.70,
        "base_facilities": 3,
        "revenue_per_facility_m": 700.0,
        "avg_facility_sqft": 58_000.0,
        "base_fleet": 10,
        "revenue_per_fleet_unit_m": 380.0,
        "production_index_per_revenue_m": 0.38,
    },
    "Retail": {
        "scope1_gas_share": 0.58,
        "base_facilities": 9,
        "revenue_per_facility_m": 260.0,
        "avg_facility_sqft": 42_000.0,
        "base_fleet": 26,
        "revenue_per_fleet_unit_m": 150.0,
        "production_index_per_revenue_m": 0.55,
    },
    "Healthcare": {
        "scope1_gas_share": 0.68,
        "base_facilities": 6,
        "revenue_per_facility_m": 340.0,
        "avg_facility_sqft": 54_000.0,
        "base_fleet": 18,
        "revenue_per_fleet_unit_m": 210.0,
        "production_index_per_revenue_m": 0.48,
    },
    "Consumer Goods": {
        "scope1_gas_share": 0.50,
        "base_facilities": 6,
        "revenue_per_facility_m": 300.0,
        "avg_facility_sqft": 61_000.0,
        "base_fleet": 22,
        "revenue_per_fleet_unit_m": 165.0,
        "production_index_per_revenue_m": 0.82,
    },
}

SECTOR_BLEND_WEIGHT = 0.30
BASELINE_BLEND_WEIGHT = 0.70


def build_company_activity_inputs(
    *,
    company_panel: pd.DataFrame | None = None,
    factor_mapping: pd.DataFrame | None = None,
    defra_factors: pd.DataFrame | None = None,
    egrid_state_factors: pd.DataFrame | None = None,
    egrid_ba_factors: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build deterministic annual activity inputs for synthetic companies."""

    if company_panel is None:
        company_panel = _read_processed_table("company_synthetic_panel.parquet")
    if factor_mapping is None:
        factor_mapping = _read_processed_table("company_factor_mapping.parquet")
    if defra_factors is None:
        defra_factors = _read_processed_table("defra_emission_factors.parquet")
    if egrid_state_factors is None:
        egrid_state_factors = _read_processed_table("egrid_state_factors.parquet")
    if egrid_ba_factors is None:
        egrid_ba_factors = _read_processed_table("egrid_ba_factors.parquet")

    scope1_factor_set = load_scope1_factor_set(defra_factors)
    latest_state_lookup = _latest_state_factor_lookup(egrid_state_factors)
    latest_ba_lookup = _latest_ba_factor_lookup(egrid_ba_factors)

    activity_input = company_panel.merge(
        factor_mapping,
        on=["company_id", "company_name", "sector", "country"],
        how="left",
        validate="one_to_one",
    )
    numeric_columns = [
        "revenue_usd_m",
        "base_year",
        "current_scope1_tco2e",
        "current_scope2_lb_tco2e",
        "renewable_share_pct",
        "fleet_electrification_pct",
    ]
    for column_name in numeric_columns:
        if column_name in activity_input.columns:
            activity_input[column_name] = pd.to_numeric(activity_input[column_name], errors="coerce")

    activity_rows = [
        _build_company_activity_row(
            company_record=company_record,
            scope1_factor_set=scope1_factor_set,
            latest_state_lookup=latest_state_lookup,
            latest_ba_lookup=latest_ba_lookup,
        )
        for company_record in activity_input.to_dict(orient="records")
    ]
    activity_dataframe = pd.DataFrame(activity_rows).convert_dtypes()

    selected_key_fields = [
        "company_id",
        "electricity_mwh",
        "natural_gas_mmbtu",
        "diesel_liters",
        "fleet_size_proxy",
        "facility_count_proxy",
    ]
    assumptions = [
        "Activity values blend a baseline-implied activity anchor from the existing synthetic emissions with a sector-midpoint anchor derived from the synthetic panel sector configuration.",
        "Scope 1 activity uses a fixed sector fuel split between natural gas and diesel, while fleet electrification dampens diesel activity for more electrified companies.",
        "Proxy fields such as fleet size, facility count, floor area, and production index are deterministic functions of revenue and sector, not random noise draws.",
    ]
    return ProcessedTableArtifact(
        output_name="company_activity_inputs.parquet",
        dataframe=activity_dataframe.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[
            "company_synthetic_panel.parquet",
            "company_factor_mapping.parquet",
            "defra_emission_factors.parquet",
            "egrid_state_factors.parquet",
            "egrid_ba_factors.parquet",
        ],
    )


def _build_company_activity_row(
    *,
    company_record: dict[str, Any],
    scope1_factor_set: Scope1FactorSet,
    latest_state_lookup: dict[str, float],
    latest_ba_lookup: dict[str, float],
) -> dict[str, Any]:
    """Build deterministic activity inputs for one company."""

    sector = str(company_record["sector"])
    sector_proxy_config = ACTIVITY_PROXY_CONFIG[sector]
    synthetic_sector_config = SECTOR_CONFIG[sector]

    revenue_usd_m = _safe_float(company_record.get("revenue_usd_m"))
    renewable_share_pct = _safe_float(company_record.get("renewable_share_pct"))
    fleet_electrification_pct = _safe_float(company_record.get("fleet_electrification_pct"))
    current_scope1_tco2e = _safe_float(company_record.get("current_scope1_tco2e"))
    current_scope2_lb_tco2e = _safe_float(company_record.get("current_scope2_lb_tco2e"))

    facility_count_proxy = _facility_count_proxy(
        revenue_usd_m=revenue_usd_m,
        sector_proxy_config=sector_proxy_config,
    )
    fleet_size_proxy = _fleet_size_proxy(
        revenue_usd_m=revenue_usd_m,
        fleet_electrification_pct=fleet_electrification_pct,
        sector_proxy_config=sector_proxy_config,
    )
    floor_area_sqft_proxy = round(
        facility_count_proxy * sector_proxy_config["avg_facility_sqft"],
        0,
    )
    production_index_proxy = round(
        revenue_usd_m * sector_proxy_config["production_index_per_revenue_m"],
        1,
    )

    scope1_gas_share = sector_proxy_config["scope1_gas_share"]
    scope1_diesel_share = 1.0 - scope1_gas_share
    location_factor_lb_mwh = _resolve_location_factor_value(
        company_record=company_record,
        latest_state_lookup=latest_state_lookup,
        latest_ba_lookup=latest_ba_lookup,
    )

    sector_scope1_midpoint = sum(synthetic_sector_config["scope1_factor"]) / 2.0
    sector_scope2_midpoint = sum(synthetic_sector_config["scope2_factor"]) / 2.0

    baseline_electricity_mwh = (
        current_scope2_lb_tco2e * LB_PER_METRIC_TON / max(1.0, location_factor_lb_mwh)
    )
    sector_electricity_mwh = (
        revenue_usd_m * sector_scope2_midpoint * LB_PER_METRIC_TON / max(1.0, location_factor_lb_mwh)
    )
    electricity_mwh = _blend_activity(
        baseline_anchor=baseline_electricity_mwh,
        sector_anchor=sector_electricity_mwh,
    )

    baseline_natural_gas_mmbtu = (
        current_scope1_tco2e * scope1_gas_share * 1000.0 / scope1_factor_set.natural_gas_kg_per_mmbtu
    )
    sector_natural_gas_mmbtu = (
        revenue_usd_m * sector_scope1_midpoint * scope1_gas_share * 1000.0
        / scope1_factor_set.natural_gas_kg_per_mmbtu
    )
    natural_gas_mmbtu = _blend_activity(
        baseline_anchor=baseline_natural_gas_mmbtu,
        sector_anchor=sector_natural_gas_mmbtu,
    )

    baseline_diesel_liters = (
        current_scope1_tco2e * scope1_diesel_share * 1000.0 / scope1_factor_set.diesel_kg_per_liter
    )
    sector_diesel_liters = (
        revenue_usd_m * sector_scope1_midpoint * scope1_diesel_share * 1000.0
        / scope1_factor_set.diesel_kg_per_liter
    )
    electrification_adjustment = 1.0 - min(0.18, fleet_electrification_pct / 400.0)
    diesel_liters = _blend_activity(
        baseline_anchor=baseline_diesel_liters,
        sector_anchor=sector_diesel_liters,
    ) * electrification_adjustment

    return {
        "company_id": company_record["company_id"],
        "company_name": company_record["company_name"],
        "sector": sector,
        "country": company_record["country"],
        "base_year": int(_safe_float(company_record.get("base_year"), default=0.0)),
        "electricity_mwh": round(electricity_mwh, 3),
        "natural_gas_mmbtu": round(natural_gas_mmbtu, 3),
        "diesel_liters": round(diesel_liters, 3),
        "fleet_size_proxy": int(fleet_size_proxy),
        "facility_count_proxy": int(facility_count_proxy),
        "floor_area_sqft_proxy": round(floor_area_sqft_proxy, 0),
        "production_index_proxy": production_index_proxy,
        "activity_notes": (
            f"Activity inputs use a {int(BASELINE_BLEND_WEIGHT * 100)}% baseline-emissions anchor and "
            f"{int(SECTOR_BLEND_WEIGHT * 100)}% sector-midpoint anchor. Scope 1 split for {sector} is "
            f"{scope1_gas_share:.0%} natural gas / {scope1_diesel_share:.0%} diesel. Diesel activity "
            f"is reduced by fleet electrification ({fleet_electrification_pct:.1f}%)."
        ),
    }


def _blend_activity(*, baseline_anchor: float, sector_anchor: float) -> float:
    """Blend baseline and sector activity anchors deterministically."""

    return max(
        0.0,
        (baseline_anchor * BASELINE_BLEND_WEIGHT) + (sector_anchor * SECTOR_BLEND_WEIGHT),
    )


def _facility_count_proxy(*, revenue_usd_m: float, sector_proxy_config: dict[str, float]) -> int:
    """Return a deterministic facility-count proxy."""

    return max(
        1,
        int(round(
            sector_proxy_config["base_facilities"]
            + (revenue_usd_m / sector_proxy_config["revenue_per_facility_m"])
        )),
    )


def _fleet_size_proxy(
    *,
    revenue_usd_m: float,
    fleet_electrification_pct: float,
    sector_proxy_config: dict[str, float],
) -> int:
    """Return a deterministic fleet-size proxy."""

    raw_fleet_size = (
        sector_proxy_config["base_fleet"]
        + (revenue_usd_m / sector_proxy_config["revenue_per_fleet_unit_m"])
    )
    electrification_adjustment = 1.0 - min(0.25, fleet_electrification_pct / 250.0)
    return max(1, int(round(raw_fleet_size * electrification_adjustment)))


def _resolve_location_factor_value(
    *,
    company_record: dict[str, Any],
    latest_state_lookup: dict[str, float],
    latest_ba_lookup: dict[str, float],
) -> float:
    """Resolve the location-based electricity factor for activity generation."""

    factor_source = str(company_record.get("scope2_lb_factor_source", "") or "")
    stored_value = _safe_float(company_record.get("scope2_lb_factor_value_lb_mwh"))

    if factor_source.startswith("egrid_state_factors::"):
        parts = factor_source.split("::")
        state_code = parts[2] if len(parts) >= 3 else ""
        return float(latest_state_lookup.get(state_code, stored_value))

    if factor_source.startswith("egrid_ba_factors::"):
        parts = factor_source.split("::")
        ba_code = parts[2] if len(parts) >= 3 else ""
        return float(latest_ba_lookup.get(ba_code, stored_value))

    return stored_value


def _latest_state_factor_lookup(egrid_state_factors: pd.DataFrame) -> dict[str, float]:
    """Return the latest state-level factor lookup."""

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
    """Return the latest BA-level factor lookup."""

    latest_year = int(pd.to_numeric(egrid_ba_factors["year"], errors="coerce").max())
    latest = egrid_ba_factors.loc[egrid_ba_factors["year"] == latest_year].copy()
    latest["ba_annual_co2_total_output_emission_rate_lb_mwh"] = pd.to_numeric(
        latest["ba_annual_co2_total_output_emission_rate_lb_mwh"],
        errors="coerce",
    )
    latest = latest.loc[latest["ba_annual_co2_total_output_emission_rate_lb_mwh"].notna()].copy()
    return latest.set_index("ba_code")["ba_annual_co2_total_output_emission_rate_lb_mwh"].to_dict()


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    """Convert a value to float with a stable fallback."""

    converted = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(converted):
        return float(default)
    return float(converted)


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
