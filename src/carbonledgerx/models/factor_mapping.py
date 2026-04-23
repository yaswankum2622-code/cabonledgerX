"""Factor-reference mapping for the synthetic company panel."""

from __future__ import annotations

import re

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


SCOPE1_FAMILY_RULES = {
    "Utilities": ("industrial_combustion", "defra_emission_factors::Scope 1::Fuels::Gaseous fuels"),
    "Materials": ("industrial_combustion", "defra_emission_factors::Scope 1::Fuels::Solid fuels"),
    "Manufacturing": ("industrial_combustion", "defra_emission_factors::Scope 1::Fuels::Gaseous fuels"),
    "Logistics": ("transport_heavy", "defra_emission_factors::Scope 1::Delivery vehicles::HGV (all diesel)"),
    "Retail": ("commercial_buildings", "defra_emission_factors::Scope 1::Fuels::Gaseous fuels"),
    "Healthcare": ("commercial_buildings", "defra_emission_factors::Scope 1::Fuels::Gaseous fuels"),
    "Consumer Goods": ("commercial_buildings", "defra_emission_factors::Scope 1::Fuels::Gaseous fuels"),
    "Technology": ("low_combustion_office", "defra_emission_factors::Scope 1::Fuels::Gaseous fuels"),
}

INTERNATIONAL_PROXY_RULES = {
    "Canada": ("international_average_proxy_low_carbon", 0.55),
    "France": ("international_average_proxy_low_carbon", 0.45),
    "Brazil": ("international_average_proxy_low_carbon", 0.50),
    "United Kingdom": ("international_average_proxy_medium_grid", 0.75),
    "Germany": ("international_average_proxy_medium_grid", 0.90),
    "Japan": ("international_average_proxy_medium_grid", 0.95),
    "Australia": ("international_average_proxy_medium_grid", 1.05),
    "Singapore": ("international_average_proxy_medium_grid", 1.10),
    "India": ("international_average_proxy_high_grid", 1.35),
}


def build_company_factor_mapping() -> ProcessedTableArtifact:
    """Build a transparent factor-reference mapping for synthetic companies."""

    company_panel = _read_processed_table("company_synthetic_panel.parquet")
    egrid_state = _read_processed_table("egrid_state_factors.parquet")

    latest_state_factors = _latest_egrid_state_factors(egrid_state)
    us_state_codes = sorted(
        state_code
        for state_code in latest_state_factors["state_code"].dropna().astype(str).unique().tolist()
        if state_code != "PR"
    )
    state_factor_lookup = latest_state_factors.set_index("state_code")[
        "state_annual_co2_total_output_emission_rate_lb_mwh"
    ].to_dict()
    us_average_lb_factor = float(
        pd.to_numeric(
            latest_state_factors["state_annual_co2_total_output_emission_rate_lb_mwh"],
            errors="coerce",
        ).mean()
    )

    mapping_rows: list[dict[str, object]] = []

    for company in company_panel.to_dict(orient="records"):
        company_id = str(company["company_id"])
        country = str(company["country"])
        sector = str(company["sector"])
        renewable_share_pct = float(company["renewable_share_pct"])
        scope1_factor_family, scope1_factor_reference = _assign_scope1_factor_family(sector)
        scope2_mb_source, scope2_mb_multiplier = _assign_market_based_rule(renewable_share_pct)

        if country == "United States":
            state_code = _assign_proxy_state_code(company_id, us_state_codes)
            lb_factor_value = float(state_factor_lookup[state_code])
            factor_region_type = "egrid_state_proxy"
            factor_region_key = state_code
            scope2_lb_factor_source = (
                f"egrid_state_factors::2023::{state_code}::"
                "state_annual_co2_total_output_emission_rate_lb_mwh"
            )
            source_notes = (
                "Synthetic panel has no US state field; assigned a deterministic proxy eGRID "
                "state_code from the 2023 state factors using company_id ordering."
            )
        else:
            proxy_category, proxy_multiplier = _assign_international_proxy(country)
            lb_factor_value = round(us_average_lb_factor * proxy_multiplier, 3)
            factor_region_type = proxy_category
            factor_region_key = country
            scope2_lb_factor_source = f"{proxy_category}::{country}::proxy_grid_intensity_lb_mwh"
            source_notes = (
                "Non-US company assigned a country-level international proxy derived from the "
                "latest US eGRID state average with a documented country multiplier."
            )

        mapping_rows.append(
            {
                "company_id": company_id,
                "company_name": company["company_name"],
                "sector": sector,
                "country": country,
                "factor_region_type": factor_region_type,
                "factor_region_key": factor_region_key,
                "scope1_factor_family": scope1_factor_family,
                "scope1_factor_reference": scope1_factor_reference,
                "scope2_lb_factor_source": scope2_lb_factor_source,
                "scope2_lb_factor_value_lb_mwh": lb_factor_value,
                "scope2_mb_factor_source": scope2_mb_source,
                "scope2_mb_adjustment_multiplier": scope2_mb_multiplier,
                "scope2_mb_reference_value_lb_mwh": round(lb_factor_value * scope2_mb_multiplier, 3),
                "mapping_notes": source_notes,
            }
        )

    mapping_dataframe = pd.DataFrame(mapping_rows).convert_dtypes()
    selected_key_fields = [
        "company_id",
        "factor_region_type",
        "factor_region_key",
        "scope1_factor_family",
        "scope2_lb_factor_source",
        "scope2_mb_factor_source",
    ]
    assumptions = [
        "US companies use deterministic proxy eGRID state references from the 2023 state factors because the synthetic panel has country but not state.",
        "Non-US companies use simple international proxy categories derived from the latest eGRID state average with documented country multipliers.",
        "Market-based Scope 2 references are modeled as procurement-adjustment categories based on renewable_share_pct rather than recalculated emissions.",
        "Scope 1 factor families are broad sector-based proxies aligned to high-level DEFRA categories.",
    ]
    return ProcessedTableArtifact(
        output_name="company_factor_mapping.parquet",
        dataframe=mapping_dataframe,
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=["company_synthetic_panel.parquet", "egrid_state_factors.parquet", "defra_emission_factors.parquet"],
    )


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))


def _latest_egrid_state_factors(egrid_state_factors: pd.DataFrame) -> pd.DataFrame:
    """Return the most recent eGRID state factors slice."""

    latest_year = int(pd.to_numeric(egrid_state_factors["year"], errors="coerce").max())
    latest = egrid_state_factors.loc[egrid_state_factors["year"] == latest_year].copy()
    latest["state_annual_co2_total_output_emission_rate_lb_mwh"] = pd.to_numeric(
        latest["state_annual_co2_total_output_emission_rate_lb_mwh"],
        errors="coerce",
    )
    latest = latest.loc[latest["state_annual_co2_total_output_emission_rate_lb_mwh"].notna()].copy()
    return latest


def _assign_proxy_state_code(company_id: str, us_state_codes: list[str]) -> str:
    """Assign a deterministic proxy US state code from the available eGRID state list."""

    numeric_part_match = re.search(r"(\d+)$", company_id)
    if numeric_part_match is None:
        raise ValueError(f"Could not derive a deterministic numeric suffix from company_id '{company_id}'.")

    numeric_part = int(numeric_part_match.group(1))
    return us_state_codes[(numeric_part - 1) % len(us_state_codes)]


def _assign_international_proxy(country: str) -> tuple[str, float]:
    """Assign a documented international electricity proxy category and multiplier."""

    if country in INTERNATIONAL_PROXY_RULES:
        return INTERNATIONAL_PROXY_RULES[country]

    return ("international_average_proxy_standard", 1.0)


def _assign_market_based_rule(renewable_share_pct: float) -> tuple[str, float]:
    """Assign a simple market-based adjustment rule from renewable share."""

    if renewable_share_pct >= 70:
        return ("market_based_proxy_high_renewable_procurement", 0.35)
    if renewable_share_pct >= 40:
        return ("market_based_proxy_partial_renewable_procurement", 0.60)
    return ("market_based_proxy_standard_procurement", 0.85)


def _assign_scope1_factor_family(sector: str) -> tuple[str, str]:
    """Assign a broad scope 1 factor family and reference from sector."""

    if sector in SCOPE1_FAMILY_RULES:
        return SCOPE1_FAMILY_RULES[sector]

    return ("commercial_buildings", "defra_emission_factors::Scope 1::Fuels::Gaseous fuels")
