"""Fixed intervention catalog for rule-based counterfactual simulation."""

from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd


@dataclass(frozen=True, slots=True)
class InterventionDefinition:
    """Metadata and simple heuristics for one intervention option."""

    intervention_name: str
    intervention_category: str
    primary_scope_impact: str
    default_start_year: int
    default_adoption_pct: float
    relative_cost_bucket: str
    sector_applicability_notes: str
    base_effect_pct: float
    base_cost_per_tco2e_usd: float


INTERVENTION_CATALOG: tuple[InterventionDefinition, ...] = (
    InterventionDefinition(
        intervention_name="renewable_ppa",
        intervention_category="clean_power_procurement",
        primary_scope_impact="scope2_mb",
        default_start_year=2026,
        default_adoption_pct=70.0,
        relative_cost_bucket="medium",
        sector_applicability_notes=(
            "Best fit for companies with material purchased power and enough contracted load "
            "to support external renewable procurement."
        ),
        base_effect_pct=60.0,
        base_cost_per_tco2e_usd=38.0,
    ),
    InterventionDefinition(
        intervention_name="fleet_electrification",
        intervention_category="transport_decarbonization",
        primary_scope_impact="scope1",
        default_start_year=2026,
        default_adoption_pct=45.0,
        relative_cost_bucket="high",
        sector_applicability_notes=(
            "Strongest fit for logistics and distribution-heavy fleets; weaker for office-led sectors."
        ),
        base_effect_pct=24.0,
        base_cost_per_tco2e_usd=95.0,
    ),
    InterventionDefinition(
        intervention_name="hvac_efficiency_upgrade",
        intervention_category="building_efficiency",
        primary_scope_impact="mixed",
        default_start_year=2026,
        default_adoption_pct=55.0,
        relative_cost_bucket="medium",
        sector_applicability_notes=(
            "Most applicable to office, retail, and healthcare footprints with sizable building energy demand."
        ),
        base_effect_pct=7.0,
        base_cost_per_tco2e_usd=52.0,
    ),
    InterventionDefinition(
        intervention_name="onsite_solar",
        intervention_category="onsite_generation",
        primary_scope_impact="scope2_lb",
        default_start_year=2027,
        default_adoption_pct=35.0,
        relative_cost_bucket="high",
        sector_applicability_notes=(
            "Moderate electricity reduction option with best fit where sites can host distributed generation."
        ),
        base_effect_pct=20.0,
        base_cost_per_tco2e_usd=78.0,
    ),
    InterventionDefinition(
        intervention_name="fuel_switch_low_carbon",
        intervention_category="fuel_decarbonization",
        primary_scope_impact="scope1",
        default_start_year=2026,
        default_adoption_pct=40.0,
        relative_cost_bucket="high",
        sector_applicability_notes=(
            "Strongest fit for industrial heat and combustion loads in utilities, materials, and manufacturing."
        ),
        base_effect_pct=30.0,
        base_cost_per_tco2e_usd=88.0,
    ),
    InterventionDefinition(
        intervention_name="process_efficiency_program",
        intervention_category="operational_efficiency",
        primary_scope_impact="mixed",
        default_start_year=2026,
        default_adoption_pct=60.0,
        relative_cost_bucket="low",
        sector_applicability_notes=(
            "Broad low-regret efficiency lever with modest abatement across most sectors."
        ),
        base_effect_pct=9.0,
        base_cost_per_tco2e_usd=22.0,
    ),
)


def get_intervention_catalog() -> tuple[InterventionDefinition, ...]:
    """Return the fixed intervention catalog."""

    return INTERVENTION_CATALOG


def intervention_catalog_dataframe() -> pd.DataFrame:
    """Return the intervention catalog as a dataframe."""

    return pd.DataFrame([asdict(intervention) for intervention in INTERVENTION_CATALOG]).convert_dtypes()


def get_intervention_definition(intervention_name: str) -> InterventionDefinition:
    """Return one intervention definition by name."""

    for intervention in INTERVENTION_CATALOG:
        if intervention.intervention_name == intervention_name:
            return intervention
    raise KeyError(f"Unknown intervention '{intervention_name}'.")
