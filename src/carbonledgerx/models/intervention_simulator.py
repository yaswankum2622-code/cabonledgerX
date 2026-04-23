"""Deterministic intervention scenario simulation for synthetic companies."""

from __future__ import annotations

from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.models.intervention_library import (
    InterventionDefinition,
    get_intervention_catalog,
)
from carbonledgerx.utils.paths import processed_data_path


MIN_INTERVENTION_START_YEAR = 2026

RISK_START_YEAR_DELAY = {
    "severe": 0,
    "high": 0,
    "moderate": 1,
    "low": 2,
}

RISK_ADOPTION_MULTIPLIER = {
    "severe": 1.10,
    "high": 1.05,
    "moderate": 1.00,
    "low": 0.90,
}

SECTOR_APPLICABILITY_MULTIPLIERS = {
    "renewable_ppa": {
        "Utilities": 1.10,
        "Technology": 1.15,
        "Retail": 1.10,
        "Healthcare": 1.10,
        "Consumer Goods": 1.00,
        "Manufacturing": 0.95,
        "Materials": 0.90,
        "Logistics": 0.85,
    },
    "fleet_electrification": {
        "Logistics": 1.35,
        "Retail": 1.10,
        "Consumer Goods": 1.10,
        "Healthcare": 1.00,
        "Technology": 0.95,
        "Manufacturing": 0.90,
        "Utilities": 0.90,
        "Materials": 0.85,
    },
    "hvac_efficiency_upgrade": {
        "Technology": 1.25,
        "Retail": 1.20,
        "Healthcare": 1.20,
        "Consumer Goods": 1.05,
        "Logistics": 0.95,
        "Utilities": 0.90,
        "Manufacturing": 0.85,
        "Materials": 0.80,
    },
    "onsite_solar": {
        "Technology": 1.10,
        "Utilities": 1.05,
        "Retail": 1.05,
        "Healthcare": 1.00,
        "Manufacturing": 1.00,
        "Consumer Goods": 1.00,
        "Logistics": 0.95,
        "Materials": 0.95,
    },
    "fuel_switch_low_carbon": {
        "Utilities": 1.20,
        "Materials": 1.20,
        "Manufacturing": 1.15,
        "Consumer Goods": 0.85,
        "Logistics": 0.90,
        "Retail": 0.70,
        "Healthcare": 0.70,
        "Technology": 0.60,
    },
    "process_efficiency_program": {
        "Utilities": 1.10,
        "Materials": 1.10,
        "Manufacturing": 1.10,
        "Logistics": 1.00,
        "Retail": 1.00,
        "Consumer Goods": 1.00,
        "Healthcare": 0.95,
        "Technology": 0.95,
    },
}


def build_company_intervention_scenarios(
    *,
    baseline: pd.DataFrame | None = None,
    assessment: pd.DataFrame | None = None,
    risk_scores: pd.DataFrame | None = None,
    company_panel: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build a deterministic company-by-intervention scenario table."""

    if baseline is None:
        baseline = _read_processed_table("company_emissions_baseline.parquet")
    if assessment is None:
        assessment = _read_processed_table("company_commitment_assessment.parquet")
    if risk_scores is None:
        risk_scores = _read_processed_table("company_commitment_risk_scores.parquet")
    if company_panel is None:
        company_panel = _read_processed_table("company_synthetic_panel.parquet")

    scenario_input = _build_scenario_input(
        baseline=baseline,
        assessment=assessment,
        risk_scores=risk_scores,
        company_panel=company_panel,
    )
    scenario_rows = [
        scenario_row
        for company_record in scenario_input.to_dict(orient="records")
        for scenario_row in _simulate_company_interventions(company_record)
    ]
    scenario_dataframe = pd.DataFrame(scenario_rows).convert_dtypes()

    selected_key_fields = [
        "company_id",
        "intervention_name",
        "modeled_abatement_tco2e",
        "cost_per_tco2e",
        "projected_total_mb_post_intervention",
        "closes_gap_flag",
    ]
    assumptions = [
        "Intervention abatement is modeled from current baseline emissions using fixed intervention effect sizes, sector applicability multipliers, adoption rates, and a simple implementation ramp.",
        "Target miss risk bands influence scenario timing and adoption modestly, but the model remains deterministic and non-probabilistic.",
        "Modeled cost uses fixed USD-per-tCO2e heuristics from the intervention catalog, lightly adjusted for sector applicability rather than any capital-budget optimization.",
    ]
    return ProcessedTableArtifact(
        output_name="company_intervention_scenarios.parquet",
        dataframe=scenario_dataframe.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[
            "company_emissions_baseline.parquet",
            "company_commitment_assessment.parquet",
            "company_commitment_risk_scores.parquet",
            "company_synthetic_panel.parquet",
        ],
    )


def _build_scenario_input(
    *,
    baseline: pd.DataFrame,
    assessment: pd.DataFrame,
    risk_scores: pd.DataFrame,
    company_panel: pd.DataFrame,
) -> pd.DataFrame:
    """Build the company-level input frame for intervention simulation."""

    baseline_columns = [
        "company_id",
        "company_name",
        "sector",
        "country",
        "baseline_year",
        "current_scope1_tco2e",
        "current_scope2_lb_tco2e",
        "current_scope2_mb_tco2e",
        "current_total_mb_tco2e",
    ]
    assessment_columns = [
        "company_id",
        "assessment_year",
        "baseline_total_mb_tco2e",
        "projected_total_mb_tco2e",
        "target_gap_tco2e",
        "target_met_flag",
    ]
    risk_columns = [
        "company_id",
        "target_miss_risk_score",
        "risk_band",
    ]
    panel_columns = [
        "company_id",
        "renewable_share_pct",
        "fleet_electrification_pct",
        "revenue_usd_m",
    ]

    scenario_input = baseline.loc[
        :,
        [column for column in baseline_columns if column in baseline.columns],
    ].copy()
    scenario_input = scenario_input.merge(
        assessment.loc[:, [column for column in assessment_columns if column in assessment.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    scenario_input = scenario_input.merge(
        risk_scores.loc[:, [column for column in risk_columns if column in risk_scores.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    scenario_input = scenario_input.merge(
        company_panel.loc[:, [column for column in panel_columns if column in company_panel.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
    )

    numeric_columns = [
        "baseline_year",
        "current_scope1_tco2e",
        "current_scope2_lb_tco2e",
        "current_scope2_mb_tco2e",
        "current_total_mb_tco2e",
        "assessment_year",
        "baseline_total_mb_tco2e",
        "projected_total_mb_tco2e",
        "target_gap_tco2e",
        "target_miss_risk_score",
        "renewable_share_pct",
        "fleet_electrification_pct",
        "revenue_usd_m",
    ]
    for column_name in numeric_columns:
        if column_name in scenario_input.columns:
            scenario_input[column_name] = pd.to_numeric(scenario_input[column_name], errors="coerce")

    return scenario_input


def _simulate_company_interventions(company_record: dict[str, Any]) -> list[dict[str, Any]]:
    """Simulate every catalog intervention for one company."""

    return [
        _simulate_one_intervention(company_record=company_record, intervention=intervention)
        for intervention in get_intervention_catalog()
    ]


def _simulate_one_intervention(
    *,
    company_record: dict[str, Any],
    intervention: InterventionDefinition,
) -> dict[str, Any]:
    """Simulate one company x intervention row."""

    sector = str(company_record["sector"])
    risk_band = str(company_record.get("risk_band", "moderate") or "moderate")
    renewable_share_pct = _safe_float(company_record.get("renewable_share_pct"))
    fleet_electrification_pct = _safe_float(company_record.get("fleet_electrification_pct"))
    baseline_year = int(_safe_float(company_record.get("baseline_year"), default=2023.0))
    assessment_year = int(_safe_float(company_record.get("assessment_year"), default=2030.0))
    projected_total_mb_base = _safe_float(company_record.get("projected_total_mb_tco2e"))
    baseline_total_mb = _safe_float(company_record.get("baseline_total_mb_tco2e"))
    target_gap_tco2e = _safe_float(company_record.get("target_gap_tco2e"))

    applicability_multiplier = _sector_applicability_multiplier(
        intervention_name=intervention.intervention_name,
        sector=sector,
    )
    start_year = _derive_start_year(
        baseline_year=baseline_year,
        default_start_year=intervention.default_start_year,
        risk_band=risk_band,
    )
    adoption_pct = _derive_adoption_pct(
        intervention=intervention,
        risk_band=risk_band,
        renewable_share_pct=renewable_share_pct,
        fleet_electrification_pct=fleet_electrification_pct,
    )
    implementation_ramp_factor = _implementation_ramp_factor(
        start_year=start_year,
        assessment_year=assessment_year,
    )
    emissions_base = _intervention_emissions_base(
        company_record=company_record,
        intervention_name=intervention.intervention_name,
    )

    modeled_abatement_tco2e = (
        emissions_base
        * (intervention.base_effect_pct / 100.0)
        * (adoption_pct / 100.0)
        * applicability_multiplier
        * implementation_ramp_factor
    )
    modeled_abatement_tco2e = round(max(0.0, modeled_abatement_tco2e), 3)

    modeled_abatement_pct = 0.0
    if baseline_total_mb > 0:
        modeled_abatement_pct = round((modeled_abatement_tco2e / baseline_total_mb) * 100.0, 3)

    cost_per_tco2e = round(
        intervention.base_cost_per_tco2e_usd
        * _cost_applicability_multiplier(applicability_multiplier),
        3,
    )
    modeled_cost_usd_m = round((modeled_abatement_tco2e * cost_per_tco2e) / 1_000_000.0, 6)

    projected_total_mb_post_intervention = round(
        max(0.0, projected_total_mb_base - modeled_abatement_tco2e),
        3,
    )
    improvement_vs_base_tco2e = modeled_abatement_tco2e
    improvement_vs_base_pct = 0.0
    if projected_total_mb_base > 0:
        improvement_vs_base_pct = round(
            (modeled_abatement_tco2e / projected_total_mb_base) * 100.0,
            3,
        )

    closes_gap_flag = target_gap_tco2e <= 0 or modeled_abatement_tco2e >= target_gap_tco2e
    partially_closes_gap_flag = (
        target_gap_tco2e > 0 and 0 < modeled_abatement_tco2e < target_gap_tco2e
    )

    return {
        "company_id": company_record["company_id"],
        "company_name": company_record["company_name"],
        "sector": sector,
        "country": company_record["country"],
        "intervention_name": intervention.intervention_name,
        "intervention_category": intervention.intervention_category,
        "primary_scope_impact": intervention.primary_scope_impact,
        "relative_cost_bucket": intervention.relative_cost_bucket,
        "assessment_year": assessment_year,
        "base_case_projected_total_mb_tco2e": round(projected_total_mb_base, 3),
        "start_year": start_year,
        "adoption_pct": round(adoption_pct, 2),
        "baseline_total_mb_tco2e": round(baseline_total_mb, 3),
        "target_gap_tco2e": round(target_gap_tco2e, 3),
        "modeled_abatement_tco2e": modeled_abatement_tco2e,
        "modeled_abatement_pct": modeled_abatement_pct,
        "modeled_cost_usd_m": modeled_cost_usd_m,
        "cost_per_tco2e": cost_per_tco2e,
        "projected_total_mb_post_intervention": projected_total_mb_post_intervention,
        "improvement_vs_base_tco2e": round(improvement_vs_base_tco2e, 3),
        "improvement_vs_base_pct": improvement_vs_base_pct,
        "closes_gap_flag": bool(closes_gap_flag),
        "partially_closes_gap_flag": bool(partially_closes_gap_flag),
        "target_miss_risk_score": round(_safe_float(company_record.get("target_miss_risk_score")), 3),
        "risk_band": risk_band,
        "sector_applicability_multiplier": round(applicability_multiplier, 3),
        "implementation_ramp_factor": round(implementation_ramp_factor, 3),
        "intervention_notes": _intervention_notes(
            intervention=intervention,
            applicability_multiplier=applicability_multiplier,
            adoption_pct=adoption_pct,
            start_year=start_year,
            risk_band=risk_band,
            target_gap_tco2e=target_gap_tco2e,
        ),
    }


def _derive_start_year(*, baseline_year: int, default_start_year: int, risk_band: str) -> int:
    """Derive a deterministic intervention start year."""

    risk_delay = RISK_START_YEAR_DELAY.get(risk_band, 1)
    return max(MIN_INTERVENTION_START_YEAR, baseline_year + 1, default_start_year + risk_delay)


def _derive_adoption_pct(
    *,
    intervention: InterventionDefinition,
    risk_band: str,
    renewable_share_pct: float,
    fleet_electrification_pct: float,
) -> float:
    """Derive a deterministic adoption rate for one company x intervention."""

    adoption_pct = intervention.default_adoption_pct * RISK_ADOPTION_MULTIPLIER.get(risk_band, 1.0)

    if intervention.intervention_name in {"renewable_ppa", "onsite_solar"}:
        if renewable_share_pct < 25:
            adoption_pct *= 1.15
        elif renewable_share_pct >= 60:
            adoption_pct *= 0.85

    if intervention.intervention_name == "fleet_electrification":
        if fleet_electrification_pct < 20:
            adoption_pct *= 1.15
        elif fleet_electrification_pct >= 45:
            adoption_pct *= 0.90

    return min(95.0, max(15.0, adoption_pct))


def _implementation_ramp_factor(*, start_year: int, assessment_year: int) -> float:
    """Return a simple implementation ramp factor by years available."""

    years_available = assessment_year - start_year
    if years_available >= 4:
        return 1.00
    if years_available >= 2:
        return 0.85
    if years_available >= 1:
        return 0.70
    return 0.55


def _intervention_emissions_base(
    *,
    company_record: dict[str, Any],
    intervention_name: str,
) -> float:
    """Return the emissions base affected by one intervention."""

    scope1 = _safe_float(company_record.get("current_scope1_tco2e"))
    scope2_lb = _safe_float(company_record.get("current_scope2_lb_tco2e"))
    scope2_mb = _safe_float(company_record.get("current_scope2_mb_tco2e"))
    total_mb = _safe_float(company_record.get("current_total_mb_tco2e"))

    if intervention_name == "renewable_ppa":
        return scope2_mb
    if intervention_name == "fleet_electrification":
        return scope1
    if intervention_name == "hvac_efficiency_upgrade":
        return (scope1 * 0.35) + (scope2_mb * 0.65)
    if intervention_name == "onsite_solar":
        return max(scope2_mb, scope2_lb * 0.85)
    if intervention_name == "fuel_switch_low_carbon":
        return scope1
    if intervention_name == "process_efficiency_program":
        return total_mb
    raise KeyError(f"Unsupported intervention '{intervention_name}'.")


def _sector_applicability_multiplier(*, intervention_name: str, sector: str) -> float:
    """Return the sector applicability multiplier for one intervention."""

    return float(SECTOR_APPLICABILITY_MULTIPLIERS.get(intervention_name, {}).get(sector, 1.0))


def _cost_applicability_multiplier(applicability_multiplier: float) -> float:
    """Translate applicability into a modest cost uplift or discount."""

    if applicability_multiplier >= 1.10:
        return 0.90
    if applicability_multiplier >= 1.00:
        return 1.00
    if applicability_multiplier >= 0.90:
        return 1.08
    return 1.18


def _intervention_notes(
    *,
    intervention: InterventionDefinition,
    applicability_multiplier: float,
    adoption_pct: float,
    start_year: int,
    risk_band: str,
    target_gap_tco2e: float,
) -> str:
    """Build a compact, auditable note for one intervention scenario."""

    gap_note = "target gap already closed in base case" if target_gap_tco2e <= 0 else "target gap remains open in base case"
    return (
        f"{intervention.intervention_name} uses {intervention.base_effect_pct:.1f}% base effect with "
        f"{applicability_multiplier:.2f} sector multiplier, {adoption_pct:.1f}% adoption, and start year {start_year}; "
        f"risk band {risk_band}; {gap_note}."
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
