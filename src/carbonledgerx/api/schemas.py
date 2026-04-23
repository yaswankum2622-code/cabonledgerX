"""Pydantic response schemas for the TargetTruth API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class APIModel(BaseModel):
    """Base API model with permissive config for parquet-backed payloads."""

    model_config = ConfigDict(extra="ignore")


class HealthResponse(APIModel):
    """Health endpoint response."""

    status: str
    project_name: str
    available_datasets: list[str]


class CompanyListItem(APIModel):
    """Compact company list item."""

    company_id: str
    company_name: str
    sector: str
    country: str
    recommended_operational_risk_band: str
    calibrated_miss_probability: float


class CompaniesResponse(APIModel):
    """Response model for the companies listing."""

    companies: list[CompanyListItem]
    total_returned: int


class BestInterventionSummary(APIModel):
    """Compact best-intervention summary."""

    intervention_name: str | None = None
    cost_per_tco2e: float | None = None
    cost_usd_m: float | None = None
    abatement_tco2e: float | None = None
    closes_gap_flag: bool | None = None
    partially_closes_gap_flag: bool | None = None
    recommendation_summary: str | None = None


class CompanyOverviewResponse(APIModel):
    """Compact overview payload for one company."""

    company_id: str
    company_name: str
    sector: str
    country: str
    target_year: int | None = None
    target_reduction_pct: float | None = None
    baseline_total_mb_tco2e: float | None = None
    projected_total_mb_tco2e: float | None = None
    target_gap_pct: float | None = None
    recommended_operational_risk_band: str | None = None
    calibrated_miss_probability: float | None = None
    credibility_band: str | None = None
    best_intervention_summary: BestInterventionSummary | None = None


class ForecastRow(APIModel):
    """One annual statistical forecast row."""

    forecast_year: int
    model_name: str
    forecast_total_mb_tco2e: float
    lower_bound_total_mb_tco2e: float
    upper_bound_total_mb_tco2e: float
    forecast_scope1_tco2e: float | None = None
    forecast_scope2_mb_tco2e: float | None = None
    forecast_notes: str | None = None


class CompanyForecastResponse(APIModel):
    """Forecast payload for one company."""

    company_id: str
    company_name: str
    sector: str
    country: str
    selected_model_name: str | None = None
    available_model_names: list[str]
    rows: list[ForecastRow]


class CompanyRiskResponse(APIModel):
    """Risk payload for one company."""

    company_id: str
    company_name: str
    heuristic_score: float
    heuristic_risk_band: str
    credibility_score: float
    credibility_band: str
    calibrated_miss_probability: float
    miss_probability_band: str
    recommended_operational_score: float
    recommended_operational_risk_band: str
    alignment_label: str
    reconciliation_status: str
    contradiction_count: int | None = None
    contradiction_summary: str | None = None
    disagreement_reason_primary: str | None = None
    disagreement_reason_secondary: str | None = None
    probabilistic_risk_note: str | None = None
    reconciliation_notes: str | None = None


class InterventionRankingRow(APIModel):
    """One ranked intervention row."""

    intervention_name: str
    intervention_category: str | None = None
    modeled_abatement_tco2e: float | None = None
    modeled_cost_usd_m: float | None = None
    cost_per_tco2e: float | None = None
    mac_rank: float | None = None
    abatement_rank: float | None = None
    feasibility_rank: float | None = None
    recommended_priority_flag: bool | None = None
    closes_gap_flag: bool | None = None
    partially_closes_gap_flag: bool | None = None


class CompanyInterventionsResponse(APIModel):
    """Intervention payload for one company."""

    company_id: str
    best_intervention_summary: BestInterventionSummary | None = None
    ranked_interventions: list[InterventionRankingRow]


class CompanyEvidenceResponse(APIModel):
    """Evidence-pack payload for one company."""

    company_id: str
    evidence_generated_flag: bool
    board_brief_path: str | None = None
    investor_memo_path: str | None = None
    lender_note_path: str | None = None


class FullIntelligenceResponse(APIModel):
    """Combined full-intelligence payload."""

    overview: CompanyOverviewResponse
    risk: CompanyRiskResponse
    forecast: CompanyForecastResponse
    interventions: CompanyInterventionsResponse
    evidence: CompanyEvidenceResponse


class ErrorResponse(APIModel):
    """Simple error payload."""

    detail: str

