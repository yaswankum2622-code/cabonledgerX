"""FastAPI application exposing read-only TargetTruth analytical outputs."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from . import data_access
from .schemas import (
    CompaniesResponse,
    CompanyEvidenceResponse,
    CompanyForecastResponse,
    CompanyInterventionsResponse,
    CompanyOverviewResponse,
    CompanyRiskResponse,
    FullIntelligenceResponse,
    HealthResponse,
)


app = FastAPI(
    title="TargetTruth API",
    description=(
        "Thin read-only API exposing the main TargetTruth analytical outputs from the "
        "project's parquet-backed processing pipeline."
    ),
)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Return a compact service health payload."""

    return HealthResponse(
        status="ok",
        project_name="TargetTruth API",
        available_datasets=data_access.get_available_datasets(),
    )


@app.get("/companies", response_model=CompaniesResponse)
def companies(
    risk_band: str | None = Query(default=None, description="Optional operational risk band filter."),
    limit: int = Query(default=100, ge=1, le=500, description="Maximum number of rows to return."),
) -> CompaniesResponse:
    """Return a compact list of companies."""

    company_rows = data_access.list_companies(risk_band=risk_band, limit=limit)
    return CompaniesResponse(companies=company_rows, total_returned=len(company_rows))


@app.get("/company/{company_id}/overview", response_model=CompanyOverviewResponse)
def company_overview(company_id: str) -> CompanyOverviewResponse:
    """Return the compact overview payload for one company."""

    try:
        return CompanyOverviewResponse(**data_access.get_company_overview(company_id))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown company_id '{company_id}'.") from None


@app.get("/company/{company_id}/forecast", response_model=CompanyForecastResponse)
def company_forecast(company_id: str) -> CompanyForecastResponse:
    """Return the statistical forecast payload for one company."""

    try:
        return CompanyForecastResponse(**data_access.get_company_forecast(company_id))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown company_id '{company_id}'.") from None


@app.get("/company/{company_id}/risk", response_model=CompanyRiskResponse)
def company_risk(company_id: str) -> CompanyRiskResponse:
    """Return the risk payload for one company."""

    try:
        return CompanyRiskResponse(**data_access.get_company_risk(company_id))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown company_id '{company_id}'.") from None


@app.get("/company/{company_id}/interventions", response_model=CompanyInterventionsResponse)
def company_interventions(company_id: str) -> CompanyInterventionsResponse:
    """Return the intervention payload for one company."""

    try:
        return CompanyInterventionsResponse(**data_access.get_company_interventions(company_id))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown company_id '{company_id}'.") from None


@app.get("/company/{company_id}/evidence", response_model=CompanyEvidenceResponse)
def company_evidence(company_id: str) -> CompanyEvidenceResponse:
    """Return evidence-pack availability for one company."""

    try:
        return CompanyEvidenceResponse(**data_access.get_company_evidence(company_id))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown company_id '{company_id}'.") from None


@app.get("/company/{company_id}/full-intelligence", response_model=FullIntelligenceResponse)
def company_full_intelligence(company_id: str) -> FullIntelligenceResponse:
    """Return the combined full-intelligence payload for one company."""

    try:
        return FullIntelligenceResponse(**data_access.get_company_full_intelligence(company_id))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown company_id '{company_id}'.") from None
