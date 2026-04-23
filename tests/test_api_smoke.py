"""Smoke tests for the parquet-backed FastAPI layer."""

from __future__ import annotations

import pandas as pd
from fastapi.testclient import TestClient

from carbonledgerx.api.main import app
from carbonledgerx.api.data_access import load_dataset


client = TestClient(app)


def test_api_health_and_company_endpoints() -> None:
    """The read-only API should load and serve the main demo endpoints."""

    health_response = client.get("/health")
    assert health_response.status_code == 200
    health_payload = health_response.json()
    assert health_payload["status"] == "ok"
    assert "company_scoring_reconciliation" in health_payload["available_datasets"]

    companies_response = client.get("/companies", params={"limit": 5})
    assert companies_response.status_code == 200
    companies_payload = companies_response.json()
    assert companies_payload["total_returned"] > 0
    assert len(companies_payload["companies"]) > 0

    sample_company_id = companies_payload["companies"][0]["company_id"]
    overview_response = client.get(f"/company/{sample_company_id}/overview")
    assert overview_response.status_code == 200
    overview_payload = overview_response.json()
    assert overview_payload["company_id"] == sample_company_id

    risk_response = client.get(f"/company/{sample_company_id}/risk")
    assert risk_response.status_code == 200
    assert "recommended_operational_risk_band" in risk_response.json()

    forecast_response = client.get(f"/company/{sample_company_id}/forecast")
    assert forecast_response.status_code == 200
    assert len(forecast_response.json()["rows"]) > 0

    interventions_response = client.get(f"/company/{sample_company_id}/interventions")
    assert interventions_response.status_code == 200
    assert "ranked_interventions" in interventions_response.json()

    evidence_response = client.get(f"/company/{sample_company_id}/evidence")
    assert evidence_response.status_code == 200
    assert "evidence_generated_flag" in evidence_response.json()

    full_response = client.get(f"/company/{sample_company_id}/full-intelligence")
    assert full_response.status_code == 200
    full_payload = full_response.json()
    assert full_payload["overview"]["company_id"] == sample_company_id
    assert full_payload["risk"]["company_id"] == sample_company_id


def test_api_unknown_company_returns_404() -> None:
    """Unknown company identifiers should return 404 on company endpoints."""

    response = client.get("/company/UNKNOWN9999/overview")
    assert response.status_code == 404


def test_api_data_access_loads_core_tables() -> None:
    """The parquet-backed data loader should return non-empty dataframes."""

    reconciliation = load_dataset("company_scoring_reconciliation")
    forecast = load_dataset("company_emissions_forecast_statistical")
    assert isinstance(reconciliation, pd.DataFrame)
    assert isinstance(forecast, pd.DataFrame)
    assert reconciliation.shape[0] > 0
    assert forecast.shape[0] > 0
