"""Smoke tests for the Streamlit dashboard data layer and app import."""

from __future__ import annotations


def test_dashboard_loader_and_app_import() -> None:
    """Core dashboard tables should load and the app module should import."""

    from carbonledgerx.dashboard.app import main
    from carbonledgerx.dashboard.data_loader import (
        get_company_bundle,
        get_company_selector_frame,
        load_dashboard_tables,
    )

    datasets = load_dashboard_tables()
    expected_tables = {
        "synthetic_panel",
        "baseline",
        "calculated",
        "activity",
        "history",
        "forecast_deterministic",
        "forecast_statistical",
        "forecast_summary",
        "assessment",
        "contradictions",
        "risk_scores",
        "probability_scores",
        "commitment_intelligence",
        "scoring_reconciliation",
        "intervention_scenarios",
        "mac_rankings",
        "intervention_intelligence",
        "model_comparison",
        "evidence_index",
    }

    assert expected_tables.issubset(datasets.keys())
    for dataframe in datasets.values():
        assert dataframe.shape[0] > 0

    selector = get_company_selector_frame(datasets)
    assert selector.shape[0] > 0

    company_id = str(selector.iloc[0]["company_id"])
    bundle = get_company_bundle(datasets, company_id)
    assert bundle.intelligence["company_id"] == company_id
    assert bundle.history.shape[0] > 0
    assert bundle.forecast_stat.shape[0] > 0
    assert bundle.forecast_det.shape[0] > 0
    assert bundle.interventions.shape[0] > 0
    assert callable(main)
