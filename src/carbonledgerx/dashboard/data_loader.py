"""Cached parquet loading and company-level dashboard helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
import streamlit as st

from carbonledgerx.utils.paths import processed_data_path


TABLE_FILE_MAP = {
    "synthetic_panel": "company_synthetic_panel.parquet",
    "baseline": "company_emissions_baseline.parquet",
    "calculated": "company_emissions_calculated.parquet",
    "activity": "company_activity_inputs.parquet",
    "history": "company_emissions_history_annual.parquet",
    "forecast_deterministic": "company_emissions_forecast.parquet",
    "forecast_statistical": "company_emissions_forecast_statistical.parquet",
    "forecast_summary": "company_forecast_summary.parquet",
    "assessment": "company_commitment_assessment.parquet",
    "contradictions": "company_contradiction_flags.parquet",
    "risk_scores": "company_commitment_risk_scores.parquet",
    "probability_scores": "company_commitment_probability_scores.parquet",
    "commitment_intelligence": "company_commitment_intelligence.parquet",
    "scoring_reconciliation": "company_scoring_reconciliation.parquet",
    "intervention_scenarios": "company_intervention_scenarios.parquet",
    "mac_rankings": "company_mac_rankings.parquet",
    "intervention_intelligence": "company_intervention_intelligence.parquet",
    "evidence_index": "company_evidence_pack_index.parquet",
    "model_comparison": "probabilistic_model_comparison.parquet",
}

DATASET_FALLBACKS = {
    "forecast_deterministic": ("forecast",),
}

CONTRADICTION_FLAG_LABELS = {
    "optimistic_claim_but_miss_flag": "Optimistic claim but miss",
    "negative_reduction_flag": "Negative implied reduction",
    "large_target_gap_flag": "Large target gap",
    "near_term_target_underperforming_flag": "Near-term underperformance",
    "low_renewable_share_flag": "Low renewable share",
    "weak_mb_procurement_flag": "Weak market-based procurement",
    "capped_target_year_flag": "Assessment capped to 2030",
    "ambition_without_support_flag": "Ambition without support",
}


@dataclass(slots=True)
class CompanyDashboardBundle:
    """Company-level dashboard payload assembled from cached parquet tables."""

    company_id: str
    profile: pd.Series | None
    intelligence: pd.Series
    baseline: pd.Series | None
    calculated: pd.Series | None
    activity: pd.Series | None
    assessment: pd.Series | None
    risk: pd.Series | None
    probability: pd.Series | None
    reconciliation: pd.Series | None
    contradictions: pd.Series | None
    intervention: pd.Series | None
    evidence: pd.Series | None
    forecast_summary: pd.Series | None
    history: pd.DataFrame
    forecast_stat: pd.DataFrame
    forecast_det: pd.DataFrame
    interventions: pd.DataFrame
    model_comparison: pd.DataFrame
    portfolio_frame: pd.DataFrame


@st.cache_data(show_spinner=False)
def load_dashboard_tables() -> dict[str, pd.DataFrame]:
    """Load and cache all processed tables needed by the dashboard."""

    datasets = {
        table_name: pd.read_parquet(processed_data_path(file_name))
        for table_name, file_name in TABLE_FILE_MAP.items()
    }
    return datasets


def get_company_selector_frame(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Return the sidebar company selector frame."""

    reconciliation = _dataset(datasets, "scoring_reconciliation")
    selector = reconciliation.loc[
        :,
        [
            "company_id",
            "company_name",
            "sector",
            "country",
            "recommended_operational_risk_band",
            "recommended_operational_score",
            "calibrated_miss_probability",
        ],
    ].copy()
    selector["recommended_operational_score"] = pd.to_numeric(
        selector["recommended_operational_score"],
        errors="coerce",
    )
    selector["calibrated_miss_probability"] = pd.to_numeric(
        selector["calibrated_miss_probability"],
        errors="coerce",
    )
    selector = selector.sort_values(
        by=["recommended_operational_score", "calibrated_miss_probability", "company_name"],
        ascending=[False, False, True],
    ).reset_index(drop=True)
    selector["company_label"] = selector.apply(
        lambda row: (
            f"{row['company_name']} ({row['company_id']})"
            f" | {row['sector']} | {row['country']} | "
            f"{row['recommended_operational_risk_band']} risk"
        ),
        axis=1,
    )
    return selector


def get_company_bundle(
    datasets: dict[str, pd.DataFrame],
    company_id: str,
) -> CompanyDashboardBundle:
    """Return all dashboard-relevant slices for one company."""

    intelligence = _required_row(_dataset(datasets, "commitment_intelligence"), company_id)

    interventions = _company_rows(_dataset(datasets, "intervention_scenarios"), company_id).merge(
        _dataset(datasets, "mac_rankings").loc[
            :,
            [
                "company_id",
                "intervention_name",
                "mac_rank",
                "abatement_rank",
                "feasibility_rank",
                "feasibility_score",
                "priority_score",
                "priority_rank",
                "recommended_priority_flag",
                "closes_gap_flag",
                "partially_closes_gap_flag",
            ],
        ],
        on=["company_id", "intervention_name"],
        how="left",
        validate="one_to_one",
    )
    interventions = interventions.sort_values(
        by=["recommended_priority_flag", "priority_rank", "cost_per_tco2e"],
        ascending=[False, True, True],
    ).reset_index(drop=True)

    portfolio_frame = build_portfolio_frame(datasets)

    return CompanyDashboardBundle(
        company_id=company_id,
        profile=_optional_row(_dataset(datasets, "synthetic_panel"), company_id),
        intelligence=intelligence,
        baseline=_optional_row(_dataset(datasets, "baseline"), company_id),
        calculated=_optional_row(_dataset(datasets, "calculated"), company_id),
        activity=_optional_row(_dataset(datasets, "activity"), company_id),
        assessment=_optional_row(_dataset(datasets, "assessment"), company_id),
        risk=_optional_row(_dataset(datasets, "risk_scores"), company_id),
        probability=_optional_row(_dataset(datasets, "probability_scores"), company_id),
        reconciliation=_optional_row(_dataset(datasets, "scoring_reconciliation"), company_id),
        contradictions=_optional_row(_dataset(datasets, "contradictions"), company_id),
        intervention=_optional_row(_dataset(datasets, "intervention_intelligence"), company_id),
        evidence=_optional_row(_dataset(datasets, "evidence_index"), company_id),
        forecast_summary=_optional_row(_dataset(datasets, "forecast_summary"), company_id),
        history=_company_rows(_dataset(datasets, "history"), company_id).sort_values("history_year").reset_index(drop=True),
        forecast_stat=_company_rows(_dataset(datasets, "forecast_statistical"), company_id)
        .sort_values("forecast_year")
        .reset_index(drop=True),
        forecast_det=_company_rows(_dataset(datasets, "forecast_deterministic"), company_id)
        .sort_values("forecast_year")
        .reset_index(drop=True),
        interventions=interventions,
        model_comparison=_dataset(datasets, "model_comparison").copy(),
        portfolio_frame=portfolio_frame,
    )


@st.cache_data(show_spinner=False)
def build_portfolio_frame(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Return a merged portfolio frame for percentile and context calculations."""

    frame = _dataset(datasets, "scoring_reconciliation").merge(
        _dataset(datasets, "commitment_intelligence").loc[
            :,
            [
                "company_id",
                "target_gap_tco2e",
                "target_miss_risk_score",
                "commitment_credibility_score",
            ],
        ],
        on="company_id",
        how="left",
        suffixes=("", "_intelligence"),
    )
    return frame


def active_contradiction_flags(company_flags: pd.Series | None) -> list[dict[str, Any]]:
    """Return active contradiction flags in readable form."""

    if company_flags is None:
        return []

    active_flags: list[dict[str, Any]] = []
    for column_name, label in CONTRADICTION_FLAG_LABELS.items():
        if column_name not in company_flags.index:
            continue
        value = company_flags[column_name]
        if pd.isna(value) or not bool(value):
            continue
        active_flags.append({"flag": label, "column_name": column_name, "value": 1})
    return active_flags


def percentile_rank(values: pd.Series, selected_value: float) -> float:
    """Return the percentile rank of a selected value within a portfolio series."""

    numeric = pd.to_numeric(values, errors="coerce").dropna().sort_values()
    if numeric.empty:
        return 0.0
    percentile = (numeric <= selected_value).mean() * 100.0
    return float(percentile)


def selected_model_row(model_comparison: pd.DataFrame) -> pd.Series | None:
    """Return the selected probabilistic model comparison row."""

    rows = model_comparison.loc[model_comparison["selected_final_model_flag"] == True]  # noqa: E712
    if rows.empty:
        return None
    return rows.iloc[0]


def _company_rows(dataframe: pd.DataFrame, company_id: str) -> pd.DataFrame:
    """Return all rows for one company."""

    return dataframe.loc[dataframe["company_id"].astype(str) == str(company_id)].copy()


def _required_row(dataframe: pd.DataFrame, company_id: str) -> pd.Series:
    """Return exactly one required company row."""

    rows = _company_rows(dataframe, company_id)
    if rows.empty:
        raise ValueError(f"Missing required row for company_id '{company_id}'.")
    return rows.iloc[0]


def _optional_row(dataframe: pd.DataFrame, company_id: str) -> pd.Series | None:
    """Return one optional company row if present."""

    rows = _company_rows(dataframe, company_id)
    if rows.empty:
        return None
    return rows.iloc[0]


def _dataset(datasets: dict[str, pd.DataFrame], dataset_name: str) -> pd.DataFrame:
    """Return a dataset from the loaded map, with explicit fallbacks for stale cache shapes."""

    if dataset_name in datasets:
        return datasets[dataset_name]

    for fallback_name in DATASET_FALLBACKS.get(dataset_name, ()):
        if fallback_name in datasets:
            return datasets[fallback_name]

    if dataset_name in TABLE_FILE_MAP:
        return pd.read_parquet(processed_data_path(TABLE_FILE_MAP[dataset_name]))

    available = ", ".join(sorted(datasets.keys()))
    raise KeyError(
        f"Dashboard dataset '{dataset_name}' is unavailable. Available datasets: {available}"
    )
