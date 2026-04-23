"""Thin parquet-backed data access layer for the TargetTruth API."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

DATASET_FILE_MAP = {
    "company_commitment_intelligence": "company_commitment_intelligence.parquet",
    "company_commitment_probability_scores": "company_commitment_probability_scores.parquet",
    "company_scoring_reconciliation": "company_scoring_reconciliation.parquet",
    "company_emissions_forecast_statistical": "company_emissions_forecast_statistical.parquet",
    "company_intervention_intelligence": "company_intervention_intelligence.parquet",
    "company_mac_rankings": "company_mac_rankings.parquet",
    "company_evidence_pack_index": "company_evidence_pack_index.parquet",
}


def get_available_datasets() -> list[str]:
    """Return the API-exposed dataset names."""

    return list(DATASET_FILE_MAP.keys())


@lru_cache(maxsize=None)
def load_dataset(dataset_name: str) -> pd.DataFrame:
    """Load one processed parquet dataset and cache it in memory."""

    if dataset_name not in DATASET_FILE_MAP:
        raise KeyError(f"Unknown dataset '{dataset_name}'.")

    dataset_path = PROCESSED_DIR / DATASET_FILE_MAP[dataset_name]
    return pd.read_parquet(dataset_path)


def list_companies(*, risk_band: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    """Return a compact company list for the API."""

    reconciliation = load_dataset("company_scoring_reconciliation")
    companies = reconciliation.loc[
        :,
        [
            "company_id",
            "company_name",
            "sector",
            "country",
            "recommended_operational_risk_band",
            "calibrated_miss_probability",
            "recommended_operational_score",
        ],
    ].copy()
    if risk_band:
        companies = companies.loc[
            companies["recommended_operational_risk_band"].astype(str).str.lower() == risk_band.lower()
        ].copy()

    companies = companies.sort_values(
        ["recommended_operational_score", "company_id"],
        ascending=[False, True],
        kind="stable",
    ).head(limit)
    return [_nativeize_record(record) for record in companies.to_dict(orient="records")]


def get_company_overview(company_id: str) -> dict[str, Any]:
    """Return the compact overview payload for one company."""

    intelligence_row = _require_company_row("company_commitment_intelligence", company_id)
    reconciliation_row = _require_company_row("company_scoring_reconciliation", company_id)
    probability_row = _require_company_row("company_commitment_probability_scores", company_id)
    intervention_row = _safe_company_row("company_intervention_intelligence", company_id)

    best_intervention_summary: dict[str, Any] | None = None
    if intervention_row:
        best_intervention_summary = {
            "intervention_name": intervention_row.get("best_intervention_name"),
            "cost_per_tco2e": intervention_row.get("best_intervention_cost_per_tco2e"),
            "cost_usd_m": intervention_row.get("best_intervention_cost_usd_m"),
            "abatement_tco2e": intervention_row.get("best_intervention_abatement_tco2e"),
            "closes_gap_flag": intervention_row.get("best_intervention_closes_gap_flag"),
            "partially_closes_gap_flag": intervention_row.get("best_intervention_partially_closes_gap_flag"),
            "recommendation_summary": intervention_row.get("intervention_recommendation_summary"),
        }

    return _nativeize_record(
        {
            "company_id": intelligence_row["company_id"],
            "company_name": intelligence_row["company_name"],
            "sector": intelligence_row["sector"],
            "country": intelligence_row["country"],
            "target_year": intelligence_row["target_year"],
            "target_reduction_pct": intelligence_row["target_reduction_pct"],
            "baseline_total_mb_tco2e": intelligence_row["baseline_total_mb_tco2e"],
            "projected_total_mb_tco2e": intelligence_row["projected_total_mb_tco2e"],
            "target_gap_pct": intelligence_row["target_gap_pct"],
            "recommended_operational_risk_band": reconciliation_row["recommended_operational_risk_band"],
            "calibrated_miss_probability": probability_row["calibrated_miss_probability"],
            "credibility_band": intelligence_row["credibility_band"],
            "best_intervention_summary": best_intervention_summary,
        }
    )


def get_company_forecast(company_id: str) -> dict[str, Any]:
    """Return company metadata plus yearly statistical forecast rows."""

    _ensure_company_exists(company_id)
    forecast_rows = load_dataset("company_emissions_forecast_statistical")
    company_forecast = forecast_rows.loc[forecast_rows["company_id"] == company_id].copy()
    if company_forecast.empty:
        raise KeyError(company_id)

    company_forecast = company_forecast.sort_values("forecast_year", kind="stable")
    first_row = _nativeize_record(company_forecast.iloc[0].to_dict())
    model_names = sorted(company_forecast["model_name"].astype(str).dropna().unique().tolist())

    return {
        "company_id": first_row["company_id"],
        "company_name": first_row["company_name"],
        "sector": first_row["sector"],
        "country": first_row["country"],
        "selected_model_name": model_names[0] if model_names else None,
        "available_model_names": model_names,
        "rows": [_nativeize_record(record) for record in company_forecast.to_dict(orient="records")],
    }


def get_company_risk(company_id: str) -> dict[str, Any]:
    """Return the risk payload for one company."""

    reconciliation_row = _require_company_row("company_scoring_reconciliation", company_id)
    intelligence_row = _require_company_row("company_commitment_intelligence", company_id)
    probability_row = _require_company_row("company_commitment_probability_scores", company_id)

    return _nativeize_record(
        {
            "company_id": reconciliation_row["company_id"],
            "company_name": reconciliation_row["company_name"],
            "heuristic_score": reconciliation_row["target_miss_risk_score"],
            "heuristic_risk_band": reconciliation_row["risk_band"],
            "credibility_score": reconciliation_row["commitment_credibility_score"],
            "credibility_band": reconciliation_row["credibility_band"],
            "calibrated_miss_probability": reconciliation_row["calibrated_miss_probability"],
            "miss_probability_band": reconciliation_row["miss_probability_band"],
            "recommended_operational_score": reconciliation_row["recommended_operational_score"],
            "recommended_operational_risk_band": reconciliation_row["recommended_operational_risk_band"],
            "alignment_label": reconciliation_row["scoring_alignment_label"],
            "reconciliation_status": reconciliation_row["reconciliation_status"],
            "contradiction_count": intelligence_row["contradiction_count"],
            "contradiction_summary": intelligence_row["contradiction_summary"],
            "disagreement_reason_primary": reconciliation_row["disagreement_reason_primary"],
            "disagreement_reason_secondary": reconciliation_row["disagreement_reason_secondary"],
            "probabilistic_risk_note": probability_row["probabilistic_risk_note"],
            "reconciliation_notes": reconciliation_row["reconciliation_notes"],
        }
    )


def get_company_interventions(company_id: str) -> dict[str, Any]:
    """Return the intervention intelligence payload for one company."""

    _ensure_company_exists(company_id)
    intervention_row = _safe_company_row("company_intervention_intelligence", company_id)
    mac_rankings = load_dataset("company_mac_rankings")
    company_rankings = mac_rankings.loc[mac_rankings["company_id"] == company_id].copy()
    company_rankings = company_rankings.sort_values(
        ["priority_rank", "mac_rank", "intervention_name"],
        kind="stable",
    )

    best_summary: dict[str, Any] | None = None
    if intervention_row:
        best_summary = {
            "intervention_name": intervention_row.get("best_intervention_name"),
            "cost_per_tco2e": intervention_row.get("best_intervention_cost_per_tco2e"),
            "cost_usd_m": intervention_row.get("best_intervention_cost_usd_m"),
            "abatement_tco2e": intervention_row.get("best_intervention_abatement_tco2e"),
            "closes_gap_flag": intervention_row.get("best_intervention_closes_gap_flag"),
            "partially_closes_gap_flag": intervention_row.get("best_intervention_partially_closes_gap_flag"),
            "recommendation_summary": intervention_row.get("intervention_recommendation_summary"),
            "target_gap_tco2e": intervention_row.get("target_gap_tco2e"),
        }

    return {
        "company_id": company_id,
        "best_intervention_summary": _nativeize_record(best_summary) if best_summary is not None else None,
        "ranked_interventions": [_nativeize_record(record) for record in company_rankings.to_dict(orient="records")],
    }


def get_company_evidence(company_id: str) -> dict[str, Any]:
    """Return evidence-pack availability for one company."""

    _ensure_company_exists(company_id)
    evidence_row = _safe_company_row("company_evidence_pack_index", company_id)
    if evidence_row is None:
        return {
            "company_id": company_id,
            "evidence_generated_flag": False,
            "board_brief_path": None,
            "investor_memo_path": None,
            "lender_note_path": None,
        }

    return _nativeize_record(
        {
            "company_id": evidence_row["company_id"],
            "evidence_generated_flag": evidence_row.get("evidence_generated_flag", False),
            "board_brief_path": evidence_row.get("board_brief_path"),
            "investor_memo_path": evidence_row.get("investor_memo_path"),
            "lender_note_path": evidence_row.get("lender_note_path"),
        }
    )


def get_company_full_intelligence(company_id: str) -> dict[str, Any]:
    """Return the combined full-intelligence payload for one company."""

    return {
        "overview": get_company_overview(company_id),
        "risk": get_company_risk(company_id),
        "forecast": get_company_forecast(company_id),
        "interventions": get_company_interventions(company_id),
        "evidence": get_company_evidence(company_id),
    }


def _ensure_company_exists(company_id: str) -> None:
    """Raise KeyError when a company id is not present in the reconciliation table."""

    reconciliation = load_dataset("company_scoring_reconciliation")
    if company_id not in reconciliation["company_id"].astype(str).values:
        raise KeyError(company_id)


def _require_company_row(dataset_name: str, company_id: str) -> dict[str, Any]:
    """Return one company row from a dataset or raise KeyError."""

    row = _safe_company_row(dataset_name, company_id)
    if row is None:
        raise KeyError(company_id)
    return row


def _safe_company_row(dataset_name: str, company_id: str) -> dict[str, Any] | None:
    """Return one company row from a dataset or None when missing."""

    dataset = load_dataset(dataset_name)
    company_rows = dataset.loc[dataset["company_id"].astype(str) == company_id].copy()
    if company_rows.empty:
        return None
    return _nativeize_record(company_rows.iloc[0].to_dict())


def _nativeize_record(record: dict[str, Any] | None) -> dict[str, Any] | None:
    """Convert a dict of pandas/numpy values into plain Python values."""

    if record is None:
        return None
    return {key: _nativeize_value(value) for key, value in record.items()}


def _nativeize_value(value: Any) -> Any:
    """Convert pandas/numpy scalars and nulls into JSON-safe Python values."""

    if isinstance(value, dict):
        return {key: _nativeize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_nativeize_value(item) for item in value]
    if isinstance(value, tuple):
        return [_nativeize_value(item) for item in value]
    if value is None:
        return None
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value
