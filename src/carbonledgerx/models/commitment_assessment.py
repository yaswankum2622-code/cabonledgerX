"""Deterministic company-level commitment assessment from forecast outputs."""

from __future__ import annotations

from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.models.forecasting import FORECAST_HORIZON_YEAR
from carbonledgerx.utils.paths import processed_data_path


def build_company_commitment_assessment(
    *,
    forecast: pd.DataFrame | None = None,
    baseline: pd.DataFrame | None = None,
    company_panel: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build a deterministic company-level target assessment table."""

    if forecast is None:
        forecast = _read_processed_table("company_emissions_forecast.parquet")
    if baseline is None:
        baseline = _read_processed_table("company_emissions_baseline.parquet")
    if company_panel is None:
        company_panel = _read_processed_table("company_synthetic_panel.parquet")

    company_targets = _build_company_targets(company_panel)
    forecast_index = forecast.set_index(["company_id", "forecast_year"])

    assessment_rows = [
        _assess_company(
            company_record=company_record,
            baseline=baseline,
            forecast_index=forecast_index,
        )
        for company_record in company_targets.to_dict(orient="records")
    ]
    assessment_dataframe = pd.DataFrame(assessment_rows).convert_dtypes()

    selected_key_fields = [
        "company_id",
        "target_year",
        "assessment_year",
        "target_reduction_pct",
        "projected_total_mb_tco2e",
        "target_gap_pct",
        "target_met_flag",
    ]
    assumptions = [
        "Assessments compare baseline current_total_mb_tco2e against the deterministic market-based forecast at the company target year.",
        "If a target year extends beyond 2030, the assessment is capped at 2030 and the assumption is recorded in assessment_notes.",
        "Synthetic panel target_reduction_pct is the primary target input; no join to real-world SBTi commitments is attempted because the synthetic companies do not share identifiers with SBTi records.",
    ]
    return ProcessedTableArtifact(
        output_name="company_commitment_assessment.parquet",
        dataframe=assessment_dataframe.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=["company_emissions_forecast.parquet", "company_emissions_baseline.parquet", "company_synthetic_panel.parquet"],
    )


def _build_company_targets(company_panel: pd.DataFrame) -> pd.DataFrame:
    """Select the company-level target fields needed for assessment."""

    target_fields = [
        "company_id",
        "company_name",
        "target_year",
        "target_reduction_pct",
    ]
    targets = company_panel.loc[
        :,
        [column for column in target_fields if column in company_panel.columns],
    ].copy()

    if "target_year" in targets.columns:
        targets["target_year"] = pd.to_numeric(targets["target_year"], errors="coerce")
    targets["target_reduction_pct"] = targets.apply(_resolve_target_reduction_pct, axis=1)
    return targets


def _assess_company(
    *,
    company_record: dict[str, Any],
    baseline: pd.DataFrame,
    forecast_index: pd.DataFrame,
) -> dict[str, Any]:
    """Assess one company against its deterministic target trajectory."""

    company_id = str(company_record["company_id"])
    company_name = str(company_record["company_name"])
    baseline_row = baseline.loc[baseline["company_id"] == company_id]
    if baseline_row.empty:
        raise ValueError(f"Missing baseline row for company_id '{company_id}'.")

    baseline_total_mb = float(
        pd.to_numeric(baseline_row.iloc[0]["current_total_mb_tco2e"], errors="coerce")
    )
    raw_target_year = company_record.get("target_year")
    if pd.isna(raw_target_year):
        assessment_year = FORECAST_HORIZON_YEAR
        target_year = FORECAST_HORIZON_YEAR
        notes = [
            "Missing target_year in synthetic panel; defaulted assessment_year to the 2030 forecast horizon."
        ]
    else:
        target_year = int(raw_target_year)
        assessment_year = min(target_year, FORECAST_HORIZON_YEAR)
        notes = []
        if target_year > FORECAST_HORIZON_YEAR:
            notes.append(
                f"Target year {target_year} exceeds forecast horizon; assessment capped at {FORECAST_HORIZON_YEAR}."
            )

    forecast_row = forecast_index.loc[(company_id, assessment_year)]
    projected_total_mb = float(pd.to_numeric(forecast_row["forecast_total_mb_tco2e"], errors="coerce"))
    target_reduction_pct = float(company_record.get("target_reduction_pct", 0.0) or 0.0)

    if baseline_total_mb > 0:
        implied_reduction_pct = ((baseline_total_mb - projected_total_mb) / baseline_total_mb) * 100.0
    else:
        implied_reduction_pct = 0.0

    target_total_mb = baseline_total_mb * max(0.0, 1.0 - (target_reduction_pct / 100.0))
    target_gap_tco2e = projected_total_mb - target_total_mb
    target_gap_pct = target_reduction_pct - implied_reduction_pct
    target_met_flag = implied_reduction_pct >= target_reduction_pct

    if "defaulted assessment_year" not in " ".join(notes):
        notes.append("Target reduction sourced from synthetic company panel fields.")

    return {
        "company_id": company_id,
        "company_name": company_name,
        "target_year": target_year,
        "assessment_year": assessment_year,
        "target_reduction_pct": round(target_reduction_pct, 3),
        "baseline_total_mb_tco2e": round(baseline_total_mb, 3),
        "projected_total_mb_tco2e": round(projected_total_mb, 3),
        "implied_reduction_pct": round(implied_reduction_pct, 3),
        "target_gap_pct": round(target_gap_pct, 3),
        "target_gap_tco2e": round(target_gap_tco2e, 3),
        "target_met_flag": bool(target_met_flag),
        "assessment_notes": " ".join(notes),
    }


def _resolve_target_reduction_pct(company_row: pd.Series) -> float:
    """Resolve a target reduction percentage from simple candidate columns."""

    candidate_columns = [
        "target_reduction_pct",
        "target_value",
        "target_pct",
        "target_reduction",
    ]
    for column_name in candidate_columns:
        if column_name not in company_row.index:
            continue
        candidate_value = pd.to_numeric(company_row[column_name], errors="coerce")
        if pd.notna(candidate_value):
            return float(candidate_value)
    return 0.0


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
