"""Smoke tests for deterministic forecast and commitment assessment outputs."""

from __future__ import annotations

import json

import pandas as pd

from carbonledgerx.data.processed_writer import write_processed_dataframe, write_processed_manifest
from carbonledgerx.models.commitment_assessment import build_company_commitment_assessment
from carbonledgerx.models.forecasting import build_company_emissions_forecast
from carbonledgerx.utils.paths import output_path, processed_data_path


REQUIRED_ASSESSMENT_COLUMNS = [
    "target_met_flag",
    "target_gap_pct",
    "projected_total_mb_tco2e",
]


def test_forecast_and_assessment_outputs_build_successfully() -> None:
    """Forecast and commitment assessment outputs should build and write successfully."""

    forecast_artifact = build_company_emissions_forecast()
    assessment_artifact = build_company_commitment_assessment(
        forecast=forecast_artifact.dataframe,
    )
    artifacts = [forecast_artifact, assessment_artifact]

    manifest_entries: list[dict[str, object]] = []
    for artifact in artifacts:
        parquet_path = write_processed_dataframe(
            artifact.dataframe,
            processed_data_path(artifact.output_name),
        )
        manifest_entries.append(artifact.manifest_entry(parquet_path))

    manifest_path = write_processed_manifest(
        {"output_count": len(manifest_entries), "outputs": manifest_entries},
        output_path("forecast_manifest.json"),
    )

    forecast_path = processed_data_path("company_emissions_forecast.parquet")
    assessment_path = processed_data_path("company_commitment_assessment.parquet")
    assert forecast_path.exists()
    assert assessment_path.exists()

    forecast_df = pd.read_parquet(forecast_path)
    assessment_df = pd.read_parquet(assessment_path)
    assert forecast_df.shape[0] > 0
    assert assessment_df.shape[0] > 0

    forecast_year_counts = forecast_df.groupby("company_id")["forecast_year"].nunique()
    assert int(forecast_year_counts.min()) >= 2

    for column_name in REQUIRED_ASSESSMENT_COLUMNS:
        assert column_name in assessment_df.columns

    assert manifest_path.exists()
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_count"] == 2
