"""Smoke tests for activity generation and calculated emissions outputs."""

from __future__ import annotations

import json

import pandas as pd

from carbonledgerx.data.processed_writer import write_processed_dataframe, write_processed_manifest
from carbonledgerx.models.activity_generator import build_company_activity_inputs
from carbonledgerx.models.emissions_calculator import build_company_emissions_calculated
from carbonledgerx.utils.paths import output_path, processed_data_path


REQUIRED_SCOPE_COLUMNS = [
    "calculated_scope1_tco2e",
    "calculated_scope2_lb_tco2e",
    "calculated_scope2_mb_tco2e",
    "calculated_total_lb_tco2e",
    "calculated_total_mb_tco2e",
]


def test_activity_and_calculated_emissions_build_successfully() -> None:
    """Activity inputs and calculated emissions outputs should build and write successfully."""

    activity_artifact = build_company_activity_inputs()
    emissions_artifact = build_company_emissions_calculated(
        activity_inputs=activity_artifact.dataframe,
    )
    artifacts = [activity_artifact, emissions_artifact]

    manifest_entries: list[dict[str, object]] = []
    for artifact in artifacts:
        parquet_path = write_processed_dataframe(
            artifact.dataframe,
            processed_data_path(artifact.output_name),
        )
        manifest_entries.append(artifact.manifest_entry(parquet_path))

    manifest_path = write_processed_manifest(
        {"output_count": len(manifest_entries), "outputs": manifest_entries},
        output_path("activity_calculation_manifest.json"),
    )

    activity_path = processed_data_path("company_activity_inputs.parquet")
    emissions_path = processed_data_path("company_emissions_calculated.parquet")
    assert activity_path.exists()
    assert emissions_path.exists()

    activity_df = pd.read_parquet(activity_path)
    emissions_df = pd.read_parquet(emissions_path)
    assert activity_df.shape[0] > 0
    assert emissions_df.shape[0] > 0

    for column_name in REQUIRED_SCOPE_COLUMNS:
        assert column_name in emissions_df.columns
        assert emissions_df[column_name].ge(0).all()

    assert manifest_path.exists()
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_count"] == 2
