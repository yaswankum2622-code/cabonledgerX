"""Smoke tests for factor mapping and baseline outputs."""

from __future__ import annotations

import json

import pandas as pd

from carbonledgerx.data.processed_writer import write_processed_dataframe, write_processed_manifest
from carbonledgerx.models.emissions_baseline import build_company_emissions_baseline
from carbonledgerx.models.factor_mapping import build_company_factor_mapping
from carbonledgerx.utils.paths import output_path, processed_data_path


REQUIRED_BASELINE_COLUMNS = [
    "current_scope1_tco2e",
    "current_scope2_lb_tco2e",
    "current_scope2_mb_tco2e",
    "current_total_lb_tco2e",
    "current_total_mb_tco2e",
]


def test_emissions_baseline_outputs_build_successfully() -> None:
    """Factor mapping and baseline outputs should build and write successfully."""

    factor_mapping_artifact = build_company_factor_mapping()
    baseline_artifact = build_company_emissions_baseline(
        factor_mapping=factor_mapping_artifact.dataframe,
    )
    artifacts = [factor_mapping_artifact, baseline_artifact]

    manifest_entries: list[dict[str, object]] = []
    for artifact in artifacts:
        parquet_path = write_processed_dataframe(
            artifact.dataframe,
            processed_data_path(artifact.output_name),
        )
        manifest_entries.append(artifact.manifest_entry(parquet_path))

    manifest_path = write_processed_manifest(
        {"output_count": len(manifest_entries), "outputs": manifest_entries},
        output_path("baseline_manifest.json"),
    )

    mapping_path = processed_data_path("company_factor_mapping.parquet")
    baseline_path = processed_data_path("company_emissions_baseline.parquet")
    assert mapping_path.exists()
    assert baseline_path.exists()

    mapping_df = pd.read_parquet(mapping_path)
    baseline_df = pd.read_parquet(baseline_path)
    assert mapping_df.shape[0] > 0
    assert baseline_df.shape[0] > 0

    for column_name in REQUIRED_BASELINE_COLUMNS:
        assert column_name in baseline_df.columns

    assert manifest_path.exists()
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_count"] == 2
