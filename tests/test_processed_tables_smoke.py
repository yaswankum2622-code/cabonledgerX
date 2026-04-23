"""Smoke tests for processed table builds."""

from __future__ import annotations

import json

import pandas as pd

from carbonledgerx.data.processed_writer import write_processed_dataframe, write_processed_manifest
from carbonledgerx.models.canonical_tables import build_all_processed_tables
from carbonledgerx.models.synthetic_company_panel import build_synthetic_company_panel
from carbonledgerx.utils.paths import output_path, processed_data_path


EXPECTED_OUTPUTS = [
    "egrid_state_factors.parquet",
    "egrid_ba_factors.parquet",
    "defra_emission_factors.parquet",
    "sbti_company_commitments.parquet",
    "company_synthetic_panel.parquet",
]


def test_processed_table_build_creates_outputs() -> None:
    """Processed parquet outputs and the manifest should be created successfully."""

    artifacts = [*build_all_processed_tables(), build_synthetic_company_panel()]
    manifest_entries: list[dict[str, object]] = []

    for artifact in artifacts:
        parquet_path = write_processed_dataframe(
            artifact.dataframe,
            processed_data_path(artifact.output_name),
        )
        manifest_entries.append(artifact.manifest_entry(parquet_path))

    manifest_path = write_processed_manifest(
        {"output_count": len(manifest_entries), "outputs": manifest_entries},
        output_path("processed_manifest.json"),
    )

    for output_name in EXPECTED_OUTPUTS:
        parquet_path = processed_data_path(output_name)
        assert parquet_path.exists()

        dataframe = pd.read_parquet(parquet_path)
        assert dataframe.shape[0] > 0

    assert manifest_path.exists()
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_count"] == len(EXPECTED_OUTPUTS)
    assert len(manifest_payload["outputs"]) == len(EXPECTED_OUTPUTS)
