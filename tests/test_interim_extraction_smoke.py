"""Smoke tests for interim parquet extraction."""

from __future__ import annotations

import json

import pandas as pd

from carbonledgerx.data.interim_writer import write_dataframe_to_parquet, write_manifest
from carbonledgerx.parsers.extractors import INTERIM_OUTPUTS, extract_all_interim_tables
from carbonledgerx.utils.paths import interim_data_path, output_path


def test_interim_extraction_creates_expected_outputs() -> None:
    """All expected interim parquet files and the manifest should be created."""

    extracted_tables = extract_all_interim_tables()
    manifest_entries: list[dict[str, object]] = []

    for extracted_table in extracted_tables:
        parquet_path = write_dataframe_to_parquet(
            extracted_table.dataframe,
            interim_data_path(extracted_table.output_name),
        )
        manifest_entries.append(extracted_table.manifest_entry(parquet_path))

    manifest_path = write_manifest(
        {"output_count": len(manifest_entries), "outputs": manifest_entries},
        output_path("interim_manifest.json"),
    )

    for output_name in INTERIM_OUTPUTS:
        parquet_path = interim_data_path(output_name)
        assert parquet_path.exists()

        dataframe = pd.read_parquet(parquet_path)
        assert dataframe.shape[0] > 0

    assert manifest_path.exists()
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_count"] == len(INTERIM_OUTPUTS)
    assert len(manifest_payload["outputs"]) == len(INTERIM_OUTPUTS)
