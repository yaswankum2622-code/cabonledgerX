"""Smoke tests for historical reconstruction outputs."""

from __future__ import annotations

import json

import pandas as pd

from carbonledgerx.data.processed_writer import write_processed_dataframe, write_processed_manifest
from carbonledgerx.models.history_reconstructor import (
    HISTORY_END_YEAR,
    HISTORY_START_YEAR,
    build_company_emissions_history_annual,
)
from carbonledgerx.models.history_summary import build_company_history_summary
from carbonledgerx.utils.paths import output_path, processed_data_path


REQUIRED_TOTAL_COLUMNS = [
    "scope1_tco2e",
    "scope2_lb_tco2e",
    "scope2_mb_tco2e",
    "total_lb_tco2e",
    "total_mb_tco2e",
]


def test_historical_reconstruction_builds_successfully() -> None:
    """History annual and summary outputs should build and write successfully."""

    history_artifact = build_company_emissions_history_annual()
    summary_artifact = build_company_history_summary(
        history_annual=history_artifact.dataframe,
    )
    artifacts = [history_artifact, summary_artifact]

    manifest_entries: list[dict[str, object]] = []
    for artifact in artifacts:
        parquet_path = write_processed_dataframe(
            artifact.dataframe,
            processed_data_path(artifact.output_name),
        )
        manifest_entries.append(artifact.manifest_entry(parquet_path))

    manifest_path = write_processed_manifest(
        {
            "history_year_start": HISTORY_START_YEAR,
            "history_year_end": HISTORY_END_YEAR,
            "output_count": len(manifest_entries),
            "outputs": manifest_entries,
        },
        output_path("history_manifest.json"),
    )

    history_path = processed_data_path("company_emissions_history_annual.parquet")
    summary_path = processed_data_path("company_history_summary.parquet")
    assert history_path.exists()
    assert summary_path.exists()

    history_df = pd.read_parquet(history_path)
    summary_df = pd.read_parquet(summary_path)
    assert history_df.shape[0] > 0
    assert summary_df.shape[0] > 0

    history_years = set(pd.to_numeric(history_df["history_year"], errors="coerce").dropna().astype(int).unique())
    assert set(range(HISTORY_START_YEAR, HISTORY_END_YEAR + 1)).issubset(history_years)

    for column_name in REQUIRED_TOTAL_COLUMNS:
        assert column_name in history_df.columns
        assert history_df[column_name].ge(0).all()

    assert summary_df["company_id"].nunique() == history_df["company_id"].nunique()
    assert len(summary_df) == history_df["company_id"].nunique()

    assert manifest_path.exists()
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_count"] == 2
