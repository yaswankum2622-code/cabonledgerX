"""Smoke tests for scoring reconciliation outputs."""

from __future__ import annotations

import json

import pandas as pd

from carbonledgerx.data.processed_writer import write_processed_dataframe, write_processed_manifest
from carbonledgerx.models.scoring_audit import (
    build_scoring_disagreement_segments,
    build_scoring_reconciliation_report_markdown,
    write_markdown_report,
    write_scoring_agreement_plot,
)
from carbonledgerx.models.scoring_reconciliation import build_company_scoring_reconciliation
from carbonledgerx.utils.paths import output_path, processed_data_path


def test_scoring_reconciliation_builds_successfully() -> None:
    """Scoring reconciliation outputs and audit artifacts should build successfully."""

    reconciliation_artifact = build_company_scoring_reconciliation()
    segments_artifact = build_scoring_disagreement_segments(
        reconciliation=reconciliation_artifact.dataframe,
    )
    artifacts = [reconciliation_artifact, segments_artifact]

    manifest_entries: list[dict[str, object]] = []
    for artifact in artifacts:
        parquet_path = write_processed_dataframe(
            artifact.dataframe,
            processed_data_path(artifact.output_name),
        )
        manifest_entries.append(artifact.manifest_entry(parquet_path))

    report_markdown = build_scoring_reconciliation_report_markdown(
        reconciliation=reconciliation_artifact.dataframe,
        segments=segments_artifact.dataframe,
    )
    report_path = write_markdown_report(
        report_markdown,
        output_path("evaluation", "scoring_reconciliation_report.md"),
    )
    plot_path = write_scoring_agreement_plot(
        reconciliation=reconciliation_artifact.dataframe,
        output_file_path=output_path("evaluation", "scoring_agreement_plot.png"),
    )

    manifest_entries.extend(
        [
            {"output_path": str(report_path), "file_type": "markdown_report"},
            {"output_path": str(plot_path), "file_type": "plot_png"},
        ]
    )
    manifest_path = write_processed_manifest(
        {"output_count": len(manifest_entries), "outputs": manifest_entries},
        output_path("scoring_reconciliation_manifest.json"),
    )

    reconciliation_path = processed_data_path("company_scoring_reconciliation.parquet")
    segments_path = processed_data_path("scoring_disagreement_segments.parquet")

    assert reconciliation_path.exists()
    assert segments_path.exists()
    assert report_path.exists()
    assert plot_path.exists()

    reconciliation_df = pd.read_parquet(reconciliation_path)
    segments_df = pd.read_parquet(segments_path)
    assert reconciliation_df.shape[0] > 0
    assert segments_df.shape[0] > 0
    assert reconciliation_df["recommended_operational_score"].between(0, 100).all()
    assert "reconciliation_status" in reconciliation_df.columns
    assert "count" in segments_df.columns

    assert manifest_path.exists()
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_count"] == 4
