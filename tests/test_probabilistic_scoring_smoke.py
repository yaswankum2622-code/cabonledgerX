"""Smoke tests for probabilistic commitment-miss scoring outputs."""

from __future__ import annotations

import json

import pandas as pd

from carbonledgerx.data.processed_writer import write_processed_dataframe, write_processed_manifest
from carbonledgerx.models.probabilistic_scoring import build_probabilistic_scoring_bundle
from carbonledgerx.models.probability_evaluation import (
    build_probability_metrics_payload,
    build_probability_model_report_markdown,
    write_json_payload,
    write_markdown_report,
    write_probability_calibration_plot,
)
from carbonledgerx.utils.paths import output_path, processed_data_path


def test_probabilistic_scoring_builds_successfully() -> None:
    """Probabilistic scoring outputs and evaluation artifacts should build successfully."""

    scoring_bundle = build_probabilistic_scoring_bundle()
    artifacts = [
        scoring_bundle.probability_scores_artifact,
        scoring_bundle.scoring_comparison_artifact,
        scoring_bundle.model_comparison_artifact,
    ]

    manifest_entries: list[dict[str, object]] = []
    for artifact in artifacts:
        parquet_path = write_processed_dataframe(
            artifact.dataframe,
            processed_data_path(artifact.output_name),
        )
        manifest_entries.append(artifact.manifest_entry(parquet_path))

    probability_metrics = build_probability_metrics_payload(
        modeling_dataset=scoring_bundle.modeling_dataset,
        model_comparison=scoring_bundle.model_comparison_artifact.dataframe,
        probability_scores=scoring_bundle.probability_scores_artifact.dataframe,
        scoring_comparison=scoring_bundle.scoring_comparison_artifact.dataframe,
        selected_model_name=scoring_bundle.selected_model_name,
    )
    probability_report = build_probability_model_report_markdown(
        model_comparison=scoring_bundle.model_comparison_artifact.dataframe,
        probability_metrics=probability_metrics,
    )

    metrics_path = write_json_payload(
        probability_metrics,
        output_path("evaluation", "probability_metrics.json"),
    )
    report_path = write_markdown_report(
        probability_report,
        output_path("evaluation", "probability_model_report.md"),
    )
    plot_path = write_probability_calibration_plot(
        holdout_predictions=scoring_bundle.holdout_predictions,
        model_comparison=scoring_bundle.model_comparison_artifact.dataframe,
        output_file_path=output_path("evaluation", "probability_calibration_plot.png"),
    )

    manifest_entries.extend(
        [
            {"output_path": str(metrics_path), "file_type": "metrics_json"},
            {"output_path": str(report_path), "file_type": "markdown_report"},
            {"output_path": str(plot_path), "file_type": "plot_png"},
        ]
    )
    manifest_path = write_processed_manifest(
        {"output_count": len(manifest_entries), "outputs": manifest_entries},
        output_path("probabilistic_scoring_manifest.json"),
    )

    probability_scores_path = processed_data_path("company_commitment_probability_scores.parquet")
    scoring_comparison_path = processed_data_path("company_scoring_comparison.parquet")
    model_comparison_path = processed_data_path("probabilistic_model_comparison.parquet")

    assert probability_scores_path.exists()
    assert scoring_comparison_path.exists()
    assert model_comparison_path.exists()
    assert metrics_path.exists()
    assert report_path.exists()
    assert plot_path.exists()

    probability_scores_df = pd.read_parquet(probability_scores_path)
    scoring_comparison_df = pd.read_parquet(scoring_comparison_path)
    model_comparison_df = pd.read_parquet(model_comparison_path)
    assert probability_scores_df.shape[0] > 0
    assert scoring_comparison_df.shape[0] > 0
    assert model_comparison_df.shape[0] > 0

    assert probability_scores_df["raw_miss_probability"].between(0, 1).all()
    assert probability_scores_df["calibrated_miss_probability"].between(0, 1).all()
    assert model_comparison_df["model_name"].nunique() >= 4
    assert int(model_comparison_df["selected_final_model_flag"].sum()) == 1

    assert manifest_path.exists()
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_count"] == 6
