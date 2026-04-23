"""Smoke tests for statistical forecast and evaluation outputs."""

from __future__ import annotations

import json

import pandas as pd

from carbonledgerx.data.processed_writer import write_processed_dataframe, write_processed_manifest
from carbonledgerx.models.backtesting import (
    NAIVE_MODEL_NAME,
    TREND_MODEL_NAME,
    build_company_forecast_backtest_results,
)
from carbonledgerx.models.forecast_evaluation import (
    build_backtest_report_markdown,
    build_calibration_summary_payload,
    build_company_forecast_summary,
    build_forecast_metrics_payload,
    write_forecast_metric_plots,
    write_json_payload,
    write_markdown_report,
)
from carbonledgerx.models.statistical_forecasting import build_company_emissions_forecast_statistical
from carbonledgerx.utils.paths import output_path, processed_data_path


def test_statistical_forecast_and_evaluation_build_successfully() -> None:
    """Statistical forecast, backtest, and evaluation artifacts should build successfully."""

    backtest_artifact = build_company_forecast_backtest_results()
    forecast_artifact = build_company_emissions_forecast_statistical(
        backtest_results=backtest_artifact.dataframe,
    )
    summary_artifact = build_company_forecast_summary(
        backtest_results=backtest_artifact.dataframe,
        statistical_forecast=forecast_artifact.dataframe,
    )
    artifacts = [forecast_artifact, backtest_artifact, summary_artifact]

    manifest_entries: list[dict[str, object]] = []
    for artifact in artifacts:
        parquet_path = write_processed_dataframe(
            artifact.dataframe,
            processed_data_path(artifact.output_name),
        )
        manifest_entries.append(artifact.manifest_entry(parquet_path))

    forecast_metrics = build_forecast_metrics_payload(
        backtest_results=backtest_artifact.dataframe,
        forecast_summary=summary_artifact.dataframe,
        statistical_forecast=forecast_artifact.dataframe,
    )
    calibration_summary = build_calibration_summary_payload(
        backtest_results=backtest_artifact.dataframe,
        forecast_summary=summary_artifact.dataframe,
    )
    backtest_report = build_backtest_report_markdown(
        backtest_results=backtest_artifact.dataframe,
        forecast_summary=summary_artifact.dataframe,
        forecast_metrics=forecast_metrics,
        calibration_summary=calibration_summary,
    )

    report_path = write_markdown_report(
        backtest_report,
        output_path("evaluation", "backtest_report.md"),
    )
    metrics_path = write_json_payload(
        forecast_metrics,
        output_path("evaluation", "forecast_metrics.json"),
    )
    calibration_path = write_json_payload(
        calibration_summary,
        output_path("evaluation", "calibration_summary.json"),
    )
    plots_path = write_forecast_metric_plots(
        backtest_results=backtest_artifact.dataframe,
        forecast_summary=summary_artifact.dataframe,
        output_file_path=output_path("evaluation", "forecast_metric_plots.png"),
    )

    manifest_entries.extend(
        [
            {"output_path": str(report_path), "file_type": "markdown_report"},
            {"output_path": str(metrics_path), "file_type": "metrics_json"},
            {"output_path": str(calibration_path), "file_type": "calibration_json"},
            {"output_path": str(plots_path), "file_type": "plot_png"},
        ]
    )
    manifest_path = write_processed_manifest(
        {"output_count": len(manifest_entries), "outputs": manifest_entries},
        output_path("statistical_forecast_manifest.json"),
    )

    forecast_path = processed_data_path("company_emissions_forecast_statistical.parquet")
    backtest_path = processed_data_path("company_forecast_backtest_results.parquet")
    summary_path = processed_data_path("company_forecast_summary.parquet")

    assert forecast_path.exists()
    assert backtest_path.exists()
    assert summary_path.exists()
    assert metrics_path.exists()
    assert calibration_path.exists()
    assert report_path.exists()
    assert plots_path.exists()

    forecast_df = pd.read_parquet(forecast_path)
    backtest_df = pd.read_parquet(backtest_path)
    summary_df = pd.read_parquet(summary_path)
    assert forecast_df.shape[0] > 0
    assert backtest_df.shape[0] > 0
    assert summary_df.shape[0] > 0

    model_names = set(backtest_df["model_name"].astype(str).unique().tolist())
    assert {NAIVE_MODEL_NAME, TREND_MODEL_NAME}.issubset(model_names)
    assert forecast_df["forecast_year"].nunique() > 1

    assert manifest_path.exists()
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_count"] == 7
