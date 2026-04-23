"""Build statistical forecasts, backtests, and evaluation artifacts."""

from __future__ import annotations

from pathlib import Path
import sys

from rich.console import Console
from rich.table import Table


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from carbonledgerx.data.processed_writer import write_processed_dataframe, write_processed_manifest
from carbonledgerx.models.backtesting import build_company_forecast_backtest_results
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


console = Console()


def main() -> int:
    """Run the statistical forecasting and evaluation build."""

    console.rule("[bold blue]CarbonLedgerX Statistical Forecast And Evaluation[/bold blue]")
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
    manifest_payload = {
        "output_count": len(manifest_entries),
        "outputs": manifest_entries,
        "aggregate_metrics": forecast_metrics,
        "calibration_summary": calibration_summary,
    }
    manifest_path = write_processed_manifest(
        manifest_payload,
        output_path("statistical_forecast_manifest.json"),
    )

    console.print(build_summary_table(artifacts))
    print_metrics(forecast_metrics, calibration_summary)
    console.print(f"\nManifest written to [bold]{manifest_path}[/bold]")
    return 0


def build_summary_table(artifacts: list[object]) -> Table:
    """Render a readable summary table for statistical forecast outputs."""

    table = Table(title="Statistical Forecast Output Summary")
    table.add_column("Output", style="cyan", no_wrap=True)
    table.add_column("Shape", justify="right")
    table.add_column("Key Fields", style="green")

    for artifact in artifacts:
        shape_text = f"{artifact.n_rows} x {artifact.n_cols}"
        key_fields = ", ".join(artifact.selected_key_fields[:8]) or "-"
        table.add_row(artifact.output_name, shape_text, key_fields)

    return table


def print_metrics(forecast_metrics: dict[str, object], calibration_summary: dict[str, object]) -> None:
    """Print concise aggregate forecast metrics."""

    console.print("\n[bold yellow]Aggregate Metrics[/bold yellow]")
    console.print(
        f" - Selected-model mean APE: {forecast_metrics['selected_model_backtest_mean_ape_pct']:.2f}%"
    )
    console.print(
        f" - Selected-model mean absolute error: {forecast_metrics['selected_model_backtest_mean_abs_error']:.2f} tCO2e"
    )
    console.print(
        f" - Selected-model interval coverage: {forecast_metrics['selected_model_backtest_interval_coverage_pct']:.2f}%"
    )
    console.print(
        f" - Calibration label: {calibration_summary['calibration_label']}"
    )
    console.print(
        f" - Model wins: {forecast_metrics['selected_model_win_counts']}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
