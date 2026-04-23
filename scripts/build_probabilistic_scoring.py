"""Build probabilistic commitment-miss scoring outputs and evaluation artifacts."""

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
from carbonledgerx.models.probabilistic_scoring import build_probabilistic_scoring_bundle
from carbonledgerx.models.probability_evaluation import (
    build_probability_metrics_payload,
    build_probability_model_report_markdown,
    write_json_payload,
    write_markdown_report,
    write_probability_calibration_plot,
)
from carbonledgerx.utils.paths import output_path, processed_data_path


console = Console()


def main() -> int:
    """Run the probabilistic scoring and evaluation build."""

    console.rule("[bold blue]CarbonLedgerX Probabilistic Scoring[/bold blue]")
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
    manifest_payload = {
        "output_count": len(manifest_entries),
        "selected_model_name": scoring_bundle.selected_model_name,
        "best_tree_model_name": scoring_bundle.best_tree_model_name,
        "outputs": manifest_entries,
        "probability_metrics": probability_metrics,
    }
    manifest_path = write_processed_manifest(
        manifest_payload,
        output_path("probabilistic_scoring_manifest.json"),
    )

    console.print(build_summary_table(artifacts))
    print_metrics(probability_metrics)
    console.print(f"\nManifest written to [bold]{manifest_path}[/bold]")
    return 0


def build_summary_table(artifacts: list[object]) -> Table:
    """Render a readable summary table for probabilistic scoring outputs."""

    table = Table(title="Probabilistic Scoring Output Summary")
    table.add_column("Output", style="cyan", no_wrap=True)
    table.add_column("Shape", justify="right")
    table.add_column("Key Fields", style="green")

    for artifact in artifacts:
        shape_text = f"{artifact.n_rows} x {artifact.n_cols}"
        key_fields = ", ".join(artifact.selected_key_fields[:8]) or "-"
        table.add_row(artifact.output_name, shape_text, key_fields)

    return table


def print_metrics(probability_metrics: dict[str, object]) -> None:
    """Print concise aggregate metrics for the selected probabilistic model."""

    selected_metrics = probability_metrics["selected_model_metrics"]
    console.print("\n[bold yellow]Probability Metrics[/bold yellow]")
    console.print(f" - Selected model: {probability_metrics['selected_model_name']}")
    console.print(f" - ROC-AUC: {selected_metrics['roc_auc']:.4f}")
    console.print(f" - Brier score: {selected_metrics['brier_score']:.4f}")
    console.print(f" - Log loss: {selected_metrics['log_loss']:.4f}")
    console.print(f" - Calibration quality: {selected_metrics['calibration_quality_label']}")


if __name__ == "__main__":
    raise SystemExit(main())
