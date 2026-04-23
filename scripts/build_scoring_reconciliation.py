"""Build scoring reconciliation outputs and compact audit artifacts."""

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
from carbonledgerx.models.scoring_audit import (
    build_scoring_disagreement_segments,
    build_scoring_reconciliation_report_markdown,
    write_markdown_report,
    write_scoring_agreement_plot,
)
from carbonledgerx.models.scoring_reconciliation import build_company_scoring_reconciliation
from carbonledgerx.utils.paths import output_path, processed_data_path


console = Console()


def main() -> int:
    """Run the scoring reconciliation and audit build."""

    console.rule("[bold blue]CarbonLedgerX Scoring Reconciliation[/bold blue]")
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
    manifest_payload = {
        "output_count": len(manifest_entries),
        "alignment_counts": {
            label: int(count)
            for label, count in reconciliation_artifact.dataframe["scoring_alignment_label"].value_counts().to_dict().items()
        },
        "reconciliation_status_counts": {
            label: int(count)
            for label, count in reconciliation_artifact.dataframe["reconciliation_status"].value_counts().to_dict().items()
        },
        "outputs": manifest_entries,
    }
    manifest_path = write_processed_manifest(
        manifest_payload,
        output_path("scoring_reconciliation_manifest.json"),
    )

    console.print(build_summary_table(artifacts))
    print_counts(reconciliation_artifact.dataframe)
    console.print(f"\nManifest written to [bold]{manifest_path}[/bold]")
    return 0


def build_summary_table(artifacts: list[object]) -> Table:
    """Render a readable summary table for scoring reconciliation outputs."""

    table = Table(title="Scoring Reconciliation Output Summary")
    table.add_column("Output", style="cyan", no_wrap=True)
    table.add_column("Shape", justify="right")
    table.add_column("Key Fields", style="green")

    for artifact in artifacts:
        shape_text = f"{artifact.n_rows} x {artifact.n_cols}"
        key_fields = ", ".join(artifact.selected_key_fields[:8]) or "-"
        table.add_row(artifact.output_name, shape_text, key_fields)

    return table


def print_counts(reconciliation: object) -> None:
    """Print alignment and status summaries."""

    console.print("\n[bold yellow]Reconciliation Counts[/bold yellow]")
    alignment_counts = reconciliation["scoring_alignment_label"].value_counts().to_dict()
    status_counts = reconciliation["reconciliation_status"].value_counts().to_dict()
    console.print(f" - Alignment labels: {alignment_counts}")
    console.print(f" - Reconciliation statuses: {status_counts}")


if __name__ == "__main__":
    raise SystemExit(main())
