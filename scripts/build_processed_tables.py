"""Build canonical processed tables and the synthetic company panel."""

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
from carbonledgerx.models.canonical_tables import build_all_processed_tables
from carbonledgerx.models.synthetic_company_panel import build_synthetic_company_panel
from carbonledgerx.utils.paths import output_path, processed_data_path


console = Console()


def main() -> int:
    """Run all processed-table builders and persist outputs plus a manifest."""

    console.rule("[bold blue]CarbonLedgerX Processed Tables[/bold blue]")
    processed_artifacts = [*build_all_processed_tables(), build_synthetic_company_panel()]

    manifest_entries: list[dict[str, object]] = []
    for artifact in processed_artifacts:
        parquet_path = write_processed_dataframe(
            artifact.dataframe,
            processed_data_path(artifact.output_name),
        )
        manifest_entries.append(artifact.manifest_entry(parquet_path))

    manifest_payload = {
        "output_count": len(manifest_entries),
        "outputs": manifest_entries,
    }
    manifest_path = write_processed_manifest(
        manifest_payload,
        output_path("processed_manifest.json"),
    )

    console.print(build_summary_table(processed_artifacts))
    print_assumptions(processed_artifacts)
    console.print(f"\nManifest written to [bold]{manifest_path}[/bold]")
    return 0


def build_summary_table(processed_artifacts: list[object]) -> Table:
    """Render a readable Rich summary of processed outputs."""

    table = Table(title="Processed Table Summary")
    table.add_column("Output", style="cyan", no_wrap=True)
    table.add_column("Shape", justify="right")
    table.add_column("Selected Key Fields", style="green")

    for artifact in processed_artifacts:
        shape_text = f"{artifact.n_rows} x {artifact.n_cols}"
        selected_fields = ", ".join(artifact.selected_key_fields[:8]) or "-"
        table.add_row(artifact.output_name, shape_text, selected_fields)

    return table


def print_assumptions(processed_artifacts: list[object]) -> None:
    """Print processing assumptions beneath the summary table."""

    console.print("\n[bold yellow]Assumptions[/bold yellow]")
    for artifact in processed_artifacts:
        for assumption in artifact.assumptions:
            console.print(f" - {artifact.output_name}: {assumption}")


if __name__ == "__main__":
    raise SystemExit(main())
