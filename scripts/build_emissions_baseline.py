"""Build factor mappings and the baseline emissions table."""

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
from carbonledgerx.models.emissions_baseline import build_company_emissions_baseline
from carbonledgerx.models.factor_mapping import build_company_factor_mapping
from carbonledgerx.utils.paths import output_path, processed_data_path


console = Console()


def main() -> int:
    """Run the factor-mapping build and baseline build, then persist artifacts."""

    console.rule("[bold blue]CarbonLedgerX Emissions Baseline[/bold blue]")
    factor_mapping_artifact = build_company_factor_mapping()
    baseline_artifact = build_company_emissions_baseline(
        factor_mapping=factor_mapping_artifact.dataframe,
    )
    artifacts = [factor_mapping_artifact, baseline_artifact]

    manifest_entries: list[dict[str, object]] = []
    for artifact in artifacts:
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
        output_path("baseline_manifest.json"),
    )

    console.print(build_summary_table(artifacts))
    print_assumptions(artifacts)
    console.print(f"\nManifest written to [bold]{manifest_path}[/bold]")
    return 0


def build_summary_table(artifacts: list[object]) -> Table:
    """Render a readable summary table for baseline outputs."""

    table = Table(title="Baseline Output Summary")
    table.add_column("Output", style="cyan", no_wrap=True)
    table.add_column("Shape", justify="right")
    table.add_column("Key Fields", style="green")

    for artifact in artifacts:
        shape_text = f"{artifact.n_rows} x {artifact.n_cols}"
        key_fields = ", ".join(artifact.selected_key_fields[:8]) or "-"
        table.add_row(artifact.output_name, shape_text, key_fields)

    return table


def print_assumptions(artifacts: list[object]) -> None:
    """Print assumptions beneath the baseline summary table."""

    console.print("\n[bold yellow]Mapping Assumptions[/bold yellow]")
    for artifact in artifacts:
        for assumption in artifact.assumptions:
            console.print(f" - {artifact.output_name}: {assumption}")


if __name__ == "__main__":
    raise SystemExit(main())
