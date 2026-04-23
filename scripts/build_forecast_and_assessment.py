"""Build deterministic emissions forecasts and company commitment assessments."""

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
from carbonledgerx.models.commitment_assessment import build_company_commitment_assessment
from carbonledgerx.models.forecasting import build_company_emissions_forecast
from carbonledgerx.utils.paths import output_path, processed_data_path


console = Console()


def main() -> int:
    """Run the forecast build and company-level assessment build."""

    console.rule("[bold blue]CarbonLedgerX Forecast And Assessment[/bold blue]")
    forecast_artifact = build_company_emissions_forecast()
    assessment_artifact = build_company_commitment_assessment(
        forecast=forecast_artifact.dataframe,
    )
    artifacts = [forecast_artifact, assessment_artifact]

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
        output_path("forecast_manifest.json"),
    )

    console.print(build_summary_table(artifacts))
    print_assumptions(artifacts)
    console.print(f"\nManifest written to [bold]{manifest_path}[/bold]")
    return 0


def build_summary_table(artifacts: list[object]) -> Table:
    """Render a readable summary table for forecast outputs."""

    table = Table(title="Forecast Output Summary")
    table.add_column("Output", style="cyan", no_wrap=True)
    table.add_column("Shape", justify="right")
    table.add_column("Key Fields", style="green")

    for artifact in artifacts:
        shape_text = f"{artifact.n_rows} x {artifact.n_cols}"
        key_fields = ", ".join(artifact.selected_key_fields[:8]) or "-"
        table.add_row(artifact.output_name, shape_text, key_fields)

    return table


def print_assumptions(artifacts: list[object]) -> None:
    """Print forecast and assessment assumptions."""

    console.print("\n[bold yellow]Forecast Assumptions[/bold yellow]")
    for artifact in artifacts:
        for assumption in artifact.assumptions:
            console.print(f" - {artifact.output_name}: {assumption}")


if __name__ == "__main__":
    raise SystemExit(main())
