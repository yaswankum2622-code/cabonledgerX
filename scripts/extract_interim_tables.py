"""Extract normalized interim parquet tables from raw workbook sources."""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any

from rich.console import Console
from rich.table import Table


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from carbonledgerx.data.interim_writer import write_dataframe_to_parquet, write_manifest
from carbonledgerx.parsers.extractors import extract_all_interim_tables, summarize_extracted_table
from carbonledgerx.utils.paths import interim_data_path, output_path


console = Console()


def main() -> int:
    """Run all interim extractions and persist parquet outputs plus a manifest."""

    console.rule("[bold blue]CarbonLedgerX Interim Extraction[/bold blue]")
    extracted_tables = extract_all_interim_tables()

    manifest_entries: list[dict[str, Any]] = []
    for extracted_table in extracted_tables:
        parquet_path = write_dataframe_to_parquet(
            extracted_table.dataframe,
            interim_data_path(extracted_table.output_name),
        )
        manifest_entries.append(extracted_table.manifest_entry(parquet_path))

    manifest_payload = {
        "output_count": len(manifest_entries),
        "outputs": manifest_entries,
    }
    manifest_path = write_manifest(
        manifest_payload,
        output_path("interim_manifest.json"),
    )

    console.print(build_summary_table(extracted_tables))
    print_notes(extracted_tables)
    console.print(f"\nManifest written to [bold]{manifest_path}[/bold]")
    return 0


def build_summary_table(extracted_tables: list[Any]) -> Table:
    """Render a readable Rich table for extracted interim outputs."""

    table = Table(title="Interim Extraction Summary")
    table.add_column("Output", style="cyan", no_wrap=True)
    table.add_column("Source", style="white")
    table.add_column("Shape", justify="right")
    table.add_column("Key Column Candidates", style="green")

    for extracted_table in extracted_tables:
        summary = summarize_extracted_table(extracted_table)
        shape_text = f"{summary['n_rows']} x {summary['n_cols']}"
        source_text = f"{extracted_table.source_dataset}:{extracted_table.source_sheet}"
        candidates_text = ", ".join(summary["key_column_candidates"][:8]) or "-"
        table.add_row(
            extracted_table.output_name,
            source_text,
            shape_text,
            candidates_text,
        )

    return table


def print_notes(extracted_tables: list[Any]) -> None:
    """Print any extraction notes or assumptions beneath the summary."""

    notes = [
        f"{extracted_table.output_name}: {note}"
        for extracted_table in extracted_tables
        for note in extracted_table.notes
    ]

    if not notes:
        return

    console.print("\n[bold yellow]Notes[/bold yellow]")
    for note in notes:
        console.print(f" - {note}")


if __name__ == "__main__":
    raise SystemExit(main())
