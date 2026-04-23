"""Build deterministic markdown evidence packs and the evidence index."""

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
from carbonledgerx.models.evidence_pack import generate_company_evidence_packs
from carbonledgerx.utils.paths import output_path, processed_data_path


console = Console()


def main() -> int:
    """Generate the Phase 9 evidence markdown files and index table."""

    console.rule("[bold blue]CarbonLedgerX Evidence Packs[/bold blue]")
    build_result = generate_company_evidence_packs()

    index_path = write_processed_dataframe(
        build_result.index_artifact.dataframe,
        processed_data_path(build_result.index_artifact.output_name),
    )
    manifest_payload = {
        "selected_company_count": len(build_result.selected_company_ids),
        "selected_company_ids": build_result.selected_company_ids,
        "markdown_file_count": len(build_result.generated_files),
        "generated_files": [str(path) for path in build_result.generated_files],
        "index_output": build_result.index_artifact.manifest_entry(index_path),
    }
    manifest_path = write_processed_manifest(
        manifest_payload,
        output_path("evidence_manifest.json"),
    )

    console.print(build_summary_table(build_result))
    console.print(f"\nSelected company IDs: {', '.join(build_result.selected_company_ids)}")
    console.print(f"Evidence files generated: {len(build_result.generated_files)}")
    console.print(f"Manifest written to [bold]{manifest_path}[/bold]")
    return 0


def build_summary_table(build_result: object) -> Table:
    """Render a concise summary of evidence-pack outputs."""

    artifact = build_result.index_artifact
    table = Table(title="Evidence Output Summary")
    table.add_column("Output", style="cyan", no_wrap=True)
    table.add_column("Shape", justify="right")
    table.add_column("Selected Companies", justify="right")
    table.add_column("Markdown Files", justify="right")
    table.add_row(
        artifact.output_name,
        f"{artifact.n_rows} x {artifact.n_cols}",
        str(len(build_result.selected_company_ids)),
        str(len(build_result.generated_files)),
    )
    return table


if __name__ == "__main__":
    raise SystemExit(main())
