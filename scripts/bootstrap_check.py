"""Validate the local CarbonLedgerX bootstrap environment."""

from __future__ import annotations

import platform
import sys
from pathlib import Path

from pydantic import ValidationError
from rich.console import Console
from rich.table import Table


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from carbonledgerx.config.settings import get_settings
from carbonledgerx.data.catalog import iter_catalog_paths


console = Console()


def build_paths_table() -> Table:
    """Create a display table for resolved project paths."""

    settings = get_settings()
    table = Table(title="Resolved Project Paths")
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Path", style="white")

    for name, path in settings.resolved_paths().items():
        table.add_row(name, str(path))

    return table


def check_catalog_files() -> list[tuple[str, Path]]:
    """Validate every expected raw file in the catalog."""

    missing_files: list[tuple[str, Path]] = []
    table = Table(title="Catalog File Check")
    table.add_column("Dataset", style="cyan")
    table.add_column("Resolved Path", style="white")
    table.add_column("Status", style="white")

    for dataset_name, path in iter_catalog_paths():
        exists = path.exists()
        status = "[green]OK[/green]" if exists else "[red]MISSING[/red]"
        table.add_row(dataset_name, str(path), status)

        if not exists:
            missing_files.append((dataset_name, path))

    console.print(table)
    return missing_files


def main() -> int:
    """Run the bootstrap environment validation."""

    console.rule("[bold blue]CarbonLedgerX Bootstrap Check[/bold blue]")
    console.print(f"Python version: [bold]{platform.python_version()}[/bold]")

    if sys.version_info < (3, 11):
        console.print("[bold red]Python 3.11 or newer is required.[/bold red]")
        return 1

    try:
        console.print(build_paths_table())
    except (ValidationError, ValueError) as exc:
        console.print("[bold red]Settings validation failed.[/bold red]")
        console.print(str(exc))
        return 1

    missing_files = check_catalog_files()
    if missing_files:
        console.print("\n[bold red]Missing catalog files:[/bold red]")
        for dataset_name, path in missing_files:
            console.print(f" - {dataset_name}: {path}")
        return 1

    console.print("\n[bold green]All bootstrap checks passed.[/bold green]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
