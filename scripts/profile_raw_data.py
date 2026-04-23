"""Profile all raw workbooks and write JSON summaries."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any

from rich.console import Console
from rich.table import Table


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from carbonledgerx.parsers.defra import build_all_defra_profiles
from carbonledgerx.parsers.egrid import build_all_egrid_profiles
from carbonledgerx.parsers.sbti import build_all_sbti_profiles
from carbonledgerx.utils.paths import output_path


console = Console()


def main() -> int:
    """Run workbook profiling and persist the resulting JSON outputs."""

    console.rule("[bold blue]CarbonLedgerX Raw Data Profiling[/bold blue]")
    profiles = collect_all_profiles()
    profiles_output_dir = output_path("profiles")
    profiles_output_dir.mkdir(parents=True, exist_ok=True)

    for profile in profiles:
        dataset_name = str(profile["dataset_name"])
        output_file = profiles_output_dir / f"{dataset_name}.json"
        write_json(output_file, profile)

    combined_summary = {
        "dataset_count": len(profiles),
        "datasets": [build_dataset_summary(profile) for profile in profiles],
    }
    write_json(profiles_output_dir / "all_datasets_summary.json", combined_summary)

    console.print(build_summary_table(profiles))
    console.print(f"\nProfiles written to [bold]{profiles_output_dir}[/bold]")
    return 0


def collect_all_profiles() -> list[dict[str, Any]]:
    """Collect profiles for every Phase 2 workbook dataset."""

    return [
        *build_all_egrid_profiles(),
        *build_all_defra_profiles(),
        *build_all_sbti_profiles(),
    ]


def build_dataset_summary(profile: dict[str, Any]) -> dict[str, Any]:
    """Build a concise workbook summary for the combined JSON file."""

    sheet_profiles = list(profile.get("sheet_profiles", []))
    summary: dict[str, Any] = {
        "dataset_name": profile["dataset_name"],
        "dataset_family": profile.get("dataset_family"),
        "file_path": profile["file_path"],
        "sheet_count": profile["sheet_count"],
        "largest_sheets": [
            {
                "sheet_name": sheet_profile["sheet_name"],
                "n_rows": sheet_profile["n_rows"],
                "n_cols": sheet_profile["n_cols"],
            }
            for sheet_profile in largest_sheet_profiles(sheet_profiles)
        ],
        "likely_machine_friendly_sheets": profile.get("likely_machine_friendly_sheets", []),
    }

    preferred_sheet = profile.get("preferred_machine_friendly_sheet")
    if preferred_sheet:
        summary["preferred_machine_friendly_sheet"] = preferred_sheet

    return summary


def build_summary_table(profiles: list[dict[str, Any]]) -> Table:
    """Render a readable Rich table summarizing workbook profiles."""

    table = Table(title="Workbook Profiling Summary")
    table.add_column("Dataset", style="cyan", no_wrap=True)
    table.add_column("Sheets", justify="right")
    table.add_column("Largest Sheets", style="white")
    table.add_column("Likely Machine-Friendly Sheets", style="green")

    for profile in profiles:
        largest_sheets = "\n".join(
            format_sheet_size(sheet_profile)
            for sheet_profile in largest_sheet_profiles(profile["sheet_profiles"])
        )
        machine_friendly_sheets = list(profile.get("likely_machine_friendly_sheets", []))
        preferred_sheet = profile.get("preferred_machine_friendly_sheet")
        if preferred_sheet and preferred_sheet not in machine_friendly_sheets:
            machine_friendly_sheets.insert(0, preferred_sheet)

        machine_friendly_text = "\n".join(machine_friendly_sheets[:5]) or "-"
        table.add_row(
            str(profile["dataset_name"]),
            str(profile["sheet_count"]),
            largest_sheets,
            machine_friendly_text,
        )

    return table


def largest_sheet_profiles(
    sheet_profiles: list[dict[str, Any]],
    *,
    top_n: int = 3,
) -> list[dict[str, Any]]:
    """Return the largest sheets by approximate cell count."""

    return sorted(
        sheet_profiles,
        key=lambda profile: (
            int(profile.get("n_rows", 0)) * int(profile.get("n_cols", 0)),
            int(profile.get("n_rows", 0)),
            int(profile.get("n_cols", 0)),
        ),
        reverse=True,
    )[:top_n]


def format_sheet_size(sheet_profile: dict[str, Any]) -> str:
    """Format one sheet summary line."""

    sheet_name = str(sheet_profile["sheet_name"])
    n_rows = int(sheet_profile["n_rows"])
    n_cols = int(sheet_profile["n_cols"])
    return f"{sheet_name} ({n_rows} x {n_cols})"


def write_json(file_path: Path, payload: dict[str, Any]) -> None:
    """Write JSON to disk with stable formatting."""

    file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
