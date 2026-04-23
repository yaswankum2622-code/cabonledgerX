"""Inspection helpers for DEFRA 2025 workbooks."""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Final, Literal

from carbonledgerx.parsers.base import WorkbookParser


DefraDatasetName = Literal[
    "defra_2025_full",
    "defra_2025_flat",
]

DEFRA_DATASETS: Final[tuple[DefraDatasetName, ...]] = (
    "defra_2025_full",
    "defra_2025_flat",
)


class DefraParser(WorkbookParser):
    """Workbook inspector for DEFRA conversion factor releases."""

    def enrich_workbook_profile(self, workbook_profile: dict[str, Any]) -> dict[str, Any]:
        """Attach the family label and identify the most machine-friendly sheet."""

        workbook_profile["dataset_family"] = "defra"
        preferred_sheet = _select_preferred_machine_friendly_sheet(workbook_profile)
        if preferred_sheet is not None:
            workbook_profile["preferred_machine_friendly_sheet"] = preferred_sheet
        return workbook_profile


def get_defra_parser(dataset_name: DefraDatasetName) -> DefraParser:
    """Build a DEFRA parser for one catalog entry."""

    return DefraParser(dataset_name=dataset_name, catalog_key=dataset_name)


@lru_cache(maxsize=len(DEFRA_DATASETS))
def build_defra_profile(dataset_name: DefraDatasetName) -> dict[str, Any]:
    """Return a structured workbook profile for one DEFRA file."""

    return get_defra_parser(dataset_name).profile_workbook()


def build_all_defra_profiles() -> list[dict[str, Any]]:
    """Return profiles for all DEFRA workbooks."""

    return [build_defra_profile(dataset_name) for dataset_name in DEFRA_DATASETS]


def _select_preferred_machine_friendly_sheet(workbook_profile: dict[str, Any]) -> str | None:
    """Select the strongest machine-friendly candidate within a DEFRA workbook."""

    best_sheet_name: str | None = None
    best_score = float("-inf")

    for sheet_profile in workbook_profile.get("sheet_profiles", []):
        sheet_name = str(sheet_profile.get("sheet_name", ""))
        sheet_score = float(sheet_profile.get("machine_friendly_score", 0.0))
        lowered_name = sheet_name.lower()

        if "factor" in lowered_name:
            sheet_score += 2.0
        if "category" in lowered_name:
            sheet_score += 1.0

        if sheet_score > best_score:
            best_score = sheet_score
            best_sheet_name = sheet_name

    return best_sheet_name
