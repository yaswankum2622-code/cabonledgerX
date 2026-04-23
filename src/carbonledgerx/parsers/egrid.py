"""Inspection helpers for eGRID workbooks."""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Final, Literal

from carbonledgerx.parsers.base import WorkbookParser


EGridDatasetName = Literal[
    "egrid_2022_data",
    "egrid_2022_metric",
    "egrid_2023_data",
]

EGRID_DATASETS: Final[tuple[EGridDatasetName, ...]] = (
    "egrid_2022_data",
    "egrid_2022_metric",
    "egrid_2023_data",
)


class EGridParser(WorkbookParser):
    """Workbook inspector for eGRID releases."""

    def enrich_workbook_profile(self, workbook_profile: dict[str, Any]) -> dict[str, Any]:
        """Attach the dataset family label."""

        workbook_profile["dataset_family"] = "egrid"
        return workbook_profile


def get_egrid_parser(dataset_name: EGridDatasetName) -> EGridParser:
    """Build an eGRID parser for one catalog entry."""

    return EGridParser(dataset_name=dataset_name, catalog_key=dataset_name)


@lru_cache(maxsize=len(EGRID_DATASETS))
def build_egrid_profile(dataset_name: EGridDatasetName) -> dict[str, Any]:
    """Return a structured workbook profile for one eGRID file."""

    return get_egrid_parser(dataset_name).profile_workbook()


def build_all_egrid_profiles() -> list[dict[str, Any]]:
    """Return profiles for all eGRID workbooks."""

    return [build_egrid_profile(dataset_name) for dataset_name in EGRID_DATASETS]
