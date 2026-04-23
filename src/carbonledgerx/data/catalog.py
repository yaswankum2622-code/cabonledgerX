"""Logical file catalog for raw data assets."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Final, Literal

from carbonledgerx.config.settings import get_settings


DatasetKey = Literal[
    "egrid_2022_data",
    "egrid_2022_metric",
    "egrid_2023_data",
    "defra_2025_full",
    "defra_2025_flat",
    "sbti_companies",
    "sbti_targets",
    "defra_methodology_pdf",
]


DATASET_CATALOG: Final[dict[DatasetKey, str]] = {
    "egrid_2022_data": "egrid2022_data.xlsx",
    "egrid_2022_metric": "egrid2022_data_metric.xlsx",
    "egrid_2023_data": "egrid2023_data_rev2.xlsx",
    "defra_2025_full": "ghg-conversion-factors-2025-full-set.xlsx",
    "defra_2025_flat": "ghg-conversion-factors-2025-flat-format.xlsx",
    "sbti_companies": "companies-excel.xlsx",
    "sbti_targets": "targets-excel.xlsx",
    "defra_methodology_pdf": "2025-GHG-CF-methodology-paper.pdf",
}


def get_catalog_path(dataset_name: DatasetKey) -> Path:
    """Resolve the absolute raw-data path for a catalog entry."""

    settings = get_settings()
    return (settings.raw_data_dir / DATASET_CATALOG[dataset_name]).resolve()


def iter_catalog_paths() -> Iterator[tuple[DatasetKey, Path]]:
    """Yield catalog entries with their resolved paths."""

    for dataset_name in DATASET_CATALOG:
        yield dataset_name, get_catalog_path(dataset_name)
