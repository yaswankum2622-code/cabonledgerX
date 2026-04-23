"""Base parser scaffolding for workbook inspection."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from carbonledgerx.data.catalog import DatasetKey, get_catalog_path
from carbonledgerx.data.profiling import build_sheet_profile, build_workbook_profile
from carbonledgerx.parsers.excel_utils import (
    SheetLoadResult,
    list_excel_sheet_names,
    read_excel_sheet_safely,
    standardize_dataframe_columns,
)


class WorkbookParserProtocol(Protocol):
    """Protocol for workbook parsers that return JSON-serializable profiles."""

    dataset_name: str
    file_path: Path

    def list_sheet_names(self) -> list[str]:
        """Return workbook sheet names."""

    def load_sheet_as_dataframe(self, sheet_name: str) -> pd.DataFrame:
        """Load one worksheet as a pandas dataframe."""

    def profile_workbook(self) -> dict[str, Any]:
        """Return a structured workbook profile."""


@dataclass(slots=True)
class WorkbookParser:
    """Small reusable base class for Excel-backed dataset inspectors."""

    dataset_name: str
    catalog_key: DatasetKey

    @property
    def file_path(self) -> Path:
        """Return the dataset workbook path from the raw-data catalog."""

        return get_catalog_path(self.catalog_key)

    def list_sheet_names(self) -> list[str]:
        """Return workbook sheet names in file order."""

        return list_excel_sheet_names(self.file_path)

    def load_sheet_result(self, sheet_name: str) -> SheetLoadResult:
        """Load a worksheet with normalized headers and header-row metadata."""

        return read_excel_sheet_safely(
            self.file_path,
            sheet_name=sheet_name,
            normalize_headers=True,
        )

    def load_sheet_as_dataframe(self, sheet_name: str) -> pd.DataFrame:
        """Load a worksheet as a pandas dataframe with normalized columns."""

        return self.load_sheet_result(sheet_name).dataframe

    def standardize_column_names(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Return a copy of a dataframe with standardized column names."""

        return standardize_dataframe_columns(dataframe)

    def profile_sheet(self, sheet_name: str) -> dict[str, Any]:
        """Profile a single worksheet."""

        load_result = self.load_sheet_result(sheet_name)
        sheet_profile = build_sheet_profile(
            sheet_name=sheet_name,
            dataframe=load_result.dataframe,
            header_row_number=load_result.header_row_number,
        )
        return self.enrich_sheet_profile(sheet_profile, load_result)

    def profile_workbook(self) -> dict[str, Any]:
        """Profile every worksheet in the workbook."""

        sheet_profiles = [self.profile_sheet(sheet_name) for sheet_name in self.list_sheet_names()]
        workbook_profile = build_workbook_profile(
            dataset_name=self.dataset_name,
            file_path=self.file_path,
            sheet_profiles=sheet_profiles,
        )
        return self.enrich_workbook_profile(workbook_profile)

    def enrich_sheet_profile(
        self,
        sheet_profile: dict[str, Any],
        load_result: SheetLoadResult,
    ) -> dict[str, Any]:
        """Hook for dataset-specific per-sheet enrichment."""

        return sheet_profile

    def enrich_workbook_profile(self, workbook_profile: dict[str, Any]) -> dict[str, Any]:
        """Hook for dataset-specific workbook-level enrichment."""

        return workbook_profile
