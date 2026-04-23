"""Inspection helpers for SBTi workbooks."""

from __future__ import annotations

from functools import lru_cache
import re
from typing import Any, Final, Literal

from carbonledgerx.parsers.base import WorkbookParser
from carbonledgerx.parsers.excel_utils import SheetLoadResult


SbtiDatasetName = Literal[
    "sbti_companies",
    "sbti_targets",
]

SBTI_DATASETS: Final[tuple[SbtiDatasetName, ...]] = (
    "sbti_companies",
    "sbti_targets",
)

SBTI_CANDIDATE_PATTERNS: Final[dict[str, tuple[str, ...]]] = {
    "company_name": (
        r"(^|_)company_name($|_)",
        r"(^|_)organisation_name($|_)",
        r"(^|_)organization_name($|_)",
    ),
    "target_year": (
        r"target_year",
        r"near_term_target_year",
        r"long_term_target_year",
        r"net_zero_year",
        r"base_year",
    ),
    "status": (
        r"(^|_)status($|_)",
        r"near_term_status",
        r"long_term_status",
        r"net_zero_status",
    ),
    "scope": (
        r"(^|_)scope($|_)",
        r"scope_",
    ),
    "reduction_percent": (
        r"reduction",
        r"target_value",
        r"percent",
        r"percentage",
    ),
}


class SbtiParser(WorkbookParser):
    """Workbook inspector for SBTi exports."""

    def enrich_sheet_profile(
        self,
        sheet_profile: dict[str, Any],
        load_result: SheetLoadResult,
    ) -> dict[str, Any]:
        """Attach heuristic candidate-key columns for SBTi sheets."""

        columns = [str(column_name) for column_name in load_result.dataframe.columns]
        sheet_profile["candidate_key_columns"] = guess_candidate_key_columns(columns)
        return sheet_profile

    def enrich_workbook_profile(self, workbook_profile: dict[str, Any]) -> dict[str, Any]:
        """Attach the dataset family label."""

        workbook_profile["dataset_family"] = "sbti"
        return workbook_profile


def get_sbti_parser(dataset_name: SbtiDatasetName) -> SbtiParser:
    """Build an SBTi parser for one catalog entry."""

    return SbtiParser(dataset_name=dataset_name, catalog_key=dataset_name)


@lru_cache(maxsize=len(SBTI_DATASETS))
def build_sbti_profile(dataset_name: SbtiDatasetName) -> dict[str, Any]:
    """Return a structured workbook profile for one SBTi file."""

    return get_sbti_parser(dataset_name).profile_workbook()


def build_all_sbti_profiles() -> list[dict[str, Any]]:
    """Return profiles for all SBTi workbooks."""

    return [build_sbti_profile(dataset_name) for dataset_name in SBTI_DATASETS]


def guess_candidate_key_columns(column_names: list[str]) -> dict[str, list[str]]:
    """Heuristically match likely key columns from normalized SBTi headers."""

    matches: dict[str, list[str]] = {}

    for semantic_name, patterns in SBTI_CANDIDATE_PATTERNS.items():
        matched_columns = [
            column_name
            for column_name in column_names
            if any(re.search(pattern, column_name) for pattern in patterns)
        ]
        matches[semantic_name] = matched_columns[:5]

    return matches
