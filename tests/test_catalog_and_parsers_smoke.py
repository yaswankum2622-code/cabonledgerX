"""Smoke tests for raw-data catalog entries and workbook parsers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from carbonledgerx.data.catalog import get_catalog_path
from carbonledgerx.parsers.defra import build_defra_profile
from carbonledgerx.parsers.egrid import build_egrid_profile
from carbonledgerx.parsers.sbti import build_sbti_profile


PROFILE_BUILDERS: list[tuple[str, Callable[[str], dict[str, Any]]]] = [
    ("egrid_2022_data", build_egrid_profile),
    ("egrid_2022_metric", build_egrid_profile),
    ("egrid_2023_data", build_egrid_profile),
    ("defra_2025_full", build_defra_profile),
    ("defra_2025_flat", build_defra_profile),
    ("sbti_companies", build_sbti_profile),
    ("sbti_targets", build_sbti_profile),
]


@pytest.mark.parametrize(("dataset_name", "builder"), PROFILE_BUILDERS)
def test_dataset_profiles_generate_without_crashing(
    dataset_name: str,
    builder: Callable[[str], dict[str, Any]],
) -> None:
    """Every workbook profile should build successfully and include sheet metadata."""

    file_path = get_catalog_path(dataset_name)
    assert file_path.exists()

    profile = builder(dataset_name)

    assert profile["dataset_name"] == dataset_name
    assert profile["file_path"] == str(file_path)
    assert isinstance(profile["sheet_profiles"], list)
    assert len(profile["sheet_profiles"]) >= 1

    for sheet_profile in profile["sheet_profiles"]:
        assert int(sheet_profile["n_cols"]) >= 1
        assert "normalized_columns_sample" in sheet_profile
        assert isinstance(sheet_profile["null_fraction_top10"], dict)
