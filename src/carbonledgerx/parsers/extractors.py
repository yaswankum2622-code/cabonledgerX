"""Interim extraction helpers for machine-usable workbook tables."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any, Final

import pandas as pd

from carbonledgerx.data.profiling import to_jsonable
from carbonledgerx.parsers.base import WorkbookParser


KEY_COLUMN_PATTERN = re.compile(
    r"(plant|state|ba|authority|subregion|company|target|scope|year|factor|unit|status)"
)


@dataclass(slots=True)
class ExtractedInterimTable:
    """Container for one extracted interim dataframe and its provenance."""

    output_name: str
    source_dataset: str
    source_sheet: str
    dataframe: pd.DataFrame
    key_column_candidates: list[str]
    notes: list[str] = field(default_factory=list)

    @property
    def n_rows(self) -> int:
        """Return the extracted row count."""

        return int(self.dataframe.shape[0])

    @property
    def n_cols(self) -> int:
        """Return the extracted column count."""

        return int(self.dataframe.shape[1])

    def summary(self) -> dict[str, Any]:
        """Return a compact extraction summary."""

        return {
            "output_name": self.output_name,
            "n_rows": self.n_rows,
            "n_cols": self.n_cols,
            "key_column_candidates": self.key_column_candidates,
        }

    def manifest_entry(self, output_file_path: str | Path) -> dict[str, Any]:
        """Return a manifest entry for persisted output."""

        return to_jsonable(
            {
                "output_file_path": Path(output_file_path).resolve(),
                "source_dataset": self.source_dataset,
                "source_sheet": self.source_sheet,
                "row_count": self.n_rows,
                "column_count": self.n_cols,
                "key_column_candidates": self.key_column_candidates,
                "notes": self.notes,
            }
        )


INTERIM_OUTPUTS: Final[tuple[str, ...]] = (
    "egrid_plant_2022.parquet",
    "egrid_plant_2023.parquet",
    "egrid_state_2022.parquet",
    "egrid_state_2023.parquet",
    "egrid_ba_2022.parquet",
    "egrid_ba_2023.parquet",
    "defra_factors_2025.parquet",
    "sbti_companies.parquet",
    "sbti_targets.parquet",
)


def extract_egrid_plant_2022() -> ExtractedInterimTable:
    """Extract the 2022 eGRID plant table."""

    return _extract_from_sheet(
        output_name="egrid_plant_2022.parquet",
        dataset_name="egrid_2022_data",
        preferred_sheet="PLNT22",
    )


def extract_egrid_plant_2023() -> ExtractedInterimTable:
    """Extract the 2023 eGRID plant table."""

    return _extract_from_sheet(
        output_name="egrid_plant_2023.parquet",
        dataset_name="egrid_2023_data",
        preferred_sheet="PLNT23",
    )


def extract_egrid_state_2022() -> ExtractedInterimTable:
    """Extract the 2022 eGRID state table."""

    return _extract_from_sheet(
        output_name="egrid_state_2022.parquet",
        dataset_name="egrid_2022_data",
        preferred_sheet="ST22",
    )


def extract_egrid_state_2023() -> ExtractedInterimTable:
    """Extract the 2023 eGRID state table."""

    return _extract_from_sheet(
        output_name="egrid_state_2023.parquet",
        dataset_name="egrid_2023_data",
        preferred_sheet="ST23",
    )


def extract_egrid_ba_2022() -> ExtractedInterimTable:
    """Extract the 2022 eGRID balancing-authority table."""

    return _extract_from_sheet(
        output_name="egrid_ba_2022.parquet",
        dataset_name="egrid_2022_data",
        preferred_sheet="BA22",
    )


def extract_egrid_ba_2023() -> ExtractedInterimTable:
    """Extract the 2023 eGRID balancing-authority table."""

    return _extract_from_sheet(
        output_name="egrid_ba_2023.parquet",
        dataset_name="egrid_2023_data",
        preferred_sheet="BA23",
        sheet_resolver=_resolve_ba23_sheet_name,
    )


def extract_defra_factors_2025() -> ExtractedInterimTable:
    """Extract the 2025 DEFRA flat factors table."""

    return _extract_from_sheet(
        output_name="defra_factors_2025.parquet",
        dataset_name="defra_2025_flat",
        preferred_sheet="Factors by Category",
    )


def extract_sbti_companies() -> ExtractedInterimTable:
    """Extract the SBTi companies table."""

    return _extract_from_sheet(
        output_name="sbti_companies.parquet",
        dataset_name="sbti_companies",
        preferred_sheet="Data",
    )


def extract_sbti_targets() -> ExtractedInterimTable:
    """Extract the SBTi targets table."""

    return _extract_from_sheet(
        output_name="sbti_targets.parquet",
        dataset_name="sbti_targets",
        preferred_sheet="WebsiteData",
    )


def extract_all_interim_tables() -> list[ExtractedInterimTable]:
    """Extract every Phase 3 interim table."""

    return [
        extract_egrid_plant_2022(),
        extract_egrid_plant_2023(),
        extract_egrid_state_2022(),
        extract_egrid_state_2023(),
        extract_egrid_ba_2022(),
        extract_egrid_ba_2023(),
        extract_defra_factors_2025(),
        extract_sbti_companies(),
        extract_sbti_targets(),
    ]


def summarize_extracted_table(extracted_table: ExtractedInterimTable) -> dict[str, Any]:
    """Return a compact summary for one extracted interim table."""

    return extracted_table.summary()


def guess_key_column_candidates(column_names: list[str]) -> list[str]:
    """Heuristically identify likely key or join-relevant columns."""

    candidates: list[str] = []
    for column_name in column_names:
        if KEY_COLUMN_PATTERN.search(column_name):
            candidates.append(column_name)

    return candidates[:20]


def _extract_from_sheet(
    *,
    output_name: str,
    dataset_name: str,
    preferred_sheet: str,
    sheet_resolver: Any = None,
) -> ExtractedInterimTable:
    """Extract, clean, and annotate one target worksheet."""

    parser = WorkbookParser(dataset_name=dataset_name, catalog_key=dataset_name)
    notes: list[str] = []

    if sheet_resolver is None:
        sheet_name = preferred_sheet
    else:
        sheet_name, resolution_notes = sheet_resolver(parser, preferred_sheet)
        notes.extend(resolution_notes)

    if sheet_name not in parser.list_sheet_names():
        raise ValueError(
            f"Sheet '{sheet_name}' was not found in dataset '{dataset_name}'. "
            f"Available sheets: {parser.list_sheet_names()}"
        )

    dataframe = parser.load_sheet_as_dataframe(sheet_name)
    dataframe = _prepare_interim_dataframe(
        dataframe=dataframe,
        dataset_name=dataset_name,
        sheet_name=sheet_name,
        notes=notes,
    )

    return ExtractedInterimTable(
        output_name=output_name,
        source_dataset=dataset_name,
        source_sheet=sheet_name,
        dataframe=dataframe,
        key_column_candidates=guess_key_column_candidates(list(dataframe.columns)),
        notes=notes,
    )


def _prepare_interim_dataframe(
    *,
    dataframe: pd.DataFrame,
    dataset_name: str,
    sheet_name: str,
    notes: list[str],
) -> pd.DataFrame:
    """Apply lightweight extraction cleanup while preserving source structure."""

    cleaned = dataframe.copy()
    cleaned = _trim_string_columns(cleaned)
    cleaned = cleaned.replace("", pd.NA)
    cleaned = cleaned.dropna(axis="rows", how="all")
    cleaned = _drop_empty_unnamed_columns(cleaned)

    if dataset_name.startswith("egrid_"):
        cleaned, dropped_rows = _drop_egrid_field_code_rows(cleaned)
        if dropped_rows:
            notes.append(
                f"Dropped {dropped_rows} eGRID field-code metadata row(s) from {sheet_name}."
            )

    cleaned = cleaned.infer_objects(copy=False).convert_dtypes()
    cleaned = _coerce_remaining_object_columns_to_string(cleaned)
    cleaned["source_dataset"] = dataset_name
    cleaned["source_sheet"] = sheet_name
    return cleaned.reset_index(drop=True)


def _trim_string_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Trim leading and trailing whitespace from string-like columns."""

    cleaned = dataframe.copy()
    string_like_columns = cleaned.select_dtypes(include=["object", "string"]).columns

    for column_name in string_like_columns:
        cleaned[column_name] = cleaned[column_name].map(
            lambda value: value.strip() if isinstance(value, str) else value
        )

    return cleaned


def _drop_empty_unnamed_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Drop columns that are both unnamed and fully empty."""

    keep_columns = [
        column_name
        for column_name in dataframe.columns
        if not str(column_name).startswith("unnamed") or not dataframe[column_name].isna().all()
    ]
    return dataframe.loc[:, keep_columns]


def _drop_egrid_field_code_rows(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Drop the eGRID field-code row that sits directly beneath the header."""

    if "data_year" not in dataframe.columns:
        return dataframe, 0

    year_values = dataframe["data_year"].map(
        lambda value: value.strip().upper() if isinstance(value, str) else value
    )
    metadata_mask = year_values.eq("YEAR")
    dropped_rows = int(metadata_mask.sum())

    if not dropped_rows:
        return dataframe, 0

    return dataframe.loc[~metadata_mask].copy(), dropped_rows


def _coerce_remaining_object_columns_to_string(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Cast any still-mixed object columns to nullable strings for parquet safety."""

    cleaned = dataframe.copy()
    object_columns = cleaned.select_dtypes(include=["object"]).columns

    for column_name in object_columns:
        cleaned[column_name] = cleaned[column_name].astype("string")

    return cleaned


def _resolve_ba23_sheet_name(
    parser: WorkbookParser,
    preferred_sheet: str,
) -> tuple[str, list[str]]:
    """Resolve the best balancing-authority sheet for 2023, with fallback logging."""

    sheet_names = parser.list_sheet_names()
    if preferred_sheet in sheet_names:
        return preferred_sheet, []

    fallback_candidates = [
        sheet_name
        for sheet_name in sheet_names
        if re.fullmatch(r"BA\d+", sheet_name) or "balancing" in sheet_name.lower()
    ]
    if not fallback_candidates:
        raise ValueError(
            f"Could not infer a balancing-authority sheet for dataset '{parser.dataset_name}'."
        )

    fallback_sheet = fallback_candidates[0]
    note = (
        f"Preferred sheet '{preferred_sheet}' was unavailable in dataset '{parser.dataset_name}'. "
        f"Used fallback sheet '{fallback_sheet}' as the closest balancing-authority table."
    )
    return fallback_sheet, [note]
