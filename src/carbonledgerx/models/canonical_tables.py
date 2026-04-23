"""Canonical processed-table builders for climate datasets."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any

import pandas as pd

from carbonledgerx.data.profiling import to_jsonable
from carbonledgerx.utils.paths import interim_data_path


EGRID_METRIC_PATTERN = re.compile(
    r"(emissions_|emission_rate|net_generation|generation_percent_resource_mix|"
    r"nameplate_capacity|heat_input|nonbaseload_generation)"
)

NUMERIC_NAME_PATTERN = re.compile(
    r"(year|value|rate|emissions|generation|capacity|heat_input|pct|percent|share)"
)


@dataclass(slots=True)
class ProcessedTableArtifact:
    """Container for one processed dataframe plus build metadata."""

    output_name: str
    dataframe: pd.DataFrame
    selected_key_fields: list[str]
    assumptions: list[str] = field(default_factory=list)
    source_inputs: list[str] = field(default_factory=list)

    @property
    def n_rows(self) -> int:
        """Return the processed row count."""

        return int(self.dataframe.shape[0])

    @property
    def n_cols(self) -> int:
        """Return the processed column count."""

        return int(self.dataframe.shape[1])

    def manifest_entry(self, output_file_path: str | Path) -> dict[str, Any]:
        """Return a JSON-safe manifest entry for the processed output."""

        return to_jsonable(
            {
                "output_path": Path(output_file_path).resolve(),
                "row_count": self.n_rows,
                "column_count": self.n_cols,
                "selected_key_fields": self.selected_key_fields,
                "assumptions": self.assumptions,
                "source_inputs": self.source_inputs,
            }
        )


def build_egrid_state_factors_table() -> ProcessedTableArtifact:
    """Build the canonical state-level eGRID factors table."""

    source_files = [
        "egrid_state_2022.parquet",
        "egrid_state_2023.parquet",
    ]
    dataframe = _build_egrid_factor_table(
        source_files=source_files,
        identifier_columns=["state_abbreviation", "fips_state_code", "source_dataset", "source_sheet"],
        alias_map={"state_abbreviation": "state_code", "fips_state_code": "state_fips_code"},
    )

    selected_key_fields = [
        "year",
        "state_code",
        "state_fips_code",
        "state_annual_co2_total_output_emission_rate_lb_mwh",
        "state_annual_net_generation_mwh",
        "state_annual_co2_emissions_tons",
    ]
    assumptions = [
        "Selected state identifiers plus columns matching eGRID emissions, emission-rate, heat-input, and generation heuristics.",
        "Standardized state_abbreviation -> state_code and fips_state_code -> state_fips_code where confidence was high.",
        "Added a canonical year column inferred from source_dataset.",
    ]
    return ProcessedTableArtifact(
        output_name="egrid_state_factors.parquet",
        dataframe=dataframe,
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=source_files,
    )


def build_egrid_ba_factors_table() -> ProcessedTableArtifact:
    """Build the canonical balancing-authority eGRID factors table."""

    source_files = [
        "egrid_ba_2022.parquet",
        "egrid_ba_2023.parquet",
    ]
    dataframe = _build_egrid_factor_table(
        source_files=source_files,
        identifier_columns=[
            "balancing_authority_name",
            "balancing_authority_code",
            "source_dataset",
            "source_sheet",
        ],
        alias_map={
            "balancing_authority_name": "ba_name",
            "balancing_authority_code": "ba_code",
        },
    )

    selected_key_fields = [
        "year",
        "ba_code",
        "ba_name",
        "ba_annual_co2_total_output_emission_rate_lb_mwh",
        "ba_annual_net_generation_mwh",
        "ba_annual_co2_emissions_tons",
    ]
    assumptions = [
        "Selected BA name/code identifiers plus columns matching eGRID emissions, emission-rate, heat-input, and generation heuristics.",
        "Standardized balancing_authority_name -> ba_name and balancing_authority_code -> ba_code where confidence was high.",
        "Added a canonical year column inferred from source_dataset.",
    ]
    return ProcessedTableArtifact(
        output_name="egrid_ba_factors.parquet",
        dataframe=dataframe,
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=source_files,
    )


def build_defra_emission_factors_table() -> ProcessedTableArtifact:
    """Build the canonical 2025 DEFRA emission factors table."""

    source_file = "defra_factors_2025.parquet"
    dataframe = _read_interim_table(source_file)
    dataframe = _drop_redundant_blank_columns(dataframe)
    dataframe = _ensure_source_columns(dataframe, dataset_name="defra_2025_flat", sheet_name="Factors by Category")
    dataframe = dataframe.rename(
        columns={
            "id": "factor_id",
            "column_text": "factor_text",
            "uom": "factor_unit",
            "ghg_conversion_factor_2025": "factor_value",
        }
    )
    dataframe["factor_year"] = 2025
    dataframe["factor_value"] = pd.to_numeric(dataframe["factor_value"], errors="coerce")
    dataframe = dataframe.loc[dataframe["factor_value"].notna()].copy()

    selected_columns = [
        "factor_id",
        "scope",
        "level_1",
        "level_2",
        "level_3",
        "level_4",
        "factor_text",
        "factor_unit",
        "ghg_unit",
        "factor_value",
        "factor_year",
        "source_dataset",
        "source_sheet",
    ]
    dataframe = dataframe.loc[:, [column for column in selected_columns if column in dataframe.columns]]
    dataframe = _coerce_numeric_candidate_columns(dataframe, protected_columns={"factor_id", "scope", "level_1", "level_2", "level_3", "level_4", "factor_text", "factor_unit", "ghg_unit", "source_dataset", "source_sheet"})

    selected_key_fields = [
        "factor_id",
        "scope",
        "level_1",
        "level_2",
        "factor_unit",
        "factor_value",
        "factor_year",
    ]
    assumptions = [
        "Built from the Phase 3 DEFRA flat factors table using the machine-friendly Factors by Category sheet.",
        "Standardized id -> factor_id, column_text -> factor_text, uom -> factor_unit, and ghg_conversion_factor_2025 -> factor_value.",
        "Retained only rows with parseable numeric factor values.",
    ]
    return ProcessedTableArtifact(
        output_name="defra_emission_factors.parquet",
        dataframe=dataframe.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[source_file],
    )


def build_sbti_company_commitments_table() -> ProcessedTableArtifact:
    """Build the canonical SBTi company commitments table."""

    companies = _read_interim_table("sbti_companies.parquet")
    targets = _read_interim_table("sbti_targets.parquet")

    companies = _drop_redundant_blank_columns(companies)
    targets = _drop_redundant_blank_columns(targets)

    companies = companies.sort_values("date_updated", ascending=False, na_position="last")
    companies = companies.drop_duplicates(subset=["sbti_id"], keep="first")

    company_enrichment_columns = [
        "sbti_id",
        "near_term_status",
        "near_term_target_year",
        "long_term_status",
        "long_term_target_year",
        "net_zero_status",
        "net_zero_year",
        "target_classification_long",
        "date_updated",
        "source_dataset",
        "source_sheet",
    ]
    company_enrichment = companies.loc[
        :,
        [column for column in company_enrichment_columns if column in companies.columns],
    ].rename(
        columns={
            "source_dataset": "company_source_dataset",
            "source_sheet": "company_source_sheet",
            "date_updated": "company_record_updated_at",
        }
    )

    commitments = targets.merge(
        company_enrichment,
        on="sbti_id",
        how="left",
        validate="many_to_one",
    )
    commitments = commitments.rename(
        columns={
            "row_entry_id": "commitment_row_id",
            "sbti_id": "sbti_company_id",
            "location": "country",
            "status": "commitment_status",
            "type": "target_type",
            "sub_type": "target_sub_type",
            "source_dataset": "target_source_dataset",
            "source_sheet": "target_source_sheet",
        }
    )

    selected_columns = [
        "commitment_row_id",
        "sbti_company_id",
        "company_name",
        "isin",
        "lei",
        "country",
        "region",
        "sector",
        "organization_type",
        "action",
        "commitment_type",
        "commitment_status",
        "near_term_status",
        "net_zero_status",
        "scope",
        "base_year",
        "target_year",
        "near_term_target_year",
        "net_zero_year",
        "target_value",
        "target_type",
        "target_sub_type",
        "target",
        "target_classification_short",
        "company_temperature_alignment",
        "target_wording",
        "full_target_language",
        "date_published",
        "company_record_updated_at",
        "target_source_dataset",
        "target_source_sheet",
        "company_source_dataset",
        "company_source_sheet",
    ]
    commitments = commitments.loc[
        :,
        [column for column in selected_columns if column in commitments.columns],
    ].copy()
    commitments = _coerce_numeric_candidate_columns(
        commitments,
        protected_columns={
            "commitment_row_id",
            "company_name",
            "isin",
            "lei",
            "country",
            "region",
            "sector",
            "organization_type",
            "action",
            "commitment_type",
            "commitment_status",
            "near_term_status",
            "net_zero_status",
            "scope",
            "target_type",
            "target_sub_type",
            "target",
            "target_classification_short",
            "company_temperature_alignment",
            "target_wording",
            "full_target_language",
            "target_source_dataset",
            "target_source_sheet",
            "company_source_dataset",
            "company_source_sheet",
        },
    )

    unmatched_company_rows = int(commitments["company_record_updated_at"].isna().sum())
    selected_key_fields = [
        "sbti_company_id",
        "company_name",
        "country",
        "sector",
        "near_term_status",
        "net_zero_status",
        "scope",
        "target_year",
        "target_value",
        "target_type",
    ]
    assumptions = [
        "Processed table grain is the SBTi targets export row, with company enrichment added from the companies export.",
        "Applied a left join from targets to companies on sbti_id only; sbti_id was fully populated in both interim inputs and unique in the companies table.",
        f"Company enrichment unmatched rows after the sbti_id join: {unmatched_company_rows}. No company_name fallback join was applied.",
    ]
    return ProcessedTableArtifact(
        output_name="sbti_company_commitments.parquet",
        dataframe=commitments.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=["sbti_companies.parquet", "sbti_targets.parquet"],
    )


def build_all_processed_tables() -> list[ProcessedTableArtifact]:
    """Build every canonical processed table except the synthetic panel."""

    return [
        build_egrid_state_factors_table(),
        build_egrid_ba_factors_table(),
        build_defra_emission_factors_table(),
        build_sbti_company_commitments_table(),
    ]


def _build_egrid_factor_table(
    *,
    source_files: list[str],
    identifier_columns: list[str],
    alias_map: dict[str, str],
) -> pd.DataFrame:
    """Build a vertically concatenated eGRID factor table from interim inputs."""

    processed_frames: list[pd.DataFrame] = []

    for source_file in source_files:
        dataframe = _read_interim_table(source_file)
        dataframe = _drop_redundant_blank_columns(dataframe)
        dataframe = _ensure_source_columns(dataframe)
        dataframe["year"] = _infer_year_from_frame(dataframe, fallback_source_name=source_file)
        dataframe = dataframe.rename(columns=alias_map)

        selected_columns = _select_likely_useful_columns(
            dataframe.columns.tolist(),
            required_columns=["year", *[alias_map.get(column, column) for column in identifier_columns]],
            include_patterns=[EGRID_METRIC_PATTERN],
        )
        dataframe = dataframe.loc[:, selected_columns].copy()
        processed_frames.append(dataframe)

    combined = pd.concat(processed_frames, ignore_index=True, sort=False)
    combined = _coerce_numeric_candidate_columns(
        combined,
        protected_columns={
            "year",
            "state_code",
            "state_fips_code",
            "ba_name",
            "ba_code",
            "source_dataset",
            "source_sheet",
        },
    )
    return combined.reset_index(drop=True)


def _read_interim_table(file_name: str) -> pd.DataFrame:
    """Read one interim parquet table."""

    return pd.read_parquet(interim_data_path(file_name))


def _drop_redundant_blank_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Drop obviously redundant unnamed columns that are fully blank."""

    keep_columns = [
        column_name
        for column_name in dataframe.columns
        if not str(column_name).startswith("unnamed") or not dataframe[column_name].isna().all()
    ]
    return dataframe.loc[:, keep_columns]


def _ensure_source_columns(
    dataframe: pd.DataFrame,
    *,
    dataset_name: str | None = None,
    sheet_name: str | None = None,
) -> pd.DataFrame:
    """Ensure source_dataset and source_sheet exist on a dataframe."""

    ensured = dataframe.copy()
    if "source_dataset" not in ensured.columns:
        if dataset_name is None:
            raise ValueError("source_dataset is missing and no dataset_name fallback was provided.")
        ensured["source_dataset"] = dataset_name
    if "source_sheet" not in ensured.columns:
        if sheet_name is None:
            raise ValueError("source_sheet is missing and no sheet_name fallback was provided.")
        ensured["source_sheet"] = sheet_name
    return ensured


def _infer_year_from_frame(dataframe: pd.DataFrame, *, fallback_source_name: str = "") -> int:
    """Infer a canonical year from the dataframe or its provenance."""

    if "data_year" in dataframe.columns:
        numeric_year = pd.to_numeric(dataframe["data_year"], errors="coerce").dropna()
        unique_years = numeric_year.astype(int).unique()
        if len(unique_years) == 1:
            return int(unique_years[0])

    source_dataset_candidates = dataframe.get("source_dataset")
    if source_dataset_candidates is not None:
        non_null_sources = source_dataset_candidates.dropna().astype(str).unique().tolist()
        for source_name in non_null_sources:
            year_match = re.search(r"(20\d{2})", source_name)
            if year_match:
                return int(year_match.group(1))

    year_match = re.search(r"(20\d{2})", fallback_source_name)
    if year_match:
        return int(year_match.group(1))

    raise ValueError("Could not infer year from the provided dataframe.")


def _select_likely_useful_columns(
    column_names: list[str],
    *,
    required_columns: list[str],
    include_patterns: list[re.Pattern[str]],
) -> list[str]:
    """Select useful columns conservatively via regex heuristics."""

    selected_columns: list[str] = []

    for column_name in column_names:
        if column_name in required_columns and column_name not in selected_columns:
            selected_columns.append(column_name)
            continue

        if any(pattern.search(column_name) for pattern in include_patterns):
            selected_columns.append(column_name)

    for column_name in required_columns:
        if column_name in column_names and column_name not in selected_columns:
            selected_columns.insert(0, column_name)

    return selected_columns


def _coerce_numeric_candidate_columns(
    dataframe: pd.DataFrame,
    *,
    protected_columns: set[str],
) -> pd.DataFrame:
    """Coerce likely numeric business columns where conversion confidence is high."""

    coerced = dataframe.copy()

    for column_name in coerced.columns:
        if column_name in protected_columns:
            continue
        if not NUMERIC_NAME_PATTERN.search(column_name):
            continue

        series = coerced[column_name]
        converted = pd.to_numeric(series, errors="coerce")
        non_null_original = int(series.notna().sum())
        non_null_converted = int(converted.notna().sum())

        if non_null_original == 0:
            coerced[column_name] = converted
            continue

        if (non_null_converted / non_null_original) >= 0.7:
            coerced[column_name] = converted

    return coerced
