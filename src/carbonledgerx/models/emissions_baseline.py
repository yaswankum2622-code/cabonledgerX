"""Baseline emissions table builder for the synthetic company panel."""

from __future__ import annotations

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


def build_company_emissions_baseline(
    *,
    company_panel: pd.DataFrame | None = None,
    factor_mapping: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build the auditable baseline emissions table for synthetic companies."""

    if company_panel is None:
        company_panel = _read_processed_table("company_synthetic_panel.parquet")
    if factor_mapping is None:
        factor_mapping = _read_processed_table("company_factor_mapping.parquet")

    baseline = company_panel.merge(
        factor_mapping,
        on=["company_id", "company_name", "sector", "country"],
        how="left",
        validate="one_to_one",
    )
    baseline["baseline_year"] = baseline["base_year"]
    baseline["current_total_lb_tco2e"] = (
        pd.to_numeric(baseline["current_scope1_tco2e"], errors="coerce").fillna(0)
        + pd.to_numeric(baseline["current_scope2_lb_tco2e"], errors="coerce").fillna(0)
    )
    baseline["current_total_mb_tco2e"] = (
        pd.to_numeric(baseline["current_scope1_tco2e"], errors="coerce").fillna(0)
        + pd.to_numeric(baseline["current_scope2_mb_tco2e"], errors="coerce").fillna(0)
    )

    revenue = pd.to_numeric(baseline["revenue_usd_m"], errors="coerce")
    baseline["emissions_per_revenue_tco2e_per_usd_m"] = baseline["current_total_lb_tco2e"] / revenue
    baseline["emissions_per_revenue_mb_tco2e_per_usd_m"] = baseline["current_total_mb_tco2e"] / revenue
    baseline["scope2_mb_savings_tco2e"] = (
        pd.to_numeric(baseline["current_scope2_lb_tco2e"], errors="coerce").fillna(0)
        - pd.to_numeric(baseline["current_scope2_mb_tco2e"], errors="coerce").fillna(0)
    )

    selected_columns = [
        "company_id",
        "company_name",
        "sector",
        "country",
        "baseline_year",
        "revenue_usd_m",
        "current_scope1_tco2e",
        "current_scope2_lb_tco2e",
        "current_scope2_mb_tco2e",
        "current_total_lb_tco2e",
        "current_total_mb_tco2e",
        "emissions_per_revenue_tco2e_per_usd_m",
        "emissions_per_revenue_mb_tco2e_per_usd_m",
        "scope2_mb_savings_tco2e",
        "factor_region_type",
        "factor_region_key",
        "scope1_factor_family",
        "scope1_factor_reference",
        "scope2_lb_factor_source",
        "scope2_lb_factor_value_lb_mwh",
        "scope2_mb_factor_source",
        "scope2_mb_adjustment_multiplier",
        "scope2_mb_reference_value_lb_mwh",
        "mapping_notes",
        "renewable_share_pct",
        "fleet_electrification_pct",
        "climate_commitment_flag",
        "modeled_disclosure_claim",
    ]
    baseline = baseline.loc[:, [column for column in selected_columns if column in baseline.columns]].copy()

    selected_key_fields = [
        "company_id",
        "baseline_year",
        "current_scope1_tco2e",
        "current_scope2_lb_tco2e",
        "current_scope2_mb_tco2e",
        "current_total_lb_tco2e",
        "current_total_mb_tco2e",
    ]
    assumptions = [
        "Synthetic panel current_scope1_tco2e, current_scope2_lb_tco2e, and current_scope2_mb_tco2e are preserved as the modeled current baseline values.",
        "Factor mapping is attached for auditability and future forecasting inputs rather than used to overwrite synthetic emissions.",
        "Baseline year is set equal to the synthetic panel base_year as requested, even though factor references come from the latest available proxy tables.",
    ]
    return ProcessedTableArtifact(
        output_name="company_emissions_baseline.parquet",
        dataframe=baseline.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=["company_synthetic_panel.parquet", "company_factor_mapping.parquet"],
    )


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
