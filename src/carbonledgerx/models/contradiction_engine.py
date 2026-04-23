"""Rule-based contradiction and plausibility flags for company commitments."""

from __future__ import annotations

from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


OPTIMISTIC_CLAIMS = {
    "Public science-based target",
    "Net-zero transition plan",
    "Operational decarbonization program",
    "High renewable electricity sourcing claim",
}

LARGE_TARGET_GAP_THRESHOLD_PCT = 25.0
NEAR_TERM_TARGET_GAP_THRESHOLD_PCT = 15.0
LOW_RENEWABLE_SHARE_THRESHOLD_PCT = 25.0
HIGH_AMBITION_TARGET_THRESHOLD_PCT = 45.0
LOW_FLEET_ELECTRIFICATION_THRESHOLD_PCT = 20.0
WEAK_MB_ADJUSTMENT_MULTIPLIER_THRESHOLD = 0.80

FLAG_ORDER = [
    "optimistic_claim_but_miss_flag",
    "negative_reduction_flag",
    "large_target_gap_flag",
    "near_term_target_underperforming_flag",
    "low_renewable_share_flag",
    "weak_mb_procurement_flag",
    "capped_target_year_flag",
    "ambition_without_support_flag",
]


def build_company_contradiction_flags(
    *,
    company_panel: pd.DataFrame | None = None,
    baseline: pd.DataFrame | None = None,
    assessment: pd.DataFrame | None = None,
    factor_mapping: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build a company-level contradiction flag table."""

    if company_panel is None:
        company_panel = _read_processed_table("company_synthetic_panel.parquet")
    if baseline is None:
        baseline = _read_processed_table("company_emissions_baseline.parquet")
    if assessment is None:
        assessment = _read_processed_table("company_commitment_assessment.parquet")
    if factor_mapping is None:
        factor_mapping = _read_processed_table("company_factor_mapping.parquet")

    contradiction_input = _build_contradiction_input(
        company_panel=company_panel,
        baseline=baseline,
        assessment=assessment,
        factor_mapping=factor_mapping,
    )
    contradiction_rows = [
        _build_company_contradiction_row(company_record)
        for company_record in contradiction_input.to_dict(orient="records")
    ]
    contradiction_dataframe = pd.DataFrame(contradiction_rows).convert_dtypes()

    selected_key_fields = [
        "company_id",
        "target_met_flag",
        "target_gap_pct",
        "implied_reduction_pct",
        "contradiction_count",
    ]
    assumptions = [
        f"Large target gaps are defined as target_gap_pct above {LARGE_TARGET_GAP_THRESHOLD_PCT} percentage points, and near-term underperformance requires assessment_year <= 2030 plus target_gap_pct above {NEAR_TERM_TARGET_GAP_THRESHOLD_PCT}.",
        f"Low renewable support is defined as renewable_share_pct below {LOW_RENEWABLE_SHARE_THRESHOLD_PCT}, and weak market-based procurement is defined by the standard procurement proxy or an adjustment multiplier >= {WEAK_MB_ADJUSTMENT_MULTIPLIER_THRESHOLD}.",
        f"Ambition without support is defined as target_reduction_pct >= {HIGH_AMBITION_TARGET_THRESHOLD_PCT} while renewable_share_pct < {LOW_RENEWABLE_SHARE_THRESHOLD_PCT} and fleet_electrification_pct < {LOW_FLEET_ELECTRIFICATION_THRESHOLD_PCT}.",
    ]
    return ProcessedTableArtifact(
        output_name="company_contradiction_flags.parquet",
        dataframe=contradiction_dataframe.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[
            "company_synthetic_panel.parquet",
            "company_emissions_baseline.parquet",
            "company_commitment_assessment.parquet",
            "company_factor_mapping.parquet",
        ],
    )


def _build_contradiction_input(
    *,
    company_panel: pd.DataFrame,
    baseline: pd.DataFrame,
    assessment: pd.DataFrame,
    factor_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Build the company-level input table used for contradiction rules."""

    panel_columns = [
        "company_id",
        "company_name",
        "modeled_disclosure_claim",
        "renewable_share_pct",
        "fleet_electrification_pct",
        "target_reduction_pct",
    ]
    baseline_columns = [
        "company_id",
        "current_total_mb_tco2e",
    ]
    assessment_columns = [
        "company_id",
        "target_met_flag",
        "target_gap_pct",
        "implied_reduction_pct",
        "assessment_year",
        "assessment_notes",
    ]
    factor_mapping_columns = [
        "company_id",
        "scope2_mb_factor_source",
        "scope2_mb_adjustment_multiplier",
    ]

    contradiction_input = company_panel.loc[
        :,
        [column for column in panel_columns if column in company_panel.columns],
    ].copy()
    contradiction_input = contradiction_input.merge(
        baseline.loc[:, [column for column in baseline_columns if column in baseline.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    contradiction_input = contradiction_input.merge(
        assessment.loc[:, [column for column in assessment_columns if column in assessment.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    contradiction_input = contradiction_input.merge(
        factor_mapping.loc[:, [column for column in factor_mapping_columns if column in factor_mapping.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
    )

    numeric_columns = [
        "renewable_share_pct",
        "fleet_electrification_pct",
        "target_reduction_pct",
        "target_gap_pct",
        "implied_reduction_pct",
        "assessment_year",
        "scope2_mb_adjustment_multiplier",
        "current_total_mb_tco2e",
    ]
    for column_name in numeric_columns:
        if column_name in contradiction_input.columns:
            contradiction_input[column_name] = pd.to_numeric(
                contradiction_input[column_name],
                errors="coerce",
            )

    return contradiction_input


def _build_company_contradiction_row(company_record: dict[str, Any]) -> dict[str, Any]:
    """Apply contradiction rules to one company record."""

    modeled_disclosure_claim = str(company_record.get("modeled_disclosure_claim", "") or "")
    target_met_flag = _safe_bool(company_record.get("target_met_flag", False))
    target_gap_pct = _safe_float(company_record.get("target_gap_pct"))
    implied_reduction_pct = _safe_float(company_record.get("implied_reduction_pct"))
    assessment_year = int(_safe_float(company_record.get("assessment_year"), default=2030.0))
    renewable_share_pct = _safe_float(company_record.get("renewable_share_pct"))
    fleet_electrification_pct = _safe_float(company_record.get("fleet_electrification_pct"))
    target_reduction_pct = _safe_float(company_record.get("target_reduction_pct"))
    scope2_mb_factor_source = str(company_record.get("scope2_mb_factor_source", "") or "")
    scope2_mb_adjustment_multiplier = _safe_float(
        company_record.get("scope2_mb_adjustment_multiplier"),
        default=1.0,
    )
    assessment_notes = str(company_record.get("assessment_notes", "") or "")

    optimistic_claim_but_miss_flag = (
        modeled_disclosure_claim in OPTIMISTIC_CLAIMS and not target_met_flag
    )
    negative_reduction_flag = implied_reduction_pct < 0.0
    large_target_gap_flag = target_gap_pct > LARGE_TARGET_GAP_THRESHOLD_PCT
    near_term_target_underperforming_flag = (
        assessment_year <= 2030 and target_gap_pct > NEAR_TERM_TARGET_GAP_THRESHOLD_PCT
    )
    low_renewable_share_flag = renewable_share_pct < LOW_RENEWABLE_SHARE_THRESHOLD_PCT
    weak_mb_procurement_flag = (
        "standard_procurement" in scope2_mb_factor_source
        or scope2_mb_adjustment_multiplier >= WEAK_MB_ADJUSTMENT_MULTIPLIER_THRESHOLD
    )
    capped_target_year_flag = "capped at 2030" in assessment_notes.lower()
    ambition_without_support_flag = (
        target_reduction_pct >= HIGH_AMBITION_TARGET_THRESHOLD_PCT
        and renewable_share_pct < LOW_RENEWABLE_SHARE_THRESHOLD_PCT
        and fleet_electrification_pct < LOW_FLEET_ELECTRIFICATION_THRESHOLD_PCT
    )

    flags = {
        "optimistic_claim_but_miss_flag": optimistic_claim_but_miss_flag,
        "negative_reduction_flag": negative_reduction_flag,
        "large_target_gap_flag": large_target_gap_flag,
        "near_term_target_underperforming_flag": near_term_target_underperforming_flag,
        "low_renewable_share_flag": low_renewable_share_flag,
        "weak_mb_procurement_flag": weak_mb_procurement_flag,
        "capped_target_year_flag": capped_target_year_flag,
        "ambition_without_support_flag": ambition_without_support_flag,
    }
    contradiction_count = int(sum(int(flag_value) for flag_value in flags.values()))

    return {
        "company_id": company_record["company_id"],
        "company_name": company_record["company_name"],
        "modeled_disclosure_claim": modeled_disclosure_claim,
        "target_met_flag": target_met_flag,
        "target_gap_pct": round(target_gap_pct, 3),
        "implied_reduction_pct": round(implied_reduction_pct, 3),
        **flags,
        "contradiction_count": contradiction_count,
        "contradiction_summary": _build_contradiction_summary(flags),
    }


def _build_contradiction_summary(flags: dict[str, bool]) -> str:
    """Render a compact semicolon-delimited contradiction summary."""

    explanations: list[str] = []
    if flags["optimistic_claim_but_miss_flag"]:
        explanations.append("optimistic claim conflicts with forecast miss")
    if flags["negative_reduction_flag"]:
        explanations.append("projected market-based emissions rise above baseline")
    if flags["large_target_gap_flag"]:
        explanations.append("target gap materially exceeds threshold")
    if flags["near_term_target_underperforming_flag"]:
        explanations.append("near-term target remains off-track by 2030")
    if flags["low_renewable_share_flag"]:
        explanations.append("renewable share remains low")
    if flags["weak_mb_procurement_flag"]:
        explanations.append("market-based procurement remains weak")
    if flags["capped_target_year_flag"]:
        explanations.append("target timing exceeds forecast horizon")
    if flags["ambition_without_support_flag"]:
        explanations.append("high ambition lacks operational support")

    if not explanations:
        return "No contradiction flags triggered."
    return "; ".join(explanations)


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    """Convert a value to float with a stable fallback."""

    converted = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(converted):
        return float(default)
    return float(converted)


def _safe_bool(value: Any, *, default: bool = False) -> bool:
    """Convert a nullable boolean-like value into a stable bool."""

    if pd.isna(value):
        return default
    return bool(value)


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
