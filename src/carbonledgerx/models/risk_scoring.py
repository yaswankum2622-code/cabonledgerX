"""Rule-based commitment risk scoring and intelligence-table assembly."""

from __future__ import annotations

from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


RISK_BASE_SCORE = 15.0
CREDIBILITY_BASE_SCORE = 85.0


def build_company_commitment_risk_scores(
    *,
    contradiction_flags: pd.DataFrame | None = None,
    company_panel: pd.DataFrame | None = None,
    assessment: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build a transparent rule-based scoring table for company commitments."""

    if contradiction_flags is None:
        contradiction_flags = _read_processed_table("company_contradiction_flags.parquet")
    if company_panel is None:
        company_panel = _read_processed_table("company_synthetic_panel.parquet")
    if assessment is None:
        assessment = _read_processed_table("company_commitment_assessment.parquet")

    scoring_input = _build_scoring_input(
        contradiction_flags=contradiction_flags,
        company_panel=company_panel,
        assessment=assessment,
    )
    scoring_rows = [
        _build_company_score_row(company_record)
        for company_record in scoring_input.to_dict(orient="records")
    ]
    scoring_dataframe = pd.DataFrame(scoring_rows).convert_dtypes()

    selected_key_fields = [
        "company_id",
        "commitment_credibility_score",
        "target_miss_risk_score",
        "risk_band",
        "credibility_band",
    ]
    assumptions = [
        "Target miss risk starts from a modest base score and adds explicit gap, claim, support, timing, and trend components.",
        "Commitment credibility starts high and subtracts penalties from the same components, with modest bonuses for stronger renewable share, electrification support, and on-track delivery.",
        "All scores are clamped to the 0 to 100 range and then converted into coarse descriptive bands.",
    ]
    return ProcessedTableArtifact(
        output_name="company_commitment_risk_scores.parquet",
        dataframe=scoring_dataframe.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[
            "company_contradiction_flags.parquet",
            "company_synthetic_panel.parquet",
            "company_commitment_assessment.parquet",
        ],
    )


def build_company_commitment_intelligence(
    *,
    company_panel: pd.DataFrame | None = None,
    baseline: pd.DataFrame | None = None,
    assessment: pd.DataFrame | None = None,
    contradiction_flags: pd.DataFrame | None = None,
    risk_scores: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build the combined company-level commitment intelligence table."""

    if company_panel is None:
        company_panel = _read_processed_table("company_synthetic_panel.parquet")
    if baseline is None:
        baseline = _read_processed_table("company_emissions_baseline.parquet")
    if assessment is None:
        assessment = _read_processed_table("company_commitment_assessment.parquet")
    if contradiction_flags is None:
        contradiction_flags = _read_processed_table("company_contradiction_flags.parquet")
    if risk_scores is None:
        risk_scores = _read_processed_table("company_commitment_risk_scores.parquet")

    panel_columns = [
        "company_id",
        "company_name",
        "sector",
        "country",
        "target_year",
        "target_reduction_pct",
        "renewable_share_pct",
        "fleet_electrification_pct",
        "modeled_disclosure_claim",
    ]
    baseline_columns = [
        "company_id",
        "current_total_mb_tco2e",
        "factor_region_type",
        "scope2_mb_factor_source",
    ]
    assessment_columns = [
        "company_id",
        "assessment_year",
        "baseline_total_mb_tco2e",
        "projected_total_mb_tco2e",
        "implied_reduction_pct",
        "target_gap_pct",
        "target_gap_tco2e",
        "target_met_flag",
        "assessment_notes",
    ]
    contradiction_columns = [
        "company_id",
        "optimistic_claim_but_miss_flag",
        "negative_reduction_flag",
        "large_target_gap_flag",
        "near_term_target_underperforming_flag",
        "low_renewable_share_flag",
        "weak_mb_procurement_flag",
        "capped_target_year_flag",
        "ambition_without_support_flag",
        "contradiction_count",
        "contradiction_summary",
    ]
    score_columns = [
        "company_id",
        "commitment_credibility_score",
        "target_miss_risk_score",
        "risk_band",
        "credibility_band",
        "score_gap_component",
        "score_claim_component",
        "score_support_component",
        "score_timing_component",
        "score_trend_component",
        "scoring_notes",
    ]

    intelligence = company_panel.loc[
        :,
        [column for column in panel_columns if column in company_panel.columns],
    ].copy()
    intelligence = intelligence.merge(
        baseline.loc[:, [column for column in baseline_columns if column in baseline.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    intelligence = intelligence.merge(
        assessment.loc[:, [column for column in assessment_columns if column in assessment.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    intelligence = intelligence.merge(
        contradiction_flags.loc[:, [column for column in contradiction_columns if column in contradiction_flags.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    intelligence = intelligence.merge(
        risk_scores.loc[:, [column for column in score_columns if column in risk_scores.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
    )

    selected_columns = [
        "company_id",
        "company_name",
        "sector",
        "country",
        "target_year",
        "target_reduction_pct",
        "baseline_total_mb_tco2e",
        "projected_total_mb_tco2e",
        "implied_reduction_pct",
        "target_gap_pct",
        "target_gap_tco2e",
        "target_met_flag",
        "modeled_disclosure_claim",
        "renewable_share_pct",
        "fleet_electrification_pct",
        "factor_region_type",
        "scope2_mb_factor_source",
        "optimistic_claim_but_miss_flag",
        "negative_reduction_flag",
        "large_target_gap_flag",
        "near_term_target_underperforming_flag",
        "low_renewable_share_flag",
        "weak_mb_procurement_flag",
        "capped_target_year_flag",
        "ambition_without_support_flag",
        "contradiction_count",
        "contradiction_summary",
        "commitment_credibility_score",
        "target_miss_risk_score",
        "risk_band",
        "credibility_band",
        "scoring_notes",
        "assessment_notes",
    ]
    intelligence = intelligence.loc[
        :,
        [column for column in selected_columns if column in intelligence.columns],
    ].copy()

    selected_key_fields = [
        "company_id",
        "target_year",
        "baseline_total_mb_tco2e",
        "projected_total_mb_tco2e",
        "contradiction_count",
        "target_miss_risk_score",
        "commitment_credibility_score",
    ]
    assumptions = [
        "The intelligence table keeps company_synthetic_panel as the core grain and enriches it with baseline, assessment, contradiction, and score outputs.",
        "Contradiction flags and score outputs are joined one-to-one by company_id after the assessment and scoring layers have already normalized the company grain.",
        "The combined table preserves both rule outcomes and the short note fields used to explain those outcomes.",
    ]
    return ProcessedTableArtifact(
        output_name="company_commitment_intelligence.parquet",
        dataframe=intelligence.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[
            "company_synthetic_panel.parquet",
            "company_emissions_baseline.parquet",
            "company_commitment_assessment.parquet",
            "company_contradiction_flags.parquet",
            "company_commitment_risk_scores.parquet",
        ],
    )


def _build_scoring_input(
    *,
    contradiction_flags: pd.DataFrame,
    company_panel: pd.DataFrame,
    assessment: pd.DataFrame,
) -> pd.DataFrame:
    """Build the company-level scoring input table."""

    panel_columns = [
        "company_id",
        "renewable_share_pct",
        "fleet_electrification_pct",
        "modeled_disclosure_claim",
    ]
    assessment_columns = [
        "company_id",
        "assessment_year",
        "target_gap_pct",
        "implied_reduction_pct",
        "target_met_flag",
    ]

    scoring_input = contradiction_flags.merge(
        company_panel.loc[:, [column for column in panel_columns if column in company_panel.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
        suffixes=("", "_panel"),
    )
    scoring_input = scoring_input.merge(
        assessment.loc[:, [column for column in assessment_columns if column in assessment.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
        suffixes=("", "_assessment"),
    )

    numeric_columns = [
        "renewable_share_pct",
        "fleet_electrification_pct",
        "target_gap_pct",
        "implied_reduction_pct",
        "assessment_year",
        "contradiction_count",
    ]
    for column_name in numeric_columns:
        if column_name in scoring_input.columns:
            scoring_input[column_name] = pd.to_numeric(scoring_input[column_name], errors="coerce")

    return scoring_input


def _build_company_score_row(company_record: dict[str, Any]) -> dict[str, Any]:
    """Compute risk and credibility scores for one company."""

    target_gap_pct = _safe_float(company_record.get("target_gap_pct"))
    implied_reduction_pct = _safe_float(company_record.get("implied_reduction_pct"))
    renewable_share_pct = _safe_float(company_record.get("renewable_share_pct"))
    fleet_electrification_pct = _safe_float(company_record.get("fleet_electrification_pct"))
    contradiction_count = int(_safe_float(company_record.get("contradiction_count")))
    target_met_flag = _safe_bool(company_record.get("target_met_flag", False))
    assessment_year = int(_safe_float(company_record.get("assessment_year"), default=2030.0))

    score_gap_component = _gap_component(target_gap_pct)
    score_claim_component = _claim_component(
        contradiction_count=contradiction_count,
        optimistic_claim_but_miss_flag=bool(company_record.get("optimistic_claim_but_miss_flag", False)),
    )
    score_support_component = _support_component(
        low_renewable_share_flag=bool(company_record.get("low_renewable_share_flag", False)),
        weak_mb_procurement_flag=bool(company_record.get("weak_mb_procurement_flag", False)),
        ambition_without_support_flag=bool(company_record.get("ambition_without_support_flag", False)),
    )
    score_timing_component = _timing_component(
        capped_target_year_flag=bool(company_record.get("capped_target_year_flag", False)),
        near_term_target_underperforming_flag=bool(
            company_record.get("near_term_target_underperforming_flag", False)
        ),
        assessment_year=assessment_year,
    )
    score_trend_component = _trend_component(implied_reduction_pct)

    target_miss_risk_score = _clamp_score(
        RISK_BASE_SCORE
        + score_gap_component
        + score_claim_component
        + score_support_component
        + score_timing_component
        + score_trend_component
    )

    credibility_bonus = _credibility_bonus(
        renewable_share_pct=renewable_share_pct,
        fleet_electrification_pct=fleet_electrification_pct,
        target_met_flag=target_met_flag,
        contradiction_count=contradiction_count,
    )
    commitment_credibility_score = _clamp_score(
        CREDIBILITY_BASE_SCORE
        - (score_gap_component * 0.7)
        - (score_claim_component * 1.0)
        - (score_support_component * 0.8)
        - (score_timing_component * 0.6)
        - (score_trend_component * 0.75)
        + credibility_bonus
    )

    return {
        "company_id": company_record["company_id"],
        "company_name": company_record["company_name"],
        "commitment_credibility_score": commitment_credibility_score,
        "target_miss_risk_score": target_miss_risk_score,
        "risk_band": _risk_band(target_miss_risk_score),
        "credibility_band": _credibility_band(commitment_credibility_score),
        "score_gap_component": round(score_gap_component, 1),
        "score_claim_component": round(score_claim_component, 1),
        "score_support_component": round(score_support_component, 1),
        "score_timing_component": round(score_timing_component, 1),
        "score_trend_component": round(score_trend_component, 1),
        "scoring_notes": _scoring_notes(
            target_gap_pct=target_gap_pct,
            implied_reduction_pct=implied_reduction_pct,
            renewable_share_pct=renewable_share_pct,
            fleet_electrification_pct=fleet_electrification_pct,
            contradiction_count=contradiction_count,
            target_met_flag=target_met_flag,
            score_gap_component=score_gap_component,
            score_claim_component=score_claim_component,
            score_support_component=score_support_component,
            score_timing_component=score_timing_component,
            score_trend_component=score_trend_component,
        ),
    }


def _gap_component(target_gap_pct: float) -> float:
    """Return the additive risk component from the target gap."""

    if target_gap_pct <= 0:
        return 0.0
    if target_gap_pct <= 10:
        return 8.0
    if target_gap_pct <= 25:
        return 18.0
    if target_gap_pct <= 50:
        return 30.0
    return 42.0


def _claim_component(
    *,
    contradiction_count: int,
    optimistic_claim_but_miss_flag: bool,
) -> float:
    """Return the additive risk component from claim contradictions."""

    score = 0.0
    if optimistic_claim_but_miss_flag:
        score += 18.0
    if contradiction_count >= 4:
        score += 8.0
    elif contradiction_count >= 2:
        score += 4.0
    return score


def _support_component(
    *,
    low_renewable_share_flag: bool,
    weak_mb_procurement_flag: bool,
    ambition_without_support_flag: bool,
) -> float:
    """Return the additive risk component from weak operational support."""

    score = 0.0
    if low_renewable_share_flag:
        score += 10.0
    if weak_mb_procurement_flag:
        score += 10.0
    if ambition_without_support_flag:
        score += 14.0
    return score


def _timing_component(
    *,
    capped_target_year_flag: bool,
    near_term_target_underperforming_flag: bool,
    assessment_year: int,
) -> float:
    """Return the additive risk component from timing pressure."""

    score = 0.0
    if capped_target_year_flag:
        score += 10.0
    if near_term_target_underperforming_flag and assessment_year <= 2030:
        score += 16.0
    return score


def _trend_component(implied_reduction_pct: float) -> float:
    """Return the additive risk component from the forecast trajectory."""

    if implied_reduction_pct < 0:
        return 20.0
    if implied_reduction_pct < 5:
        return 10.0
    if implied_reduction_pct < 15:
        return 5.0
    return 0.0


def _credibility_bonus(
    *,
    renewable_share_pct: float,
    fleet_electrification_pct: float,
    target_met_flag: bool,
    contradiction_count: int,
) -> float:
    """Return modest credibility bonuses for stronger supporting signals."""

    bonus = 0.0
    if renewable_share_pct >= 60:
        bonus += 8.0
    elif renewable_share_pct >= 40:
        bonus += 4.0

    if fleet_electrification_pct >= 40:
        bonus += 5.0
    elif fleet_electrification_pct >= 25:
        bonus += 2.0

    if target_met_flag:
        bonus += 6.0
    if contradiction_count == 0:
        bonus += 4.0
    return bonus


def _risk_band(score: float) -> str:
    """Map a risk score to a coarse descriptive band."""

    if score < 25:
        return "low"
    if score < 50:
        return "moderate"
    if score < 75:
        return "high"
    return "severe"


def _credibility_band(score: float) -> str:
    """Map a credibility score to a coarse descriptive band."""

    if score >= 75:
        return "strong"
    if score >= 55:
        return "watch"
    if score >= 35:
        return "weak"
    return "critical"


def _clamp_score(value: float) -> float:
    """Clamp a score to the 0 to 100 range with one decimal place."""

    return round(min(100.0, max(0.0, value)), 1)


def _scoring_notes(
    *,
    target_gap_pct: float,
    implied_reduction_pct: float,
    renewable_share_pct: float,
    fleet_electrification_pct: float,
    contradiction_count: int,
    target_met_flag: bool,
    score_gap_component: float,
    score_claim_component: float,
    score_support_component: float,
    score_timing_component: float,
    score_trend_component: float,
) -> str:
    """Render a concise explanation of the score drivers."""

    notes = [
        f"gap component {score_gap_component:.1f} from target_gap_pct {target_gap_pct:.1f}",
        f"claim component {score_claim_component:.1f} from {contradiction_count} contradiction flags",
        f"support component {score_support_component:.1f} with renewable_share_pct {renewable_share_pct:.1f} and fleet_electrification_pct {fleet_electrification_pct:.1f}",
        f"timing component {score_timing_component:.1f} from target timing pressure",
        f"trend component {score_trend_component:.1f} from implied_reduction_pct {implied_reduction_pct:.1f}",
    ]
    if target_met_flag:
        notes.append("company remains on-track against the assessed target year")
    else:
        notes.append("company remains off-track against the assessed target year")
    return "; ".join(notes)


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
