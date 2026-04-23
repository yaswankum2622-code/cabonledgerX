"""Heuristic-versus-probabilistic scoring reconciliation for TargetTruth."""

from __future__ import annotations

from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


LOW_PROBABILITY_THRESHOLD = 0.25
HIGH_PROBABILITY_THRESHOLD = 0.75
MODEST_TARGET_GAP_THRESHOLD_PCT = 20.0
MATERIAL_TARGET_GAP_THRESHOLD_PCT = 35.0
EXTREME_HEURISTIC_SCORE_THRESHOLD = 80.0
HIGH_CONTRADICTION_THRESHOLD = 4
LOW_RENEWABLE_SHARE_THRESHOLD_PCT = 25.0
LOW_FLEET_ELECTRIFICATION_THRESHOLD_PCT = 20.0

RISK_BAND_LEVELS = {
    "low": 1,
    "moderate": 2,
    "high": 3,
    "severe": 4,
}


def build_company_scoring_reconciliation(
    *,
    scoring_comparison: pd.DataFrame | None = None,
    probability_scores: pd.DataFrame | None = None,
    risk_scores: pd.DataFrame | None = None,
    contradiction_flags: pd.DataFrame | None = None,
    assessment: pd.DataFrame | None = None,
    commitment_intelligence: pd.DataFrame | None = None,
    company_panel: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build the company-level scoring reconciliation table."""

    if scoring_comparison is None:
        scoring_comparison = _read_processed_table("company_scoring_comparison.parquet")
    if probability_scores is None:
        probability_scores = _read_processed_table("company_commitment_probability_scores.parquet")
    if risk_scores is None:
        risk_scores = _read_processed_table("company_commitment_risk_scores.parquet")
    if contradiction_flags is None:
        contradiction_flags = _read_processed_table("company_contradiction_flags.parquet")
    if assessment is None:
        assessment = _read_processed_table("company_commitment_assessment.parquet")
    if commitment_intelligence is None:
        commitment_intelligence = _read_processed_table("company_commitment_intelligence.parquet")
    if company_panel is None:
        company_panel = _read_processed_table("company_synthetic_panel.parquet")

    reconciliation_input = _build_reconciliation_input(
        scoring_comparison=scoring_comparison,
        probability_scores=probability_scores,
        risk_scores=risk_scores,
        contradiction_flags=contradiction_flags,
        assessment=assessment,
        commitment_intelligence=commitment_intelligence,
        company_panel=company_panel,
    )

    reconciliation_rows = [
        _build_reconciliation_row(company_record)
        for company_record in reconciliation_input.to_dict(orient="records")
    ]
    reconciliation_dataframe = pd.DataFrame(reconciliation_rows).convert_dtypes()

    selected_key_fields = [
        "company_id",
        "target_miss_risk_score",
        "calibrated_miss_probability",
        "scoring_alignment_label",
        "reconciliation_status",
        "recommended_operational_risk_band",
        "recommended_operational_score",
    ]
    assumptions = [
        "Reconciliation compares the existing rule-based risk score with the calibrated miss probability and then classifies disagreement into a compact, explainable set of operational statuses.",
        "Operational recommendations are deterministic blends of heuristic risk and calibrated probability, adjusted by contradiction burden, target gap size, and weak operational support signals.",
        "Investigation statuses are reserved for cases where heuristic severity remains high while the probabilistic layer stays low, especially when contradiction burden suggests a label-boundary mismatch rather than clean agreement.",
    ]
    return ProcessedTableArtifact(
        output_name="company_scoring_reconciliation.parquet",
        dataframe=reconciliation_dataframe.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[
            "company_scoring_comparison.parquet",
            "company_commitment_probability_scores.parquet",
            "company_commitment_risk_scores.parquet",
            "company_contradiction_flags.parquet",
            "company_commitment_assessment.parquet",
            "company_commitment_intelligence.parquet",
            "company_synthetic_panel.parquet",
        ],
    )


def _build_reconciliation_input(
    *,
    scoring_comparison: pd.DataFrame,
    probability_scores: pd.DataFrame,
    risk_scores: pd.DataFrame,
    contradiction_flags: pd.DataFrame,
    assessment: pd.DataFrame,
    commitment_intelligence: pd.DataFrame,
    company_panel: pd.DataFrame,
) -> pd.DataFrame:
    """Build the merged reconciliation input table."""

    reconciliation = scoring_comparison.merge(
        probability_scores.loc[
            :,
            [
                "company_id",
                "selected_model_name",
                "training_label_miss_flag",
                "key_feature_driver_summary",
                "probabilistic_risk_note",
            ],
        ],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    reconciliation = reconciliation.merge(
        risk_scores.loc[
            :,
            [
                "company_id",
                "scoring_notes",
            ],
        ],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    reconciliation = reconciliation.merge(
        contradiction_flags.loc[
            :,
            [
                "company_id",
                "contradiction_count",
                "optimistic_claim_but_miss_flag",
                "negative_reduction_flag",
                "large_target_gap_flag",
                "near_term_target_underperforming_flag",
                "low_renewable_share_flag",
                "weak_mb_procurement_flag",
                "capped_target_year_flag",
                "ambition_without_support_flag",
                "contradiction_summary",
            ],
        ],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    reconciliation = reconciliation.merge(
        assessment.loc[
            :,
            [
                "company_id",
                "target_gap_pct",
                "target_gap_tco2e",
                "implied_reduction_pct",
                "target_met_flag",
                "assessment_notes",
            ],
        ],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    reconciliation = reconciliation.merge(
        commitment_intelligence.loc[
            :,
            [
                "company_id",
                "sector",
                "country",
                "renewable_share_pct",
                "fleet_electrification_pct",
                "target_year",
                "target_reduction_pct",
            ],
        ],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    reconciliation = reconciliation.merge(
        company_panel.loc[
            :,
            [
                "company_id",
                "renewable_share_pct",
                "fleet_electrification_pct",
            ],
        ].rename(
            columns={
                "renewable_share_pct": "renewable_share_pct_panel",
                "fleet_electrification_pct": "fleet_electrification_pct_panel",
            }
        ),
        on="company_id",
        how="left",
        validate="one_to_one",
    )

    for column_name in ["renewable_share_pct", "fleet_electrification_pct"]:
        panel_column_name = f"{column_name}_panel"
        if panel_column_name in reconciliation.columns:
            reconciliation[column_name] = reconciliation[column_name].fillna(reconciliation[panel_column_name])

    numeric_columns = [
        "target_miss_risk_score",
        "commitment_credibility_score",
        "calibrated_miss_probability",
        "heuristic_vs_probability_gap",
        "contradiction_count",
        "target_gap_pct",
        "target_gap_tco2e",
        "implied_reduction_pct",
        "renewable_share_pct",
        "fleet_electrification_pct",
        "target_year",
        "target_reduction_pct",
    ]
    for column_name in numeric_columns:
        if column_name in reconciliation.columns:
            reconciliation[column_name] = pd.to_numeric(reconciliation[column_name], errors="coerce")

    boolean_columns = [
        "training_label_miss_flag",
        "target_met_flag",
        "optimistic_claim_but_miss_flag",
        "negative_reduction_flag",
        "large_target_gap_flag",
        "near_term_target_underperforming_flag",
        "low_renewable_share_flag",
        "weak_mb_procurement_flag",
        "capped_target_year_flag",
        "ambition_without_support_flag",
    ]
    for column_name in boolean_columns:
        if column_name in reconciliation.columns:
            reconciliation[column_name] = reconciliation[column_name].fillna(False).astype(bool)

    return reconciliation


def _build_reconciliation_row(company_record: dict[str, Any]) -> dict[str, Any]:
    """Build one reconciled scoring row."""

    heuristic_score = _safe_float(company_record.get("target_miss_risk_score"))
    calibrated_probability = _safe_float(company_record.get("calibrated_miss_probability"))
    probability_score = calibrated_probability * 100.0
    heuristic_gap = _safe_float(company_record.get("heuristic_vs_probability_gap"))
    contradiction_count = int(_safe_float(company_record.get("contradiction_count")))
    target_gap_pct = _safe_float(company_record.get("target_gap_pct"))
    implied_reduction_pct = _safe_float(company_record.get("implied_reduction_pct"))
    renewable_share_pct = _safe_float(company_record.get("renewable_share_pct"))
    fleet_electrification_pct = _safe_float(company_record.get("fleet_electrification_pct"))

    reconciliation_status = _reconciliation_status(company_record)
    disagreement_reason_primary, disagreement_reason_secondary = _disagreement_reasons(
        company_record=company_record,
        reconciliation_status=reconciliation_status,
    )
    recommended_operational_score = _recommended_operational_score(
        company_record=company_record,
        reconciliation_status=reconciliation_status,
    )

    return {
        "company_id": company_record["company_id"],
        "company_name": company_record["company_name"],
        "sector": company_record.get("sector"),
        "country": company_record.get("country"),
        "target_miss_risk_score": round(heuristic_score, 3),
        "risk_band": company_record.get("risk_band"),
        "commitment_credibility_score": round(
            _safe_float(company_record.get("commitment_credibility_score")),
            3,
        ),
        "credibility_band": company_record.get("credibility_band"),
        "calibrated_miss_probability": round(calibrated_probability, 6),
        "miss_probability_band": company_record.get("miss_probability_band"),
        "heuristic_vs_probability_gap": round(heuristic_gap, 6),
        "scoring_alignment_label": company_record.get("scoring_alignment_label"),
        "reconciliation_status": reconciliation_status,
        "recommended_operational_risk_band": _risk_band_from_score(recommended_operational_score),
        "recommended_operational_score": recommended_operational_score,
        "target_gap_pct": round(target_gap_pct, 3),
        "contradiction_count": contradiction_count,
        "renewable_share_pct": round(renewable_share_pct, 3),
        "fleet_electrification_pct": round(fleet_electrification_pct, 3),
        "implied_reduction_pct": round(implied_reduction_pct, 3),
        "training_label_miss_flag": bool(company_record.get("training_label_miss_flag", False)),
        "selected_probability_model_name": company_record.get("selected_model_name"),
        "disagreement_reason_primary": disagreement_reason_primary,
        "disagreement_reason_secondary": disagreement_reason_secondary,
        "reconciliation_notes": _reconciliation_notes(
            company_record=company_record,
            reconciliation_status=reconciliation_status,
            disagreement_reason_primary=disagreement_reason_primary,
            disagreement_reason_secondary=disagreement_reason_secondary,
            recommended_operational_score=recommended_operational_score,
            probability_score=probability_score,
        ),
    }


def _reconciliation_status(company_record: dict[str, Any]) -> str:
    """Assign a compact reconciliation status."""

    risk_band = str(company_record.get("risk_band", "") or "")
    probability_band = str(company_record.get("miss_probability_band", "") or "")
    risk_level = RISK_BAND_LEVELS.get(risk_band, 0)
    probability_level = RISK_BAND_LEVELS.get(probability_band, 0)
    band_distance = risk_level - probability_level
    heuristic_gap = _safe_float(company_record.get("heuristic_vs_probability_gap"))
    contradiction_count = int(_safe_float(company_record.get("contradiction_count")))
    target_gap_pct = _safe_float(company_record.get("target_gap_pct"))

    if risk_level == probability_level and abs(heuristic_gap) <= 0.30:
        return "aligned"

    if (
        band_distance >= 2
        or (band_distance >= 1 and heuristic_gap >= 0.20)
    ):
        if (
            contradiction_count >= HIGH_CONTRADICTION_THRESHOLD
            and risk_band in {"high", "severe"}
            and probability_band == "low"
        ):
            return "investigate_label_definition"
        return "heuristic_overcalling_candidate"

    if (
        band_distance <= -1
        and heuristic_gap <= -0.15
        and probability_band in {"high", "severe"}
    ):
        return "probability_overcalling_candidate"

    if (
        risk_band in {"high", "severe"}
        and probability_band == "low"
        and target_gap_pct < MODEST_TARGET_GAP_THRESHOLD_PCT
    ):
        return "heuristic_overcalling_candidate"

    return "mixed_signal_case"


def _disagreement_reasons(
    *,
    company_record: dict[str, Any],
    reconciliation_status: str,
) -> tuple[str, str]:
    """Return primary and secondary disagreement reasons."""

    target_gap_pct = _safe_float(company_record.get("target_gap_pct"))
    contradiction_count = int(_safe_float(company_record.get("contradiction_count")))
    negative_reduction_flag = bool(company_record.get("negative_reduction_flag", False))
    low_renewable_share_flag = bool(company_record.get("low_renewable_share_flag", False))
    weak_mb_procurement_flag = bool(company_record.get("weak_mb_procurement_flag", False))
    ambitious_without_support = bool(company_record.get("ambition_without_support_flag", False))
    heuristic_score = _safe_float(company_record.get("target_miss_risk_score"))
    calibrated_probability = _safe_float(company_record.get("calibrated_miss_probability"))
    risk_band = str(company_record.get("risk_band", "") or "")
    probability_band = str(company_record.get("miss_probability_band", "") or "")

    if reconciliation_status == "aligned":
        return ("aligned_multi_signal_view", "same_direction_risk_signal")

    if reconciliation_status == "investigate_label_definition":
        secondary_reason = "contradiction_burden_high_but_probability_low"
        if target_gap_pct < MODEST_TARGET_GAP_THRESHOLD_PCT:
            secondary_reason = "heuristic_threshold_sensitivity"
        return ("label_model_mismatch_candidate", secondary_reason)

    if reconciliation_status == "heuristic_overcalling_candidate":
        if target_gap_pct < MODEST_TARGET_GAP_THRESHOLD_PCT and heuristic_score >= EXTREME_HEURISTIC_SCORE_THRESHOLD:
            return ("heuristic_threshold_sensitivity", "modest_gap_with_extreme_heuristic")
        if contradiction_count >= HIGH_CONTRADICTION_THRESHOLD:
            return ("contradiction_burden_overrides_probability", "probability_label_boundary_case")
        if low_renewable_share_flag or weak_mb_procurement_flag or ambitious_without_support:
            return ("support_flags_keep_heuristic_elevated", "heuristic_overcalling_pattern")
        return ("heuristic_overcalling_pattern", "probability_model_downweights_non_material_gap")

    if reconciliation_status == "probability_overcalling_candidate":
        secondary_reason = "probabilistic_feature_interaction"
        if negative_reduction_flag:
            secondary_reason = "trajectory_deterioration_signal"
        elif probability_band == "severe" and risk_band in {"moderate", "high"}:
            secondary_reason = "probability_escalation_above_band"
        return ("probability_overcalling_pattern", secondary_reason)

    if low_renewable_share_flag or weak_mb_procurement_flag:
        return ("mixed_support_signal_case", "operational_support_gap")
    if negative_reduction_flag:
        return ("mixed_trajectory_case", "negative_reduction_signal")
    if contradiction_count >= HIGH_CONTRADICTION_THRESHOLD and calibrated_probability < LOW_PROBABILITY_THRESHOLD:
        return ("mixed_label_boundary_case", "contradiction_probability_tension")
    return ("mixed_signal_case", "same_band_score_scale_divergence")


def _recommended_operational_score(
    *,
    company_record: dict[str, Any],
    reconciliation_status: str,
) -> float:
    """Build a deterministic operational score from heuristic and probabilistic views."""

    heuristic_score = _safe_float(company_record.get("target_miss_risk_score"))
    probability_score = _safe_float(company_record.get("calibrated_miss_probability")) * 100.0
    contradiction_count = int(_safe_float(company_record.get("contradiction_count")))
    target_gap_pct = _safe_float(company_record.get("target_gap_pct"))
    implied_reduction_pct = _safe_float(company_record.get("implied_reduction_pct"))
    renewable_share_pct = _safe_float(company_record.get("renewable_share_pct"))
    fleet_electrification_pct = _safe_float(company_record.get("fleet_electrification_pct"))

    weights = {
        "aligned": (0.50, 0.50),
        "heuristic_overcalling_candidate": (0.30, 0.70),
        "probability_overcalling_candidate": (0.40, 0.60),
        "investigate_label_definition": (0.40, 0.60),
        "mixed_signal_case": (0.45, 0.55),
    }
    heuristic_weight, probability_weight = weights.get(reconciliation_status, (0.45, 0.55))
    blended_score = (heuristic_score * heuristic_weight) + (probability_score * probability_weight)

    contradiction_adjustment = min(8.0, contradiction_count * 1.5)
    gap_adjustment = 0.0
    if target_gap_pct >= 60:
        gap_adjustment = 10.0
    elif target_gap_pct >= MATERIAL_TARGET_GAP_THRESHOLD_PCT:
        gap_adjustment = 6.0
    elif target_gap_pct >= MODEST_TARGET_GAP_THRESHOLD_PCT:
        gap_adjustment = 3.0
    elif target_gap_pct < 10:
        gap_adjustment = -4.0

    trajectory_adjustment = 0.0
    if implied_reduction_pct < 0:
        trajectory_adjustment += 4.0
    elif implied_reduction_pct < 5:
        trajectory_adjustment += 2.0

    support_adjustment = 0.0
    if (
        renewable_share_pct < LOW_RENEWABLE_SHARE_THRESHOLD_PCT
        and fleet_electrification_pct < LOW_FLEET_ELECTRIFICATION_THRESHOLD_PCT
    ):
        support_adjustment += 4.0
    elif renewable_share_pct < LOW_RENEWABLE_SHARE_THRESHOLD_PCT:
        support_adjustment += 2.0

    status_adjustment = 0.0
    if reconciliation_status == "heuristic_overcalling_candidate" and target_gap_pct < 15 and contradiction_count <= 2:
        status_adjustment -= 6.0
    elif reconciliation_status == "probability_overcalling_candidate" and probability_score >= 75:
        status_adjustment += 4.0
    elif reconciliation_status == "investigate_label_definition":
        status_adjustment += 2.0

    operational_score = (
        blended_score
        + contradiction_adjustment
        + gap_adjustment
        + trajectory_adjustment
        + support_adjustment
        + status_adjustment
    )
    return round(min(100.0, max(0.0, operational_score)), 1)


def _risk_band_from_score(score_value: float) -> str:
    """Map a 0-100 operational score to a risk band."""

    if score_value < 25:
        return "low"
    if score_value < 50:
        return "moderate"
    if score_value < 75:
        return "high"
    return "severe"


def _reconciliation_notes(
    *,
    company_record: dict[str, Any],
    reconciliation_status: str,
    disagreement_reason_primary: str,
    disagreement_reason_secondary: str,
    recommended_operational_score: float,
    probability_score: float,
) -> str:
    """Build a compact reconciliation note."""

    heuristic_score = _safe_float(company_record.get("target_miss_risk_score"))
    return (
        f"Reconciliation status {reconciliation_status} because heuristic score {heuristic_score:.1f} "
        f"and calibrated probability score {probability_score:.1f} diverge with primary reason "
        f"{disagreement_reason_primary} and secondary reason {disagreement_reason_secondary}. "
        f"Recommended operational score is {recommended_operational_score:.1f}."
    )


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    """Convert a value to float with a stable fallback."""

    converted = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(converted):
        return float(default)
    return float(converted)


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
