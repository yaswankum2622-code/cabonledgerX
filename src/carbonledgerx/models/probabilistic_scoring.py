"""Probabilistic commitment-miss scoring built on top of rule-based outputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


RANDOM_STATE = 20260423
TEST_SIZE = 0.25
N_CALIBRATION_BINS = 8

NAIVE_LOW_THRESHOLD = 0.25
NAIVE_MODERATE_THRESHOLD = 0.50
NAIVE_HIGH_THRESHOLD = 0.75

MATERIAL_TARGET_GAP_THRESHOLD_PCT = 35.0
MATERIAL_TARGET_GAP_SHARE_OF_BASELINE = 0.20
NEGATIVE_REDUCTION_CONTRADICTION_THRESHOLD = 4

FORECAST_DIRECTION_PROXY_MAP = {
    "declining": -1.0,
    "flat": 0.0,
    "rising": 1.0,
}

MODEL_EXPLAINABILITY_RANK = {
    "calibrated_logistic_regression": 0,
    "logistic_regression": 1,
    "calibrated_best_tree_model": 2,
    "hist_gradient_boosting": 3,
    "random_forest": 4,
}

FEATURE_COLUMNS = [
    "target_gap_pct",
    "target_gap_tco2e",
    "implied_reduction_pct",
    "contradiction_count",
    "renewable_share_pct",
    "fleet_electrification_pct",
    "capped_target_year_flag_int",
    "optimistic_claim_but_miss_flag_int",
    "negative_reduction_flag_int",
    "large_target_gap_flag_int",
    "near_term_target_underperforming_flag_int",
    "low_renewable_share_flag_int",
    "weak_mb_procurement_flag_int",
    "ambition_without_support_flag_int",
    "target_miss_risk_score",
    "commitment_credibility_score",
    "mean_ape_pct",
    "mean_abs_error",
    "interval_coverage_pct",
    "forecast_2030_total_mb_tco2e",
    "latest_actual_total_mb_tco2e",
    "forecast_change_pct_to_2030",
    "forecast_direction_proxy",
]

CANDIDATE_MODEL_NAMES = [
    "logistic_regression",
    "random_forest",
    "hist_gradient_boosting",
    "calibrated_logistic_regression",
    "calibrated_best_tree_model",
]


@dataclass(slots=True)
class ProbabilisticScoringBundle:
    """Container for probabilistic model artifacts and evaluation data."""

    modeling_dataset: pd.DataFrame
    holdout_predictions: pd.DataFrame
    selected_model_name: str
    best_tree_model_name: str
    model_comparison_artifact: ProcessedTableArtifact
    probability_scores_artifact: ProcessedTableArtifact
    scoring_comparison_artifact: ProcessedTableArtifact


def build_probabilistic_scoring_bundle(
    *,
    assessment: pd.DataFrame | None = None,
    rule_based_scores: pd.DataFrame | None = None,
    contradiction_flags: pd.DataFrame | None = None,
    commitment_intelligence: pd.DataFrame | None = None,
    forecast_summary: pd.DataFrame | None = None,
    company_panel: pd.DataFrame | None = None,
) -> ProbabilisticScoringBundle:
    """Build probabilistic scoring outputs from a compact candidate-model workflow."""

    if assessment is None:
        assessment = _read_processed_table("company_commitment_assessment.parquet")
    if rule_based_scores is None:
        rule_based_scores = _read_processed_table("company_commitment_risk_scores.parquet")
    if contradiction_flags is None:
        contradiction_flags = _read_processed_table("company_contradiction_flags.parquet")
    if commitment_intelligence is None:
        commitment_intelligence = _read_processed_table("company_commitment_intelligence.parquet")
    if forecast_summary is None:
        forecast_summary = _read_processed_table("company_forecast_summary.parquet")
    if company_panel is None:
        company_panel = _read_processed_table("company_synthetic_panel.parquet")

    modeling_dataset = build_probabilistic_modeling_dataset(
        assessment=assessment,
        rule_based_scores=rule_based_scores,
        contradiction_flags=contradiction_flags,
        commitment_intelligence=commitment_intelligence,
        forecast_summary=forecast_summary,
        company_panel=company_panel,
    )

    X = modeling_dataset.loc[:, FEATURE_COLUMNS].copy()
    y = modeling_dataset["training_label_miss_flag"].astype(int)
    company_ids = modeling_dataset["company_id"].astype(str)

    X_train, X_test, y_train, y_test, company_id_train, company_id_test = train_test_split(
        X,
        y,
        company_ids,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=y,
    )

    best_tree_model_name = _choose_best_tree_model(X_train=X_train, y_train=y_train)
    comparison_dataframe, holdout_predictions = _evaluate_candidate_models(
        X_train=X_train,
        X_test=X_test,
        y_train=y_train,
        y_test=y_test,
        company_id_test=company_id_test,
        best_tree_model_name=best_tree_model_name,
    )
    selected_model_name = _select_final_model_name(comparison_dataframe)
    probability_scores = _score_companies_with_selected_model(
        modeling_dataset=modeling_dataset,
        selected_model_name=selected_model_name,
        best_tree_model_name=best_tree_model_name,
    )
    scoring_comparison = _build_scoring_comparison_table(
        modeling_dataset=modeling_dataset,
        probability_scores=probability_scores,
    )

    model_comparison_artifact = ProcessedTableArtifact(
        output_name="probabilistic_model_comparison.parquet",
        dataframe=comparison_dataframe.reset_index(drop=True).convert_dtypes(),
        selected_key_fields=[
            "model_name",
            "roc_auc",
            "brier_score",
            "log_loss",
            "calibration_quality_label",
            "selected_final_model_flag",
        ],
        assumptions=[
            "Candidate models are evaluated on a reproducible stratified train/test split, with calibration and Brier quality prioritized over raw discrimination.",
            "The candidate set is intentionally compact: logistic regression, random forest, hist gradient boosting, calibrated logistic regression, and a calibrated version of the best tree-based model.",
            "The synthetic training label is deterministic and internal to this portfolio; metrics describe consistency against that internal label rather than real observed corporate outcomes.",
        ],
        source_inputs=[
            "company_commitment_assessment.parquet",
            "company_commitment_risk_scores.parquet",
            "company_contradiction_flags.parquet",
            "company_commitment_intelligence.parquet",
            "company_forecast_summary.parquet",
            "company_synthetic_panel.parquet",
        ],
    )
    probability_scores_artifact = ProcessedTableArtifact(
        output_name="company_commitment_probability_scores.parquet",
        dataframe=probability_scores.reset_index(drop=True).convert_dtypes(),
        selected_key_fields=[
            "company_id",
            "raw_miss_probability",
            "calibrated_miss_probability",
            "miss_probability_band",
            "selected_model_name",
            "training_label_miss_flag",
        ],
        assumptions=[
            "All company-level probabilities come from the selected final model fit on the full synthetic portfolio after model comparison on a held-out test split.",
            "Raw miss probability is preserved alongside calibrated miss probability so the effect of calibration remains transparent even when the selected final model is calibrated.",
            "The key driver summary is deterministic text based on the same input features used in the compact modeling dataset; it is not a SHAP or causal explanation.",
        ],
        source_inputs=[
            "company_commitment_assessment.parquet",
            "company_commitment_risk_scores.parquet",
            "company_contradiction_flags.parquet",
            "company_commitment_intelligence.parquet",
            "company_forecast_summary.parquet",
            "company_synthetic_panel.parquet",
        ],
    )
    scoring_comparison_artifact = ProcessedTableArtifact(
        output_name="company_scoring_comparison.parquet",
        dataframe=scoring_comparison.reset_index(drop=True).convert_dtypes(),
        selected_key_fields=[
            "company_id",
            "target_miss_risk_score",
            "calibrated_miss_probability",
            "miss_probability_band",
            "risk_band",
            "scoring_alignment_label",
        ],
        assumptions=[
            "Comparison rows keep the existing rule-based risk and credibility outputs intact, then line them up with the new calibrated miss probability.",
            "Alignment labels are based on the absolute gap between normalized heuristic risk score and calibrated miss probability rather than on the descriptive risk bands alone.",
            "The comparison table is intended to expose agreement and disagreement between heuristic and probabilistic views, not to replace the existing rule-based workflow.",
        ],
        source_inputs=[
            "company_commitment_risk_scores.parquet",
            "company_commitment_intelligence.parquet",
            "company_forecast_summary.parquet",
        ],
    )

    return ProbabilisticScoringBundle(
        modeling_dataset=modeling_dataset,
        holdout_predictions=holdout_predictions,
        selected_model_name=selected_model_name,
        best_tree_model_name=best_tree_model_name,
        model_comparison_artifact=model_comparison_artifact,
        probability_scores_artifact=probability_scores_artifact,
        scoring_comparison_artifact=scoring_comparison_artifact,
    )


def build_probabilistic_modeling_dataset(
    *,
    assessment: pd.DataFrame,
    rule_based_scores: pd.DataFrame,
    contradiction_flags: pd.DataFrame,
    commitment_intelligence: pd.DataFrame,
    forecast_summary: pd.DataFrame,
    company_panel: pd.DataFrame,
) -> pd.DataFrame:
    """Build the one-row-per-company modeling dataset used for candidate models."""

    modeling_dataset = assessment.merge(
        rule_based_scores.loc[
            :,
            [
                "company_id",
                "target_miss_risk_score",
                "commitment_credibility_score",
                "risk_band",
                "credibility_band",
            ],
        ],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    modeling_dataset = modeling_dataset.merge(
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
    modeling_dataset = modeling_dataset.merge(
        commitment_intelligence.loc[
            :,
            [
                "company_id",
                "sector",
                "country",
                "renewable_share_pct",
                "fleet_electrification_pct",
                "modeled_disclosure_claim",
            ],
        ],
        on="company_id",
        how="left",
        validate="one_to_one",
        suffixes=("", "_intelligence"),
    )
    for column_name in ["sector", "country"]:
        if f"{column_name}_intelligence" in modeling_dataset.columns:
            modeling_dataset[column_name] = modeling_dataset[column_name].fillna(
                modeling_dataset[f"{column_name}_intelligence"]
            )
    modeling_dataset = modeling_dataset.merge(
        forecast_summary.loc[
            :,
            [
                "company_id",
                "selected_model_name",
                "mean_ape_pct",
                "mean_abs_error",
                "interval_coverage_pct",
                "latest_actual_total_mb_tco2e",
                "forecast_2030_total_mb_tco2e",
                "forecast_direction_label",
            ],
        ].rename(columns={"selected_model_name": "selected_statistical_forecast_model_name"}),
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    modeling_dataset = modeling_dataset.merge(
        company_panel.loc[
            :,
            [
                "company_id",
                "annual_activity_growth_pct",
                "climate_commitment_flag",
            ],
        ],
        on="company_id",
        how="left",
        validate="one_to_one",
    )

    boolean_columns = [
        "target_met_flag",
        "optimistic_claim_but_miss_flag",
        "negative_reduction_flag",
        "large_target_gap_flag",
        "near_term_target_underperforming_flag",
        "low_renewable_share_flag",
        "weak_mb_procurement_flag",
        "capped_target_year_flag",
        "ambition_without_support_flag",
        "climate_commitment_flag",
    ]
    for column_name in boolean_columns:
        modeling_dataset[column_name] = modeling_dataset[column_name].fillna(False).astype(bool)

    numeric_columns = [
        "target_year",
        "assessment_year",
        "target_reduction_pct",
        "baseline_total_mb_tco2e",
        "projected_total_mb_tco2e",
        "implied_reduction_pct",
        "target_gap_pct",
        "target_gap_tco2e",
        "contradiction_count",
        "renewable_share_pct",
        "fleet_electrification_pct",
        "target_miss_risk_score",
        "commitment_credibility_score",
        "mean_ape_pct",
        "mean_abs_error",
        "interval_coverage_pct",
        "latest_actual_total_mb_tco2e",
        "forecast_2030_total_mb_tco2e",
        "annual_activity_growth_pct",
    ]
    for column_name in numeric_columns:
        if column_name in modeling_dataset.columns:
            modeling_dataset[column_name] = pd.to_numeric(modeling_dataset[column_name], errors="coerce")

    modeling_dataset["forecast_direction_proxy"] = modeling_dataset["forecast_direction_label"].map(
        FORECAST_DIRECTION_PROXY_MAP
    ).fillna(0.0)
    modeling_dataset["forecast_change_pct_to_2030"] = np.where(
        modeling_dataset["latest_actual_total_mb_tco2e"].gt(0),
        (
            (
                modeling_dataset["forecast_2030_total_mb_tco2e"]
                - modeling_dataset["latest_actual_total_mb_tco2e"]
            )
            / modeling_dataset["latest_actual_total_mb_tco2e"]
        )
        * 100.0,
        0.0,
    )
    for flag_column in [
        "capped_target_year_flag",
        "optimistic_claim_but_miss_flag",
        "negative_reduction_flag",
        "large_target_gap_flag",
        "near_term_target_underperforming_flag",
        "low_renewable_share_flag",
        "weak_mb_procurement_flag",
        "ambition_without_support_flag",
    ]:
        modeling_dataset[f"{flag_column}_int"] = modeling_dataset[flag_column].astype(int)

    modeling_dataset["training_label_miss_flag"] = modeling_dataset.apply(
        _deterministic_training_label,
        axis=1,
    )
    modeling_dataset["label_definition_notes"] = modeling_dataset.apply(
        _label_definition_note,
        axis=1,
    )
    return modeling_dataset.reset_index(drop=True)


def _evaluate_candidate_models(
    *,
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
    company_id_test: pd.Series,
    best_tree_model_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fit and evaluate each candidate model on the held-out split."""

    comparison_rows: list[dict[str, Any]] = []
    holdout_prediction_frames: list[pd.DataFrame] = []

    for model_name in CANDIDATE_MODEL_NAMES:
        estimator = _build_estimator(
            model_name=model_name,
            best_tree_model_name=best_tree_model_name,
        )
        estimator.fit(X_train, y_train)
        predicted_probabilities = estimator.predict_proba(X_test)[:, 1]
        metrics_payload = _classification_metrics(y_true=y_test, y_prob=predicted_probabilities)
        calibration_error = _expected_calibration_error(y_true=y_test, y_prob=predicted_probabilities)

        comparison_rows.append(
            {
                "model_name": model_name,
                "roc_auc": round(metrics_payload["roc_auc"], 6),
                "brier_score": round(metrics_payload["brier_score"], 6),
                "log_loss": round(metrics_payload["log_loss"], 6),
                "accuracy": round(metrics_payload["accuracy"], 6),
                "precision": round(metrics_payload["precision"], 6),
                "recall": round(metrics_payload["recall"], 6),
                "f1": round(metrics_payload["f1"], 6),
                "calibration_error": round(calibration_error, 6),
                "calibration_quality_label": _calibration_quality_label(calibration_error),
                "selected_final_model_flag": False,
                "model_notes": _candidate_model_note(
                    model_name=model_name,
                    best_tree_model_name=best_tree_model_name,
                ),
            }
        )
        holdout_prediction_frames.append(
            pd.DataFrame(
                {
                    "company_id": company_id_test.to_numpy(),
                    "model_name": model_name,
                    "actual_label": y_test.to_numpy(),
                    "predicted_probability": np.clip(predicted_probabilities, 1e-6, 1 - 1e-6),
                }
            )
        )

    comparison_dataframe = pd.DataFrame(comparison_rows)
    selected_model_name = _select_final_model_name(comparison_dataframe)
    comparison_dataframe["selected_final_model_flag"] = comparison_dataframe["model_name"].eq(selected_model_name)
    holdout_predictions = pd.concat(holdout_prediction_frames, ignore_index=True).convert_dtypes()
    return (comparison_dataframe.convert_dtypes(), holdout_predictions)


def _select_final_model_name(comparison_dataframe: pd.DataFrame) -> str:
    """Select the final model using calibration and Brier quality first."""

    sortable = comparison_dataframe.copy()
    sortable["explainability_rank"] = sortable["model_name"].map(MODEL_EXPLAINABILITY_RANK).fillna(99)
    selected_row = sortable.sort_values(
        [
            "calibration_error",
            "brier_score",
            "log_loss",
            "roc_auc",
            "explainability_rank",
        ],
        ascending=[True, True, True, False, True],
        kind="stable",
    ).iloc[0]
    return str(selected_row["model_name"])


def _score_companies_with_selected_model(
    *,
    modeling_dataset: pd.DataFrame,
    selected_model_name: str,
    best_tree_model_name: str,
) -> pd.DataFrame:
    """Fit the selected model on the full dataset and score all companies."""

    X_full = modeling_dataset.loc[:, FEATURE_COLUMNS].copy()
    y_full = modeling_dataset["training_label_miss_flag"].astype(int)

    selected_estimator = _build_estimator(
        model_name=selected_model_name,
        best_tree_model_name=best_tree_model_name,
    )
    selected_estimator.fit(X_full, y_full)

    raw_probability_model_name = _raw_probability_model_name(
        selected_model_name=selected_model_name,
        best_tree_model_name=best_tree_model_name,
    )
    raw_estimator = _build_estimator(
        model_name=raw_probability_model_name,
        best_tree_model_name=best_tree_model_name,
    )
    raw_estimator.fit(X_full, y_full)

    raw_probabilities = raw_estimator.predict_proba(X_full)[:, 1]
    calibrated_probabilities = selected_estimator.predict_proba(X_full)[:, 1]

    probability_scores = modeling_dataset.loc[
        :,
        [
            "company_id",
            "company_name",
            "target_gap_pct",
            "target_gap_tco2e",
            "implied_reduction_pct",
            "contradiction_count",
            "renewable_share_pct",
            "fleet_electrification_pct",
            "forecast_direction_label",
            "target_miss_risk_score",
            "commitment_credibility_score",
            "training_label_miss_flag",
            "risk_band",
            "credibility_band",
        ],
    ].copy()
    probability_scores["raw_miss_probability"] = np.clip(raw_probabilities, 0.0, 1.0)
    probability_scores["calibrated_miss_probability"] = np.clip(calibrated_probabilities, 0.0, 1.0)
    probability_scores["miss_probability_band"] = probability_scores["calibrated_miss_probability"].apply(
        probability_band
    )
    probability_scores["selected_model_name"] = selected_model_name
    probability_scores["key_feature_driver_summary"] = probability_scores.apply(
        _key_feature_driver_summary,
        axis=1,
    )
    probability_scores["probabilistic_risk_note"] = probability_scores.apply(
        _probabilistic_risk_note,
        axis=1,
    )

    selected_columns = [
        "company_id",
        "company_name",
        "raw_miss_probability",
        "calibrated_miss_probability",
        "miss_probability_band",
        "selected_model_name",
        "probabilistic_risk_note",
        "training_label_miss_flag",
        "key_feature_driver_summary",
    ]
    return probability_scores.loc[:, selected_columns].copy()


def _build_scoring_comparison_table(
    *,
    modeling_dataset: pd.DataFrame,
    probability_scores: pd.DataFrame,
) -> pd.DataFrame:
    """Build the heuristic-versus-probability comparison table."""

    scoring_comparison = modeling_dataset.loc[
        :,
        [
            "company_id",
            "company_name",
            "target_miss_risk_score",
            "commitment_credibility_score",
            "risk_band",
            "credibility_band",
        ],
    ].merge(
        probability_scores.loc[
            :,
            [
                "company_id",
                "calibrated_miss_probability",
                "miss_probability_band",
            ],
        ],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    scoring_comparison["heuristic_vs_probability_gap"] = (
        scoring_comparison["target_miss_risk_score"] / 100.0
        - scoring_comparison["calibrated_miss_probability"]
    )
    scoring_comparison["scoring_alignment_label"] = scoring_comparison[
        "heuristic_vs_probability_gap"
    ].apply(_scoring_alignment_label)
    scoring_comparison["scoring_comparison_notes"] = scoring_comparison.apply(
        _scoring_comparison_note,
        axis=1,
    )
    return scoring_comparison.loc[
        :,
        [
            "company_id",
            "company_name",
            "target_miss_risk_score",
            "commitment_credibility_score",
            "calibrated_miss_probability",
            "miss_probability_band",
            "risk_band",
            "credibility_band",
            "heuristic_vs_probability_gap",
            "scoring_alignment_label",
            "scoring_comparison_notes",
        ],
    ].copy()


def probability_band(probability_value: float) -> str:
    """Map a calibrated miss probability to a descriptive band."""

    if probability_value < NAIVE_LOW_THRESHOLD:
        return "low"
    if probability_value < NAIVE_MODERATE_THRESHOLD:
        return "moderate"
    if probability_value < NAIVE_HIGH_THRESHOLD:
        return "high"
    return "severe"


def _deterministic_training_label(company_row: pd.Series) -> int:
    """Build a deterministic internal miss label for the synthetic portfolio."""

    target_gap_pct = float(company_row.get("target_gap_pct", 0.0) or 0.0)
    baseline_total_mb_tco2e = float(company_row.get("baseline_total_mb_tco2e", 0.0) or 0.0)
    target_gap_tco2e = float(company_row.get("target_gap_tco2e", 0.0) or 0.0)
    negative_reduction_flag = bool(company_row.get("negative_reduction_flag", False))
    contradiction_count = int(company_row.get("contradiction_count", 0) or 0)
    target_met_flag = bool(company_row.get("target_met_flag", False))

    material_tco2e_gap = target_gap_tco2e >= (baseline_total_mb_tco2e * MATERIAL_TARGET_GAP_SHARE_OF_BASELINE)
    material_gap = target_gap_pct >= MATERIAL_TARGET_GAP_THRESHOLD_PCT
    worsening_case = negative_reduction_flag and contradiction_count >= NEGATIVE_REDUCTION_CONTRADICTION_THRESHOLD
    label_flag = material_gap or (material_tco2e_gap and not target_met_flag) or worsening_case
    return int(label_flag)


def _label_definition_note(company_row: pd.Series) -> str:
    """Describe how the internal synthetic miss label was assigned."""

    if int(company_row["training_label_miss_flag"]) == 1:
        return (
            "Material miss label triggered by large target gap, large absolute gap relative to baseline, "
            "or a worsening trajectory combined with multiple contradiction flags."
        )
    return "Material miss label not triggered under the internal synthetic threshold."


def _choose_best_tree_model(*, X_train: pd.DataFrame, y_train: pd.Series) -> str:
    """Choose the stronger tree-based base model using simple CV Brier quality."""

    cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=RANDOM_STATE)
    candidate_names = ["random_forest", "hist_gradient_boosting"]
    candidate_scores: list[dict[str, float | str]] = []

    for model_name in candidate_names:
        brier_scores: list[float] = []
        roc_auc_scores: list[float] = []
        for train_index, valid_index in cv.split(X_train, y_train):
            X_fit = X_train.iloc[train_index]
            X_valid = X_train.iloc[valid_index]
            y_fit = y_train.iloc[train_index]
            y_valid = y_train.iloc[valid_index]

            estimator = _build_estimator(model_name=model_name, best_tree_model_name=model_name)
            estimator.fit(X_fit, y_fit)
            probabilities = estimator.predict_proba(X_valid)[:, 1]
            brier_scores.append(brier_score_loss(y_valid, probabilities))
            roc_auc_scores.append(roc_auc_score(y_valid, probabilities))

        candidate_scores.append(
            {
                "model_name": model_name,
                "mean_brier_score": float(np.mean(brier_scores)),
                "mean_roc_auc": float(np.mean(roc_auc_scores)),
            }
        )

    candidate_dataframe = pd.DataFrame(candidate_scores)
    selected = candidate_dataframe.sort_values(
        ["mean_brier_score", "mean_roc_auc"],
        ascending=[True, False],
        kind="stable",
    ).iloc[0]
    return str(selected["model_name"])


def _build_estimator(*, model_name: str, best_tree_model_name: str) -> Any:
    """Build one candidate estimator with deterministic settings."""

    if model_name == "logistic_regression":
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                (
                    "model",
                    LogisticRegression(
                        max_iter=2000,
                        class_weight="balanced",
                        random_state=RANDOM_STATE,
                    ),
                ),
            ]
        )
    if model_name == "random_forest":
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    RandomForestClassifier(
                        n_estimators=300,
                        max_depth=6,
                        min_samples_leaf=4,
                        class_weight="balanced_subsample",
                        random_state=RANDOM_STATE,
                    ),
                ),
            ]
        )
    if model_name == "hist_gradient_boosting":
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    HistGradientBoostingClassifier(
                        max_depth=4,
                        learning_rate=0.08,
                        max_iter=250,
                        random_state=RANDOM_STATE,
                    ),
                ),
            ]
        )
    if model_name == "calibrated_logistic_regression":
        return CalibratedClassifierCV(
            estimator=_build_estimator(
                model_name="logistic_regression",
                best_tree_model_name=best_tree_model_name,
            ),
            method="sigmoid",
            cv=3,
        )
    if model_name == "calibrated_best_tree_model":
        return CalibratedClassifierCV(
            estimator=_build_estimator(
                model_name=best_tree_model_name,
                best_tree_model_name=best_tree_model_name,
            ),
            method="sigmoid",
            cv=3,
        )

    raise ValueError(f"Unsupported model_name '{model_name}'.")


def _raw_probability_model_name(*, selected_model_name: str, best_tree_model_name: str) -> str:
    """Return the raw estimator used alongside the selected calibrated model."""

    if selected_model_name == "calibrated_logistic_regression":
        return "logistic_regression"
    if selected_model_name == "calibrated_best_tree_model":
        return best_tree_model_name
    return selected_model_name


def _classification_metrics(*, y_true: pd.Series, y_prob: np.ndarray) -> dict[str, float]:
    """Compute the compact set of classifier metrics used for comparison."""

    y_pred = (y_prob >= 0.5).astype(int)
    clipped_probabilities = np.clip(y_prob, 1e-6, 1 - 1e-6)
    return {
        "roc_auc": float(roc_auc_score(y_true, clipped_probabilities)),
        "brier_score": float(brier_score_loss(y_true, clipped_probabilities)),
        "log_loss": float(log_loss(y_true, clipped_probabilities)),
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
    }


def _expected_calibration_error(*, y_true: pd.Series, y_prob: np.ndarray) -> float:
    """Compute a simple expected calibration error across equal-width bins."""

    probabilities = np.clip(y_prob, 1e-6, 1 - 1e-6)
    bin_edges = np.linspace(0.0, 1.0, N_CALIBRATION_BINS + 1)
    total_count = len(probabilities)
    calibration_error = 0.0

    for bin_index in range(N_CALIBRATION_BINS):
        left_edge = bin_edges[bin_index]
        right_edge = bin_edges[bin_index + 1]
        if bin_index == N_CALIBRATION_BINS - 1:
            in_bin = (probabilities >= left_edge) & (probabilities <= right_edge)
        else:
            in_bin = (probabilities >= left_edge) & (probabilities < right_edge)
        if not in_bin.any():
            continue

        observed_rate = float(np.mean(y_true[in_bin]))
        average_confidence = float(np.mean(probabilities[in_bin]))
        calibration_error += abs(observed_rate - average_confidence) * (int(in_bin.sum()) / total_count)

    return calibration_error


def _calibration_quality_label(calibration_error: float) -> str:
    """Map expected calibration error to a descriptive quality label."""

    if calibration_error <= 0.03:
        return "excellent"
    if calibration_error <= 0.06:
        return "good"
    if calibration_error <= 0.10:
        return "fair"
    return "weak"


def _candidate_model_note(*, model_name: str, best_tree_model_name: str) -> str:
    """Return a concise note describing one candidate model."""

    if model_name == "logistic_regression":
        return "Scaled linear baseline with class balancing."
    if model_name == "random_forest":
        return "Tree ensemble with shallow depth and balanced subsampling."
    if model_name == "hist_gradient_boosting":
        return "Histogram gradient boosting on numeric commitment features."
    if model_name == "calibrated_logistic_regression":
        return "Logistic regression with sigmoid probability calibration via 3-fold CV."
    if model_name == "calibrated_best_tree_model":
        return f"Sigmoid-calibrated version of the best tree-based base model: {best_tree_model_name}."
    return "Compact candidate model."


def _key_feature_driver_summary(company_row: pd.Series) -> str:
    """Build a deterministic short summary of the main probability drivers."""

    drivers: list[str] = []
    target_gap_pct = float(company_row["target_gap_pct"])
    implied_reduction_pct = float(company_row["implied_reduction_pct"])
    contradiction_count = int(company_row["contradiction_count"])
    renewable_share_pct = float(company_row["renewable_share_pct"])
    risk_score = float(company_row["target_miss_risk_score"])
    credibility_score = float(company_row["commitment_credibility_score"])
    forecast_direction_label = str(company_row["forecast_direction_label"])

    if target_gap_pct >= MATERIAL_TARGET_GAP_THRESHOLD_PCT:
        drivers.append(f"target gap remains material at {target_gap_pct:.1f}%")
    if implied_reduction_pct < 0:
        drivers.append(f"implied reduction is negative at {implied_reduction_pct:.1f}%")
    if contradiction_count >= 4:
        drivers.append(f"{contradiction_count} contradiction flags are active")
    if renewable_share_pct < 25:
        drivers.append(f"renewable share is low at {renewable_share_pct:.1f}%")
    if risk_score >= 75:
        drivers.append(f"rule-based risk score is already severe at {risk_score:.1f}")
    if credibility_score < 35:
        drivers.append(f"credibility score remains critical at {credibility_score:.1f}")
    if forecast_direction_label == "rising":
        drivers.append("statistical 2030 outlook is still rising")

    if not drivers:
        drivers.append("gap, contradiction, and forecast signals remain comparatively contained")
    return "; ".join(drivers[:3])


def _probabilistic_risk_note(company_row: pd.Series) -> str:
    """Build a concise note describing the selected-model probability output."""

    label_text = "material miss" if int(company_row["training_label_miss_flag"]) == 1 else "non-material miss"
    return (
        f"{company_row['selected_model_name']} assigns a calibrated miss probability of "
        f"{float(company_row['calibrated_miss_probability']):.3f} ({company_row['miss_probability_band']} band); "
        f"the internal synthetic label for this company is {label_text}."
    )


def _scoring_alignment_label(gap_value: float) -> str:
    """Map a heuristic-probability gap to an alignment label."""

    absolute_gap = abs(float(gap_value))
    if absolute_gap <= 0.10:
        return "strong_agreement"
    if absolute_gap <= 0.25:
        return "mild_disagreement"
    return "major_disagreement"


def _scoring_comparison_note(company_row: pd.Series) -> str:
    """Build a short note explaining heuristic-versus-probability alignment."""

    normalized_heuristic = float(company_row["target_miss_risk_score"]) / 100.0
    calibrated_probability = float(company_row["calibrated_miss_probability"])
    gap_value = float(company_row["heuristic_vs_probability_gap"])
    if gap_value > 0:
        direction = "heuristic score is higher"
    elif gap_value < 0:
        direction = "probability score is higher"
    else:
        direction = "heuristic and probability scores are aligned"

    return (
        f"Heuristic risk normalizes to {normalized_heuristic:.3f} versus calibrated probability "
        f"{calibrated_probability:.3f}; {direction} by {abs(gap_value):.3f}."
    )


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
