"""Evaluation artifacts for probabilistic commitment-miss scoring."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.calibration import calibration_curve

from carbonledgerx.utils.paths import output_path


def build_probability_metrics_payload(
    *,
    modeling_dataset: pd.DataFrame,
    model_comparison: pd.DataFrame,
    probability_scores: pd.DataFrame,
    scoring_comparison: pd.DataFrame,
    selected_model_name: str,
) -> dict[str, Any]:
    """Build aggregate metrics for the probabilistic scoring layer."""

    selected_model_row = model_comparison.loc[model_comparison["selected_final_model_flag"]]
    if selected_model_row.empty:
        raise ValueError("No selected final model row found in probabilistic_model_comparison.")
    selected_model_metrics = selected_model_row.iloc[0]

    raw_reference_model_name = _raw_reference_model_name(
        selected_model_name=selected_model_name,
        model_comparison=model_comparison,
    )
    raw_reference_row = model_comparison.loc[model_comparison["model_name"] == raw_reference_model_name]
    calibration_effect = None
    if not raw_reference_row.empty and raw_reference_model_name != selected_model_name:
        raw_metrics = raw_reference_row.iloc[0]
        calibration_effect = {
            "reference_model_name": raw_reference_model_name,
            "brier_score_delta": round(
                float(raw_metrics["brier_score"]) - float(selected_model_metrics["brier_score"]),
                6,
            ),
            "calibration_error_delta": round(
                float(raw_metrics["calibration_error"]) - float(selected_model_metrics["calibration_error"]),
                6,
            ),
        }

    return {
        "company_count": int(probability_scores["company_id"].nunique()),
        "label_positive_rate_pct": round(
            float(modeling_dataset["training_label_miss_flag"].mean() * 100.0),
            3,
        ),
        "selected_model_name": selected_model_name,
        "selected_model_metrics": {
            "roc_auc": round(float(selected_model_metrics["roc_auc"]), 6),
            "brier_score": round(float(selected_model_metrics["brier_score"]), 6),
            "log_loss": round(float(selected_model_metrics["log_loss"]), 6),
            "accuracy": round(float(selected_model_metrics["accuracy"]), 6),
            "precision": round(float(selected_model_metrics["precision"]), 6),
            "recall": round(float(selected_model_metrics["recall"]), 6),
            "f1": round(float(selected_model_metrics["f1"]), 6),
            "calibration_error": round(float(selected_model_metrics["calibration_error"]), 6),
            "calibration_quality_label": str(selected_model_metrics["calibration_quality_label"]),
        },
        "candidate_model_count": int(model_comparison.shape[0]),
        "miss_probability_band_counts": {
            band_name: int(count)
            for band_name, count in probability_scores["miss_probability_band"].value_counts().to_dict().items()
        },
        "alignment_label_counts": {
            label: int(count)
            for label, count in scoring_comparison["scoring_alignment_label"].value_counts().to_dict().items()
        },
        "mean_calibrated_miss_probability": round(
            float(probability_scores["calibrated_miss_probability"].mean()),
            6,
        ),
        "calibration_effect_vs_raw_reference": calibration_effect,
    }


def build_probability_model_report_markdown(
    *,
    model_comparison: pd.DataFrame,
    probability_metrics: dict[str, Any],
) -> str:
    """Render a readable markdown report for probabilistic scoring."""

    selected_model_name = str(probability_metrics["selected_model_name"])
    selected_metrics = probability_metrics["selected_model_metrics"]
    calibration_effect = probability_metrics.get("calibration_effect_vs_raw_reference")

    candidate_lines = [
        (
            f"- `{row.model_name}`: Brier {float(row.brier_score):.4f}, "
            f"ECE {float(row.calibration_error):.4f}, ROC-AUC {float(row.roc_auc):.4f}, "
            f"calibration `{row.calibration_quality_label}`"
        )
        for row in model_comparison.itertuples(index=False)
    ]

    calibration_line = "- No separate raw reference model was needed for the selected final model."
    if calibration_effect is not None:
        calibration_line = (
            f"- Relative to `{calibration_effect['reference_model_name']}`, calibration changed Brier by "
            f"{calibration_effect['brier_score_delta']:+.4f} and calibration error by "
            f"{calibration_effect['calibration_error_delta']:+.4f}."
        )

    lines = [
        "# CarbonLedgerX Probabilistic Commitment-Miss Scoring Report",
        "",
        "## Scope",
        f"- Companies scored: {probability_metrics['company_count']}",
        f"- Positive synthetic miss label rate: {probability_metrics['label_positive_rate_pct']:.2f}%",
        f"- Candidate models evaluated: {probability_metrics['candidate_model_count']}",
        "",
        "## Candidate Models",
        *candidate_lines,
        "",
        "## Final Model Selection",
        f"- Selected final model: `{selected_model_name}`",
        "- Selection prioritizes calibration error and Brier score first, then log loss and ROC-AUC, with explainability as the final tie-breaker.",
        f"- Selected model calibration quality: `{selected_metrics['calibration_quality_label']}`",
        calibration_line,
        "",
        "## Selected Model Metrics",
        f"- ROC-AUC: {selected_metrics['roc_auc']:.4f}",
        f"- Brier score: {selected_metrics['brier_score']:.4f}",
        f"- Log loss: {selected_metrics['log_loss']:.4f}",
        f"- Accuracy: {selected_metrics['accuracy']:.4f}",
        f"- Precision: {selected_metrics['precision']:.4f}",
        f"- Recall: {selected_metrics['recall']:.4f}",
        f"- F1: {selected_metrics['f1']:.4f}",
        f"- Calibration error: {selected_metrics['calibration_error']:.4f}",
        "",
        "## Limitations",
        "- The training label is deterministic and synthetic, based on the internal commitment-assessment outputs rather than real observed corporate misses.",
        "- High apparent performance should therefore be interpreted as consistency against the internal ruleset, not as evidence of real-world generalization.",
        "- The existing rule-based risk scores remain in place; this probabilistic layer is additive and comparative.",
    ]
    return "\n".join(lines) + "\n"


def write_probability_calibration_plot(
    *,
    holdout_predictions: pd.DataFrame,
    model_comparison: pd.DataFrame,
    output_file_path: str | Path | None = None,
) -> Path:
    """Write a probability calibration plot for candidate models."""

    if output_file_path is None:
        output_file_path = output_path("evaluation", "probability_calibration_plot.png")

    resolved_output_path = Path(output_file_path).resolve()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)

    figure, axes = plt.subplots(1, 2, figsize=(13.5, 5.0))

    for model_name in model_comparison["model_name"].astype(str).tolist():
        model_predictions = holdout_predictions.loc[holdout_predictions["model_name"] == model_name].copy()
        if model_predictions.empty:
            continue
        observed, predicted = calibration_curve(
            model_predictions["actual_label"].astype(int),
            model_predictions["predicted_probability"].astype(float),
            n_bins=6,
            strategy="quantile",
        )
        axes[0].plot(predicted, observed, marker="o", linewidth=1.5, label=model_name)

    axes[0].plot([0, 1], [0, 1], linestyle="--", color="black", linewidth=1)
    axes[0].set_title("Calibration Curve")
    axes[0].set_xlabel("Mean predicted probability")
    axes[0].set_ylabel("Observed miss rate")
    axes[0].legend(frameon=False, fontsize=8)

    brier_sorted = model_comparison.sort_values("brier_score", kind="stable")
    axes[1].bar(
        brier_sorted["model_name"].astype(str),
        brier_sorted["brier_score"].astype(float),
        color="#4c78a8",
    )
    axes[1].set_title("Candidate Model Brier Scores")
    axes[1].set_ylabel("Brier score")
    axes[1].tick_params(axis="x", rotation=25)

    figure.tight_layout()
    figure.savefig(resolved_output_path, dpi=180, bbox_inches="tight")
    plt.close(figure)
    return resolved_output_path


def write_markdown_report(markdown_text: str, output_file_path: str | Path) -> Path:
    """Write a markdown report to disk."""

    resolved_output_path = Path(output_file_path).resolve()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_output_path.write_text(markdown_text, encoding="utf-8")
    return resolved_output_path


def write_json_payload(payload: dict[str, Any], output_file_path: str | Path) -> Path:
    """Write a JSON payload to disk."""

    resolved_output_path = Path(output_file_path).resolve()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return resolved_output_path


def _raw_reference_model_name(*, selected_model_name: str, model_comparison: pd.DataFrame) -> str:
    """Return the raw reference model for a selected calibrated model."""

    if selected_model_name == "calibrated_logistic_regression":
        return "logistic_regression"
    if selected_model_name == "calibrated_best_tree_model":
        tree_rows = model_comparison.loc[
            model_comparison["model_name"].isin(["random_forest", "hist_gradient_boosting"])
        ].sort_values(["brier_score", "roc_auc"], ascending=[True, False], kind="stable")
        if not tree_rows.empty:
            return str(tree_rows.iloc[0]["model_name"])
    return selected_model_name
