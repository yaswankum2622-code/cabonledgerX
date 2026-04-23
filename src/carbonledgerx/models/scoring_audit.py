"""Portfolio-level audit outputs for scoring reconciliation."""

from __future__ import annotations

from pathlib import Path
import json
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import output_path, processed_data_path


def build_scoring_disagreement_segments(
    *,
    reconciliation: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build a portfolio-level segmentation table for scoring disagreement patterns."""

    if reconciliation is None:
        reconciliation = _read_processed_table("company_scoring_reconciliation.parquet")

    segments = (
        reconciliation.groupby(
            ["scoring_alignment_label", "reconciliation_status"],
            as_index=False,
        )
        .agg(
            count=("company_id", "count"),
            avg_heuristic_score=("target_miss_risk_score", "mean"),
            avg_calibrated_probability=("calibrated_miss_probability", "mean"),
            avg_target_gap_pct=("target_gap_pct", "mean"),
            avg_contradiction_count=("contradiction_count", "mean"),
            avg_renewable_share=("renewable_share_pct", "mean"),
            avg_fleet_electrification=("fleet_electrification_pct", "mean"),
            avg_implied_reduction_pct=("implied_reduction_pct", "mean"),
        )
        .sort_values(["count", "scoring_alignment_label"], ascending=[False, True], kind="stable")
        .reset_index(drop=True)
        .convert_dtypes()
    )

    selected_key_fields = [
        "scoring_alignment_label",
        "reconciliation_status",
        "count",
        "avg_heuristic_score",
        "avg_calibrated_probability",
        "avg_target_gap_pct",
    ]
    assumptions = [
        "Segments are grouped jointly by alignment label and reconciliation status so the audit view can separate clean agreement from operational mismatch categories.",
        "Average calibrated probability remains on the native 0 to 1 scale rather than being converted into a 0 to 100 score.",
        "The segment summary is descriptive only; it is meant to help understand disagreement patterns rather than define new company-level labels.",
    ]
    return ProcessedTableArtifact(
        output_name="scoring_disagreement_segments.parquet",
        dataframe=segments,
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=["company_scoring_reconciliation.parquet"],
    )


def build_scoring_reconciliation_report_markdown(
    *,
    reconciliation: pd.DataFrame,
    segments: pd.DataFrame,
) -> str:
    """Render a markdown report summarizing alignment and disagreement patterns."""

    company_count = int(reconciliation["company_id"].nunique())
    alignment_counts = reconciliation["scoring_alignment_label"].value_counts().to_dict()
    status_counts = reconciliation["reconciliation_status"].value_counts().to_dict()
    primary_reason_counts = reconciliation["disagreement_reason_primary"].value_counts().head(5).to_dict()

    strong_agreement_pct = (
        float((reconciliation["scoring_alignment_label"] == "strong_agreement").mean() * 100.0)
        if company_count > 0
        else 0.0
    )
    investigation_pct = (
        float(reconciliation["reconciliation_status"].isin(["investigate_label_definition", "mixed_signal_case"]).mean() * 100.0)
        if company_count > 0
        else 0.0
    )
    product_recommendation = recommended_primary_ui_score(reconciliation)

    top_reason_lines = [
        f"- `{reason}`: {count}"
        for reason, count in primary_reason_counts.items()
    ]
    alignment_lines = [
        f"- `{label}`: {count}"
        for label, count in alignment_counts.items()
    ]
    status_lines = [
        f"- `{label}`: {count}"
        for label, count in status_counts.items()
    ]

    largest_segment = segments.iloc[0] if not segments.empty else None
    largest_segment_line = "- No disagreement segments available."
    if largest_segment is not None:
        largest_segment_line = (
            f"- Largest segment: `{largest_segment['scoring_alignment_label']}` / "
            f"`{largest_segment['reconciliation_status']}` with {int(largest_segment['count'])} companies."
        )

    lines = [
        "# CarbonLedgerX Scoring Reconciliation Report",
        "",
        "## Portfolio Summary",
        f"- Companies audited: {company_count}",
        f"- Strong-agreement proportion: {strong_agreement_pct:.1f}%",
        f"- Proportion requiring investigation: {investigation_pct:.1f}%",
        largest_segment_line,
        "",
        "## Alignment Summary",
        *alignment_lines,
        "",
        "## Reconciliation Status Summary",
        *status_lines,
        "",
        "## Top Disagreement Reasons",
        *top_reason_lines,
        "",
        "## Product Recommendation",
        f"- Recommended primary product view: {product_recommendation}",
        "- Show the calibrated miss probability as the core numeric metric when users expand detail, and keep the raw heuristic score as an audit diagnostic rather than the headline status.",
        "",
        "## Notes",
        "- This audit layer is deterministic and operational; it does not replace either the heuristic scoring layer or the probabilistic model outputs.",
        "- Reconciliation statuses are intended to help product surfaces and analyst workflows choose a stable default signal in disagreement cases.",
    ]
    return "\n".join(lines) + "\n"


def recommended_primary_ui_score(reconciliation: pd.DataFrame) -> str:
    """Recommend the primary score view for final product surfaces."""

    strong_agreement_pct = float((reconciliation["scoring_alignment_label"] == "strong_agreement").mean() * 100.0)
    major_disagreement_pct = float((reconciliation["scoring_alignment_label"] == "major_disagreement").mean() * 100.0)

    if strong_agreement_pct >= 60.0 and major_disagreement_pct >= 10.0:
        return (
            "`recommended_operational_risk_band` should be the primary headline status, "
            "with calibrated miss probability shown as the primary numeric drill-down metric."
        )
    return (
        "`calibrated_miss_probability` can be primary, but the reconciled operational band should remain visible whenever heuristic disagreement is material."
    )


def write_scoring_agreement_plot(
    *,
    reconciliation: pd.DataFrame,
    output_file_path: str | Path | None = None,
) -> Path:
    """Write a visual comparing heuristic score and calibrated probability."""

    if output_file_path is None:
        output_file_path = output_path("evaluation", "scoring_agreement_plot.png")

    resolved_output_path = Path(output_file_path).resolve()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)

    status_color_map = {
        "aligned": "#4c78a8",
        "heuristic_overcalling_candidate": "#f58518",
        "probability_overcalling_candidate": "#54a24b",
        "investigate_label_definition": "#e45756",
        "mixed_signal_case": "#b279a2",
    }

    figure, axes = plt.subplots(1, 2, figsize=(13.5, 5.2))
    plotting_frame = reconciliation.copy()
    plotting_frame["probability_score_0_100"] = plotting_frame["calibrated_miss_probability"] * 100.0

    for status, status_frame in plotting_frame.groupby("reconciliation_status", sort=True):
        axes[0].scatter(
            status_frame["target_miss_risk_score"],
            status_frame["probability_score_0_100"],
            s=22,
            alpha=0.75,
            label=status,
            color=status_color_map.get(status, "#666666"),
        )
    axes[0].plot([0, 100], [0, 100], linestyle="--", color="black", linewidth=1)
    axes[0].set_title("Heuristic Score vs Calibrated Probability")
    axes[0].set_xlabel("Heuristic risk score (0-100)")
    axes[0].set_ylabel("Calibrated miss probability (0-100)")
    axes[0].legend(frameon=False, fontsize=8)

    status_counts = plotting_frame["reconciliation_status"].value_counts()
    axes[1].bar(
        status_counts.index.tolist(),
        status_counts.tolist(),
        color=[status_color_map.get(status, "#666666") for status in status_counts.index.tolist()],
    )
    axes[1].set_title("Reconciliation Status Counts")
    axes[1].set_ylabel("Companies")
    axes[1].tick_params(axis="x", rotation=20)

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


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
