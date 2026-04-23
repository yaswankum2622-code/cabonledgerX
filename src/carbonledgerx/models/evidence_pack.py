"""Deterministic evidence-pack markdown generation and indexing."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import output_path, processed_data_path


@dataclass(slots=True)
class EvidencePackBuildResult:
    """Container for the evidence-pack index and generated markdown metadata."""

    index_artifact: ProcessedTableArtifact
    selected_company_ids: list[str]
    generated_files: list[Path]


def generate_company_evidence_packs(
    *,
    commitment_intelligence: pd.DataFrame | None = None,
    intervention_intelligence: pd.DataFrame | None = None,
    baseline: pd.DataFrame | None = None,
    assessment: pd.DataFrame | None = None,
    evidence_output_dir: str | Path | None = None,
) -> EvidencePackBuildResult:
    """Generate deterministic markdown evidence packs and an index table."""

    if commitment_intelligence is None:
        commitment_intelligence = _read_processed_table("company_commitment_intelligence.parquet")
    if intervention_intelligence is None:
        intervention_intelligence = _read_processed_table("company_intervention_intelligence.parquet")
    if baseline is None:
        baseline = _read_processed_table("company_emissions_baseline.parquet")
    if assessment is None:
        assessment = _read_processed_table("company_commitment_assessment.parquet")

    evidence_input = _build_evidence_input(
        commitment_intelligence=commitment_intelligence,
        intervention_intelligence=intervention_intelligence,
        baseline=baseline,
        assessment=assessment,
    )
    selected_company_ids = select_evidence_company_ids(evidence_input)

    resolved_output_dir = Path(
        evidence_output_dir if evidence_output_dir is not None else output_path("evidence")
    ).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    index_dataframe = evidence_input.loc[
        :,
        [
            "company_id",
            "company_name",
            "risk_band",
            "credibility_band",
            "target_miss_risk_score",
            "commitment_credibility_score",
            "contradiction_count",
        ],
    ].copy()
    index_dataframe["evidence_generated_flag"] = False
    index_dataframe["board_brief_path"] = pd.NA
    index_dataframe["investor_memo_path"] = pd.NA
    index_dataframe["lender_note_path"] = pd.NA

    generated_files: list[Path] = []
    for company_id in selected_company_ids:
        company_row = evidence_input.loc[evidence_input["company_id"] == company_id]
        if company_row.empty:
            raise ValueError(f"Could not find evidence input row for company_id '{company_id}'.")
        company_record = company_row.iloc[0].to_dict()

        board_path = _write_markdown(
            resolved_output_dir / f"company_board_brief_{company_id}.md",
            render_board_brief(company_record),
        )
        investor_path = _write_markdown(
            resolved_output_dir / f"company_investor_memo_{company_id}.md",
            render_investor_memo(company_record),
        )
        lender_path = _write_markdown(
            resolved_output_dir / f"company_lender_note_{company_id}.md",
            render_lender_note(company_record),
        )

        generated_files.extend([board_path, investor_path, lender_path])
        row_mask = index_dataframe["company_id"] == company_id
        index_dataframe.loc[row_mask, "evidence_generated_flag"] = True
        index_dataframe.loc[row_mask, "board_brief_path"] = str(board_path)
        index_dataframe.loc[row_mask, "investor_memo_path"] = str(investor_path)
        index_dataframe.loc[row_mask, "lender_note_path"] = str(lender_path)

    selected_key_fields = [
        "company_id",
        "risk_band",
        "credibility_band",
        "target_miss_risk_score",
        "contradiction_count",
        "evidence_generated_flag",
    ]
    assumptions = [
        "Evidence companies are selected deterministically from the union of the top 10 highest-risk, top 5 lowest-risk, and top 5 highest-contradiction companies, with overlap removed in first-seen order.",
        "Markdown outputs are generated from existing processed fields only; no LLM text generation or free-form inference is used.",
        "The evidence index retains all companies and marks only the selected subset with generated markdown paths.",
    ]
    index_artifact = ProcessedTableArtifact(
        output_name="company_evidence_pack_index.parquet",
        dataframe=index_dataframe.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[
            "company_commitment_intelligence.parquet",
            "company_intervention_intelligence.parquet",
            "company_emissions_baseline.parquet",
            "company_commitment_assessment.parquet",
        ],
    )
    return EvidencePackBuildResult(
        index_artifact=index_artifact,
        selected_company_ids=selected_company_ids,
        generated_files=generated_files,
    )


def select_evidence_company_ids(evidence_input: pd.DataFrame) -> list[str]:
    """Select the deterministic company sample used for evidence packs."""

    highest_risk = evidence_input.sort_values(
        by=["target_miss_risk_score", "contradiction_count", "company_id"],
        ascending=[False, False, True],
    ).head(10)
    lowest_risk = evidence_input.sort_values(
        by=["target_miss_risk_score", "commitment_credibility_score", "company_id"],
        ascending=[True, False, True],
    ).head(5)
    strongest_contradictions = evidence_input.sort_values(
        by=["contradiction_count", "target_miss_risk_score", "company_id"],
        ascending=[False, False, True],
    ).head(5)

    selected = pd.concat(
        [highest_risk, lowest_risk, strongest_contradictions],
        ignore_index=True,
    )
    selected = selected.drop_duplicates(subset=["company_id"], keep="first")
    return selected["company_id"].astype(str).tolist()


def render_board_brief(company_record: dict[str, Any]) -> str:
    """Render the board-brief markdown template."""

    return "\n".join(
        [
            f"# Board Brief: {company_record['company_name']} ({company_record['company_id']})",
            "",
            "## Company Snapshot",
            f"- Sector: {company_record['sector']}",
            f"- Country: {company_record['country']}",
            f"- Baseline year: {_fmt_int(company_record.get('baseline_year'))}",
            f"- Assessment year: {_fmt_int(company_record.get('assessment_year'))}",
            "",
            "## Emissions And Target Position",
            f"- Target year: {_fmt_int(company_record.get('target_year'))}",
            f"- Target reduction: {_fmt_pct(company_record.get('target_reduction_pct'))}",
            f"- Baseline emissions (market-based): {_fmt_tco2e(company_record.get('baseline_total_mb_tco2e'))}",
            f"- Projected emissions at assessment year: {_fmt_tco2e(company_record.get('projected_total_mb_tco2e'))}",
            f"- Implied reduction: {_fmt_pct(company_record.get('implied_reduction_pct'))}",
            f"- Target gap: {_fmt_pct(company_record.get('target_gap_pct'))}",
            f"- Target gap volume: {_fmt_tco2e(company_record.get('target_gap_tco2e'))}",
            "",
            "## Risk And Contradictions",
            f"- Target miss risk score: {_fmt_score(company_record.get('target_miss_risk_score'))} ({company_record['risk_band']})",
            f"- Commitment credibility score: {_fmt_score(company_record.get('commitment_credibility_score'))} ({company_record['credibility_band']})",
            f"- Contradiction count: {_fmt_int(company_record.get('contradiction_count'))}",
            f"- Contradiction summary: {company_record['contradiction_summary']}",
            "",
            "## Intervention View",
            f"- Best intervention: {company_record['best_intervention_name']}",
            f"- Modeled abatement: {_fmt_tco2e(company_record.get('best_intervention_abatement_tco2e'))}",
            f"- Cost per tCO2e: {_fmt_currency(company_record.get('best_intervention_cost_per_tco2e'))} per tCO2e",
            f"- Gap closure status: {_gap_closure_text(company_record)}",
            "",
            "## Recommendation",
            *_board_recommendation_lines(company_record),
            "",
        ]
    )


def render_investor_memo(company_record: dict[str, Any]) -> str:
    """Render the investor-memo markdown template."""

    return "\n".join(
        [
            f"# Investor Memo: {company_record['company_name']} ({company_record['company_id']})",
            "",
            "## Company Snapshot",
            f"- Sector: {company_record['sector']}",
            f"- Country: {company_record['country']}",
            f"- Target year: {_fmt_int(company_record.get('target_year'))}",
            f"- Target reduction: {_fmt_pct(company_record.get('target_reduction_pct'))}",
            "",
            "## Emissions And Target Position",
            f"- Baseline emissions (market-based): {_fmt_tco2e(company_record.get('baseline_total_mb_tco2e'))}",
            f"- Projected emissions at assessment year {_fmt_int(company_record.get('assessment_year'))}: {_fmt_tco2e(company_record.get('projected_total_mb_tco2e'))}",
            f"- Implied reduction: {_fmt_pct(company_record.get('implied_reduction_pct'))}",
            f"- Target gap: {_fmt_pct(company_record.get('target_gap_pct'))} / {_fmt_tco2e(company_record.get('target_gap_tco2e'))}",
            "",
            "## Credibility And Challenge Points",
            f"- Target miss risk score: {_fmt_score(company_record.get('target_miss_risk_score'))} ({company_record['risk_band']})",
            f"- Commitment credibility score: {_fmt_score(company_record.get('commitment_credibility_score'))} ({company_record['credibility_band']})",
            f"- Contradiction summary: {company_record['contradiction_summary']}",
            f"- Best intervention: {company_record['best_intervention_name']} with {_gap_closure_text(company_record).lower()}",
            "",
            "## Recommendation",
            *_investor_recommendation_lines(company_record),
            "",
        ]
    )


def render_lender_note(company_record: dict[str, Any]) -> str:
    """Render the lender-note markdown template."""

    return "\n".join(
        [
            f"# Lender Note: {company_record['company_name']} ({company_record['company_id']})",
            "",
            "## Borrower Snapshot",
            f"- Sector: {company_record['sector']}",
            f"- Country: {company_record['country']}",
            f"- Assessment year: {_fmt_int(company_record.get('assessment_year'))}",
            f"- Target year / reduction: {_fmt_int(company_record.get('target_year'))} / {_fmt_pct(company_record.get('target_reduction_pct'))}",
            "",
            "## Transition Position",
            f"- Baseline emissions (market-based): {_fmt_tco2e(company_record.get('baseline_total_mb_tco2e'))}",
            f"- Projected emissions at assessment year: {_fmt_tco2e(company_record.get('projected_total_mb_tco2e'))}",
            f"- Implied reduction: {_fmt_pct(company_record.get('implied_reduction_pct'))}",
            f"- Target gap: {_fmt_pct(company_record.get('target_gap_pct'))}",
            f"- Target gap volume: {_fmt_tco2e(company_record.get('target_gap_tco2e'))}",
            "",
            "## Risk View",
            f"- Target miss risk score: {_fmt_score(company_record.get('target_miss_risk_score'))} ({company_record['risk_band']})",
            f"- Commitment credibility score: {_fmt_score(company_record.get('commitment_credibility_score'))} ({company_record['credibility_band']})",
            f"- Contradiction summary: {company_record['contradiction_summary']}",
            f"- Best intervention: {company_record['best_intervention_name']} / {_gap_closure_text(company_record)}",
            "",
            "## Recommendation",
            *_lender_recommendation_lines(company_record),
            "",
        ]
    )


def _build_evidence_input(
    *,
    commitment_intelligence: pd.DataFrame,
    intervention_intelligence: pd.DataFrame,
    baseline: pd.DataFrame,
    assessment: pd.DataFrame,
) -> pd.DataFrame:
    """Build the company-level evidence input frame."""

    baseline_columns = ["company_id", "baseline_year"]
    assessment_columns = ["company_id", "assessment_year"]
    intervention_columns = [
        "company_id",
        "best_intervention_name",
        "best_intervention_cost_per_tco2e",
        "best_intervention_cost_usd_m",
        "best_intervention_abatement_tco2e",
        "best_intervention_closes_gap_flag",
        "best_intervention_partially_closes_gap_flag",
        "intervention_recommendation_summary",
    ]

    evidence_input = commitment_intelligence.copy()
    evidence_input = evidence_input.merge(
        baseline.loc[:, [column for column in baseline_columns if column in baseline.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    evidence_input = evidence_input.merge(
        assessment.loc[:, [column for column in assessment_columns if column in assessment.columns]],
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    evidence_input = evidence_input.merge(
        intervention_intelligence.loc[
            :,
            [column for column in intervention_columns if column in intervention_intelligence.columns],
        ],
        on="company_id",
        how="left",
        validate="one_to_one",
    )

    numeric_columns = [
        "baseline_year",
        "assessment_year",
        "target_year",
        "target_reduction_pct",
        "baseline_total_mb_tco2e",
        "projected_total_mb_tco2e",
        "implied_reduction_pct",
        "target_gap_pct",
        "target_gap_tco2e",
        "commitment_credibility_score",
        "target_miss_risk_score",
        "contradiction_count",
        "best_intervention_cost_per_tco2e",
        "best_intervention_cost_usd_m",
        "best_intervention_abatement_tco2e",
    ]
    for column_name in numeric_columns:
        if column_name in evidence_input.columns:
            evidence_input[column_name] = pd.to_numeric(evidence_input[column_name], errors="coerce")

    return evidence_input


def _write_markdown(path: Path, content: str) -> Path:
    """Write one markdown file to disk."""

    resolved_path = path.resolve()
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_path.write_text(content, encoding="utf-8")
    return resolved_path


def _board_recommendation_lines(company_record: dict[str, Any]) -> list[str]:
    """Return board-brief recommendation bullets."""

    return [
        f"- Require management to track delivery against the {_fmt_int(company_record.get('assessment_year'))} assessment year and the current gap of {_fmt_tco2e(company_record.get('target_gap_tco2e'))}.",
        f"- Tie board oversight to implementation milestones for {company_record['best_intervention_name']} and confirm whether it {_gap_closure_text(company_record).lower()}.",
        f"- Use the current credibility view of {_fmt_score(company_record.get('commitment_credibility_score'))} ({company_record['credibility_band']}) when reviewing target feasibility and capital planning.",
    ]


def _investor_recommendation_lines(company_record: dict[str, Any]) -> list[str]:
    """Return investor-memo recommendation bullets."""

    return [
        f"- Challenge management on the current contradiction summary: {company_record['contradiction_summary']}.",
        f"- Treat {company_record['best_intervention_name']} as the lead evidence point; it {_gap_closure_text(company_record).lower()} at {_fmt_currency(company_record.get('best_intervention_cost_per_tco2e'))} per tCO2e.",
        f"- Use the target miss risk score of {_fmt_score(company_record.get('target_miss_risk_score'))} to frame engagement on target credibility and execution milestones.",
    ]


def _lender_recommendation_lines(company_record: dict[str, Any]) -> list[str]:
    """Return lender-note recommendation bullets."""

    return [
        f"- Treat the current target gap of {_fmt_tco2e(company_record.get('target_gap_tco2e'))} as a transition-risk diligence item.",
        f"- Request implementation evidence and capital coverage for {company_record['best_intervention_name']}, which {_gap_closure_text(company_record).lower()}.",
        f"- Use the combination of risk band {company_record['risk_band']} and credibility band {company_record['credibility_band']} when sizing follow-up diligence intensity.",
    ]


def _gap_closure_text(company_record: dict[str, Any]) -> str:
    """Return a short textual description of intervention gap closure."""

    if bool(company_record.get("best_intervention_closes_gap_flag", False)):
        return "Closes the current target gap"
    if bool(company_record.get("best_intervention_partially_closes_gap_flag", False)):
        return "Partially closes the current target gap"
    return "Does not close the current target gap"


def _fmt_tco2e(value: Any) -> str:
    """Format a tCO2e-style numeric value."""

    numeric_value = _safe_float(value)
    return f"{numeric_value:,.0f} tCO2e"


def _fmt_pct(value: Any) -> str:
    """Format a percentage value."""

    numeric_value = _safe_float(value)
    return f"{numeric_value:.1f}%"


def _fmt_score(value: Any) -> str:
    """Format a score value."""

    numeric_value = _safe_float(value)
    return f"{numeric_value:.1f}"


def _fmt_currency(value: Any) -> str:
    """Format a currency-like numeric value."""

    numeric_value = _safe_float(value)
    return f"${numeric_value:,.1f}"


def _fmt_int(value: Any) -> str:
    """Format an integer-like numeric value."""

    numeric_value = _safe_float(value)
    return f"{int(round(numeric_value))}"


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    """Convert a value to float with a stable fallback."""

    converted = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(converted):
        return float(default)
    return float(converted)


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
