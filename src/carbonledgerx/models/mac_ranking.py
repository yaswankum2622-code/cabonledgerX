"""MAC-style ranking and intervention intelligence assembly."""

from __future__ import annotations

from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


def build_company_mac_rankings(
    *,
    intervention_scenarios: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build a company-by-intervention MAC-style ranking table."""

    if intervention_scenarios is None:
        intervention_scenarios = _read_processed_table("company_intervention_scenarios.parquet")

    rankings = intervention_scenarios.copy()
    rankings["feasibility_score"] = rankings.apply(_feasibility_score, axis=1)
    rankings["mac_rank"] = rankings.groupby("company_id")["cost_per_tco2e"].rank(
        method="dense",
        ascending=True,
    )
    rankings["abatement_rank"] = rankings.groupby("company_id")["modeled_abatement_tco2e"].rank(
        method="dense",
        ascending=False,
    )
    rankings["feasibility_rank"] = rankings.groupby("company_id")["feasibility_score"].rank(
        method="dense",
        ascending=False,
    )
    rankings["priority_score"] = rankings.groupby(
        "company_id",
        group_keys=False,
    )[
        [
            "mac_rank",
            "abatement_rank",
            "feasibility_rank",
            "closes_gap_flag",
            "partially_closes_gap_flag",
        ]
    ].apply(_priority_score_frame)
    rankings["priority_rank"] = rankings.groupby("company_id")["priority_score"].rank(
        method="dense",
        ascending=False,
    )
    rankings["recommended_priority_flag"] = rankings["priority_rank"] <= 2

    selected_columns = [
        "company_id",
        "company_name",
        "sector",
        "country",
        "intervention_name",
        "intervention_category",
        "modeled_abatement_tco2e",
        "modeled_cost_usd_m",
        "cost_per_tco2e",
        "mac_rank",
        "abatement_rank",
        "feasibility_rank",
        "feasibility_score",
        "priority_score",
        "priority_rank",
        "recommended_priority_flag",
        "closes_gap_flag",
        "partially_closes_gap_flag",
    ]
    rankings = rankings.loc[:, [column for column in selected_columns if column in rankings.columns]].copy()

    selected_key_fields = [
        "company_id",
        "intervention_name",
        "cost_per_tco2e",
        "mac_rank",
        "abatement_rank",
        "recommended_priority_flag",
    ]
    assumptions = [
        "MAC-style ranking prefers lower cost_per_tco2e, while abatement_rank prefers higher modeled abatement and feasibility_rank prefers easier, earlier, and lower-cost actions.",
        "recommended_priority_flag marks the top two composite actions per company using a weighted mix of MAC, abatement, and feasibility ranks plus a small gap-closure bonus.",
        "The ranking remains company-relative and deterministic rather than optimized across companies or capital budgets.",
    ]
    return ProcessedTableArtifact(
        output_name="company_mac_rankings.parquet",
        dataframe=rankings.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=["company_intervention_scenarios.parquet"],
    )


def build_company_intervention_intelligence(
    *,
    commitment_intelligence: pd.DataFrame | None = None,
    mac_rankings: pd.DataFrame | None = None,
    intervention_scenarios: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build one company-level intervention intelligence table."""

    if commitment_intelligence is None:
        commitment_intelligence = _read_processed_table("company_commitment_intelligence.parquet")
    if mac_rankings is None:
        mac_rankings = _read_processed_table("company_mac_rankings.parquet")
    if intervention_scenarios is None:
        intervention_scenarios = _read_processed_table("company_intervention_scenarios.parquet")

    ranked_actions = mac_rankings.sort_values(
        by=["company_id", "priority_rank", "cost_per_tco2e", "modeled_abatement_tco2e"],
        ascending=[True, True, True, False],
    ).copy()
    best_actions = ranked_actions.groupby("company_id", as_index=False).first()
    second_actions = (
        ranked_actions.loc[ranked_actions["priority_rank"] == 2, ["company_id", "intervention_name"]]
        .rename(columns={"intervention_name": "second_best_intervention_name"})
        .copy()
    )

    best_actions = best_actions.rename(
        columns={
            "intervention_name": "best_intervention_name",
            "cost_per_tco2e": "best_intervention_cost_per_tco2e",
            "modeled_abatement_tco2e": "best_intervention_abatement_tco2e",
            "modeled_cost_usd_m": "best_intervention_cost_usd_m",
            "closes_gap_flag": "best_intervention_closes_gap_flag",
            "partially_closes_gap_flag": "best_intervention_partially_closes_gap_flag",
        }
    )
    best_actions = best_actions.merge(
        second_actions,
        on="company_id",
        how="left",
        validate="one_to_one",
    )

    intelligence = commitment_intelligence.merge(
        best_actions,
        on="company_id",
        how="left",
        validate="one_to_one",
        suffixes=("", "_best"),
    )

    intelligence["intervention_recommendation_summary"] = intelligence.apply(
        _intervention_recommendation_summary,
        axis=1,
    )

    selected_columns = [
        "company_id",
        "company_name",
        "sector",
        "country",
        "target_year",
        "target_reduction_pct",
        "target_gap_tco2e",
        "target_miss_risk_score",
        "risk_band",
        "credibility_band",
        "best_intervention_name",
        "best_intervention_cost_per_tco2e",
        "best_intervention_cost_usd_m",
        "best_intervention_abatement_tco2e",
        "best_intervention_closes_gap_flag",
        "best_intervention_partially_closes_gap_flag",
        "intervention_recommendation_summary",
        "scoring_notes",
    ]
    intelligence = intelligence.loc[
        :,
        [column for column in selected_columns if column in intelligence.columns],
    ].copy()

    selected_key_fields = [
        "company_id",
        "target_gap_tco2e",
        "target_miss_risk_score",
        "best_intervention_name",
        "best_intervention_cost_per_tco2e",
        "best_intervention_abatement_tco2e",
    ]
    assumptions = [
        "Intervention intelligence starts from the existing company_commitment_intelligence table and adds the top-ranked intervention recommendation per company.",
        "The best action is chosen from the MAC ranking priority order rather than solely from abatement size, so low-cost high-feasibility options can win.",
        "The recommendation summary may reference the second-ranked action when the best action only partially closes the target gap.",
    ]
    return ProcessedTableArtifact(
        output_name="company_intervention_intelligence.parquet",
        dataframe=intelligence.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[
            "company_commitment_intelligence.parquet",
            "company_intervention_scenarios.parquet",
            "company_mac_rankings.parquet",
        ],
    )


def _feasibility_score(row: pd.Series) -> float:
    """Compute a simple rule-based feasibility score for one intervention row."""

    applicability_multiplier = _safe_float(row.get("sector_applicability_multiplier"), default=1.0)
    adoption_pct = _safe_float(row.get("adoption_pct"))
    start_year = int(_safe_float(row.get("start_year"), default=2026.0))
    relative_cost_bucket = str(row.get("relative_cost_bucket", "") or "")
    closes_gap_flag = bool(row.get("closes_gap_flag", False))
    partially_closes_gap_flag = bool(row.get("partially_closes_gap_flag", False))

    score = 50.0
    if applicability_multiplier >= 1.10:
        score += 20.0
    elif applicability_multiplier >= 1.00:
        score += 10.0
    elif applicability_multiplier >= 0.90:
        score += 2.0
    else:
        score -= 10.0

    if relative_cost_bucket == "low":
        score += 10.0
    elif relative_cost_bucket == "medium":
        score += 4.0
    elif relative_cost_bucket == "high":
        score -= 8.0

    if start_year <= 2026:
        score += 10.0
    elif start_year <= 2027:
        score += 5.0

    if 40.0 <= adoption_pct <= 80.0:
        score += 8.0
    elif 25.0 <= adoption_pct <= 95.0:
        score += 4.0

    if closes_gap_flag:
        score += 10.0
    elif partially_closes_gap_flag:
        score += 5.0

    return round(score, 3)


def _priority_score_frame(group: pd.DataFrame) -> pd.Series:
    """Compute a simple composite recommendation score within one company."""

    group_size = max(1, len(group))
    mac_score = group_size - group["mac_rank"] + 1
    abatement_score = group_size - group["abatement_rank"] + 1
    feasibility_score = group_size - group["feasibility_rank"] + 1
    closure_bonus = group["closes_gap_flag"].astype(int) * 1.5 + group["partially_closes_gap_flag"].astype(int) * 0.5
    return (
        (mac_score * 0.40)
        + (abatement_score * 0.35)
        + (feasibility_score * 0.25)
        + closure_bonus
    )


def _intervention_recommendation_summary(row: pd.Series) -> str:
    """Render a concise intervention recommendation summary."""

    best_name = str(row.get("best_intervention_name", "") or "")
    second_name = str(row.get("second_best_intervention_name", "") or "")
    target_gap_tco2e = _safe_float(row.get("target_gap_tco2e"))
    abatement_tco2e = _safe_float(row.get("best_intervention_abatement_tco2e"))
    cost_per_tco2e = _safe_float(row.get("best_intervention_cost_per_tco2e"))
    closes_gap_flag = bool(row.get("best_intervention_closes_gap_flag", False))
    partially_closes_gap_flag = bool(row.get("best_intervention_partially_closes_gap_flag", False))

    if not best_name:
        return "No intervention recommendation available."

    if target_gap_tco2e <= 0:
        return (
            f"Company is already on-track; {best_name} is a low-regret next action at "
            f"{cost_per_tco2e:.1f} USD/tCO2e."
        )

    if closes_gap_flag:
        return (
            f"Recommend {best_name} first; modeled abatement of {abatement_tco2e:.0f} tCO2e at "
            f"{cost_per_tco2e:.1f} USD/tCO2e is sufficient to close the current gap."
        )

    if partially_closes_gap_flag and second_name:
        return (
            f"Recommend {best_name} first, then {second_name}; the first action abates "
            f"{abatement_tco2e:.0f} tCO2e at {cost_per_tco2e:.1f} USD/tCO2e and only partially closes the gap."
        )

    return (
        f"Recommend {best_name} first; modeled abatement of {abatement_tco2e:.0f} tCO2e at "
        f"{cost_per_tco2e:.1f} USD/tCO2e improves the base case but does not close the full gap."
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
