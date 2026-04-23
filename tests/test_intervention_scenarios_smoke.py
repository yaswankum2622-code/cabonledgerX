"""Smoke tests for intervention scenarios, MAC rankings, and intelligence outputs."""

from __future__ import annotations

import json

import pandas as pd

from carbonledgerx.data.processed_writer import write_processed_dataframe, write_processed_manifest
from carbonledgerx.models.intervention_simulator import build_company_intervention_scenarios
from carbonledgerx.models.mac_ranking import (
    build_company_intervention_intelligence,
    build_company_mac_rankings,
)
from carbonledgerx.utils.paths import output_path, processed_data_path


def test_intervention_outputs_build_successfully() -> None:
    """Intervention scenario, MAC ranking, and intelligence outputs should build and write."""

    scenario_artifact = build_company_intervention_scenarios()
    mac_artifact = build_company_mac_rankings(
        intervention_scenarios=scenario_artifact.dataframe,
    )
    intelligence_artifact = build_company_intervention_intelligence(
        mac_rankings=mac_artifact.dataframe,
        intervention_scenarios=scenario_artifact.dataframe,
    )
    artifacts = [scenario_artifact, mac_artifact, intelligence_artifact]

    manifest_entries: list[dict[str, object]] = []
    for artifact in artifacts:
        parquet_path = write_processed_dataframe(
            artifact.dataframe,
            processed_data_path(artifact.output_name),
        )
        manifest_entries.append(artifact.manifest_entry(parquet_path))

    manifest_path = write_processed_manifest(
        {"output_count": len(manifest_entries), "outputs": manifest_entries},
        output_path("intervention_manifest.json"),
    )

    scenario_path = processed_data_path("company_intervention_scenarios.parquet")
    mac_path = processed_data_path("company_mac_rankings.parquet")
    intelligence_path = processed_data_path("company_intervention_intelligence.parquet")
    assert scenario_path.exists()
    assert mac_path.exists()
    assert intelligence_path.exists()

    scenario_df = pd.read_parquet(scenario_path)
    mac_df = pd.read_parquet(mac_path)
    intelligence_df = pd.read_parquet(intelligence_path)

    assert scenario_df.shape[0] > 0
    assert mac_df.shape[0] > 0
    assert intelligence_df.shape[0] > 0

    interventions_per_company = scenario_df.groupby("company_id")["intervention_name"].nunique()
    assert int(interventions_per_company.min()) >= 3

    assert "mac_rank" in mac_df.columns
    assert "best_intervention_name" in intelligence_df.columns
    assert "best_intervention_cost_per_tco2e" in intelligence_df.columns
    assert "best_intervention_abatement_tco2e" in intelligence_df.columns

    assert manifest_path.exists()
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_count"] == 3
