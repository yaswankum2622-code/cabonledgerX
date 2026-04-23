"""Smoke tests for contradiction, risk, and intelligence outputs."""

from __future__ import annotations

import json

import pandas as pd

from carbonledgerx.data.processed_writer import write_processed_dataframe, write_processed_manifest
from carbonledgerx.models.contradiction_engine import build_company_contradiction_flags
from carbonledgerx.models.risk_scoring import (
    build_company_commitment_intelligence,
    build_company_commitment_risk_scores,
)
from carbonledgerx.utils.paths import output_path, processed_data_path


def test_risk_and_contradiction_outputs_build_successfully() -> None:
    """Contradiction, scoring, and intelligence outputs should build and write successfully."""

    contradiction_artifact = build_company_contradiction_flags()
    risk_score_artifact = build_company_commitment_risk_scores(
        contradiction_flags=contradiction_artifact.dataframe,
    )
    intelligence_artifact = build_company_commitment_intelligence(
        contradiction_flags=contradiction_artifact.dataframe,
        risk_scores=risk_score_artifact.dataframe,
    )
    artifacts = [contradiction_artifact, risk_score_artifact, intelligence_artifact]

    manifest_entries: list[dict[str, object]] = []
    for artifact in artifacts:
        parquet_path = write_processed_dataframe(
            artifact.dataframe,
            processed_data_path(artifact.output_name),
        )
        manifest_entries.append(artifact.manifest_entry(parquet_path))

    manifest_path = write_processed_manifest(
        {"output_count": len(manifest_entries), "outputs": manifest_entries},
        output_path("risk_manifest.json"),
    )

    contradiction_path = processed_data_path("company_contradiction_flags.parquet")
    scoring_path = processed_data_path("company_commitment_risk_scores.parquet")
    intelligence_path = processed_data_path("company_commitment_intelligence.parquet")
    assert contradiction_path.exists()
    assert scoring_path.exists()
    assert intelligence_path.exists()

    contradiction_df = pd.read_parquet(contradiction_path)
    scoring_df = pd.read_parquet(scoring_path)
    intelligence_df = pd.read_parquet(intelligence_path)

    assert contradiction_df.shape[0] > 0
    assert scoring_df.shape[0] > 0
    assert intelligence_df.shape[0] > 0

    assert "contradiction_count" in contradiction_df.columns
    assert scoring_df["target_miss_risk_score"].between(0, 100).all()
    assert scoring_df["commitment_credibility_score"].between(0, 100).all()
    assert "risk_band" in intelligence_df.columns
    assert "credibility_band" in intelligence_df.columns

    assert manifest_path.exists()
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_count"] == 3
