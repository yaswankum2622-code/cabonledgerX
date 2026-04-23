"""Smoke tests for deterministic evidence-pack generation."""

from __future__ import annotations

import json

import pandas as pd

from carbonledgerx.data.processed_writer import write_processed_dataframe, write_processed_manifest
from carbonledgerx.models.evidence_pack import generate_company_evidence_packs
from carbonledgerx.utils.paths import output_path, processed_data_path


def test_evidence_packs_build_successfully() -> None:
    """Evidence markdown files, index parquet, and manifest should build successfully."""

    build_result = generate_company_evidence_packs()

    index_path = write_processed_dataframe(
        build_result.index_artifact.dataframe,
        processed_data_path(build_result.index_artifact.output_name),
    )
    manifest_path = write_processed_manifest(
        {
            "selected_company_count": len(build_result.selected_company_ids),
            "selected_company_ids": build_result.selected_company_ids,
            "markdown_file_count": len(build_result.generated_files),
            "generated_files": [str(path) for path in build_result.generated_files],
            "index_output": build_result.index_artifact.manifest_entry(index_path),
        },
        output_path("evidence_manifest.json"),
    )

    assert index_path.exists()
    assert manifest_path.exists()

    index_df = pd.read_parquet(index_path)
    assert index_df.shape[0] > 0
    assert "evidence_generated_flag" in index_df.columns

    selected_rows = index_df.loc[index_df["evidence_generated_flag"] == True].copy()
    assert selected_rows.shape[0] > 0
    assert len(build_result.generated_files) == int(selected_rows.shape[0] * 3)

    board_path = selected_rows.iloc[0]["board_brief_path"]
    investor_path = selected_rows.iloc[0]["investor_memo_path"]
    lender_path = selected_rows.iloc[0]["lender_note_path"]

    for markdown_path in [board_path, investor_path, lender_path]:
        assert markdown_path is not None
        path_text = str(markdown_path)
        with open(path_text, encoding="utf-8") as handle:
            content = handle.read()
        assert "## Company Snapshot" in content or "## Borrower Snapshot" in content
        assert "## Emissions And Target Position" in content or "## Transition Position" in content
        assert "## Recommendation" in content

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["selected_company_count"] == len(build_result.selected_company_ids)
