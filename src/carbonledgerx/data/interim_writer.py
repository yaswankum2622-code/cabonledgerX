"""Helpers for writing interim tables to parquet and manifest files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from carbonledgerx.data.profiling import to_jsonable


def write_dataframe_to_parquet(dataframe: pd.DataFrame, output_path: str | Path) -> Path:
    """Write a dataframe to parquet and print a concise write summary."""

    resolved_output_path = Path(output_path).resolve()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(resolved_output_path, index=False)
    print(
        f"Wrote {dataframe.shape[0]} rows x {dataframe.shape[1]} cols -> "
        f"{resolved_output_path}"
    )
    return resolved_output_path


def write_manifest(payload: dict[str, Any], output_path: str | Path) -> Path:
    """Write a JSON manifest with stable formatting."""

    resolved_output_path = Path(output_path).resolve()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_output_path.write_text(
        json.dumps(to_jsonable(payload), indent=2),
        encoding="utf-8",
    )
    print(f"Wrote manifest -> {resolved_output_path}")
    return resolved_output_path
