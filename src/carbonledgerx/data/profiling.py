"""Reusable workbook profiling helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd


JsonScalar = None | bool | int | float | str
JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


def build_workbook_profile(
    dataset_name: str,
    file_path: str | Path,
    sheet_profiles: Sequence[Mapping[str, Any]],
) -> dict[str, JsonValue]:
    """Build a JSON-serializable workbook profile."""

    serialized_sheet_profiles = [to_jsonable(dict(sheet_profile)) for sheet_profile in sheet_profiles]
    machine_friendly_sheet_profiles = sorted(
        (
            sheet_profile
            for sheet_profile in serialized_sheet_profiles
            if bool(sheet_profile.get("likely_machine_friendly"))
            and not _is_navigation_sheet_name(str(sheet_profile.get("sheet_name", "")))
        ),
        key=lambda sheet_profile: float(sheet_profile.get("machine_friendly_score", 0.0)),
        reverse=True,
    )
    likely_machine_friendly_sheets = [
        str(sheet_profile["sheet_name"])
        for sheet_profile in machine_friendly_sheet_profiles
    ]

    return {
        "dataset_name": dataset_name,
        "file_path": str(Path(file_path).resolve()),
        "sheet_count": len(serialized_sheet_profiles),
        "likely_machine_friendly_sheets": likely_machine_friendly_sheets,
        "sheet_profiles": serialized_sheet_profiles,
    }


def build_sheet_profile(
    *,
    sheet_name: str,
    dataframe: pd.DataFrame,
    header_row_number: int,
) -> dict[str, JsonValue]:
    """Build a JSON-serializable sheet profile from a loaded dataframe."""

    normalized_columns = [str(column) for column in dataframe.columns]
    named_column_fraction = _named_column_fraction(normalized_columns)
    null_fraction_top10 = build_null_fraction_top10(dataframe)
    sheet_profile: dict[str, JsonValue] = {
        "sheet_name": sheet_name,
        "header_row_number": int(header_row_number),
        "n_rows": int(dataframe.shape[0]),
        "n_cols": int(dataframe.shape[1]),
        "normalized_columns_sample": normalized_columns[:20],
        "null_fraction_top10": null_fraction_top10,
        "named_column_fraction": round(named_column_fraction, 4),
    }

    machine_friendly_score = score_machine_friendliness(sheet_profile)
    sheet_profile["machine_friendly_score"] = round(machine_friendly_score, 2)
    sheet_profile["likely_machine_friendly"] = (
        machine_friendly_score >= 4.0
        and int(sheet_profile["n_cols"]) >= 5
        and int(sheet_profile["n_rows"]) >= 5
    )

    return to_jsonable(sheet_profile)


def build_null_fraction_top10(dataframe: pd.DataFrame) -> dict[str, JsonValue]:
    """Return the top 10 columns by null fraction."""

    if dataframe.empty or dataframe.shape[1] == 0:
        return {}

    null_fraction = dataframe.isna().mean().sort_values(ascending=False).head(10)
    return {
        str(column): round(float(fraction), 4)
        for column, fraction in null_fraction.items()
    }


def score_machine_friendliness(sheet_profile: Mapping[str, Any]) -> float:
    """Score whether a sheet looks structured enough for later parser work."""

    n_rows = int(sheet_profile.get("n_rows", 0))
    n_cols = int(sheet_profile.get("n_cols", 0))
    named_column_fraction = float(sheet_profile.get("named_column_fraction", 0.0))
    normalized_columns_sample = [
        str(column_name)
        for column_name in sheet_profile.get("normalized_columns_sample", [])
    ]
    informative_sample_fraction = _named_column_fraction(normalized_columns_sample)

    score = 0.0
    if n_cols >= 5:
        score += 2.0
    elif n_cols >= 2:
        score += 0.5

    if n_rows >= 100:
        score += 2.0
    elif n_rows >= 20:
        score += 1.0
    elif n_rows >= 5:
        score += 0.5

    if named_column_fraction >= 0.8:
        score += 1.5
    elif named_column_fraction >= 0.5:
        score += 0.5

    if informative_sample_fraction >= 0.8:
        score += 1.0
    elif informative_sample_fraction >= 0.5:
        score += 0.5

    return score


def to_jsonable(value: Any) -> JsonValue:
    """Recursively convert common Python and pandas objects to JSON-safe values."""

    if value is None:
        return None
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(item) for item in value]
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if pd.isna(value):
            return None
        return value
    if hasattr(value, "isoformat") and callable(value.isoformat):
        return value.isoformat()
    if hasattr(value, "item") and callable(value.item):
        try:
            return to_jsonable(value.item())
        except (TypeError, ValueError):
            pass

    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass

    return str(value)


def _named_column_fraction(column_names: Sequence[str]) -> float:
    """Return the share of columns that look informative rather than placeholder-like."""

    if not column_names:
        return 0.0

    named_columns = [
        column_name
        for column_name in column_names
        if column_name and not str(column_name).startswith("unnamed")
    ]
    return len(named_columns) / len(column_names)


def _is_navigation_sheet_name(sheet_name: str) -> bool:
    """Return whether a sheet name looks like a navigation or cover tab."""

    lowered_name = sheet_name.strip().lower()
    navigation_markers = (
        "contents",
        "introduction",
        "front page",
        "index",
        "what's new",
    )
    return any(marker in lowered_name for marker in navigation_markers)
