"""Company-level summary outputs for reconstructed annual emissions history."""

from __future__ import annotations

from typing import Any

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import processed_data_path


def build_company_history_summary(
    *,
    history_annual: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build one summary row per company from the annual history table."""

    if history_annual is None:
        history_annual = _read_processed_table("company_emissions_history_annual.parquet")

    summary_rows = [
        _build_company_summary(company_history)
        for _, company_history in history_annual.groupby("company_id", sort=True)
    ]
    summary_dataframe = pd.DataFrame(summary_rows).sort_values("company_id").reset_index(drop=True)
    summary_dataframe = summary_dataframe.convert_dtypes()

    selected_key_fields = [
        "company_id",
        "first_history_year",
        "last_history_year",
        "history_start_total_mb_tco2e",
        "history_end_total_mb_tco2e",
        "total_mb_cagr_pct",
        "total_mb_direction_label",
    ]
    assumptions = [
        "Each summary row condenses the full 2015-2024 reconstructed history for one company into directional and volatility-style metrics.",
        "The summary uses total market-based emissions to describe overall trend direction because the later commitment assessment also evaluates MB totals.",
        "Volatility is represented with a simple coefficient-of-variation proxy rather than a more complex time-series statistic.",
    ]
    return ProcessedTableArtifact(
        output_name="company_history_summary.parquet",
        dataframe=summary_dataframe,
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=["company_emissions_history_annual.parquet"],
    )


def _build_company_summary(company_history: pd.DataFrame) -> dict[str, Any]:
    """Build one company-level history summary row."""

    history = company_history.sort_values("history_year", kind="stable").reset_index(drop=True)
    start_row = history.iloc[0]
    end_row = history.iloc[-1]
    first_history_year = int(start_row["history_year"])
    last_history_year = int(end_row["history_year"])
    baseline_year = int(end_row["base_year"])
    start_total_mb_tco2e = _safe_float(start_row["total_mb_tco2e"])
    end_total_mb_tco2e = _safe_float(end_row["total_mb_tco2e"])
    year_span = max(1, last_history_year - first_history_year)

    if start_total_mb_tco2e > 0:
        total_mb_cagr_pct = (((end_total_mb_tco2e / start_total_mb_tco2e) ** (1 / year_span)) - 1.0) * 100.0
    else:
        total_mb_cagr_pct = 0.0

    total_mb_series = pd.to_numeric(history["total_mb_tco2e"], errors="coerce").fillna(0.0)
    total_mb_direction_label = _direction_label(
        start_total_mb_tco2e=start_total_mb_tco2e,
        end_total_mb_tco2e=end_total_mb_tco2e,
    )
    mean_total_mb_tco2e = float(total_mb_series.mean())
    history_volatility_proxy_pct = (
        float(total_mb_series.std(ddof=0)) / mean_total_mb_tco2e * 100.0
        if mean_total_mb_tco2e > 0
        else 0.0
    )

    return {
        "company_id": start_row["company_id"],
        "company_name": start_row["company_name"],
        "first_history_year": first_history_year,
        "last_history_year": last_history_year,
        "baseline_year": baseline_year,
        "history_start_total_mb_tco2e": round(start_total_mb_tco2e, 3),
        "history_end_total_mb_tco2e": round(end_total_mb_tco2e, 3),
        "total_mb_cagr_pct": round(total_mb_cagr_pct, 3),
        "total_mb_direction_label": total_mb_direction_label,
        "min_total_mb_tco2e": round(float(total_mb_series.min()), 3),
        "max_total_mb_tco2e": round(float(total_mb_series.max()), 3),
        "history_volatility_proxy_pct": round(history_volatility_proxy_pct, 3),
        "summary_notes": (
            f"Summary covers {first_history_year}-{last_history_year} with base-year anchor "
            f"{baseline_year}; total MB emissions are {total_mb_direction_label} over the "
            "reconstructed period."
        ),
    }


def _direction_label(*, start_total_mb_tco2e: float, end_total_mb_tco2e: float) -> str:
    """Return a simple direction label for reconstructed MB emissions."""

    if start_total_mb_tco2e <= 0:
        return "flat"

    change_ratio = (end_total_mb_tco2e - start_total_mb_tco2e) / start_total_mb_tco2e
    if change_ratio >= 0.02:
        return "rising"
    if change_ratio <= -0.02:
        return "declining"
    return "flat"


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    """Convert a value to float with a stable fallback."""

    converted = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(converted):
        return float(default)
    return float(converted)


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
