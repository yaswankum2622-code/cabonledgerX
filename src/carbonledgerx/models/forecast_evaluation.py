"""Evaluation artifacts for statistical forecast outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from carbonledgerx.models.backtesting import MODEL_NAMES, NOMINAL_INTERVAL_COVERAGE_PCT, select_best_models
from carbonledgerx.models.canonical_tables import ProcessedTableArtifact
from carbonledgerx.utils.paths import output_path, processed_data_path


def build_company_forecast_summary(
    *,
    backtest_results: pd.DataFrame | None = None,
    statistical_forecast: pd.DataFrame | None = None,
    history_annual: pd.DataFrame | None = None,
) -> ProcessedTableArtifact:
    """Build one company-level summary of selected model quality and 2030 outlook."""

    if backtest_results is None:
        backtest_results = _read_processed_table("company_forecast_backtest_results.parquet")
    if statistical_forecast is None:
        statistical_forecast = _read_processed_table("company_emissions_forecast_statistical.parquet")
    if history_annual is None:
        history_annual = _read_processed_table("company_emissions_history_annual.parquet")

    selected_models = select_best_models(backtest_results)
    latest_actuals = (
        history_annual.sort_values(["company_id", "history_year"], kind="stable")
        .groupby("company_id", as_index=False)
        .last()
        .loc[:, ["company_id", "company_name", "sector", "country", "history_year", "total_mb_tco2e"]]
        .rename(
            columns={
                "history_year": "latest_actual_year",
                "total_mb_tco2e": "latest_actual_total_mb_tco2e",
            }
        )
    )
    forecast_2030 = statistical_forecast.loc[
        statistical_forecast["forecast_year"] == 2030,
        ["company_id", "forecast_total_mb_tco2e"],
    ].rename(columns={"forecast_total_mb_tco2e": "forecast_2030_total_mb_tco2e"})

    summary = selected_models.merge(
        latest_actuals,
        on=["company_id", "company_name", "sector", "country"],
        how="left",
        validate="one_to_one",
    ).merge(
        forecast_2030,
        on="company_id",
        how="left",
        validate="one_to_one",
    )
    summary["forecast_direction_label"] = summary.apply(_forecast_direction_label, axis=1)
    summary["model_selection_notes"] = summary.apply(_model_selection_note, axis=1)

    selected_columns = [
        "company_id",
        "company_name",
        "sector",
        "country",
        "selected_model_name",
        "mean_ape_pct",
        "mean_abs_error",
        "interval_coverage_pct",
        "latest_actual_year",
        "latest_actual_total_mb_tco2e",
        "forecast_2030_total_mb_tco2e",
        "forecast_direction_label",
        "model_selection_notes",
    ]
    summary = summary.loc[:, selected_columns].copy().convert_dtypes()

    selected_key_fields = [
        "company_id",
        "selected_model_name",
        "mean_ape_pct",
        "mean_abs_error",
        "interval_coverage_pct",
        "latest_actual_total_mb_tco2e",
        "forecast_2030_total_mb_tco2e",
        "forecast_direction_label",
    ]
    assumptions = [
        "Each company summary row is based on the selected model from walk-forward backtests rather than the deterministic forecast layer.",
        "The 2030 outlook compares the selected-model statistical forecast against the latest available reconstructed annual history point.",
        "Direction labels are conservative: changes within +/-2% between latest actual and 2030 forecast are classified as flat.",
    ]
    return ProcessedTableArtifact(
        output_name="company_forecast_summary.parquet",
        dataframe=summary.reset_index(drop=True),
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[
            "company_forecast_backtest_results.parquet",
            "company_emissions_forecast_statistical.parquet",
            "company_emissions_history_annual.parquet",
        ],
    )


def build_forecast_metrics_payload(
    *,
    backtest_results: pd.DataFrame,
    forecast_summary: pd.DataFrame,
    statistical_forecast: pd.DataFrame,
) -> dict[str, Any]:
    """Build portfolio-level aggregate forecast metrics."""

    selected_model_rows = _selected_backtest_rows(backtest_results=backtest_results, forecast_summary=forecast_summary)
    model_metrics_by_name = (
        backtest_results.groupby("model_name", as_index=False)
        .agg(
            mean_ape_pct=("ape_pct", "mean"),
            mean_abs_error=("abs_error", "mean"),
            interval_coverage_pct=("within_interval_flag", lambda values: float(pd.Series(values).mean() * 100.0)),
        )
        .sort_values("model_name", kind="stable")
    )
    model_metrics_dict = {
        row["model_name"]: {
            "mean_ape_pct": round(float(row["mean_ape_pct"]), 3),
            "mean_abs_error": round(float(row["mean_abs_error"]), 3),
            "interval_coverage_pct": round(float(row["interval_coverage_pct"]), 3),
        }
        for _, row in model_metrics_by_name.iterrows()
    }

    return {
        "company_count": int(forecast_summary["company_id"].nunique()),
        "backtest_row_count": int(backtest_results.shape[0]),
        "forecast_row_count": int(statistical_forecast.shape[0]),
        "models_evaluated": MODEL_NAMES,
        "selected_model_win_counts": {
            model_name: int(count)
            for model_name, count in forecast_summary["selected_model_name"].value_counts().to_dict().items()
        },
        "aggregate_selected_mean_ape_pct": round(float(forecast_summary["mean_ape_pct"].mean()), 3),
        "aggregate_selected_mean_abs_error": round(float(forecast_summary["mean_abs_error"].mean()), 3),
        "aggregate_selected_interval_coverage_pct": round(float(forecast_summary["interval_coverage_pct"].mean()), 3),
        "selected_model_backtest_mean_ape_pct": round(float(selected_model_rows["ape_pct"].mean()), 3),
        "selected_model_backtest_median_ape_pct": round(float(selected_model_rows["ape_pct"].median()), 3),
        "selected_model_backtest_mean_abs_error": round(float(selected_model_rows["abs_error"].mean()), 3),
        "selected_model_backtest_interval_coverage_pct": round(
            float(selected_model_rows["within_interval_flag"].mean() * 100.0),
            3,
        ),
        "model_metrics_by_name": model_metrics_dict,
    }


def build_calibration_summary_payload(
    *,
    backtest_results: pd.DataFrame,
    forecast_summary: pd.DataFrame,
) -> dict[str, Any]:
    """Build an interval-coverage calibration summary."""

    coverage_by_model = (
        backtest_results.groupby("model_name")["within_interval_flag"]
        .mean()
        .mul(100.0)
        .round(3)
        .to_dict()
    )
    overall_coverage_pct = float(backtest_results["within_interval_flag"].mean() * 100.0)
    selected_rows = _selected_backtest_rows(backtest_results=backtest_results, forecast_summary=forecast_summary)
    selected_coverage_pct = float(selected_rows["within_interval_flag"].mean() * 100.0)
    coverage_gap = overall_coverage_pct - NOMINAL_INTERVAL_COVERAGE_PCT

    if coverage_gap < -10:
        calibration_label = "under_covering"
    elif coverage_gap > 10:
        calibration_label = "over_covering"
    else:
        calibration_label = "near_nominal"

    return {
        "nominal_interval_coverage_pct": NOMINAL_INTERVAL_COVERAGE_PCT,
        "overall_interval_coverage_pct": round(overall_coverage_pct, 3),
        "selected_model_interval_coverage_pct": round(selected_coverage_pct, 3),
        "coverage_gap_vs_nominal_pct": round(coverage_gap, 3),
        "calibration_label": calibration_label,
        "coverage_by_model": {
            model_name: round(float(coverage_pct), 3)
            for model_name, coverage_pct in coverage_by_model.items()
        },
        "backtest_observation_count": int(backtest_results.shape[0]),
    }


def build_backtest_report_markdown(
    *,
    backtest_results: pd.DataFrame,
    forecast_summary: pd.DataFrame,
    forecast_metrics: dict[str, Any],
    calibration_summary: dict[str, Any],
) -> str:
    """Render a concise markdown backtest report."""

    selected_model_counts = forecast_metrics["selected_model_win_counts"]
    top_companies = (
        forecast_summary.sort_values("mean_ape_pct", ascending=False, kind="stable")
        .head(5)
        .loc[:, ["company_id", "company_name", "selected_model_name", "mean_ape_pct"]]
    )
    top_company_lines = [
        f"- `{row.company_id}` {row.company_name}: {row.selected_model_name} with mean APE {float(row.mean_ape_pct):.2f}%"
        for row in top_companies.itertuples(index=False)
    ]

    report_lines = [
        "# CarbonLedgerX Statistical Forecast Backtest Report",
        "",
        "## Scope",
        f"- Companies evaluated: {forecast_metrics['company_count']}",
        f"- Walk-forward windows: {min(backtest_results['actual_year'])}-{max(backtest_results['actual_year'])}",
        f"- Models compared: {', '.join(forecast_metrics['models_evaluated'])}",
        "",
        "## Aggregate Metrics",
        f"- Selected-model mean APE: {forecast_metrics['selected_model_backtest_mean_ape_pct']:.2f}%",
        f"- Selected-model median APE: {forecast_metrics['selected_model_backtest_median_ape_pct']:.2f}%",
        f"- Selected-model mean absolute error: {forecast_metrics['selected_model_backtest_mean_abs_error']:.2f} tCO2e",
        f"- Selected-model interval coverage: {forecast_metrics['selected_model_backtest_interval_coverage_pct']:.2f}%",
        "",
        "## Model Selection",
        f"- Naive wins: {selected_model_counts.get('naive_last_value', 0)}",
        f"- Linear trend wins: {selected_model_counts.get('linear_trend', 0)}",
        "- Selection is based on mean walk-forward APE with mean absolute error as the tiebreaker.",
        "",
        "## Calibration",
        f"- Nominal interval coverage target: {calibration_summary['nominal_interval_coverage_pct']:.1f}%",
        f"- Observed overall coverage: {calibration_summary['overall_interval_coverage_pct']:.2f}%",
        f"- Calibration label: {calibration_summary['calibration_label']}",
        "",
        "## Highest Error Companies",
        *top_company_lines,
        "",
        "## Notes",
        "- Forecast intervals are heuristic and backtest-derived; they are intended to be honest uncertainty bands rather than formal probabilistic confidence intervals.",
        "- The forecast layer is additive and does not replace the deterministic forecast outputs already present in the project.",
    ]
    return "\n".join(report_lines) + "\n"


def write_forecast_metric_plots(
    *,
    backtest_results: pd.DataFrame,
    forecast_summary: pd.DataFrame,
    output_file_path: str | Path | None = None,
) -> Path:
    """Write a simple PNG containing forecast evaluation plots."""

    if output_file_path is None:
        output_file_path = output_path("evaluation", "forecast_metric_plots.png")

    resolved_output_path = Path(output_file_path).resolve()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)

    coverage_by_model = (
        backtest_results.groupby("model_name")["within_interval_flag"]
        .mean()
        .mul(100.0)
        .reindex(MODEL_NAMES)
        .fillna(0.0)
    )
    win_counts = (
        forecast_summary["selected_model_name"]
        .value_counts()
        .reindex(MODEL_NAMES)
        .fillna(0)
    )

    figure, axes = plt.subplots(1, 3, figsize=(16, 4.8))

    for model_name in MODEL_NAMES:
        model_ape = pd.to_numeric(
            backtest_results.loc[backtest_results["model_name"] == model_name, "ape_pct"],
            errors="coerce",
        ).dropna()
        axes[0].hist(model_ape, bins=20, alpha=0.6, label=model_name)
    axes[0].set_title("APE Distribution")
    axes[0].set_xlabel("APE %")
    axes[0].set_ylabel("Backtest rows")
    axes[0].legend(frameon=False, fontsize=8)

    axes[1].bar(win_counts.index.tolist(), win_counts.tolist(), color=["#4c78a8", "#f58518"])
    axes[1].set_title("Selected Model Wins")
    axes[1].set_ylabel("Companies")
    axes[1].tick_params(axis="x", rotation=20)

    axes[2].bar(coverage_by_model.index.tolist(), coverage_by_model.tolist(), color=["#72b7b2", "#e45756"])
    axes[2].axhline(NOMINAL_INTERVAL_COVERAGE_PCT, color="black", linestyle="--", linewidth=1)
    axes[2].set_title("Interval Coverage by Model")
    axes[2].set_ylabel("Coverage %")
    axes[2].tick_params(axis="x", rotation=20)

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


def _selected_backtest_rows(
    *,
    backtest_results: pd.DataFrame,
    forecast_summary: pd.DataFrame,
) -> pd.DataFrame:
    """Return only backtest rows corresponding to each company's selected model."""

    selected_pairs = forecast_summary.loc[:, ["company_id", "selected_model_name"]].rename(
        columns={"selected_model_name": "model_name"}
    )
    return backtest_results.merge(
        selected_pairs,
        on=["company_id", "model_name"],
        how="inner",
        validate="many_to_one",
    )


def _forecast_direction_label(summary_row: pd.Series) -> str:
    """Label 2030 forecast direction versus the latest actual value."""

    latest_actual = pd.to_numeric(summary_row["latest_actual_total_mb_tco2e"], errors="coerce")
    forecast_2030 = pd.to_numeric(summary_row["forecast_2030_total_mb_tco2e"], errors="coerce")
    if pd.isna(latest_actual) or latest_actual <= 0 or pd.isna(forecast_2030):
        return "flat"

    change_pct = ((forecast_2030 - latest_actual) / latest_actual) * 100.0
    if change_pct >= 2.0:
        return "rising"
    if change_pct <= -2.0:
        return "declining"
    return "flat"


def _model_selection_note(summary_row: pd.Series) -> str:
    """Build a concise model-selection note."""

    return (
        f"Selected {summary_row['selected_model_name']} from walk-forward backtests "
        f"with mean APE {float(summary_row['mean_ape_pct']):.2f}% and interval coverage "
        f"{float(summary_row['interval_coverage_pct']):.1f}%."
    )


def _read_processed_table(file_name: str) -> pd.DataFrame:
    """Read one processed parquet table."""

    return pd.read_parquet(processed_data_path(file_name))
