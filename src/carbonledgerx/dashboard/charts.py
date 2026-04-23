"""Chart builders for the premium TargetTruth dashboard."""

from __future__ import annotations

import math

import altair as alt
import pandas as pd


HISTORY_COLOR = "#12345b"
STAT_FORECAST_COLOR = "#0f766e"
DET_FORECAST_COLOR = "#d1a23d"
TARGET_COLOR = "#d1495b"
MARKER_COLOR = "#5f7086"
SELECTED_COLOR = "#1f9d74"
INACTIVE_COLOR = "#d4dde8"


def build_trajectory_chart(
    *,
    history: pd.DataFrame,
    forecast_stat: pd.DataFrame,
    forecast_det: pd.DataFrame,
    baseline_year: int,
    baseline_value: float,
    target_year: int,
    assessment_year: int,
    target_threshold: float,
    mode: str,
) -> alt.Chart:
    """Return a history/forecast chart for the selected viewing mode."""

    history_frame = history.loc[:, ["history_year", "total_mb_tco2e"]].copy()
    history_frame = history_frame.rename(
        columns={"history_year": "year", "total_mb_tco2e": "value"}
    )
    history_frame["series"] = "Historical annual MB"

    stat_frame = forecast_stat.loc[
        :, ["forecast_year", "forecast_total_mb_tco2e", "lower_bound_total_mb_tco2e", "upper_bound_total_mb_tco2e"]
    ].copy()
    stat_frame = stat_frame.rename(
        columns={
            "forecast_year": "year",
            "forecast_total_mb_tco2e": "value",
            "lower_bound_total_mb_tco2e": "lower",
            "upper_bound_total_mb_tco2e": "upper",
        }
    )
    stat_frame["series"] = "Statistical forecast"

    det_frame = forecast_det.loc[:, ["forecast_year", "forecast_total_mb_tco2e"]].copy()
    det_frame = det_frame.rename(
        columns={"forecast_year": "year", "forecast_total_mb_tco2e": "value"}
    )
    det_frame["series"] = "Deterministic forecast"

    interval = alt.Chart(stat_frame).mark_area(
        color=STAT_FORECAST_COLOR,
        opacity=0.12,
    ).encode(
        x=alt.X("year:O", title="Year"),
        y=alt.Y("lower:Q", title="Total market-based emissions (tCO2e)"),
        y2="upper:Q",
        tooltip=[
            alt.Tooltip("year:O", title="Year"),
            alt.Tooltip("lower:Q", title="Lower bound", format=",.0f"),
            alt.Tooltip("upper:Q", title="Upper bound", format=",.0f"),
        ],
    )

    history_line = alt.Chart(history_frame).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("year:O", title="Year"),
        y=alt.Y("value:Q", title="Total market-based emissions (tCO2e)"),
        color=alt.value(HISTORY_COLOR),
        tooltip=[
            alt.Tooltip("year:O", title="Year"),
            alt.Tooltip("value:Q", title="Historical", format=",.0f"),
        ],
    )

    stat_line = alt.Chart(stat_frame).mark_line(point=True, strokeWidth=3).encode(
        x="year:O",
        y="value:Q",
        color=alt.value(STAT_FORECAST_COLOR),
        tooltip=[
            alt.Tooltip("year:O", title="Year"),
            alt.Tooltip("value:Q", title="Statistical forecast", format=",.0f"),
        ],
    )

    det_line = alt.Chart(det_frame).mark_line(point=True, strokeWidth=2.5, strokeDash=[6, 4]).encode(
        x="year:O",
        y="value:Q",
        color=alt.value(DET_FORECAST_COLOR),
        tooltip=[
            alt.Tooltip("year:O", title="Year"),
            alt.Tooltip("value:Q", title="Deterministic forecast", format=",.0f"),
        ],
    )

    baseline_marker = alt.Chart(
        pd.DataFrame([{"year": baseline_year, "value": baseline_value}])
    ).mark_point(shape="diamond", size=180, filled=True, color=HISTORY_COLOR).encode(
        x="year:O",
        y="value:Q",
        tooltip=[
            alt.Tooltip("year:O", title="Baseline year"),
            alt.Tooltip("value:Q", title="Baseline MB", format=",.0f"),
        ],
    )

    threshold_rule = alt.Chart(
        pd.DataFrame([{"target_threshold": target_threshold}])
    ).mark_rule(color=TARGET_COLOR, strokeDash=[8, 5], strokeWidth=2).encode(
        y=alt.Y("target_threshold:Q"),
        tooltip=[alt.Tooltip("target_threshold:Q", title="Target threshold", format=",.0f")],
    )

    year_marker = alt.Chart(
        pd.DataFrame(
            [{"marker_year": min(target_year, assessment_year), "target_year": target_year}]
        )
    ).mark_rule(color=MARKER_COLOR, strokeDash=[2, 4], strokeWidth=2).encode(
        x=alt.X("marker_year:O"),
        tooltip=[alt.Tooltip("target_year:Q", title="Target year")],
    )

    if mode == "Deterministic vs statistical comparison":
        layered = alt.layer(history_line, det_line, stat_line, interval, baseline_marker, threshold_rule, year_marker)
    else:
        layered = alt.layer(history_line, stat_line, interval, baseline_marker, threshold_rule, year_marker)
    return _style_chart(layered)


def build_risk_component_chart(component_frame: pd.DataFrame) -> alt.Chart:
    """Render a horizontal bar chart for risk score components."""

    chart = alt.Chart(component_frame).mark_bar(cornerRadiusEnd=5, color="#12345b").encode(
        x=alt.X("component_value:Q", title="Contribution to rule-based risk score"),
        y=alt.Y("component_label:N", sort="-x", title=""),
        tooltip=[
            alt.Tooltip("component_label:N", title="Component"),
            alt.Tooltip("component_value:Q", title="Value", format=",.1f"),
        ],
    )
    return _style_chart(chart)


def build_flag_chart(flag_frame: pd.DataFrame) -> alt.Chart:
    """Render active contradiction flags as a compact chart."""

    chart = alt.Chart(flag_frame).mark_bar(cornerRadiusEnd=6, color=TARGET_COLOR).encode(
        x=alt.X("value:Q", title=None, axis=alt.Axis(labels=False, ticks=False, domain=False)),
        y=alt.Y("flag:N", sort="-x", title=""),
        tooltip=[alt.Tooltip("flag:N", title="Active contradiction")],
    )
    return _style_chart(chart, height=max(100, 34 * len(flag_frame)))


def build_intervention_abatement_chart(interventions: pd.DataFrame) -> alt.Chart:
    """Render an abatement comparison chart."""

    chart = alt.Chart(interventions).mark_bar(cornerRadiusEnd=8).encode(
        x=alt.X("modeled_abatement_tco2e:Q", title="Modeled abatement (tCO2e)"),
        y=alt.Y("intervention_name:N", sort="-x", title=""),
        color=alt.condition(
            "datum.recommended_priority_flag == true",
            alt.value(SELECTED_COLOR),
            alt.value(INACTIVE_COLOR),
        ),
        tooltip=[
            "intervention_name",
            alt.Tooltip("modeled_abatement_tco2e:Q", title="Abatement", format=",.0f"),
            alt.Tooltip("cost_per_tco2e:Q", title="Cost / tCO2e", format=",.1f"),
        ],
    )
    return _style_chart(chart)


def build_mac_chart(interventions: pd.DataFrame) -> alt.Chart:
    """Render a MAC-style rank chart."""

    chart = alt.Chart(interventions).mark_circle(opacity=0.9).encode(
        x=alt.X("cost_per_tco2e:Q", title="Cost per tCO2e (USD)"),
        y=alt.Y("modeled_abatement_tco2e:Q", title="Modeled abatement (tCO2e)"),
        size=alt.Size("priority_score:Q", title="Priority score"),
        color=alt.condition(
            "datum.recommended_priority_flag == true",
            alt.value(STAT_FORECAST_COLOR),
            alt.value("#8ba0b8"),
        ),
        tooltip=[
            "intervention_name",
            alt.Tooltip("cost_per_tco2e:Q", title="Cost / tCO2e", format=",.1f"),
            alt.Tooltip("modeled_abatement_tco2e:Q", title="Abatement", format=",.0f"),
            alt.Tooltip("priority_rank:Q", title="Priority rank", format=".0f"),
        ],
    )
    return _style_chart(chart)


def build_cost_abatement_scatter(interventions: pd.DataFrame) -> alt.Chart:
    """Render cost versus total cost with annotation-friendly points."""

    chart = alt.Chart(interventions).mark_circle(size=160, opacity=0.9).encode(
        x=alt.X("modeled_cost_usd_m:Q", title="Modeled cost (USD m)"),
        y=alt.Y("modeled_abatement_tco2e:Q", title="Modeled abatement (tCO2e)"),
        color=alt.condition(
            "datum.recommended_priority_flag == true",
            alt.value("#0f766e"),
            alt.value("#d1a23d"),
        ),
        tooltip=[
            "intervention_name",
            alt.Tooltip("modeled_cost_usd_m:Q", title="Cost (USD m)", format=",.3f"),
            alt.Tooltip("modeled_abatement_tco2e:Q", title="Abatement", format=",.0f"),
        ],
    )
    return _style_chart(chart)


def build_distribution_chart(values: pd.Series, selected_value: float, *, title: str, color: str) -> alt.Chart:
    """Render a compact portfolio distribution with a selected-company marker."""

    distribution = pd.DataFrame({"value": pd.to_numeric(values, errors="coerce")}).dropna()
    histogram = alt.Chart(distribution).mark_bar(color=color, opacity=0.65).encode(
        x=alt.X("value:Q", bin=alt.Bin(maxbins=24), title=title),
        y=alt.Y("count():Q", title="Companies"),
        tooltip=[alt.Tooltip("count():Q", title="Companies")],
    )
    marker = alt.Chart(pd.DataFrame({"value": [selected_value]})).mark_rule(
        color=TARGET_COLOR,
        strokeWidth=3,
    ).encode(x=alt.X("value:Q"))
    return _style_chart(alt.layer(histogram, marker), height=210)


def build_model_comparison_chart(model_comparison: pd.DataFrame) -> alt.Chart:
    """Render a model comparison plot focused on calibration and discrimination."""

    comparison = model_comparison.copy()
    comparison["selected_label"] = comparison["selected_final_model_flag"].map(
        {True: "Selected", False: "Candidate"}
    )
    chart = alt.Chart(comparison).mark_circle(size=220, opacity=0.92).encode(
        x=alt.X("calibration_error:Q", title="Calibration error (lower is better)"),
        y=alt.Y("roc_auc:Q", title="ROC-AUC"),
        color=alt.Color(
            "selected_label:N",
            scale=alt.Scale(domain=["Selected", "Candidate"], range=[STAT_FORECAST_COLOR, "#9aacbf"]),
            legend=alt.Legend(title="Model status"),
        ),
        tooltip=[
            "model_name",
            alt.Tooltip("brier_score:Q", title="Brier score", format=".4f"),
            alt.Tooltip("log_loss:Q", title="Log loss", format=".4f"),
            alt.Tooltip("roc_auc:Q", title="ROC-AUC", format=".4f"),
            alt.Tooltip("calibration_quality_label:N", title="Calibration"),
        ],
    )
    return _style_chart(chart)


def _style_chart(chart: alt.Chart, *, height: int = 330) -> alt.Chart:
    """Apply a consistent polished Altair styling."""

    return (
        chart.properties(height=height)
        .configure_view(strokeOpacity=0)
        .configure_axis(
            labelColor="#405367",
            titleColor="#10233a",
            gridColor="#e5edf6",
            domainColor="#cad5e1",
            labelFontSize=12,
            titleFontSize=12,
            titleFontWeight="bold",
        )
        .configure_legend(
            labelColor="#405367",
            titleColor="#10233a",
            labelFontSize=12,
            titleFontSize=12,
            symbolType="circle",
        )
    )

