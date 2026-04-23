"""Deterministic narrative blocks for the premium TargetTruth dashboard."""

from __future__ import annotations

import pandas as pd


def executive_verdict(bundle: object) -> str:
    """Return the top-line executive verdict for the selected company."""

    risk_band = _row_text(bundle.reconciliation, "recommended_operational_risk_band", fallback="moderate").lower()
    target_met = bool(_row_value(bundle.assessment, "target_met_flag", default=False))
    best_action = _row_text(bundle.intervention, "best_intervention_name", fallback="process efficiency program")
    if target_met and risk_band in {"low", "moderate"}:
        return (
            f"{risk_band.title()} commitment-failure risk: the modeled trajectory is broadly consistent "
            f"with the stated target; maintain discipline and prioritize {best_action.replace('_', ' ')}."
        )
    if risk_band == "severe":
        return (
            f"Severe commitment-failure risk: the stated target is not supported by the current trajectory; "
            f"best first action is {best_action.replace('_', ' ')}."
        )
    if risk_band == "high":
        return (
            f"High commitment pressure: current trajectory remains off target and requires corrective action; "
            f"best first action is {best_action.replace('_', ' ')}."
        )
    return (
        f"Moderate commitment pressure: delivery risk is manageable but still material; "
        f"best first action is {best_action.replace('_', ' ')}."
    )


def risk_summary(bundle: object) -> str:
    """Return a concise explanation of why the company is risky or stable."""

    gap_pct = _row_number(bundle.assessment, "target_gap_pct")
    implied = _row_number(bundle.assessment, "implied_reduction_pct")
    contradictions = int(round(_row_number(bundle.contradictions, "contradiction_count")))
    probability = _row_number(bundle.reconciliation, "calibrated_miss_probability") * 100.0
    return (
        f"The company shows a {gap_pct:.1f}% target gap, an implied reduction of {implied:.1f}%, "
        f"{contradictions} active contradiction flags, and a calibrated miss probability of {probability:.1f}%."
    )


def reconciliation_summary(bundle: object) -> str:
    """Explain how the reconciliation layer interprets the scoring relationship."""

    status = _row_text(bundle.reconciliation, "reconciliation_status", fallback="aligned").replace("_", " ")
    primary = _row_text(bundle.reconciliation, "disagreement_reason_primary", fallback="aligned_multi_signal_view").replace(
        "_", " "
    )
    secondary = _row_text(bundle.reconciliation, "disagreement_reason_secondary", fallback="same_direction_risk_signal").replace(
        "_", " "
    )
    return (
        f"The reconciliation layer classifies this case as {status}. "
        f"Primary reason: {primary}. Secondary reason: {secondary}."
    )


def calculator_summary(bundle: object) -> str:
    """Explain the calculator layer in one deterministic paragraph."""

    delta_pct = _row_number(bundle.calculated, "delta_vs_prior_baseline_mb_pct")
    return (
        "The audit calculator recomputes Scope 1 from natural gas and diesel activity, then recomputes "
        "Scope 2 from electricity using mapped location-based and market-based factors. "
        f"The calculator result differs from the prior baseline by {delta_pct:.1f}%."
    )


def model_summary(bundle: object) -> str:
    """Explain why the selected forecast model was chosen."""

    selected_model = _row_text(bundle.forecast_summary, "selected_model_name", fallback="statistical model").replace("_", " ")
    mean_ape = _row_number(bundle.forecast_summary, "mean_ape_pct")
    coverage = _row_number(bundle.forecast_summary, "interval_coverage_pct")
    direction = _row_text(bundle.forecast_summary, "forecast_direction_label", fallback="flat")
    return (
        f"{selected_model.title()} was selected from walk-forward backtests because it delivered "
        f"{mean_ape:.2f}% mean APE with {coverage:.1f}% interval coverage; the 2030 outlook is {direction}."
    )


def intervention_summary(bundle: object) -> str:
    """Explain why the top intervention is recommended."""

    intervention = _row_text(bundle.intervention, "best_intervention_name", fallback="process_efficiency_program").replace(
        "_", " "
    )
    abatement = _row_number(bundle.intervention, "best_intervention_abatement_tco2e")
    cost = _row_number(bundle.intervention, "best_intervention_cost_per_tco2e")
    closure = (
        "fully closes"
        if bool(_row_value(bundle.intervention, "best_intervention_closes_gap_flag", default=False))
        else "partially closes"
        if bool(_row_value(bundle.intervention, "best_intervention_partially_closes_gap_flag", default=False))
        else "does not close"
    )
    return (
        f"{intervention.title()} is the leading action because it abates {abatement:,.0f} tCO2e at "
        f"{cost:,.1f} USD per tCO2e and {closure} the target gap."
    )


def evidence_summary(bundle: object) -> str:
    """Explain evidence pack availability."""

    if bundle.evidence is None or not bool(_row_value(bundle.evidence, "evidence_generated_flag", default=False)):
        return (
            "Evidence packs were not generated for this company in the selected sample set; "
            "the analytical data is still available for API and dashboard review."
        )
    return (
        "Board, investor, and lender evidence outputs are available for this company from the deterministic "
        "evidence-pack layer."
    )


def _row_number(row: pd.Series | None, column_name: str, *, default: float = 0.0) -> float:
    if row is None:
        return default
    value = pd.to_numeric(pd.Series([row.get(column_name)]), errors="coerce").iloc[0]
    if pd.isna(value):
        return default
    return float(value)


def _row_text(row: pd.Series | None, column_name: str, *, fallback: str = "-") -> str:
    if row is None:
        return fallback
    value = row.get(column_name)
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return fallback
    return str(value)


def _row_value(row: pd.Series | None, column_name: str, *, default: object = None) -> object:
    if row is None:
        return default
    value = row.get(column_name)
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return default
    return value

