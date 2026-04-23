"""Premium TargetTruth Streamlit dashboard."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from carbonledgerx.dashboard.charts import (
    build_cost_abatement_scatter,
    build_distribution_chart,
    build_flag_chart,
    build_intervention_abatement_chart,
    build_mac_chart,
    build_model_comparison_chart,
    build_risk_component_chart,
    build_trajectory_chart,
)
from carbonledgerx.dashboard.components import (
    color_for_band,
    render_brand_header,
    render_callout,
    render_data_table,
    render_divider,
    render_dual_recommendation_cards,
    render_kpi_cards,
    render_mini_stats,
    render_named_chips,
    render_path_status,
    render_section_header,
)
from carbonledgerx.dashboard.data_loader import (
    CompanyDashboardBundle,
    active_contradiction_flags,
    get_company_bundle,
    get_company_selector_frame,
    load_dashboard_tables,
    percentile_rank,
    selected_model_row,
)
from carbonledgerx.dashboard.text_blocks import (
    calculator_summary,
    evidence_summary,
    executive_verdict,
    intervention_summary,
    model_summary,
    reconciliation_summary,
    risk_summary,
)
from carbonledgerx.dashboard.theme import inject_theme


def main() -> None:
    """Run the premium single-page TargetTruth dashboard."""

    st.set_page_config(
        page_title="TargetTruth",
        page_icon="TT",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_theme()

    datasets = load_dashboard_tables()
    selector_frame = get_company_selector_frame(datasets)
    selected_company_id = render_sidebar(selector_frame)
    bundle = get_company_bundle(datasets, selected_company_id)

    render_executive_header(bundle)
    render_kpi_command_center(bundle)
    render_trajectory_section(bundle)
    render_risk_section(bundle)
    render_calculator_section(bundle)
    render_intervention_section(bundle)
    render_model_quality_section(bundle)
    render_portfolio_context_section(bundle)
    render_evidence_section(bundle)


def render_sidebar(selector_frame: pd.DataFrame) -> str:
    """Render the sidebar controls and return the selected company id."""

    st.sidebar.markdown("## TargetTruth")
    st.sidebar.caption("Executive climate commitment intelligence")

    risk_options = ["All"] + sorted(
        selector_frame["recommended_operational_risk_band"].dropna().astype(str).unique().tolist()
    )
    selected_risk = st.sidebar.selectbox("Portfolio risk filter", options=risk_options, index=0)

    filtered = selector_frame
    if selected_risk != "All":
        filtered = filtered.loc[
            filtered["recommended_operational_risk_band"].astype(str) == selected_risk
        ].copy()

    labels = filtered["company_label"].tolist()
    selected_label = st.sidebar.selectbox("Selected company", options=labels, index=0)
    selected_row = filtered.loc[filtered["company_label"] == selected_label].iloc[0]

    severe_count = int(
        (selector_frame["recommended_operational_risk_band"].astype(str) == "severe").sum()
    )
    high_count = int(
        (selector_frame["recommended_operational_risk_band"].astype(str) == "high").sum()
    )
    render_sidebar_summary(selector_frame.shape[0], severe_count, high_count)

    st.sidebar.caption(
        f"{selected_row['company_name']} | {selected_row['sector']} | "
        f"{selected_row['country']} | probability {float(selected_row['calibrated_miss_probability']) * 100:.1f}%"
    )
    return str(selected_row["company_id"])


def render_sidebar_summary(total_companies: int, severe_count: int, high_count: int) -> None:
    """Render the portfolio summary in the sidebar."""

    st.sidebar.markdown("---")
    st.sidebar.metric("Companies in view", f"{total_companies}")
    st.sidebar.metric("Severe risk cases", f"{severe_count}")
    st.sidebar.metric("High risk cases", f"{high_count}")
    st.sidebar.caption("Parquet-backed read-only dashboard over the full CarbonLedgerX analytics stack.")


def render_executive_header(bundle: CompanyDashboardBundle) -> None:
    """Render the top hero header."""

    render_brand_header(
        company_name=_row_text(bundle.intelligence, "company_name"),
        company_id=bundle.company_id,
        sector=_row_text(bundle.intelligence, "sector"),
        country=_row_text(bundle.intelligence, "country"),
        risk_band=_row_text(bundle.reconciliation, "recommended_operational_risk_band", fallback="moderate"),
        credibility_band=_row_text(bundle.intelligence, "credibility_band", fallback="watch"),
        verdict_text=executive_verdict(bundle),
    )

    render_section_header(
        "Executive Snapshot",
        "An executive surface tying company context, target posture, scoring reconciliation, and the current best intervention into a single product-grade view.",
        kicker="Selected company",
    )
    render_mini_stats(
        [
            {"label": "Target year", "value": _fmt_int(_row_number(bundle.intelligence, "target_year"))},
            {"label": "Target reduction", "value": _fmt_pct(_row_number(bundle.intelligence, "target_reduction_pct"))},
            {"label": "Assessment year", "value": _fmt_int(_row_number(bundle.assessment, "assessment_year"))},
            {"label": "Selected forecast model", "value": _row_text(bundle.forecast_summary, "selected_model_name", fallback="-").replace("_", " ")},
        ]
    )


def render_kpi_command_center(bundle: CompanyDashboardBundle) -> None:
    """Render premium KPI cards."""

    render_section_header(
        "KPI Command Center",
        "Priority signals distilled into premium cards for screenshots, stakeholder walkthroughs, and executive interpretation.",
        kicker="Command center",
    )
    intervention_name = _row_text(bundle.intervention, "best_intervention_name", fallback="-").replace("_", " ")
    render_kpi_cards(
        [
            {
                "label": "Recommended operational risk band",
                "value": _row_text(bundle.reconciliation, "recommended_operational_risk_band", fallback="-").title(),
                "subvalue": f"Score {_fmt_score(_row_number(bundle.reconciliation, 'recommended_operational_score'))}",
                "accent_color": color_for_band(
                    _row_text(bundle.reconciliation, "recommended_operational_risk_band"),
                    band_type="risk",
                ),
            },
            {
                "label": "Calibrated miss probability",
                "value": _fmt_probability(_row_number(bundle.reconciliation, "calibrated_miss_probability")),
                "subvalue": _row_text(bundle.reconciliation, "miss_probability_band", fallback="-").title(),
                "accent_color": "#12345b",
            },
            {
                "label": "Target gap",
                "value": _fmt_pct(_row_number(bundle.assessment, "target_gap_pct")),
                "subvalue": _fmt_tco2e(_row_number(bundle.assessment, "target_gap_tco2e")),
                "accent_color": "#d1495b",
            },
            {
                "label": "Credibility score",
                "value": _fmt_score(_row_number(bundle.intelligence, "commitment_credibility_score")),
                "subvalue": _row_text(bundle.intelligence, "credibility_band", fallback="-").title(),
                "accent_color": color_for_band(
                    _row_text(bundle.intelligence, "credibility_band"),
                    band_type="credibility",
                ),
            },
            {
                "label": "Best intervention",
                "value": intervention_name.title() if intervention_name != "-" else "-",
                "subvalue": _gap_status_text(bundle.intervention),
                "accent_color": "#0f766e",
            },
            {
                "label": "Best intervention cost / tCO2e",
                "value": _fmt_currency(_row_number(bundle.intervention, "best_intervention_cost_per_tco2e")),
                "subvalue": _fmt_tco2e(_row_number(bundle.intervention, "best_intervention_abatement_tco2e")),
                "accent_color": "#d1a23d",
            },
            {
                "label": "Gap closure status",
                "value": _gap_status_text(bundle.intervention),
                "subvalue": _row_text(bundle.intervention, "intervention_recommendation_summary", fallback="-"),
                "accent_color": "#405367",
            },
        ]
    )


def render_trajectory_section(bundle: CompanyDashboardBundle) -> None:
    """Render the history and forecast section."""

    render_divider()
    render_section_header(
        "Historical + Forecast Trajectory",
        "A combined view of reconstructed emissions history, forecasted 2030 outlook, and the target threshold used in commitment assessment.",
        kicker="Trajectory",
    )

    view_mode = st.radio(
        "Trajectory mode",
        options=[
            "Historical + statistical forecast",
            "Deterministic vs statistical comparison",
        ],
        horizontal=True,
        label_visibility="collapsed",
    )

    baseline_year = int(round(_row_number(bundle.baseline, "baseline_year", default=_row_number(bundle.assessment, "assessment_year", default=2024))))
    baseline_total = _row_number(bundle.assessment, "baseline_total_mb_tco2e")
    target_year = int(round(_row_number(bundle.assessment, "target_year", default=2030)))
    assessment_year = int(round(_row_number(bundle.assessment, "assessment_year", default=2030)))
    target_reduction_pct = _row_number(bundle.assessment, "target_reduction_pct")
    target_threshold = max(0.0, baseline_total * (1.0 - target_reduction_pct / 100.0))

    chart = build_trajectory_chart(
        history=bundle.history,
        forecast_stat=bundle.forecast_stat,
        forecast_det=bundle.forecast_det,
        baseline_year=baseline_year,
        baseline_value=baseline_total,
        target_year=target_year,
        assessment_year=assessment_year,
        target_threshold=target_threshold,
        mode=view_mode,
    )
    st.altair_chart(chart, width="stretch")

    left, right = st.columns([1.2, 0.9])
    with left:
        render_callout("Trajectory interpretation", _trajectory_summary(bundle, target_threshold), tone="attention")
    with right:
        render_mini_stats(
            [
                {"label": "Latest actual MB", "value": _fmt_tco2e(_row_number(bundle.forecast_summary, "latest_actual_total_mb_tco2e"))},
                {"label": "2030 statistical MB", "value": _fmt_tco2e(_row_number(bundle.forecast_summary, "forecast_2030_total_mb_tco2e"))},
                {"label": "Target threshold", "value": _fmt_tco2e(target_threshold)},
                {"label": "Interval coverage", "value": _fmt_pct(_row_number(bundle.forecast_summary, "interval_coverage_pct"))},
            ]
        )
        if "capped at 2030" in _row_text(bundle.assessment, "assessment_notes").lower():
            render_callout(
                "Assessment horizon note",
                "The stated target year extends beyond the forecast horizon, so the analytical assessment is capped at 2030.",
                tone="warning",
            )


def render_risk_section(bundle: CompanyDashboardBundle) -> None:
    """Render contradictions, risk scoring, and reconciliation."""

    render_divider()
    render_section_header(
        "Risk, Contradictions & Reconciliation",
        "Rule-based risk, calibrated miss probability, contradiction burden, and the reconciliation layer are shown together so disagreement is transparent rather than hidden.",
        kicker="Risk intelligence",
    )

    active_flags = active_contradiction_flags(bundle.contradictions)
    flag_labels = [flag["flag"] for flag in active_flags]
    flag_chips = [(label, "rose" if "Low" not in label else "gold") for label in flag_labels]
    if flag_chips:
        render_named_chips(flag_chips)

    top_left, top_mid, top_right = st.columns(3)
    with top_left:
        render_callout(
            "Contradiction burden",
            f"{_fmt_int(_row_number(bundle.contradictions, 'contradiction_count'))} active contradictions. "
            f"{_row_text(bundle.contradictions, 'contradiction_summary', fallback='No contradiction summary available.')}",
            tone="warning",
        )
    with top_mid:
        render_callout("Why this company is risky", risk_summary(bundle), tone="attention")
    with top_right:
        render_callout("Reconciliation interpretation", reconciliation_summary(bundle), tone="good")

    render_kpi_cards(
        [
            {
                "label": "Heuristic risk score",
                "value": _fmt_score(_row_number(bundle.risk, "target_miss_risk_score")),
                "subvalue": _row_text(bundle.risk, "risk_band", fallback="-").title(),
                "accent_color": color_for_band(_row_text(bundle.risk, "risk_band")),
            },
            {
                "label": "Calibrated miss probability",
                "value": _fmt_probability(_row_number(bundle.reconciliation, "calibrated_miss_probability")),
                "subvalue": _row_text(bundle.reconciliation, "miss_probability_band", fallback="-").title(),
                "accent_color": "#12345b",
            },
            {
                "label": "Recommended operational view",
                "value": _fmt_score(_row_number(bundle.reconciliation, "recommended_operational_score")),
                "subvalue": _row_text(bundle.reconciliation, "recommended_operational_risk_band", fallback="-").title(),
                "accent_color": color_for_band(_row_text(bundle.reconciliation, "recommended_operational_risk_band")),
            },
            {
                "label": "Alignment label",
                "value": _row_text(bundle.reconciliation, "scoring_alignment_label", fallback="-").replace("_", " ").title(),
                "subvalue": _row_text(bundle.reconciliation, "reconciliation_status", fallback="-").replace("_", " "),
                "accent_color": "#405367",
            },
        ]
    )

    left, right = st.columns([1.1, 0.9])
    with left:
        component_frame = _risk_component_frame(bundle.risk)
        if not component_frame.empty:
            st.altair_chart(build_risk_component_chart(component_frame), width="stretch")
        else:
            render_callout("Score components", "No rule-based score component breakdown is available.", tone="attention")
    with right:
        if active_flags:
            st.altair_chart(build_flag_chart(pd.DataFrame(active_flags)), width="stretch")
        else:
            render_callout("Active contradiction flags", "No contradiction flags are active for this company.", tone="good")

    detail_frame = pd.DataFrame(
        [
            {
                "signal": "Primary disagreement reason",
                "value": _row_text(bundle.reconciliation, "disagreement_reason_primary", fallback="-").replace("_", " "),
            },
            {
                "signal": "Secondary disagreement reason",
                "value": _row_text(bundle.reconciliation, "disagreement_reason_secondary", fallback="-").replace("_", " "),
            },
            {
                "signal": "Probability driver summary",
                "value": _row_text(bundle.probability, "key_feature_driver_summary", fallback="-"),
            },
        ]
    )
    render_data_table(detail_frame, height=180)


def render_calculator_section(bundle: CompanyDashboardBundle) -> None:
    """Render the calculator and baseline audit section."""

    render_divider()
    render_section_header(
        "Calculator & Baseline Audit",
        "A transparent audit layer showing activity inputs, factor references, and the delta between the calculator-derived baseline and the earlier synthetic baseline.",
        kicker="Calculator audit",
    )

    render_kpi_cards(
        [
            {
                "label": "Calculator total MB",
                "value": _fmt_tco2e(_row_number(bundle.calculated, "calculated_total_mb_tco2e")),
                "subvalue": "Activity-based recalculation",
                "accent_color": "#12345b",
            },
            {
                "label": "Prior baseline total MB",
                "value": _fmt_tco2e(_row_number(bundle.calculated, "prior_baseline_total_mb_tco2e")),
                "subvalue": "Original modeled baseline",
                "accent_color": "#405367",
            },
            {
                "label": "Delta vs prior baseline",
                "value": _fmt_tco2e(_row_number(bundle.calculated, "delta_vs_prior_baseline_mb_tco2e")),
                "subvalue": _fmt_pct(_row_number(bundle.calculated, "delta_vs_prior_baseline_mb_pct")),
                "accent_color": "#d1495b" if _row_number(bundle.calculated, "delta_vs_prior_baseline_mb_tco2e") > 0 else "#1f9d74",
            },
            {
                "label": "Base year",
                "value": _fmt_int(_row_number(bundle.calculated, "base_year")),
                "subvalue": _row_text(bundle.baseline, "factor_region_type", fallback="-"),
                "accent_color": "#0f766e",
            },
        ]
    )

    left, right = st.columns([0.95, 1.05])
    with left:
        render_callout("How the calculator works", calculator_summary(bundle), tone="good")
        render_mini_stats(
            [
                {"label": "Electricity", "value": _fmt_mwh(_row_number(bundle.activity, "electricity_mwh"))},
                {"label": "Natural gas", "value": _fmt_mmbtu(_row_number(bundle.activity, "natural_gas_mmbtu"))},
                {"label": "Diesel", "value": _fmt_liters(_row_number(bundle.activity, "diesel_liters"))},
                {"label": "Fleet proxy", "value": _fmt_int(_row_number(bundle.activity, "fleet_size_proxy"))},
                {"label": "Facilities", "value": _fmt_int(_row_number(bundle.activity, "facility_count_proxy"))},
                {"label": "Floor area", "value": _fmt_sqft(_row_number(bundle.activity, "floor_area_sqft_proxy"))},
            ]
        )
    with right:
        factor_frame = pd.DataFrame(
            [
                {"factor layer": "Scope 1", "reference": _row_text(bundle.calculated, "scope1_factor_reference", fallback="-")},
                {"factor layer": "Scope 2 LB", "reference": _row_text(bundle.calculated, "scope2_lb_factor_reference", fallback="-")},
                {"factor layer": "Scope 2 MB", "reference": _row_text(bundle.calculated, "scope2_mb_factor_reference", fallback="-")},
            ]
        )
        render_data_table(factor_frame, height=180)
        render_callout(
            "Audit notes",
            _row_text(bundle.calculated, "calculation_notes", fallback="No calculator notes available."),
            tone="attention",
        )


def render_intervention_section(bundle: CompanyDashboardBundle) -> None:
    """Render intervention strategy studio."""

    render_divider()
    render_section_header(
        "Intervention Strategy Studio",
        "Recommended actions are ranked by modeled abatement, cost efficiency, feasibility, and gap-closure value rather than displayed as an undifferentiated list.",
        kicker="Intervention strategy",
    )

    top_rows = bundle.interventions.head(2)
    recommendation_cards = []
    for _, row in top_rows.iterrows():
        recommendation_cards.append(
            {
                "title": str(row["intervention_name"]).replace("_", " ").title(),
                "body": (
                    f"{_fmt_tco2e(row['modeled_abatement_tco2e'])} abatement at "
                    f"{_fmt_currency(row['cost_per_tco2e'])} per tCO2e; "
                    f"start year {_fmt_int(row['start_year'])}; adoption {_fmt_pct(row['adoption_pct'])}. "
                    f"{_gap_status_text(row)}."
                ),
                "tone": "good" if bool(row.get("recommended_priority_flag", False)) else "attention",
            }
        )
    render_dual_recommendation_cards(recommendation_cards)

    left, right = st.columns(2)
    with left:
        st.altair_chart(build_intervention_abatement_chart(bundle.interventions), width="stretch")
    with right:
        st.altair_chart(build_mac_chart(bundle.interventions), width="stretch")

    left, right = st.columns([1.0, 0.95])
    with left:
        st.altair_chart(build_cost_abatement_scatter(bundle.interventions), width="stretch")
    with right:
        render_callout("Recommendation summary", intervention_summary(bundle), tone="good")
        render_callout(
            "Intervention rationale",
            _row_text(bundle.intervention, "intervention_recommendation_summary", fallback="No intervention summary available."),
            tone="attention",
        )

    table_columns = [
        "intervention_name",
        "primary_scope_impact",
        "modeled_abatement_tco2e",
        "cost_per_tco2e",
        "priority_rank",
        "mac_rank",
        "closes_gap_flag",
        "partially_closes_gap_flag",
        "recommended_priority_flag",
    ]
    render_data_table(
        bundle.interventions.loc[:, [column for column in table_columns if column in bundle.interventions.columns]],
        height=265,
    )


def render_model_quality_section(bundle: CompanyDashboardBundle) -> None:
    """Render model comparison and forecast quality."""

    render_divider()
    render_section_header(
        "Model Comparison & Forecast Quality",
        "This section shows that the project is using evaluated model selection rather than an uninspected toy forecast, with explicit calibration and backtest signals.",
        kicker="Forecast rigor",
    )

    selected_prob_model = selected_model_row(bundle.model_comparison)
    selected_prob_name = _row_text(selected_prob_model, "model_name", fallback="-").replace("_", " ")
    render_kpi_cards(
        [
            {
                "label": "Selected statistical forecast model",
                "value": _row_text(bundle.forecast_summary, "selected_model_name", fallback="-").replace("_", " ").title(),
                "subvalue": _row_text(bundle.forecast_summary, "forecast_direction_label", fallback="-").title(),
                "accent_color": "#12345b",
            },
            {
                "label": "Walk-forward mean APE",
                "value": _fmt_pct(_row_number(bundle.forecast_summary, "mean_ape_pct")),
                "subvalue": _fmt_tco2e(_row_number(bundle.forecast_summary, "mean_abs_error")),
                "accent_color": "#0f766e",
            },
            {
                "label": "Interval coverage",
                "value": _fmt_pct(_row_number(bundle.forecast_summary, "interval_coverage_pct")),
                "subvalue": "Coverage of backtest intervals",
                "accent_color": "#d1a23d",
            },
            {
                "label": "Selected probability model",
                "value": selected_prob_name.title(),
                "subvalue": _row_text(selected_prob_model, "calibration_quality_label", fallback="-").title(),
                "accent_color": "#405367",
            },
        ]
    )

    left, right = st.columns([1.0, 0.95])
    with left:
        render_callout("Forecast model rationale", model_summary(bundle), tone="good")
    with right:
        render_callout(
            "Probability model rationale",
            _probability_model_summary(selected_prob_model),
            tone="attention",
        )

    st.altair_chart(build_model_comparison_chart(bundle.model_comparison), width="stretch")
    comparison_table = bundle.model_comparison.loc[
        :,
        [
            "model_name",
            "roc_auc",
            "brier_score",
            "log_loss",
            "calibration_error",
            "calibration_quality_label",
            "selected_final_model_flag",
        ],
    ].copy()
    render_data_table(comparison_table, height=240)


def render_portfolio_context_section(bundle: CompanyDashboardBundle) -> None:
    """Render selected-company context versus the portfolio."""

    render_divider()
    render_section_header(
        "Portfolio Context",
        "Selected-company signals are shown against the full synthetic portfolio so screenshots convey relative severity, not only raw point values.",
        kicker="Portfolio context",
    )

    portfolio = bundle.portfolio_frame
    probability = _row_number(bundle.reconciliation, "calibrated_miss_probability")
    target_gap = _row_number(bundle.reconciliation, "target_gap_pct")
    credibility = _row_number(bundle.reconciliation, "commitment_credibility_score")
    contradictions = _row_number(bundle.reconciliation, "contradiction_count")

    render_kpi_cards(
        [
            {
                "label": "Miss-probability percentile",
                "value": _fmt_pct(percentile_rank(portfolio["calibrated_miss_probability"], probability)),
                "subvalue": _fmt_probability(probability),
                "accent_color": "#12345b",
            },
            {
                "label": "Target-gap percentile",
                "value": _fmt_pct(percentile_rank(portfolio["target_gap_pct"], target_gap)),
                "subvalue": _fmt_pct(target_gap),
                "accent_color": "#d1495b",
            },
            {
                "label": "Credibility-score percentile",
                "value": _fmt_pct(percentile_rank(portfolio["commitment_credibility_score"], credibility)),
                "subvalue": _fmt_score(credibility),
                "accent_color": "#0f766e",
            },
            {
                "label": "Contradiction-count percentile",
                "value": _fmt_pct(percentile_rank(portfolio["contradiction_count"], contradictions)),
                "subvalue": _fmt_int(contradictions),
                "accent_color": "#405367",
            },
        ]
    )

    left, right = st.columns(2)
    with left:
        st.altair_chart(
            build_distribution_chart(
                portfolio["calibrated_miss_probability"],
                probability,
                title="Calibrated miss probability",
                color="#c8d8f0",
            ),
            width="stretch",
        )
    with right:
        st.altair_chart(
            build_distribution_chart(
                portfolio["target_gap_pct"],
                target_gap,
                title="Target gap (%)",
                color="#f4d7dc",
            ),
            width="stretch",
        )


def render_evidence_section(bundle: CompanyDashboardBundle) -> None:
    """Render evidence-pack status and paths."""

    render_divider()
    render_section_header(
        "Evidence Packs",
        "Executive-facing markdown outputs remain available as product artifacts, with clean availability states for board, investor, and lender audiences.",
        kicker="Evidence outputs",
    )

    render_callout("Evidence summary", evidence_summary(bundle), tone="good" if _row_bool(bundle.evidence, "evidence_generated_flag") else "attention")

    left, middle, right = st.columns(3)
    with left:
        render_path_status(
            "Board brief",
            _row_text(bundle.evidence, "board_brief_path", fallback=""),
            available=_row_bool(bundle.evidence, "evidence_generated_flag"),
        )
    with middle:
        render_path_status(
            "Investor memo",
            _row_text(bundle.evidence, "investor_memo_path", fallback=""),
            available=_row_bool(bundle.evidence, "evidence_generated_flag"),
        )
    with right:
        render_path_status(
            "Lender note",
            _row_text(bundle.evidence, "lender_note_path", fallback=""),
            available=_row_bool(bundle.evidence, "evidence_generated_flag"),
        )


def _trajectory_summary(bundle: CompanyDashboardBundle, target_threshold: float) -> str:
    latest_actual = _row_number(bundle.forecast_summary, "latest_actual_total_mb_tco2e")
    forecast_2030 = _row_number(bundle.forecast_summary, "forecast_2030_total_mb_tco2e")
    direction = _row_text(bundle.forecast_summary, "forecast_direction_label", fallback="flat")
    return (
        f"The company exits history at {_fmt_tco2e(latest_actual)} and reaches a statistical 2030 projection of "
        f"{_fmt_tco2e(forecast_2030)}. That remains {'above' if forecast_2030 > target_threshold else 'below'} the "
        f"target threshold of {_fmt_tco2e(target_threshold)}, with an overall {direction} trajectory."
    )


def _probability_model_summary(selected_prob_model: pd.Series | None) -> str:
    if selected_prob_model is None:
        return "No probabilistic model comparison row is available."
    return (
        f"{_row_text(selected_prob_model, 'model_name', fallback='selected model').replace('_', ' ').title()} "
        f"was retained because it balances calibration quality "
        f"({_row_text(selected_prob_model, 'calibration_quality_label', fallback='-')}) with "
        f"a Brier score of {_fmt_decimal(_row_number(selected_prob_model, 'brier_score'), 4)} "
        f"and ROC-AUC of {_fmt_decimal(_row_number(selected_prob_model, 'roc_auc'), 4)}."
    )


def _risk_component_frame(risk_row: pd.Series | None) -> pd.DataFrame:
    if risk_row is None:
        return pd.DataFrame()
    components = [
        ("Gap", _row_number(risk_row, "score_gap_component")),
        ("Claim", _row_number(risk_row, "score_claim_component")),
        ("Support", _row_number(risk_row, "score_support_component")),
        ("Timing", _row_number(risk_row, "score_timing_component")),
        ("Trend", _row_number(risk_row, "score_trend_component")),
    ]
    return pd.DataFrame(
        [{"component_label": label, "component_value": value} for label, value in components]
    )


def _gap_status_text(row: pd.Series | None) -> str:
    if row is None:
        return "No intervention view"
    if bool(row.get("best_intervention_closes_gap_flag", False)) or bool(row.get("closes_gap_flag", False)):
        return "Closes gap"
    if bool(row.get("best_intervention_partially_closes_gap_flag", False)) or bool(row.get("partially_closes_gap_flag", False)):
        return "Partially closes gap"
    return "Gap remains open"


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


def _row_bool(row: pd.Series | None, column_name: str) -> bool:
    if row is None:
        return False
    value = row.get(column_name)
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return False
    return bool(value)


def _fmt_tco2e(value: float | int | str | None) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    return f"{float(numeric_value):,.0f} tCO2e"


def _fmt_pct(value: float | int | str | None) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    return f"{float(numeric_value):.1f}%"


def _fmt_probability(value: float | int | str | None) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    return f"{float(numeric_value) * 100.0:,.1f}%"


def _fmt_score(value: float | int | str | None) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    return f"{float(numeric_value):,.1f}"


def _fmt_currency(value: float | int | str | None) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    return f"${float(numeric_value):,.1f}"


def _fmt_decimal(value: float | int | str | None, digits: int) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    return f"{float(numeric_value):,.{digits}f}"


def _fmt_int(value: float | int | str | None) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    return str(int(round(float(numeric_value))))


def _fmt_mwh(value: float | int | str | None) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    return f"{float(numeric_value):,.0f} MWh"


def _fmt_mmbtu(value: float | int | str | None) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    return f"{float(numeric_value):,.0f} MMBtu"


def _fmt_liters(value: float | int | str | None) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    return f"{float(numeric_value):,.0f} L"


def _fmt_sqft(value: float | int | str | None) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    return f"{float(numeric_value):,.0f} sqft"


if __name__ == "__main__":
    main()

