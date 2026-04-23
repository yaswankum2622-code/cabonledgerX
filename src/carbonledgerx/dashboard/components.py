"""Presentation helpers for the premium TargetTruth Streamlit dashboard."""

from __future__ import annotations

from html import escape
from typing import Iterable, Sequence

import pandas as pd
import streamlit as st

from carbonledgerx.dashboard.theme import ENGINE_SUBTITLE, PRODUCT_NAME, PRODUCT_TAGLINE, band_color, chip_html


def render_brand_header(
    *,
    company_name: str,
    company_id: str,
    sector: str,
    country: str,
    risk_band: str,
    credibility_band: str,
    verdict_text: str,
) -> None:
    """Render the executive product header and company identification card."""

    risk_chip = chip_html(risk_band.title(), tone=_tone_for_risk(risk_band), bordered=True)
    credibility_chip = chip_html(
        credibility_band.title(),
        tone=_tone_for_credibility(credibility_band),
        bordered=True,
    )
    sector_chip = chip_html(sector, tone="navy", bordered=True)
    country_chip = chip_html(country, tone="teal", bordered=True)

    st.markdown(
        (
            "<div class='tt-shell'>"
            "<div class='tt-hero'>"
            "<div class='tt-hero-grid'>"
            "<div>"
            "<div class='tt-kicker'>Climate intelligence interface</div>"
            f"<div class='tt-product-title'>{escape(PRODUCT_NAME)}</div>"
            f"<div class='tt-product-subtitle'>{escape(PRODUCT_TAGLINE)}</div>"
            f"<div class='tt-engine-subtitle'>{escape(ENGINE_SUBTITLE)}</div>"
            f"<div class='tt-banner'><strong>Executive verdict.</strong> {escape(verdict_text)}</div>"
            "</div>"
            "<div class='tt-company-card'>"
            "<div class='tt-kicker'>Selected company</div>"
            f"<div class='tt-company-name'>{escape(company_name)}</div>"
            f"<div class='tt-company-id'>{escape(company_id)}</div>"
            "<div class='tt-chip-row'>"
            f"{sector_chip}{country_chip}{risk_chip}{credibility_chip}"
            "</div>"
            "</div>"
            "</div>"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_section_header(title: str, subtitle: str, *, kicker: str | None = None) -> None:
    """Render a section title block."""

    kicker_html = (
        f"<div class='tt-section-kicker'>{escape(kicker)}</div>" if kicker else ""
    )
    st.markdown(
        (
            "<div class='tt-section-head'>"
            f"{kicker_html}"
            f"<div class='tt-section-title'>{escape(title)}</div>"
            f"<div class='tt-section-subtitle'>{escape(subtitle)}</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_surface_open(extra_class: str | None = None) -> None:
    """Open a styled content surface."""

    classes = "tt-surface"
    if extra_class:
        classes = f"{classes} {extra_class}"
    st.markdown(f"<div class='{classes}'>", unsafe_allow_html=True)


def render_surface_close() -> None:
    """Close a styled content surface."""

    st.markdown("</div>", unsafe_allow_html=True)


def render_kpi_cards(cards: Iterable[dict[str, str]]) -> None:
    """Render a premium KPI card grid."""

    card_html: list[str] = []
    for card in cards:
        accent = card.get("accent_color")
        accent_bar = (
            f"style='box-shadow: inset 0 3px 0 {accent};'" if accent else ""
        )
        subvalue = card.get("subvalue", "")
        footnote = f"<div class='tt-kpi-subvalue'>{escape(subvalue)}</div>" if subvalue else ""
        card_html.append(
            (
                f"<div class='tt-kpi-card' {accent_bar}>"
                f"<div class='tt-kpi-label'>{escape(card['label'])}</div>"
                f"<div class='tt-kpi-value'>{escape(card['value'])}</div>"
                f"{footnote}"
                "</div>"
            )
        )
    st.markdown(
        "<div class='tt-kpi-grid'>" + "".join(card_html) + "</div>",
        unsafe_allow_html=True,
    )


def render_callout(title: str, body: str, *, tone: str = "neutral") -> None:
    """Render a compact callout panel."""

    tone_class = {
        "warning": "tt-warning",
        "good": "tt-good",
        "attention": "tt-attention",
    }.get(tone, "")
    st.markdown(
        (
            f"<div class='tt-callout {tone_class}'>"
            f"<div class='tt-callout-title'>{escape(title)}</div>"
            f"<div class='tt-callout-body'>{escape(body)}</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_chip_row(labels: Sequence[str], *, tone: str = "slate") -> None:
    """Render a row of chips."""

    if not labels:
        return
    chip_markup = "".join(chip_html(label, tone=tone, bordered=True) for label in labels)
    st.markdown(f"<div class='tt-chip-row'>{chip_markup}</div>", unsafe_allow_html=True)


def render_named_chips(chips: Sequence[tuple[str, str]]) -> None:
    """Render chips with explicit tones."""

    if not chips:
        return
    markup = "".join(chip_html(label, tone=tone, bordered=True) for label, tone in chips)
    st.markdown(f"<div class='tt-chip-row'>{markup}</div>", unsafe_allow_html=True)


def render_mini_stats(items: Iterable[dict[str, str]]) -> None:
    """Render a small summary stat grid."""

    cards = []
    for item in items:
        cards.append(
            (
                "<div class='tt-mini-card'>"
                f"<div class='tt-mini-label'>{escape(item['label'])}</div>"
                f"<div class='tt-mini-value'>{escape(item['value'])}</div>"
                "</div>"
            )
        )
    st.markdown(
        "<div class='tt-mini-grid'>" + "".join(cards) + "</div>",
        unsafe_allow_html=True,
    )


def render_dual_recommendation_cards(cards: Sequence[dict[str, str]]) -> None:
    """Render one or two recommendation cards."""

    columns = st.columns(len(cards) if cards else 1)
    for column, card in zip(columns, cards, strict=False):
        with column:
            render_callout(card["title"], card["body"], tone=card.get("tone", "attention"))


def render_data_table(dataframe: pd.DataFrame, *, height: int = 260) -> None:
    """Render a compact dataframe using modern width handling."""

    st.dataframe(
        dataframe,
        width="stretch",
        height=height,
        hide_index=True,
    )


def render_path_status(title: str, path_value: str | None, *, available: bool) -> None:
    """Render evidence pack path availability."""

    tone = "good" if available else "attention"
    body = path_value if path_value else "Not generated for this company."
    escaped_body = escape(body)
    st.markdown(
        (
            f"<div class='tt-callout {('tt-good' if available else 'tt-attention')}'>"
            f"<div class='tt-callout-title'>{escape(title)}</div>"
            f"<div class='tt-path'>{escaped_body}</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_divider() -> None:
    """Render a subtle divider between content groups."""

    st.markdown("<div class='tt-divider'></div>", unsafe_allow_html=True)


def _tone_for_risk(risk_band: str) -> str:
    normalized = (risk_band or "").strip().lower()
    return {
        "low": "teal",
        "moderate": "gold",
        "high": "rose",
        "severe": "rose",
    }.get(normalized, "slate")


def _tone_for_credibility(credibility_band: str) -> str:
    normalized = (credibility_band or "").strip().lower()
    return {
        "strong": "teal",
        "watch": "gold",
        "weak": "rose",
        "critical": "rose",
    }.get(normalized, "slate")


def color_for_band(label: str | None, *, band_type: str = "risk") -> str:
    """Expose the theme band-color helper for app modules."""

    return band_color(label, band_type=band_type)

