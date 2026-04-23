"""Theme helpers and shared styling for the TargetTruth dashboard."""

from __future__ import annotations

from html import escape


PRODUCT_NAME = "TargetTruth"
PRODUCT_TAGLINE = "Climate Commitment Failure Intelligence Platform"
ENGINE_SUBTITLE = "Built on the CarbonLedgerX analytical engine"

RISK_BAND_COLORS = {
    "low": "#1f9d74",
    "moderate": "#d1a23d",
    "high": "#dd6b3f",
    "severe": "#d1495b",
}

CREDIBILITY_BAND_COLORS = {
    "strong": "#1f9d74",
    "watch": "#d1a23d",
    "weak": "#dd6b3f",
    "critical": "#d1495b",
}

NEUTRAL_TONES = {
    "slate": ("#dce4ef", "#10233a"),
    "navy": ("#d8e8ff", "#12345b"),
    "teal": ("#d8f2ee", "#0d5a57"),
    "gold": ("#f7edd4", "#785e14"),
    "rose": ("#f9dde2", "#7e2131"),
    "ink": ("#e8edf4", "#22313f"),
}


APP_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Sans:wght@400;500;600&display=swap');

:root {
  --tt-bg: #f4f7fb;
  --tt-panel: rgba(255, 255, 255, 0.86);
  --tt-panel-strong: rgba(255, 255, 255, 0.97);
  --tt-border: rgba(16, 35, 58, 0.10);
  --tt-text: #10233a;
  --tt-muted: #5f7086;
  --tt-accent: #12345b;
  --tt-accent-soft: #d8e8ff;
  --tt-shadow: 0 18px 48px rgba(18, 52, 91, 0.10);
}

html, body, [class*="css"] {
  font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
  color: var(--tt-text);
}

h1, h2, h3, h4 {
  font-family: "Manrope", "IBM Plex Sans", sans-serif !important;
  letter-spacing: -0.02em;
}

.stApp {
  background:
    radial-gradient(circle at top left, rgba(59, 130, 246, 0.10), transparent 30%),
    radial-gradient(circle at top right, rgba(15, 118, 110, 0.08), transparent 28%),
    linear-gradient(180deg, #f6f8fc 0%, #edf2f8 100%);
}

.block-container {
  max-width: 1460px;
  padding-top: 1.2rem;
  padding-bottom: 3.5rem;
}

[data-testid="stSidebar"] {
  background: linear-gradient(180deg, #10233a 0%, #17365a 100%);
  border-right: 1px solid rgba(255, 255, 255, 0.08);
}

[data-testid="stSidebar"] * {
  color: #eef4fb;
}

[data-testid="stSidebar"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] .stSelectbox > div > div,
[data-testid="stSidebar"] .stMultiSelect > div > div {
  background: rgba(255, 255, 255, 0.08);
  border: 1px solid rgba(255, 255, 255, 0.12);
}

.tt-shell {
  display: block;
}

.tt-hero {
  padding: 1.5rem 1.6rem 1.4rem 1.6rem;
  border-radius: 26px;
  background:
    linear-gradient(135deg, rgba(18, 52, 91, 0.98) 0%, rgba(27, 73, 120, 0.95) 52%, rgba(15, 118, 110, 0.88) 100%);
  color: #f5f9ff;
  box-shadow: var(--tt-shadow);
  border: 1px solid rgba(255,255,255,0.10);
  overflow: hidden;
}

.tt-hero-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.55fr) minmax(320px, 0.95fr);
  gap: 1.2rem;
  align-items: start;
}

.tt-kicker {
  display: inline-block;
  font-size: 0.73rem;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: rgba(245, 249, 255, 0.72);
  margin-bottom: 0.75rem;
}

.tt-product-title {
  font-size: 2.3rem;
  font-weight: 800;
  line-height: 1.04;
  margin: 0;
}

.tt-product-subtitle {
  font-size: 1.05rem;
  color: rgba(245, 249, 255, 0.86);
  margin-top: 0.35rem;
}

.tt-engine-subtitle {
  font-size: 0.95rem;
  color: rgba(245, 249, 255, 0.72);
  margin-top: 0.4rem;
}

.tt-company-card {
  background: rgba(255, 255, 255, 0.10);
  border: 1px solid rgba(255, 255, 255, 0.14);
  border-radius: 22px;
  padding: 1rem 1rem 1.05rem 1rem;
  backdrop-filter: blur(10px);
}

.tt-company-name {
  font-size: 1.28rem;
  font-weight: 700;
  margin-bottom: 0.2rem;
}

.tt-company-id {
  color: rgba(245, 249, 255, 0.74);
  font-size: 0.90rem;
}

.tt-banner {
  margin-top: 1.05rem;
  padding: 1rem 1.05rem;
  border-radius: 18px;
  border: 1px solid rgba(255,255,255,0.16);
  background: rgba(255,255,255,0.10);
  font-size: 1rem;
  line-height: 1.5;
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.08);
}

.tt-banner strong {
  color: #ffffff;
}

.tt-chip-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.45rem;
  margin-top: 0.7rem;
}

.tt-chip {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
  padding: 0.34rem 0.72rem;
  border-radius: 999px;
  font-size: 0.80rem;
  font-weight: 600;
  border: 1px solid transparent;
  white-space: nowrap;
}

.tt-section-head {
  margin: 1.2rem 0 0.7rem 0;
}

.tt-section-kicker {
  font-size: 0.74rem;
  text-transform: uppercase;
  letter-spacing: 0.16em;
  color: var(--tt-muted);
  margin-bottom: 0.28rem;
}

.tt-section-title {
  font-size: 1.42rem;
  font-weight: 800;
  color: var(--tt-text);
}

.tt-section-subtitle {
  margin-top: 0.24rem;
  color: var(--tt-muted);
  font-size: 0.96rem;
  max-width: 880px;
}

.tt-surface {
  background: var(--tt-panel);
  border: 1px solid var(--tt-border);
  border-radius: 24px;
  box-shadow: var(--tt-shadow);
  padding: 1.05rem 1.15rem;
  backdrop-filter: blur(12px);
}

.tt-surface-compact {
  padding: 0.95rem 1rem;
}

.tt-kpi-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(165px, 1fr));
  gap: 0.85rem;
}

.tt-kpi-card {
  background: linear-gradient(180deg, rgba(255,255,255,0.97), rgba(247,250,255,0.95));
  border: 1px solid var(--tt-border);
  border-radius: 22px;
  box-shadow: 0 18px 40px rgba(16, 35, 58, 0.06);
  padding: 0.95rem 1rem 1.02rem 1rem;
  min-height: 132px;
}

.tt-kpi-label {
  font-size: 0.76rem;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  color: var(--tt-muted);
  margin-bottom: 0.5rem;
}

.tt-kpi-value {
  font-family: "Manrope", "IBM Plex Sans", sans-serif;
  font-size: 1.55rem;
  font-weight: 800;
  line-height: 1.1;
  color: var(--tt-text);
}

.tt-kpi-subvalue {
  margin-top: 0.45rem;
  color: var(--tt-muted);
  font-size: 0.88rem;
  line-height: 1.35;
}

.tt-callout {
  border-radius: 22px;
  border: 1px solid var(--tt-border);
  padding: 0.95rem 1rem;
  background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(248,251,255,0.96));
  box-shadow: 0 18px 40px rgba(16, 35, 58, 0.06);
}

.tt-callout-title {
  font-size: 0.82rem;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  color: var(--tt-muted);
  margin-bottom: 0.42rem;
}

.tt-callout-body {
  font-size: 0.95rem;
  line-height: 1.55;
  color: var(--tt-text);
}

.tt-mini-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 0.72rem;
}

.tt-mini-card {
  background: rgba(255,255,255,0.92);
  border: 1px solid var(--tt-border);
  border-radius: 18px;
  padding: 0.85rem 0.9rem;
}

.tt-mini-label {
  font-size: 0.74rem;
  text-transform: uppercase;
  letter-spacing: 0.11em;
  color: var(--tt-muted);
}

.tt-mini-value {
  margin-top: 0.42rem;
  font-weight: 700;
  font-size: 1.12rem;
  color: var(--tt-text);
}

.tt-path {
  font-family: "IBM Plex Sans", monospace;
  font-size: 0.82rem;
  color: var(--tt-text);
  word-break: break-all;
}

.tt-divider {
  height: 1px;
  background: linear-gradient(90deg, rgba(16,35,58,0.0), rgba(16,35,58,0.12), rgba(16,35,58,0.0));
  margin: 1.1rem 0 0.4rem 0;
}

.tt-footnote {
  color: var(--tt-muted);
  font-size: 0.84rem;
  line-height: 1.4;
}

.tt-quiet {
  color: var(--tt-muted);
}

.tt-warning {
  border-left: 4px solid #d1495b;
}

.tt-good {
  border-left: 4px solid #1f9d74;
}

.tt-attention {
  border-left: 4px solid #d1a23d;
}

@media (max-width: 1100px) {
  .tt-hero-grid {
    grid-template-columns: 1fr;
  }
}
</style>
"""


def inject_theme() -> None:
    """Return the global CSS block for the dashboard."""

    import streamlit as st

    st.markdown(APP_CSS, unsafe_allow_html=True)


def band_color(label: str | None, *, band_type: str = "risk") -> str:
    """Return a consistent color for risk and credibility labels."""

    normalized = (label or "").strip().lower()
    if band_type == "credibility":
        return CREDIBILITY_BAND_COLORS.get(normalized, "#5f7086")
    return RISK_BAND_COLORS.get(normalized, "#5f7086")


def tone_colors(tone: str) -> tuple[str, str]:
    """Return background/text colors for a named tone."""

    return NEUTRAL_TONES.get(tone, NEUTRAL_TONES["slate"])


def chip_html(text: str, *, tone: str = "slate", bordered: bool = False) -> str:
    """Return HTML for a styled chip."""

    background, foreground = tone_colors(tone)
    border = f"border:1px solid {foreground}22;" if bordered else "border:none;"
    return (
        f"<span class='tt-chip' style='background:{background};color:{foreground};{border}'>"
        f"{escape(text)}"
        "</span>"
    )

