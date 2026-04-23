# 08 Dashboard And API

## Delivery Surface Philosophy

The analytical pipeline is only useful if it can be consumed. TargetTruth therefore exposes the same intelligence through two separate surfaces:

- a premium Streamlit dashboard for narrative review and screenshots
- a thin FastAPI layer for read-only structured access

The two surfaces intentionally sit on top of parquet outputs rather than reimplementing modeling logic.

## Streamlit Dashboard

Main code:

- `src/carbonledgerx/dashboard/app.py`
- supporting modules under `src/carbonledgerx/dashboard/`

Current sections:

- Executive Header
- KPI Command Center
- Historical + Forecast Trajectory
- Risk, Contradictions & Reconciliation
- Calculator & Baseline Audit
- Intervention Strategy Studio
- Model Comparison & Forecast Quality
- Portfolio Context
- Evidence Packs

What the dashboard is trying to do:

- feel like a serious product surface rather than a default Streamlit prototype
- make analytical rigor visible
- support screenshots for GitHub and portfolio review
- keep deterministic narrative blocks separate from raw data tables

## Dashboard Design Choices

The dashboard uses:

- custom CSS and visual hierarchy
- deterministic text blocks instead of LLM narration
- cached parquet loading
- sectioned charts and callout cards
- company-level filtering through the sidebar

This makes the dashboard useful for demos, reviews, and interview walkthroughs.

## FastAPI Layer

Main code:

- `src/carbonledgerx/api/main.py`
- `src/carbonledgerx/api/data_access.py`
- `src/carbonledgerx/api/schemas.py`

The API is intentionally thin and read-only. It exposes:

- health status
- company list
- overview payload
- forecast payload
- risk payload
- intervention payload
- evidence payload
- full intelligence payload

This is the right scope for the current repo because it:

- demonstrates service design
- keeps business logic in the analytical layer
- avoids premature database or auth complexity

## Why Streamlit Plus FastAPI

The combination works well at this stage:

- Streamlit is fast for a premium demo surface
- FastAPI is strong for structured access and clean typing
- both can share parquet-backed data access patterns

This split also mirrors a realistic product evolution:

- dashboard first for stakeholder interaction
- API next for integration and future service expansion

## What Is Not Yet In The Delivery Stack

The current repo does not yet include:

- auth
- user accounts
- background jobs
- database persistence
- cloud deployment infrastructure
- role-based access control

That is a deliberate staging choice, not an oversight.

## Demo Flow

A strong demo sequence is:

1. show the executive verdict
2. show historical and forecast trajectory
3. show contradiction and reconciliation logic
4. show calculator audit
5. show intervention strategy
6. show evidence availability
7. show the API docs endpoint

That sequence makes the product feel coherent and rigorous.
