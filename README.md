

<h1 align="center">CarbonLedgerX</h1>
<p align="center"><strong>Climate Commitment Failure Intelligence Platform</strong></p>
<p align="center"><em>Previously prototyped as "TargetTruth" - unified under CarbonLedgerX for v1.0</em></p>

<p align="center">
  <a href="https://github.com/yaswankum2622-code/cabonledgerX"><img src="https://img.shields.io/badge/repo-public_portfolio-0F172A?style=for-the-badge&logo=github&logoColor=white" alt="Public portfolio repo"></a>
  <img src="https://img.shields.io/badge/python-3.11%2B-1F6FEB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/streamlit-premium_dashboard-E4572E?style=for-the-badge&logo=streamlit&logoColor=white" alt="Streamlit dashboard">
  <img src="https://img.shields.io/badge/fastapi-read--only_API-059669?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI read-only API">
  <img src="https://img.shields.io/badge/forecasting-deterministic_%2B_statistical-2563EB?style=for-the-badge" alt="Deterministic and statistical forecasting">
  <img src="https://img.shields.io/badge/scoring-heuristic_%2B_probabilistic_%2B_reconciled-B7791F?style=for-the-badge" alt="Scoring layers">
  <a href="./LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge" alt="License"></a>
</p>

<p align="center">
  <img src="https://img.shields.io/github/last-commit/yaswankum2622-code/cabonledgerX?style=flat-square&color=0F766E" alt="Last commit">
  <img src="https://img.shields.io/github/languages/top/yaswankum2622-code/cabonledgerX?style=flat-square&color=1D4ED8" alt="Top language">
  <img src="https://img.shields.io/github/repo-size/yaswankum2622-code/cabonledgerX?style=flat-square&color=334155" alt="Repo size">
</p>

## What This Project Is

TargetTruth is a portfolio-scale climate intelligence product that asks a hard operational question:

> **Given a company's stated climate target, current operating footprint, modeled emissions path, and available interventions, how credible is the target and how likely is a miss?**

This repo is not a notebook bundle and it is not a generic ESG dashboard. It is a full analytical system with:

- factor ingestion for eGRID, DEFRA, and SBTi source workbooks
- canonical parquet layers for raw, interim, and processed data products
- a synthetic 500-company portfolio for repeatable modeling and demos
- an activity-based emissions calculator plus historical annual reconstruction
- deterministic forecasting and a separate statistical forecast stack with backtesting
- contradiction detection, heuristic scoring, probabilistic scoring, and score reconciliation
- intervention simulation, MAC-style ranking, and evidence-pack generation
- a premium Streamlit dashboard and a thin FastAPI service

---

## Headline Results

| Dimension | Result | Details |
|---|---|---|
| Data sources ingested | 3 real public datasets | EPA eGRID (2022, 2023), DEFRA GHG Conversion Factors 2025, SBTi target database |
| Companies modeled | ~500 synthetic portfolio | Sector-conditioned, reproducible, demonstrates methodology at scale |
| Forecasting approaches | 2 parallel stacks | Deterministic business-rule projection + statistical models with backtesting |
| Scoring layers | 3 reconciled views | Heuristic · Probabilistic (calibrated) · Reconciled (disagreement-aware) |
| Decision surfaces | Intervention MAC ranking | Marginal abatement cost analysis + evidence packs |
| Code footprint | 81 Python files | Layered package architecture under `src/carbonledgerx/` |

---

## Why It Stands Out

- **Auditable**: the calculator layer shows activity inputs, factor references, and baseline deltas instead of hiding the math.
- **Methodologically honest**: deterministic and statistical forecasts are both exposed; the repo includes backtesting and calibration artifacts.
- **Decision-oriented**: the system does not stop at "risk high/low"; it ties risk to intervention ranking and evidence outputs.
- **Product-shaped**: the same analytical core is surfaced through docs, scripts, tests, dashboard, API, screenshots, and interview-ready architecture notes.

## Architecture

The platform is organized as a layered climate intelligence stack: raw factors and commitment data are normalized into processed tables, the calculator and reconstruction layers build a historical emissions view, forecasting layers project trajectories through 2030, scoring layers explain risk from multiple angles, and the final surfaces expose that intelligence through interventions, evidence, dashboard, and API.

![TargetTruth architecture diagram](docs/assets/architecture_diagram.svg)

## Product Gallery

<div align="center">

### Executive Overview
![Executive overview dashboard](docs/assets/screenshots/dashboard_executive.png)

### Risk and Reconciliation
![Risk and reconciliation dashboard](docs/assets/screenshots/dashboard_risk.png)

### Calculator Audit
![Calculator and baseline audit dashboard](docs/assets/screenshots/dashboard_calculator.png)

### Intervention Strategy
![Intervention strategy dashboard](docs/assets/screenshots/dashboard_interventions.png)

</div>

## Core Capabilities

### 1. Factor and Commitment Data Pipeline
- profiles and inspects eGRID, DEFRA, and SBTi workbooks
- extracts machine-usable interim tables
- builds canonical processed factor and commitment layers

### 2. Activity-Based Emissions Logic
- generates sector-conditioned company activity inputs
- calculates Scope 1 and Scope 2 emissions from explicit factor references
- compares calculated outputs with the earlier synthetic baseline for auditability

### 3. Historical + Forecasting Stack
- reconstructs annual company emissions from 2015 through 2024
- keeps a deterministic forecast layer for transparent business-rule projection
- adds a statistical forecast layer with naive vs trend model backtesting and model selection

### 4. Risk Intelligence Stack
- commitment assessment vs forecasted trajectory
- contradiction flags
- rule-based risk and credibility scoring
- probabilistic commitment-miss scoring
- scoring reconciliation to handle disagreement cases explicitly

### 5. Decision Support
- intervention simulation and MAC-style ranking
- company-level intervention intelligence
- board, investor, and lender evidence packs
- FastAPI endpoints and a premium Streamlit intelligence console

## Repository Map

```text
src/carbonledgerx/
|- api/         # FastAPI service
|- config/      # project settings and paths
|- dashboard/   # premium Streamlit product surface
|- data/        # writers, catalogs, profiling helpers
|- models/      # calculator, forecasting, scoring, interventions
|- parsers/     # raw workbook inspection and extraction
+- utils/       # shared path and helper logic

scripts/         # phase-by-phase reproducible build entry points
tests/           # smoke tests for pipeline, dashboard, and API
docs/            # architecture, ADRs, deployment, interview prep
```

## Quickstart

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
python scripts/bootstrap_check.py
```

## Build Sequence

```powershell
python scripts/profile_raw_data.py
python scripts/extract_interim_tables.py
python scripts/build_processed_tables.py
python scripts/build_emissions_baseline.py
python scripts/build_forecast_and_assessment.py
python scripts/build_risk_and_contradictions.py
python scripts/build_intervention_scenarios.py
python scripts/build_evidence_packs.py
python scripts/build_activity_and_calculated_emissions.py
python scripts/build_historical_reconstruction.py
python scripts/build_statistical_forecast_and_evaluation.py
python scripts/build_probabilistic_scoring.py
python scripts/build_scoring_reconciliation.py
```

## How To Demo

Run the dashboard:

```powershell
python -m streamlit run src/carbonledgerx/dashboard/app.py
```

Run the API:

```powershell
python -m uvicorn src.carbonledgerx.api.main:app --reload
```

Open:

- Streamlit: `http://localhost:8501`
- FastAPI docs: `http://127.0.0.1:8000/docs`
- FastAPI redoc: `http://127.0.0.1:8000/redoc`

Best demo flow:

1. start on the executive overview and KPI command center
2. move to trajectory and model comparison to show rigor
3. open the calculator audit section to prove transparency
4. show risk reconciliation and intervention ranking
5. end with evidence-pack availability and API docs

## Documentation Map

- [01 Problem Statement](docs/01_problem_statement.md)
- [02 Architecture](docs/02_architecture.md)
- [03 Data Pipeline](docs/03_data_pipeline.md)
- [04 Calculator And History](docs/04_calculator_and_history.md)
- [05 Forecasting And Evaluation](docs/05_forecasting_and_evaluation.md)
- [06 Scoring And Reconciliation](docs/06_scoring_and_reconciliation.md)
- [07 Interventions And MAC](docs/07_interventions_and_mac.md)
- [08 Dashboard And API](docs/08_dashboard_and_api.md)
- [09 Innovations And Uniqueness](docs/09_innovations_and_uniqueness.md)
- [10 Future Work](docs/10_future_work.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Interview Prep](docs/INTERVIEW_PREP.md)
- [Verification and Reproducibility Guide](docs/VERIFICATION.md)
- [Changelog](docs/CHANGELOG.md)
- [Architecture Decision Records](docs/adr)

## Why This Is Not A Generic ESG Dashboard

- It does not stop at static emissions cards; it reconstructs history and projects forward.
- It does not use a single opaque score; it exposes heuristic, probabilistic, and reconciled views.
- It does not bury assumptions; the calculator, forecast evaluation, and reconciliation layers are all explicit.
- It does not end at diagnosis; it recommends ranked interventions and evidence-pack outputs.

## Why I Built This

Most ESG tooling in 2026 falls into two buckets: enterprise platforms that cost $50K–$500K per year and are black-box, or Excel-plus-PDF workflows that consultants charge hourly to maintain. Neither surfaces the question that actually matters to a board, a lender, or a climate-aware investor: is this company's commitment credible, and if not, what should be done?

CarbonLedgerX is my attempt to show that this question can be answered by a layered analytical system - real factor ingestion, activity-based emissions logic, dual-stack forecasting, multi-layer scoring, and intervention ranking - not by a dashboard and a vibe. The entire analytical core runs locally on parquet, is auditable end-to-end, and is decision-oriented rather than diagnostic-only.

It is deliberately a non-LLM project. My portfolio already includes an agentic RAG system (CortexAgent). This project demonstrates classical ML discipline: probabilistic calibration, forecast backtesting, and causal framing for interventions. One project shows I can build with LLMs. The other shows I know when to use different tools.

## What I'd Do Differently

Honest reflection from the build:

- **Company data is synthetic by design, but I would layer a real adapter earlier.** The methodology is sound; a real issuer adapter would make the product story tighter.
- **Probabilistic scoring is calibrated against synthetic deterministic labels.** In a production setting I would recalibrate against the actual 2015–2020 SBTi commitment outcome cohort now that those targets have matured.
- **The dashboard is a single Streamlit app.** For a real product deployment I would split the intelligence console from the audit console - different users, different access patterns.
- **Storage is local parquet.** For multi-tenant use this would move to DuckDB-backed or Postgres-backed persistence with a thin API caching layer.

## Current Constraints

- company data is synthetic, so the repo demonstrates analytical method and product architecture rather than real issuer coverage
- some factor mapping logic uses documented proxies where direct company-specific factors do not exist
- probabilistic scoring is trained on synthetic deterministic labels, so it demonstrates calibrated methodology rather than production-grade empirical calibration
- the current storage model is local parquet-backed infrastructure rather than a database-backed deployment

## Best Docs To Read First

1. [02 Architecture](docs/02_architecture.md)
2. [04 Calculator And History](docs/04_calculator_and_history.md)
3. [05 Forecasting And Evaluation](docs/05_forecasting_and_evaluation.md)
4. [06 Scoring And Reconciliation](docs/06_scoring_and_reconciliation.md)
5. [08 Dashboard And API](docs/08_dashboard_and_api.md)
