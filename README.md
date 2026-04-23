# TargetTruth

TargetTruth is a climate commitment failure intelligence platform built on the CarbonLedgerX analytical engine. It combines emissions factor parsing, an activity-based calculator, historical reconstruction, deterministic and statistical forecasting, rule-based and probabilistic scoring, intervention ranking, evidence-pack generation, a premium Streamlit dashboard, and a thin FastAPI layer.

The repository is designed as a public portfolio artifact: the code is production-shaped, the modeling logic is transparent, and the documentation is organized for technical review, interviews, and GitHub presentation rather than only local experimentation.

## What The Project Does

TargetTruth answers a practical question:

**Given a company’s stated climate commitment, current operating profile, and modeled trajectory, how credible is the commitment, how likely is a miss, and what intervention is the best first response?**

The implemented pipeline currently covers:

- factor ingestion and profiling for eGRID, DEFRA, and SBTi source workbooks
- normalized interim and processed parquet layers
- synthetic company generation for portfolio-scale experimentation
- factor mapping and baseline emissions construction
- an auditable activity-based emissions calculator
- annual historical reconstruction from 2015 through 2024
- deterministic and statistical forecasts through 2030
- commitment assessment against target thresholds
- contradiction flags, heuristic scoring, probabilistic scoring, and scoring reconciliation
- intervention simulation and MAC-style ranking
- deterministic evidence packs for board, investor, and lender audiences
- a premium Streamlit dashboard and a read-only FastAPI service

## Why It Matters

Many climate dashboards stop at static KPIs or a single risk label. This project goes further:

- it shows how emissions were calculated
- it explains why a forecast was selected
- it separates heuristic and probabilistic risk views instead of hiding disagreement
- it ties risk assessment to concrete intervention options
- it packages the result into both product-facing and interview-facing surfaces

That makes it more useful as a portfolio product for climate-tech, analytics, product, and decision-intelligence roles than a generic ESG dashboard.

## Repository Guide

High-signal repo areas:

- `src/carbonledgerx/`
  - core parsers, models, dashboard, API, and utility code
- `scripts/`
  - reproducible phase entry points that build each data product
- `tests/`
  - smoke tests covering the pipeline stages and API/dashboard imports
- `docs/`
  - architecture, modeling, deployment, ADRs, and interview-prep material

Local-only / generated areas:

- `data/raw/`
  - source workbooks and PDFs
- `data/interim/`
  - extracted intermediate parquet tables
- `data/processed/`
  - processed analytical outputs
- `outputs/`
  - manifests, evaluation artifacts, evidence packs, and screenshots

## Documentation Map

- [01 Problem Statement](<docs/01_problem_statement.md>)
- [02 Architecture](<docs/02_architecture.md>)
- [03 Data Pipeline](<docs/03_data_pipeline.md>)
- [04 Calculator And History](<docs/04_calculator_and_history.md>)
- [05 Forecasting And Evaluation](<docs/05_forecasting_and_evaluation.md>)
- [06 Scoring And Reconciliation](<docs/06_scoring_and_reconciliation.md>)
- [07 Interventions And MAC](<docs/07_interventions_and_mac.md>)
- [08 Dashboard And API](<docs/08_dashboard_and_api.md>)
- [09 Innovations And Uniqueness](<docs/09_innovations_and_uniqueness.md>)
- [10 Future Work](<docs/10_future_work.md>)
- [Deployment](<docs/DEPLOYMENT.md>)
- [Interview Prep](<docs/INTERVIEW_PREP.md>)
- [Changelog](<docs/CHANGELOG.md>)
- [ADRs](<docs/adr>)
- [Archived Scope Reference](<docs/archive/CarbonLedger_ScopeFreeze.md>)

## Quickstart

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
python scripts/bootstrap_check.py
```

## Core Build Sequence

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

## Demo Commands

Run the dashboard:

```powershell
python -m streamlit run src/carbonledgerx/dashboard/app.py
```

Run the API:

```powershell
python -m uvicorn src.carbonledgerx.api.main:app --reload
```

Useful URLs:

- Streamlit: `http://localhost:8501`
- FastAPI docs: `http://127.0.0.1:8000/docs`
- FastAPI redoc: `http://127.0.0.1:8000/redoc`

## Screenshots

Executive surface:

![TargetTruth Executive Surface](<docs/assets/screenshots/dashboard_executive.png>)

Risk and reconciliation surface:

![TargetTruth Risk And Reconciliation](<docs/assets/screenshots/dashboard_risk.png>)

Additional curated screenshots:

- [Calculator Audit](<docs/assets/screenshots/dashboard_calculator.png>)
- [Intervention Strategy Studio](<docs/assets/screenshots/dashboard_interventions.png>)

## What Makes This Repo High-Signal

- It is end-to-end rather than notebook-fragmented.
- It uses explicit, inspectable business logic where synthetic data limits make heavier modeling hard to justify.
- It includes both engineering surfaces and communication surfaces: API, dashboard, evidence packs, ADRs, and interview-prep material.
- It acknowledges where outputs are proxy-based or synthetic instead of overstating realism.

## Current Limitations

- source company data is synthetic rather than real issuer disclosures
- the activity-based calculator uses documented proxies where perfect company-specific factors are unavailable
- the probabilistic scoring layer is trained on deterministic synthetic labels, so it is useful for methodology demonstration rather than real-world calibration claims
- the current deployment model is local parquet-backed demo infrastructure, not a multi-user production stack

## Next Review Docs

For the fastest technical walkthrough, read:

1. [02 Architecture](<docs/02_architecture.md>)
2. [04 Calculator And History](<docs/04_calculator_and_history.md>)
3. [05 Forecasting And Evaluation](<docs/05_forecasting_and_evaluation.md>)
4. [06 Scoring And Reconciliation](<docs/06_scoring_and_reconciliation.md>)
5. [08 Dashboard And API](<docs/08_dashboard_and_api.md>)
