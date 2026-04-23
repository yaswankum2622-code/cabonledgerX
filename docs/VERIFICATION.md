# Reproducibility & Verification Guide

This document explains how to verify — from a fresh clone — that CarbonLedgerX is a real, runnable analytical system and not a scaffolded repo of empty files.

Every command below is repo-specific. Total verification time: ~5 minutes.

## 1. Project Location and Git Status

```powershell
cd C:\Users\User\CarbonLedgerX

git log --oneline -10
git remote -v
git status
```

Expected:
- Multiple commits with meaningful messages
- Remote pointing to `origin` on GitHub
- Working tree clean, tracking `origin/main`

## 2. Codebase Size Check (excluding .venv)

```powershell
Get-ChildItem src, scripts, tests -Recurse -Filter *.py | Measure-Object
```

Expected:
- ~81 Python files
- ~500 KB+ of code across `src/carbonledgerx/`, `scripts/`, `tests/`

## 3. Core Package Import Verification

```powershell
$env:PYTHONPATH = "$PWD\src"

.\.venv\Scripts\python.exe -c "from carbonledgerx.api.main import app; print('API title:', app.title)"
.\.venv\Scripts\python.exe -c "from carbonledgerx.models.emissions_calculator import build_company_emissions_calculated; print('calculator ok')"
.\.venv\Scripts\python.exe -c "from carbonledgerx.models.probabilistic_scoring import build_probabilistic_modeling_dataset; print('probabilistic scoring ok')"
.\.venv\Scripts\python.exe -c "from carbonledgerx.models.history_reconstructor import build_company_emissions_history_annual; print('history reconstructor ok')"
.\.venv\Scripts\python.exe -c "from carbonledgerx.data.catalog import list_available_datasets; print('catalog datasets:', len(list_available_datasets()))"
```

Expected: all prints succeed with no ImportError or AttributeError.

## 4. Raw and Processed Data Verification

```powershell
dir data\raw
dir data\processed
```

Expected raw files (real, MB-scale, not stubs):
- `egrid2022_data.xlsx` (~15 MB)
- `egrid2022_data_metric.xlsx` (~15 MB)
- `egrid2023_data_rev2.xlsx` (~21 MB)
- `targets-excel.xlsx` (~5 MB)
- `companies-excel.xlsx` (~2 MB)
- `ghg-conversion-factors-2025-full-set.xlsx` (~1.8 MB)
- `2025-GHG-CF-methodology-paper.pdf` (~1.8 MB)

Expected processed parquet outputs:
- `company_emissions_history_annual.parquet`
- `company_emissions_forecast.parquet`
- `company_emissions_forecast_statistical.parquet`
- `company_emissions_calculated.parquet`
- `company_commitment_intelligence.parquet`
- `sbti_company_commitments.parquet` (~3.7 MB)
- `egrid_state_factors.parquet`
- `egrid_ba_factors.parquet`

If any are missing, re-run the build sequence in the main README.

## 5. API Boot Verification

```powershell
.\.venv\Scripts\python.exe -m uvicorn carbonledgerx.api.main:app --reload
```

Then in another shell:

```powershell
curl http://127.0.0.1:8000/health
```

Expected response:

```json
{
  "status": "ok",
  "project_name": "TargetTruth API",
  "available_datasets": [
    "company_commitment_intelligence",
    "company_commitment_probability_scores",
    "company_scoring_reconciliation",
    "company_emissions_forecast_statistical",
    "company_intervention_intelligence",
    "company_mac_rankings",
    "company_evidence_pack_index"
  ]
}
```

Swagger docs available at http://127.0.0.1:8000/docs.

## 6. Dashboard Boot Verification

```powershell
.\.venv\Scripts\python.exe -m streamlit run src/carbonledgerx/dashboard/app.py
```

Expected: Streamlit serves on http://localhost:8501 with the full intelligence console.

## What This Verification Proves

- The package imports cleanly — no stub files, no missing dependencies
- The data pipeline has executed against real public workbooks
- The FastAPI service is correctly wired to the parquet-backed data layer
- The dashboard renders against the same canonical data layer

## Known Caveats

This verification does not cover statistical validity of forecasts or calibration of probabilistic scores — those are addressed in:

- [docs/05_forecasting_and_evaluation.md](./05_forecasting_and_evaluation.md)
- [docs/06_scoring_and_reconciliation.md](./06_scoring_and_reconciliation.md)
