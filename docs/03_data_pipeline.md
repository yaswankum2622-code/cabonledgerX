# 03 Data Pipeline

## Pipeline Philosophy

The data pipeline is staged and artifact-driven. Each phase writes a named output and avoids hidden transformations. That makes the repository easier to review and easier to explain in interviews.

## Source Families

The implemented pipeline currently uses three source families:

### eGRID

Used for:

- state-level grid emissions and generation views
- balancing-authority emissions and generation views
- location-based electricity factor references

### DEFRA

Used for:

- fuel conversion factors
- electricity-related factor references and factor taxonomy
- activity-based Scope 1 calculation proxies

### SBTi

Used for:

- company and target metadata
- commitment-oriented fields such as target year, scope, and status

## Pipeline Stages

### Stage 1: Raw Profiling

Scripts:

- `scripts/profile_raw_data.py`

Outputs:

- workbook profile JSON files under `outputs/profiles/`

What it does:

- inspects sheet names
- records shapes
- samples normalized columns
- identifies machine-friendly sheets

### Stage 2: Interim Extraction

Scripts:

- `scripts/extract_interim_tables.py`

Outputs:

- source-shaped parquet tables under `data/interim/`

What it does:

- extracts specific sheets from eGRID, DEFRA, and SBTi
- standardizes headers
- preserves most source columns
- tags tables with source dataset and source sheet

### Stage 3: Canonical Processed Tables

Scripts:

- `scripts/build_processed_tables.py`

Outputs:

- canonical processed tables under `data/processed/`

Key outputs:

- `egrid_state_factors.parquet`
- `egrid_ba_factors.parquet`
- `defra_emission_factors.parquet`
- `sbti_company_commitments.parquet`
- `company_synthetic_panel.parquet`

### Stage 4: Company-Level Modeling Layers

Representative scripts:

- `scripts/build_emissions_baseline.py`
- `scripts/build_forecast_and_assessment.py`
- `scripts/build_risk_and_contradictions.py`
- `scripts/build_intervention_scenarios.py`
- `scripts/build_activity_and_calculated_emissions.py`
- `scripts/build_historical_reconstruction.py`
- `scripts/build_statistical_forecast_and_evaluation.py`
- `scripts/build_probabilistic_scoring.py`
- `scripts/build_scoring_reconciliation.py`

Outputs:

- company-level processed tables such as:
  - baseline
  - calculated emissions
  - historical reconstruction
  - forecasts
  - assessments
  - scoring
  - interventions
  - evidence-pack index

## Key Processed Tables

The most important downstream tables are:

- `company_emissions_baseline.parquet`
- `company_emissions_calculated.parquet`
- `company_emissions_history_annual.parquet`
- `company_emissions_forecast.parquet`
- `company_emissions_forecast_statistical.parquet`
- `company_commitment_assessment.parquet`
- `company_commitment_intelligence.parquet`
- `company_commitment_probability_scores.parquet`
- `company_scoring_reconciliation.parquet`
- `company_intervention_intelligence.parquet`

## Data Quality And Traceability Choices

The implemented pipeline favors transparency over premature optimization:

- source-sheet provenance is preserved where practical
- factor references are stored as text rather than hidden inside opaque code
- each stage writes a manifest or evaluation artifact
- tests are smoke-oriented, focused on artifact existence and basic integrity

## What Is Intentionally Not In The Data Pipeline

The current public repository does not include:

- a relational database layer
- raw-data download automation from the internet
- user-uploaded company activity ingestion
- entity resolution against real issuers

Those are valid future steps, but the current repo is intentionally local and deterministic.
