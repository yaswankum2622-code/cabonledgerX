# 02 Architecture

## System Overview

TargetTruth uses a layered architecture built around parquet artifacts rather than a database-first design. That choice keeps the system local, reproducible, and easy to review while the product logic is still evolving.

The stack has five major layers:

1. ingestion and profiling
2. canonical data modeling
3. emissions and forecast analytics
4. scoring and intervention intelligence
5. delivery surfaces

## Architectural Layers

### 1. Ingestion And Profiling

Code area:

- `src/carbonledgerx/parsers/`
- `src/carbonledgerx/data/profiling.py`
- `scripts/profile_raw_data.py`
- `scripts/extract_interim_tables.py`

Purpose:

- inspect raw workbook structure
- normalize sheets into machine-usable interim tables
- preserve provenance and source-sheet traceability

### 2. Canonical Data Modeling

Code area:

- `src/carbonledgerx/models/canonical_tables.py`
- `src/carbonledgerx/data/interim_writer.py`
- `src/carbonledgerx/data/processed_writer.py`
- `scripts/build_processed_tables.py`

Purpose:

- standardize eGRID, DEFRA, and SBTi outputs
- build reusable processed tables with conservative column selection
- create the stable inputs used by later modeling layers

### 3. Emissions And Forecast Analytics

Code area:

- `src/carbonledgerx/models/activity_generator.py`
- `src/carbonledgerx/models/emissions_calculator.py`
- `src/carbonledgerx/models/history_reconstructor.py`
- `src/carbonledgerx/models/forecasting.py`
- `src/carbonledgerx/models/statistical_forecasting.py`
- `src/carbonledgerx/models/backtesting.py`
- `src/carbonledgerx/models/forecast_evaluation.py`

Purpose:

- map companies to factor references
- generate auditable activity inputs
- calculate emissions from activity rather than only synthetic placeholders
- reconstruct annual history from 2015 to 2024
- forecast to 2030 with both deterministic and statistical methods
- evaluate forecast quality through walk-forward backtesting

### 4. Scoring And Intervention Intelligence

Code area:

- `src/carbonledgerx/models/commitment_assessment.py`
- `src/carbonledgerx/models/contradiction_engine.py`
- `src/carbonledgerx/models/risk_scoring.py`
- `src/carbonledgerx/models/probabilistic_scoring.py`
- `src/carbonledgerx/models/scoring_reconciliation.py`
- `src/carbonledgerx/models/intervention_simulator.py`
- `src/carbonledgerx/models/mac_ranking.py`
- `src/carbonledgerx/models/evidence_pack.py`

Purpose:

- compare trajectory to target threshold
- flag contradictions between claims and modeled direction
- calculate heuristic and probabilistic risk views
- reconcile disagreement between scoring systems
- simulate interventions and rank them economically
- package findings into evidence-ready outputs

### 5. Delivery Surfaces

Code area:

- `src/carbonledgerx/dashboard/`
- `src/carbonledgerx/api/`

Purpose:

- expose the same analytics through a premium dashboard and a thin read-only API
- keep presentation separate from the analytical engine
- make the system demo-friendly without changing underlying logic

## Data Architecture

The repository uses a simple but effective data contract:

- `data/raw/`
  - source workbooks and documents
- `data/interim/`
  - extracted but still source-shaped parquet tables
- `data/processed/`
  - canonical tables and company-level analytical outputs
- `outputs/`
  - manifests, evaluation artifacts, screenshots, and evidence packs

This structure makes the pipeline easy to audit:

- raw sources remain distinct from transformed data
- each stage writes named artifacts
- scripts map closely to pipeline phases

## Final Public Repo Structure

Recommended public-facing structure:

```text
.
|-- README.md
|-- pyproject.toml
|-- .gitignore
|-- .env.example
|-- docs/
|   |-- 01_problem_statement.md
|   |-- 02_architecture.md
|   |-- 03_data_pipeline.md
|   |-- 04_calculator_and_history.md
|   |-- 05_forecasting_and_evaluation.md
|   |-- 06_scoring_and_reconciliation.md
|   |-- 07_interventions_and_mac.md
|   |-- 08_dashboard_and_api.md
|   |-- 09_innovations_and_uniqueness.md
|   |-- 10_future_work.md
|   |-- DEPLOYMENT.md
|   |-- INTERVIEW_PREP.md
|   |-- CHANGELOG.md
|   |-- adr/
|   +-- archive/
|-- src/carbonledgerx/
|   |-- api/
|   |-- config/
|   |-- dashboard/
|   |-- data/
|   |-- models/
|   |-- parsers/
|   +-- utils/
|-- scripts/
|-- tests/
|-- data/
|   +-- raw/          # local only
|-- outputs/          # local only
```

## What Should Stay Public

High-signal public contents:

- source code
- build scripts
- smoke tests
- architecture and interview docs
- a small curated screenshot set if desired

Low-signal or local-only contents:

- raw factor workbooks
- large generated parquet outputs
- screenshots generated during experimentation
- logs, caches, and notebook-like scratch artifacts

## Why This Architecture Works For The Current Stage

This design is strong for a portfolio repository because it balances:

- technical depth
- transparency
- local reproducibility
- demo readiness

It is not yet optimized for:

- concurrent multi-user serving
- database-backed persistence
- cloud-native scale

Those are appropriate later steps, not missing basics in the current system.
