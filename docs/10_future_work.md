# 10 Future Work

## Immediate Next Steps

The most sensible near-term improvements are:

1. add real deployment scaffolding for dashboard and API
2. add a small curated screenshot asset set into the repo
3. improve API ergonomics with filtering, pagination, and response metadata
4. formalize data acquisition instructions for raw public datasets

## Analytical Improvements

### Real Company Activity Inputs

The current company portfolio is synthetic. A major next step would be:

- user-supplied activity inputs
- issuer-level operational profiles
- explicit entity matching

### Better Factor Coverage

The current system already uses documented proxies well, but future work should include:

- broader non-US electricity factors
- cleaner factor lineage tables
- explicit factor versioning

### Forecasting v2

The current statistical layer is intentionally compact. A second version could add:

- more feature-aware time-series models
- improved interval calibration
- scenario-specific forecast branches
- evaluation against real observed corporate trajectories

### Probabilistic Scoring v2

The probability layer would be much stronger with:

- real observed commitment outcomes
- real calibration validation over time
- cohort-specific label definitions
- external validation sets

## Product Improvements

### API v2

- auth
- pagination
- richer query filters
- batch company endpoints
- explicit versioning

### Dashboard v2

- curated presentation mode
- downloadable PDF evidence briefs
- comparison mode between companies
- saved views and preset analytical stories

### Data Platform v2

- database-backed serving
- scheduled rebuilds
- artifact registry
- cloud deployment and CI-based publishing

## Productionization Path

If this were taken beyond portfolio mode, the likely path would be:

1. move from parquet-only serving to a small warehouse or OLAP backend
2. separate build jobs from serving jobs
3. add API auth and rate limiting
4. add dataset versioning and model versioning
5. formalize monitoring and evaluation dashboards
6. define a real release process with tagged data/model artifacts

## What Should Not Be Done Too Early

The repo should avoid premature complexity such as:

- heavy orchestration frameworks
- unnecessary microservices
- advanced model families without better real data
- multi-cloud deployment work before the analytical core has stabilized

The strongest next step is still better real data, not more platform ceremony.
