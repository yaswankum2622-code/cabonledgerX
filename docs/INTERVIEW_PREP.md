# Interview Prep

## Elevator Pitch

TargetTruth is a climate commitment failure intelligence platform built on the CarbonLedgerX analytical engine. It starts from real public factor sources, builds a synthetic but operationally grounded company portfolio, computes emissions through an auditable activity-based calculator, reconstructs historical annual behavior, forecasts forward with both deterministic and statistical models, scores commitment risk through heuristic and probabilistic layers, reconciles disagreement, ranks interventions, and exposes everything through a premium dashboard, a read-only API, and deterministic evidence packs.

## Problem Statement

The project solves a practical decision problem:

**How do you determine whether a company’s climate commitment is credible, whether it is likely to miss, and what intervention should be prioritized first?**

Most ESG dashboards stop at disclosure or scoring. This project is stronger because it links:

- factor logic
- activity logic
- forecast logic
- risk logic
- intervention logic
- communication outputs

## Why The Project Matters

This matters because climate commitments are often discussed at the wrong level of abstraction. Stakeholders usually see a target and a score, but not:

- how emissions were calculated
- whether current direction is actually improving
- whether the score is heuristic or probabilistic
- whether different scoring layers disagree
- what action could close the gap

The project makes those layers visible.

## What Makes It Unique

- transparent activity-based calculator rather than only synthetic outcome tables
- annual historical reconstruction before forecasting
- deterministic and statistical forecast layers side by side
- explicit contradiction detection
- rule-based scoring plus calibrated probabilistic scoring
- scoring reconciliation rather than hiding disagreement
- intervention ranking tied to gap closure and cost
- evidence packs for multiple stakeholder audiences

## Why Each Major Module Exists

### Parsers

To inspect and normalize eGRID, DEFRA, and SBTi raw workbooks into a repeatable data pipeline.

### Canonical Tables

To standardize raw-sheet outputs into stable processed inputs for downstream modeling.

### Synthetic Company Panel

To create a scalable portfolio when real issuer-level activity inputs are not available in the repo.

### Factor Mapping

To connect each company to electricity and fuel factor references transparently.

### Activity Generator

To create explicit operational inputs so the repo can demonstrate a real calculation layer.

### Emissions Calculator

To translate activity into Scope 1 and Scope 2 outputs using explicit factor references.

### Historical Reconstruction

To create a plausible annual series that supports backtesting and stronger forecast narratives.

### Deterministic Forecast

To encode clear business assumptions in a way that is easy to audit.

### Statistical Forecast

To add model-based forecasting and evaluation discipline on top of reconstructed history.

### Commitment Assessment

To compare forecasted emissions against target thresholds at the relevant assessment horizon.

### Contradiction Engine

To convert business inconsistencies into explicit flags instead of vague narrative claims.

### Rule-Based Risk Scoring

To provide an interpretable additive risk layer that can be explained without model internals.

### Probabilistic Scoring

To estimate miss probability from feature interactions and to compare model calibration quality.

### Scoring Reconciliation

To explain disagreement between heuristic and probabilistic views and produce an operational score.

### Intervention Simulator And MAC Ranking

To move from diagnosis into recommended action.

### Evidence Packs

To translate analytical outputs into stakeholder-facing communication artifacts.

### Dashboard And API

To expose the system as a product rather than only a codebase.

## Algorithms Used And Why

### Column Normalization And Workbook Profiling

Used because workbook structure is messy and needs a generic inspection layer before business logic can be trusted.

### Heuristic Factor Mapping

Used because the synthetic portfolio does not have real issuer-grade location and energy metadata, so explicit proxy assignment is more honest than false precision.

### Activity-Based Emissions Calculation

Used to show direct conversion from operational inputs to emissions totals rather than relying only on preserved synthetic outcomes.

### Historical Reconstruction

Used because annual history is necessary for backtesting and trajectory reasoning, but real annual company activity data is not available in-repo.

### Deterministic Recursive Forecast

Used because it makes business assumptions inspectable: activity growth, efficiency drift, grid decarbonization, and procurement effects.

### Naive And Linear Trend Statistical Forecasts

Used because the annual time series is short. A compact model family is more defensible than complex forecasting machinery on a synthetic, reconstructed series.

### Walk-Forward Backtesting

Used because forecasting claims without evaluation are weak. The windows are short but realistic for the available annual history.

### Additive Rule-Based Scoring

Used because interviewers and product stakeholders can immediately understand how the heuristic score was formed.

### Logistic/Tree-Based Probability Modeling With Calibration Comparison

Used because the repo needs a real probability layer, but model selection should prioritize calibration quality over classifier flashiness.

### Rule-Based Reconciliation

Used because disagreement between heuristic and probabilistic scores is analytically meaningful and should not be suppressed.

### Rule-Based Intervention Simulation

Used because the portfolio is synthetic and the goal is explainability, not claiming engineering-accurate decarbonization economics.

## Why Alternative Models Were Not Chosen

### Why Not Prophet

The annual history is short and reconstructed. Prophet would add surface complexity faster than real signal in this setting.

### Why Not LSTM Or Sequence Models

The data length and realism do not justify deep sequence models. That would look performative rather than disciplined.

### Why Not XGBoost Or A Larger Model Zoo For Probabilistic Scoring

The goal was not leaderboard chasing. A compact, calibrated model comparison is easier to explain and easier to trust in a synthetic setting.

### Why Not A Full Causal Framework

True causal or counterfactual intervention modeling would require stronger real-world assumptions and data support than the current synthetic portfolio provides.

### Why Not A Database-Backed Serving Layer Yet

The current goal is a strong portfolio repo and local demo product, not multi-user production hosting.

## Likely Interview Questions And Strong Answers

### What is the project in one sentence?

It is a climate commitment intelligence system that links emissions calculation, historical reconstruction, forecasting, risk scoring, intervention ranking, and stakeholder-ready outputs in one product-shaped repository.

### Why did you build both heuristic and probabilistic scoring?

Because they answer different questions. The heuristic layer is highly interpretable and good for contradiction logic; the probabilistic layer is better for estimating miss likelihood from interactions. The reconciliation layer exists because disagreement between them is itself valuable information.

### Why is the activity-based calculator important if you already had a baseline table?

Because a preserved synthetic baseline is not enough to demonstrate analytical credibility. The calculator proves the system can go from explicit activity inputs and factor references to emissions totals and audit the difference.

### Why did you reconstruct history instead of only forecasting from the baseline?

Because annual history gives the system a stronger narrative and makes backtesting possible. Without reconstruction, the forecast would be much weaker from an evaluation standpoint.

### Why keep both deterministic and statistical forecasts?

Because deterministic forecasts are better for explaining assumptions, while statistical forecasts are better for evaluation and model selection. They serve different product needs.

### Why did you choose a compact statistical model family?

Because the history is annual, short, and reconstructed. A compact model family is more defensible than using a heavier forecasting framework that would create a false sense of rigor.

### Why is the probabilistic model not enough on its own?

Because the labels are synthetic and deterministic. A calibrated probability is useful, but it should not replace business-rule contradictions and operational judgment.

### What is the most product-like part of the repo?

The combination of scoring reconciliation, intervention ranking, evidence packs, a premium dashboard, and a thin API. That turns the analytics into a coherent product surface.

### What is the biggest limitation?

The issuer-level portfolio is synthetic. That means the system demonstrates architecture and methodology well, but it does not yet claim real-world calibrated commitment-failure accuracy.

### If you had more time, what would you improve first?

I would add real issuer activity inputs and a stronger external validation set before expanding model complexity.

## Role-Specific Positioning

### Financial Analyst

How to frame it:

- emphasize baseline versus projected emissions
- explain target gap and intervention economics
- highlight evidence packs and lender/investor use cases

Strong answer:

I built the project to move beyond a generic climate score. It shows baseline emissions, 2030 outlook, gap to target, and the most cost-effective first intervention. That makes it easier to discuss transition exposure in a finance-oriented decision context.

### Business Analyst

How to frame it:

- emphasize requirements decomposition
- explain process design
- show how raw datasets became a structured analytical workflow

Strong answer:

The project demonstrates how to take an ambiguous business problem and turn it into staged analytical components: data profiling, canonical modeling, scoring logic, action ranking, and stakeholder-facing outputs.

### Product Analyst

How to frame it:

- emphasize user surfaces
- show reconciliation and evidence layers as product choices
- explain why the dashboard is structured section-by-section

Strong answer:

I treated disagreement between scores as a product problem, not only a modeling problem. That is why the dashboard has an explicit reconciliation section instead of hiding model conflict behind one badge.

### Data Analyst / Data Scientist

How to frame it:

- emphasize table design, reconstruction logic, forecasting, evaluation, and calibration
- point out where the project is honest about synthetic labels

Strong answer:

The strongest data-science choice in the repo is that I limited model complexity to what the data could support and kept evaluation explicit. The point was trustworthy methodology, not decorative complexity.

### AI Engineer / Software Engineer

How to frame it:

- emphasize modular pipeline design
- show API and dashboard separation
- explain parquet-backed data access
- show smoke tests and docs discipline

Strong answer:

The repo is intentionally organized as a production-shaped package with scripts, tests, typed modules, API schemas, dashboard helpers, ADRs, and layered docs. The design goal was maintainability and demo-readiness, not a notebook prototype.

## Limitations

- synthetic company-level labels and activity inputs
- proxy factor assignment in some geographies
- compact statistical model family
- local parquet-backed serving only
- no auth or multi-user state

## What I Would Improve In v2

- add real company activity inputs or ingestion templates
- improve non-US factor coverage
- add curated screenshot assets and polished release notes
- add stronger interval calibration and richer forecast comparison
- add better API filtering and batch access patterns

## How I Would Productionize It

1. move generated artifacts to managed storage
2. separate build jobs from serve jobs
3. add API versioning and auth
4. add data and model version registries
5. add CI checks for artifact freshness and schema integrity
6. add observability for API usage and dashboard health
7. replace synthetic labels with observed commitment outcome data where available

## Closing Interview Positioning

The strongest way to present this project is:

- not as a claim of full real-world carbon accounting
- but as a serious, product-shaped climate intelligence system that demonstrates end-to-end analytical design, careful modeling choices, transparent tradeoffs, and strong communication surfaces
