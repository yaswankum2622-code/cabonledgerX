# TargetTruth

## Climate Commitment Failure Intelligence & Disclosure-Risk Platform

**Tagline:** *An early-warning system for climate commitments that are likely to fail before the auditor, lender, investor, or regulator finds out.*

---

## 1. Project Identity

### Working Name
**TargetTruth**

### Alternate Names
- ClimateCred
- CommitmentIQ
- CarbonLedgerX

### Final Positioning
This is **not** a carbon accounting dashboard.
This is **not** an ESG reporting wrapper.
This is **not** a generic climate analytics project.

It is a **climate commitment failure prediction and disclosure contradiction platform**.

The core idea is simple:
Many companies publicly announce emissions-reduction targets, but very few actually test whether those targets are **statistically achievable**, **operationally plausible**, and **internally consistent** with their historical energy mix, capex trajectory, intervention plans, and grid assumptions.

TargetTruth is designed to answer that gap.

---

## 2. Problem Statement

### The Real Problem
Companies publish climate commitments such as:
- “50% reduction by 2030”
- “Net zero by 2040”
- “On track with SBTi-aligned pathway”

But in practice:
- their baselines are messy,
- their forecasts are simplistic,
- their assumptions are hidden,
- their intervention plans are vague,
- and their disclosures often sound more confident than the underlying numbers justify.

This creates a major blind spot:

### Nobody is rigorously answering:
1. **What happens if the company changes nothing?**
2. **What is the probability the target is missed?**
3. **Are the company’s public claims mathematically consistent with the data?**
4. **Which assumptions are carrying the forecast?**
5. **What is the cheapest realistic path to improve target-achievement odds?**

### Why this matters
When a company overstates confidence in a climate target:
- investors misprice risk,
- lenders may extend sustainability-linked financing on weak assumptions,
- consultants waste weeks building spreadsheet forecasts,
- boards make capital decisions on shaky planning,
- regulators and auditors may later treat the gap as disclosure risk.

### What existing tools do poorly
Most current tools focus on:
- retrospective emissions reporting,
- data collection,
- dashboards,
- factor libraries,
- compliance workflows.

Very few treat climate commitments as a **forecasting + credibility + contradiction detection** problem.

That is the wedge.

---

## 3. The Unique Wedge

### This project becomes highly unique because it combines five things in one system:

1. **Counterfactual emissions forecasting**
2. **Commitment failure probability scoring**
3. **Public disclosure contradiction detection**
4. **Peer-relative plausibility benchmarking**
5. **Intervention ranking via marginal abatement economics**

### Why that matters
Each of these ideas exists somewhere in isolation.
But putting them together into a single workflow for climate target credibility is uncommon.

### The one-line wedge
> TargetTruth does not merely track emissions. It tests whether a company’s climate promise is actually believable.

---

## 4. Who Has This Pain

### Primary users
- ESG strategy teams at mid-cap and upper-mid-cap companies
- Sustainability consultants
- Climate fintech lenders and underwriters
- ESG analysts and stewardship teams
- Internal audit / disclosure review teams
- Corporate strategy teams evaluating decarbonization plans

### Secondary users
- Activist investors
- Ratings/research teams
- Transition-risk analysts
- Sustainability product teams
- Climate-focused consulting firms

---

## 5. Quantified Business Impact

### For operating companies
- Reduce baseline + forecast preparation from **4–6 weeks** to **2–3 hours**
- Replace fragile spreadsheet workflows with a reproducible model pipeline
- Turn “on track / not on track” into a **calibrated miss probability**
- Identify which intervention improves target-achievement odds the most
- Surface disclosure inconsistency before investor or auditor review

### For consultants
- Faster diagnostic workflows
- Reusable scenario analysis
- Better client conversations around credibility, not just reporting

### For lenders / investors
- Faster diligence on whether climate-linked commitments are plausible
- Better challenge questions
- More rigorous transition-risk evaluation

### For portfolio value
This project shows strength in:
- forecasting
- causal reasoning
- calibration
- domain depth
- business-product framing
- production engineering
- analytical decision support

---

## 6. What Makes It Feel 10/10 Unique

### Not “never existed in history” unique
That standard is unrealistic.

### But it can be “shockingly uncommon and memorable” unique if framed correctly
The project should be positioned around these standout features:

#### A. Commitment Credibility Score
A calibrated score representing how believable a company’s climate target is, given observed history and modeled scenarios.

#### B. Disclosure Contradiction Engine
Flags when public messaging such as “on track” is inconsistent with modeled miss probability, trend, capex assumptions, or peer context.

#### C. Target Stress Testing
Allows scenario stress-testing under:
- slower grid decarbonization
- delayed intervention rollout
- higher activity growth
- lower capex realization
- energy-price changes
- carbon-price shocks

#### D. Peer Plausibility Layer
Benchmarks commitment ambition and achievability relative to similar firms by sector, size, geography, and energy mix.

#### E. Evidence Pack Generation
Produces structured outputs such as:
- auditor note
- investor challenge memo
- lender diligence summary
- board-level climate risk brief

That product framing makes it far more unique than “carbon forecasting platform.”

---

## 7. IN SCOPE

## 7.1 MVP Scope
The MVP must produce a full, defensible end-to-end analytical loop.

### MVP Features
1. Public factor ingestion
   - EPA eGRID
   - DEFRA / BEIS conversion factors
   - optional country-level historical grid factors

2. Company activity data loader
   - CSV upload
   - synthetic company panel generator

3. Scope 1 and Scope 2 calculator
   - location-based
   - market-based

4. Historical baseline reconstruction
   - 2015–2024 annual history

5. Forecasting engine
   - multi-year forecast to 2030
   - 80/95% prediction intervals

6. Commitment failure probability scoring
   - probability of missing target
   - confidence band around risk score

7. Counterfactual intervention simulation
   - fleet electrification
   - renewable PPA
   - HVAC efficiency upgrade
   - fuel switch

8. Marginal Abatement Cost ranking
   - intervention
   - expected abatement
   - cost per tCO2e

9. Contradiction engine
   - compare modeled reality with public “on track” claims
   - generate contradiction flags

10. Evaluation harness
   - backtest forecasts
   - calibration curves
   - reliability analysis

11. API layer
12. Dashboard layer
13. Evidence pack export

---

## 7.2 Advanced Scope

### Advanced Features
1. Peer plausibility benchmarking
2. Sector-conditioned commitment realism score
3. Scenario stress-test library
4. Commitment sensitivity heatmap
5. Assumption attribution engine
6. Company-vs-peer optimism gap chart
7. Portfolio-level climate diligence mode for lenders / funds
8. Optional final narrative generation using an LLM only after the analytical core is complete

---

## 7.3 Explicitly Out of Scope
To preserve uniqueness and avoid feature creep, the following are out of scope for v1:

- Full Scope 3 inventory
- Generic ESG reporting workflow management
- Multi-tenant SaaS auth system
- Blockchain / tokenization features
- Arbitrary chatbot assistant
- RAG over policy documents in the core product
- Mobile app
- Real NDAs/private company integrations

---

## 8. MVP Ladder — Build in This Order

### Prompt 01 — Repo skeleton and settings
Success criterion:
- project installs cleanly
- config loads correctly
- directory structure is frozen

### Prompt 02 — Factor ingestion pipeline
Success criterion:
- public factor files download or parse
- counts and schema checks pass

### Prompt 03 — Synthetic company panel generator
Success criterion:
- generates reproducible 500-company dataset
- includes sector, geography, energy mix, growth, target metadata

### Prompt 04 — Emissions calculator
Success criterion:
- Scope 1 / Scope 2 numbers compute correctly
- known test cases are within tolerance

### Prompt 05 — Historical baseline reconstruction
Success criterion:
- per-company annual emissions history generated
- quality checks pass

### Prompt 06 — Forecasting engine
Success criterion:
- forecast to 2030 with intervals
- walk-forward backtest produces metrics

### Prompt 07 — Counterfactual simulator
Success criterion:
- intervention scenarios change forecast plausibly
- sensitivity checks run

### Prompt 08 — Failure risk scorer
Success criterion:
- target miss probability computed
- confidence and components returned

### Prompt 09 — Contradiction engine
Success criterion:
- public-claim status can be compared against forecasted risk and historical trend
- contradiction flags generated

### Prompt 10 — MAC curve engine
Success criterion:
- interventions ranked by abatement per dollar and cost per ton

### Prompt 11 — Peer benchmarking layer
Success criterion:
- sector/geography-adjusted plausibility comparison generated

### Prompt 12 — FastAPI endpoints
Success criterion:
- health check + all endpoints return live outputs

### Prompt 13 — Streamlit dashboard
Success criterion:
- end-to-end UX works with charts, gauges, and scenario controls

### Prompt 14 — Evaluation harness
Success criterion:
- backtest report and calibration plots generated

### Prompt 15 — Evidence pack exporter
Success criterion:
- structured board/investor/lender/auditor summary generated from outputs

### Prompt 16 — Docker, CI, docs, release
Success criterion:
- clean startup from fresh clone
- release candidate ready

---

## 9. Data

## 9.1 Inputs

### Real Public Inputs
- EPA eGRID factors
- DEFRA / BEIS factor packs
- SBTi target data
- optional historical grid-intensity data by region/country
- optional public disclosed emissions datasets

### Synthetic Inputs
A reproducible synthetic company panel with:
- sector
- revenue
- energy mix
- geography
- facilities/fleet estimate
- historical growth trajectory
- target type
- target year
- target reduction %
- intervention choices
- public claim label (e.g. on track / accelerating / behind)

### User Inputs
- company selector
- commitment target parameters
- intervention assumptions
- capex assumptions
- grid-decarbonization scenario
- demand/activity growth assumptions

---

## 9.2 Outputs

### Core outputs
- historical baseline emissions
- 2030 forecast with intervals
- miss probability score
- commitment credibility score
- contradiction flags
- ranked intervention list
- MAC curve
- peer plausibility percentile
- sensitivity analysis output

### Exportable outputs
- JSON API payloads
- backtest HTML report
- calibration chart
- reliability diagram
- board risk note
- investor challenge memo
- lender diligence summary
- auditor-facing evidence summary

---

## 10. Technology Stack

## 10.1 Core Modeling
- **Polars** for data processing
- **DuckDB** for local analytical storage
- **Prophet** for interpretable trend/seasonality forecasting
- **LightGBM quantile regression** for interval estimates
- **DoWhy** for counterfactual framing and refutation
- **EconML** for intervention effect estimation where useful
- **scikit-learn** for calibrated classification and scoring
- **NumPy / SciPy** for simulation and uncertainty support

## 10.2 Serving & Product
- **FastAPI** for backend API
- **Pydantic** for schemas
- **Streamlit** for UI
- **Altair / Matplotlib** for charts
- **Docker Compose** for reproducibility
- **GitHub Actions** for CI

## 10.3 Optional, Non-Core
- Small LLM only for final natural-language report paragraph generation
- This must remain optional and outside the analytical core

### Deliberate choices that make the design stronger
- no agent framework
- no vector database
- no RAG dependency
- no unnecessary multi-model LLM stack
- no “AI for the sake of AI” choices

---

## 11. Concepts Covered — Master Checklist

### Forecasting
- multi-step forecasting
- prediction intervals
- walk-forward validation
- MAPE / sMAPE / coverage
- trend vs seasonality interpretation

### Causal / Counterfactual
- difference between prediction and causation
- intervention analysis
- identification assumptions
- sensitivity / refutation logic

### Probabilistic Risk
- calibration
- reliability diagrams
- probability-of-failure interpretation
- uncertainty communication

### Domain / Climate
- Scope 1 vs Scope 2
- location-based vs market-based accounting
- commitment target structures
- grid decarbonization assumptions
- marginal abatement economics
- disclosure-risk framing

### Product / Business
- board-ready communication
- investor/lender evidence framing
- what makes a risk signal actionable
- balancing rigor with usability

### Engineering
- reproducible pipelines
- local analytical infra
- API design
- documentation discipline
- CI/CD and packaging

---

## 12. Algorithm Choices — Why Each Was Selected

### Prophet + Quantile LightGBM Ensemble
Chosen because:
- explainable enough for interviews and stakeholder discussion
- handles trend and simple seasonality well
- quantile model adds uncertainty bands without overcomplicating architecture

Not chosen:
- Transformer forecasting because it is harder to defend and unnecessary for this signal

### Calibrated Logistic Risk Model
Chosen because:
- risk probability calibration matters more than fancy classification
- readable, stable, and easier to explain

Not chosen:
- deep models because the key output is believable probability, not leaderboard accuracy

### DoWhy / Counterfactual Framework
Chosen because:
- the project’s uniqueness depends on defensible intervention reasoning
- refutation is a high-signal differentiator

Not chosen:
- naive before/after comparisons, because they are too weak for credibility claims

### Polars
Chosen because:
- modern, fast, and a signal that the stack is current

### DuckDB
Chosen because:
- analytical workload fits DuckDB better than transactional storage
- easy local reproducibility

### Median / Robust Aggregation
Chosen because:
- emissions and intervention outcomes can have skew and outliers
- robustness matters more than fragile optimization tricks

---

## 13. Advanced Advantages

1. **Forecasts target failure, not just emissions**
2. **Flags contradiction between modeled reality and public optimism**
3. **Benchmarks plausibility against peers**
4. **Stress-tests commitments under realistic adverse scenarios**
5. **Ranks interventions by economic impact, not just carbon impact**
6. **Produces evidence-ready outputs for real stakeholders**
7. **Runs mostly offline and locally**
8. **Shows strong range after LLM-heavy projects**

---

## 14. Future Work

### High-value next steps
1. Scope 3 Category 1 extension
2. PDF disclosure parser for existing climate reports
3. Carbon-price scenario engine
4. Peer-based commitment realism model
5. Portfolio mode for investors and lenders

### Medium-term
6. More advanced probabilistic scenario simulation
7. Better sector-specific intervention libraries
8. International factor expansion
9. Real company connector adapters

### Nice to have
10. Narrow narrative assistant for explanation-only mode
11. Sector-specific benchmarking dashboards
12. Commitment revision recommender

---

## 15. Target Roles & Companies

### Best-fit roles
- Financial Analyst
- Business Analyst
- Product Analyst
- Data Analyst
- Data Scientist
- AI Engineer
- Software Engineer
- Climate / ESG Analytics roles
- Quantitative transition-risk roles

### Why it fits your role mix
It combines:
- finance/risk thinking
- business impact framing
- product logic
- data science
- engineering delivery

### Best-fit companies
- climate-tech firms
- ESG analytics platforms
- sustainability consultancies
- climate fintech lenders
- stewardship/research teams at asset managers
- Big 4 sustainability and analytics groups
- corporate sustainability data teams

---

## 16. This Can Be a Prototype or Not?

### Recommended answer
This should be built as a **production-targeted portfolio system**, not a toy prototype.

### What that means
- real docs
- tests
- backtests
- calibration artifacts
- Docker
- release tag
- screenshots
- interview prep notes

### Honest positioning
It is not a full enterprise product.
But it is beyond prototype and should be presented as:

> **A production-shaped analytical intelligence platform built for serious portfolio and interview signal.**

---

## 17. Final Uniqueness Verdict

### If judged honestly
- never existed anywhere: **no**
- never practiced by anyone: **no**
- rarely combined this way: **yes**
- highly uncommon in candidate portfolios: **yes**
- standout-capable when executed seriously: **absolutely yes**

### Best final claim
> TargetTruth is not unique because it invents a brand-new scientific method. It is unique because it combines climate forecasting, contradiction detection, calibrated commitment-failure scoring, peer plausibility benchmarking, and intervention economics into a single product-shaped system that very few candidates would think to build and very few would package this clearly.

---

## 18. Final Decision Statement

If the goal is just “good,” CarbonLedger is enough.

If the goal is **10/10 memorable**, then this project should be finalized and presented as:

# **TargetTruth**
## *Climate Commitment Failure Intelligence & Disclosure-Risk Platform*

That is the stronger identity.

