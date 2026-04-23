# 06 Scoring And Reconciliation

## Why This Layer Exists

Climate commitment risk should not collapse into one unexamined number. In practice, different analytical lenses capture different failure modes:

- explicit rule logic catches visible contradictions and policy-relevant heuristics
- probabilistic models capture nonlinear feature interactions
- reconciliation is needed when those two views disagree

TargetTruth implements all three rather than pretending one score is universally sufficient.

## Contradiction Engine

Main code:

- `src/carbonledgerx/models/contradiction_engine.py`

Output:

- `company_contradiction_flags.parquet`

Examples of implemented flags:

- optimistic claim but modeled miss
- negative implied reduction
- large target gap
- near-term target underperforming
- low renewable share
- weak market-based procurement
- capped target year
- ambition without supporting operating signals

This layer is intentionally explicit and auditable. It answers:

**What concrete conditions make the company’s climate story look inconsistent?**

## Rule-Based Scoring

Main code:

- `src/carbonledgerx/models/risk_scoring.py`

Outputs:

- `company_commitment_risk_scores.parquet`
- fields surfaced later in `company_commitment_intelligence.parquet`

The heuristic layer uses additive components such as:

- gap magnitude
- contradiction burden
- claim inconsistency
- support weakness
- timing pressure
- trend weakness

Why it matters:

- easy to explain
- easy to tune in policy/product conversations
- useful even when training labels are weak or synthetic

## Probabilistic Scoring

Main code:

- `src/carbonledgerx/models/probabilistic_scoring.py`
- `src/carbonledgerx/models/probability_evaluation.py`

Outputs:

- `company_commitment_probability_scores.parquet`
- `probabilistic_model_comparison.parquet`

Candidate models:

- logistic regression
- random forest
- histogram gradient boosting
- calibrated logistic regression
- calibrated tree-based candidate

Selection logic:

- calibration quality first
- Brier score next
- ROC-AUC secondary
- explainability as tie-break context

This is important because the repository is trying to estimate **miss probability**, not only rank-order risk.

## Why Synthetic Labels Are Still Useful Here

The current label set is deterministic and synthetic, based on modeled miss conditions. That means the probabilistic layer should be interpreted as:

- a probability-engineering demonstration
- a calibration and model-selection exercise
- a comparison layer against the heuristic score

It should not be presented as a real-world issuer probability benchmark.

## Reconciliation Layer

Main code:

- `src/carbonledgerx/models/scoring_reconciliation.py`
- `src/carbonledgerx/models/scoring_audit.py`

Outputs:

- `company_scoring_reconciliation.parquet`
- `scoring_disagreement_segments.parquet`
- reconciliation report and agreement plot artifacts

The reconciliation layer exists because disagreement is analytically useful. It classifies cases such as:

- aligned
- heuristic overcalling candidate
- probability overcalling candidate
- investigate label definition
- mixed signal case

It then produces a recommended operational score and band.

## Why This Is A Strong Design Choice

Many projects would choose one score and suppress disagreement. This repo does the opposite:

- it exposes the disagreement
- it explains the reason
- it provides a practical operational view without deleting the raw views

That is a stronger product design for real decision support.

## Recommended Product Surface

The current best product posture is:

- headline: recommended operational risk band
- numeric drill-down: calibrated miss probability
- diagnostic detail: rule-based score and reconciliation notes

This matches the current implemented logic and avoids overselling any single scoring layer.
