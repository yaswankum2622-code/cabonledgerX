# 07 Interventions And MAC

## Purpose Of The Intervention Layer

A climate intelligence product should not stop at identifying failure risk. It should also propose where action should begin. The intervention layer turns risk outputs into structured response options.

## Intervention Library

Main code:

- `src/carbonledgerx/models/intervention_library.py`

Implemented intervention families:

- renewable PPA
- fleet electrification
- HVAC efficiency upgrade
- onsite solar
- low-carbon fuel switch
- process efficiency program

Each intervention includes:

- category
- primary scope impact
- default adoption assumptions
- cost bucket
- sector applicability notes

## Intervention Simulator

Main code:

- `src/carbonledgerx/models/intervention_simulator.py`

Output:

- `company_intervention_scenarios.parquet`

The simulator is intentionally rule-based rather than Monte Carlo or causal. For each company-intervention pair, it estimates:

- modeled abatement
- abatement percent
- modeled cost
- cost per tCO2e
- projected emissions after intervention
- whether the target gap closes fully or partially

This design is appropriate for the current stage because the input portfolio is synthetic and the goal is explainability.

## MAC Ranking

Main code:

- `src/carbonledgerx/models/mac_ranking.py`

Output:

- `company_mac_rankings.parquet`

The MAC-style layer ranks interventions using:

- cost per tCO2e
- abatement magnitude
- feasibility rank
- priority rank

This gives the dashboard and evidence packs a grounded answer to:

**What should the company do first, and why?**

## Intervention Intelligence

Output:

- `company_intervention_intelligence.parquet`

This table reduces the full scenario set to a company-level recommendation surface:

- best intervention name
- best intervention cost
- best intervention abatement
- gap-closure status
- recommendation summary

That makes the intervention layer usable in:

- the dashboard
- the API
- board, investor, and lender evidence packs

## Why The Current Design Is Defensible

The implemented intervention logic is intentionally not overfit. It uses:

- sector-conditioned applicability multipliers
- direct adoption scaling
- broad cost heuristics
- deterministic outputs

That is appropriate because:

- the portfolio is synthetic
- the goal is ranking and explainability
- the repo is demonstrating product intelligence, not claiming engineering-grade capex modeling

## What This Layer Adds To The Product

Without this layer, the project would stop at risk detection. With it, the system becomes more decision-oriented:

- not only “this target looks weak”
- but also “this is the best first lever to pull”

That is a much stronger portfolio story.
