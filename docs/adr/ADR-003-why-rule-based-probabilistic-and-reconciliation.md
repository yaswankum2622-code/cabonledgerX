# ADR-003: Why Rule-Based + Probabilistic Scoring + Reconciliation

## Status

Accepted

## Context

Commitment-failure risk can be framed in two valid but different ways:

- business-rule contradictions and threshold logic
- model-based miss probability

The two views do not always agree.

## Decision

Implement:

- contradiction flags
- rule-based risk and credibility scoring
- probabilistic miss scoring
- explicit reconciliation layer

## Rationale

- Rule-based scoring is highly interpretable and useful for governance-style reasoning.
- Probabilistic scoring captures feature interaction and probability language.
- Reconciliation turns disagreement into an explicit analytical artifact instead of hiding it.

## Consequences

Positive:

- more honest decision surface
- stronger product differentiation
- better explanation of disagreement cases

Negative:

- more complexity than a single score
- requires clearer documentation of which score should headline product surfaces

## Alternatives Considered

- heuristic score only: too narrow
- probabilistic score only: too dependent on synthetic labels
- single blended score with no audit trail: easier to show, weaker to defend
