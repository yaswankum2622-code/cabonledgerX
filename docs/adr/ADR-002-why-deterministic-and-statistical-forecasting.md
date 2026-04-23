# ADR-002: Why Both Deterministic And Statistical Forecasting

## Status

Accepted

## Context

The project needed forecasting that was both:

- explainable to stakeholders
- evaluable through backtesting

No single method handled both goals equally well.

## Decision

Keep two forecast layers:

- deterministic recursive forecast
- statistical forecast selected through walk-forward backtests

## Rationale

- The deterministic layer exposes explicit business assumptions such as growth, efficiency drift, and procurement effects.
- The statistical layer supports model comparison and forecast evaluation.
- Annual history is short and reconstructed, so the statistical family should stay compact and interpretable.

## Consequences

Positive:

- stronger analytical credibility
- better interview story around evaluation
- clearer separation between assumption-driven and data-driven views

Negative:

- more outputs to explain
- possible confusion if consumers expect one single forecast truth

## Alternatives Considered

- deterministic only: too weak on evaluation discipline
- statistical only: too weak on assumption transparency
- complex time-series model zoo: not justified by the current data shape
