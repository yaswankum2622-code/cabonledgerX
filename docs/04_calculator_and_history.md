# 04 Calculator And History

## Why This Layer Exists

The earlier baseline workflow already provided modeled company emissions. That was useful for product scaffolding, but not sufficient if the project was going to claim analytical rigor. The calculator and history layers were added to close that gap.

They answer two credibility questions:

1. Can the system compute emissions from activity inputs and factor references rather than only preserve synthetic totals?
2. Can the system reconstruct a plausible annual history instead of starting analysis at a single baseline snapshot?

## Activity Generator

Main code:

- `src/carbonledgerx/models/activity_generator.py`

Output:

- `company_activity_inputs.parquet`

For each synthetic company, the generator creates annual activity proxies such as:

- electricity use in MWh
- natural gas use in MMBtu
- diesel use in liters
- fleet size proxy
- facility count proxy
- floor area proxy
- production index proxy

Design choices:

- sector-conditioned assumptions
- deterministic or fixed-seed generation
- no hidden random noise layer
- values tied back to the synthetic company profile and factor-mapping context

## Activity-Based Emissions Calculator

Main code:

- `src/carbonledgerx/models/emissions_calculator.py`

Output:

- `company_emissions_calculated.parquet`

The calculator computes:

- Scope 1 from natural gas and diesel activity
- Scope 2 location-based from electricity times location-based factor reference
- Scope 2 market-based from electricity times procurement-adjusted factor reference

Important properties:

- factor references are carried into the output
- unit conversions are explicit
- proxy use is documented in notes
- calculator totals are compared against the earlier baseline rather than silently replacing it

## Why This Matters

This layer changes the character of the repository. Without it, the system risks looking like a scoring and dashboard exercise on synthetic outputs. With it, the repo demonstrates:

- activity-to-emissions translation
- factor selection discipline
- traceability from inputs to totals
- audit-oriented product thinking

## Historical Reconstruction

Main code:

- `src/carbonledgerx/models/history_reconstructor.py`
- `src/carbonledgerx/models/history_summary.py`

Outputs:

- `company_emissions_history_annual.parquet`
- `company_history_summary.parquet`

The reconstruction engine creates one row per company per year for 2015 through 2024.

It is anchored to:

- current/base-year activity inputs
- calculated emissions outputs
- factor mapping context

It works backward and forward using explicit annual proxy assumptions for:

- activity growth
- scope 1 efficiency drift
- grid decarbonization
- market-based procurement improvement

## Why The Reconstruction Is Defensible

The history layer does not pretend to recover real historical company disclosures. Instead, it produces:

- internally consistent annual trajectories
- a transparent base for backtesting and forecast model selection
- a stronger analytical story than a single-year baseline table

That is the correct level of ambition for a synthetic portfolio project.

## Limitations

- the historical series is reconstructed, not observed
- sector growth and efficiency assumptions are proxy-based
- market-based procurement history is modeled, not issuer-specific
- factor histories are simplified rather than fully jurisdiction-specific

## Why This Is Still High-Signal

Even with those limitations, this layer is valuable because it demonstrates:

- emissions system design
- unit-aware calculation logic
- annual time-series reconstruction
- a careful distinction between observed data and modeled data
