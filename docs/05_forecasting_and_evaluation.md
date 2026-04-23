# 05 Forecasting And Evaluation

## Forecasting Strategy

TargetTruth uses two forecast layers on purpose:

- a deterministic recursive forecast
- a statistical forecast selected through backtesting

These serve different roles.

### Deterministic Forecast

Main code:

- `src/carbonledgerx/models/forecasting.py`

Purpose:

- express explicit business assumptions
- make activity growth, efficiency, grid decarbonization, and procurement effects easy to audit
- support commitment assessment logic directly

Strength:

- high interpretability

Weakness:

- less adaptive to company-specific historical shape

### Statistical Forecast

Main code:

- `src/carbonledgerx/models/statistical_forecasting.py`
- `src/carbonledgerx/models/backtesting.py`
- `src/carbonledgerx/models/forecast_evaluation.py`

Purpose:

- add a data-driven forecast layer on top of annual reconstructed history
- compare simple model families rather than assume one forecast form
- produce evaluation artifacts that make forecast quality discussable

Implemented candidate models:

- naive last value
- linear trend

Model selection:

- walk-forward backtests
- mean APE and mean absolute error
- selected model stored in company summary output

## Why The Statistical Layer Is Deliberately Compact

The historical series is annual and only spans 2015 to 2024. That means:

- deep sequence models are unjustified
- Prophet or more complex time-series tooling would add complexity faster than signal
- a compact model comparison is easier to defend in interviews

The repository therefore prioritizes honest evaluation over model-library breadth.

## Backtesting Design

Walk-forward windows:

- train through 2021, predict 2022
- train through 2022, predict 2023
- train through 2023, predict 2024

Backtest output:

- actual year
- model name
- actual total MB emissions
- predicted total MB emissions
- absolute error
- APE
- interval coverage flag

This is enough to demonstrate forecasting discipline without turning the repo into a tuning exercise.

## Forecast Intervals

The interval logic is intentionally conservative. It uses residual and backtest error spread rather than pretending to provide perfect probabilistic uncertainty.

That matters because:

- the repo is synthetic
- the history is reconstructed
- interval honesty is more valuable than false precision

## Evaluation Artifacts

Key outputs:

- `company_forecast_backtest_results.parquet`
- `company_forecast_summary.parquet`
- `outputs/evaluation/backtest_report.md`
- `outputs/evaluation/forecast_metrics.json`
- `outputs/evaluation/calibration_summary.json`
- `outputs/evaluation/forecast_metric_plots.png`

These artifacts allow the repo to answer:

- which model won
- how accurate the selected model is
- whether intervals are too tight or too wide
- what the portfolio-level forecast direction looks like

## Why This Layer Is Important

Without evaluation, a forecast chart is just a picture. This layer makes the forecast reviewable by:

- showing the backtesting procedure
- documenting selection logic
- exposing interval quality
- keeping the models simple enough to explain clearly

## Limitations

- the forecast target is still synthetic portfolio behavior
- the statistical model family is intentionally narrow
- interval calibration is useful for demo rigor but not equivalent to real-world validation on observed issuer data

Those limitations are acceptable because the repository never claims otherwise.
