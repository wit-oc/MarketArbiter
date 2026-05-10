# Arbiter Pre-SR Backtest Controls V1

Status: implemented prep slice while canonical SR bundle generation is pending.

## Purpose

These controls let Arbiter/backtest work proceed before the final Surveyor SR levels are ready, without tuning level selection or claiming edge.

The first implemented control is a deterministic time-shift negative control:

- source dataset shape: `foxian_retest_backtest_dataset_v0` or compatible
- output dataset shape: backtest-consumable `trade_candidates` + `event_study_rows`
- contract: `arbiter_backtest_control_dataset_v1`
- implementation: `market_arbiter.arbiter.backtest_controls`
- CLI: `python3 -m market_arbiter.ops.strategy_backtest_control_run`
- tests: `tests/test_backtest_controls.py`

## Time-shift negative control

A valid SR/retest hypothesis should usually degrade when the same trade candidate is moved away from its observed retest timestamp.

The control keeps these unchanged:

- symbol
- side
- stop/invalidation assumption
- target/risk/cost fields
- existing backtest runner compatibility

It changes:

- `entry_ts`
- linked `event_study_rows[*].event_ts`
- `entry_event_id` / `event_id` suffixes
- explicit `control` provenance metadata

Rows that cannot be shifted inside the symbol candle series are omitted and reported under `control.skipped_rows`.

## Intended use once canonical SR bundles land

For every candidate SR bundle replay:

1. run the primary replay/backtest normally
2. generate at least one forward time-shift control dataset
3. run the same backtest config on the control dataset
4. compare primary vs control expectancy/drawdown using `strategy_backtest_control_run_report_v1`
5. require primary performance to beat the control on expectancy, drawdown, and symbol/year robustness before promotion

Suggested initial controls:

```bash
PYTHONPATH=. python3 -m market_arbiter.ops.strategy_backtest_control_run \
  --dataset artifacts/strategy_backtests/<run>/canonical_surveyor_retest_dataset.json \
  --ohlcv-dir <per-symbol-ohlcv-dir> \
  --timeframe 1d \
  --control-shifts 20,60 \
  --output artifacts/strategy_backtests/<run>/control_report.json
```

Control IDs:

- `time_shift_forward_20bars`
- `time_shift_forward_60bars`
- optionally `time_shift_backward_20bars` for historical-only diagnostic runs where lookahead concerns are clearly labeled

## Promotion impact

A candidate variant should not move from research-only to paper/shadow unless:

- canonical SR bundle replay is positive out of sample,
- time-shift controls degrade materially,
- lower-timeframe execution-order ambiguity is bounded or resolved,
- no single symbol/year is carrying the result,
- the promotion gate passes with the intended universe and timeframe.

This is a guardrail against accidentally optimizing Arbiter policies around spurious timestamp/market-regime effects while SR generation is still being finalized.
