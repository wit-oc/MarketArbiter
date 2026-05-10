# Arbiter Retest Execution Contract V1

Status: research-candidate contract, reusable by Arbiter modules and replay tests.

Intent: preserve Surveyor as the descriptive analysis layer while making Arbiter's tradeability decisions explicit, testable, and reusable.

## Boundary

Surveyor supplies:

- durable `sr_zones` / daily-major level facts,
- `selection_score`, family provenance, and zone bounds,
- structure/reaction/base evidence.

Arbiter decides:

- whether the current retest is actionable,
- where the thesis is invalidated,
- how much risk to allocate,
- whether and how to DCA into the setup.

## Reusable modules

- `market_arbiter.arbiter.setup_score`
  - contract: `arbiter_retest_setup_score_v1`
  - key function: `score_retest_setup(...)`
- `market_arbiter.arbiter.stop_policy`
  - contract: `arbiter_retest_stop_policy_v1`
  - key function: `resolve_retest_stop(...)`
- `market_arbiter.arbiter.dca_execution`
  - contract: `arbiter_dca_execution_v1`
  - key functions: `planned_dca_entries(...)`, `graduated_confluence_risk_pct(...)`
- `market_arbiter.arbiter.take_profit`
  - contract: `arbiter_take_profit_policy_v1`
  - key function: `planned_take_profits(...)`

## Setup score v1

`setup_score_v1` gates actionability using train-window thresholds:

1. retest candle body/displacement >= train threshold,
2. Surveyor `selection_score` >= train threshold,
3. `merge_family_count >= 3`.

This score does **not** replace Surveyor level quality. It is an Arbiter setup/actionability score.

## Stop policy v1

Supported replay-proven policies:

- `full_zone_5bps`: stop slightly outside the full SR zone.
- `full_zone_adaptive`: stop outside full SR zone using ATR/min-bps buffer.
- `sweep_or_zone_adaptive`: if retest candle sweeps the zone and reclaims, stop beyond sweep wick; otherwise fallback to full-zone adaptive.

Doctrine:

- invalidation should be outside the SR zone, not merely outside the operator core;
- liquidity hunts are allowed, but risk geometry must still be sane;
- swing-anchored stops remain a future improvement because the current replay swing detector was too crude.

## DCA semantics v1

Plans:

- `single_100`: first entry only.
- `dca_50_50`: first entry + full-zone midpoint.
- `dca_20_30_50`: first entry + full-zone midpoint + far zone boundary.

Risk semantics:

- total trade risk is the budget;
- tranche weights allocate that budget;
- unfilled DCA tranches leave unused risk rather than reallocating into filled tranches;
- all tranche sizes use the same final invalidation stop;
- if a tranche would have non-positive risk to the stop, it is not filled in replay.

Intrabar ambiguity semantics:

- Intrabar flags mean the replay candle's OHLC range touches multiple relevant prices but does not prove the order of events inside that candle.
- `execution_order_ambiguity_rate` is the promotion-gating metric: the candle ordering can change realized fill/exit accounting.
- `diagnostic_only_intrabar_rate` is review-only: the candle is coarse enough to deserve inspection, but the known flag does not by itself change realized PnL ordering.
- first tranche executes at next candle open;
- later tranches execute as limits when candle high/low crosses the tranche price;
- if stop and target occur in the same candle, current replay uses conservative `stop_first` accounting and flags `same_candle_stop_and_target`;
- if a DCA limit fill and stop/target occur in the same candle, replay emits `same_candle_limit_fill_and_stop` / `same_candle_limit_fill_and_target` flags because daily candles cannot prove ordering;
- if TP and moved-to-entry stop occur in the same candle, replay flags `same_candle_take_profit_and_breakeven_stop`;
- multiple later TPs in one candle after the stop already moved to entry are flagged as `same_candle_multiple_take_profits_after_breakeven`, but treated as diagnostic-only unless the same candle also threatens the moved-to-entry stop;
- high execution-order ambiguity rates block promotion until lower-timeframe execution or conservative/optimistic cohort splitting is run.

## Take-profit semantics v1

Default current promotion-candidate plan: fixed `2R` final target with `tp_25_50_25` mechanics.

- 25% closes at `1R`.
- At fixed `2R`, the second and final tranches merge: 75% closes at `2R`.
- For research ablations above `2R`, 50% closes at `2R` and the final 25% closes at the configured final target.
- Opposing SR-zone final targets are future work once point-in-time profiles carry the full ranked zone surface.
- When the first TP fills, Arbiter immediately moves stop to average entry and cancels all pending DCA entries.
- Any same-candle TP/stop or TP/DCA ordering uncertainty remains an intrabar ambiguity flag on daily candles.

## Risk models

- flat `3%`: fixed research baseline.
- existing candidate-scaled: `confluence_scaled_fixed_fractional_v0` from `strategy_backtest.py`.
- `graduated_confluence_v1`: capped at `4%`, based on family confluence, Surveyor selection threshold, and retest-body threshold.

## Current research evidence

Primary artifacts:

- `artifacts/strategy_backtests/canonical_surveyor_walk_forward/SUMMARY.md`
- `artifacts/strategy_backtests/canonical_surveyor_dca_risk/SUMMARY.md`

Current primary promotion-gate candidate:

- stop: `sweep_or_zone_adaptive`
- setup: body p60 + selection p60 + family3
- target: fixed `2R`
- take profit: `tp_25_50_25` (`25%` at `1R`, `75%` at `2R`)
- DCA: `20/30/50`
- risk: graduated confluence or flat/candidate 3% comparison

Caveat: this is still research evidence. Promotion requires fixture coverage, symbol/year diagnostics, low intrabar ambiguity, and especially 2025 failure analysis.

## Promotion gate

Current gate artifact:

- `artifacts/strategy_backtests/canonical_surveyor_promotion_gate/SUMMARY.md`
- `artifacts/strategy_backtests/canonical_surveyor_promotion_gate/promotion_gate_report.json`

The promotion gate checks:

- expected 10-pair universe is present,
- enough closed trades,
- positive aggregate avgR,
- execution-order ambiguity under limit,
- enough symbols have trades,
- no symbol has negative avgR,
- no year has negative avgR.

Every future DCA/TP report should include a non-compounded `$1,000` closed-trade portfolio curve:

- final PnL,
- max/peak PnL,
- max drawdown dollars,
- max drawdown percent of the `$1,000` baseline.

Failing this gate means the candidate remains research-only, even if aggregate expectancy is positive.
