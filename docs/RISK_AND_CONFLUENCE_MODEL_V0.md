# Risk and Confluence Model V0

## Purpose

Define the backtest layer beyond “did the SR-zone retest happen?” so results are evaluated with realistic position sizing, waiting/expiry rules, and secondary confluence weighting.

This is a research/backtest model, not live-trading advice.

## Candle timeframe decision

For the first meaningful historical BTC and top-100 research pass, default to **4H candles**, not 5m.

Rationale:

- The strategy thesis is higher-timeframe support/resistance retest + confluence, not scalping.
- 4H gives enough historical depth without an enormous data burden, but only if we hydrate **multi-year** history.
- Daily candles are useful for macro context and dynamic levels, but they are probably too coarse for retest execution.
- 5m remains useful later as an execution-refinement layer once the higher-timeframe edge is proven.

Recommended data stack:

| Layer | Candle | Use |
|---|---:|---|
| Macro/trend context | 1D / 1W | HTF trend, major SR, major EMA levels |
| Primary strategy/replay | 4H | zone lifecycle, breakout/retest, backtest execution |
| Optional refinement later | 15m / 5m | tighter entries after 4H edge is validated |

So the simulator default is now `4h`; 5m is still supported by passing `--timeframe 5m`.

## Current status

Implemented:

- primary SR-zone retest signal mechanism,
- confluence-scaled candidate risk model,
- reusable graduated confluence risk helper: `market_arbiter.arbiter.dca_execution.graduated_confluence_risk_pct`,
- reusable DCA ladder helper: `market_arbiter.arbiter.dca_execution.planned_dca_entries`,
- reusable take-profit helper: `market_arbiter.arbiter.take_profit.planned_take_profits`,
- reusable retest setup gate: `market_arbiter.arbiter.setup_score.score_retest_setup`,
- reusable retest stop policy helper: `market_arbiter.arbiter.stop_policy.resolve_retest_stop`,
- fixed / full-zone / sweep-aware stop research policies,
- fixed RR target,
- fees/slippage/funding in OHLCV simulation,
- basic account-level risk fields in simulated trades.

Still incomplete:

- full portfolio/equity sequencing across overlapping trades,
- wait/expiry logic before a retest triggers,
- portfolio/correlation caps,
- secondary-confluence parameter sweeps.

## Risk model

Use fixed-fractional risk:

```text
risk_dollars = equity * risk_pct
position_units = risk_dollars / abs(entry_price - stop_price)
notional = position_units * entry_price
```

Redact-approved research risk scale:

| Setup quality | Risk per trade | Meaning |
|---|---:|---|
| Bare valid SR retest | 1% | primary condition only |
| SR retest + one strong secondary | 2% | modest support |
| SR retest + multiple supports | 3% | good setup |
| strong setup | 4% | strong secondary alignment |
| perfect setup | 5% | first SR retest, strong Fib in zone, multiple dynamic levels like 1D EMA200 in zone, and HTF trend aligned |

Important: report both equal-risk and confluence-scaled-risk variants. Equal-risk tells us whether the setup has edge; confluence-scaled risk tells us whether weighting quality improves portfolio behavior.

Current reusable comparison artifact:

- `artifacts/strategy_backtests/canonical_surveyor_dca_risk/SUMMARY.md`
- `artifacts/strategy_backtests/canonical_surveyor_dca_risk/dca_risk_report.json`

First-pass DCA + TP semantics:

- `single_100`: one market/next-open entry.
- `dca_50_50`: first entry plus full-zone midpoint.
- `dca_20_30_50`: first entry plus midpoint plus far full-zone boundary.
- Total trade risk is the budget; tranche weights allocate that budget; unfilled tranches leave risk unused.
- Current primary target is fixed `2R` because the point-in-time profile does not yet carry the full opposing SR-zone surface.
- `tp_25_50_25`: at fixed `2R`, 25% exits at 1R and the remaining 75% exits at 2R.
- Above 2R research ablations split as 25% at 1R, 50% at 2R, final 25% at configured target.
- First TP moves stop to average entry and cancels pending DCA.

## Risk caps before top-100 claims

Add/validate these before trusting broad results:

- max open risk: `10%` of equity during research, lower for live use,
- max per-symbol open risk: `5%`,
- max daily realized loss: configurable; start with `5%`,
- max weekly realized loss: configurable; start with `10%`,
- pause-after-consecutive-losses: start with `4`,
- skip trade if stop distance is too narrow/wide:
  - too narrow: likely noise / fee-dominated,
  - too wide: poor R and excessive notional.

## Waiting / expiry logic

Foxian retest logic implies patience, but the backtest needs explicit state windows.

Recommended v0 parameters:

| Parameter | Default | Meaning |
|---|---:|---|
| `min_bars_after_breakout_before_retest` | 1 4H bar | Avoid immediate fake retest classification |
| `max_bars_waiting_for_retest` | 90 days in 4H bars | Keep old zones alive, but bounded for testing |
| `max_retests_per_zone` | 3 | First retest is primary; later retests are separate cohort |
| `first_retest_bonus` | +1 score | Capture lecture emphasis without hardcoding only-first-retest |
| `zone_expiry_after_failed_retests` | 2 failed retests | Prevent zombie zones |

Run cohort splits:

- first retest only,
- first + second retest,
- all retests up to expiry.

## Primary vs secondary confluences

Primary condition:

- qualified SR/flip zone + confirmed retest.

Secondary confluences should not create trades alone. They should only:

1. increase score,
2. allow/deny candidate promotion,
3. scale risk from 1% to 5%.

Recommended weighted score:

| Confluence | Weight | Notes |
|---|---:|---|
| SR/flip zone present | required | primary |
| retest confirmed | required | primary |
| HTF structure aligned | +1 | long only in HTF uptrend, short only in HTF downtrend |
| first retest of zone | +1 | major quality boost |
| strong Fib level/cluster inside SR zone | +1 | supportive only |
| multiple dynamic levels inside/near zone | +1 to +2 | e.g. 1D EMA200, VWAP, 4H EMA200; cap so this does not dominate |
| zone quality high | +1 | clean selector/rank/low clutter |
| approach quality / consolidation | +1 | once lifecycle exposes it |
| excessive retest count | -1 to -2 | degradation |
| opposing 4H structure fresh/unstalled | reject or -3 | no-trade filter |

Risk mapping implemented in candidate templates:

```text
risk_pct = min(5%, 1% + secondary_confluence_score)
```

So:

- primary SR retest only => `1%`,
- primary + trend aligned => `2%`,
- primary + trend + Fib + dynamic + first retest => capped at `5%`.

## BTC-only first test needs

For a true BTC test we need either:

1. **Fast smoke test**: a BTC candidate dataset plus existing/local 4H OHLCV.
2. **Real strategy test**: historical replay builder that turns BTC OHLCV into point-in-time Surveyor `backtest_core` profiles, then emits candidates automatically.

Existing local DB currently has enough 4H/1D/1W BTC-USDT history only for a mechanics smoke test. It is **not enough** for a meaningful BTC strategy verdict because the 4H slice is under one year. The target should be at least 3 years of BTC 4H history, preferably 5 years, plus matching 1D/1W context.

## Next implementation tasks

1. Hydrate/export a multi-year BTC 4H dataset: 3Y minimum, 5Y preferred, with matching 1D/1W context.
2. Add/export BTC 4H OHLCV run artifact from that hydrated dataset.
3. Build the real replay builder for historical BTC profiles.
4. Add waiting/expiry fields to lifecycle/replay events.
5. Add stronger secondary-confluence extraction: Fib cluster, dynamic-level count/types, HTF trend alignment, first retest.
6. Add equal-risk vs confluence-scaled report comparison.
7. Add portfolio/equity sequencing for overlapping multi-pair top-100 runs.
