# Strategy Backtesting Mechanism V0

## Purpose

Build the mentorship-derived strategy as a **backtestable Arbiter mechanism**, not as another discretionary chart narrative.

The first quantified strategy lane is `foxian_retest_flip_confluence_v0`: support/resistance flip-zone retests, gated by Surveyor's point-in-time market-state packet and emitted as replay-ready event rows plus trade-candidate templates.

## Lecture basis

The initial strategy extraction is intentionally narrow:

- Lecture 15 frames retest as a major component of the support/resistance strategy: `foxian-ingest/deliverables/final-2026-02-15/content/048-15th-mastering-retests-a-deep-dive-into-support-resistance-trading-strategy-foxi.md:78-94`.
- Lecture 15 defines the flip-zone idea: supports become resistances and resistances become support: `.../048-15th-...md:442-462` and `.../048-15th-...md:930-1002`.
- Lecture 16 gives the breakout-side mapping: breakout above resistance implies long; breakout below support implies short after support flips into resistance: `foxian-ingest/deliverables/final-2026-02-15/content/047-16th-the-art-of-retests-advance-strategies-in-support-resistance-trading-foxian-.md:105-145`.
- Lecture 16 states the chronological sequence: breakout, consolidation, retest, entry, with S/R as regions/areas and confluence added after the zone logic: `.../047-16th-...md:285-337`.
- Lecture 16 also warns that a valid retest can arrive much later, so replay must preserve old zones rather than dropping them just because newer levels appeared: `.../047-16th-...md:957-1037` and `.../048-15th-...md:1118-1138`.
- Lecture 12 reinforces using “flip zone” language rather than pretending it is a single exact line: `foxian-ingest/deliverables/final-2026-02-15/content/051-12th-support-resistance-understanding-levels-flip-zones-foxian-org.md:130-146` and `.../051-12th-...md:1130-1158`.

## Existing Surveyor logic used

This mechanism consumes the Surveyor profile already created for replay/backtesting:

- `backtest_core` profile selected by `market_arbiter.core.surveyor_bundle_profile.select_surveyor_bundle_profile(...)`.
- Required families: `feed_state`, `structure_state`, `sr_zones`, `interaction_lifecycle`.
- Optional but scored families: `fib_context`, `dynamic_levels`.

Surveyor remains descriptive. Arbiter converts a point-in-time Surveyor profile into:

1. `event_study_row`: one row per qualified retest event.
2. `trade_candidate`: entry/stop/target/cost template for later OHLCV simulation.
3. `verdict`: `candidate`, `watch`, or `reject`.

## Quantified rules v0

### Required inputs

For each timestamped profile:

- Feed state is usable.
- Structure state exists and can optionally provide directional bias.
- S/R zones exist and include a selected support/resistance or demand/supply zone.
- Interaction lifecycle contains a confirmed retest event.

### Candidate side

- Retest of support/demand or resistance flipped to support => `long`.
- Retest of resistance/supply or support flipped to resistance => `short`.
- If event side exists, event side wins; otherwise zone role wins; otherwise structure bias is used only as fallback.

### Confluence score

Current deterministic score components:

- `+1` structure bias aligns with signal side.
- `+1` qualified S/R zone present.
- `+1` zone quality is high when a numeric score/rank is present and >= `0.7`.
- `+1` Fib context is active / value-zone-related.
- `+1` dynamic level overlaps, touches, or sits near the selected zone.
- `+1` retest is explicitly confirmed.

Default threshold: `min_confluence_score = 2`.

This is deliberately not optimized. It is a first backtestable contract, meant to make ambiguity visible before performance claims.

## Trade template v0

A `candidate` emits:

- `entry_policy`: `next_bar_open_after_retest_confirmation`.
- `stop_policy`:
  - long: below `zone_low` plus buffer,
  - short: above `zone_high` plus buffer.
- `target_policy`: first opposing qualified zone, else fixed RR ladder.
- RR ladder defaults: `1R`, `2R`.
- Costs included by default:
  - taker fee: `5 bps`,
  - slippage: `2 bps`,
  - funding: configurable, default `0 bps / 8h` until the dataset provides funding.

## Code surface

Implemented in:

- `market_arbiter/arbiter/strategy_backtest.py`

Public API:

- `evaluate_foxian_retest_strategy(profile, config=None)`
- `build_foxian_retest_backtest_dataset(profiles, config=None)`
- `FoxianRetestStrategyConfig`

## Fast OHLCV first-pass path

Before the full point-in-time Surveyor replay exists, the rough research adapter can scan historical OHLCV directly for prior-window breakout + first-retest patterns and feed those synthetic replay profiles into the existing Foxian strategy evaluator:

```bash
python3 -m market_arbiter.ops.fast_ohlcv_retest_backtest \
  --db-path data/market_arbiter.sqlite \
  --symbol BTCUSDT \
  --timeframe 4h \
  --provider-id binance_public_data \
  --venue binance_usdm_futures \
  --output-dir artifacts/strategy_backtests/fast_ohlcv_retest_btc_5y
```

Implemented in:

- `market_arbiter/arbiter/ohlcv_retest_adapter.py`
- `market_arbiter/ops/fast_ohlcv_retest_backtest.py`

Boundary: this is a rough signal-dead/alive pass. It is chronological and avoids using future candles to construct the breakout zone, but it is not a substitute for the final Surveyor `backtest_core` replay/integrity report.

## Backtest path

The fuller OHLCV runner path is now started:

1. Build/replay Surveyor `backtest_core` profiles at each historical decision timestamp.
2. Call `build_foxian_retest_backtest_dataset(...)`.
3. Run `python3 -m market_arbiter.ops.strategy_backtest_run --dataset <dataset.json> --ohlcv-dir <ohlcv_dir> --timeframe 4h --output <report.json>`.
4. The runner computes event-study windows after every `event_study_row`.
5. The runner simulates every `trade_candidate` on supplied per-symbol OHLCV with fees/slippage/funding.
6. Next comparison layer should add baselines:
   - no-trade / cash,
   - simple breakout without retest,
   - retest without confluence filters.

OHLCV input contract: `docs/OHLCV_BACKTEST_INPUT_CONTRACT_V0.md`.
Runner code: `market_arbiter/arbiter/ohlcv_backtest.py` and `market_arbiter/ops/strategy_backtest_run.py`.

## Integrity guardrails

- Every profile must be point-in-time: no future zone construction, future swing significance, or final-chart hindsight.
- Surveyor emits descriptive market state only; Arbiter owns interpretation.
- Candidate trades must include costs before any performance claim.
- Preserve stale-but-valid historical flip zones until invalidation rules remove them; do not erase them because newer zones formed.
- Report ambiguity as reason codes instead of silently overfitting.
