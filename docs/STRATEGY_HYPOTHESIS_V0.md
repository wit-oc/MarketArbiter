# Strategy Hypothesis V0 — Foxian Retest / Flip-Zone Lane

## Hypothesis

A higher-timeframe support/resistance flip-zone retest, when confirmed and aligned with basic confluence, produces a replay-testable edge after realistic fees/slippage.

This is not yet a claim that the edge exists. It is the first falsifiable hypothesis extracted from the mentorship lecture content and converted into a Surveyor → Arbiter → Backtest contract.

## Lecture-derived strategy shape

1. Identify support/resistance as **regions**, not exact lines.
2. Wait for breakout through the region.
3. Preserve the flipped region as a valid historical area of interest.
4. Wait for retest / reaction at that region.
5. Use confluences to qualify the retest, not to override invalid S/R structure.
6. Emit a trade candidate only after the retest is confirmed.

Key transcript anchors:

- Retest is identified as a major component of the support/resistance strategy: `foxian-ingest/deliverables/final-2026-02-15/content/048-15th-mastering-retests-a-deep-dive-into-support-resistance-trading-strategy-foxi.md:78-94`.
- Support/resistance flips define the meaning of retest: `.../048-15th-...md:442-462`.
- Breakout side maps to trade side: `foxian-ingest/deliverables/final-2026-02-15/content/047-16th-the-art-of-retests-advance-strategies-in-support-resistance-trading-foxian-.md:105-145`.
- Chronology is breakout → consolidation → retest → entry; S/R are areas/regions and confluence comes after the zone logic: `.../047-16th-...md:285-337`.
- Old levels/zones can remain valid after new levels form, so replay cannot drop them merely because the chart evolved: `.../048-15th-...md:1118-1138`.

## First instrument universe

- BTCUSDT first.
- ETHUSDT second.
- Then broaden only after the point-in-time replay path is validated.

## Default timeframe intent

- 1D / 4H: structure and selected zone context.
- 5m: eventual execution simulation layer.
- 15m: optional bridge/quality layer if evidence supports it later.

## Falsification criteria

This hypothesis should be marked weak or dead if:

- event-study forward returns after confirmed retests are no better than baseline,
- trade simulation expectancy is negative after fees/slippage/funding,
- max drawdown or tail loss is unacceptable,
- results vanish outside BTC or outside the tuned sample,
- the signal relies on future-aware zone construction or final-chart hindsight.

## Current implementation

- Ruleset id: `foxian_retest_flip_confluence_v0`
- Code: `market_arbiter/arbiter/strategy_backtest.py`
- Mechanism doc: `docs/STRATEGY_BACKTESTING_MECHANISM_V0.md`
- Tests: `tests/test_foxian_strategy_backtest.py`
