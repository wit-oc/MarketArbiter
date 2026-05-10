# Strategy Quantification Map V0

## Scope

Map the Foxian mentorship support/resistance retest strategy into measurable Surveyor / Arbiter fields.

Primary implementation: `foxian_retest_flip_confluence_v0` in `market_arbiter/arbiter/strategy_backtest.py`.

| Lecture concept | Backtestable condition | Surveyor / Arbiter source | Current ambiguity |
|---|---|---|---|
| Support/resistance is a region, not an exact line | Zone has `zone_low`/`zone_high` or equivalent bounds; point-in-time zone id is preserved | `sr_zones` family | Some legacy surfaces may expose only `level`/`price`; simulator must decide buffer width if bounds are absent |
| Flip zone | Zone `current_role` changes to support/resistance after breakout, or lifecycle event references flipped zone | `sr_zones`, `interaction_lifecycle` | Need stricter provenance for old role → new role transition |
| Breakout above resistance → long setup | Event or zone resolves `side=long` after resistance becomes support | `interaction_lifecycle`, `sr_zones`, fallback `structure_state` | If event side and zone role disagree, current v0 records reason codes but does not deeply arbitrate |
| Breakout below support → short setup | Event or zone resolves `side=short` after support becomes resistance | `interaction_lifecycle`, `sr_zones`, fallback `structure_state` | Same as above |
| Consolidation / sustained acceptance beyond broken level | Lifecycle includes accepted/confirmed post-break state before retest | `interaction_lifecycle` | Needs explicit duration / candle-count thresholds in a future version |
| Retest | Lifecycle has `event_type` containing `retest` and not failed/invalid/unconfirmed | `interaction_lifecycle` | v0 accepts flexible event names so older fixtures can participate; later contract should tighten event taxonomy |
| First retest emphasis | Event ordering can identify first retest per zone | `interaction_lifecycle` | v0 takes the first retest event in profile order; full replay should track first/second/nth retest by zone id |
| Confluence | Score from structure alignment, zone quality, Fib availability, dynamic-level overlap, retest confirmation | `structure_state`, `sr_zones`, `fib_context`, `dynamic_levels`, `interaction_lifecycle` | Score is a deterministic starting point, not optimized |
| Entry | Next bar open after retest confirmation | Arbiter trade candidate | Requires downstream OHLCV simulator to execute on canonical candles |
| Stop / invalidation | Long: below `zone_low` plus buffer; short: above `zone_high` plus buffer | Arbiter trade candidate + `sr_zones` | If zone bounds missing, simulator must reject or derive a conservative width |
| Targets | Fixed `2R` final target with graduated partial exits; future opposing-zone target resolver | `market_arbiter.arbiter.take_profit.planned_take_profits`; future opposing-zone resolver | Opposing-zone resolution is not built yet because replay profiles currently carry only the active retest zone; v1 primary path is 25% at 1R and 75% at 2R |
| DCA entries | Optional total-risk-budgeted tranches: single 100%, 50/50, or 20/30/50 | `market_arbiter.arbiter.dca_execution.planned_dca_entries` | Current reusable v1 ladder uses market first tranche and full-zone midpoint/boundary limit tranches; first TP cancels pending DCA |
| Fees/slippage/funding | Always included in candidate cost model | `FoxianRetestStrategyConfig` | Funding defaults to zero until dataset supplies funding history |
| Risk sizing | Fixed-fractional equity risk; compare flat 3%, candidate confluence-scaled, and graduated confluence risk | `market_arbiter.arbiter.dca_execution.graduated_confluence_risk_pct`; see `docs/RISK_AND_CONFLUENCE_MODEL_V0.md` and `docs/ARBITER_RETEST_EXECUTION_CONTRACT_V1.md` | Portfolio sequencing/correlation caps are not implemented yet |
| Waiting / expiry | Retest window, max retests per zone, failed-retest expiry | Planned replay/lifecycle fields; see `docs/RISK_AND_CONFLUENCE_MODEL_V0.md` | Not implemented yet; v0 evaluates already-emitted retest events |
| Secondary confluences | Weighted score beyond primary SR/retest | `structure_state`, `fib_context`, `dynamic_levels`, future approach/regime fields | Initial simple score exists; weighted/tiered scoring is next |
| No hindsight | Profiles are evaluated independently at `as_of_ts` | `backtest_core` profile sequence | Replay builder must prove per-bar point-in-time reconstruction |

## V0 decision outputs

Each point-in-time profile becomes one of:

- `candidate`: all required families exist, usable state, retest event, enough confluence, side resolved.
- `watch`: descriptive setup exists but trigger/confluence/side is incomplete.
- `reject`: required family missing or unusable, or setup state is structurally invalid.

## Minimum reports required before strategy deepening

1. Event-study report: forward returns after every emitted retest event. Initial implementation: `market_arbiter.arbiter.ohlcv_backtest.run_event_study`.
2. Trade-simulation report: win rate, expectancy, max drawdown, and fee/slippage/funding drag. Initial implementation: `market_arbiter.arbiter.ohlcv_backtest.run_ohlcv_backtest`.
3. Baseline comparison report: no-trade/cash, breakout-without-retest, retest-without-confluence. Not implemented yet.
4. Ambiguity report: reason-code counts for watch/reject cases.
5. Split-audit report: train/test fold chronology, threshold-training event provenance, test event ids, and overlap checks. Initial implementation: `market_arbiter.arbiter.backtest_splits`, wired into canonical walk-forward reports.
6. Integrity report: proof that zone construction and retest qualification are point-in-time.
