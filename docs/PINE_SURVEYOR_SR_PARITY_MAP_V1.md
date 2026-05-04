# Pine Surveyor SR Parity Map V1

Status: T1 pipeline lock/spec extraction for `Surveyor_SR_Levels_v1.pine`.
Scope: TradingView Pine parity with the authoritative Surveyor daily-major SR path, not a new/tuned pivot overlay.

## Source-of-truth snapshot

The current authoritative daily-major path is a bridge from MarketArbiter replay tooling into the canonical LiquidSniper Surveyor V3 zone engine:

1. MarketArbiter adapter entrypoint:
   - `/Users/wit/.openclaw/workspace/MarketArbiter/market_arbiter/ops/canonical_surveyor_retest_backtest.py`
   - `CanonicalSurveyorConfig`
   - `_build_daily_major_zones(symbol, candles, cfg)`
2. Canonical Surveyor engine source:
   - `/Users/wit/.openclaw/workspace/LiquidSniper/liquidsniper/core/zone_engine_v3.py`
   - `/Users/wit/.openclaw/workspace/LiquidSniper/liquidsniper/core/zone_selectors.py`
   - `/Users/wit/.openclaw/workspace/LiquidSniper/liquidsniper/core/sr_engine_v2.py` for reaction-family pivots/touches
   - `/Users/wit/.openclaw/workspace/LiquidSniper/liquidsniper/core/zone_primitives.py` for ATR and role/interaction helpers
   - `/Users/wit/.openclaw/workspace/LiquidSniper/IntradayTrading/engine/phase1_contract.py` via `zone_engine_v3` for structure events

Important repository caveat: the MarketArbiter adapter source above is currently observed in the dirty main worktree, while the clean initiative worktree was created from `origin/main`. Treat this document as the locked extraction and make T2 explicitly decide whether to port/copy the adapter path into this branch or build the fixture exporter directly against the LiquidSniper source.

## Authoritative computation sequence

The canonical Python sequence for one symbol/timeframe/as-of slice is:

```python
surveyor_candles = [_as_surveyor_candle(candle) for candle in candles]
kwargs = {
    "cluster_eps": cfg.daily_cluster_eps,
    "reaction_atr_min": cfg.daily_reaction_atr_min,
    "min_meaningful_touches": cfg.daily_min_meaningful_touches,
}
structure = build_structure_candidates(symbol, "1D", surveyor_candles, **kwargs)
base = build_base_candidates(symbol, "1D", surveyor_candles, **kwargs)
reaction = build_reaction_candidates(symbol, "1D", surveyor_candles, **kwargs)
merged = merge_candidate_zones(structure, base, reaction)
last_price = float(candles[-1]["close"])
scored = [score_zone(zone, last_price=last_price) for zone in merged]
kept = select_daily_majors(
    scored,
    min_strength=cfg.daily_min_strength,
    min_zone_separation_bps=cfg.daily_min_zone_separation_bps,
    max_zones=cfg.daily_max_zones,
    strict_retest_quality=cfg.daily_require_first_retest_quality,
    reference_price=last_price,
)
```

Pine must mirror this exact ordering:

`structure candidates + base candidates + reaction candidates -> merge_candidate_zones -> score_zone -> select_daily_majors -> operator core bounds -> renderer`.

## Locked defaults and constants

From `CanonicalSurveyorConfig`:

| Name | Value | Pine note |
|---|---:|---|
| `min_history_bars` | `365` | The Pine script should not emit authoritative daily majors before 365 daily bars are available. |
| `discovery_cadence_bars` | `7` | Used by replay profile generation; Pine live display may recompute every bar, but fixtures should compare at as-of dates matching this cadence where relevant. |
| `daily_cluster_eps` | `1.10` | Passed to all three candidate generators; reaction family uses it as ATR-normalized pivot cluster epsilon. |
| `daily_reaction_atr_min` | `0.60` | Passed to reaction/base/structure call seam; reaction lifecycle uses it as minimum meaningful reaction ATR. |
| `daily_min_meaningful_touches` | `5` | Confirmation threshold for reaction-family zones. |
| `daily_min_zone_separation_bps` | `250.0` | Selector spacing threshold; also seeds wider macro-pocket thresholds. |
| `daily_min_strength` | `70.0` | Daily-major prefilter is on `strength_score`, not `selection_score`. |
| `daily_max_zones` | `8` | Renderer should cap selected daily-major operator-core zones at 8. |
| `daily_require_first_retest_quality` | `True` | Strict mode for soft retest weighting. |
| `use_operator_core_bounds` | `True` | Fixture/retest adapter consumes core bounds when present; Pine renderer should render cores, with full-zone debug available. |

Additional constants embedded in source:

### Structure family (`zone_engine_v3.py`)

- Contract stamps: `V3A_CONTRACT = "zone_engine_v3a"`, `STRUCTURE_SEED_POLICY_VERSION = "zone_engine_v3_structure_seed_rules_v1"`.
- Seed sources: `bos_confirmed`, `choch_detected`.
- Allowed seed kinds: `bos_anchor`, `flip_anchor`.
- Lock event policy: bullish transitions require `swing_low_locked`; bearish transitions require `swing_high_locked`.
- `max_lock_distance_bars = 3`.
- Requires at least `8` candles and `local_atr(period=14) > 0`.
- Drops structure seeds where `break_distance_atr < 0.05`.
- Zone construction uses the locked anchor candle body/wick around `anchor_price`:
  - support: `zone_low = min(anchor_low, anchor_mid)`, `zone_high = max(anchor_body_high, anchor_mid)`.
  - resistance: `zone_low = min(anchor_body_low, anchor_mid)`, `zone_high = max(anchor_high, anchor_mid)`.
  - if degenerate, pad by `max(atr * 0.08, max(anchor_span, body_span, 1e-6) * 0.25)`.
- Score components:
  - `strength_score = min(100, 58 + 18*min(break_distance_atr, 2) + 10*min(anchor_span/atr, 1.5))`.
  - `reaction_score = min(100, 50 + 16*min(break_distance_atr, 2))`.
  - `reaction_efficiency_score = min(100, 52 + 22*min(break_distance_atr, 1.5) - 6*max(width_atr - 1, 0))`.
  - `carry_score = min(100, 48 + 10*min(body_span/zone_width, 1) + (8 if flip_anchor else 4))`.
  - `body_respect_score = min(100, 44 + 18*min(body_span/zone_width, 1))`.
  - `meaningful_touch_count = 1`, `first_touch_state = "virgin"`.

Pine portability notes:
- The hard dependency is Phase 1 structure events. Pine cannot import `phase1_contract`; T4 must either port enough BoS/CHoCH/swing-lock logic or mark structure-family parity incomplete. Do not fake structure candidates with generic pivots and call it parity.
- Preserve event ordering and the 3-bar lock search; Pine arrays should store event name, index, price, regime direction, transition reason, and anchor index/price.

### Base/shelf family (`zone_engine_v3.py`)

Constants:

- `window = 5`
- `breakout_lookahead = 3`
- `compression_max_atr = 1.10`
- `breakout_min_atr = 0.80`
- `breakout_close_min_atr = 0.35`
- `overlap_min_ratio = 0.45`
- `min_overlap_links = 2`
- `touch_tol = atr_ref * 0.10`
- Requires at least `14` candles and `local_atr(period=14) > 0`.
- Base qualifies only with compression, repeated overlap, both edge participation, and close/range breakout.
- `kind = support` when close-up breakout ATR is greater/equal to close-down breakout ATR; otherwise resistance.
- `score = min(100, 28 + 24*breakout_atr + 18*close_breakout_atr + 14*overlap_score + 12*edge_score + 10*compression_bonus + 8*battle_score)`.
- Output score fields are derived from the same breakout/compression metrics and carry `candidate_family = "base"`, `source_family = "base_shelf_v3b"`, `source_version = "zone_engine_v3b_base_v2"`.

Pine portability notes:
- This loop is tractable in Pine because the lookback windows are fixed and small.
- Avoid dynamic object maps; store per-candidate primitive arrays and debug counts.

### Reaction family (`sr_engine_v2.py` through `zone_engine_v3.zone_candidates_from_reaction`)

Key defaults as called by daily-major path:

- `pivot_k = 3` default.
- `cluster_eps = 1.10` from `daily_cluster_eps`.
- `reaction_atr_min = 0.60` from `daily_reaction_atr_min`.
- `min_meaningful_touches = 5` from `daily_min_meaningful_touches`.
- ATR is `local_atr(candles, period=14)`.
- Pivot extraction requires `2*k + 1` candles and uses high >= window max for resistance, low <= window min for support.
- Cluster width floor is `max(mid * 0.0005, atr * 0.25)` after 20/80 price quantiles.
- Meaningful touch looks ahead 3 candles for reaction magnitude; carry/adverse look ahead 7 candles.
- Confirmation status is `confirmed` when meaningful touches >= threshold.
- First retest begins after the confirmation index; result is `reject`, `accept`, or `deviation` by close vs zone bounds, with a two-candle deviation follow-through check.
- `_zone_scores` weights:
  - touch component `18 * log_norm(meaningful_touch_count, 40)`
  - pivot component `15 * log_norm(pivot_count, 18)`
  - reaction component `30 * clamp(max_reaction_atr / 2.5)`
  - carry component `9 * carry_norm`
  - body component `12 * body_norm`
  - efficiency component `16 * reaction_efficiency`
  - retest component `12` for reject, `7` for deviation
  - spent-zone, width, and chop penalties as in `_zone_scores`

Pine portability notes:
- This is the largest candidate-count loop. Prefer bounded lookback (`min_history_bars` plus max chart bars) and cap arrays defensively.
- Pine percentile/quantile behavior must be implemented explicitly for clustered pivot prices; do not substitute average-only clusters unless documented as a parity break.
- Reaction lookahead is historical-only on completed bars; live current-bar behavior must not repaint. Use confirmed historical candles for parity checks.

## Merge/arbitration map

Source: `zone_engine_v3.merge_candidate_zones`.

1. Flatten all family candidates and default missing `candidate_sources` / `source_family`.
2. Sort descending by `(selection_score or strength_score, reaction_efficiency_score, carry_score)`.
3. Create clusters by symbol, timeframe, compatible zone kind, and either:
   - overlapping intervals, or
   - midpoint distance <= `merge_tol` where `merge_tol = max(width, seed_width, atr*0.35, seed_atr*0.35, abs(seed_mid)*0.0035)`.
4. For each cluster:
   - keep the top-ranked candidate as representative;
   - full macro bounds become min(low) / max(high), mid becomes average of candidate mids;
   - `candidate_families` are sorted normalized family names;
   - `merge_family_count = len(families)`;
   - `selection_score = max(candidate selection/strength) + 4.0 * max(0, family_count - 1)`;
   - `strength_score = max(candidate strength_score)`;
   - `arbitration_diagnostics.candidates` records each candidate low/high/mid/base score and whether it was kept.

Pine portability notes:
- Implement clustering as stable arrays sorted by rank. Pine lacks nested dicts, so debug rows should be flattened into parallel arrays or compact strings.
- `arbitration_diagnostics.candidates` is needed later for operator-core overlap-density derivation; keep at least candidate low/high/base score for each merged zone.

## Score-zone and role map

Source: `zone_engine_v3.score_zone`.

Base scoring before daily-major selector:

- `base_score = 0.54*strength + 0.16*reaction + 0.16*efficiency + 0.10*carry`.
- If ATR is present, `zone_width_atr = width / atr` and `width_bonus = clamp((1.2 - width_atr) * 4.0, -10.0, 8.0)`.
- `family_bonus = 3.0 * max(0, len(candidate_sources) - 1)`.
- Price-aware lifecycle bonus/penalty comes from `classify_zone_state` / `side_aware_interaction`:
  - `+4` for virgin in buy/sell view.
  - `+2` for first_touch.
  - `-4` for deep_test.
  - `-10` for broken.
- Final preliminary `selection_score = base_score + width_bonus + lifecycle_bonus + family_bonus`.

Operator-core derivation exists in two places:

1. `zone_engine_v3._derive_operator_core_bounds` immediately after `score_zone`:
   - non-1D: full bounds are core, display `macro`.
   - 1D factor: `0.38` if family count >= 3, `0.50` if 2, else `0.62`.
   - If ATR present, target span is `max(min(full_span, atr*2.5), full_span*factor)`, then clipped to full span.
2. `zone_selectors._apply_daily_operator_core` after daily selection:
   - If arbitration has at least two candidate intervals, use overlap of strong intervals (`score >= top_score*0.82`), else top two; definition `overlap_density_core`.
   - If no useful overlap, use representative family bounds; if core is >= 90% of macro width, fall back to midpoint narrowed core.
   - Fallback midpoint narrowed core uses `half_width = macro_width * 0.275` and definition `midpoint_narrowed_core`.

Renderer rule for this initiative: render the final `core_low/core_high/core_mid` returned by `select_daily_majors`; expose full macro bounds as debug only.

## Daily-major selector map

Source: `zone_selectors.select_daily_majors`.

Exact sequence:

1. `confirmed = [z for z in zones if z.status == "confirmed"]`.
2. `apply_daily_soft_retest_weights(confirmed, strict_mode=True)`:
   - `daily_retest_weight` base:
     - reject `1.00`
     - deviation `0.92`
     - accept `0.80` in strict mode
     - empty/none `0.82` in strict mode
     - other `0.84`
   - dynamic: `+0.05*carry_norm +0.04*body_norm -0.06*counter_close_rate -0.03*close_inside_rate`, clamped `[0.6, 1.0]`.
   - daily provenance weight:
     - `+0.06` if structure present;
     - `+min(0.04, 0.02*(merge_family_count-1))` if 2+ families;
     - `-0.06` if pure base-only;
     - clamp `[0.88, 1.12]`.
   - daily `selection_score = strength*retest_weight*provenance_weight + 0.08*reaction + 0.16*efficiency + 0.06*carry + 0.10*body_respect`.
3. Prefilter by `strength_score >= 70.0`.
4. `select_daily_local_band_representatives` with `band_span_bps = max(min_zone_separation_bps*2.6, 1100.0)`, max zones `16` for default `8`.
5. `collapse_zones_by_distance` using `zone_rank_key`, rejecting mids closer than `250 bps`, cap `8`.
6. `select_spatially_diverse_zones`, cap `8`.
7. Apply daily operator core.
8. `_consolidate_daily_selected_pockets`:
   - membership thresholds use display/core intervals: overlap >= `0.18`, edge gap <= `max(250*2.5, 950)`, or mid gap <= `max(250*3.4, 1350)` with tighter edge gap.
   - representatives ranked by `_daily_pocket_rank_key` and capped at `8`.
9. Apply current-regime coverage with `reference_price = last close`:
   - fills large gaps around/above/through current price when candidate score/coverage conditions qualify.
   - upside containing-to-above gap threshold `2200 bps`, candidate must have score >= `72`.
   - below/above gap threshold `2500 bps`, candidate must have score >= `max(78, weakest_selected*0.9)`.
10. Rank by `_daily_pocket_rank_key` and stamp:
    - `selector_surface = "daily_major"`
    - `selector_status = "kept"`
    - `selector_reason`
    - `selector_rank`

## Required zone fields for Pine parity/debug

Minimum per candidate/zone arrays:

- Identity: `zone_id`, `symbol`, `tf`, `status`, `candidate_family`, `source_family`, `source_version`, `engine_contract`.
- Bounds: `zone_low`, `zone_high`, `zone_mid`, `zone_kind`, `origin_kind`, `current_role`, `relative_position`.
- Scores: `strength_score`, `reaction_score`, `reaction_efficiency_score`, `carry_score`, `body_respect_score`, `selection_score`, `family_confluence_bonus`, `daily_major_provenance_weight`, `retest_weight`.
- Counts/rates: `pivot_count`, `touch_count`, `meaningful_touch_count`, `merge_family_count`, `merge_candidate_count`, `zone_width_bps`, `zone_width_atr`, `body_overlap_rate`, `wick_only_rate`, `close_inside_rate`, `directional_close_rate`, `counter_close_rate`.
- Retest/lifecycle: `first_touch_state`, `first_retest_pending`, `first_retest_ts`, `first_retest_result`, `deviation_retest`.
- Provenance flags: has structure/base/reaction, pure base-only, source versions/contracts where feasible.
- Merge diagnostics: merged candidate lows/highs/mids/base scores for operator-core derivation.
- Final selector/core: `selector_surface`, `selector_status`, `selector_reason`, `selector_rank`, `core_low`, `core_high`, `core_mid`, `core_definition`, `daily_pocket_id`, `daily_pocket_member_count`.

## TradingView/Pine parity cautions

- Data source mismatch is expected: Python golden fixtures use stored candles loaded by MarketArbiter, currently with Binance public-data futures provenance in replay tooling. TradingView exchange symbols may not have identical OHLCV, sessions, or corrections.
- Pine cannot import Python or dynamic dictionaries. Implement family candidates as typed parallel arrays with explicit max capacities and debug counters.
- Repainting risk: pivot and retest logic must only use completed/confirmed historical bars for parity. Current forming daily bar should be excluded or clearly marked non-parity.
- Tolerance must compare selected core/macro mids and bounds, not just visual line roughness.
- Any approximation must be labeled as a divergence. The objective is parity with the Surveyor path, not a nicer chart overlay.

## Acceptance checks for later tasks

T2 fixture exporter should emit, for BTCUSDT 1D fixed dates:

- source candle metadata and last close/reference price;
- candidate counts for structure/base/reaction/merged/scored/selected;
- each selected zone's full bounds, core bounds, scores, families, selector rank/reason, and source timestamp/provenance where available;
- data-source caveat and exact source commit/path used.

T3-T7 should use the fixture fields above to compare Pine output by selected rank, core mid, core low/high, full macro low/high, candidate family flags, and debug counts.
