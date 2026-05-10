# SR Levels Release V0

Date: 2026-05-09
Status: V0 metadata producer/consumer wired; validation evidence captured
Owner surface: Market Arbiter / Surveyor bundle / Pine SR map overlay
Related contracts: `SURVEYOR_UNIFIED_DATASET_CONTRACT_V1.md`, `ARBITER_ARCHITECTURE_V1.md`, `STRATEGY_BACKTESTING_MECHANISM_V0.md`, `RISK_AND_CONFLUENCE_MODEL_V0.md`

## 1) Purpose

Define the first release boundary for support/resistance level quality so Market Arbiter can distinguish:

1. **invalidated levels** — levels that should not be used as active support/resistance for decisions, watches, or confidence coloring; and
2. **degraded levels** — levels that may still matter, but should be marked less trustworthy and shown with lower confidence.

This release intentionally anchors the logic before indicator work. The Pine overlay should consume this contract rather than inventing visual-only confidence rules.

## 2) Scope

This release covers selected S/R zones from the Surveyor `sr_zones` family, plus lifecycle metadata that can later be consumed by:

- Surveyor bundle metadata,
- Arbiter zone-of-interest selection,
- Sentinel watch intents,
- Pine SR map overlay confidence coloring,
- backtest/replay integrity checks.

This release does **not** define exchange execution, position sizing, live order placement, or a profitable trading claim.

## Foxian alignment constraints

The Foxian concordance audit for this release lives at `artifacts/sr_levels_release_v0/foxian_concordance.md`. V0 stays aligned with that audit by treating these as contract constraints, not optional commentary:

1. **Zones over lines:** S/R must carry explicit bounds and provenance; unbounded line-only levels are display-only until converted with recorded evidence.
2. **Formation evidence before eligibility:** a decision-eligible zone needs repeated reactions and historical context from the upstream selector, not just a price coordinate.
3. **Higher-timeframe confirmation:** decisive breaks should be confirmed on the zone timeframe or a canonical confirmation timeframe; a single noisy lower-timeframe wick cannot invalidate a higher-timeframe zone.
4. **Confirmed break as V0 approximation:** V0's close-plus-buffer rule is a deterministic approximation of Foxian decisive-break/consolidation behavior. Implementations must preserve confirmation timeframe/candle evidence so later versions can distinguish `single_close` from stronger `consolidated_close` or `htf_confirmed` breaks.
5. **Flip zones preserve history:** invalidating the current role should preserve the level as a possible flip candidate rather than deleting it.
6. **Retest decay is post-formation:** formation touches establish the zone; post-establishment retests affect trade-opportunity quality and candidate eligibility.
7. **Wick/fakeout context degrades, not auto-deletes:** reclaimed wicks and failed breakouts usually reduce eligibility or force watch-only/display-only, unless a confirmed break invalidates the role.
8. **S/R is primary confluence:** optional Fib, dynamic level, or lower-timeframe confluence should modify confidence; they should not outrank source S/R validity.
9. **Clean overlay bias:** the overlay should suppress clutter and render upstream lifecycle/quality truth rather than becoming a second S/R scoring engine.

## 3) Core definitions

### Level vs zone

Market Arbiter should treat S/R as a **zone**, not a single price line.

Minimum zone fields:

```json
{
  "zone_id": "deterministic-id-or-fingerprint",
  "symbol": "BTCUSDT",
  "timeframe": "1W|1D|4H|1H|15m|5m",
  "role": "support|resistance|flip_support|flip_resistance|unknown",
  "zone_low": 0.0,
  "zone_high": 0.0,
  "mid": 0.0,
  "source": "surveyor_sr_selector_v1",
  "source_rank": 1,
  "formation_reaction_count": 3,
  "formation_first_seen_at": "...",
  "formation_last_seen_at": "...",
  "formation_span_bars": 0,
  "historical_context_score": 0.0,
  "created_at": "...",
  "last_evaluated_at": "..."
}
```

If a source only exposes a line price, Market Arbiter must either derive an explicit conservative width with provenance or mark the zone `display_only`. Silent line-to-zone conversion is not allowed for decision use.

Decision use also requires formation evidence. If upstream cannot provide reaction/history metadata, the zone should be blocked or degraded instead of promoted by geometry alone.

Formation evidence reason codes:

- `blocked_insufficient_reaction_history`
- `degraded_weak_historical_context`
- `degraded_missing_formation_evidence`

### Lifecycle status

Use these statuses for the current role of the zone:

- `active`: valid and eligible for confidence ranking.
- `degraded`: valid but lower trust.
- `watch_only`: usable for visual/context awareness but not for candidate generation without more confirmation.
- `invalidated`: the current support/resistance role is broken.
- `flipped_pending`: the old role is invalidated, but the level may become a new flip-zone candidate after confirmation/retest.
- `superseded`: replaced by a stronger overlapping or fresher zone from the same/higher timeframe.
- `expired`: aged out by policy without a fresh confirming interaction.
- `blocked`: input/provenance quality makes the zone unusable for live decisions.

Important distinction: **invalidating the current role does not mean deleting the historical level**. A broken resistance may become `flipped_pending` support after a confirmed breakout and later retest; a broken support may become `flipped_pending` resistance.

## 4) Hard invalidation rules

A hard invalidation means the zone should no longer be used as active support/resistance in its current role. It should be hidden from the main confidence overlay unless explicitly shown as historical/invalidated context.

### I1 — Confirmed close beyond the far edge plus buffer

For support / flip-support:

```text
close < zone_low - invalidation_buffer
```

For resistance / flip-resistance:

```text
close > zone_high + invalidation_buffer
```

Default close timeframe:

- use the zone timeframe when available;
- otherwise use the nearest lower canonical confirmation timeframe;
- never invalidate a high-timeframe zone from a single noisy low-timeframe wick alone.

Initial buffer policy:

```text
invalidation_buffer = max(0.10 * zone_width, 0.25 * ATR(zone_tf, 14), min_bps_by_symbol)
```

The exact buffer can be tuned later, but it must be explicit and recorded.

Foxian confirmation note: this rule must record `confirmation_timeframe`, `confirmation_candle_count`, and `break_quality` (`single_close|consolidated_close|htf_confirmed`). A single confirmed close can invalidate a V0 current role only when it is evaluated on the zone timeframe or approved confirmation timeframe and clears the explicit buffer. Consolidated or higher-timeframe confirmation should be ranked stronger than `single_close` evidence.

Reason codes:

- `invalidated_close_below_support_buffer`
- `invalidated_close_above_resistance_buffer`
- `invalidated_consolidated_close_below_support`
- `invalidated_consolidated_close_above_resistance`

### I2 — No-reaction blow-through after armed watch

If Sentinel/overlay enters a watched zone and price traverses through the far edge without the required lower-timeframe reaction/confirmation, the active watch is invalidated even if the higher-timeframe close is still pending.

This should invalidate the **watch/candidate**, not necessarily the entire high-timeframe zone until the close rule confirms.

Reason codes:

- `watch_invalidated_no_reaction_blow_through`
- `watch_invalidated_confirmation_missing`

### I3 — Structure anchor invalidated

If the source swing/protected level that created the zone is invalidated by the canonical structure engine, the zone loses active eligibility.

Examples:

- support anchored to a protected low after that low is decisively broken;
- resistance anchored to a protected high after that high is decisively broken;
- source swing is revised/reclassified in a way that changes the zone fingerprint.

Reason codes:

- `invalidated_source_swing_broken`
- `invalidated_structure_reclassification`
- `invalidated_unstable_zone_fingerprint`

### I4 — Provenance/freshness hard fail

The zone is blocked for live decision use if required upstream truth is unsafe.

Hard-fail examples:

- missing `zone_low` / `zone_high` for a decision path;
- missing or unstable `zone_id` / fingerprint;
- required Surveyor families missing for the active profile;
- feed state `blocked` or equivalent circuit-breaker posture;
- known future-leaking replay construction;
- mixed/replay-only provenance used where live decision mode requires live truth.

Reason codes:

- `blocked_missing_zone_bounds`
- `blocked_missing_zone_id`
- `blocked_required_family_missing`
- `blocked_feed_quality`
- `blocked_future_leakage_risk`
- `blocked_replay_only_for_live_mode`

### I5 — Explicit expiry policy reached

A zone can be expired by lifecycle policy even if price never cleanly invalidated it.

Initial defaults for testing:

- `max_age_without_touch`: 90 days for 4H zones; 180 days for 1D zones; no automatic weekly expiry until tested.
- `max_failed_retests`: 2 failed retests.
- `max_total_retests_for_candidate_use`: 3; later retests can remain visual/context only.

Expiry must be source/timeframe-aware. Higher-timeframe historically respected zones should prefer `degraded_stale_last_touch`, `display_only`, or `superseded` over deletion unless the source contract explicitly supports hard expiry.

Reason codes:

- `expired_max_age_without_touch`
- `expired_failed_retest_limit`
- `expired_retest_count_limit`

## 5) Degradation rules: less trustworthy but not invalid

A degradation lowers confidence but does not break the zone's current role by itself.

### D1 — Wick breach with reclaim

A wick through the far edge followed by a close back inside/above support or inside/below resistance is not a hard invalidation. It is a sweep/liquidity-tap condition.

Trust impact depends on reaction quality:

- reclaim + strong reaction can preserve or even improve context for a watch;
- repeated wick breaches without clean reaction degrade the zone.
- severe fakeout or failed-breakout context should force `watch_only` or `display_only` even if the historical level remains useful.

Reason codes:

- `degraded_wick_breach_reclaimed`
- `degraded_repeated_wick_breaches`
- `degraded_severe_fakeout_wick`
- `watch_only_failed_breakout_context`

### D2 — Retest count increases

First retest should be treated as highest quality. Later retests remain possible but should decay.

`retest_count` means post-establishment retests only. Formation touches used to prove the zone belong in `formation_reaction_count` and must not be penalized as exhausted trade opportunities.

Initial penalty:

- first retest: no penalty / possible bonus;
- second retest: mild penalty;
- third retest: medium penalty;
- fourth+ retest: `watch_only` unless explicitly overridden by strategy pack.

Reason codes:

- `degraded_second_retest`
- `degraded_third_retest`
- `degraded_excessive_retests`

### D3 — Age/staleness without invalidation

Old zones can still matter, especially higher-timeframe zones, but stale lower-timeframe zones should not crowd the overlay or drive decisions without fresh interaction.

Reason codes:

- `degraded_zone_age`
- `degraded_stale_last_touch`
- `degraded_stale_reselection`

### D4 — Lower-quality feed or partial family state

Feed or family state that is elevated/degraded but not blocked should lower confidence and decision eligibility.

Examples:

- benign repair: no or low penalty;
- elevated repair quality band: medium penalty;
- partial optional family: low/medium caution;
- stale optional confluence family: caution, not hard rejection.

Reason codes:

- `degraded_feed_quality_elevated`
- `degraded_family_partial`
- `degraded_optional_confluence_stale`

### D5 — Zone clutter / overlap / ambiguity

If multiple zones overlap tightly, the map should not show all of them. Merge or choose the highest-ranked zone and degrade ambiguity.

Reason codes:

- `degraded_overlapping_zone_cluster`
- `degraded_nearby_opposing_zone`
- `degraded_selector_ambiguity`

### D6 — Poor zone geometry

A zone can be technically valid but poor quality if it is too wide, too narrow, or too close to current noise.

Reason codes:

- `degraded_zone_too_wide`
- `degraded_zone_too_narrow_noise_risk`
- `degraded_stop_distance_unusable`

### D7 — Missing secondary confluence

Absence of Fib/dynamic-level/HTF alignment should not invalidate S/R. It should reduce confidence and candidate score.

Reason codes:

- `degraded_missing_fib_confluence`
- `degraded_missing_dynamic_level_confluence`
- `degraded_htf_structure_misaligned`

### D8 — Source priority / fallback source

A level from a fallback source can remain displayable, but should not be ranked equally with canonical Surveyor-selected levels.

Reason codes:

- `degraded_fallback_source`
- `degraded_synthetic_replay_source`
- `degraded_missing_lifecycle_events`

## 6) Confidence tiers

Initial confidence should be deterministic and explainable, not optimized.

Recommended tiers:

| Tier | Meaning | Overlay behavior | Arbiter behavior |
|---|---|---|---|
| `A` | high confidence active zone | strong color; eligible if not too cluttered | eligible for watch/candidate if strategy pack agrees |
| `B` | medium confidence active zone | normal color | eligible for watch with caution |
| `C` | low confidence / degraded zone | muted color | watch-only unless confluence is strong |
| `D` | display-only context | faint/outline only | not candidate eligible |
| `X` | invalidated/blocked/expired current role | hidden by default; optional historical layer | reject/disarm |

Initial scoring sketch:

```text
confidence_score = weighted_source_sr_validity
                 + weighted_historical_context
                 + weighted_htf_alignment
                 + first_retest_bonus
                 + secondary_confluence_bonus
                 - retest_decay_penalty
                 - age_penalty
                 - feed_quality_penalty
                 - ambiguity_penalty
                 - geometry_penalty
```

Source/SR validity and historical context should dominate the score. Secondary confluences such as Fib or dynamic levels are modifiers, not peer gates, unless a strategy pack explicitly promotes one for a bounded experiment.

Tier mapping:

- `A`: `>= 0.80`
- `B`: `>= 0.60 and < 0.80`
- `C`: `>= 0.40 and < 0.60`
- `D`: `< 0.40` but not hard-invalid
- `X`: any hard invalidation/block reason

Every score must carry `reason_codes`, not just a number.

## 7) Metadata contract additions

Add a lifecycle/quality block per zone:

```json
{
  "zone_id": "...",
  "lifecycle": {
    "status": "active|degraded|watch_only|invalidated|flipped_pending|superseded|expired|blocked",
    "current_role_valid": true,
    "invalidated_at": null,
    "invalidated_by": null,
    "flip_candidate": false,
    "superseded_by_zone_id": null,
    "last_touch_at": null,
    "formation_reaction_count": 0,
    "formation_span_bars": 0,
    "formation_first_seen_at": null,
    "historical_context_score": 0.0,
    "retest_count": 0,
    "failed_retest_count": 0
  },
  "quality": {
    "confidence_score": 0.0,
    "confidence_tier": "A|B|C|D|X",
    "decision_eligibility": "candidate_eligible|watch_eligible|watch_only|display_only|reject",
    "reason_codes": [],
    "degradation_reasons": [],
    "invalidation_reasons": [],
    "confirmation_timeframe": null,
    "confirmation_candle_count": 0,
    "break_quality": null,
    "wick_breach_size_bps": null,
    "fakeout_severity": "none|mild|severe"
  },
  "visual": {
    "show_on_overlay": true,
    "overlay_priority": 0,
    "color_class": "sr_high|sr_medium|sr_low|sr_display|sr_invalid",
    "label_density": "minimal|detail_on_hover"
  }
}
```

The `visual` block is downstream-friendly metadata. It should be derived from the lifecycle/quality state, not hand-authored by the indicator.

## 8) Overlay behavior

The overlay should avoid zone spam.

Initial display rules:

1. show at most the top `N` zones per side/timeframe bucket near current price;
2. merge overlapping same-side zones into a composite visual band when their bounds overlap materially;
3. keep invalidated/blocked zones hidden by default;
4. use color intensity for confidence tier, not a wall of labels;
5. show reason-code detail only in tooltip/debug mode or Surveyor metadata panel.

Suggested color classes:

- `sr_high`: strong active zone (`A`)
- `sr_medium`: normal active zone (`B`)
- `sr_low`: muted degraded zone (`C`)
- `sr_display`: faint context-only zone (`D`)
- `sr_invalid`: hidden by default / optional dashed historical (`X`)

## 9) Surveyor vs overlay sequencing

Recommended sequence:

1. **Foundation contract** — land this doc and reason-code vocabulary.
2. **Surveyor metadata pass** — add lifecycle/quality/visual metadata into `sr_zones` output or bundle adapter fixtures.
3. **Arbiter/Sentinel eligibility pass** — consume the metadata to reject, watch, or candidate-qualify zones deterministically.
4. **Pine overlay pass** — render only the derived visual fields so the indicator stays dumb and consistent with Market Arbiter truth.
5. **Backtest/replay pass** — verify invalidation/degradation decisions are point-in-time and compare cohorts by confidence tier.

Rationale: Surveyor metadata should come before polished overlay behavior because the overlay must not become a second, divergent S/R quality engine.

## 10) Acceptance criteria for V0 implementation

V0 is ready when:

- each selected zone has stable bounds, role, id/fingerprint, lifecycle status, confidence tier, and reason codes;
- hard invalidation can be explained by explicit `invalidated_by` evidence;
- degraded zones remain visible/usable only according to confidence tier and decision eligibility;
- overlay display decisions are derived from metadata, not duplicated Pine heuristics;
- replay/backtest paths preserve point-in-time lifecycle state and do not delete stale-but-valid flip zones prematurely;
- tests cover at least:
  - confirmed close invalidation,
  - wick breach with reclaim degradation,
  - retest-count decay,
  - missing bounds hard block,
  - overlapping-zone display suppression,
  - flip-pending transition after confirmed breakout.

## 11) Open questions for later tuning

- Exact ATR/bps buffer constants by symbol class and timeframe.
- Whether weekly zones should ever auto-expire or only be superseded/invalidated.
- How much a strong reclaim after sweep should improve confidence versus merely preserve it.
- Whether lower-timeframe CHoCH confirmation belongs entirely in Sentinel or partly in Arbiter strategy-pack scoring.
- How to visualize flip-pending zones without making the overlay noisy.

## 12) Arbiter/Sentinel eligibility consumption

Arbiter and Sentinel must consume Surveyor's `lifecycle`, `quality`, and `visual` blocks as their SR validity source of truth. They should not duplicate support/resistance invalidation or confidence scoring.

Initial shared gate: `market_arbiter.arbiter.setup_score:evaluate_sr_zone_eligibility` (`arbiter_sr_lifecycle_eligibility_v1`). Behavior:

- `candidate_eligible` => Arbiter may create a candidate only after its own actionability/confluence gates also pass; Sentinel may watch; overlay may display if `visual.show_on_overlay=true`.
- `watch_eligible` / `watch_only` => no trade candidate; Sentinel/watch context is allowed.
- `display_only` => no trade candidate and no active watch; visual/context only.
- `reject` or hard lifecycle statuses (`blocked`, `expired`, `invalidated`, `flipped_pending`, `superseded`) => no candidate/watch/display by default.
- Missing lifecycle metadata fails closed to watch/caution with no trade candidate.

Detailed wiring artifact: `artifacts/sr_levels_release_v0/arbiter_sentinel_eligibility.md`.

## 13) Pine overlay rendering contract

The Pine overlay remains a downstream renderer of Surveyor SR lifecycle truth. It should consume these derived fields directly:

- `visual.show_on_overlay`
- `visual.overlay_priority`
- `visual.color_class`
- `quality.confidence_tier`
- compact labels derived from `quality.reason_codes`

Renderer behavior:

- hide `blocked`, `expired`, `invalidated`, `flipped_pending`, and `superseded` zones by default;
- cap visible zones after metadata filtering, using `visual.overlay_priority` and selector/source rank before proximity;
- merge only visual bands for materially overlapping same-side zones; never create a new Pine-side confidence score, lifecycle status, or decision eligibility;
- render operator-core bounds when exported, with full macro bounds reserved for debug/parity mode;
- keep reason-code detail in debug/tooltip views rather than chart-spam labels.

Current implementation gap: the main MarketArbiter worktree does not yet contain a merged `tradingview/pine` renderer or a live bundle-to-TradingView ingestion path. The active V0 contract is documented in `artifacts/sr_levels_release_v0/pine_overlay_rendering_contract.md`; the later Pine port should replace any branch-local confidence/invalidation display logic with the Surveyor-derived metadata fields above.

## 14) V0 validation evidence

Validation captured on 2026-05-09:

- Focused lifecycle, Surveyor bundle, Arbiter/Sentinel, and replay adapter tests passed: `python3 -m pytest tests/test_sr_lifecycle.py tests/test_surveyor_snapshot.py tests/test_surveyor_bundle_profile.py tests/test_surveyor_bundle_export.py tests/test_arbiter_retest_execution_contract.py tests/test_foxian_strategy_backtest.py tests/test_ohlcv_retest_adapter.py -q` => `47 passed`.
- Lightweight fixture validation split SR lifecycle behavior across confidence tiers A/B/C/D/X and proved the shared Arbiter/Sentinel gate maps them to candidate/watch/display/reject behavior. Artifact: `artifacts/sr_levels_release_v0/validation_confidence_tier_split_20260509T1903Z.md` with JSON sidecar `validation_confidence_tier_split_20260509T1903Z.json`.
- Compile and whitespace gates passed for the V0 implementation, tests, documentation, and validation artifacts.
