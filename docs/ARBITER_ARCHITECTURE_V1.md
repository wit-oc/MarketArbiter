# Arbiter Architecture V1

Date: 2026-04-29
Updated: 2026-04-30
Status: proposed architecture draft; do not treat as implemented
Intent: define the Arbiter / Sentinel / strategy-pack boundary before live decision logic is built.

---

## 1) Core separation

MarketArbiter should keep four concerns separate:

- **Surveyor** describes market state with provenance.
- **Arbiter** interprets Surveyor output through exactly one active strategy pack for a given run/scope.
- **Sentinel** watches Arbiter-armed symbol/zone contexts and emits deterministic events.
- **Execution** remains downstream and out of scope for this architecture slice.

Arbiter may emit a candidate or decision record. It must not place orders directly.

---

## 2) Arbiter responsibility

Arbiter is the selective interpretation and control layer.

It should:

1. load one active strategy pack,
2. request the Surveyor profile/families required by that pack,
3. scan every in-scope pair for qualified setups,
4. emit watch intents for Sentinel when a setup is worth monitoring,
5. consume Sentinel watch events,
6. produce deterministic decision records,
7. fail closed on missing, stale, degraded, or provenance-incomplete required inputs.

For v1, Arbiter should consume `arbiter_core` as its primary live/fixture profile and may consume `backtest_core` for replay/backtest flows.

---

## 3) Sentinel responsibility

Sentinel is the armed-watch event layer.

Sentinel should not scan the whole market looking for strategies. It should only watch contexts explicitly armed by Arbiter.

Sentinel also prevents Surveyor from becoming a continuous lower-timeframe trigger engine. Surveyor remains broad and descriptive. Arbiter decides when a Surveyor-described SR / structure context is close enough and qualified enough to become a zone of interest. Only then does Sentinel arm a narrow watch at the lower trigger cadence.

A Sentinel watch is scoped by:

- `strategy_pack_id`,
- `strategy_pack_version`,
- `symbol`,
- `timeframe` or cadence assumptions,
- `zone_id` or deterministic zone fingerprint,
- zone bounds and buffers,
- invalidation rules,
- timeout / expiry rules,
- confirmation trigger rules, such as 15m CHoCH for the first SR retest pack,
- source Surveyor bundle/profile identifiers,
- replay/live mode.

Sentinel should emit event records, not trade decisions.

Expected v1 event vocabulary:

- `watch_armed`
- `approached_zone`
- `entered_zone`
- `first_retest_touch`
- `confirmation_window_open`
- `choch_observed`
- `confirmation_observed`
- `invalidation_breach`
- `no_reaction_blow_through`
- `moved_away`
- `timeout`
- `disarm`
- `watch_degraded`

Arbiter decides what those events mean.

---

## 4) Strategy packs

Arbiter should support strategy packs so new strategies can be added without turning Arbiter into a monolith.

A strategy pack is a versioned unit containing configuration, code, and fixtures for one strategy family.

A pack should define:

- `strategy_pack_id` and semantic version,
- supported modes: `live`, `replay`, `backtest`, or explicit subset,
- required Surveyor profile(s), starting with `arbiter_core` / `backtest_core`,
- required Surveyor families,
- optional Surveyor families,
- in-scope pair universe filters,
- setup qualification rules,
- zone-of-interest selection logic,
- Sentinel arming parameters,
- Sentinel event handlers,
- decision-record schema additions,
- rejection/caution reason codes,
- fixtures and acceptance tests.

Only one strategy pack should be active for a given Arbiter run/scope. If two strategies need simultaneous evaluation later, run them as separate Arbiter scopes and keep their outputs explicitly separated.

---

## 5) First strategy pack: SR first retest

The first pack should be SR / first-retest focused and aligned with the current `foxian_retest_flip_confluence_v0` research lane.

Working name:

- `sr_first_retest_v1`

Purpose:

- scan all in-scope pairs,
- identify Surveyor-described SR / flip-zone contexts,
- prioritize first retests,
- arm Sentinel near qualified zones,
- interpret Sentinel events into deterministic reject / keep-watching / candidate / disarm outcomes.

Likely required Surveyor families:

- `feed_state`
- `structure_state`
- `sr_zones`
- `interaction_lifecycle`

Likely optional / caution-influencing families:

- `fib_context`
- `dynamic_levels`

The pack should not assume every SR zone is tradeable. Its first job is to identify qualified zones of interest and define the conditions under which a Sentinel watch should begin.

---

## 6) Control loop

### Live loop

1. Surveyor updates descriptive bundles for the in-scope pair universe.
2. Arbiter loads the active strategy pack.
3. Arbiter scans each pair's Surveyor bundle for qualified zones.
4. Arbiter emits a `watch_intent` for each qualified watch context.
5. Sentinel watches price/action around each armed context.
6. Sentinel emits watch events.
7. Arbiter routes each event to the active strategy pack handler.
8. Arbiter emits a decision record.
9. Sentinel disarms when Arbiter revokes the watch, the watch expires, the setup produces a candidate/trade decision, or price blows through the level without the required reaction.
10. Downstream execution or human review consumes Arbiter records later.

### Replay/backtest loop

Replay should simulate Sentinel using historical OHLCV and the same watch-intent/event vocabulary.

For the SR first-retest pack, historical `15m` high/low/close can determine when price first approaches or enters the zone. From that point, the same confirmation, CHoCH, invalidation, no-reaction blow-through, timeout, and disarm rules should drive the Arbiter event handler.

The replay path must not give Arbiter information that would not have been available at the historical event timestamp.

---

## 7) Initial contracts

### Watch intent shape

Minimum fields:

```json
{
  "schema": "sentinel_watch_intent_v1",
  "watch_id": "deterministic-id",
  "created_at": "2026-04-29T00:00:00Z",
  "mode": "live|replay|backtest",
  "strategy_pack_id": "sr_first_retest_v1",
  "strategy_pack_version": "0.1.0",
  "symbol": "BTC-USDT-SWAP",
  "source_profile": "arbiter_core",
  "source_bundle_id": "...",
  "zone": {
    "zone_id": "...",
    "side": "support|resistance|flip_support|flip_resistance",
    "low": 0.0,
    "high": 0.0,
    "timeframe": "4H"
  },
  "arm": {
    "approach_threshold": "max(0.5_ATR_4H, 0.75pct)",
    "confirmation_timeframe": "15m",
    "confirmation_trigger": "15m_CHoCH",
    "expires_after": "PT6H"
  },
  "invalidation": {
    "rule": "breach_zone_with_buffer",
    "buffer": "strategy-defined",
    "no_reaction_rule": "blow_through_level_without_15m_CHoCH"
  }
}
```

### Sentinel event shape

Minimum fields:

```json
{
  "schema": "sentinel_watch_event_v1",
  "event_id": "deterministic-id",
  "watch_id": "deterministic-id",
  "observed_at": "2026-04-29T00:15:00Z",
  "event_type": "entered_zone",
  "symbol": "BTC-USDT-SWAP",
  "mode": "live|replay|backtest",
  "evidence": {
    "timeframe": "15m",
    "candle_open_time": "2026-04-29T00:00:00Z",
    "high": 0.0,
    "low": 0.0,
    "close": 0.0
  },
  "provenance": {
    "source": "canonical_candles|replay_ohlcv",
    "source_bundle_id": "..."
  }
}
```

### Arbiter decision record

Minimum fields:

```json
{
  "schema": "arbiter_decision_record_v1",
  "decision_id": "deterministic-id",
  "strategy_pack_id": "sr_first_retest_v1",
  "strategy_pack_version": "0.1.0",
  "symbol": "BTC-USDT-SWAP",
  "mode": "watch|candidate|reject|disarm",
  "caution_level": "none|low|medium|high|blocked",
  "reason_codes": [],
  "source_bundle_id": "...",
  "watch_id": "...",
  "sentinel_event_id": "...",
  "provenance": {}
}
```

---

## 8) Fail-closed rules

Arbiter should reject or block if:

- the active strategy pack is missing or ambiguous,
- more than one strategy pack is armed for the same run/scope,
- required Surveyor families are missing,
- required families are stale or degraded beyond pack tolerance,
- Sentinel events cannot be linked to a valid watch intent,
- replay events include future-leaking evidence,
- zone identifiers or fingerprints are unstable,
- provenance is incomplete.

Sentinel should disarm or degrade if:

- Arbiter emits a candidate / trade-taken decision for the watch,
- the watch expires,
- invalidation fires,
- price blows through the watched level without the required 15m CHoCH / reaction,
- price moves materially away from the watch context,
- source feed continuity becomes unsafe,
- Arbiter revokes the watch,
- the source Surveyor context is superseded in a way the pack marks invalid.

---

## 9) Non-goals

This architecture does not yet define:

- exchange order placement,
- position sizing,
- account/risk integration,
- portfolio-level conflict resolution,
- multi-pack concurrent policy arbitration,
- live top-100 readiness claims,
- low-latency/HFT behavior.

Those can be designed later. V1 should prove the Surveyor -> Arbiter -> Sentinel -> Arbiter record loop first.

---

## 10) First build slice

Recommended first implementation modules after this spec is accepted:

- `market_arbiter/arbiter/profile.py`
- `market_arbiter/arbiter/strategy_pack.py`
- `market_arbiter/arbiter/watch_intent.py`
- `market_arbiter/arbiter/decision_record.py`
- `market_arbiter/arbiter/sr_first_retest.py`
- `market_arbiter/sentinel/`
- fixture tests for complete / stale / degraded / replay-only / invalidated / timeout watch flows

The first acceptance target should be fixture/replay parity: the SR first-retest pack should produce the same decision sequence from live-shaped Sentinel events and replay-simulated Sentinel events when given equivalent evidence.
