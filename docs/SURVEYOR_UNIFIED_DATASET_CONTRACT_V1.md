# Surveyor Unified Dataset Contract V1

Date: 2026-04-21  
Status: proposed near-term contract  
Intent: freeze one shared Surveyor dataset bundle shape before deeper code extraction

---

## 1) Why this contract exists

The next boundary to freeze is not another one-off packet field.
It is a **unified Surveyor dataset bundle** that can support the same market-state output across:
- operator UI rendering,
- downstream payload delivery,
- Arbiter dataset selection,
- and later replay / simulation / backtesting.

The point is to stop treating UI JSON, payload JSON, and replay/backtest inputs as separate truths.
They should all come from the same descriptive Surveyor contract.

---

## 2) Design rules

1. **Surveyor stays descriptive**
   - it publishes market-state datasets
   - it does not decide whether a setup should be traded

2. **One bundle, many dataset families**
   - Surveyor should publish a bundle that can contain multiple analysis families
   - Arbiter can consume only the subset it cares about

3. **UI and payload delivery should read the same bundle**
   - no UI-only hidden structure
   - no payload-only custom shape that drifts from UI truth

4. **Replay and backtesting should use the same bundle shape**
   - live and replay modes may differ in provenance
   - they should not differ in core dataset envelope shape

5. **Every dataset family carries its own status and provenance**
   - complete, partial, stale, replay_only, unavailable
   - provider / dataset / trace / source-event lineage should stay visible

6. **Historical repair quality must be first-class, not hidden in logs**
   - if candles were repaired deterministically, that must be visible
   - if a timeframe crossed into elevated/degraded/blocked quality, that must be visible
   - circuit-breaker posture belongs in the dataset, not only in operator memory

7. **Partial truth is allowed, hidden partiality is not**
   - a bundle may be partial
   - consumers must be able to see exactly which families or timeframes are degraded

---

## 3) Unified bundle shape

Near-term proposed top-level shape:

```json
{
  "meta": {
    "bundle_contract": "surveyor_unified_dataset_bundle_v1",
    "bundle_id": "surveyor_bundle:BTCUSDT:...",
    "symbol": "BTCUSDT",
    "as_of_ts": "2026-04-21T17:00:00Z",
    "build_mode": "live|replay|audit_ui|payload",
    "bundle_status": "complete|partial|degraded",
    "primary_feed_provider": "OKX",
    "continuity_state": "live_continuous|one_shot_refresh|replay_only|mixed"
  },
  "coverage": {
    "required_timeframes": ["1W", "1D", "4H", "5m"],
    "available_timeframes": ["1W", "1D", "4H", "5m"],
    "missing_timeframes": [],
    "freshness_summary": {
      "fresh": ["1W", "1D", "4H", "5m"],
      "stale": [],
      "partial": [],
      "replay_only": []
    },
    "quality_band_summary": {
      "clean": ["1W", "1D", "4H"],
      "benign": [],
      "elevated": ["5m"],
      "degraded": [],
      "blocked": []
    }
  },
  "datasets": {
    "feed_state": {},
    "structure_state": {},
    "sr_zones": {},
    "fib_context": {},
    "dynamic_levels": {},
    "interaction_lifecycle": {}
  },
  "delivery_profiles": {
    "ui_full": ["feed_state", "structure_state", "sr_zones", "fib_context", "dynamic_levels", "interaction_lifecycle"],
    "arbiter_core": ["feed_state", "structure_state", "sr_zones", "fib_context", "dynamic_levels"],
    "backtest_core": ["feed_state", "structure_state", "sr_zones", "fib_context", "dynamic_levels", "interaction_lifecycle"]
  }
}
```

This is intentionally a **bundle of families**, not a single flat packet that assumes all future analysis types look the same.

---

## 4) Standard dataset-family envelope

Each dataset family should use the same outer envelope even if the inner payload differs.

```json
{
  "family": "sr_zones",
  "contract_version": "surveyor_sr_dataset_v1",
  "status": "complete|partial|stale|replay_only|unavailable",
  "summary": {},
  "timeframes": {
    "1D": {},
    "4H": {}
  },
  "provenance": {
    "feed_provider": "OKX",
    "dataset_mode": "live|replay|mixed",
    "dataset_id": "...",
    "trace_id": "...",
    "source_event_id": "...",
    "source_swing_id": "...",
    "source_contract_version": "..."
  },
  "payload": {}
}
```

Required envelope semantics:
- `family`: stable dataset-family key
- `contract_version`: family-local contract version, not just the top-level bundle version
- `status`: explicit family health/completeness
- `summary`: compact surface for UI cards, payload summaries, and Arbiter quick checks
- `timeframes`: per-timeframe surfaces when relevant
- `provenance`: where the family came from and what upstream structure it depends on
- `payload`: full family-specific detail

---

## 5) Initial family set

### A. `feed_state`
Current source: `market_data`

Purpose:
- canonical candle availability
- freshness / checkpoint state
- dataset mode and continuity posture
- historical repair summary / quality band / circuit-breaker posture

This is the family that tells downstream consumers whether the rest of the bundle is trustworthy for live decisions or only useful for replay/research.
It is also the family that tells Arbiter and backtesting whether the canonical candles are:
- clean,
- benignly repaired,
- elevated and warning-worthy,
- degraded for a timeframe,
- or blocked at the canonical `5m` layer.

### B. `structure_state`
Current source: `structure`

Purpose:
- current regime / transition state by timeframe
- protected / validated levels
- structure events and swings
- upstream structure provenance

### C. `sr_zones`
Current source: `sr`

Purpose:
- selected support / resistance surfaces
- grouped zones by timeframe and relative position
- selector surface used to choose the active operator-facing truth

This should remain a first-class family instead of being buried under generic structure metadata.
That matters for the intraday revisit and later robust backtesting because S/R logic needs explicit provenance and family-level versioning.

### D. `fib_context`
Current source: `fib`

Purpose:
- active fib contexts by timeframe
- anchor provenance
- tap history and overlap state

### E. `dynamic_levels`
Current source: `dynamic_levels`

Purpose:
- dynamic levels derived from current market state
- selected zone relation
- distance / interaction context

### F. `interaction_lifecycle`
Current source: `interaction_lifecycle`

Purpose:
- structure events
- zone interactions
- level interactions
- retests / breaches / state changes

This family is especially important for replay and backtesting because it captures eventful state transitions rather than only static snapshots.

---

## 6) Planned future family slots

The bundle should be open to additional dataset families without changing the top-level shape.
Near-term planned examples:
- `supply_demand_zones`
- `delta_volume`
- `liquidity_sweeps`
- `regime_context`
- `session_context`
- `backtest_labels`

Adding a family should usually mean:
1. define one new family-local contract
2. add it under `datasets`
3. optionally add it to one or more delivery profiles
4. keep the rest of the bundle unchanged

---

## 7) Arbiter selection contract

Arbiter should not parse everything by default.
It should request a declared profile or family subset.

Near-term profile shape:

```json
{
  "profile_id": "arbiter_sr_intraday_v1",
  "required_families": ["feed_state", "structure_state", "sr_zones"],
  "optional_families": ["fib_context", "dynamic_levels", "interaction_lifecycle"],
  "hard_fail_on_missing": ["feed_state", "structure_state", "sr_zones"]
}
```

Example posture:
- an SR-centric intraday Arbiter may require `sr_zones`
- a fib-heavy research pass may require `fib_context`
- a replay/backtest evaluator may require `interaction_lifecycle`

This keeps Surveyor broad while letting Arbiter stay selective.

### Consumer acceptance gate

Profile selection only answers “which families are included?” A separate acceptance gate answers “may this consumer use the payload?”

Initial implementation lives in `market_arbiter/core/surveyor_profile_acceptance.py` and emits `surveyor_profile_acceptance_v1` records.

Initial policy:
- `arbiter_core` requires `feed_state`, `structure_state`, and `sr_zones`; missing, stale, partial, unavailable, degraded, blocked, invalid, unknown, or replay-only required families are rejected for live decision use.
- `arbiter_core` treats optional `fib_context` / `dynamic_levels` issues as explicit caution/watch-only, not silent contamination.
- `backtest_core` requires `feed_state`, `structure_state`, `sr_zones`, and `interaction_lifecycle`; replay-mode / replay-only required families are allowed for backtest evaluation.
- unknown profile policies reject instead of guessing downstream consumer rules.

Acceptance statuses are `accepted`, `accepted_with_caution`, and `rejected`; decision modes are consumer-specific (`live_decision_candidate`, `live_watch_only`, `backtest_candidate`, or `rejected`). Exported profile payloads can embed this verdict under `acceptance` when generated with `--include-acceptance`.

---

## 8) UI and payload delivery posture

### UI
The operator UI now renders the dataset bundle directly.
That means:
- top-level bundle meta at the top
- family cards / tabs driven by `datasets`
- status banners driven by family `status`
- no separate hidden UI-only packet contract

Implementation status as of 2026-04-27: complete for the UI-migration slice in `market_arbiter/web/app.py`; legacy packet JSON remains compatibility/raw inspection only.

### Payload delivery
Outgoing payloads are generated from the same bundle by profile.
That means:
- full payloads for operator/audit surfaces
- reduced payloads for downstream consumers
- profile-driven filtering, not alternate bespoke schemas
- selected payloads preserve dataset-family envelopes exactly as emitted by the unified bundle

Implementation status as of 2026-04-27: initial profile-selected payload helper exists in `market_arbiter/core/surveyor_bundle_profile.py`. It emits deterministic `surveyor_bundle_profile_payload_v1` payloads, preserves dataset-family envelopes, supports deterministic serialization, and fails closed on missing required families for Arbiter/backtest profiles.

Operator export path: `python3 -m market_arbiter.ops.surveyor_bundle_export --bundle-path <bundle.json> --profile arbiter_core --output-path <payload.json>`. Add `--include-acceptance` to embed the `surveyor_profile_acceptance_v1` verdict in the exported payload. The same command can export from a live DB snapshot with `--db-path <sqlite> --symbol <symbol> --profile <profile>`.

### Backtesting and replay
Replay and backtesting should use the same family envelopes, with provenance differences only.
That means:
- `build_mode` and dataset provenance can say `replay`
- family contracts should remain stable
- evaluation logic should not depend on a separate replay-only schema

Initial frozen fixtures live under `tests/fixtures/surveyor_bundle_profiles/` for complete live-ish, partial/degraded, and replay-mode bundle shapes.

---

## 9) Mapping from the current packet

Near-term mapping from the extracted packet shape:
- `market_data` -> `datasets.feed_state`
- `structure` -> `datasets.structure_state`
- `sr` -> `datasets.sr_zones`
- `fib` -> `datasets.fib_context`
- `dynamic_levels` -> `datasets.dynamic_levels`
- `interaction_lifecycle` -> `datasets.interaction_lifecycle`
- `contract_versions` -> bundle-level and family-level contract metadata

This means the current extracted packet is not discarded.
It becomes the phase-1 source material for the bundle refactor.

---

## 10) Near-term implementation bias

Current status:
1. keep the current packet builder working — done
2. introduce the bundle envelope without changing family payload logic first — done
3. move UI rendering to family-driven bundle sections — done
4. move payload delivery to profile-driven bundle selection — initial helper/tests/operator export path landed
5. freeze replay/backtest fixtures against the same family envelopes — initial fixture pack landed
6. freeze consumer acceptance rules for profile payloads — initial `arbiter_core` / `backtest_core` rules landed
7. only then add new dataset families or real Arbiter logic

That order keeps the contract freeze ahead of code sprawl.

---

## 11) Decision

The next permanent truth surface for Surveyor should be:
- one **unified dataset bundle**,
- with stable family envelopes,
- generated once,
- rendered in UI and payload delivery from the same source,
- and reusable for live analysis plus robust replay/backtesting.
