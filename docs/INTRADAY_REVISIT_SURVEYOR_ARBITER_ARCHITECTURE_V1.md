# Intraday Revisit v1, Surveyor / Arbiter Architecture

Date: 2026-04-19
Updated: 2026-04-30
Status: current branch checkpoint, shareable external architecture note
Audience: collaborators, prospective data providers, and technically literate reviewers

---

## 1) Why this exists

This project is building a deterministic intraday market-analysis stack for crypto pairs.

The immediate goal is **not** to build an execution bot first. The immediate goal is to:
- ingest a canonical market-data feed,
- compute a consistent multi-timeframe market-state packet,
- audit that packet in a human-facing UI,
- and use that packet as the basis for robust replay, simulation, and backtesting.

This writeup is meant to be shareable with potential data-feed partners so the use case is explicit up front.

---

## 2) Current use case, stated plainly

We want programmatic market data access so we can run an internal research and analysis workflow for:
- multi-timeframe structure analysis,
- support/resistance surface generation,
- Fib-anchor context,
- dynamic-level context,
- packetized market-state snapshots,
- and later replay / simulation / backtesting.

### What the feed is used for now

Right now, the feed is used to populate a canonical candle store and feed-health checkpoints that drive the **Surveyor** layer.

### What the feed is not used for now

At the current phase, this feed integration is **not** the order-execution layer.
The project intentionally keeps:
- market-data ingestion,
- descriptive analysis,
- decision logic,
- and future execution logic

as separate concerns.

---

## 3) Architectural principle

The central design rule is:

- **Surveyor** = descriptive only
- **Arbiter** = strategy interpretation / decision layer
- **Sentinel** = armed watch layer for live or replay event detection
- **Execution** = separate future boundary

That separation is deliberate.

### Surveyor
Surveyor reports what the market state appears to be, with provenance.
It should describe:
- candle availability and freshness,
- structure state by timeframe,
- selected S/R surfaces,
- Fib context,
- dynamic levels,
- and packet completeness / partiality.

Surveyor should **not** decide whether a setup is worth trading.

### Arbiter
Arbiter is the first layer allowed to interpret Surveyor output into decision logic.
Examples:
- does this packet represent a valid setup,
- is the evidence strong enough,
- is the packet incomplete or degraded,
- should this become watch-only, reject, or candidate.

Arbiter is also the layer that owns the active strategy pack. A strategy pack defines which Surveyor families are required, how zones of interest are selected, how Sentinel watches should be armed, and how Sentinel feedback is converted into deterministic decision records. Only one strategy pack should be armed for a given Arbiter run/scope at a time.

### Sentinel
Sentinel sits between Arbiter and the live/replay market stream.

Its job is to watch narrowly-scoped conditions after Arbiter has identified a zone or setup worth monitoring. Sentinel should answer event questions such as:
- has price approached the zone,
- has price entered or rejected the zone,
- has a first-retest watch condition fired,
- has the 15m CHoCH / reaction trigger appeared after entry,
- has price blown through the level without that reaction,
- has invalidation happened,
- has the watch timed out,
- or should the watch be disarmed because the underlying Surveyor/Arbiter context is no longer valid.

Sentinel should **not** decide trades. It reports watch events back to Arbiter. Arbiter remains responsible for accepting, rejecting, escalating, or producing a downstream candidate record.

This keeps Surveyor from carrying a permanent lower-timeframe trigger feed. Surveyor remains the broad descriptive state layer; Sentinel temporarily owns lower-timeframe watch cadence only for Arbiter-armed zones of interest.

### Execution
Execution, if it exists later, remains downstream of Arbiter and should not be fused into the feed/analysis/watch layers. In this document, “execute” at the Arbiter layer means “emit a deterministic decision/candidate record for a downstream executor or human review,” not place orders directly.

---

## 4) Current system shape

## 4.1 Canonical feed ingestion

Current implementation status:
- primary source in this checkpoint: **OKX via CCXT**
- current covered timeframes for Surveyor: `1W`, `1D`, `4H`, `5m`
- persistence target: SQLite canonical market-data store

Current code path:
- `market_arbiter/ops/surveyor_feed_refresh.py`

Current persisted domains used by this path:
- `market_candles`
- `feed_checkpoints`
- `feed_health_events`

The feed-refresh layer is responsible for:
- pulling recent closed OHLCV windows,
- repairing narrowly-invalid historical candles deterministically before canonical persistence,
- upserting canonical candles,
- checkpointing freshness/state,
- classifying repair quality bands and timeframe-scoped circuit-breaker actions,
- and surfacing degraded conditions explicitly.

This is important because the downstream packet should know the difference between:
- fresh,
- stale,
- partial,
- replay-only inputs,
- and repaired-but-still-usable vs degraded/blocked historical inputs.

## 4.2 Surveyor packet assembly

Current code path:
- `market_arbiter/core/surveyor_snapshot.py`
- `market_arbiter/surveyor/surveyor_packet.py`

The packet assembly layer pulls together:
- canonical market candles,
- feed checkpoint state,
- repair metadata / quality bands / circuit-breaker posture,
- structure context,
- authoritative S/R surfaces,
- Fib context,
- dynamic levels,
- and packet metadata / provenance.

The packet is intended to be a single descriptive handoff artifact for downstream consumers.

## 4.3 Structure / S&R / Fib / dynamic-level composition

Current branch posture:
- structure state is a first-class upstream input,
- S/R surfaces remain their own selected operator-facing truth,
- Fib uses the Phase 1 contract path,
- dynamic levels are packetized as context,
- and the system is moving toward one shared structure/provenance contract across these consumers.

That means the design goal is not to collapse everything into one algorithm.
The design goal is to make the upstream market-state contract consistent while allowing downstream consumers to interpret that state differently.

## 4.4 Operator-facing UI

Current code path:
- `market_arbiter/web/app.py`

The UI currently serves as an audit and operator review surface.
It now includes a Surveyor packet view wired to the canonical store.

Its purpose is to let a human inspect:
- whether packet inputs are fresh,
- whether required timeframes are present,
- what the descriptive packet says,
- and whether the system is complete or partial before any later decision layer is trusted.

## 4.5 Arbiter / Sentinel control loop

The missing live-decision element is Sentinel.

The intended control loop is:

1. **Surveyor** maintains descriptive market-state bundles across the in-scope trading universe.
2. **Arbiter**, using exactly one active strategy pack, scans those bundles for zones of interest.
3. For the first strategy pack, Arbiter looks for first-retest SR / flip-zone opportunities across every in-scope pair.
4. When a qualified zone exists, Arbiter emits a `watch_intent` to **Sentinel** instead of continuously running full decision logic on every tick.
5. **Sentinel** watches only the armed symbol/zone/strategy context at a tighter cadence and emits events such as `approached_zone`, `entered_zone`, `first_retest_touch`, `confirmation_window_open`, `choch_observed`, `no_reaction_blow_through`, `invalidation_breach`, `timeout`, or `disarm`.
6. **Arbiter** consumes Sentinel events through the active strategy pack and produces deterministic decision records: reject, keep watching, promote to candidate, or disarm.
7. Any real order execution remains a separate downstream boundary.

This keeps Surveyor broad, Arbiter selective, Sentinel focused, and execution separate.

For backtesting and replay, Sentinel should be simulated from historical OHLCV using the same watch intent and event vocabulary. Example: historical `15m` high/low/close determines when price first approached or entered the zone, then the active strategy pack evaluates the same CHoCH confirmation, invalidation, no-reaction blow-through, timeout, and disarm sequence Arbiter would receive live.

### Strategy packs

Arbiter should not hard-code every future strategy into one monolith.

A strategy pack is a versioned, testable bundle that can include:
- required and optional Surveyor families / profiles,
- pair-universe filters,
- setup qualification rules,
- zone-of-interest selection logic,
- Sentinel arming parameters,
- Sentinel event handlers,
- decision-record output schema extensions,
- replay/backtest adapters,
- fixtures and acceptance tests.

Only one strategy pack should be active for a given Arbiter run/scope. That constraint prevents hidden policy mixing while we are still proving each edge independently.

The first expected pack is an SR first-retest pack, aligned with the existing `foxian_retest_flip_confluence_v0` research/backtest lane. Its job is not “trade everything”; its job is to decide when a Surveyor-described SR/structure context is strong enough to arm Sentinel for the first retest and how to interpret the resulting watch events.

---

## 5) Why a canonical feed matters

The project is intentionally moving away from over-relying on ad hoc bootstrap/static artifacts.

The core requirement is:
- one declared primary feed,
- one canonical persisted candle surface,
- visible freshness/provenance,
- and deterministic downstream packet generation.

Without that, backtesting and replay become too easy to contaminate with mixed assumptions.

So the feed request is not “we want more data because data is nice.”
It is specifically because the architecture depends on a stable, auditable market-data baseline.

---

## 6) What we would want from a data provider

At minimum, useful access would be:
- programmatic OHLCV / candlestick data,
- recent and historical coverage,
- stable symbol/timeframe semantics,
- enough rate/reliability characteristics for close-cadence polling,
- and clear policy around retention / replay use.

Nice-to-have additions later:
- funding history,
- open-interest history,
- or other context fields that can be kept clearly separate from the canonical candle baseline.

Current architecture does **not** require an HFT-style ultra-low-latency feed.
The immediate system is oriented around deterministic close-cadence analysis, packet assembly, and replayable research.

---

## 7) Current checkpoint status

Done in the current branch checkpoint:
- canonical Surveyor packet assembly path exists,
- canonical OKX/CCXT feed refresh path exists,
- BloFin recovery supports deterministic historical candle repair for repairable envelope defects,
- feed-state packet surfaces can carry `repair_summary`, `quality_band`, and `circuit_breaker_action`,
- unified Surveyor dataset bundle assembly exists and is attached to packet snapshots,
- dataset-family envelopes exist for `feed_state`, `structure_state`, `sr_zones`, `fib_context`, `dynamic_levels`, and `interaction_lifecycle`,
- bundle delivery profiles exist for `ui_full`, `arbiter_core`, and `backtest_core`,
- profile-selected bundle payloads can be exported deterministically,
- `arbiter_core` and `backtest_core` profile selection fail closed on missing required families,
- consumer acceptance records can be generated and optionally embedded in exported profile payloads,
- the initial live/degraded/replay fixture corpus exists,
- Surveyor UI migration to bundle-driven rendering is complete,
- legacy packet JSON remains available only as compatibility/raw inspection,
- manifest-driven symbol onboarding and active workset rendering exist,
- local rollout `report` and `advance-check` artifacts can be generated,
- initial Foxian mentorship strategy backtesting mechanism exists: `market_arbiter/arbiter/strategy_backtest.py` consumes `backtest_core` profiles and emits deterministic retest event rows plus trade-candidate templates,
- initial OHLCV simulation path exists: `market_arbiter/arbiter/ohlcv_backtest.py` and `market_arbiter/ops/strategy_backtest_run.py` load per-symbol OHLCV CSV/JSON, simulate candidates across multiple pairs, and emit trade + event-study reports,
- snapshot, feed, onboarding, bundle-UI, profile-export, profile-acceptance, strategy-backtest, and OHLCV simulator helper tests exist.

Relevant files:
- `market_arbiter/core/surveyor_snapshot.py`
- `market_arbiter/ops/surveyor_feed_refresh.py`
- `market_arbiter/ops/blofin_ws_candle5m_consumer.py`
- `market_arbiter/ops/surveyor_symbol_onboarding.py`
- `market_arbiter/ops/surveyor_bundle_export.py`
- `market_arbiter/surveyor/surveyor_packet.py`
- `market_arbiter/web/app.py`
- `market_arbiter/core/surveyor_bundle_profile.py`
- `market_arbiter/core/surveyor_profile_acceptance.py`
- `tests/test_surveyor_snapshot.py`
- `tests/test_surveyor_feed_runner.py`
- `tests/test_surveyor_symbol_onboarding.py`
- `tests/test_surveyor_ui_bundle.py`
- `tests/test_surveyor_bundle_profile.py`
- `tests/test_surveyor_bundle_export.py`
- `tests/test_surveyor_profile_acceptance.py`

Known current blocker:
- feed top-100 readiness is still a separate execution lane. The feed thread owns confirmed-close / soak / supervision / shard-readiness evidence, and no Arbiter or replay work should claim live top-100 readiness until that lane produces its readiness packet.

Architecturally in scope but not yet implemented:
- `docs/ARBITER_ARCHITECTURE_V1.md` should define the Arbiter / Sentinel / strategy-pack boundary before real live Arbiter logic is added,
- Arbiter needs an active strategy-pack registry with a one-pack-active constraint,
- Sentinel needs a watch-intent contract and event vocabulary,
- the first SR first-retest strategy pack needs fixtures that prove live and replay Sentinel events are interpreted identically.

Validated recently:
- targeted packet/feed/UI/onboarding tests have passed in the current implementation slices,
- bundle/profile closeout proof: `27 passed, 1 warning` for the profile export / acceptance / bundle UI helper slice; the warning is the known local urllib3/LibreSSL environment warning,
- the live UI responded on `http://127.0.0.1:8501/` during the bundle-driven UI validation,
- current documentation status and roadmap are summarized in `docs/MARKETARBITER_STATUS_AND_ROADMAP_2026-04-27.md`.

---

## 8) Where we are and what is next

The UI-migration slice is complete, and the unified bundle/profile freeze thread is complete enough to close. Surveyor now has a stable descriptive handoff surface for UI, profile-selected payload export, initial Arbiter consumption, and replay/backtest fixtures.

Current macro-plan:

1. **Phase 1 — unified bundle/profile freeze: closed for this thread**
   Landed: profile-selected Surveyor bundle payloads, deterministic operator export path, initial fixture corpus, fail-closed profile selection, consumer acceptance rules, and optional embedded acceptance verdicts in exports.

2. **Phase 2 — replay/backtest contract + broader fixture matrix: ready to start before feed top-100 is done**
   This can proceed from frozen fixtures and exported `backtest_core` payloads. The first mechanism is documented in `docs/STRATEGY_BACKTESTING_MECHANISM_V0.md` and implemented as `foxian_retest_flip_confluence_v0`. The first OHLCV runner is documented in `docs/OHLCV_BACKTEST_INPUT_CONTRACT_V0.md`. Remaining work: replay bundle builder path from historical OHLCV into point-in-time Surveyor profiles, deterministic baseline comparison harness, top-100 OHLCV manifest/run artifacts, and a broader accepted/rejected/degraded/replay-only fixture matrix.

3. **Phase 3 — feed top-100 readiness: continues in the feed execution lane**
   This remains necessary for live operational readiness but should not block fixture-driven replay/backtest work or Arbiter architecture writing. Remaining work includes confirmed-close/soak proof, shard-aware websocket ingestion, per-shard status, bounded recompute worker pool, provider/IP guardrails, cohort evidence, and a pre-top-100 readiness packet.

4. **Phase 4 — Arbiter / Sentinel architecture specification: can begin now, with one constraint**
   `ARBITER_ARCHITECTURE_V1.md` can be written against the frozen `arbiter_core` profile and fixture matrix before feed top-100 is complete. It must not assume live top-100 readiness yet. It should define module boundaries, input profiles, strategy-pack registry, one-active-pack constraint, watch-intent shape, Sentinel event vocabulary, decision-record shape, caution/eligibility model, rejection reasons, provenance requirements, replay/backtest behavior, non-goals, and the execution boundary.

5. **Phase 5 — Arbiter v1 build: partially unblocked before feed completion**
   The first narrow implementation can be fixture-driven: consume `arbiter_core`, activate the SR first-retest strategy pack, scan in-scope pairs for zone/watch opportunities, emit Sentinel watch intents, consume simulated Sentinel events, classify usability/caution/decision mode, and fail closed on missing/degraded required inputs. Live-feed Sentinel validation waits for Phase 3 evidence.

6. **Phase 6 — Arbiter review UI: can be designed early; live-backed review waits**
   The review surface can be designed and demoed from fixtures before feed completion, but live operator confidence depends on Phase 3 feed-readiness evidence.

---

## 9) Summary for external sharing

If shared with a prospective feed provider, the honest summary is:

> We are building a deterministic intraday research stack that uses programmatic market data to populate a canonical candle store, generate a multi-timeframe descriptive market-state packet, inspect that packet in an operator UI, and later drive robust replay/simulation/backtesting. The current phase is analysis and evidence-building first, with decision and execution intentionally kept as separate downstream layers.

That is the actual use case this feed access would support.
