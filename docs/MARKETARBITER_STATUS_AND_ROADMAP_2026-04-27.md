# MarketArbiter Status + Roadmap

Date: 2026-04-27
Updated: 2026-04-30
Status: current control-plane snapshot for `#marketarbiter`
Intent: record what is complete, what remains, and the recommended roadmap from Surveyor closure through Arbiter design/build.

---

## 1) Current summary

MarketArbiter is now correctly centered on a layered architecture:

- **Surveyor** is the descriptive market-state layer.
- **Arbiter** is the future selective interpretation / decision layer and active strategy-pack owner.
- **Sentinel** is the newly identified armed-watch layer that monitors Arbiter-selected symbol/zone contexts and reports events back to Arbiter.
- **Execution** remains deliberately downstream and out of scope for the current build.

The most important update: the **Surveyor UI migration to bundle-driven rendering is complete**. The UI now reads the unified Surveyor dataset bundle directly and renders dataset-family tabs from the bundle/profile contract instead of maintaining a separate UI-only packet shape.

That closes the UI-migration slice. The remaining bundle work is **not** UI migration anymore; it belongs to the broader **unified bundle + family envelopes + profile delivery / bundle freeze** slice.

---

## 1.1) Active phase / thread map

Checked: 2026-04-30

Authoritative control plane:

- `#marketarbiter` / Discord channel `1469501772367138887`

Current phase ownership:

- **Phase 0 — control-plane reset in `#marketarbiter`: in flight / active**
  - Thread/channel: `#marketarbiter` (`1469501772367138887`)
  - Notes: This is the macro-plan surface. Cross-lane sequencing and status rollups should land here.

- **Phase 1 — unified bundle/profile freeze: closed, not in flight**
  - Threads:
    - `Surveyor unified bundle + family envelopes` (`1497455208152830022`) — archived / closed.
    - `MarketArbiter bundle profile freeze` (`1498351248620130475`) — thread still exists, but workstream is closed.
  - Notes: Do not treat Phase 1 as an active lane unless a consumer-specific hardening request reopens it.

- **Phase 2 — replay/backtest contract and fixtures: in flight**
  - Thread: `Strategy Backtesting` (`1498877004123078756`) under `#marketarbiter`.
  - Current scope: strategy hypothesis, historical data source/import, OHLCV simulation, and validation spike before deeper Arbiter build.
  - Gap: this thread is active, but it has not yet fully closed the original Phase 2 replay-bundle-builder / broader fixture-matrix exit gate.

- **Phase 3 — feed top-100-ready execution lane: in flight, but still in legacy thread**
  - Thread: `Intraday Revisit v1 — SR Zone Logic + Robust Backtesting Plan` (`1477100380662927520`) under the older ideation parent.
  - Current scope: provider bakeoff / canonical top-100 Surveyor feed selection, including provider-neutral smoke runner and later 24h side-by-side bakeoff.
  - Note: This is active work, but it is not yet a clean child thread under `#marketarbiter`. Either keep explicitly bridging status back here or create a replacement `#marketarbiter` feed-provider bakeoff thread.

- **Phase 4 — Arbiter / Sentinel architecture specification: in flight, no dedicated thread**
  - Thread: none yet.
  - Current surface: `#marketarbiter` channel plus `docs/ARBITER_ARCHITECTURE_V1.md`.
  - Gap: the architecture draft exists and was updated for Sentinel arm/disarm + 15m CHoCH semantics, but there is no active dedicated architecture thread.

- **Phase 5 — Arbiter v1 build: empty / no active thread**
  - Thread: none.
  - Status: not started as an implementation lane. Fixture-first build is partially unblocked, but should wait for Phase 4 contract hardening and Phase 2 validation clarity.

- **Phase 6 — Arbiter review UI / operator inspection: empty / no active thread**
  - Thread: none.
  - Status: not started. Can be designed from fixtures later, but live-backed confidence waits on feed evidence.

---

## 2) Completed / materially landed

### A. Repo extraction and identity

Complete enough to treat `MarketArbiter` as the successor repo for the Surveyor/Arbiter effort:

- extracted repo identity and README exist,
- old LiquidSniper execution/paper-runtime identity is intentionally not the center,
- `market_arbiter/surveyor/` holds descriptive analysis code,
- `market_arbiter/arbiter/` exists only as a scaffold,
- current docs now treat Surveyor/Arbiter separation as the project boundary.

### B. Canonical feed and packet foundation

Substantially landed:

- SQLite-backed canonical candle storage,
- market-data migrations,
- feed checkpoints / feed-health surfaces,
- OKX/CCXT refresh path retained from the first extraction,
- BloFin feed contract and recovery docs added,
- BloFin websocket 5m consumer exists,
- historical hydration and deterministic repair paths exist,
- Surveyor packet snapshot assembly exists,
- targeted feed / packet / scheduler tests exist.

### C. Unified Surveyor bundle

Landed in code as the primary Surveyor handoff surface:

- `build_surveyor_dataset_bundle(...)` exists in `market_arbiter/core/surveyor_snapshot.py`,
- bundle meta includes contract, symbol, status, build mode, feed provider, continuity state, and legacy packet status,
- dataset-family envelopes now exist for:
  - `feed_state`,
  - `structure_state`,
  - `sr_zones`,
  - `fib_context`,
  - `dynamic_levels`,
  - `interaction_lifecycle`,
- delivery profiles exist in the bundle:
  - `ui_full`,
  - `arbiter_core`,
  - `backtest_core`.

### D. Bundle-driven operator UI

Complete for the UI-migration slice:

- `market_arbiter/web/app.py` renders the unified bundle directly,
- top-level bundle status / contract / coverage are visible,
- dataset families are rendered from the `ui_full` profile,
- each family has summary / timeframes / payload / issues sections,
- legacy packet JSON is retained only as a compatibility/raw expander,
- focused UI helper tests exist in `tests/test_surveyor_ui_bundle.py`.

### E. Top-100-ready feed target and onboarding start

The feed target has been reset correctly:

- closeout target is **top-100-ready**, not merely single-pair-operable,
- `docs/SURVEYOR_FEED_TOP100_READY_TARGET_V1.md` defines the staged path,
- `docs/FEED_PROVIDER_BAKEOFF_PLAN_V1.md` now defines the provider-selection gate; BloFin is incumbent/canary, not presumed canonical,
- `docs/SYMBOL_ONBOARDING_AND_100_PAIR_SCALE_V1.md` defines the onboarding/scale posture,
- onboarding manifest + active workset rendering exist,
- onboarding CLI has `validate`, `render-workset`, `admit`, `pause`, `report`, and `advance-check`,
- local rollout artifacts can be generated.

### F. Known live-feed / provider-selection blocker

The BTC-only live soak did **not** start because the bounded BloFin demo websocket confirm canary timed out waiting for a confirmed 5m candle close.

Current blocker shape:

- canary saw websocket messages,
- no confirmed close was observed within the bounded runtime,
- no background soak should be treated as active from that attempt,
- next work should not merely fix BloFin in isolation; it should run the feed-provider bakeoff and select a canonical top-100 Surveyor provider before starting soak automation.

---

## 3) Current non-complete areas

### A. Unified bundle/profile freeze thread is complete enough to close

Done:

- bundle shape exists,
- family envelopes exist,
- UI reads the bundle,
- profile-selected payload helper/export CLI exists,
- `arbiter_core` and `backtest_core` fail closed on missing required families,
- consumer acceptance records can be embedded in exported payloads.

Post-thread hardening follow-ups:

- broaden profile-selected payload delivery beyond the initial helper/export CLI contract when a real consumer asks for it,
- expand the initial live/degraded/replay fixture corpus into consumer-ready examples,
- keep hardening `arbiter_core` and `backtest_core` profile behavior as Arbiter/backtest consumers appear,
- any remaining legacy packet consumers should either migrate or be explicitly marked compatibility-only.

### B. Feed is not top-100-ready yet

Done:

- target contract exists,
- first onboarding CLI slice exists,
- manifest-driven active workset exists.

Remaining before top-100-ready:

- feed-provider bakeoff decision packet,
- confirmed BTC live close canary / soak,
- shard-aware websocket ingestion implementation,
- per-shard status,
- bounded cross-symbol recompute worker pool,
- provider/IP guardrails wired to real feed events,
- small/medium cohort evidence,
- pre-top-100 architecture checkpoint.

### C. Replay/backtest contract has an initial profile/acceptance freeze

Current state:

- bundle contract points in the right direction,
- `backtest_core` profile exists as a bundle profile,
- docs consistently say replay/backtest should consume the same family envelopes,
- initial fixture corpus exists for complete live-ish, partial/degraded, and replay-mode bundles,
- `surveyor_profile_acceptance_v1` can reject or accept profile payloads explicitly.

Remaining for the dedicated replay/backtest lane:

- replay bundle builder path,
- deterministic comparison harness,
- minimal backtest harness that consumes `backtest_core`,
- broader acceptance/rejection fixture matrix for partial/degraded/replay-only bundles.

### D. Arbiter architecture is still thin

Current state:

- `docs/ARBITER_DIRECTION.md` defines the intent and first boundary,
- `docs/ARBITER_ARCHITECTURE_V1.md` now exists as a proposed Arbiter / Sentinel / strategy-pack draft,
- `market_arbiter/arbiter/__init__.py` is only a package scaffold,
- no real Arbiter module architecture, data model, policy engine, fixtures, or tests exist yet,
- Sentinel is now recognized as the armed-watch layer that removes the need for Surveyor to maintain a continuous lower-timeframe trigger feed,
- the draft now captures the intended Sentinel posture: arm only near Arbiter-qualified zones of interest, monitor 15m-style confirmation/invalidation events, and disarm on trade/candidate resolution, timeout, revocation, or no-reaction blow-through,
- no implemented watch-intent/event contract, Sentinel module, or Arbiter handler exists yet,
- strategy packs are now part of the intended Arbiter design, with a one-active-pack constraint for each Arbiter run/scope.

This is fine, but we should distinguish architecture drafting from implementation. Arbiter / Sentinel design has started; Arbiter / Sentinel build has not.

---

## 4) Recommended next slice

The bundle/profile freeze thread is complete enough to close. The next best non-feed slice is now:

> **Start the replay/backtest contract + broader fixture matrix, while tightening the first Arbiter / Sentinel architecture specification against the frozen `arbiter_core` handoff.**

Reason: the UI slice is done and profile-selected bundle export is now stable enough for downstream consumers. Feed top-100 readiness still matters, but fixture-driven replay/backtest and Arbiter architecture do not need to wait for live top-100 feed evidence.

Minimum deliverable for the next non-feed slice:

1. add a replay bundle builder/input path that consumes the same family-envelope contract,
2. add deterministic comparison output for live-ish vs replay/degraded fixture cases,
3. add a minimal backtest harness that consumes `backtest_core`,
4. broaden fixture coverage for complete / partial / degraded / stale / replay-only / missing-required families,
5. tighten `ARBITER_ARCHITECTURE_V1.md` around `arbiter_core`, active strategy packs, Sentinel watch intents/events, zone-of-interest arming, 15m CHoCH confirmation/invalidation, explicit acceptance/rejection/caution records, replay/backtest behavior, and the execution non-goal,
6. keep live-feed assumptions out of Arbiter until the feed lane produces top-100 readiness evidence.

---

## 5) Roadmap from here through Arbiter design/build

### Phase 0 — control-plane reset in `#marketarbiter`

Goal: make `#marketarbiter` the visible macro-plan surface.

Deliverables:

- this status/roadmap doc linked from README,
- active workstream map:
  - unified bundle/profile freeze,
  - feed top-100 readiness,
  - replay/backtest contract,
  - Arbiter / Sentinel architecture/design,
  - strategy-pack contract design,
- child threads only for narrow execution lanes,
- all child threads roll status back up to `#marketarbiter`.

Exit gate:

- one current plan exists and the old UI-migration slice is marked closed.

### Phase 1 — unified bundle/profile freeze

Goal: make the Surveyor bundle the one stable descriptive handoff for UI, payload delivery, Arbiter, and replay/backtesting.

Deliverables:

- family-envelope contract tests,
- profile selection implementation,
- `arbiter_core` and `backtest_core` fixture payloads,
- payload exporter/serializer,
- compatibility-only treatment for old packet JSON.

Exit gate:

- a consumer can request a profile-selected bundle and get deterministic output with explicit required-family failures.

### Phase 2 — replay/backtest contract and fixtures

Goal: prove the same bundle shape works for live-ish, replay, and backtest contexts.

Deliverables:

- replay bundle fixture corpus,
- deterministic replay input contract,
- acceptance rules for degraded/partial/replay-only bundles — initial profile-payload gate landed in `market_arbiter/core/surveyor_profile_acceptance.py`,
- minimal backtest harness that consumes `backtest_core`,
- comparison output that can say why a candidate bundle was accepted, rejected, or marked invalid.

Exit gate:

- replay/backtest does not depend on a separate schema and can reject bad bundles explicitly. Initial acceptance records now use `surveyor_profile_acceptance_v1` with accepted / accepted-with-caution / rejected outcomes.

### Phase 3 — feed top-100-ready execution lane

Goal: finish Surveyor feed scale readiness without blocking Arbiter design forever.

Deliverables:

- feed-provider bakeoff decision packet,
- confirmed BTC live close canary and single-pair soak,
- shard-aware WS ingestion,
- per-shard health report,
- bounded recompute worker pool,
- provider/IP guardrail wiring,
- small and medium cohort evidence,
- pre-top-100 readiness packet.

Exit gate:

- `feed_provider_bakeoff_decision_v1` has selected a canonical provider posture, and `surveyor_feed_top100_readiness_packet_v1` is `pass` or explicit `conditional_pass` with remaining work limited to operational soak, not missing architecture/provider selection.

### Phase 4 — Arbiter / Sentinel architecture specification

Goal: design Arbiter as a selective interpretation layer and Sentinel as an armed-watch event layer, not an execution bot.

Current status: **in progress**. The first `ARBITER_ARCHITECTURE_V1.md` draft exists. The remaining Phase 4 work is to harden it into an implementation-ready contract, especially the Sentinel arm/disarm and 15m confirmation semantics.

Deliverables:

- `ARBITER_ARCHITECTURE_V1.md`, covering:
  - module boundaries,
  - input profiles,
  - strategy-pack registry and one-active-pack constraint,
  - Sentinel watch-intent shape,
  - Sentinel event vocabulary,
  - decision record shape,
  - caution/eligibility model,
  - rejection reasons,
  - provenance requirements,
  - replay/backtest behavior,
  - non-goals and execution boundary,
- first strategy family scope: SR / first-retest using `arbiter_core`,
- explicit control loop for Surveyor bundle -> Arbiter zone selection -> Sentinel watch -> Arbiter decision record,
- explicit statement that Surveyor does not keep a continuous lower-timeframe trigger feed; Sentinel owns narrow armed lower-timeframe watches only after Arbiter identifies a zone of interest,
- explicit disarm semantics for candidate/trade resolution, timeout, Arbiter revocation, invalidation, and no-reaction blow-through without 15m CHoCH,
- explicit design for how Arbiter reads:
  - `feed_state`,
  - `structure_state`,
  - `sr_zones`,
  - `fib_context`,
  - `dynamic_levels`,
- fixture matrix for complete / partial / degraded / replay-only inputs.

Exit gate:

- Arbiter and Sentinel can be implemented without making new Surveyor contract decisions.

### Phase 5 — Arbiter v1 build

Goal: implement the first narrow Arbiter slice.

Initial build surface:

- `market_arbiter/arbiter/profile.py` — requested families + required/optional handling,
- `market_arbiter/arbiter/eligibility.py` — bundle usability and caution classification,
- `market_arbiter/arbiter/decision_record.py` — stable output contract,
- `market_arbiter/arbiter/strategy_pack.py` — active-pack registry and one-pack guard,
- `market_arbiter/arbiter/watch_intent.py` — Sentinel arming contract,
- `market_arbiter/arbiter/sr_first_retest.py` — first narrow strategy pack,
- `market_arbiter/sentinel/` — watch-event generation from live or replay data once the contract is frozen,
- `market_arbiter/sentinel/choch.py` or equivalent — narrow 15m CHoCH / no-reaction blow-through detection for armed watches,
- `tests/test_arbiter_*.py` — fixture-driven validation.

Arbiter v1 output should answer only:

- is this bundle usable,
- under what caution level,
- for what decision mode,
- should Sentinel be armed for a specific symbol/zone,
- what Sentinel event changed the decision state,
- and why.

It should not execute trades, mutate Surveyor truth, or silently override provenance.

Exit gate:

- Arbiter consumes `arbiter_core`, activates exactly one strategy pack, produces deterministic watch intents / decision records, consumes Sentinel events, and fails closed on missing/degraded required inputs.

### Phase 6 — Arbiter review UI / operator inspection

Goal: make Arbiter decisions inspectable before any downstream automation exists.

Deliverables:

- UI section for Arbiter decision record,
- reason/rejection/caution display,
- link from Arbiter decision back to bundle family provenance,
- fixture/demo mode for review without live feed dependency.

Exit gate:

- a human can inspect why Arbiter accepted, rejected, or watch-listed a bundle.

---

## 6) Recommended immediate work order

1. Keep the UI-migration slice closed.
2. Keep the unified bundle/profile freeze thread closed; its landed output is now the stable handoff surface.
3. Continue feed top-100 readiness in the feed execution thread; do not treat live readiness as proven until the readiness packet says so.
   - First feed action: run the provider bakeoff and stop treating BloFin as presumed canonical.
4. Start the non-feed lane now:
   - replay/backtest contract,
   - broader fixture matrix,
   - first real `ARBITER_ARCHITECTURE_V1.md` draft covering Arbiter, Sentinel, and strategy packs.
5. Build Arbiter v1 narrowly from that architecture using fixtures first.
6. Add live-feed validation only after the feed lane produces confirmed readiness evidence.

This avoids the classic mistake: building a decision layer on top of clay that has not yet fired in the kiln, while also avoiding the opposite mistake of blocking all design work on feed soak mechanics.
