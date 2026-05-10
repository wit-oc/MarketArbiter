# Arbiter Direction

Status: directional boundary only; real Arbiter / Sentinel architecture design has not started beyond this scope note.

Arbiter is the selective decision layer above Surveyor and the control layer that arms Sentinel watches.

Surveyor should eventually publish multiple analysis families, for example:
- structure and SR
- supply / demand zones
- delta volume
- fib context
- dynamic levels

Arbiter should be able to cherry-pick the datasets relevant to a strategy, regime, or research program instead of assuming one fixed primary surface.

Sentinel is the focused watch layer below Arbiter.
It should monitor only armed symbol/zone contexts and emit events back to Arbiter.
Sentinel should not decide trades or create its own strategy context.

## Initial boundary

Arbiter should start narrow.

Its first job is not execution.
Its first job is to consume the repair-aware Surveyor bundle and answer:
- is this packet usable,
- under what caution level,
- and for which downstream decision mode.

That means Arbiter v1 should explicitly read:
- `feed_state`
- `structure_state`
- `sr_zones`
- `fib_context`
- `dynamic_levels`

And it should explicitly reason about:
- continuity state
- freshness state
- repair quality bands
- degraded / blocked timeframe conditions
- replay-only or mixed-source conditions when present

Arbiter v1 should not yet own:
- execution
- exchange/broker actions
- silent overrides of Surveyor provenance
- broad strategy sprawl across unrelated dataset families
- multiple simultaneously armed strategy packs in the same Arbiter run/scope

## Strategy pack boundary

Arbiter should support strategy packs rather than hard-coding every strategy into one permanent policy module.

A strategy pack should define:
- required and optional Surveyor families / profiles,
- pair-universe filters,
- setup qualification rules,
- zone-of-interest selection logic,
- Sentinel arming parameters,
- Sentinel event handlers,
- decision-record output extensions,
- replay/backtest adapters,
- fixtures and acceptance tests.

Only one strategy pack should be active for a given Arbiter run/scope. This keeps strategy evidence auditable and prevents hidden policy mixing while each strategy is being proven independently.

The first expected pack is SR / first-retest focused. Arbiter scans every in-scope pair's Surveyor bundle for qualified first-retest zones, emits a Sentinel `watch_intent` when a zone deserves live or replay monitoring, and then interprets Sentinel feedback through the active pack.

## Sentinel boundary

Sentinel is an event detector, not a decision engine.

It should consume:
- Arbiter `watch_intent` records,
- the relevant live or replay candle/ticker stream,
- watch thresholds and invalidation rules from the active strategy pack,
- and any required Surveyor context snapshot identifiers.

It should emit deterministic events such as:
- `approached_zone`,
- `entered_zone`,
- `first_retest_touch`,
- `confirmation_window_open`,
- `invalidation_breach`,
- `timeout`,
- `disarm`.

Arbiter consumes those events and remains responsible for reject / keep-watching / promote-candidate / disarm decisions.

## Current implementation status

Current state:
- `market_arbiter/arbiter/` is only a package scaffold.
- No Arbiter policy engine, data model, decision-record contract, fixtures, or tests exist yet.
- The unified Surveyor bundle now exposes an `arbiter_core` profile and profile-selected payload delivery exists, but Arbiter fixtures and real decision logic still need to be frozen before implementation.
- No Sentinel watch-intent contract, event vocabulary, or strategy-pack registry exists yet.

## Architecture work required before build

Create `docs/ARBITER_ARCHITECTURE_V1.md` before adding real Arbiter or Sentinel logic.

That architecture doc should define:
- module boundaries,
- accepted input profile(s), starting with `arbiter_core`,
- required vs optional families,
- strategy-pack registry and one-active-pack constraint,
- Sentinel watch-intent and event contracts,
- decision record shape,
- caution / eligibility / rejection model,
- replay/backtest behavior,
- provenance and quality-band requirements,
- operator inspection requirements,
- and explicit non-goals around execution.

Likely first build modules after that spec:
- `market_arbiter/arbiter/profile.py` for requested-family handling,
- `market_arbiter/arbiter/eligibility.py` for usability/caution classification,
- `market_arbiter/arbiter/decision_record.py` for stable output shape,
- `market_arbiter/arbiter/strategy_pack.py` for active-pack loading and validation,
- `market_arbiter/arbiter/watch_intent.py` for Sentinel arming records,
- `market_arbiter/arbiter/sr_first_retest.py` for the first narrow strategy pack,
- `market_arbiter/sentinel/` for watch-event generation once the contract is frozen.

The sequencing / ownership handoff for getting from Surveyor completion to Arbiter start now lives in:
- `docs/SURVEYOR_TO_ARBITER_CONTROL_PLANE_HANDOFF_V1.md`

The current status and roadmap through Arbiter design/build now lives in:
- `docs/MARKETARBITER_STATUS_AND_ROADMAP_2026-04-27.md`
