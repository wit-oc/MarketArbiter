# Surveyor -> Arbiter Control-Plane Handoff V1

Date: 2026-04-24  
Status: active sequencing + channel-ownership handoff plan; updated by `MARKETARBITER_STATUS_AND_ROADMAP_2026-04-27.md`  
Intent: finish remaining Surveyor feed/bundle closure work, then use `#marketarbiter` as the active control plane for replay/backtest and Arbiter design/build

---

## 1) Why this doc exists

The project now needs a cleaner operating model.

The feed/recovery workstream is still active and should stay concentrated until it is truly done.
But the broader project needs a stable control surface that can:
- own macro sequencing,
- track Surveyor completion criteria,
- define the Arbiter starting boundary,
- and later spawn parallel child threads without losing the main plan.

This document makes that split explicit.

---

## 2) Channel / thread operating model

### A. Current feed thread
Use the current feed thread as the **active execution thread for remaining feed work only**.

Its job is to finish:
- canonical BloFin feed path hardening,
- continuity proof,
- historical hydration / repair posture,
- recompute scheduling closure,
- and any feed-truth issues required before Surveyor can be called operationally complete.

When that feed work is done, this thread should be treated as **closed out**.
It should not quietly mutate into the long-term project control surface.

### B. `#marketarbiter` (`1469501772367138887`)
Use `#marketarbiter` as the **active control-plane channel** once the feed thread is closed.

Its job is to hold:
- current macro plan,
- Surveyor completion status,
- Arbiter start criteria,
- active workstream map,
- dependency/order decisions,
- and links to child execution threads.

### C. Child threads under `#marketarbiter`
Once `#marketarbiter` is active as control plane:
- create child threads for distinct implementation lanes,
- keep each child thread narrow and execution-focused,
- route decisions, sequencing changes, and cross-lane coordination back to `#marketarbiter`.

That keeps execution parallel without letting ownership drift.

---

## 3) What still belongs to Surveyor

Surveyor is not finished just because packet assembly exists.
Surveyor should be considered complete only when the following are true:

### Feed / continuity boundary
- canonical feed ingestion path is frozen
- continuity state is explicit and trustworthy
- startup/reconnect recovery is deterministic
- historical hydration windows are frozen by timeframe
- repair metadata / quality bands / circuit-breaker posture are exposed in feed truth
- recompute scheduling is symbol-scoped, ordered, and rebuilds from authoritative candles when trust is broken

### Bundle / packet boundary
- the unified Surveyor dataset bundle is the stable descriptive handoff shape
- provenance is explicit at packet, family, and timeframe level
- partial/degraded/replay-only truth is surfaced honestly
- UI and downstream consumers read the same descriptive bundle shape

### Operator boundary
- operator UI can inspect canonical feed state, continuity, repair posture, and packet/bundle output
- the repo can truthfully distinguish one-shot refresh, live continuous, replay-only, and mixed states

### Validation boundary
- targeted tests cover feed ingest, recovery, continuity, bundle assembly, and packet diagnostics
- replay/backtesting has not yet replaced Arbiter, but Surveyor outputs are reliable enough to serve as the only descriptive source of truth

---

## 4) Remaining Surveyor sequence

This is the recommended order from here.

### Phase S1 — finish Surveyor closure work
Close the remaining Surveyor lane in this order:
1. freeze profile-selected payload delivery for the unified bundle (`ui_full`, `arbiter_core`, `backtest_core`)
2. prove or fix the BloFin confirmed-close canary before starting BTC-only background soak
3. finish continuity / recompute / hydration / recovery closure items required by the top-100-ready feed target
4. confirm the Surveyor bundle is reading finalized feed truth honestly
5. produce explicit evidence for what is green, conditional, blocked, or intentionally deferred

Note: the UI-migration slice is complete. Do not keep scheduling work under a closed UI slice; remaining bundle work belongs to the unified bundle/profile freeze slice.

### Phase S2 — declare Surveyor complete in `#marketarbiter`
Once S1 is done, `#marketarbiter` becomes the primary coordination surface and should record:
- what Surveyor now guarantees,
- what remains intentionally out of scope,
- and what Arbiter is allowed to assume about Surveyor outputs.

### Phase S3 — start Arbiter with a narrow first boundary
Arbiter should begin as a **selective interpretation layer**, not as an execution bot and not as a giant strategy kitchen sink.

The first Arbiter slice should do three things only:
1. consume the repair-aware Surveyor bundle
2. declare packet usability / eligibility / caution state
3. produce a minimal handoff contract for candidate / reject / watch-only style downstream use

That is the right start point because it preserves the Surveyor -> Arbiter boundary instead of blurring it immediately.

---

## 5) Arbiter start contract

Arbiter v1 should explicitly read at least:
- `feed_state`
- `structure_state`
- `sr_zones`
- `fib_context`
- `dynamic_levels`

And it should explicitly reason about:
- continuity state
- freshness state
- repair quality bands
- timeframe-level degraded/blocked conditions
- mixed-source / replay-only conditions when present

Arbiter v1 should **not** yet own:
- execution
- broker/exchange actions
- strategy sprawl across many families
- silent overrides of Surveyor provenance or quality posture

Its first task is to say:
- is this bundle usable,
- under what caution level,
- and for which decision mode.

---

## 6) Closure criteria for the current feed thread

Before the current feed thread is considered closed, it should leave behind:
- final feed-state contract references
- final continuity/recompute/recovery contract references
- explicit note of what was proven by tests vs not yet proven
- explicit next-owner statement: `#marketarbiter` is now the control plane

Do **not** close the feed thread with a vague “continue later in marketarbiter” line.
Close it with an explicit finished-feed / remaining-project split.

---

## 7) Control-plane contents required in `#marketarbiter`

`#marketarbiter` should have, in one visible control post:
- the current project boundary
- the current sequencing order
- the Surveyor completion checklist
- the Arbiter start boundary
- the current active thread map
- the artifact/doc index
- and the explicit rule that child execution threads roll status back up there

That makes the channel usable as real control plane instead of just another chat lane.

---

## 8) Artifact / doc index for the control plane

These are the core docs the control plane should point to:
- `README.md`
- `docs/MARKETARBITER_STATUS_AND_ROADMAP_2026-04-27.md`
- `docs/INTRADAY_REVISIT_SURVEYOR_ARBITER_ARCHITECTURE_V1.md`
- `docs/SURVEYOR_UNIFIED_DATASET_CONTRACT_V1.md`
- `docs/SURVEYOR_CONTINUOUS_FEED_PLAN_V1.md`
- `docs/BLOFIN_MARKET_DATA_CONTRACT_V1.md`
- `docs/BLOFIN_GAP_RECOVERY_WORKFLOW_V1.md`
- `docs/BLOFIN_HISTORY_REQUIREMENTS_V1.md`
- `docs/SURVEYOR_RECOMPUTE_SCHEDULE_V1.md`
- `docs/ARBITER_DIRECTION.md`
- `docs/SURVEYOR_TO_ARBITER_CONTROL_PLANE_HANDOFF_V1.md`

---

## 9) Recommended next action after this doc lands

1. keep finishing feed work in the current thread
2. post the control-plane bootstrap summary into `#marketarbiter`
3. once feed closure is real, mark `#marketarbiter` as the active owner thread for macro coordination
4. create child threads under `#marketarbiter` for parallel lanes only after the control post exists

That is the cleanest way to finish Surveyor without losing the Arbiter start boundary.
