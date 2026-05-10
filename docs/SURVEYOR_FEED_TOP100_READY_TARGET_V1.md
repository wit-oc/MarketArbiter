# Surveyor Feed Top-100 Ready Target V1

Date: 2026-04-26  
Updated: 2026-05-05  
Status: target-state closeout contract for the intraday feed thread  
Intent: define what “ready to go for top 100” means, and reset the thread closeout boundary from single-pair operational closure to top-100-ready staged rollout readiness

---

## 1) Decision

The end target for this feed thread is **top-100-ready**, not merely **single-pair-operable**.

That means this thread should leave Surveyor with:
- a frozen feed vision,
- an operator-safe onboarding path,
- scale-safe ingestion/recompute mechanics,
- local evidence artifacts and automated check-ins,
- and a staged rollout process that can move from 1 pair toward the top 100 without redesigning the feed.

This does **not** mean jumping directly from 1 pair to 100 in one operational move. It means every sequential stage should be building toward that final top-100 operating shape.

Provider selection is now an explicit prerequisite. As of the Phase B reconnect canary decision packet, **Bybit is the conditional canonical live provider**, **OKX is the conditional shadow/fallback/repair/history source**, and **BloFin is non-canonical venue-context canary evidence**. The controlling provider-selection artifacts are `docs/FEED_PROVIDER_BAKEOFF_PLAN_V1.md`, `configs/surveyor_feed_provider_policy.v1.json`, and `artifacts/feed_bakeoff/feed-bakeoff-phase-b-reconnect-20260501-24h/feed_provider_bakeoff_decision_v1.json`.

Important Phase C constraint: Bybit REST repair/history is blocked from this host by a CloudFront/country restriction, so Bybit must not own repair/history unless access changes. Redact accepted the split-source posture on 2026-05-05: keep Bybit for live WS and proceed with OKX as the active REST history/repair path. The top-50/top-100 manifest still needs a wider depth audit and explicit handling for symbol/timeframe exceptions such as BNB/TON multi-year `1d`/`1w` depth.

---

## 2) Terminology

### Top-100-ready
The system is top-100-ready when it can safely begin the 50/75/100 staged rollout with the required control surfaces already implemented and proven at smaller scale.

Top-100-ready requires:
- manifest-driven symbol admission,
- shard-aware websocket ingestion,
- per-shard health visibility,
- bounded recompute concurrency across symbols,
- pause/disable/admit/report/advance-check operator controls,
- local check-in artifacts,
- provider/IP guardrails,
- and evidence from prior soak stages.

### Top-100-proven
The system is top-100-proven only after the 100-pair rollout has actually soaked and passed its review window.

This thread’s target is **top-100-ready**. Top-100-proven may be a later operational milestone unless the staged rollout reaches and soaks 100 pairs inside this thread.

---

## 3) Revised closeout boundary

The prior closeout boundary was too narrow for Redact’s target. It was appropriate for closing a single-pair operational slice, but not for wrapping the feed vision.

### This thread should not close until these are done

#### A. Control plane / onboarding
- onboarding manifest exists and is the rollout source of truth
- active workset is generated from onboarding state
- onboarding CLI exists with at least:
  - `validate`
  - `hydrate`
  - `ws-canary`
  - `runner-canary`
  - `admit`
  - `pause`
  - `report`
  - `advance-check`
- symbol state changes do not require Python edits

#### B. Provider/IP safety
- canonical provider is selected by the feed-provider bakeoff or explicitly marked conditional
- all provider REST traffic goes through one governor path per provider
- rollout cap stays below hard ceiling, currently `90 rpm` target / `120 rpm` ceiling
- `429`, `403`, and rapid-failure cooldowns are enforced in the rollout path
- provider-scope freeze prevents widening after firewall/rate-limit incidents
- ad hoc provider-touching scripts are explicitly out of bounds during rollout

#### C. WebSocket scale posture
- websocket ingestion is shard-aware, not a single unbounded symbol-list subscribe
- shard assignment is config/manifest-driven
- shard reconnects are bounded and observable
- shard failure can pause/degrade only the affected shard/cohort where possible

#### D. Health/status visibility
- operator report includes symbol rollup and provider-safety rollup
- per-shard status exists before moving past the medium cohort
- stale/degraded/tripped/resync-required states are visible from local state
- reports are written as durable local artifacts, not only console output

#### E. Recompute scale posture
- close events remain symbol-scoped and stacked-boundary aware
- one symbol is serialized internally
- multiple symbols are processed through a bounded worker pool
- weekly-boundary behavior is reviewed under bounded concurrency, not accidental fanout
- blocked manifests preserve reason and next action
- packet refresh is dataset/timeframe-scoped, so a candle close only refreshes the metric families whose inputs changed at that boundary
- full S/R-zone discovery is decoupled from ordinary `4H` recompute and has its own slower cadence, initially weekly for in-scope symbols

#### F. Automated check-ins
- 30-minute status pulse during active soak reads local state only
- daily `advance-check` prompts review when minimum soak has elapsed
- weekly heavy-boundary review compares weekly pressure to ordinary baseline
- automation never widens stage automatically
- alerts are reserved for `pause_required`, provider/IP risk, or blocked recovery/recompute

#### G. Evidence gates
- single-pair soak passes or receives an explicit conditional-pass decision
- small cohort soak passes before medium cohort
- medium cohort / 25-pair stage produces enough evidence for the architecture checkpoint
- pre-top-100 architecture checkpoint passes before 50/75/100 rollout begins

---

## 4) Sequential rollout path

### Stage 1 — single-pair soak
Target:
- `BTC-USDT`

Purpose:
- prove live WS close handling,
- startup recovery,
- checkpoint freshness,
- recompute ordering,
- report/advance-check ergonomics,
- and provider safety behavior.

Exit gate:
- minimum 72 hours unless Redact explicitly accepts a shorter conditional pass,
- at least one daily boundary observed,
- no unresolved provider/IP safety events,
- no open recovery/recompute blockers,
- report + advance-check artifacts exist.

### Stage 2 — small cohort soak
Target:
- 5 to 10 pairs

Purpose:
- prove ordinary fanout,
- symbol-specific onboarding,
- pause/admit flow,
- and one-shard or first-shard stability.

Exit gate:
- 72 to 120 hours,
- majority green with no systemic provider safety incidents,
- recompute backlog clears,
- operator report remains understandable.

### Stage 3 — medium cohort soak
Target:
- 25 pairs

Purpose:
- force realistic pressure before top-100 readiness,
- validate shard assignment,
- validate bounded recompute worker pool,
- measure DB/write/recompute pressure.

Exit gate:
- per-shard health exists,
- bounded recompute pressure is measured,
- weekly or heavy-boundary behavior is reviewed if timing permits,
- pause/disable is proven without code revert.

### Stage 4 — pre-top-100 architecture checkpoint
Target:
- architecture, not symbol count

Required pass criteria:
- shard-aware WS implementation exists,
- per-shard status/reporting exists,
- bounded recompute worker pool exists,
- onboarding CLI is the only approved rollout path,
- provider/IP guardrails are enforced by code/config,
- reports and advance-checks are durable artifacts.

### Stage 5 — top-100 rollout-ready staging
Target:
- 50 -> 75 -> 100 staged rollout plan is executable from manifests

Entry requirement:
- Stage 4 passes.

Exit condition for this thread:
- either the staged rollout begins with the control surfaces above in place,
- or the thread hands off a clear top-100 rollout packet with all remaining work limited to soak/execution rather than design/architecture.

---

## 5) Implementation order from here

The build order should now be:

0. **Feed-provider bakeoff** — provider selection slice conditionally closed
   - Phase B selected Bybit live / OKX shadow+repair+history / BloFin canary-only
   - Phase C bounded 10-symbol audit found Bybit REST blocked from this host and OKX depth exceptions to resolve before top-100-ready
   - produced `feed_provider_bakeoff_decision_v1`
   - update the final top-100 readiness packet with selected canonical live provider and any separate historical seed provider

1. **Manifest loader + active workset renderer** — landed initial slice
   - parse onboarding manifest
   - validate symbol states
   - render only `soaking|promoted` enabled symbols into runner workset

2. **Thin onboarding CLI skeleton** — landed initial slice
   - current commands: `validate`, `render-workset`, `admit`, `pause`, `report`, `advance-check`
   - remaining contract commands still to wire: `hydrate`, `ws-canary`, `runner-canary`

3. **Report + advance-check artifacts** — landed initial manifest-only slice
   - write JSON artifacts first
   - make chat/cron consume those artifacts, not scattered console text
   - remaining: wire report fields to real feed/recompute/provider safety events

4. **Single-pair live soak wiring** — blocked pending confirmed-close canary proof
   - current BTC demo websocket canary timed out waiting for a confirmed 5m close
   - run against the selected canonical provider after bakeoff, with BloFin retained as canary/venue-context evidence if it does not win
   - cron/check-ins should remain local-only once soak starts
   - pause-required alerts only

5. **Shard model implementation**
   - config-driven shard groups
   - one bounded WS consumer per shard or equivalent internal shard abstraction
   - per-shard status

6. **Bounded recompute worker pool**
   - cross-symbol concurrency only
   - strict per-symbol serialization
   - coalescing or queueing for close manifests

7. **Small and medium cohort soaks**
   - use the CLI/report surfaces for every promotion
   - no manual side-door edits

8. **Pre-top-100 readiness checkpoint**
   - produce one final readiness packet that says PASS / CONDITIONAL PASS / FAIL

---

## 6) Non-goals for this thread

These are still not required to call the feed vision wrapped:
- execution bot integration,
- multi-exchange abstraction,
- distributed queue infrastructure,
- cloud deployment,
- permanent UI for every CLI action,
- `1m` canonical storage,
- tick-level replay.

If any of those become required, that is a separate design expansion, not a hidden prerequisite for top-100-ready Surveyor feed closure.

---

## 7) Final closeout artifact

Before this thread closes, produce a final packet:

- `surveyor_feed_top100_readiness_packet_v1`

Minimum fields:

```json
{
  "contract": "surveyor_feed_top100_readiness_packet_v1",
  "as_of_ms": 1777219200000,
  "readiness": "pass|conditional_pass|fail",
  "current_stage": "single_pair_soak|small_cohort_soak|medium_cohort_soak|pre_top100_checkpoint|top100_rollout",
  "implemented": {
    "feed_provider_bakeoff": true,
    "onboarding_manifest": true,
    "active_workset_renderer": true,
    "onboarding_cli": true,
    "report_artifact": true,
    "advance_check_artifact": true,
    "ws_sharding": true,
    "per_shard_health": true,
    "bounded_recompute_worker_pool": true,
    "provider_ip_guardrails": true
  },
  "evidence": [
    "path/to/feed_provider_bakeoff_decision_v1.json",
    "path/to/report.json",
    "path/to/advance-check.json",
    "path/to/soak-summary.md"
  ],
  "remaining_before_top100_proven": [
    "execute 50-pair soak",
    "execute 75-pair soak",
    "execute 100-pair soak"
  ]
}
```

This packet is the explicit handoff between “feed vision/build closure” and “large-scale operational soak.”

---

## 8) Final closeout verdict update — 2026-05-05

Final packet: `artifacts/feed_bakeoff/feed-bakeoff-phase-b-reconnect-20260501-24h/surveyor_feed_top100_readiness_packet_v1.json` / `.md`

Verdict: **fail / not yet top-100-ready**.

The closure initiative completed the remaining evidence slices that could be safely run locally:
- top-50 public metadata + history-depth audit,
- artifact-local live-write canary for Bybit primary live WS and OKX shadow/fallback WS,
- shard-aware ingestion/per-shard health proof,
- bounded cross-symbol recompute worker-pool proof,
- and the final packet with explicit limits.

The status is still **not** top-100-ready because the hard gates are not true yet:
- Bybit REST instruments/history remains blocked from this host by HTTP 403 CloudFront/country restriction; Bybit is live-WS-only here until access changes or a separate approved egress path is tested.
- OKX is the conditional repair/history source, but the top-50 conservative audit found only 16/50 symbols with all probed timeframes ok and 34/50 with at least one exception; top-100 history coverage is not proven.
- canonical Surveyor production writes were intentionally not enabled; live-write evidence is artifact-local only.
- provider/IP guardrails are not fully proven in the canonical rollout path.
- advance-check remains hold until 72h soak evidence is wired/proven.

Operationally: keep widening held, but the next critical path is now the OKX split-source path. The first staged admission packet is `artifacts/feed_bakeoff/feed-bakeoff-phase-b-reconnect-20260501-24h/okx_staged_admission_manifest_20260505.json` with exception policy `okx_history_exception_policy_20260505.json`: 16 full-history/repair-eligible symbols, 8 conditional hot-only symbols, 23 deferred for missing structural history, and 3 deferred for no OKX history probe. Treat Bybit REST egress testing as optional separate work, not a blocker for the next readiness slice.

## 8.1) Final readiness refresh — 2026-05-06

Refreshed packet: `artifacts/feed_bakeoff/feed-bakeoff-phase-b-reconnect-20260501-24h/surveyor_feed_top100_readiness_packet_v1.json` / `.md`  
Refresh artifact: `artifacts/feed_bakeoff/feed-bakeoff-phase-b-reconnect-20260501-24h/top100_readiness_final_refresh_20260505/surveyor_feed_top100_readiness_packet_v1.final_refresh_20260506.json` / `.md`

Verdicts after the top-100 readiness runner:

| Gate | Verdict |
|---|---:|
| Provider/IP guardrails | PASS |
| Split-source write routing | PASS |
| 72h soak evidence / advance-check semantics | PASS |
| 10-symbol OKX full-history start packet | CONDITIONAL PASS |
| Top-100-ready | FAIL |

The next gate is conditionally ready to start: a real guarded 72h small-cohort soak for `BTC-USDT`, `ETH-USDT`, `OKB-USDT`, `SOL-USDT`, `DOGE-USDT`, `XRP-USDT`, `BCH-USDT`, `1INCH-USDT`, `AAVE-USDT`, and `ADA-USDT`.

Conditions:
- Redact must explicitly enable guarded live execution/canonical writes before a real soak starts.
- Bybit remains live WS only from this host; do not use or force Bybit REST repair/history.
- OKX remains the active REST history/repair path and WS shadow/fallback.
- Advance-check remains manual-review-only with `auto_widening_enabled=false`.
- No 72h final guard was scheduled by the runner because no real soak was started.

Top-100 readiness remains **fail / not yet top-100-ready** because real small/medium/top-100 soaks have not run, the actual top-100 admission manifest is not fully proven, and canonical production writes remain disabled.

## 9) Bottom line

The north star remains:

> Surveyor feed closes when the system is ready to stage into the top 100 from manifests, with shard-aware ingestion, bounded recompute, provider-safe automation, and durable evidence artifacts.

As of the 2026-05-06 final refresh, the next small-cohort gate is conditionally start-ready after operator enablement, but the feed must remain held before top-100 rollout.
