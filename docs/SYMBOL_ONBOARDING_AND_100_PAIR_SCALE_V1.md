# Symbol Onboarding and 100-Pair Scaling Plan V1

Date: 2026-04-24  
Status: proposed onboarding + scale design  
Intent: define how new symbols should be admitted into the live feed without code edits, and what must change before the system should be trusted for ~100 concurrent pairs

Related closeout target:
- `docs/SURVEYOR_FEED_TOP100_READY_TARGET_V1.md` defines the revised thread end-state: top-100-ready staged rollout readiness, not merely single-pair operation.

---

## 1) Executive answer

### Should new pairs require code changes?
No.

New symbols should be admitted through **config / manifest changes**, not Python edits.
The code should own:
- validation
- hydration / repair policy
- canary checks
- shard assignment
- concurrency limits
- failure isolation

The symbol list itself should live in an operator-managed manifest.

### Do we need to run single-pair testing for a while?
Yes, but **not indefinitely**.

Recommended posture:
- prove correctness on **1 pair** first
- then prove connection/recovery behavior on a **small cohort**
- then scale in explicit stages

This is mainly a soak / operational-confidence question, not a code-architecture question.

---

## 2) Current repo reality

As of 2026-04-24, the repo already moved in the right direction, but it is not yet a finished top-100 feed platform.

### What already exists
- `market_arbiter/ops/surveyor_feed_runner.py`
  - supports explicit `symbols` + `timeframes`
  - supports `manifest_path`
  - supports `once`, `loop`, `status`, and `canary`
- `configs/surveyor_feed_workset.default.json`
  - checked-in workset manifest with explicit symbol/timeframe config
- `market_arbiter/ops/blofin_ws_candle5m_consumer.py`
  - accepts an explicit symbol list from config/CLI
  - performs startup recovery before subscribing
  - supports a bounded confirm canary
- recompute orchestration now exists
  - symbol-scoped manifest execution
  - strict `5m -> 4h -> 1d -> 1w` ordering
  - derived aggregate materialization
  - real snapshot recompute checks

### Important current limits
The repo is still missing some scale-safe boundaries.

#### A. Feed runner loop is still sequential
`run_loop(...)` currently builds one flat key list and runs `_run_cycle_set(...)` across it in order.
That is acceptable for the current closure work, but it is not a finished scale plan for 100 pairs.

#### B. WebSocket consumer currently subscribes the full symbol list on one connection
The BloFin WS consumer currently does:
- startup recovery for `config.symbols`
- one `subscribe_candle_5m(config.symbols)` call

That is fine for a small cohort, but should not be treated as the final top-100 scaling posture without empirical validation.

#### C. Recompute lock is in-process only
`SymbolRecomputeLockManager` is currently an in-memory lock.
That is fine for one process, but it is not a cross-process/distributed coordination mechanism.

#### D. Onboarding is still implicit
Today, adding a symbol is basically:
- edit config
- restart
- manually ensure history and canary are good

That works for now, but it is not yet a formal operator workflow.

---

## 3) Design goals

The onboarding design should achieve all of these:

1. **No code edit per symbol change**
2. **Deterministic admission flow** for new symbols
3. **Safe rollback** if a symbol behaves badly
4. **Bounded concurrency** as symbol count grows
5. **Shard-ready architecture** before we trust ~100 pairs
6. **Explicit canary evidence** before promotion
7. **Clear operator source of truth** for what is active now

---

## 4) Recommended source of truth

Use a checked-in manifest as the canonical live workset.

### Recommended model
- one top-level manifest for the active deployment
- optional cohort/shard manifests derived from it later
- symbol admission happens by editing manifest content, not code

### Recommended manifest shape

```json
{
  "contract": "surveyor_feed_workset_manifest_v2",
  "db_path": "data/market_arbiter.sqlite",
  "provider": "blofin_ws_plus_rest",
  "timeframes": ["5m", "4h", "1d", "1w"],
  "loop_sleep_ms": 15000,
  "close_lag_ms": 2500,
  "symbols": [
    {
      "symbol": "BTC-USDT",
      "enabled": true,
      "cohort": "core",
      "priority": 1,
      "canary_state": "promoted",
      "shard_hint": "ws-a"
    },
    {
      "symbol": "ETH-USDT",
      "enabled": true,
      "cohort": "core",
      "priority": 1,
      "canary_state": "promoted",
      "shard_hint": "ws-a"
    }
  ]
}
```

### Why this is better than a plain symbol list
It gives us room for:
- staged rollout
- canary/promotion state
- shard assignment
- future per-symbol throttling or disable flags

We do **not** need all of this implemented immediately, but the contract should leave room for it.

---

## 5) Recommended onboarding workflow

A new pair should move through explicit states.

### State model
- `proposed`
- `hydrating`
- `canary`
- `promoted`
- `paused`
- `rejected`

### Admission workflow

#### Step 1 — add symbol to manifest as `proposed`
Operator adds the new pair to the manifest, but it is not yet live-routed into the full production workset.

#### Step 2 — validate symbol + venue support
Checks should verify:
- exchange symbol format is valid
- REST history path exists
- WS subscription path exists
- history depth is sufficient for required `5m -> 4h -> 1d -> 1w` derivation needs

#### Step 3 — hydrate bounded history
Before live promotion, hydrate the bounded required history window:
- canonical `5m`
- enough history to derive/seed `4h`, `1d`, `1w`
- any required weekly context per the existing history contract

#### Step 4 — run bounded canary for the symbol
Canary should prove:
- ingest works
- checkpoint advances
- close manifest emits
- derived aggregates materialize
- recompute completes
- status surface remains healthy

#### Step 5 — short live soak in a small cohort
Do not immediately mix a new symbol into the full 100-pair fleet.
Put it in a bounded cohort first.

#### Step 6 — promote to active live workset
Only after the symbol is green for the defined soak window.

#### Step 7 — keep rollback simple
If the symbol degrades:
- mark `paused`
- remove from active shard subscription
- keep its data for diagnosis
- do not require a code revert

---

## 6) What should change in code for proper onboarding

### Required near-term changes

#### A. Formal symbol admission tooling
Add a small operator-facing path that can:
- read a manifest
- validate a proposed symbol
- run hydration
- run canary
- emit a promotion-ready result

This can be a CLI first.
It does not need a UI before it is useful.

#### B. Manifest contract upgrade
Move from a plain `symbols: ["BTCUSDT", ...]` list toward symbol objects with admission metadata.

#### C. Promotion gating
Promotion should require:
- hydration success
- canary success
- no unresolved recovery blocker

#### D. Explicit pause/disable semantics
Operator should be able to pause a symbol without editing code or inventing ad hoc ignore lists.

---

## 7) What must change before trusting ~100 pairs concurrently

This is the core scale section.

### 7.1 Feed ingestion architecture

#### Current posture
- one symbol list
- one connection path
- one startup recovery sweep
- one subscription call for the configured set

That is okay for a small cohort.
It should not be assumed sufficient for 100 without further hardening.

#### Required changes

##### A. WebSocket sharding
Move from “one giant symbol list on one connection” to a shard model.

Recommended first pass:
- split symbols across **multiple WS shards**
- each shard owns a bounded symbol cohort
- shard count should be configurable

Why:
- lowers blast radius of one socket failure
- makes reconnect storms less catastrophic
- allows incremental scale testing
- makes it easier to rebalance hot symbols later

##### B. Per-shard state and health
Each shard should expose:
- shard id
- configured symbols
- connected state
- reconnect count
- message rate
- last confirmed candle time
- symbols currently degraded / repair pending

##### C. Staged recovery, not global panic
A shard failure should ideally:
- mark only the affected shard/symbols `repair_pending`
- recover that shard
- avoid stopping the entire universe if one socket path misbehaves

---

### 7.2 Recompute architecture

#### Current posture
- symbol-level sequencing exists
- symbol lock is in-process
- manifest execution is deterministic
- concurrency across symbols is not yet a full worker-queue system

#### Required changes

##### A. Bounded worker pool across symbols
The docs already point the right direction here.
For ~100 pairs, make it explicit in code.

Recommended first pass:
- worker pool: **4 to 8** symbols in parallel
- strict per-symbol serialization
- weekly boundary work may still be forced to a lower concurrency tier

##### B. Queue / coalescing behavior
If multiple close events pile up for the same symbol:
- queue or coalesce manifests
- do not let them explode into uncontrolled parallelism

##### C. Stronger lock semantics if multi-process is introduced
If the system stays single-process, the current in-memory lock may be enough for a while.
If we split ingestion/recompute across processes later, we will need a durable coordination surface.

---

### 7.3 Storage and database posture

SQLite is still plausible here, but only with discipline.

#### Near-term requirements
- keep WAL enabled
- keep indexes disciplined
- separate noisy raw staging retention from canonical storage
- keep derived aggregates persisted instead of forcing deep repeated recomputation

#### Watch items for 100 pairs
- write latency during recovery bursts
- lock contention if many shards/processes appear
- DB growth from staging + health-event churn
- weekly boundary spikes

This does **not** automatically mean “move off SQLite now.”
It means: measure it honestly during staged rollout.

---

### 7.4 Operator controls

Before trusting 100 pairs, operators should have these controls:
- enable/disable symbol
- assign symbol to shard/cohort
- run symbol canary
- rehydrate one symbol
- pause one symbol without stopping the universe
- inspect shard health
- inspect symbol health

Without these, a top-100 rollout becomes painful even if the code mostly works.

---

## 8) Recommended rollout stages

Do **not** jump from 1 pair straight to 100.

### Stage 0 — deterministic repo canary
Already achieved.
- seeded candles
- end-to-end manifest/recompute proof

### Stage 1 — live single-pair soak
Target: `BTC-USDT` only

Purpose:
- validate live WS + repair + checkpoint + recompute behavior
- validate operator ergonomics
- establish baseline reconnect/noise patterns

Recommended soak window:
- **3 to 7 days**
- must include at least one daily boundary
- ideally includes one weekly boundary if timing allows

### Stage 2 — small cohort soak
Target: **5 to 10 pairs**

Purpose:
- validate one-shard behavior under modest fanout
- observe recovery posture on a nontrivial cohort
- catch symbol-specific history/format weirdness

Recommended soak window:
- **3 to 5 days**

### Stage 3 — medium cohort
Target: **25 pairs**

Purpose:
- confirm shard policy
- observe DB and recompute pressure
- measure boundary spikes

Recommended soak window:
- **3 to 5 days**

### Stage 4 — pre-top-100 architecture checkpoint
Before going to ~100, require:
- shard model in place
- per-shard health visible
- bounded recompute worker pool in place
- pause/disable flow in place
- admission/canary flow documented and repeatable

### Stage 5 — top-100 phased rollout
Target:
- 50 pairs
- then 75
- then 100

Do not promote in one jump unless earlier stages show unusually clean behavior.

---

## 9) Answer to “do we need single-pair testing for a while?”

### Short answer
Yes.

### Longer answer
We need **single-pair live soak first**, but not because the final system should stay single-pair.
We need it because it gives us clean evidence about:
- reconnect behavior
- repair latency
- canary truthfulness
- checkpoint freshness
- recompute timing
- weekly boundary handling

If we skip that and jump straight to a large cohort, we make it much harder to tell whether a failure is:
- symbol-specific,
- shard-specific,
- exchange-rate-limit related,
- or a core logic defect.

### Recommended minimum
- start with **1 pair**
- soak until behavior is boring
- then widen deliberately

“Boring” here means:
- expected close cadence
- no unexplained checkpoint stalls
- no recurring repair loops
- no recompute backlog surprise
- no operator confusion about status surfaces

---

## 10) What does *not* need to happen yet

We do **not** need to solve all of this before the next useful step.

Not required immediately:
- fully dynamic hot-reload of symbol lists without restart
- distributed worker orchestration
- cloud-native queueing
- production-grade UI for every operator action
- permanent multi-exchange abstraction

Near-term, a very reasonable posture is:
- config/manifest change
- restart
- hydrate
- canary
- promote

That is enough to move forward safely.

---

## 11) Recommended next implementation steps

### Immediate next doc/code sequence
1. freeze this onboarding contract
2. add a symbol-admission manifest shape
3. add a small onboarding CLI flow
   - validate symbol
   - hydrate symbol
   - run canary
   - emit promotion-ready result
4. add shard concept to the WS consumer config
5. add per-shard status output
6. add bounded recompute worker pool across symbols

### Recommendation boundary
Do **not** claim “top-100 ready” until at least:
- single-pair live soak is complete
- small cohort soak is complete
- shard plan exists in code, not just in docs

---

## 12) Bottom line

The right operating model is:
- **config-driven symbol onboarding**
- **code-enforced validation / hydration / canary / concurrency**
- **phased rollout from 1 -> small cohort -> medium cohort -> top 100**

So yes, we should run single-pair live testing for a while first.
But that is a **staged proving ground**, not the permanent product shape.

The end-of-thread target is now stronger than the immediate single-pair closure line: the feed vision should be considered wrapped only when the system is **top-100-ready** per `SURVEYOR_FEED_TOP100_READY_TARGET_V1.md`, with shard-aware ingestion, bounded recompute, provider-safe automation, and durable readiness artifacts in place.
