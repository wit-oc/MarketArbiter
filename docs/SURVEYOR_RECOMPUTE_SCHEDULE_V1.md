# Surveyor Recompute Schedule V1

Date: 2026-04-21  
Status: proposed scheduling and execution contract  
Intent: freeze how Surveyor recompute work should be triggered, ordered, and retried as the BloFin-first feed path comes online

---

## 1) Why this exists

Surveyor is not just a candle-ingest service.
It is a descriptive market-state system that needs deterministic recomputes when closed candles arrive.

The heavy point is not ordinary `5m` ingestion.
The heavy point is the **stacked close boundary**, especially at weekly close, where a single new `5m` close can also mean:
- a `4H` close,
- a `1D` close,
- and a `1W` close

for the same symbol at the same moment.

If those are treated as separate competing jobs, the system risks:
- race conditions,
- partial updates,
- contradictory provenance,
- and hard-to-debug dataset drift.

So the scheduling contract must treat these as **one ordered symbol-level event**, not four unrelated watches.

---

## 2) Core scheduling rules

1. **One symbol-level close event is the unit of work**
   - not one timeframe event by itself

2. **One active recompute per symbol at a time**
   - symbol-scoped lock
   - no concurrent `4H`, `1D`, and `1W` recomputes for the same symbol

3. **Higher-timeframe recomputes are triggered only by confirmed closed upstream bars**
   - never by in-progress candles

4. **Stacked close boundaries must be processed in strict order**
   - finalize `5m`
   - then `4H`
   - then `1D`
   - then `1W`

5. **Worker concurrency is allowed across symbols, not within the same symbol**
   - a bounded pool may process multiple symbols
   - but one symbol must remain strictly serialized

6. **Recompute should be incremental and close-triggered**
   - do not full-recompute the universe every 5 minutes

---

## 3) Event model

### A. Base event
The base event is a **confirmed closed `5m` candle** for one symbol.

That event should first be persisted canonically, then evaluated to determine which larger timeframe boundaries were crossed.

### B. Derived boundary detection
After a new confirmed `5m` close is accepted, the scheduler should determine whether that timestamp also closes:
- a `4H` bar
- a `1D` bar
- a `1W` bar

### C. Stacked close manifest
The scheduler should build one manifest per symbol.

Example:

```json
{
  "contract": "surveyor_close_manifest_v1",
  "symbol": "BTC-USDT",
  "venue": "blofin",
  "trigger_timeframe": "5m",
  "closed_timeframes": ["5m", "4H", "1D", "1W"],
  "close_ts_ms": 1776902400000,
  "trace_id": "..."
}
```

This manifest is the authoritative work order for recompute.

---

## 4) Ordered execution contract

For a single-symbol manifest, processing order should be:

1. **persist canonical `5m` close**
2. **materialize derived `4H` close if boundary crossed**
3. **recompute `4H` Surveyor datasets**
4. **materialize derived `1D` close if boundary crossed**
5. **recompute `1D` Surveyor datasets**
6. **materialize derived `1W` close if boundary crossed**
7. **recompute `1W` Surveyor datasets**
8. **publish one final symbol update status**

This ordering matters because higher-timeframe datasets should only run after the newly closed aggregate bar is finalized and trusted.

---

## 5) Symbol-scoped lock contract

Each symbol must have a recompute lock.

### Lock rules
- only one manifest for a symbol may be active at a time
- if another close event arrives for the same symbol while a recompute is active:
  - enqueue it behind the active manifest
  - or coalesce it if the newer manifest supersedes the earlier one safely

### Why
This prevents:
- `4H` recompute reading stale `1D` state
- `1W` recompute racing with `1D`
- partial provenance updates

### Minimal lock shape

```json
{
  "symbol": "BTC-USDT",
  "lock_state": "held",
  "active_manifest_id": "...",
  "held_since_ts_ms": 1776902401000
}
```

---

## 6) Worker-pool model

### Recommended first pass
- bounded worker pool across symbols
- start small, for example **4 to 8 workers**
- strict serialization per symbol

### Allowed concurrency
- BTC and ETH may recompute at the same time
- BTC `4H` and BTC `1D` may **not** recompute independently at the same time

### Operational implication
Weekly close will still be heavier than an ordinary `5m` close, but it becomes:
- a queue of symbol-level manifests
- processed by a bounded worker pool
- with deterministic ordering inside each symbol

That is manageable.

---

## 7) Dataset recompute scope

The recompute scheduler should be selective.

### Packet refresh principle
Packet refresh must be **timeframe-boundary scoped**, not full-packet-by-habit.

When a candle closes, recompute only the dataset families whose inputs actually changed at that boundary.
For example:
- ordinary `5m` close: update feed continuity / freshness and any explicitly `5m`-scoped packet surfaces
- `4H` close: materialize `4H`, then refresh `4H`-dependent structure / fib / dynamic context
- `1D` close: materialize `1D`, then refresh `1D`-dependent structure / fib / higher context
- `1W` close: materialize `1W`, then refresh weekly / macro context

The current implementation may still rebuild a broader packet snapshot as a convenience path, but the target state is narrower: **dataset-specific refresh only on the candle close for that dataset's timeframe.**

### On ordinary `5m` close
Default behavior:
- persist `5m` close
- maybe update feed continuity / freshness state
- no heavy Surveyor family recompute unless a larger timeframe boundary is crossed

### On `4H` close
Run `4H`-dependent dataset recomputes for that symbol, for example:
- market structure
- dynamic levels
- fib context
- any derived `4H` summary surfaces

### On `1D` close
Run `1D`-dependent dataset recomputes for that symbol.

### On `1W` close
Run `1W`-dependent dataset recomputes for that symbol.

### Important constraint
Do **not** brute-force recompute all timeframes and all families on every `5m` close.
That is wasted compute and creates unnecessary risk.

### SR-zone cadence exception
S/R zone discovery should not ride the normal `4H` recompute cadence by default.

The default posture is:
- selected / authoritative S/R surfaces may be packaged into the packet whenever the packet is built
- dynamic levels may update frequently against the selected S/R surface
- full S/R-zone discovery / reselection should run on its own slower cadence, initially **weekly for in-scope symbols**, plus explicit manual/operator-triggered rebuilds when needed

This keeps stable S/R levels from thrashing while still allowing faster downstream context to react to price, fib, and structure changes.

---

## 8) Weekly close posture

Weekly close is the heaviest normal boundary because it may coincide with:
- `5m`
- `4H`
- `1D`
- `1W`

for every in-scope symbol.

### Required posture
Treat weekly close as a **coordinated heavy checkpoint**.

### Practical rules
- precompute the expected weekly boundary timestamp
- allow the symbol-level manifests to queue naturally
- process symbols with bounded workers
- avoid launching unbounded same-minute fanout

### Recommendation
For the first pass:
- process weekly boundary work **sequentially or with very low concurrency**
- correctness is more important than shaving a few minutes off completion time

For the top 100, that is likely acceptable.

---

## 9) Retry and failure behavior

### Failure domain
Failure should be tracked per:
- symbol
- timeframe
- manifest

### Retry policy
- retry the failed manifest for that symbol
- do not let downstream timeframes claim success if an upstream required step failed
- preserve failure reason and provenance

### Example
If `4H` aggregate materialization fails for BTC-USDT at a stacked weekly boundary:
- do not mark `1D` or `1W` recompute as complete for that manifest
- either:
  - stop the manifest and mark blocked
  - or retry upstream step first

### Suggested result shape

```json
{
  "manifest_id": "...",
  "symbol": "BTC-USDT",
  "status": "blocked",
  "failed_step": "materialize_4H_aggregate",
  "reason": "missing_source_bars",
  "next_action": "reload_authoritative_5m_window_then_retry"
}
```

---

## 10) Recovery interaction

Recompute scheduling must cooperate with feed recovery.

### Rule
If a symbol/timeframe is currently under recovery:
- do not run normal recompute jobs for that invalid window
- recovery must finish first
- then a fresh manifest may be emitted from the repaired authoritative candles

### Why
This prevents recompute from running against half-repaired or partially truncated data.

---

## 11) Source-of-truth rule

The scheduler should always assume:
- canonical closed candles are the source of truth
- recompute outputs are disposable and rebuildable
- if trust is in doubt, discard derived outputs and rebuild from authoritative candles

This is especially important because Redact explicitly prefers trusted rehydrate over clever stitching when data integrity is uncertain.

---

## 12) Suggested job contracts

### A. Close manifest
- `surveyor_close_manifest_v1`

### B. Recompute task
- `surveyor_recompute_task_v1`

Example:

```json
{
  "contract": "surveyor_recompute_task_v1",
  "manifest_id": "...",
  "symbol": "BTC-USDT",
  "timeframe": "1D",
  "action": "recompute_surveyor_families",
  "depends_on": ["materialize_1D_aggregate"],
  "trace_id": "..."
}
```

### C. Recompute result
- `surveyor_recompute_result_v1`

---

## 13) What this means operationally

For the first implementation:
- keep the worker system simple
- use deterministic queues
- use symbol-level locking
- use stacked close manifests
- prefer sequential correctness over aggressive parallelism

That should let the system scale from:
- one symbol
- to top 100
- without changing the core correctness model

---

## 14) Decision

The canonical recompute posture for Surveyor is:
- **close-triggered**
- **symbol-scoped**
- **stacked-boundary aware**
- **strictly ordered within symbol**
- **bounded-concurrency across symbols**
- and **rebuild-from-authoritative-candles first when trust is in doubt**

That is the scheduling model the rest of the BloFin feed and Surveyor execution work should assume.
