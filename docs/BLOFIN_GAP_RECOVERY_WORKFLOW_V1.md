# BloFin Gap Recovery Workflow V1

Date: 2026-04-21  
Status: proposed startup, reconnect, and authoritative reseed contract  
Intent: freeze how MarketArbiter should recover from feed interruptions without introducing stitched-data corruption

---

## 1) Why this exists

The BloFin-first feed path should not rely on clever gap stitching as its primary recovery strategy.

Redact’s explicit preference is correct:
- if trust is in doubt,
- discard the affected authoritative candle window,
- reload from the exchange,
- rebuild derived outputs,
- then resume streaming.

That is the safest way to reduce the risk of silent dataset contamination.

---

## 2) Recovery principles

1. **The exchange is the authoritative source**
   - not the partially remembered local store
   - not an inferred stitch path

2. **Canonical closed candles are rebuildable**
   - Surveyor outputs are disposable derivatives
   - if needed, rebuild them from authoritative candles

3. **WebSocket is not the replay path**
   - WebSocket is for live updates and closed-candle confirmation
   - REST is for bootstrap, repair, and reconciliation

4. **Recovery decisions are made per symbol**
   - not one giant all-or-nothing global switch unless the service state is broadly compromised

5. **When the gap exceeds the hot retention / repair horizon, reseed instead of patching**
   - simple and deterministic beats clever and fragile

6. **Repair narrowly-invalid history deterministically or quarantine it explicitly**
   - repairable envelope defects should become repaired canonical candles with audit metadata
   - unrepairable rows should not be smuggled into canonical truth

---

## 3) Canonical timeframes in scope

### Live canonical raw timeframe
- `5m`

### Derived aggregate timeframes
- `4H`
- `1D`
- `1W`

### Historical hydration posture
Do not hydrate all history from `5m` alone.
Hydrate each timeframe according to its own contract window.

---

## 4) Core checkpoints

The system must store a per-symbol authoritative checkpoint for canonical `5m`.

### Required checkpoint fields

```json
{
  "contract": "blofin_feed_checkpoint_v1",
  "symbol": "BTC-USDT",
  "venue": "blofin",
  "canonical_timeframe": "5m",
  "last_closed_ts_open_ms": 1776902100000,
  "last_closed_ts_close_ms": 1776902400000,
  "last_good_ingest_ts_ms": 1776902401234,
  "continuity_state": "live|repair_pending|reseed_required",
  "trace_id": "..."
}
```

This checkpoint is the first input to startup and reconnect recovery.

---

## 5) Recovery modes

## A. Clean startup
Use when:
- no existing local state
- or the local state is intentionally discarded

Actions:
1. hydrate historical windows from REST by timeframe contract
2. validate completeness
3. build derived `4H`, `1D`, `1W`
4. then attach WS `candle5m`

## B. Short-gap repair
Use when:
- a valid checkpoint exists
- and the missing `5m` interval is within the repair horizon

Recommended first-pass repair horizon:
- **5 days**

Actions:
1. pause normal recompute for the symbol
2. drop the affected `5m` repair window if needed
3. reload authoritative `5m` candles for the missing window from REST
4. repair narrowly-invalid candles deterministically when they only violate candle-envelope integrity
5. classify the repaired window with a quality band (`clean|benign|elevated|degraded|blocked`)
6. apply timeframe-scoped circuit-breaker posture (`accept|warn|quarantine_timeframe|block_symbol`)
7. invalidate overlapping derived windows
8. rebuild affected `4H`, `1D`, `1W`
9. validate closure of the gap
10. resume WS

## C. Full reseed
Use when:
- no trustworthy checkpoint exists
- or the gap exceeds the repair horizon
- or validation detects inconsistency in the local authoritative candle window

Actions:
1. mark symbol `reseed_required`
2. discard local authoritative `5m` hot window for that symbol
3. reload timeframe-specific historical windows from REST:
   - `1W` history contract
   - `1D` history contract
   - `4H` history contract
   - `5m` hot retention contract
4. rebuild all derived outputs for that symbol
5. validate
6. resume WS

---

## 6) Startup workflow

At process start, for each symbol:

1. load the last checkpoint
2. compare checkpoint close time to current time
3. compute the missing `5m` interval
4. choose recovery mode:
   - no checkpoint -> **clean startup**
   - gap <= 5D -> **short-gap repair**
   - gap > 5D -> **full reseed**
5. complete recovery for the symbol
6. only after recovery succeeds, subscribe the symbol to WS live updates

### Important rule
**Do not attach WebSocket first and then try to recover later.**  
Recovery should establish the authoritative base before live consumption resumes.

---

## 7) Reconnect workflow

When the live WS consumer disconnects:

1. mark affected symbols `repair_pending`
2. record disconnect time
3. stop normal recompute manifests for invalid windows
4. on reconnect, calculate the exact gap from the checkpoint
5. if the gap is within the repair horizon, run short-gap repair
6. if it exceeds the repair horizon, run full reseed
7. only then rejoin normal live consumption

---

## 8) Timeframe-specific REST hydration contract

### `1W`
- hydrate full bounded weekly context according to the weekly history contract
- do not rely on `5m` to reconstruct deep weekly history

### `1D`
- hydrate the daily history contract directly from REST
- current planning bias: up to **1440 candles**

### `4H`
- hydrate the `4H` contract directly from REST
- current planning bias: **180 days**, about **1080 candles**

### `5m`
- hydrate only the hot operational window
- current planning bias: **rolling 5D**

---

## 9) Authoritative-window discard policy

When recovery runs, prefer discarding the affected authoritative window over trying to surgically preserve suspect fragments.

### Recommended discard rules

#### Short-gap repair
- discard only the overlapping `5m` repair window for the affected symbol
- rebuild derived windows touched by that `5m` interval

#### Full reseed
- discard the local hot `5m` window for the symbol
- retain only what is outside the reseed contract if explicitly trusted and non-overlapping
- rebuild higher derived windows from newly hydrated authoritative sources

### Why
This is the core trust rule:
- authoritative candles may be reloaded
- derived outputs should be assumed rebuildable
- silent partial preservation is more dangerous than explicit reload

---

## 10) Derived-window invalidation rules

If `5m` authoritative candles are reloaded for a symbol, then overlapping derived windows must be invalidated before rebuild.

### Example
If the repaired `5m` window overlaps:
- one `4H` bar
- one `1D` bar
- one `1W` bar

then those derived bars and their downstream Surveyor datasets must be marked stale and recomputed from the new authoritative input.

### Rule
Derived datasets should never survive authoritative-candle replacement without explicit revalidation.

---

## 11) Validation requirements

Recovery is not done just because REST returned rows.

### Required validations
- no gaps remain in canonical `5m` for the repaired/reseeded window
- boundary alignment is valid
- derived source-bar counts match expected counts
- `4H`, `1D`, and `1W` rebuilds succeeded for overlapping windows
- symbol continuity state returns to `live`

### Failure posture
If any of these fail:
- keep the symbol blocked
- do not resume normal recompute
- surface the failure reason explicitly

---

## 12) Recovery result contract

Suggested result shape:

```json
{
  "contract": "blofin_gap_recovery_result_v1",
  "symbol": "BTC-USDT",
  "mode": "repair|reseed",
  "status": "ok|blocked|failed",
  "authoritative_windows_reloaded": [
    {"timeframe": "5m", "from_ts_ms": 1776800000000, "to_ts_ms": 1776902400000}
  ],
  "derived_windows_rebuilt": ["4H", "1D"],
  "validation": {
    "gaps_closed": true,
    "derived_counts_ok": true,
    "continuity_restored": true
  },
  "reason": null,
  "trace_id": "..."
}
```

---

## 13) Interaction with the recompute scheduler

Recovery takes precedence over normal recompute.

### Rules
- while recovery is running for a symbol, do not emit normal stacked close manifests for invalid windows
- once recovery succeeds, the scheduler may emit a fresh manifest derived from the repaired authoritative candles
- recovery completion should produce one explicit scheduler-safe handoff point

This keeps recompute from running against half-repaired state.

---

## 14) Operational recommendation

### First-pass behavior
- prefer correctness over minimal reload size
- keep the logic obvious and auditable
- resist the temptation to over-optimize recovery too early

### Default thresholds
- `5m` hot retention: **5D**
- `5m` short-gap repair horizon: **5D**
- beyond that: **full reseed**

These values can change later, but the mode split should remain.

---

## 15) Rate-limit / ban safety requirement

All BloFin REST recovery traffic must pass through a shared governor.

### Required behavior
- honor BloFin's documented REST limit before reaching it
- treat HTTP `429` as a normal rate-limit cooldown signal
- treat HTTP `403` as a stronger protective restriction / possible temporary ban condition
- after `403`, do not immediately keep hammering requests
- recovery jobs must surface the distinction between `rate_limited` and `firewall_restricted`

### Design implication
Recovery orchestration should assume that a noisy or poorly-governed repair loop can make a bad situation worse. The governor is therefore part of correctness, not just politeness.

## 16) Decision

The canonical BloFin recovery posture for MarketArbiter is:
- **REST-first recovery before WS resume**
- **short-gap repair within the hot `5m` horizon**
- **full reseed when trust or time horizon is exceeded**
- **deterministic repair for narrow historical envelope defects**
- **quality-band classification on repaired history**
- **timeframe-scoped circuit-breaker actions instead of one vague degraded bucket**
- **authoritative candle reload over clever stitching**
- **derived outputs invalidated and rebuilt after authoritative replacement**
- **all REST recovery traffic routed through one governor path**

That is the recovery model the feed service, derived-candle builder, and recompute scheduler should all assume.
