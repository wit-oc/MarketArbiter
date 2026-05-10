# Surveyor Continuous Feed Plan V1

Date: 2026-04-21  
Status: current-state review plus minimum viable continuous-ingestion plan  
Intent: verify continuity honestly before assuming live Surveyor is already continuous

---

## 1) Honest current state

The repo has real feed pieces, but it does **not** yet prove a full continuous runner.

What exists now:
- `market_arbiter/ops/surveyor_feed_refresh.py`
  - one-shot OKX/CCXT refresh entrypoint
  - fetches recent closed windows
  - writes `market_candles`, `feed_checkpoints`, and `feed_health_events`
- `market_arbiter/core/market_scheduler.py`
  - tested scheduler primitive with backfill, retry, rate budget, circuit breaker, gap detection, and health logging
- `market_arbiter/core/surveyor_snapshot.py`
  - reads feed checkpoints and canonical candles to build Surveyor packet state

What does **not** yet exist in repo as a completed boundary:
- a long-running runner that repeatedly invokes `MarketDataScheduler`
- a first-class symbol/timeframe manifest for continuous operation
- a durable service/daemon contract for live ingestion
- an operator status surface that clearly says whether the feed is truly continuous or only refreshed on demand

Bottom line:
- **feed refresh exists**
- **scheduler primitives exist**
- **continuous ingestion is not yet a frozen, repo-level product surface**

---

## 2) What “continuous” should mean here

For this repo, continuous does **not** mean tick-by-tick or HFT.
It means:
- the declared symbol/timeframe set is refreshed automatically on an ongoing loop
- closed-candle cadence is respected
- checkpoints and health events are persisted every cycle
- stale, degraded, tripped, and resync-required states remain explicit
- Surveyor can state whether its bundle came from live continuous ingestion, one-shot refresh, replay, or a mixed mode

That is enough for deterministic intraday analysis and honest backtesting inputs.

---

## 3) Required runner contract

The minimum continuous runner should own these responsibilities:

1. open the canonical SQLite database
2. instantiate provider clients once per process
3. declare the symbol/timeframe workset
4. schedule `MarketDataScheduler.run_cycle(...)` per key
5. persist checkpoints and feed-health events
6. emit a compact cycle summary for logs / operator inspection
7. expose enough state for Surveyor bundle meta to declare continuity honestly

Implemented entrypoint:
- `market_arbiter/ops/surveyor_feed_runner.py`

Current implemented modes:
- `once`
- `loop`
- `status`

Supported run modes:
- `once`
- `loop`
- `status`

`once` keeps parity with the current refresh style.  
`loop` is the first true continuous mode.  
`status` gives the UI and operator tooling a direct read of current continuity posture.

---

## 4) Proposed config shape

Near-term config can stay simple and file/CLI driven.

Example shape:

```json
{
  "db_path": "data/market_arbiter.sqlite",
  "provider": "okx_ccxt",
  "symbols": ["BTCUSDT", "ETHUSDT"],
  "timeframes": ["5m", "4h", "1d", "1w"],
  "loop_sleep_ms": 15000,
  "close_lag_ms": 2500,
  "max_backfill_bars": {
    "5m": 2500,
    "4h": 800,
    "1d": 500,
    "1w": 300
  }
}
```

This does not need a heavy config system yet.
The important thing is that the workset becomes explicit and durable.

---

## 5) Scheduling posture

The runner should not blindly refetch every timeframe at full cadence every pass.
It should rely on scheduler state and candle-close boundaries.

Practical posture:
- loop frequently enough to notice newly closed candles
- let `MarketDataScheduler` decide whether a timeframe is due, stale, gapped, or still waiting for close lag
- prioritize `5m` first, then `4h`, `1d`, `1w`
- keep trace ids per cycle and per key

Near-term key ordering bias:
1. all `5m`
2. all `4h`
3. all `1d`
4. all `1w`

That fits current intraday importance without overcomplicating orchestration.

---

## 6) Continuity state exposed to Surveyor

The unified Surveyor dataset bundle should expose continuity explicitly.

Near-term `meta.continuity_state` values:
- `live_continuous`
- `one_shot_refresh`
- `replay_only`
- `mixed`
- `unknown`

Near-term `datasets.feed_state.summary` should also expose:
- last successful cycle time per timeframe
- latest candle close time per timeframe
- checkpoint state per timeframe
- freshness age per timeframe
- whether the current bundle depends on replay fallback

This is how UI, payload delivery, and Arbiter can reason honestly about live trustworthiness.

---

## 7) Relationship to replay and backtesting

The continuous runner should improve live feed honesty, but replay/backtesting must stay a separate concern.

Rules:
- live continuity should write canonical candles and health state
- replay/backtesting should read canonical family contracts, not live-runner internals
- replay may populate the same bundle contract with `build_mode = replay`
- backtest evaluation should reject or flag bundles whose family status is partial/stale unless the experiment explicitly allows it

That last point matters. Robust backtesting dies when degraded live assumptions get silently mixed into replay logic.

---

## 8) Minimum implementation sequence

### Phase 1
Create the runner boundary.
- add `surveyor_feed_runner.py` ✅
- support `--mode once|loop|status` ✅
- reuse `MarketDataScheduler` ✅
- keep current one-shot refresh entrypoint working ✅

### Phase 2
Freeze a feed-workset manifest.
- explicit symbols
- explicit timeframes
- explicit loop cadence / sleep

### Phase 3
Expose runner status to the UI and bundle meta.
- continuity state
- last cycle state
- latest errors / reason codes

### Phase 4
Add focused tests.
- loop mode does not over-fetch before candle close
- degraded/tripped states persist correctly across cycles
- continuity state is visible to Surveyor bundle assembly

---

## 9) Non-goals for this phase

Do not treat these as required for V1 continuity:
- websocket ingestion
- sub-second updates
- execution-bot integration
- cross-provider routing/failover
- distributed workers
- cloud-only infra assumptions

Those may come later, but they are not necessary to make Surveyor honest and continuous now.

---

## 10) Recommendation

Treat the current repo as **continuity-capable but not continuity-frozen**.

The next code move should be small and explicit:
- build a thin continuous runner on top of `MarketDataScheduler`,
- expose continuity state into the unified Surveyor bundle,
- and only then claim that live Surveyor is truly continuous.
