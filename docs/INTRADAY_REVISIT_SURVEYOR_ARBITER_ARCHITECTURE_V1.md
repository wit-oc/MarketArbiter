# Intraday Revisit v1, Surveyor / Arbiter Architecture

Date: 2026-04-19  
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
- **Arbiter** = interpretation / decision layer
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

### Execution
Execution, if it exists later, remains downstream of Arbiter and should not be fused into the feed/analysis layer.

---

## 4) Current system shape

## 4.1 Canonical feed ingestion

Current implementation status:
- primary source in this checkpoint: **OKX via CCXT**
- current covered timeframes for Surveyor: `1W`, `1D`, `4H`, `5m`
- persistence target: SQLite canonical market-data store

Current code path:
- `liquidsniper/ops/surveyor_feed_refresh.py`

Current persisted domains used by this path:
- `market_candles`
- `feed_checkpoints`
- `feed_health_events`

The feed-refresh layer is responsible for:
- pulling recent closed OHLCV windows,
- upserting canonical candles,
- checkpointing freshness/state,
- and surfacing degraded conditions explicitly.

This is important because the downstream packet should know the difference between:
- fresh,
- stale,
- partial,
- or replay-only inputs.

## 4.2 Surveyor packet assembly

Current code path:
- `liquidsniper/core/surveyor_snapshot.py`
- `IntradayTrading/engine/surveyor_packet.py`

The packet assembly layer pulls together:
- canonical market candles,
- feed checkpoint state,
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
- `liquidsniper/web/app.py`

The UI currently serves as an audit and operator review surface.
It now includes a Surveyor packet view wired to the canonical store.

Its purpose is to let a human inspect:
- whether packet inputs are fresh,
- whether required timeframes are present,
- what the descriptive packet says,
- and whether the system is complete or partial before any later decision layer is trusted.

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
- Surveyor UI tab is wired into Streamlit,
- repo-root path fixes were added so the UI can launch reliably outside repo cwd,
- snapshot tests exist for complete and partial packet states.

Relevant files:
- `liquidsniper/core/surveyor_snapshot.py`
- `liquidsniper/ops/surveyor_feed_refresh.py`
- `IntradayTrading/engine/surveyor_packet.py`
- `liquidsniper/web/app.py`
- `tests/test_surveyor_snapshot.py`

Validated in this checkpoint:
- the Streamlit app imports cleanly,
- the Surveyor app can listen on the LAN,
- packet assembly completes against populated canonical feed state,
- and the relevant targeted tests pass.

---

## 8) What is next

The next three priorities are:

1. **Freeze one shared structure/provenance adapter**  
   Surveyor, Fib, and related consumers should share one upstream structure contract rather than drifting.

2. **Define the Arbiter handoff contract**  
   Surveyor should stay descriptive, while Arbiter becomes the explicit interpretation layer.

3. **Build robust replay / simulation / backtesting on top of the canonical packet**  
   The backtesting layer should consume canonical candles + canonical Surveyor packet outputs, not legacy mixed-state paths.

---

## 9) Summary for external sharing

If shared with a prospective feed provider, the honest summary is:

> We are building a deterministic intraday research stack that uses programmatic market data to populate a canonical candle store, generate a multi-timeframe descriptive market-state packet, inspect that packet in an operator UI, and later drive robust replay/simulation/backtesting. The current phase is analysis and evidence-building first, with decision and execution intentionally kept as separate downstream layers.

That is the actual use case this feed access would support.
