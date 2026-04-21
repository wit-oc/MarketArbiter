# MarketArbiter

MarketArbiter is the extracted successor repo for the Surveyor/Arbiter trading architecture.

## Current boundary

- **Surveyor**: canonical feed ingestion, market-state packet assembly, and operator inspection
- **Arbiter**: future decision layer that selectively consumes Surveyor datasets
- **Execution**: intentionally downstream and out of scope for the initial extraction

## Extracted in this first pass

- canonical SQLite market-data storage and migrations
- OKX/CCXT feed refresh path
- Surveyor packet assembly
- phase-1 structure, fib, and dynamic-level primitives
- a thin Streamlit packet inspector
- focused tests for market data, scheduler behavior, and Surveyor packet construction

## Not carried forward in this first pass

- telegram / mobchart ingestion
- paper-runtime machinery
- TradingView automation
- strategy sweep legacy tooling
- old LiquidSniper identity surfaces

## Layout

- `market_arbiter/core/` core storage + snapshot logic
- `market_arbiter/surveyor/` descriptive analysis modules
- `market_arbiter/arbiter/` decision-layer scaffold
- `market_arbiter/ops/` operational entrypoints
- `market_arbiter/web/` operator UI
- `docs/` migrated architecture + extraction notes
