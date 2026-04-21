# MarketArbiter Extraction Manifest (2026-04-21)

## Intent
Create a clean successor repo centered on Surveyor and Arbiter rather than continue in-place purification of LiquidSniper.

## Copied now

### Core runtime
- `market_arbiter/core/db.py`
- `market_arbiter/core/market_data.py`
- `market_arbiter/core/market_quality.py`
- `market_arbiter/core/market_scheduler.py`
- `market_arbiter/core/surveyor_snapshot.py`
- `market_arbiter/ops/surveyor_feed_refresh.py`

### Surveyor analysis modules
- `market_arbiter/surveyor/phase1_contract.py`
- `market_arbiter/surveyor/htf_phase1.py`
- `market_arbiter/surveyor/structure.py`
- `market_arbiter/surveyor/fib_anchors.py`
- `market_arbiter/surveyor/fib_context.py`
- `market_arbiter/surveyor/dynamic_levels.py`
- `market_arbiter/surveyor/zones.py`
- `market_arbiter/surveyor/surveyor_packet.py`

### Thin replacements
- `market_arbiter/web/app.py` is a new thin Surveyor packet inspector, not a full copy of the legacy Streamlit app
- `market_arbiter/core/pair_analytics.py` and `market_arbiter/core/sr_universe.py` are reduced helpers kept only to support replay fallback in the extracted snapshot path

### Tests
- `tests/test_market_data_contract.py`
- `tests/test_market_scheduler.py`
- `tests/test_surveyor_snapshot.py`

## Deferred
- Arbiter selection / decision modules
- richer operator UI tabs
- replay harness and backtest evaluator extraction
- alternate dataset families like delta volume and supply/demand zones

## Left behind on purpose
- telegram/mobchart ingestion
- paper-runtime stack
- TradingView automation/tooling
- strategy sweep legacy tooling
- LiquidSniper-specific naming and archive ballast
