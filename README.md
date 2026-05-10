# MarketArbiter

MarketArbiter is the extracted successor repo for the Surveyor/Arbiter trading architecture.

## Current boundary

- **Surveyor**: canonical feed ingestion, market-state packet assembly, and operator inspection
- **Arbiter**: future decision layer that selectively consumes Surveyor datasets through one active strategy pack
- **Sentinel**: proposed armed-watch layer that monitors Arbiter-selected symbol/zone contexts and reports events back to Arbiter
- **Execution**: intentionally downstream and out of scope for the initial extraction

## Current landed capabilities

- canonical SQLite market-data storage and migrations
- OKX/CCXT feed refresh path retained from extraction
- BloFin feed contract, historical hydration/recovery docs, websocket 5m consumer, and deterministic repair posture; BloFin is now treated as incumbent/canary pending provider bakeoff rather than presumed final canonical feed
- Surveyor packet snapshot assembly
- unified Surveyor dataset bundle with stable family envelopes for `feed_state`, `structure_state`, `sr_zones`, `fib_context`, `dynamic_levels`, and `interaction_lifecycle`
- bundle delivery profiles for `ui_full`, `arbiter_core`, and `backtest_core`, with a profile-selected export utility and consumer acceptance gate
- Binance Public Data historical OHLCV archive downloader/importer for 4H/1D/1W research seeds, with checksum/provenance manifests and continuity reports
- bundle-driven Streamlit operator UI; legacy packet JSON is compatibility/raw only
- manifest-driven symbol onboarding and active workset rendering with local report / advance-check artifacts
- focused tests for market data, scheduler behavior, Surveyor packet construction, bundle UI helpers, profile acceptance rules, feed runner behavior, and onboarding controls

## Not carried forward in this first pass

- telegram / mobchart ingestion
- paper-runtime machinery
- TradingView automation
- strategy sweep legacy tooling
- old LiquidSniper identity surfaces

## Layout

- `configs/` checked-in runner/workset manifests
- `market_arbiter/core/` core storage + snapshot logic
- `market_arbiter/surveyor/` descriptive analysis modules
- `market_arbiter/arbiter/` decision-layer scaffold
- `market_arbiter/ops/` operational entrypoints, including one-shot refresh, the thin continuous Surveyor feed runner, Binance historical import, and profile-selected bundle export
- `market_arbiter/web/` operator UI
- `docs/` migrated architecture + extraction notes

## Current planning docs

- `docs/MARKETARBITER_STATUS_AND_ROADMAP_2026-04-27.md` , current status snapshot plus roadmap from Surveyor closure through Arbiter design/build; use this as the live control-plane summary for `#marketarbiter`
- `docs/FEED_PROVIDER_BAKEOFF_PLAN_V1.md` , provider-selection plan for choosing the canonical top-100 Surveyor feed before large-scale soak
- `docs/HISTORICAL_DATA_SOURCE_DECISION_V0.md` , Binance Public Data cold-history seed decision for Strategy Backtesting, distinct from the live feed bakeoff
- `docs/OHLCV_BACKTEST_INPUT_CONTRACT_V0.md` , OHLCV simulation input contract and Binance historical import command shape
- `docs/ARBITER_ARCHITECTURE_V1.md` , proposed Arbiter / Sentinel / strategy-pack boundary before live decision logic is built
- `docs/SURVEYOR_TO_ARBITER_CONTROL_PLANE_HANDOFF_V1.md` , macro sequencing + ownership handoff for finishing Surveyor feed work and moving project control into `#marketarbiter`
- `docs/SURVEYOR_UNIFIED_DATASET_CONTRACT_V1.md` , shared dataset-bundle contract for UI, payload delivery, Arbiter selection, and replay/backtesting; UI rendering and the first profile-selected payload/fixture freeze slice are complete enough to close
- `docs/SURVEYOR_CONTINUOUS_FEED_PLAN_V1.md` , current-state continuity review plus the minimum viable live runner plan
- `docs/BLOFIN_MARKET_DATA_CONTRACT_V1.md` , proposed raw and aggregate candle contracts, retention posture, and BloFin websocket/REST responsibilities
- `docs/BLOFIN_GAP_RECOVERY_WORKFLOW_V1.md` , authoritative repair/reseed workflow before websocket resume
- `docs/BLOFIN_HISTORY_REQUIREMENTS_V1.md` , timeframe-specific historical hydration and retention contract
- `docs/SURVEYOR_RECOMPUTE_SCHEDULE_V1.md` , symbol-scoped stacked-close scheduling contract for 5m/4H/1D/1W recompute work
- `docs/SYMBOL_ONBOARDING_AND_100_PAIR_SCALE_V1.md` , config-driven symbol admission flow plus required changes before trusting ~100 concurrent pairs
- `docs/SURVEYOR_FEED_ONBOARDING_CONTRACT_V1.md` , staged rollout manifest, onboarding CLI, automated local check-ins, and provider/IP safety contract
- `docs/SURVEYOR_FEED_TOP100_READY_TARGET_V1.md` , revised intraday feed thread closeout target: top-100-ready staged rollout readiness, not merely single-pair operation
- `docs/INTRADAY_REVISIT_SURVEYOR_ARBITER_ARCHITECTURE_V1.md` , architecture overview and feed-provider framing
