# Historical Data Source Decision V0

Date: 2026-04-29
Status: recommended source plan for Strategy Backtesting

## Recommendation

Use **Binance Public Data** as the first bulk historical OHLCV source for research, then validate promising results against the intended execution venue feed, currently BloFin/OKX depending on the rollout lane.

## Why Binance Public Data first

For historical research, we need multi-year 4H / 1D / 1W candles across BTC and eventually a top-100 universe. Binance Public Data is the best first source because:

- bulk historical files are available without account/API-key workflow,
- klines are already aggregated into daily/monthly archives,
- spot and USD-M futures are both available,
- 4H, 1D, and 1W intervals are supported,
- checksum files exist for integrity verification,
- download behavior is more suitable for top-100 backfills than paginating REST candle APIs pair-by-pair.

Primary source reference: `https://github.com/binance/binance-public-data` / `https://data.binance.vision/`.

## Which Binance market

Use two tracks:

1. **Primary research track: USD-M futures klines**
   - Best match for crypto perp-style trading research.
   - Use `BTCUSDT` first, then liquid USDT perpetual symbols.

2. **Secondary validation track: spot klines**
   - Useful for longer/cleaner spot-history sanity checks.
   - Not always identical to perp behavior, especially during funding/liquidation regimes.

## Validation sources

After a candidate strategy looks promising on Binance bulk history:

- validate BTC/ETH on **BloFin** historical candles if we expect BloFin execution / feed alignment,
- optionally cross-check with **OKX** candles via paginated REST/CCXT,
- consider paid/institutional vendors only if venue discrepancies become material or we need normalized survivorship-safe universes.

## Why not BloFin/OKX first

BloFin and OKX are useful validation/execution-alignment sources, but their REST APIs are paginated and rate-limited. That is fine for validation, but awkward as the first bulk research dataset for top-100 multi-year history.

## Minimum BTC research dataset

- BTCUSDT 4H: 3 years minimum, 5 years preferred.
- BTCUSDT 1D: matching or longer context window.
- BTCUSDT 1W: matching or longer macro context window.

## Top-100 universe rule

Do not use “top 100 by CoinMarketCap market cap” as the default backtest universe.

Use a tradable universe instead:

- top liquid USDT perpetuals on the chosen research/execution venue,
- sufficient listing age for the selected lookback,
- acceptable candle continuity,
- exclude symbols with too-short history from headline results or report them separately.

## Implementation plan

1. Add a Binance Public Data downloader/importer for 4H/1D/1W klines. ✅ Initial implementation: `market_arbiter/feed/binance_public_data.py` and `market_arbiter/ops/binance_history_import.py`.
2. Normalize symbols into MarketArbiter format. ✅ `BTCUSDT`/USDT-style symbols normalize to canonical uppercase exchange symbols for this research store.
3. Store raw archive provenance and checksum status. ✅ Import artifacts include source URLs, local archive paths, SHA-256, and checksum pass/fail.
4. Import into `market_candles` or an explicit research candle store. ✅ Initial path imports into existing `market_candles` with provider `binance_public_data` and venue `binance_usdm_futures`/`binance_spot`.
5. Start with BTCUSDT 5Y 4H + 1D + 1W. Next command shape is documented in `docs/OHLCV_BACKTEST_INPUT_CONTRACT_V0.md`.
6. Run continuity report. ✅ Import report includes per-symbol/timeframe continuity status, gap counts, duplicate counts, and first/last open timestamps. Direct Binance `1w` archives may be sparse; use the importer’s deterministic `--derive-weekly-from-daily` path for a complete 1W research-context series when the 1D series is continuous.
7. Then run BTC replay/backtest.
8. Expand to ETH and a 5–10 pair cohort before top-100.

Boundary with feed bakeoff: this implementation intentionally reuses the existing canonical candle table and provenance style, but it does not reuse the real-time feed bakeoff adapters. That is the right separation: Binance Public Data is a cold historical seed for REST/HTTP archive research, while the feed bakeoff remains focused on live/near-real-time provider fitness.
