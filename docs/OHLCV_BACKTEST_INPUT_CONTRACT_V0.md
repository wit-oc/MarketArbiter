# OHLCV Backtest Input Contract V0

## Purpose

Let us provide OHLCV data directly to MarketArbiter and run the Arbiter strategy candidates across many symbols, including a future top-100 cohort, without trying to force the full strategy into Pine Script.

TradingView/Pine remains useful for visual review, but the Python path is now the canonical route for complex multi-timeframe strategy replay, event studies, and cross-pair simulation.

## Current code surface

- Loader/simulator: `market_arbiter/arbiter/ohlcv_backtest.py`
- CLI runner: `market_arbiter/ops/strategy_backtest_run.py`
- Tests: `tests/test_ohlcv_backtest.py`

Run shape:

```bash
python3 -m market_arbiter.ops.strategy_backtest_run \
  --dataset path/to/foxian_retest_backtest_dataset.json \
  --ohlcv-dir path/to/ohlcv \
  --timeframe 4h \
  --output artifacts/strategy_backtests/report.json
```

Optional:

```bash
  --symbols BTCUSDT,ETHUSDT,SOLUSDT \
  --max-hold-bars 288 \
  --target-rr 2.0 \
  --same-bar-fill-policy stop_first
```

## Dataset input

The runner consumes the strategy dataset emitted by:

- `market_arbiter.arbiter.strategy_backtest.build_foxian_retest_backtest_dataset(...)`

Expected contract:

```json
{
  "contract": "foxian_retest_backtest_dataset_v0",
  "ruleset_id": "foxian_retest_flip_confluence_v0",
  "event_study_rows": [...],
  "trade_candidates": [...]
}
```

`trade_candidates` must include at minimum:

```json
{
  "symbol": "BTCUSDT",
  "side": "long",
  "entry_ts": "2026-01-01T00:00:00Z",
  "entry_event_id": "event-id",
  "invalidation_level_hint": 99000.0,
  "stop_buffer_bps": 5.0,
  "target_rr": [1.0, 2.0],
  "cost_model": {
    "taker_fee_bps": 5.0,
    "slippage_bps": 2.0,
    "funding_bps_per_8h": 0.0
  }
}
```

## OHLCV directory input

Put one file per symbol in a directory.

Accepted CSV names:

- `BTCUSDT.4h.csv`
- `BTCUSDT_4h.csv`
- `BTCUSDT-4h.csv`
- `BTCUSDT.csv`

Accepted JSON names use the same stem forms with `.json`.

CSV columns:

```csv
timestamp,open,high,low,close,volume
2026-01-01T00:00:00Z,99000,99500,98500,99200,123.45
```

Also accepted timestamp aliases:

- `ts`
- `time`
- `datetime`
- `date`

Timestamp formats:

- ISO8601 with `Z`
- epoch seconds
- epoch milliseconds

JSON may be either a list of candle objects or an object containing `candles`, `ohlcv`, or `rows`.

## Simulation rules V0

For each candidate:

1. Entry fills at the **next candle open after `entry_ts`**.
2. Slippage is applied adversely on entry and exit.
3. Long stop: below `invalidation_level_hint` after `stop_buffer_bps`.
4. Short stop: above `invalidation_level_hint` after `stop_buffer_bps`.
5. Target uses candidate `target_rr[0]` unless CLI/config overrides `--target-rr`.
6. Same-bar target/stop ambiguity defaults to `stop_first`.
7. Fees are charged on both entry and exit.
8. Funding is charged as elapsed-time drag using `funding_bps_per_8h`.
9. If neither target nor stop hits before `max_hold_bars`, exit at close of the max-hold candle.

This is conservative and intentionally simple. Later versions can add partial exits, opposing-zone targets, trailing management, and portfolio overlap rules.

## Report output

The CLI emits:

```json
{
  "contract": "strategy_backtest_run_report_v0",
  "trade_report": {
    "contract": "ohlcv_strategy_backtest_report_v0",
    "coverage": {...},
    "summary": {...},
    "by_symbol": {...},
    "trades": [...],
    "skipped": [...]
  },
  "event_study_report": {
    "contract": "ohlcv_event_study_report_v0",
    "rows": [...]
  }
}
```

Key metrics:

- closed/skipped trades
- win rate
- total and average net bps
- average net R multiple
- max drawdown in cumulative bps
- per-symbol summaries
- event-study forward returns by horizon

## Binance historical import path

First implementation for multi-year research hydration:

```bash
python3 -m market_arbiter.ops.binance_history_import \
  --market usdm-futures \
  --symbols BTCUSDT \
  --timeframes 4h,1d,1w \
  --years 5 \
  --download \
  --import-to-db \
  --skip-missing \
  --derive-weekly-from-daily \
  --db-path data/market_arbiter.sqlite \
  --archive-dir data/historical/binance_public_data \
  --output-dir artifacts/historical_data/binance_btc_5y
```

The CLI writes:

- `binance_history_import_plan.json` before network work,
- `binance_history_download_manifest.json` with source URLs, local paths, SHA-256 checksum status, and missing archives,
- `binance_history_import_report.json` with insert/idempotency totals and per-symbol/timeframe continuity reports.

Default range is the previous five years through the last closed UTC month, because Binance monthly archives for the current open month may not exist yet. Use `--include-open-month` only when explicitly testing current-month archive availability.

Imported raw candles use provider `binance_public_data`, venue `binance_usdm_futures` or `binance_spot`, and the existing canonical `market_candles` table. If direct `1w` archives are sparse, `--derive-weekly-from-daily` also writes a deterministic `binance_public_data_derived` 1W series from the imported 1D candles and records its own continuity report. This is a historical research seed path, not the live feed-provider bakeoff path.

## Fast BTC strategy attempt

Once BTCUSDT 4H history is imported into `market_candles`, run the rough candidate adapter + simulator with:

```bash
python3 -m market_arbiter.ops.fast_ohlcv_retest_backtest \
  --db-path data/market_arbiter.sqlite \
  --symbol BTCUSDT \
  --timeframe 4h \
  --provider-id binance_public_data \
  --venue binance_usdm_futures \
  --output-dir artifacts/strategy_backtests/fast_ohlcv_retest_btc_5y
```

This writes:

- `fast_ohlcv_retest_dataset.json` — synthetic Foxian candidate dataset generated from OHLCV breakout/retest patterns.
- `fast_ohlcv_retest_report.json` — trade simulation and event-study report.

Use this only as the first rough research cut. The stronger version still needs a Surveyor `backtest_core` replay builder so zones, lifecycle events, and confluence are reconstructed from the same point-in-time path the live system will use.

## Top-100 path

The intended scale path is:

1. BTC 4H mechanics smoke fixture from existing local data.
2. BTC meaningful research run with **3Y minimum / 5Y preferred** 4H history plus 1D/1W context.
3. ETH equivalent run.
4. 5–10 liquid pair cohort.
5. 25-pair medium cohort.
6. Top-100 candidate cohort from the Surveyor feed/onboarding workset.

Important boundary: top-100 **simulation** can run from supplied OHLCV files as soon as files exist. Top-100 **live feed readiness** is still governed by `docs/SURVEYOR_FEED_TOP100_READY_TARGET_V1.md` and should not be claimed until that lane produces its evidence packet.

## Current limitation

This runner simulates emitted Arbiter candidates against OHLCV. Default research timeframe is 4H; daily/weekly provide context and 5m remains an optional later execution-refinement layer. The remaining bridge for fully end-to-end historical research is the replay builder that creates point-in-time Surveyor `backtest_core` profiles from the OHLCV store for every symbol/timestamp. That is the next design/build slice.

Risk sizing, waiting/expiry, and weighted secondary confluence design are tracked in `docs/RISK_AND_CONFLUENCE_MODEL_V0.md`.
