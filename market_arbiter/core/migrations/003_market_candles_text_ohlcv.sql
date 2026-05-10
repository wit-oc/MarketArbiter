CREATE TABLE IF NOT EXISTS market_candles_text_migration_v003 (
    id INTEGER PRIMARY KEY,
    provider_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    ts_open_ms INTEGER NOT NULL,
    ts_close_ms INTEGER NOT NULL,
    open TEXT NOT NULL,
    high TEXT NOT NULL,
    low TEXT NOT NULL,
    close TEXT NOT NULL,
    volume TEXT NOT NULL,
    ingest_ts_ms INTEGER NOT NULL,
    dataset_version TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    UNIQUE(provider_id, venue, symbol, timeframe, ts_open_ms)
);

INSERT OR IGNORE INTO market_candles_text_migration_v003(
    id, provider_id, venue, symbol, timeframe,
    ts_open_ms, ts_close_ms,
    open, high, low, close, volume,
    ingest_ts_ms, dataset_version, trace_id
)
SELECT
    id, provider_id, venue, symbol, timeframe,
    ts_open_ms, ts_close_ms,
    CAST(open AS TEXT), CAST(high AS TEXT), CAST(low AS TEXT), CAST(close AS TEXT), CAST(volume AS TEXT),
    ingest_ts_ms, dataset_version, trace_id
FROM market_candles;

DROP TABLE market_candles;
ALTER TABLE market_candles_text_migration_v003 RENAME TO market_candles;

CREATE INDEX IF NOT EXISTS idx_market_candles_symbol_timeframe_open
    ON market_candles(symbol, timeframe, ts_open_ms);

CREATE INDEX IF NOT EXISTS idx_market_candles_trace_id
    ON market_candles(trace_id);
