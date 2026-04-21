CREATE TABLE IF NOT EXISTS market_candles (
    id INTEGER PRIMARY KEY,
    provider_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    ts_open_ms INTEGER NOT NULL,
    ts_close_ms INTEGER NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    ingest_ts_ms INTEGER NOT NULL,
    dataset_version TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    UNIQUE(provider_id, venue, symbol, timeframe, ts_open_ms)
);

CREATE INDEX IF NOT EXISTS idx_market_candles_symbol_timeframe_open
    ON market_candles(symbol, timeframe, ts_open_ms);

CREATE INDEX IF NOT EXISTS idx_market_candles_trace_id
    ON market_candles(trace_id);

CREATE TABLE IF NOT EXISTS feed_checkpoints (
    provider_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    last_ts_open_ms INTEGER,
    last_success_ms INTEGER,
    last_attempt_ms INTEGER,
    failure_count INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'ok',
    last_reason_code TEXT,
    trace_id TEXT,
    PRIMARY KEY (provider_id, venue, symbol, timeframe)
);

CREATE INDEX IF NOT EXISTS idx_feed_checkpoints_state
    ON feed_checkpoints(state);

CREATE TABLE IF NOT EXISTS feed_health_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    state TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL,
    as_of_ms INTEGER NOT NULL,
    trace_id TEXT,
    created_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_feed_health_events_lookup
    ON feed_health_events(provider_id, venue, symbol, timeframe, as_of_ms DESC);
