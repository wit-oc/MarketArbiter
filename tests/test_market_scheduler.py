from __future__ import annotations

from dataclasses import dataclass

from market_arbiter.core.db import init_db
from market_arbiter.core.market_data import CandleDTO
from market_arbiter.core.market_scheduler import (
    CircuitBreaker,
    MarketDataScheduler,
    ProviderRateLimitError,
    ProviderUpstreamError,
    RateBudgetManager,
    SchedulerKey,
)


@dataclass
class StubProvider:
    batches: list[list[CandleDTO]]
    error: Exception | None = None
    calls: list[tuple[str, str, int | None, int]] | None = None

    def fetch_ohlcv(self, symbol: str, timeframe: str, since_ms: int | None, limit: int):
        if self.calls is not None:
            self.calls.append((symbol, timeframe, since_ms, limit))
        if self.error:
            raise self.error
        if not self.batches:
            return []
        return self.batches.pop(0)

    def fetch_funding(self, symbol: str, since_ms: int | None, limit: int):
        return []

    def fetch_open_interest(self, symbol: str, since_ms: int | None, limit: int):
        return []

    def provider_health(self):
        raise NotImplementedError


def _candle(ts_open_ms: int, trace_id: str = "t") -> CandleDTO:
    return CandleDTO(
        provider_id="ccxt",
        venue="binance",
        symbol="BTC/USDT",
        timeframe="1m",
        ts_open_ms=ts_open_ms,
        ts_close_ms=ts_open_ms + 60_000,
        open="100",
        high="101",
        low="99",
        close="100.5",
        volume="10",
        dataset_version="v1",
        trace_id=trace_id,
    )


def test_boundary_close_ingest_success_updates_checkpoint(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(batches=[[ _candle(120_000) ]])
    scheduler = MarketDataScheduler(conn, provider)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")
    conn.execute(
        """
        INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("ccxt", "binance", "BTC/USDT", "1m", 60_000, 60_500, 60_500, 0, "ok", None, "seed"),
    )

    out = scheduler.run_cycle(key, now_ms=185_000, trace_id="trace-1")

    row = conn.execute("SELECT last_ts_open_ms, state, failure_count FROM feed_checkpoints").fetchone()
    assert row == (120_000, "ok", 0)
    assert out["state"] == "ok"
    assert out["inserted"] == 1


def test_incremental_cycle_fetches_after_checkpoint_boundary(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    calls: list[tuple[str, str, int | None, int]] = []
    provider = StubProvider(batches=[[_candle(180_000)]], calls=calls)
    scheduler = MarketDataScheduler(conn, provider)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")
    conn.execute(
        """
        INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("ccxt", "binance", "BTC/USDT", "1m", 120_000, 120_500, 120_500, 0, "ok", None, "seed"),
    )

    out = scheduler.run_cycle(key, now_ms=245_000, trace_id="trace-boundary")

    assert calls == [("BTC/USDT", "1m", 180_000, 1000)]
    assert out["state"] == "ok"
    assert out["inserted"] == 1


def test_incremental_cycle_accepts_multi_candle_catchup_after_checkpoint_gap(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(batches=[[_candle(180_000), _candle(240_000), _candle(300_000)]])
    scheduler = MarketDataScheduler(conn, provider)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")
    conn.execute(
        """
        INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("ccxt", "binance", "BTC/USDT", "1m", 120_000, 120_500, 120_500, 0, "ok", None, "seed"),
    )

    out = scheduler.run_cycle(key, now_ms=365_000, trace_id="trace-catchup")

    row = conn.execute("SELECT last_ts_open_ms, state, failure_count FROM feed_checkpoints").fetchone()
    assert row == (300_000, "ok", 0)
    assert out["state"] == "ok"
    assert out["inserted"] == 3


def test_incremental_cycle_ignores_not_yet_closed_candles(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(batches=[[_candle(180_000), _candle(240_000)]])
    scheduler = MarketDataScheduler(conn, provider)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")
    conn.execute(
        """
        INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("ccxt", "binance", "BTC/USDT", "1m", 120_000, 120_500, 120_500, 0, "ok", None, "seed"),
    )

    out = scheduler.run_cycle(key, now_ms=245_000, trace_id="trace-closed-only")

    assert out["state"] == "ok"
    assert out["inserted"] == 1
    rows = conn.execute("SELECT ts_open_ms FROM market_candles ORDER BY ts_open_ms").fetchall()
    assert rows == [(180_000,)]


def test_cycle_clears_transient_error_when_checkpoint_is_current(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(batches=[])
    scheduler = MarketDataScheduler(conn, provider)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")
    conn.execute(
        """
        INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("ccxt", "binance", "BTC/USDT", "1m", 180_000, 120_500, 120_500, 3, "tripped", "PROVIDER_UPSTREAM_ERROR", "seed"),
    )

    out = scheduler.run_cycle(key, now_ms=230_000, trace_id="trace-current-after-trip")

    row = conn.execute("SELECT last_ts_open_ms, state, failure_count, last_reason_code FROM feed_checkpoints").fetchone()
    assert row == (180_000, "ok", 0, None)
    assert out["state"] == "ok"


def test_startup_bootstrap_backfill_persists_checkpoint(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(batches=[[ _candle(60_000), _candle(120_000) ], []])
    scheduler = MarketDataScheduler(conn, provider, max_backfill_bars=10, backfill_page_limit=10)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")

    out = scheduler.run_cycle(key, now_ms=250_000, trace_id="trace-2")

    cp = conn.execute("SELECT last_ts_open_ms, state FROM feed_checkpoints").fetchone()
    assert cp == (120_000, "ok")
    assert out["state"] == "ok"


def test_backfill_continues_when_provider_returns_partial_pages_but_progresses(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(
        batches=[
            [_candle(60_000), _candle(120_000)],
            [_candle(180_000), _candle(240_000)],
            [_candle(300_000)],
            [],
        ]
    )
    scheduler = MarketDataScheduler(conn, provider, max_backfill_bars=10, backfill_page_limit=10)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")

    out = scheduler.run_cycle(key, now_ms=365_000, trace_id="trace-pages")

    cp = conn.execute("SELECT last_ts_open_ms, state FROM feed_checkpoints").fetchone()
    assert cp == (300_000, "ok")
    assert out["state"] == "ok"


def test_gap_detection_marks_resync_required(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(batches=[[ _candle(60_000), _candle(300_000) ]])
    scheduler = MarketDataScheduler(conn, provider)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")
    conn.execute(
        """
        INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("ccxt", "binance", "BTC/USDT", "1m", 60_000, 60_500, 60_500, 0, "ok", None, "seed"),
    )

    out = scheduler.run_cycle(key, now_ms=400_000, trace_id="trace-3")

    cp = conn.execute("SELECT state, last_reason_code FROM feed_checkpoints").fetchone()
    assert cp == ("resync_required", "CANDLE_GAP_DETECTED")
    assert out["state"] == "resync_required"


def test_stale_window_sets_degraded_state(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(batches=[[ _candle(60_000), _candle(120_000) ]])
    scheduler = MarketDataScheduler(conn, provider)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")

    out = scheduler.run_cycle(key, now_ms=500_000, trace_id="trace-stale")

    cp = conn.execute("SELECT state, last_reason_code FROM feed_checkpoints").fetchone()
    assert cp == ("degraded", "CANDLE_STALE_WINDOW")
    assert out["state"] == "degraded"
    assert out["reason_codes"] == ["CANDLE_STALE_WINDOW"]


def test_quality_dedupe_reason_code_is_preserved_on_success(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(batches=[[ _candle(180_000), _candle(180_000) ]])
    scheduler = MarketDataScheduler(conn, provider)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")
    conn.execute(
        """
        INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("ccxt", "binance", "BTC/USDT", "1m", 120_000, 120_500, 120_500, 0, "ok", None, "seed"),
    )

    out = scheduler.run_cycle(key, now_ms=250_000, trace_id="trace-dedupe")

    assert out["state"] == "ok"
    assert out["reason_codes"] == ["CANDLE_DEDUPED"]


def test_rate_limited_sets_degraded_state(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(batches=[], error=ProviderRateLimitError("429"))
    scheduler = MarketDataScheduler(conn, provider)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")

    out = scheduler.run_cycle(key, now_ms=200_000, trace_id="trace-4")

    cp = conn.execute("SELECT state, last_reason_code, failure_count FROM feed_checkpoints").fetchone()
    assert cp == ("degraded", "PROVIDER_RATE_LIMITED", 1)
    assert out["state"] == "degraded"


def test_rate_budget_exceeded_sets_degraded_state(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(batches=[[ _candle(120_000) ]])
    budget = RateBudgetManager(max_tokens=1, interval_ms=60_000)
    scheduler = MarketDataScheduler(conn, provider, rate_budget=budget)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")
    assert budget.consume("ccxt", "binance", 200_000) is True

    out = scheduler.run_cycle(key, now_ms=200_000, trace_id="trace-budget")

    assert out["state"] == "degraded"
    assert out["reason_codes"] == ["PROVIDER_RATE_LIMITED"]


def test_feed_health_events_logged(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(batches=[], error=ProviderRateLimitError("429"))
    scheduler = MarketDataScheduler(conn, provider)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")

    scheduler.run_cycle(key, now_ms=200_000, trace_id="trace-health")

    row = conn.execute("SELECT state, reason_codes_json, trace_id FROM feed_health_events ORDER BY id DESC LIMIT 1").fetchone()
    assert row[0] == "degraded"
    assert "PROVIDER_RATE_LIMITED" in row[1]
    assert row[2] == "trace-health"


def test_breaker_trip_and_cooldown_probe_recovery(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = StubProvider(batches=[], error=ProviderUpstreamError("5xx"))
    breaker = CircuitBreaker(threshold=2, window_ms=120_000, cooldown_ms=10_000)
    scheduler = MarketDataScheduler(conn, provider, breaker=breaker)
    key = SchedulerKey("ccxt", "binance", "BTC/USDT", "1m")

    out1 = scheduler.run_cycle(key, now_ms=100_000, trace_id="trace-5a")
    out2 = scheduler.run_cycle(key, now_ms=101_000, trace_id="trace-5b")
    out3 = scheduler.run_cycle(key, now_ms=105_000, trace_id="trace-5c")
    assert out1["state"] == "degraded"
    assert out2["state"] == "tripped"
    assert out3["state"] == "tripped"

    provider.error = None
    provider.batches = [[_candle(0)]]
    out4 = scheduler.run_cycle(key, now_ms=112_000, trace_id="trace-5d")
    cp = conn.execute("SELECT state FROM feed_checkpoints").fetchone()[0]
    assert out4["state"] == "ok"
    assert cp == "ok"
