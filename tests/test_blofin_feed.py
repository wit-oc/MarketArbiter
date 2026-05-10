from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

from market_arbiter.core.db import init_db
from market_arbiter.core.market_data import CandleDTO
from market_arbiter.feed import (
    BLOFIN_FIREWALL_TEMP_BAN_SECONDS,
    HISTORICAL_REPAIR_POLICY_CONTRACT,
    BlofinEnvironment,
    BlofinFirewallBanError,
    BlofinGapRecoveryEngine,
    BlofinPublicRestClient,
    BlofinRecoveryBlockedError,
    BlofinRateLimitError,
    BlofinWsCandle5mIngestor,
    RecoveryMode,
    build_history_plan,
    determine_recovery_mode,
    parse_ws_candle_5m_payload,
)
from market_arbiter.ops.blofin_ws_candle5m_consumer import (
    BlofinWsCandle5mConsumerConfig,
    BlofinWsReconnectPolicy,
    BlofinWsReconnectState,
    collect_status,
    run_confirm_canary,
    run_live,
)


@dataclass
class FakeResponse:
    payload: dict
    status_code: int = 200
    text: str = ""

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payloads: list[dict]):
        self.payloads = payloads
        self.headers = {}
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict, timeout: float):
        self.calls.append((url, dict(params)))
        payload = self.payloads.pop(0)
        return FakeResponse(payload)

    def close(self):
        return None


class StubBlofinClient:
    def __init__(self):
        self.calls: list[tuple[str, str, int]] = []

    def iter_history(self, *, inst_id: str, bar: str, max_candles: int, trace_id: str | None = None):
        self.calls.append((inst_id, bar, max_candles))
        timeframe = {"1W": "1w", "1D": "1d", "4H": "4h", "5m": "5m"}[bar]
        step = {"1w": 604_800_000, "1d": 86_400_000, "4h": 14_400_000, "5m": 300_000}[timeframe]
        for idx in range(min(3, max_candles)):
            yield CandleDTO(
                provider_id="blofin_rest",
                venue="blofin",
                symbol=inst_id,
                timeframe=timeframe,
                ts_open_ms=1_700_000_000_000 + (idx * step),
                ts_close_ms=1_700_000_000_000 + ((idx + 1) * step),
                open="100",
                high="101",
                low="99",
                close="100.5",
                volume="10",
                dataset_version=f"blofin_rest_{timeframe}_v1",
                trace_id=trace_id or "trace",
            )

    def close(self):
        return None


class FlakyBlofinClient(StubBlofinClient):
    def __init__(self, failures: list[Exception]):
        super().__init__()
        self.failures = list(failures)

    def iter_history(self, *, inst_id: str, bar: str, max_candles: int, trace_id: str | None = None):
        self.calls.append((inst_id, bar, max_candles))
        if self.failures:
            raise self.failures.pop(0)
        yield from super().iter_history(inst_id=inst_id, bar=bar, max_candles=max_candles, trace_id=trace_id)


class InvalidHistoricalBlofinClient(StubBlofinClient):
    def __init__(self, *, invalid_bars: set[str]):
        super().__init__()
        self.invalid_bars = {str(bar) for bar in invalid_bars}

    def iter_history(self, *, inst_id: str, bar: str, max_candles: int, trace_id: str | None = None):
        self.calls.append((inst_id, bar, max_candles))
        timeframe = {"1W": "1w", "1D": "1d", "4H": "4h", "5m": "5m"}[bar]
        step = {"1w": 604_800_000, "1d": 86_400_000, "4h": 14_400_000, "5m": 300_000}[timeframe]
        yield CandleDTO(
            provider_id="blofin_rest",
            venue="blofin",
            symbol=inst_id,
            timeframe=timeframe,
            ts_open_ms=1_700_000_000_000,
            ts_close_ms=1_700_000_000_000 + step,
            open="100",
            high="101",
            low="99",
            close="100.5",
            volume="10",
            dataset_version=f"blofin_rest_{timeframe}_v1",
            trace_id=trace_id or "trace",
        )
        if bar in self.invalid_bars:
            yield CandleDTO(
                provider_id="blofin_rest",
                venue="blofin",
                symbol=inst_id,
                timeframe=timeframe,
                ts_open_ms=1_700_000_000_000 + step,
                ts_close_ms=1_700_000_000_000 + (2 * step),
                open="95",
                high="110",
                low="99",
                close="105",
                volume="11",
                dataset_version=f"blofin_rest_{timeframe}_v1",
                trace_id=trace_id or "trace",
            )


class UnrepairableHistoricalBlofinClient(StubBlofinClient):
    def __init__(self, *, invalid_bars: set[str]):
        super().__init__()
        self.invalid_bars = {str(bar) for bar in invalid_bars}

    def iter_history(self, *, inst_id: str, bar: str, max_candles: int, trace_id: str | None = None):
        self.calls.append((inst_id, bar, max_candles))
        timeframe = {"1W": "1w", "1D": "1d", "4H": "4h", "5m": "5m"}[bar]
        step = {"1w": 604_800_000, "1d": 86_400_000, "4h": 14_400_000, "5m": 300_000}[timeframe]
        yield CandleDTO(
            provider_id="blofin_rest",
            venue="blofin",
            symbol=inst_id,
            timeframe=timeframe,
            ts_open_ms=1_700_000_000_000,
            ts_close_ms=1_700_000_000_000 + step,
            open="100",
            high="101",
            low="99",
            close="100.5",
            volume="10",
            dataset_version=f"blofin_rest_{timeframe}_v1",
            trace_id=trace_id or "trace",
        )
        if bar in self.invalid_bars:
            yield CandleDTO(
                provider_id="blofin_rest",
                venue="blofin",
                symbol=inst_id,
                timeframe=timeframe,
                ts_open_ms=1_700_000_000_000 + step,
                ts_close_ms=1_700_000_000_000 + (2 * step),
                open="oops",
                high="110",
                low="99",
                close="105",
                volume="11",
                dataset_version=f"blofin_rest_{timeframe}_v1",
                trace_id=trace_id or "trace",
            )


class FakeWsClient:
    def __init__(self, payloads: list[dict]):
        self.payloads = payloads
        self.connected = False
        self.subscribed_symbols: list[str] = []
        self.closed = False

    async def connect(self):
        self.connected = True

    async def subscribe_candle_5m(self, symbols):
        self.subscribed_symbols = list(symbols)

    async def recv(self):
        return self.payloads.pop(0)

    async def close(self):
        self.closed = True


class ErrorWsClient(FakeWsClient):
    def __init__(self, error: Exception, *, diagnostics: dict | None = None):
        super().__init__(payloads=[])
        self.error = error
        self.diagnostics = diagnostics or {
            "url": "wss://example.invalid/ws/public",
            "environment": "test",
            "last_message_type": "WSMsgType.CLOSED",
            "close_code": 1006,
            "ws_exception": str(error),
        }

    async def recv(self):
        raise self.error

    def diagnostic_state(self):
        return dict(self.diagnostics)


class BlockingWsClient(FakeWsClient):
    def __init__(self):
        super().__init__(payloads=[])

    async def recv(self):
        await asyncio.sleep(3600)


class ControlThenBlockingWsClient(FakeWsClient):
    def __init__(self, payloads: list[dict]):
        super().__init__(payloads=payloads)

    async def recv(self):
        if self.payloads:
            return self.payloads.pop(0)
        await asyncio.sleep(3600)


def test_environment_defaults_to_demo_safe_endpoints():
    assert BlofinEnvironment.DEMO.rest_base_url == "https://demo-trading-openapi.blofin.com"
    assert BlofinEnvironment.DEMO.ws_public_url == "wss://demo-trading-openapi.blofin.com/ws/public"
    assert BlofinEnvironment.PROD.rest_base_url == "https://openapi.blofin.com"


def test_fetch_candles_page_uses_safe_limit_and_normalizes_rows():
    session = FakeSession(
        payloads=[
            {
                "code": "0",
                "msg": "success",
                "data": [
                    [1700000300000, "101", "102", "100", "101.5", "11"],
                    [1700000000000, "100", "101", "99", "100.5", "10"],
                ],
            }
        ]
    )
    client = BlofinPublicRestClient(session=session)

    page = client.fetch_candles_page(inst_id="BTC-USDT", bar="5m", limit=5000, trace_id="trace-1")

    assert len(page.candles) == 2
    assert page.candles[0].timeframe == "5m"
    assert session.calls[0][1]["limit"] == 500


def test_ws_candle5m_payload_parses_confirm_state():
    payload = {
        "arg": {"channel": "candle5m", "instId": "BTC-USDT"},
        "data": [
            [1_700_000_000_000, "100", "101", "99", "100.5", "10", "0.1", "1000", "0"],
            [1_700_000_300_000, "101", "102", "100", "101.5", "11", "0.2", "1100", "1"],
        ],
    }

    rows = parse_ws_candle_5m_payload(payload, received_ts_ms=1_700_000_600_000, trace_id="trace-ws")

    assert [row.confirm for row in rows] == ["0", "1"]
    assert rows[1].is_confirmed is True
    assert rows[1].to_candle_dto().provider_id == "blofin_ws"
    assert rows[1].to_candle_dto().dataset_version == "blofin_5m_v1"


def test_ws_candle5m_payload_accepts_single_object_data():
    payload = {
        "arg": {"channel": "candle5m", "instId": "BTC-USDT"},
        "data": {
            "ts": 1_700_000_300_000,
            "open": "101",
            "high": "102",
            "low": "100",
            "close": "101.5",
            "vol": "11",
            "volCurrency": "0.2",
            "volCurrencyQuote": "1100",
            "confirm": "1",
        },
    }

    rows = parse_ws_candle_5m_payload(payload, received_ts_ms=1_700_000_600_000, trace_id="trace-ws")

    assert len(rows) == 1
    assert rows[0].is_confirmed is True
    assert rows[0].to_candle_dto().ts_open_ms == 1_700_000_300_000


def test_ws_candle5m_ingestor_only_persists_confirmed_rows(tmp_path):
    conn = init_db(str(tmp_path / "market_arbiter.sqlite"))
    ingestor = BlofinWsCandle5mIngestor(conn)
    payload = {
        "arg": {"channel": "candle5m", "instId": "BTC-USDT"},
        "data": [
            [1_700_000_000_000, "100", "101", "99", "100.5", "10", "0.1", "1000", "0"],
            [1_700_000_300_000, "101", "102", "100", "101.5", "11", "0.2", "1100", "1"],
        ],
    }

    result = ingestor.ingest_payload(payload, received_ts_ms=1_700_000_600_000, trace_id="trace-ws")

    assert result["received"] == 2
    assert result["confirmed"] == 1
    assert result["inserted"] == 1
    candle = conn.execute(
        """
        SELECT provider_id, venue, symbol, timeframe, ts_open_ms, volume, dataset_version, trace_id
        FROM market_candles;
        """
    ).fetchone()
    assert candle == ("blofin_ws", "blofin", "BTC-USDT", "5m", 1_700_000_300_000, "11", "blofin_5m_v1", "trace-ws")
    checkpoint = conn.execute(
        "SELECT provider_id, last_ts_open_ms, state FROM feed_checkpoints WHERE symbol = ?;",
        ("BTC-USDT",),
    ).fetchone()
    assert checkpoint == ("blofin_ws", 1_700_000_300_000, "ok")
    health = conn.execute("SELECT provider_id, state, reason_codes_json FROM feed_health_events;").fetchone()
    assert health == ("blofin_ws", "ok", '["ws_confirmed_candle"]')
    conn.close()


def test_rest_client_raises_429_as_rate_limit_and_403_as_firewall_restriction():
    rate_session = FakeSession(payloads=[{"code": "0", "data": []}])
    rate_session.payloads = []
    rate_session.get = lambda url, params, timeout: FakeResponse({"code": "429"}, status_code=429, text="too many requests")
    rate_client = BlofinPublicRestClient(session=rate_session, sleep_fn=lambda _seconds: None)

    firewall_session = FakeSession(payloads=[{"code": "0", "data": []}])
    firewall_session.payloads = []
    firewall_session.get = lambda url, params, timeout: FakeResponse({"code": "403"}, status_code=403, text="forbidden")
    firewall_client = BlofinPublicRestClient(session=firewall_session, sleep_fn=lambda _seconds: None)

    try:
        rate_client.fetch_instruments()
        assert False, "expected rate limit"
    except BlofinRateLimitError:
        pass

    try:
        firewall_client.fetch_instruments()
        assert False, "expected firewall restriction"
    except BlofinFirewallBanError:
        pass

    assert firewall_client.governor.firewall_ban_backoff_seconds == BLOFIN_FIREWALL_TEMP_BAN_SECONDS


def test_determine_recovery_mode_respects_five_day_horizon():
    now_ms = 1_800_000_000_000
    assert determine_recovery_mode(last_closed_ts_open_ms=None, now_ms=now_ms) is RecoveryMode.CLEAN_STARTUP
    assert determine_recovery_mode(last_closed_ts_open_ms=now_ms - (2 * 24 * 60 * 60 * 1000), now_ms=now_ms) is RecoveryMode.REPAIR
    assert determine_recovery_mode(last_closed_ts_open_ms=now_ms - (6 * 24 * 60 * 60 * 1000), now_ms=now_ms) is RecoveryMode.RESEED


def test_build_history_plan_matches_contract():
    repair = build_history_plan(symbol="BTC-USDT", mode=RecoveryMode.REPAIR)
    reseed = build_history_plan(symbol="BTC-USDT", mode=RecoveryMode.RESEED)

    assert [(row.timeframe, row.max_candles) for row in repair] == [("5m", 1440)]
    assert [(row.timeframe, row.max_candles) for row in reseed] == [
        ("1w", 520),
        ("1d", 1440),
        ("4h", 1080),
        ("5m", 1440),
    ]


def test_gap_recovery_engine_loads_expected_timeframes(tmp_path):
    conn = init_db(str(tmp_path / "market_arbiter.sqlite"))
    client = StubBlofinClient()
    engine = BlofinGapRecoveryEngine(conn, client)

    out = engine.recover_symbol(symbol="BTC-USDT", now_ms=1_800_000_000_000, trace_id="trace-2", last_closed_ts_open_ms=None)

    assert out["mode"] == "clean_startup"
    assert [row[1] for row in client.calls] == ["1W", "1D", "4H", "5m"]
    counts = conn.execute("SELECT timeframe, COUNT(*) FROM market_candles GROUP BY timeframe ORDER BY timeframe").fetchall()
    assert counts == [("1d", 3), ("1w", 3), ("4h", 3), ("5m", 3)]
    checkpoint_states = conn.execute("SELECT timeframe, state FROM feed_checkpoints ORDER BY timeframe").fetchall()
    assert checkpoint_states == [("1d", "ok"), ("1w", "ok"), ("4h", "ok"), ("5m", "ok")]
    conn.close()


def test_gap_recovery_engine_repair_only_reloads_5m(tmp_path):
    conn = init_db(str(tmp_path / "market_arbiter.sqlite"))
    client = StubBlofinClient()
    engine = BlofinGapRecoveryEngine(conn, client)

    out = engine.recover_symbol(
        symbol="BTC-USDT",
        now_ms=1_800_000_000_000,
        trace_id="trace-3",
        last_closed_ts_open_ms=1_800_000_000_000 - (2 * 24 * 60 * 60 * 1000),
    )

    assert out["mode"] == "repair"
    assert client.calls == [("BTC-USDT", "5m", 1440)]
    counts = conn.execute("SELECT timeframe, COUNT(*) FROM market_candles GROUP BY timeframe ORDER BY timeframe").fetchall()
    assert counts == [("5m", 3)]
    conn.close()


def test_gap_recovery_engine_repairs_range_invalid_history_and_surfaces_quality_band(tmp_path):
    conn = init_db(str(tmp_path / "market_arbiter.sqlite"))
    client = InvalidHistoricalBlofinClient(invalid_bars={"1W", "5m"})
    engine = BlofinGapRecoveryEngine(conn, client)

    out = engine.recover_symbol(symbol="BTC-USDT", now_ms=1_800_000_000_000, trace_id="trace-repair", last_closed_ts_open_ms=None)

    assert out["status"] == "ok"
    per_tf = {row["timeframe"]: row for row in out["results"]}
    assert per_tf["1w"]["state"] == "ok"
    assert per_tf["1w"]["repaired_candles"] == 1
    assert per_tf["1w"]["quality_band"] == "benign"
    assert per_tf["5m"]["state"] == "ok"
    assert per_tf["5m"]["repair_summary"]["repair_policy_contract"] == HISTORICAL_REPAIR_POLICY_CONTRACT
    assert per_tf["5m"]["repair_summary"]["repaired_candles"] == 1
    assert per_tf["5m"]["circuit_breaker_action"] == "accept"

    counts = conn.execute("SELECT timeframe, COUNT(*) FROM market_candles GROUP BY timeframe ORDER BY timeframe").fetchall()
    assert counts == [("1d", 1), ("1w", 2), ("4h", 1), ("5m", 2)]
    checkpoints = conn.execute(
        "SELECT timeframe, state, last_reason_code FROM feed_checkpoints WHERE provider_id = 'blofin_rest' ORDER BY timeframe"
    ).fetchall()
    assert checkpoints == [
        ("1d", "ok", None),
        ("1w", "ok", "HISTORICAL_REPAIR_BENIGN"),
        ("4h", "ok", None),
        ("5m", "ok", "HISTORICAL_REPAIR_BENIGN"),
    ]
    health = conn.execute(
        "SELECT timeframe, state, reason_codes_json, metadata_json FROM feed_health_events WHERE provider_id = 'blofin_rest' ORDER BY id"
    ).fetchall()
    repaired_1w = next(row for row in health if row[0] == "1w")
    assert repaired_1w[1] == "ok"
    assert repaired_1w[2] == '["CANDLE_RANGE_REPAIRED", "HISTORICAL_REPAIR_BENIGN"]'
    assert '"quality_band": "benign"' in (repaired_1w[3] or "")
    conn.close()


def test_gap_recovery_engine_quarantines_unrepairable_noncanonical_history_and_marks_degraded(tmp_path):
    conn = init_db(str(tmp_path / "market_arbiter.sqlite"))
    client = UnrepairableHistoricalBlofinClient(invalid_bars={"1W", "1D"})
    engine = BlofinGapRecoveryEngine(conn, client)

    out = engine.recover_symbol(symbol="BTC-USDT", now_ms=1_800_000_000_000, trace_id="trace-degraded", last_closed_ts_open_ms=None)

    assert out["status"] == "ok"
    per_tf = {row["timeframe"]: row for row in out["results"]}
    assert per_tf["1w"]["state"] == "degraded"
    assert per_tf["1w"]["invalid_candles"] == 1
    assert per_tf["1w"]["quality_band"] == "degraded"
    assert per_tf["1d"]["state"] == "degraded"
    assert per_tf["5m"]["state"] == "ok"

    counts = conn.execute("SELECT timeframe, COUNT(*) FROM market_candles GROUP BY timeframe ORDER BY timeframe").fetchall()
    assert counts == [("1d", 1), ("1w", 1), ("4h", 1), ("5m", 1)]
    checkpoints = conn.execute(
        "SELECT timeframe, state, last_reason_code FROM feed_checkpoints WHERE provider_id = 'blofin_rest' ORDER BY timeframe"
    ).fetchall()
    assert checkpoints == [
        ("1d", "degraded", "HISTORICAL_REPAIR_DEGRADED"),
        ("1w", "degraded", "HISTORICAL_REPAIR_DEGRADED"),
        ("4h", "ok", None),
        ("5m", "ok", None),
    ]
    conn.close()


def test_gap_recovery_engine_blocks_when_canonical_5m_contains_invalid_history(tmp_path):
    conn = init_db(str(tmp_path / "market_arbiter.sqlite"))
    client = UnrepairableHistoricalBlofinClient(invalid_bars={"5m"})
    engine = BlofinGapRecoveryEngine(conn, client)

    out = engine.recover_symbol(symbol="BTC-USDT", now_ms=1_800_000_000_000, trace_id="trace-blocked", last_closed_ts_open_ms=None)

    assert out["status"] == "blocked"
    assert out["blocked_reason_codes"] == ["CANDLE_VALUE_INVALID", "HISTORICAL_REPAIR_BLOCKED"]
    health = conn.execute(
        "SELECT timeframe, state, reason_codes_json, metadata_json FROM feed_health_events WHERE provider_id = 'blofin_rest' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert health[0:3] == ("5m", "blocked", '["recovery_mode:clean_startup", "CANDLE_VALUE_INVALID", "HISTORICAL_REPAIR_BLOCKED"]')
    assert '"symbol_status": "blocked"' in (health[3] or "")
    conn.close()


def test_live_ws_consumer_recovers_before_subscribe_and_ingests_confirmed_candle(tmp_path):
    config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        symbols=["BTC-USDT"],
    )
    rest_client = StubBlofinClient()
    ws_payload = {
        "arg": {"channel": "candle5m", "instId": "BTC-USDT"},
        "data": [[1_700_001_000_000, "110", "111", "109", "110.5", "12", "0.3", "1200", "1"]],
    }
    ws_client = FakeWsClient([ws_payload])

    result = asyncio.run(
        run_live(
            config,
            rest_client=rest_client,
            ws_client=ws_client,
            now_fn=lambda: 1_800_000_000_000,
            max_messages=1,
        )
    )

    assert result["environment"] == "demo"
    assert [row[1] for row in rest_client.calls] == ["1W", "1D", "4H", "5m"]
    assert ws_client.connected is True
    assert ws_client.subscribed_symbols == ["BTC-USDT"]
    assert result["messages_seen"] == 1

    conn = init_db(config.db_path)
    try:
        ws_count = conn.execute("SELECT COUNT(*) FROM market_candles WHERE provider_id = 'blofin_ws';").fetchone()[0]
        ws_checkpoint = conn.execute(
            "SELECT last_ts_open_ms, state FROM feed_checkpoints WHERE provider_id = 'blofin_ws' AND symbol = 'BTC-USDT';"
        ).fetchone()
    finally:
        conn.close()
    assert ws_count == 1
    assert ws_checkpoint == (1_700_001_000_000, "ok")


def test_live_ws_consumer_allows_ws_attach_when_only_higher_timeframe_history_is_invalid(tmp_path):
    config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        symbols=["BTC-USDT"],
    )
    rest_client = UnrepairableHistoricalBlofinClient(invalid_bars={"1W", "1D"})
    ws_payload = {
        "arg": {"channel": "candle5m", "instId": "BTC-USDT"},
        "data": [[1_700_001_000_000, "110", "111", "109", "110.5", "12", "0.3", "1200", "1"]],
    }
    ws_client = FakeWsClient([ws_payload])

    result = asyncio.run(
        run_live(
            config,
            rest_client=rest_client,
            ws_client=ws_client,
            now_fn=lambda: 1_800_000_000_000,
            max_messages=1,
        )
    )

    assert result["messages_seen"] == 1
    assert result["startup_recoveries"][0]["status"] == "ok"
    degraded = {row["timeframe"]: row["state"] for row in result["startup_recoveries"][0]["results"]}
    assert degraded["1w"] == "degraded"
    assert degraded["1d"] == "degraded"
    assert degraded["5m"] == "ok"


def test_live_ws_consumer_blocks_when_canonical_5m_history_is_invalid(tmp_path):
    config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        symbols=["BTC-USDT"],
    )
    rest_client = UnrepairableHistoricalBlofinClient(invalid_bars={"5m"})

    try:
        asyncio.run(
            run_live(
                config,
                rest_client=rest_client,
                ws_client=BlockingWsClient(),
                now_fn=lambda: 1_800_000_000_000,
                max_reconnects=0,
            )
        )
        assert False, "expected recovery to block"
    except BlofinRecoveryBlockedError:
        pass


def test_live_ws_consumer_reconnects_with_backoff_and_repairs_before_resubscribe(tmp_path):
    config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        symbols=["BTC-USDT"],
    )
    rest_client = StubBlofinClient()
    ws_clients = [
        ErrorWsClient(RuntimeError("websocket closed")),
        FakeWsClient(
            [
                {
                    "arg": {"channel": "candle5m", "instId": "BTC-USDT"},
                    "data": [[1_700_000_900_000, "110", "111", "109", "110.5", "12", "0.3", "1200", "1"]],
                }
            ]
        ),
    ]
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float):
        sleep_calls.append(seconds)

    result = asyncio.run(
        run_live(
            config,
            rest_client=rest_client,
            ws_client_factory=lambda: ws_clients.pop(0),
            now_fn=lambda: 1_700_001_200_000,
            sleep_fn=fake_sleep,
            random_fn=lambda lower, upper: upper,
            reconnect_policy=BlofinWsReconnectPolicy(
                base_delay_seconds=7.0,
                max_delay_seconds=30.0,
                jitter_ratio=0.0,
                rapid_failure_threshold=10,
            ),
            max_messages=1,
            max_reconnects=1,
        )
    )

    assert sleep_calls == [7.0]
    assert [row[1] for row in rest_client.calls] == ["1W", "1D", "4H", "5m", "5m"]
    assert result["reconnect_attempts"] == 1
    assert result["disconnect_events"][0]["reason_code"] == "WS_DISCONNECTED"
    assert result["disconnect_events"][0]["exception_type"] == "RuntimeError"
    assert result["disconnect_events"][0]["message"] == "websocket closed"
    assert result["disconnect_events"][0]["ws_diagnostics"]["close_code"] == 1006

    status = collect_status(config, now_fn=lambda: 1_700_001_200_000)
    assert status["latest_disconnect_event"]["message"] == "websocket closed"
    assert status["disconnect_events_recent"][0]["ws_diagnostics"]["close_code"] == 1006


def test_live_ws_consumer_rapid_failures_trigger_cooldown(tmp_path):
    config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        symbols=["BTC-USDT"],
    )
    rest_client = StubBlofinClient()
    ws_clients = [
        ErrorWsClient(RuntimeError("ws closed 1")),
        ErrorWsClient(RuntimeError("ws closed 2")),
        FakeWsClient(
            [
                {
                    "arg": {"channel": "candle5m", "instId": "BTC-USDT"},
                    "data": [[1_700_000_900_000, "110", "111", "109", "110.5", "12", "0.3", "1200", "1"]],
                }
            ]
        ),
    ]
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float):
        sleep_calls.append(seconds)

    result = asyncio.run(
        run_live(
            config,
            rest_client=rest_client,
            ws_client_factory=lambda: ws_clients.pop(0),
            now_fn=lambda: 1_700_001_200_000,
            sleep_fn=fake_sleep,
            random_fn=lambda lower, upper: upper,
            reconnect_policy=BlofinWsReconnectPolicy(
                base_delay_seconds=5.0,
                max_delay_seconds=30.0,
                jitter_ratio=0.0,
                rapid_failure_threshold=2,
                rapid_failure_cooldown_seconds=45.0,
            ),
            max_messages=1,
            max_reconnects=2,
        )
    )

    assert sleep_calls == [5.0, 45.0]
    assert result["reconnect_attempts"] == 2


def test_live_ws_consumer_rate_limit_failure_uses_safer_backoff_floor(tmp_path):
    config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        symbols=["BTC-USDT"],
    )
    rest_client = FlakyBlofinClient([BlofinRateLimitError("too many requests")])
    ws_client = FakeWsClient(
        [
            {
                "arg": {"channel": "candle5m", "instId": "BTC-USDT"},
                "data": [[1_700_000_900_000, "110", "111", "109", "110.5", "12", "0.3", "1200", "1"]],
            }
        ]
    )
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float):
        sleep_calls.append(seconds)

    result = asyncio.run(
        run_live(
            config,
            rest_client=rest_client,
            ws_client=ws_client,
            now_fn=lambda: 1_700_001_200_000,
            sleep_fn=fake_sleep,
            random_fn=lambda lower, upper: upper,
            reconnect_policy=BlofinWsReconnectPolicy(
                base_delay_seconds=5.0,
                max_delay_seconds=30.0,
                jitter_ratio=0.0,
                rapid_failure_threshold=10,
                rate_limit_delay_seconds=90.0,
            ),
            max_messages=1,
            max_reconnects=1,
        )
    )

    assert sleep_calls == [90.0]
    assert result["disconnect_events"][0]["reason_code"] == "REST_RATE_LIMIT"


def test_live_ws_consumer_firewall_ban_failure_uses_firewall_backoff_floor(tmp_path):
    config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        symbols=["BTC-USDT"],
    )
    rest_client = FlakyBlofinClient([BlofinFirewallBanError("temporary firewall restriction")])
    ws_client = FakeWsClient(
        [
            {
                "arg": {"channel": "candle5m", "instId": "BTC-USDT"},
                "data": [[1_700_000_900_000, "110", "111", "109", "110.5", "12", "0.3", "1200", "1"]],
            }
        ]
    )
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float):
        sleep_calls.append(seconds)

    result = asyncio.run(
        run_live(
            config,
            rest_client=rest_client,
            ws_client=ws_client,
            now_fn=lambda: 1_700_001_200_000,
            sleep_fn=fake_sleep,
            random_fn=lambda lower, upper: upper,
            reconnect_policy=BlofinWsReconnectPolicy(
                base_delay_seconds=5.0,
                max_delay_seconds=30.0,
                jitter_ratio=0.0,
                rapid_failure_threshold=10,
                firewall_ban_delay_seconds=300.0,
            ),
            max_messages=1,
            max_reconnects=1,
        )
    )

    assert sleep_calls == [300.0]
    assert result["disconnect_events"][0]["reason_code"] == "REST_FIREWALL_BAN"


def test_reconnect_state_jitter_bounds_are_explicit_and_bounded():
    state = BlofinWsReconnectState()
    calls: list[tuple[float, float]] = []

    def capture_upper(lower: float, upper: float) -> float:
        calls.append((lower, upper))
        return upper

    policy = BlofinWsReconnectPolicy(
        base_delay_seconds=10.0,
        max_delay_seconds=100.0,
        multiplier=2.0,
        jitter_ratio=0.2,
        rapid_failure_threshold=10,
    )

    first_delay = state.note_failure(
        now_ms=1_000,
        connected_started_ms=None,
        policy=policy,
        exc=RuntimeError("ws closed"),
        random_fn=capture_upper,
    )
    second_delay = state.note_failure(
        now_ms=2_000,
        connected_started_ms=None,
        policy=policy,
        exc=RuntimeError("ws closed again"),
        random_fn=capture_upper,
    )

    assert calls == [(10.0, 12.0), (16.0, 24.0)]
    assert first_delay == 12.0
    assert second_delay == 24.0


def test_blofin_ws_status_reports_no_state_and_stale_state(tmp_path):
    no_state_config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "missing_state.sqlite"),
        symbols=["BTC-USDT"],
        state_path=str(tmp_path / "missing_state.json"),
    )

    no_state = collect_status(no_state_config, now_fn=lambda: 2_000, stale_after_ms=500)

    assert no_state["status_source"] == "no_state"
    assert no_state["feed_state"] == "unknown"
    assert no_state["summary"]["missing_symbols"] == 1

    state_config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        symbols=["BTC-USDT"],
        state_path=str(tmp_path / "blofin_state.json"),
    )
    ws_client = FakeWsClient(
        [
            {
                "arg": {"channel": "candle5m", "instId": "BTC-USDT"},
                "data": [[1_700_000_900_000, "110", "111", "109", "110.5", "12", "0.3", "1200", "1"]],
            }
        ]
    )

    asyncio.run(
        run_live(
            state_config,
            rest_client=StubBlofinClient(),
            ws_client=ws_client,
            now_fn=lambda: 1_700_001_200_000,
            max_messages=1,
        )
    )
    fresh = collect_status(state_config, now_fn=lambda: 1_700_001_200_250, stale_after_ms=500)
    stale = collect_status(state_config, now_fn=lambda: 1_700_001_201_000, stale_after_ms=500)

    assert fresh["status_source"] == "live_state"
    assert fresh["feed_state"] == "stopped"
    assert fresh["last_successful_ingest_ms"] == 1_700_001_200_000
    assert fresh["summary"]["ok_symbols"] == 1
    assert stale["status_source"] == "stale_state"


def test_blofin_ws_status_downgrades_stale_active_state(tmp_path):
    config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        symbols=["BTC-USDT"],
        state_path=str(tmp_path / "blofin_state.json"),
    )
    with open(config.state_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "contract": "blofin_ws_candle5m_consumer_state_v1",
                "db_path": config.db_path,
                "state_path": config.state_path,
                "environment": "demo",
                "symbols": ["BTC-USDT"],
                "timeframe": "5m",
                "state": "live",
                "updated_ms": 1_000,
                "messages_seen": 2,
                "reconnect_attempts": 0,
            },
            handle,
        )

    status = collect_status(config, now_fn=lambda: 5_000, stale_after_ms=500)

    assert status["status_source"] == "stale_state"
    assert status["feed_state"] == "stale"


def test_confirm_canary_returns_timeout_and_marks_state(tmp_path):
    config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        symbols=["BTC-USDT"],
        state_path=str(tmp_path / "blofin_canary_state.json"),
    )

    result = asyncio.run(
        run_confirm_canary(
            config,
            rest_client=StubBlofinClient(),
            ws_client=BlockingWsClient(),
            now_fn=lambda: 1_700_001_200_000,
            max_runtime_seconds=0.01,
            heartbeat_timeout_seconds=30.0,
        )
    )

    status = collect_status(config, now_fn=lambda: 1_700_001_200_000)

    assert result["status"] == "timeout"
    assert result["reason"] == "WAITING_FOR_CONFIRMED_CANDLE"
    assert status["feed_state"] == "canary_timeout"
    assert status["latest_disconnect_reason"] == "WAITING_FOR_CONFIRMED_CANDLE"


def test_confirm_canary_timeout_preserves_ack_only_diagnostics(tmp_path):
    config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        symbols=["BTC-USDT"],
        state_path=str(tmp_path / "blofin_canary_state.json"),
    )

    result = asyncio.run(
        run_confirm_canary(
            config,
            rest_client=StubBlofinClient(),
            ws_client=ControlThenBlockingWsClient(
                [{"event": "subscribe", "arg": {"channel": "candle5m", "instId": "BTC-USDT"}}]
            ),
            now_fn=lambda: 1_700_001_200_000,
            max_runtime_seconds=0.01,
            heartbeat_timeout_seconds=30.0,
        )
    )

    status = collect_status(config, now_fn=lambda: 1_700_001_200_000)

    assert result["status"] == "timeout"
    assert result["messages_seen"] == 1
    assert result["control_messages"] == 1
    assert result["data_messages"] == 0
    assert result["last_payload_summary"] == {
        "event": "subscribe",
        "channel": "candle5m",
        "instId": "BTC-USDT",
        "has_data": False,
        "data_count": 0,
    }
    assert status["control_messages"] == 1
    assert status["data_messages"] == 0


def test_confirm_canary_returns_ok_after_first_confirmed_candle(tmp_path):
    config = BlofinWsCandle5mConsumerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        symbols=["BTC-USDT"],
        state_path=str(tmp_path / "blofin_canary_state.json"),
    )
    ws_client = FakeWsClient(
        [
            {
                "arg": {"channel": "candle5m", "instId": "BTC-USDT"},
                "data": [[1_700_000_900_000, "110", "111", "109", "110.5", "12", "0.3", "1200", "1"]],
            }
        ]
    )

    result = asyncio.run(
        run_confirm_canary(
            config,
            rest_client=StubBlofinClient(),
            ws_client=ws_client,
            now_fn=lambda: 1_700_001_200_000,
            max_runtime_seconds=5.0,
            required_confirmed_messages=1,
        )
    )

    status = collect_status(config, now_fn=lambda: 1_700_001_200_100)

    assert result["status"] == "ok"
    assert result["confirmed_messages"] == 1
    assert result["confirmed_inserted"] == 1
    assert status["feed_state"] == "stopped"
    assert status["confirmed_messages"] == 1
    assert status["confirmed_inserted"] == 1
