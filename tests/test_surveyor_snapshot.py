from __future__ import annotations

import time

from market_arbiter.core.db import init_db
from market_arbiter.core.market_data import CandleDTO, upsert_market_candles
from market_arbiter.core.surveyor_snapshot import build_surveyor_packet_snapshot, load_surveyor_timeframe_inputs


def _seed_candles(conn, *, symbol: str, timeframe: str, start_ms: int, step_ms: int, count: int) -> None:
    candles = []
    for idx in range(count):
        open_px = 100.0 + idx
        candles.append(
            CandleDTO(
                provider_id="ccxt",
                venue="okx",
                symbol=symbol,
                timeframe=timeframe,
                ts_open_ms=start_ms + (idx * step_ms),
                ts_close_ms=start_ms + ((idx + 1) * step_ms),
                open=str(open_px),
                high=str(open_px + 2.0),
                low=str(open_px - 1.0),
                close=str(open_px + 1.0),
                volume="10",
                dataset_version=f"test_{timeframe}",
                trace_id=f"trace:{timeframe}:{idx}",
            )
        )

    with conn:
        upsert_market_candles(conn, candles, ingest_ts_ms=start_ms + (count * step_ms))
        conn.execute(
            """
            INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider_id, venue, symbol, timeframe)
            DO UPDATE SET
                last_ts_open_ms=excluded.last_ts_open_ms,
                last_success_ms=excluded.last_success_ms,
                last_attempt_ms=excluded.last_attempt_ms,
                failure_count=excluded.failure_count,
                state=excluded.state,
                last_reason_code=excluded.last_reason_code,
                trace_id=excluded.trace_id;
            """,
            (
                "ccxt",
                "okx",
                symbol,
                timeframe,
                start_ms + ((count - 1) * step_ms),
                start_ms + (count * step_ms),
                start_ms + (count * step_ms),
                0,
                "ok",
                None,
                f"checkpoint:{timeframe}",
            ),
        )


def _authoritative_view() -> dict:
    zone = {
        "zone_id": "z1",
        "tf": "4H",
        "kind": "support",
        "current_role": "support",
        "relative_position": "above",
        "bounds": {"low": 100.0, "mid": 101.0, "high": 102.0},
    }
    return {
        "contract": "authoritative_levels_view_v1",
        "timeframes": {
            "1D": {
                "contract": "authoritative_levels_view_v1",
                "tf": "1D",
                "selector_surface": "daily_major",
                "group_perspective": "zone_relative_to_price",
                "entry": 105.0,
                "groups": {"below_price": [zone], "contains_price": [], "above_price": []},
            },
            "4H": {
                "contract": "authoritative_levels_view_v1",
                "tf": "4H",
                "selector_surface": "operational_4h",
                "group_perspective": "zone_relative_to_price",
                "entry": 105.0,
                "groups": {"below_price": [zone], "contains_price": [], "above_price": []},
            },
        },
    }


def test_load_surveyor_timeframe_inputs_prefers_market_candles(tmp_path) -> None:
    conn = init_db(str(tmp_path / "liquidsniper.sqlite"))
    now_ms = int(time.time() * 1000)
    _seed_candles(conn, symbol="BTC/USDT", timeframe="5m", start_ms=now_ms - (12 * 5 * 60 * 1000), step_ms=5 * 60 * 1000, count=12)

    payload = load_surveyor_timeframe_inputs(conn, symbol="BTCUSDT", allow_replay_fallback=False, now_ms=now_ms)
    conn.close()

    assert payload["5m"]["source_kind"] == "market_candles"
    assert payload["5m"]["freshness_state"] == "fresh"
    assert payload["5m"]["dataset_mode"] == "live"
    assert len(payload["5m"]["candles"]) == 12


def test_build_surveyor_packet_snapshot_returns_complete_packet_with_fresh_store(tmp_path) -> None:
    conn = init_db(str(tmp_path / "liquidsniper.sqlite"))
    now_ms = int(time.time() * 1000)
    _seed_candles(conn, symbol="BTC/USDT", timeframe="5m", start_ms=now_ms - (24 * 5 * 60 * 1000), step_ms=5 * 60 * 1000, count=24)
    _seed_candles(conn, symbol="BTC/USDT", timeframe="4h", start_ms=now_ms - (20 * 4 * 60 * 60 * 1000), step_ms=4 * 60 * 60 * 1000, count=10)
    _seed_candles(conn, symbol="BTC/USDT", timeframe="1d", start_ms=now_ms - (20 * 24 * 60 * 60 * 1000), step_ms=24 * 60 * 60 * 1000, count=10)
    _seed_candles(conn, symbol="BTC/USDT", timeframe="1w", start_ms=now_ms - (20 * 7 * 24 * 60 * 60 * 1000), step_ms=7 * 24 * 60 * 60 * 1000, count=10)

    packet = build_surveyor_packet_snapshot(
        conn,
        symbol="BTCUSDT",
        authoritative_view=_authoritative_view(),
        ladders={"nearest_support": {"zone_id": "z1", "kind": "support", "bounds": {"low": 100.0, "high": 102.0}}},
        allow_replay_fallback=False,
    )
    conn.close()

    assert packet["meta"]["packet_status"] == "complete"
    assert sorted(packet["market_data"]["timeframes"].keys()) == ["1D", "1W", "4H", "5m"]
    assert packet["market_data"]["timeframes"]["5m"]["freshness_state"] == "fresh"
    assert packet["structure"]["timeframes"]["5m"]["status"] == "ok"
    assert packet["sr"]["selected_surfaces"]["4H"]["selector_surface"] == "operational_4h"
    assert packet["fib"]["contexts_by_timeframe"]
    assert packet["dynamic_levels"]["levels"]


def test_build_surveyor_packet_snapshot_is_partial_when_required_feed_missing(tmp_path) -> None:
    conn = init_db(str(tmp_path / "liquidsniper.sqlite"))
    now_ms = int(time.time() * 1000)
    _seed_candles(conn, symbol="BTC/USDT", timeframe="4h", start_ms=now_ms - (20 * 4 * 60 * 60 * 1000), step_ms=4 * 60 * 60 * 1000, count=10)
    _seed_candles(conn, symbol="BTC/USDT", timeframe="1d", start_ms=now_ms - (20 * 24 * 60 * 60 * 1000), step_ms=24 * 60 * 60 * 1000, count=10)

    packet = build_surveyor_packet_snapshot(
        conn,
        symbol="BTCUSDT",
        authoritative_view=_authoritative_view(),
        ladders=None,
        allow_replay_fallback=False,
    )
    conn.close()

    assert packet["meta"]["packet_status"] == "partial"
    assert packet["market_data"]["timeframes"]["5m"]["freshness_state"] == "partial"

