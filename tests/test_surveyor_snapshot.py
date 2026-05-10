from __future__ import annotations

import json
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


def _seed_feed_health_event(
    conn,
    *,
    symbol: str,
    timeframe: str,
    state: str,
    as_of_ms: int,
    reason_codes: list[str],
    metadata: dict | None = None,
) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO feed_health_events(provider_id, venue, symbol, timeframe, state, reason_codes_json, as_of_ms, trace_id, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "ccxt",
                "okx",
                symbol,
                timeframe,
                state,
                json.dumps(reason_codes, sort_keys=True),
                as_of_ms,
                f"health:{timeframe}",
                json.dumps(metadata, sort_keys=True) if metadata else None,
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
        "formation_reaction_count": 3,
        "historical_context_score": 0.80,
        "retest_count": 1,
        "selection_score": 0.90,
        "source_rank": 1,
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
    sr_zone = packet["sr"]["levels_by_timeframe"]["4H"][0]
    assert sr_zone["lifecycle"]["status"] == "active"
    assert sr_zone["quality"]["confidence_tier"] == "A"
    assert sr_zone["quality"]["decision_eligibility"] == "candidate_eligible"
    assert sr_zone["visual"]["show_on_overlay"] is True
    sr_dataset = packet["bundle"]["datasets"]["sr_zones"]
    assert sr_dataset["summary"]["metadata_zone_count"] == 2
    assert sr_dataset["summary"]["lifecycle_status_counts"] == {"active": 2}
    assert sr_dataset["payload"]["selected_surfaces"]["4H"]["groups"]["below_price"][0]["quality"]["confidence_tier"] == "A"
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


def test_missing_feed_checkpoint_reason_is_preserved_in_diagnostics(tmp_path) -> None:
    conn = init_db(str(tmp_path / "liquidsniper.sqlite"))
    now_ms = int(time.time() * 1000)
    _seed_candles(conn, symbol="BTC/USDT", timeframe="5m", start_ms=now_ms - (24 * 5 * 60 * 1000), step_ms=5 * 60 * 1000, count=24)
    with conn:
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
                "BTC/USDT",
                "4h",
                None,
                None,
                now_ms,
                1,
                "degraded",
                "PROVIDER_RATE_LIMITED",
                "checkpoint:4h",
            ),
        )

    packet = build_surveyor_packet_snapshot(
        conn,
        symbol="BTCUSDT",
        authoritative_view=None,
        ladders=None,
        allow_replay_fallback=False,
    )
    conn.close()

    feed_issue = next(
        issue for issue in packet["diagnostics"]["issues"]
        if issue.get("family") == "feed_state" and issue.get("timeframe") == "4H"
    )
    assert feed_issue["reason"] == "PROVIDER_RATE_LIMITED"
    assert packet["bundle"]["datasets"]["structure_state"]["summary"]["suspected_code_defect_timeframes"] == []


def test_build_surveyor_packet_snapshot_surfaces_repair_quality_metadata(tmp_path) -> None:
    conn = init_db(str(tmp_path / "liquidsniper.sqlite"))
    now_ms = int(time.time() * 1000)
    _seed_candles(conn, symbol="BTC/USDT", timeframe="5m", start_ms=now_ms - (24 * 5 * 60 * 1000), step_ms=5 * 60 * 1000, count=24)
    _seed_candles(conn, symbol="BTC/USDT", timeframe="4h", start_ms=now_ms - (20 * 4 * 60 * 60 * 1000), step_ms=4 * 60 * 60 * 1000, count=10)
    _seed_candles(conn, symbol="BTC/USDT", timeframe="1d", start_ms=now_ms - (20 * 24 * 60 * 60 * 1000), step_ms=24 * 60 * 60 * 1000, count=10)
    _seed_candles(conn, symbol="BTC/USDT", timeframe="1w", start_ms=now_ms - (20 * 7 * 24 * 60 * 60 * 1000), step_ms=7 * 24 * 60 * 60 * 1000, count=10)
    _seed_feed_health_event(
        conn,
        symbol="BTC/USDT",
        timeframe="5m",
        state="ok",
        as_of_ms=now_ms,
        reason_codes=["CANDLE_RANGE_REPAIRED", "HISTORICAL_REPAIR_ELEVATED"],
        metadata={
            "repair_policy_contract": "blofin_historical_repair_policy_v1",
            "quality_band": "elevated",
            "circuit_breaker_action": "warn",
            "repair_provenance": {
                "provider_id": "blofin_rest",
                "venue": "blofin",
                "timeframe": "5m",
            },
            "repair_summary": {
                "contract": "blofin_repair_summary_v1",
                "repair_policy_contract": "blofin_historical_repair_policy_v1",
                "fetched_candles": 24,
                "accepted_candles": 24,
                "repaired_candles": 2,
                "unrepairable_candles": 0,
                "repair_rate": 0.083333,
            },
        },
    )

    packet = build_surveyor_packet_snapshot(
        conn,
        symbol="BTCUSDT",
        authoritative_view=_authoritative_view(),
        ladders=None,
        allow_replay_fallback=False,
    )
    conn.close()

    assert packet["market_data"]["timeframes"]["5m"]["quality_band"] == "elevated"
    assert packet["market_data"]["timeframes"]["5m"]["circuit_breaker_action"] == "warn"
    assert packet["market_data"]["timeframes"]["5m"]["repair_summary"]["repaired_candles"] == 2
    assert packet["bundle"]["datasets"]["feed_state"]["summary"]["quality_band_summary"]["elevated"] == ["5m"]
    issue = next(
        issue for issue in packet["bundle"]["diagnostics"]["issues"]
        if issue.get("family") == "feed_state" and issue.get("issue_kind") == "historical_repair_quality"
    )
    assert issue["timeframe"] == "5m"
    assert issue["circuit_breaker_action"] == "warn"
