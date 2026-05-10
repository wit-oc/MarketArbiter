from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from market_arbiter.feed.bakeoff import FeedBakeoffEvent, build_provider, enabled_provider_ids, render_smoke_markdown, summarize_provider_event_file
from market_arbiter.ops.feed_bakeoff_status import summarize_run


def test_enabled_provider_ids_excludes_disabled_and_paid_candidate():
    config = {
        "providers": [
            {"id": "blofin", "enabled_by_default": True},
            {"id": "binance", "enabled_by_default": False},
            {"id": "paid_normalized_candidate", "enabled_by_default": True},
        ]
    }

    assert enabled_provider_ids(config) == ["blofin"]


def test_bybit_parse_closed_candle_event():
    provider = build_provider("bybit")
    events = provider.parse_payload(
        {
            "topic": "kline.5.BTCUSDT",
            "ts": 1700000300123,
            "data": [
                {
                    "start": 1700000000000,
                    "end": 1700000299999,
                    "interval": "5",
                    "open": "100",
                    "high": "102",
                    "low": "99",
                    "close": "101",
                    "volume": "12",
                    "turnover": "1212",
                    "confirm": True,
                    "timestamp": 1700000300000,
                }
            ],
        },
        run_id="run-1",
        symbols_by_provider={"BTCUSDT": "BTC-USDT"},
        timeframe="5m",
        received_ts_ms=1700000300456,
        raw_ref="raw:1",
    )

    assert len(events) == 1
    event = events[0]
    assert event.contract == "feed_bakeoff_close_event_v1"
    assert event.provider == "bybit"
    assert event.symbol == "BTC-USDT"
    assert event.provider_symbol == "BTCUSDT"
    assert event.event_kind == "closed_candle"
    assert event.ts_close_ms == 1700000300000
    assert event.close_latency_ms == 456
    assert event.volume_quote == "1212"


def test_blofin_parse_working_and_closed_events():
    provider = build_provider("blofin")
    events = provider.parse_payload(
        {
            "arg": {"channel": "candle5m", "instId": "ETH-USDT"},
            "data": [
                [1700000000000, "10", "12", "9", "11", "1", "2", "22", "0"],
                [1700000300000, "11", "13", "10", "12", "2", "3", "36", "1"],
            ],
        },
        run_id="run-1",
        symbols_by_provider={"ETH-USDT": "ETH-USDT"},
        timeframe="5m",
        received_ts_ms=1700000600123,
        raw_ref="raw:2",
    )

    assert [event.event_kind for event in events] == ["working_candle", "closed_candle"]
    assert events[1].volume_base == "3"
    assert events[1].volume_quote == "36"


def test_okx_parse_closed_candle_event():
    provider = build_provider("okx")
    events = provider.parse_payload(
        {
            "arg": {"channel": "candle5m", "instId": "SOL-USDT"},
            "data": [["1700000000000", "20", "22", "19", "21", "100", "50", "1050", "1"]],
        },
        run_id="run-1",
        symbols_by_provider={"SOL-USDT": "SOL-USDT"},
        timeframe="5m",
        received_ts_ms=1700000300007,
        raw_ref="raw:3",
    )

    assert len(events) == 1
    assert events[0].provider == "okx"
    assert events[0].event_kind == "closed_candle"
    assert events[0].close_latency_ms == 7


def test_render_smoke_markdown_includes_artifacts():
    markdown = render_smoke_markdown(
        {
            "run_id": "feed-bakeoff-test",
            "status": "needs_review",
            "providers": [
                {
                    "provider": "blofin",
                    "status": "completed",
                    "event_path": "artifacts/feed_bakeoff/run/blofin/close_events.jsonl",
                    "summary": {
                        "duplicates": 0,
                        "conflicts": 0,
                        "symbols": {"BTC-USDT": {"closed_candles": 3, "working_candles": 12, "close_latency_ms": {"p50": 100, "max": 200}}},
                    },
                }
            ],
        }
    )

    assert "feed-bakeoff-test" in markdown
    assert "artifacts/feed_bakeoff/run/blofin/close_events.jsonl" in markdown
    assert "BTC-USDT: closed=3" in markdown


def test_summarize_provider_event_file_detects_duplicates_conflicts_and_gaps(tmp_path):
    event_path = tmp_path / "close_events.jsonl"
    rows = [
        FeedBakeoffEvent(
            contract="feed_bakeoff_close_event_v1",
            run_id="run-1",
            provider="bybit",
            symbol="BTC-USDT",
            provider_symbol="BTCUSDT",
            timeframe="5m",
            ts_open_ms=0,
            ts_close_ms=300_000,
            event_kind="closed_candle",
            provider_event_ts_ms=300_010,
            received_ts_ms=300_456,
            close_latency_ms=456,
            open="100",
            high="101",
            low="99",
            close="100.5",
            volume_base="10",
            volume_quote="1005",
            raw_ref="raw:1",
        ),
        FeedBakeoffEvent(
            contract="feed_bakeoff_close_event_v1",
            run_id="run-1",
            provider="bybit",
            symbol="BTC-USDT",
            provider_symbol="BTCUSDT",
            timeframe="5m",
            ts_open_ms=0,
            ts_close_ms=300_000,
            event_kind="closed_candle",
            provider_event_ts_ms=300_020,
            received_ts_ms=300_789,
            close_latency_ms=789,
            open="100",
            high="101",
            low="99",
            close="100.6",
            volume_base="10",
            volume_quote="1006",
            raw_ref="raw:2",
        ),
        FeedBakeoffEvent(
            contract="feed_bakeoff_close_event_v1",
            run_id="run-1",
            provider="bybit",
            symbol="BTC-USDT",
            provider_symbol="BTCUSDT",
            timeframe="5m",
            ts_open_ms=600_000,
            ts_close_ms=900_000,
            event_kind="closed_candle",
            provider_event_ts_ms=900_010,
            received_ts_ms=900_400,
            close_latency_ms=400,
            open="101",
            high="102",
            low="100",
            close="101.5",
            volume_base="11",
            volume_quote="1116.5",
            raw_ref="raw:3",
        ),
    ]
    event_path.write_text("".join(json.dumps(row.to_json()) + "\n" for row in rows), encoding="utf-8")

    summary = summarize_provider_event_file(
        provider_id="bybit",
        event_path=event_path,
        symbols=["BTC-USDT"],
        timeframe="5m",
        target_closes_per_symbol=2,
    )

    assert summary["duplicates"] == 1
    assert summary["conflicts"] == 1
    assert summary["unique_closed_candles"] == 2
    assert summary["symbols"]["BTC-USDT"]["missing_between_observed"] == 1
    assert summary["symbols"]["BTC-USDT"]["close_latency_ms"]["p95"] == 789


def test_phase_b_status_treats_completed_one_shot_as_closed_ok(tmp_path):
    artifact_dir = tmp_path / "feed-bakeoff-phase-b-completed"
    provider_dir = artifact_dir / "bybit"
    provider_dir.mkdir(parents=True)
    started_at = datetime.now(timezone.utc) - timedelta(hours=1)
    metadata = {
        "contract": "feed_bakeoff_phase_b_live_metadata_v1",
        "run_id": "feed-bakeoff-phase-b-completed",
        "providers": ["bybit"],
        "symbols": ["BTC-USDT"],
        "timeframe": "5m",
        "duration_seconds": 300,
        "target_closes_per_symbol": 1,
        "started_at_utc": started_at.isoformat(),
    }
    (artifact_dir / "phase_b_live_metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    row = FeedBakeoffEvent(
        contract="feed_bakeoff_close_event_v1",
        run_id="feed-bakeoff-phase-b-completed",
        provider="bybit",
        symbol="BTC-USDT",
        provider_symbol="BTCUSDT",
        timeframe="5m",
        ts_open_ms=300_000,
        ts_close_ms=600_000,
        event_kind="closed_candle",
        provider_event_ts_ms=300_010,
        received_ts_ms=300_456,
        close_latency_ms=456,
        open="100",
        high="101",
        low="99",
        close="100.5",
        volume_base="10",
        volume_quote="1005",
        raw_ref="raw:1",
    )
    (provider_dir / "close_events.jsonl").write_text(json.dumps(row.to_json()) + "\n", encoding="utf-8")

    status = summarize_run(artifact_dir=artifact_dir, pid=999_999)

    assert status["status"] == "ok"
    assert status["runner_running"] is False
    assert status["runner_state"] == "completed"
    assert status["planned_duration_elapsed"] is True
    assert status["issues"] == []
