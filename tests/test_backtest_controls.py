from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_arbiter.arbiter.backtest_controls import (
    ARBITER_BACKTEST_CONTROL_DATASET_CONTRACT_V1,
    TimeShiftControlConfig,
    build_time_shift_control_dataset,
)
from market_arbiter.arbiter.ohlcv_backtest import OHLCVBacktestConfig, run_ohlcv_backtest
from market_arbiter.ops.strategy_backtest_control_run import main as control_run_cli


def _candles() -> dict[str, list[dict]]:
    return {
        "BTCUSDT": [
            {"ts": 1_767_225_600, "timestamp": "2026-01-01T00:00:00Z", "symbol": "BTCUSDT", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10.0},
            {"ts": 1_767_225_900, "timestamp": "2026-01-01T00:05:00Z", "symbol": "BTCUSDT", "open": 100.0, "high": 102.0, "low": 99.5, "close": 101.5, "volume": 10.0},
            {"ts": 1_767_226_200, "timestamp": "2026-01-01T00:10:00Z", "symbol": "BTCUSDT", "open": 101.0, "high": 103.5, "low": 100.5, "close": 103.0, "volume": 10.0},
            {"ts": 1_767_226_500, "timestamp": "2026-01-01T00:15:00Z", "symbol": "BTCUSDT", "open": 103.0, "high": 104.0, "low": 98.5, "close": 99.0, "volume": 10.0},
            {"ts": 1_767_226_800, "timestamp": "2026-01-01T00:20:00Z", "symbol": "BTCUSDT", "open": 99.0, "high": 100.0, "low": 97.0, "close": 98.0, "volume": 10.0},
        ]
    }


def _dataset() -> dict:
    return {
        "contract": "foxian_retest_backtest_dataset_v0",
        "ruleset_id": "foxian_retest_flip_confluence_v0",
        "event_study_rows": [
            {
                "event_id": "btc-retest",
                "symbol": "BTCUSDT",
                "event_ts": "2026-01-01T00:00:00Z",
                "side": "long",
                "zone_id": "zone-1",
            }
        ],
        "trade_candidates": [
            {
                "symbol": "BTCUSDT",
                "side": "long",
                "entry_event_id": "btc-retest",
                "entry_ts": "2026-01-01T00:00:00Z",
                "zone_id": "zone-1",
                "invalidation_level_hint": 99.0,
                "stop_buffer_bps": 0.0,
                "target_rr": [2.0],
                "cost_model": {"taker_fee_bps": 0.0, "slippage_bps": 0.0, "funding_bps_per_8h": 0.0},
                "risk_model": {"risk_pct": 1.0},
            }
        ],
    }


def test_time_shift_control_dataset_keeps_backtest_shape_and_marks_provenance() -> None:
    control = build_time_shift_control_dataset(
        _dataset(),
        _candles(),
        config=TimeShiftControlConfig(shift_bars=2, direction="forward"),
    )

    assert control["contract"] == ARBITER_BACKTEST_CONTROL_DATASET_CONTRACT_V1
    assert control["source_contract"] == "foxian_retest_backtest_dataset_v0"
    assert control["control"]["control_id"] == "time_shift_forward_2bars"
    assert control["control"]["input_trade_candidates"] == 1
    assert control["control"]["output_trade_candidates"] == 1
    assert control["control"]["skipped_count"] == 0
    assert control["trade_candidates"][0]["entry_ts"] == "2026-01-01T00:10:00Z"
    assert control["trade_candidates"][0]["entry_event_id"] == "btc-retest:time_shift_forward_2bars"
    assert control["event_study_rows"][0]["event_id"] == "btc-retest:time_shift_forward_2bars"
    assert control["trade_candidates"][0]["control"]["source_entry_event_id"] == "btc-retest"


def test_time_shift_control_dataset_is_consumable_by_existing_ohlcv_backtest() -> None:
    base = run_ohlcv_backtest(_dataset(), _candles(), config=OHLCVBacktestConfig(timeframe="5m", max_hold_bars=3, target_rr=2.0, default_taker_fee_bps=0.0, default_slippage_bps=0.0))
    control_dataset = build_time_shift_control_dataset(_dataset(), _candles(), config=TimeShiftControlConfig(shift_bars=2))
    control = run_ohlcv_backtest(control_dataset, _candles(), config=OHLCVBacktestConfig(timeframe="5m", max_hold_bars=3, target_rr=2.0, default_taker_fee_bps=0.0, default_slippage_bps=0.0))

    assert base["coverage"]["closed_trades"] == 1
    assert control["coverage"]["closed_trades"] == 1
    assert base["trades"][0]["exit_reason"] == "target"
    assert control["trades"][0]["exit_reason"] == "stop_loss"


def test_time_shift_control_dataset_records_out_of_range_skips() -> None:
    control = build_time_shift_control_dataset(
        _dataset(),
        _candles(),
        config=TimeShiftControlConfig(shift_bars=10),
    )

    assert control["trade_candidates"] == []
    assert control["control"]["skipped_count"] == 1
    assert control["control"]["skipped_rows"][0]["reason"] == "shift_out_of_range"


def test_time_shift_control_rejects_zero_shift() -> None:
    with pytest.raises(ValueError, match="shift_bars must be non-zero"):
        build_time_shift_control_dataset(_dataset(), _candles(), config=TimeShiftControlConfig(shift_bars=0))


def test_strategy_backtest_control_run_cli_writes_primary_and_control_report(tmp_path: Path) -> None:
    dataset_path = tmp_path / "dataset.json"
    dataset_path.write_text(json.dumps(_dataset()), encoding="utf-8")
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    ohlcv_dir.joinpath("BTCUSDT.5m.json").write_text(json.dumps({"candles": _candles()["BTCUSDT"]}), encoding="utf-8")
    output_path = tmp_path / "control_report.json"

    code = control_run_cli([
        "--dataset",
        str(dataset_path),
        "--ohlcv-dir",
        str(ohlcv_dir),
        "--timeframe",
        "5m",
        "--max-hold-bars",
        "3",
        "--target-rr",
        "2",
        "--control-shifts",
        "2",
        "--output",
        str(output_path),
    ])

    assert code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["contract"] == "strategy_backtest_control_run_report_v1"
    assert payload["primary"]["trade_report"]["coverage"]["closed_trades"] == 1
    assert payload["controls"][0]["control_id"] == "time_shift_forward_2bars"
    assert payload["controls"][0]["reports"]["trade_report"]["coverage"]["closed_trades"] == 1
    assert payload["controls"][0]["comparison_to_primary"]["control_degraded_expectancy"] is True
