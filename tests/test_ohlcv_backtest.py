from __future__ import annotations

import json
from pathlib import Path

from market_arbiter.arbiter.ohlcv_backtest import (
    OHLCV_BACKTEST_REPORT_CONTRACT,
    OHLCVBacktestConfig,
    load_ohlcv_directory,
    run_event_study,
    run_ohlcv_backtest,
)
from market_arbiter.ops.strategy_backtest_run import main as run_cli


def _dataset() -> dict:
    return {
        "contract": "foxian_retest_backtest_dataset_v0",
        "ruleset_id": "foxian_retest_flip_confluence_v0",
        "event_study_rows": [
            {
                "event_id": "btc-event",
                "symbol": "BTCUSDT",
                "event_ts": "2026-01-01T00:00:00Z",
                "side": "long",
                "zone_id": "btc-zone",
                "confluence_score": 4,
            },
            {
                "event_id": "eth-event",
                "symbol": "ETHUSDT",
                "event_ts": "2026-01-01T00:00:00Z",
                "side": "short",
                "zone_id": "eth-zone",
                "confluence_score": 3,
            },
        ],
        "trade_candidates": [
            {
                "symbol": "BTCUSDT",
                "side": "long",
                "entry_event_id": "btc-event",
                "entry_ts": "2026-01-01T00:00:00Z",
                "zone_id": "btc-zone",
                "invalidation_level_hint": 99.0,
                "stop_buffer_bps": 0.0,
                "target_rr": [2.0],
                "cost_model": {"taker_fee_bps": 0.0, "slippage_bps": 0.0, "funding_bps_per_8h": 0.0},
                "risk_model": {"risk_pct": 5.0},
            },
            {
                "symbol": "ETHUSDT",
                "side": "short",
                "entry_event_id": "eth-event",
                "entry_ts": "2026-01-01T00:00:00Z",
                "zone_id": "eth-zone",
                "invalidation_level_hint": 51.0,
                "stop_buffer_bps": 0.0,
                "target_rr": [2.0],
                "cost_model": {"taker_fee_bps": 0.0, "slippage_bps": 0.0, "funding_bps_per_8h": 0.0},
                "risk_model": {"risk_pct": 1.0},
            },
        ],
    }


def _write_csv(path: Path, rows: list[tuple[str, float, float, float, float, float]]) -> None:
    path.write_text(
        "timestamp,open,high,low,close,volume\n"
        + "".join(f"{ts},{open_},{high},{low},{close},{volume}\n" for ts, open_, high, low, close, volume in rows),
        encoding="utf-8",
    )


def _ohlcv_dir(tmp_path: Path) -> Path:
    root = tmp_path / "ohlcv"
    root.mkdir()
    _write_csv(
        root / "BTCUSDT.5m.csv",
        [
            ("2026-01-01T00:00:00Z", 99.0, 100.0, 98.0, 99.5, 10),
            ("2026-01-01T00:05:00Z", 100.0, 101.0, 99.5, 100.5, 10),
            ("2026-01-01T00:10:00Z", 100.5, 102.2, 100.0, 102.0, 10),
            ("2026-01-01T00:15:00Z", 102.0, 103.0, 101.5, 102.5, 10),
        ],
    )
    _write_csv(
        root / "ETHUSDT.5m.csv",
        [
            ("2026-01-01T00:00:00Z", 50.0, 50.2, 49.8, 50.0, 10),
            ("2026-01-01T00:05:00Z", 50.0, 51.2, 49.7, 51.0, 10),
            ("2026-01-01T00:10:00Z", 51.0, 51.5, 50.0, 51.2, 10),
        ],
    )
    return root


def test_load_ohlcv_directory_accepts_per_symbol_timeframe_csv(tmp_path: Path) -> None:
    ohlcv = load_ohlcv_directory(_ohlcv_dir(tmp_path), timeframe="5m")

    assert sorted(ohlcv) == ["BTCUSDT", "ETHUSDT"]
    assert ohlcv["BTCUSDT"][0]["open"] == 99.0
    assert ohlcv["ETHUSDT"][1]["timestamp"] == "2026-01-01T00:05:00Z"


def test_run_ohlcv_backtest_closes_multi_pair_candidates(tmp_path: Path) -> None:
    ohlcv = load_ohlcv_directory(_ohlcv_dir(tmp_path), timeframe="5m")

    report = run_ohlcv_backtest(_dataset(), ohlcv, config=OHLCVBacktestConfig(max_hold_bars=5))

    assert report["contract"] == OHLCV_BACKTEST_REPORT_CONTRACT
    assert report["coverage"] == {
        "input_trade_candidates": 2,
        "symbols_with_ohlcv": 2,
        "closed_trades": 2,
        "skipped_trades": 0,
    }
    by_event = {trade["entry_event_id"]: trade for trade in report["trades"]}
    assert by_event["btc-event"]["exit_reason"] == "target"
    assert by_event["btc-event"]["net_return_bps"] == 200.0
    assert by_event["btc-event"]["risk_pct"] == 5.0
    assert by_event["btc-event"]["equity_return_pct"] == 10.0
    assert by_event["eth-event"]["exit_reason"] == "stop_loss"
    assert by_event["eth-event"]["net_return_bps"] == -200.0
    assert report["by_symbol"]["BTCUSDT"]["win_count"] == 1
    assert report["by_symbol"]["ETHUSDT"]["loss_count"] == 1


def test_run_event_study_returns_direction_adjusted_forward_returns(tmp_path: Path) -> None:
    ohlcv = load_ohlcv_directory(_ohlcv_dir(tmp_path), timeframe="5m")

    report = run_event_study(_dataset(), ohlcv, config=OHLCVBacktestConfig(event_study_horizons_bars=(1, 2)))

    rows = {row["event_id"]: row for row in report["rows"]}
    assert rows["btc-event"]["forward_return_bps"]["1_bars"] > 0
    assert rows["eth-event"]["forward_return_bps"]["1_bars"] < 0


def test_strategy_backtest_run_cli_writes_combined_report(tmp_path: Path) -> None:
    dataset_path = tmp_path / "dataset.json"
    dataset_path.write_text(json.dumps(_dataset()), encoding="utf-8")
    output_path = tmp_path / "report.json"

    code = run_cli([
        "--dataset",
        str(dataset_path),
        "--ohlcv-dir",
        str(_ohlcv_dir(tmp_path)),
        "--timeframe",
        "5m",
        "--output",
        str(output_path),
    ])

    assert code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["contract"] == "strategy_backtest_run_report_v0"
    assert payload["trade_report"]["coverage"]["closed_trades"] == 2
