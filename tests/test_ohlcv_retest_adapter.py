from __future__ import annotations

from pathlib import Path

from market_arbiter.arbiter.ohlcv_backtest import OHLCVBacktestConfig, normalize_ohlcv_rows, run_ohlcv_backtest
from market_arbiter.arbiter.ohlcv_retest_adapter import (
    FAST_OHLCV_RETEST_ADAPTER_CONTRACT,
    FastOHLCVRetestAdapterConfig,
    build_fast_ohlcv_retest_dataset,
    load_market_candles_from_db,
)
from market_arbiter.core.db import init_db
from market_arbiter.core.market_data import CandleDTO, upsert_market_candles
from market_arbiter.ops.fast_ohlcv_retest_backtest import main as run_cli


def _candles() -> list[dict]:
    rows: list[dict] = []
    base = 1_700_000_000
    # Prior range: resistance 100, support 90.
    for idx in range(5):
        rows.append(
            {
                "timestamp": base + idx * 14_400,
                "open": 95,
                "high": 100,
                "low": 90,
                "close": 96,
                "volume": 10,
            }
        )
    # Breakout above resistance.
    rows.append({"timestamp": base + 5 * 14_400, "open": 99, "high": 104, "low": 98, "close": 103, "volume": 12})
    # Retest flipped resistance as support, confirmed above midpoint.
    rows.append({"timestamp": base + 6 * 14_400, "open": 102, "high": 103, "low": 99.5, "close": 101, "volume": 12})
    # Next bar opens above zone, then target gets hit.
    rows.append({"timestamp": base + 7 * 14_400, "open": 101, "high": 104, "low": 100.5, "close": 103.5, "volume": 13})
    rows.append({"timestamp": base + 8 * 14_400, "open": 103.5, "high": 106, "low": 103, "close": 105, "volume": 13})
    return rows


def test_fast_ohlcv_retest_adapter_emits_candidate_and_simulates_trade() -> None:
    dataset = build_fast_ohlcv_retest_dataset(
        _candles(),
        symbol="BTCUSDT",
        timeframe="4h",
        config=FastOHLCVRetestAdapterConfig(lookback_bars=5, retest_window_bars=4, breakout_buffer_bps=5, zone_width_bps=10, zone_atr_fraction=0.1),
    )

    assert dataset["source_adapter"]["contract"] == FAST_OHLCV_RETEST_ADAPTER_CONTRACT
    assert len(dataset["trade_candidates"]) == 1
    candidate = dataset["trade_candidates"][0]
    assert candidate["side"] == "long"
    assert candidate["zone_id"].startswith("fast-zone:BTCUSDT:4h")
    assert candidate["risk_model"]["risk_pct"] >= 2.0

    report = run_ohlcv_backtest(dataset, {"BTCUSDT": normalize_ohlcv_rows(_candles(), symbol="BTCUSDT")}, config=OHLCVBacktestConfig(max_hold_bars=4))
    assert report["coverage"]["closed_trades"] == 1
    assert report["trades"][0]["exit_reason"] == "target"


def test_load_market_candles_from_db_and_cli_run(tmp_path: Path) -> None:
    db_path = tmp_path / "market.sqlite"
    conn = init_db(str(db_path))
    try:
        candles = [
            CandleDTO(
                provider_id="binance_public_data",
                venue="binance_usdm_futures",
                symbol="BTCUSDT",
                timeframe="4h",
                ts_open_ms=int(row["timestamp"] * 1000),
                ts_close_ms=int((row["timestamp"] + 14_400) * 1000),
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
                dataset_version="test",
                trace_id="test",
            )
            for row in _candles()
        ]
        with conn:
            upsert_market_candles(conn, candles, ingest_ts_ms=1)
        loaded = load_market_candles_from_db(conn, symbol="BTCUSDT", timeframe="4h")
    finally:
        conn.close()

    assert len(loaded) == len(_candles())
    output_dir = tmp_path / "out"
    code = run_cli(
        [
            "--db-path",
            str(db_path),
            "--output-dir",
            str(output_dir),
            "--lookback-bars",
            "5",
            "--retest-window-bars",
            "4",
            "--max-hold-bars",
            "4",
        ]
    )

    assert code == 0
    assert (output_dir / "fast_ohlcv_retest_dataset.json").exists()
    assert (output_dir / "fast_ohlcv_retest_report.json").exists()
