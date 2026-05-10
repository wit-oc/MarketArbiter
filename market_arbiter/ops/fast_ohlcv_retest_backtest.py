from __future__ import annotations

import argparse
import json
from pathlib import Path

from market_arbiter.arbiter.ohlcv_backtest import OHLCVBacktestConfig, run_event_study, run_ohlcv_backtest
from market_arbiter.arbiter.ohlcv_retest_adapter import (
    FAST_OHLCV_RETEST_RUN_CONTRACT,
    FastOHLCVRetestAdapterConfig,
    build_fast_ohlcv_retest_dataset,
    load_market_candles_from_db,
    write_json,
)
from market_arbiter.core.db import init_db


def _float_or_none(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the fast BTC OHLCV -> Foxian retest first-pass backtest.")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--timeframe", default="4h")
    parser.add_argument("--provider-id", default="binance_public_data")
    parser.add_argument("--venue", default="binance_usdm_futures")
    parser.add_argument("--output-dir", default="artifacts/strategy_backtests/fast_ohlcv_retest_btc_5y")
    parser.add_argument("--lookback-bars", type=int, default=30)
    parser.add_argument("--retest-window-bars", type=int, default=42)
    parser.add_argument("--breakout-buffer-bps", type=float, default=5.0)
    parser.add_argument("--zone-width-bps", type=float, default=15.0)
    parser.add_argument("--zone-atr-fraction", type=float, default=0.35)
    parser.add_argument("--max-hold-bars", type=int, default=42)
    parser.add_argument("--target-rr", default=None)
    parser.add_argument("--initial-equity", type=float, default=100_000.0)
    args = parser.parse_args(argv)

    conn = init_db(args.db_path)
    try:
        candles = load_market_candles_from_db(
            conn,
            symbol=args.symbol.upper(),
            timeframe=args.timeframe.lower(),
            provider_id=args.provider_id,
            venue=args.venue or None,
        )
    finally:
        conn.close()

    adapter_config = FastOHLCVRetestAdapterConfig(
        lookback_bars=int(args.lookback_bars),
        retest_window_bars=int(args.retest_window_bars),
        breakout_buffer_bps=float(args.breakout_buffer_bps),
        zone_width_bps=float(args.zone_width_bps),
        zone_atr_fraction=float(args.zone_atr_fraction),
    )
    dataset = build_fast_ohlcv_retest_dataset(candles, symbol=args.symbol.upper(), timeframe=args.timeframe.lower(), config=adapter_config)
    sim_config = OHLCVBacktestConfig(
        timeframe=args.timeframe.lower(),
        max_hold_bars=int(args.max_hold_bars),
        target_rr=_float_or_none(args.target_rr),
        initial_equity=float(args.initial_equity),
    )
    ohlcv_by_symbol = {args.symbol.upper(): candles}
    trade_report = run_ohlcv_backtest(dataset, ohlcv_by_symbol, config=sim_config)
    event_study_report = run_event_study(dataset, ohlcv_by_symbol, config=sim_config)
    run_report = {
        "contract": FAST_OHLCV_RETEST_RUN_CONTRACT,
        "source": {
            "db_path": args.db_path,
            "provider_id": args.provider_id,
            "venue": args.venue,
            "symbol": args.symbol.upper(),
            "timeframe": args.timeframe.lower(),
            "candle_count": len(candles),
            "first_ts": candles[0]["timestamp"] if candles else None,
            "last_ts": candles[-1]["timestamp"] if candles else None,
        },
        "dataset_summary": {
            "evaluations": len(dataset.get("evaluations") or []),
            "event_study_rows": len(dataset.get("event_study_rows") or []),
            "trade_candidates": len(dataset.get("trade_candidates") or []),
            "source_adapter": dataset.get("source_adapter"),
        },
        "trade_report": trade_report,
        "event_study_report": event_study_report,
    }

    output_dir = Path(args.output_dir)
    write_json(output_dir / "fast_ohlcv_retest_dataset.json", dataset)
    write_json(output_dir / "fast_ohlcv_retest_report.json", run_report)
    print(
        json.dumps(
            {
                "ok": True,
                "dataset_path": str(output_dir / "fast_ohlcv_retest_dataset.json"),
                "report_path": str(output_dir / "fast_ohlcv_retest_report.json"),
                "candle_count": len(candles),
                "trade_candidates": len(dataset.get("trade_candidates") or []),
                "closed_trades": trade_report["coverage"]["closed_trades"],
                "skipped_trades": trade_report["coverage"]["skipped_trades"],
                "win_rate": trade_report["summary"]["win_rate"],
                "avg_net_r_multiple": trade_report["summary"]["avg_net_r_multiple"],
                "total_net_bps": trade_report["summary"]["total_net_bps"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
