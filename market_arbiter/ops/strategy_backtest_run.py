from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from market_arbiter.arbiter.ohlcv_backtest import (
    OHLCVBacktestConfig,
    load_ohlcv_directory,
    run_event_study,
    run_ohlcv_backtest,
    serialize_report,
)


def _load_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object at {path}")
    return payload


def _symbols(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [symbol.strip().upper() for symbol in value.split(",") if symbol.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run OHLCV simulation for Arbiter strategy backtest candidates.")
    parser.add_argument("--dataset", required=True, help="Path to foxian_retest_backtest_dataset_v0 JSON")
    parser.add_argument("--ohlcv-dir", required=True, help="Directory of per-symbol OHLCV CSV/JSON files")
    parser.add_argument("--timeframe", default="4h", help="OHLCV timeframe suffix to load, default: 4h")
    parser.add_argument("--symbols", help="Optional comma-separated symbol allowlist, e.g. BTCUSDT,ETHUSDT")
    parser.add_argument("--max-hold-bars", type=int, default=288, help="Maximum bars to hold a simulated trade")
    parser.add_argument("--target-rr", type=float, help="Override candidate RR target; default uses candidate first target_rr")
    parser.add_argument("--same-bar-fill-policy", choices=["stop_first", "target_first"], default="stop_first")
    parser.add_argument("--output", help="Write combined report JSON to this path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    dataset = _load_json(args.dataset)
    config = OHLCVBacktestConfig(
        timeframe=args.timeframe,
        max_hold_bars=args.max_hold_bars,
        target_rr=args.target_rr,
        same_bar_fill_policy=args.same_bar_fill_policy,
    )
    ohlcv_by_symbol = load_ohlcv_directory(args.ohlcv_dir, timeframe=args.timeframe, symbols=_symbols(args.symbols))
    trade_report = run_ohlcv_backtest(dataset, ohlcv_by_symbol, config=config)
    event_report = run_event_study(dataset, ohlcv_by_symbol, config=config)
    combined = {
        "contract": "strategy_backtest_run_report_v0",
        "dataset_path": str(args.dataset),
        "ohlcv_dir": str(args.ohlcv_dir),
        "trade_report": trade_report,
        "event_study_report": event_report,
    }
    serialized = serialize_report(combined)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(serialized, encoding="utf-8")
    else:
        print(serialized, end="")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
