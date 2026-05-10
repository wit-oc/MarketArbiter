from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from market_arbiter.arbiter.backtest_controls import TimeShiftControlConfig, build_time_shift_control_dataset
from market_arbiter.arbiter.ohlcv_backtest import (
    OHLCVBacktestConfig,
    load_ohlcv_directory,
    run_event_study,
    run_ohlcv_backtest,
    serialize_report,
)
from market_arbiter.ops.strategy_backtest_run import _load_json, _symbols


STRATEGY_BACKTEST_CONTROL_RUN_CONTRACT_V1 = "strategy_backtest_control_run_report_v1"


def _shift_values(value: str) -> list[int]:
    shifts = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        shift = int(item)
        if shift == 0:
            raise ValueError("control shifts must be non-zero")
        shifts.append(abs(shift))
    if not shifts:
        raise ValueError("at least one control shift is required")
    return shifts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Arbiter backtest plus deterministic time-shift negative controls.")
    parser.add_argument("--dataset", required=True, help="Path to foxian_retest_backtest_dataset_v0-compatible JSON")
    parser.add_argument("--ohlcv-dir", required=True, help="Directory of per-symbol OHLCV CSV/JSON files")
    parser.add_argument("--timeframe", default="4h", help="OHLCV timeframe suffix to load, default: 4h")
    parser.add_argument("--symbols", help="Optional comma-separated symbol allowlist, e.g. BTCUSDT,ETHUSDT")
    parser.add_argument("--max-hold-bars", type=int, default=288, help="Maximum bars to hold a simulated trade")
    parser.add_argument("--target-rr", type=float, help="Override candidate RR target; default uses candidate first target_rr")
    parser.add_argument("--same-bar-fill-policy", choices=["stop_first", "target_first"], default="stop_first")
    parser.add_argument("--control-shifts", default="20,60", help="Comma-separated positive bar shifts for time-shift controls")
    parser.add_argument("--control-direction", choices=["forward", "backward"], default="forward")
    parser.add_argument("--output", help="Write combined report JSON to this path")
    return parser


def _run_reports(dataset: dict[str, Any], ohlcv_by_symbol: dict[str, Any], config: OHLCVBacktestConfig) -> dict[str, Any]:
    return {
        "trade_report": run_ohlcv_backtest(dataset, ohlcv_by_symbol, config=config),
        "event_study_report": run_event_study(dataset, ohlcv_by_symbol, config=config),
    }


def _comparison(primary: dict[str, Any], control: dict[str, Any]) -> dict[str, Any]:
    primary_summary = dict(primary.get("trade_report", {}).get("summary", {}))
    control_summary = dict(control.get("trade_report", {}).get("summary", {}))
    primary_avg_r = float(primary_summary.get("avg_net_r_multiple") or 0.0)
    control_avg_r = float(control_summary.get("avg_net_r_multiple") or 0.0)
    primary_dd = float(primary_summary.get("max_drawdown_bps") or 0.0)
    control_dd = float(control_summary.get("max_drawdown_bps") or 0.0)
    return {
        "primary_trade_count": int(primary_summary.get("trade_count") or 0),
        "control_trade_count": int(control_summary.get("trade_count") or 0),
        "primary_avg_net_r_multiple": primary_avg_r,
        "control_avg_net_r_multiple": control_avg_r,
        "avg_r_delta_primary_minus_control": primary_avg_r - control_avg_r,
        "primary_total_net_bps": float(primary_summary.get("total_net_bps") or 0.0),
        "control_total_net_bps": float(control_summary.get("total_net_bps") or 0.0),
        "primary_max_drawdown_bps": primary_dd,
        "control_max_drawdown_bps": control_dd,
        "control_degraded_expectancy": control_avg_r < primary_avg_r,
    }


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
    primary = _run_reports(dataset, ohlcv_by_symbol, config)

    controls = []
    for shift in _shift_values(args.control_shifts):
        control_dataset = build_time_shift_control_dataset(
            dataset,
            ohlcv_by_symbol,
            config=TimeShiftControlConfig(shift_bars=shift, direction=args.control_direction),
        )
        reports = _run_reports(control_dataset, ohlcv_by_symbol, config)
        controls.append({
            "control_id": control_dataset.get("control", {}).get("control_id"),
            "dataset_control": control_dataset.get("control"),
            "reports": reports,
            "comparison_to_primary": _comparison(primary, reports),
        })

    combined = {
        "contract": STRATEGY_BACKTEST_CONTROL_RUN_CONTRACT_V1,
        "dataset_path": str(args.dataset),
        "ohlcv_dir": str(args.ohlcv_dir),
        "timeframe": args.timeframe,
        "primary": primary,
        "controls": controls,
        "interpretation": [
            "Controls are negative controls, not strategy variants. A promotable SR/retest hypothesis should beat these time-shifted controls before paper/shadow consideration.",
            "Control degradation is necessary but not sufficient; canonical SR provenance, walk-forward robustness, symbol/year coverage, and lower-timeframe ambiguity checks still gate promotion.",
        ],
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
