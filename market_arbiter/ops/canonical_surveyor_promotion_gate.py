from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


DEFAULT_PRIMARY_VARIANT = {
    "stop_policy": "sweep_or_zone_adaptive",
    "setup_id": "body_p60_selection_p60_family3",
    "target": "2R",
    "take_profit_plan": "tp_25_50_25",
    "risk_model": "graduated_confluence",
    "dca_plan": "dca_20_30_50",
}


EXECUTION_ORDER_AMBIGUITY_FLAGS = frozenset({
    "same_candle_limit_fill_and_first_take_profit",
    "same_candle_limit_fill_and_stop",
    "same_candle_limit_fill_and_target",
    "same_candle_stop_and_target",
    "same_candle_take_profit_and_breakeven_stop",
})
DIAGNOSTIC_INTRABAR_FLAGS = frozenset({
    "same_candle_multiple_take_profits_after_breakeven",
})


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _ambiguity_signal(row: Mapping[str, Any]) -> dict[str, Any]:
    flags = sorted({str(flag) for flag in _as_list(row.get("ambiguity_flags")) if str(flag)})
    has_classified_flag_lists = any(
        key in row
        for key in ("execution_order_ambiguity_flags", "diagnostic_intrabar_flags", "unclassified_intrabar_flags")
    )
    if has_classified_flag_lists:
        execution = sorted(str(flag) for flag in _as_list(row.get("execution_order_ambiguity_flags")))
        diagnostic = sorted(str(flag) for flag in _as_list(row.get("diagnostic_intrabar_flags")))
        unclassified = sorted(str(flag) for flag in _as_list(row.get("unclassified_intrabar_flags")))
    else:
        execution = sorted(flag for flag in flags if flag in EXECUTION_ORDER_AMBIGUITY_FLAGS)
        diagnostic = sorted(flag for flag in flags if flag in DIAGNOSTIC_INTRABAR_FLAGS)
        unclassified = sorted(flag for flag in flags if flag not in EXECUTION_ORDER_AMBIGUITY_FLAGS and flag not in DIAGNOSTIC_INTRABAR_FLAGS)
        execution = sorted(execution + unclassified)
    if execution:
        signal = "execution_order"
    elif diagnostic:
        signal = "diagnostic_only"
    else:
        signal = "none"
    return {
        "has_intrabar_ambiguity": bool(flags),
        "has_execution_order_ambiguity": bool(execution),
        "has_diagnostic_intrabar_signal": bool(diagnostic),
        "intrabar_ambiguity_signal": signal,
    }


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _year(value: Any) -> str:
    raw = str(value or "")
    if len(raw) >= 4 and raw[:4].isdigit():
        return raw[:4]
    try:
        return str(datetime.fromtimestamp(float(raw)).year)
    except Exception:
        return "unknown"


def _trade_sort_key(trade: Mapping[str, Any]) -> tuple[str, str, str]:
    return (str(trade.get("exit_ts") or ""), str(trade.get("entry_ts") or ""), str(trade.get("entry_event_id") or ""))


def _portfolio_metrics(rows: Sequence[Mapping[str, Any]], *, baseline_equity: float = 1_000.0) -> dict[str, Any]:
    equity = baseline_equity
    peak_equity = equity
    cumulative_pnl = 0.0
    max_pnl = 0.0
    max_drawdown = 0.0
    for row in sorted(rows, key=_trade_sort_key):
        pnl = baseline_equity * _float(row.get("net_return_bps")) / 10_000.0
        cumulative_pnl += pnl
        equity = baseline_equity + cumulative_pnl
        peak_equity = max(peak_equity, equity)
        max_pnl = max(max_pnl, cumulative_pnl)
        max_drawdown = max(max_drawdown, peak_equity - equity)
    return {
        "portfolio_baseline_equity": baseline_equity,
        "portfolio_final_equity": equity,
        "portfolio_final_pnl": cumulative_pnl,
        "portfolio_max_pnl": max_pnl,
        "portfolio_max_drawdown": max_drawdown,
        "portfolio_max_drawdown_pct_of_baseline": max_drawdown / baseline_equity if baseline_equity else None,
        "portfolio_max_drawdown_pct_from_peak": max_drawdown / peak_equity if peak_equity else None,
    }


def _summarize(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    n = len(rows)
    if not n:
        return {
            "trade_count": 0,
            "intrabar_ambiguity_count": 0,
            "intrabar_ambiguity_rate": 0.0,
            "execution_order_ambiguity_count": 0,
            "execution_order_ambiguity_rate": 0.0,
            "diagnostic_only_intrabar_count": 0,
            "diagnostic_only_intrabar_rate": 0.0,
            **_portfolio_metrics([]),
        }
    wins = [row for row in rows if _float(row.get("net_r_multiple")) > 0]
    signals = [_ambiguity_signal(row) for row in rows]
    ambiguous = [signal for signal in signals if signal.get("has_intrabar_ambiguity")]
    execution_ambiguous = [signal for signal in signals if signal.get("has_execution_order_ambiguity")]
    diagnostic_only = [signal for signal in signals if signal.get("intrabar_ambiguity_signal") == "diagnostic_only"]
    return {
        "trade_count": n,
        "win_rate": len(wins) / n,
        "avg_net_r_multiple": sum(_float(row.get("net_r_multiple")) for row in rows) / n,
        "total_net_bps": sum(_float(row.get("net_return_bps")) for row in rows),
        "avg_risk_used_pct": sum(_float(row.get("risk_budget_used_pct")) for row in rows) / n,
        "intrabar_ambiguity_count": len(ambiguous),
        "intrabar_ambiguity_rate": len(ambiguous) / n,
        "execution_order_ambiguity_count": len(execution_ambiguous),
        "execution_order_ambiguity_rate": len(execution_ambiguous) / n,
        "diagnostic_only_intrabar_count": len(diagnostic_only),
        "diagnostic_only_intrabar_rate": len(diagnostic_only) / n,
        **_portfolio_metrics(rows),
    }


def _matches(row: Mapping[str, Any], variant: Mapping[str, Any]) -> bool:
    return all(str(row.get(key)) == str(value) for key, value in variant.items())


def _variant_rows(report: Mapping[str, Any], variant: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    out = []
    for fold in _as_list(report.get("fold_results")):
        fold = _as_dict(fold)
        if not _matches(fold, variant):
            continue
        for trade in _as_list(_as_dict(fold.get("backtest")).get("trades")):
            item = _as_dict(trade)
            item["fold_id"] = fold.get("fold_id")
            out.append(item)
    return out


def _group(rows: Sequence[Mapping[str, Any]], key_fn) -> dict[str, Any]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(key_fn(row))].append(row)
    return {key: _summarize(group) for key, group in sorted(groups.items())}


def _gate_report(report: Mapping[str, Any], variant: Mapping[str, Any], *, expected_symbols: set[str], min_trades: int, max_ambiguity_rate: float, min_symbols_with_trades: int) -> dict[str, Any]:
    rows = _variant_rows(report, variant)
    symbols_in_report = {str(symbol).upper() for symbol in _as_list(report.get("symbols"))}
    traded_symbols = {str(row.get("symbol") or "").upper() for row in rows if row.get("symbol")}
    by_symbol = _group(rows, lambda row: str(row.get("symbol") or "UNKNOWN").upper())
    by_year = _group(rows, lambda row: _year(row.get("entry_ts")))
    by_symbol_year = _group(rows, lambda row: f"{str(row.get('symbol') or 'UNKNOWN').upper()}:{_year(row.get('entry_ts'))}")
    summary = _summarize(rows)

    gates = [
        {
            "gate": "expected_10_pair_universe_present",
            "passed": expected_symbols.issubset(symbols_in_report),
            "observed": sorted(symbols_in_report),
            "expected": sorted(expected_symbols),
        },
        {
            "gate": "minimum_closed_trades",
            "passed": int(summary.get("trade_count", 0)) >= min_trades,
            "observed": int(summary.get("trade_count", 0)),
            "minimum": min_trades,
        },
        {
            "gate": "positive_avg_r",
            "passed": _float(summary.get("avg_net_r_multiple")) > 0,
            "observed": summary.get("avg_net_r_multiple"),
        },
        {
            "gate": "execution_order_ambiguity_rate_within_limit",
            "passed": _float(summary.get("execution_order_ambiguity_rate")) <= max_ambiguity_rate,
            "observed": summary.get("execution_order_ambiguity_rate"),
            "maximum": max_ambiguity_rate,
        },
        {
            "gate": "minimum_symbol_coverage",
            "passed": len(traded_symbols) >= min_symbols_with_trades,
            "observed": sorted(traded_symbols),
            "minimum_count": min_symbols_with_trades,
        },
        {
            "gate": "no_symbol_has_negative_avg_r",
            "passed": all(_float(s.get("avg_net_r_multiple")) > 0 for s in by_symbol.values()),
            "failing": {symbol: s for symbol, s in by_symbol.items() if _float(s.get("avg_net_r_multiple")) <= 0},
        },
        {
            "gate": "no_year_has_negative_avg_r",
            "passed": all(_float(s.get("avg_net_r_multiple")) > 0 for year, s in by_year.items() if year != "unknown"),
            "failing": {year: s for year, s in by_year.items() if year != "unknown" and _float(s.get("avg_net_r_multiple")) <= 0},
        },
    ]
    all_passed = all(bool(gate.get("passed")) for gate in gates)
    return {
        "contract": "canonical_surveyor_promotion_gate_v0",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "decision": "promote_candidate" if all_passed else "research_only_not_promoted",
        "variant": variant,
        "summary": summary,
        "gates": gates,
        "by_symbol": by_symbol,
        "by_year": by_year,
        "by_symbol_year": by_symbol_year,
        "next_actions": [
            "If execution-order ambiguity is high, rerun the primary variant on lower-timeframe execution candles or split ambiguous trades into conservative/optimistic cohorts.",
            "Treat diagnostic-only intrabar flags as review signals, not promotion blockers, unless manual examples show they can change realized PnL.",
            "If symbol/year gates fail, run failure diagnostics by symbol, side, stop source, setup bucket, and market regime before promotion.",
            "Promotion should require exact Surveyor point-in-time SR surfaces and this gate passing on the intended timeframe/universe.",
        ],
    }


def _markdown(gate: Mapping[str, Any]) -> str:
    lines = [
        "# Canonical Surveyor Promotion Gate",
        "",
        f"Decision: **{gate.get('decision')}**",
        "",
        "## Primary variant",
        "",
    ]
    for key, value in _as_dict(gate.get("variant")).items():
        lines.append(f"- `{key}`: `{value}`")
    s = _as_dict(gate.get("summary"))
    lines.extend([
        "",
        "## Summary",
        "",
        f"- Trades: `{s.get('trade_count', 0)}`",
        f"- Win rate: `{_float(s.get('win_rate')):.2%}`",
        f"- Avg R: `{_float(s.get('avg_net_r_multiple')):+.4f}`",
        f"- Total equity bps: `{_float(s.get('total_net_bps')):+.2f}`",
        f"- $1,000 final PnL: `${_float(s.get('portfolio_final_pnl')):+.2f}`",
        f"- $1,000 max PnL: `${_float(s.get('portfolio_max_pnl')):+.2f}`",
        f"- $1,000 max drawdown: `${_float(s.get('portfolio_max_drawdown')):.2f}` (`{_float(s.get('portfolio_max_drawdown_pct_of_baseline')):.2%}` of baseline)",
        f"- Execution-order ambiguity: `{_float(s.get('execution_order_ambiguity_rate')):.2%}`",
        f"- Diagnostic-only intrabar signal: `{_float(s.get('diagnostic_only_intrabar_rate')):.2%}`",
        f"- Total intrabar flags: `{_float(s.get('intrabar_ambiguity_rate')):.2%}`",
        "",
        "## Gates",
        "",
        "| Gate | Pass | Observed |",
        "|---|---:|---|",
    ])
    for item in _as_list(gate.get("gates")):
        row = _as_dict(item)
        observed = row.get("observed", row.get("failing", ""))
        lines.append(f"| {row.get('gate')} | {'yes' if row.get('passed') else 'NO'} | `{observed}` |")
    lines.extend(["", "## By symbol", "", "| Symbol | Trades | Win | Avg R | $1k PnL | $1k max DD | Exec ambig | Diag-only |", "|---|---:|---:|---:|---:|---:|---:|---:|"])
    for symbol, item in _as_dict(gate.get("by_symbol")).items():
        row = _as_dict(item)
        lines.append(f"| {symbol} | {row.get('trade_count', 0)} | {_float(row.get('win_rate')):.2%} | {_float(row.get('avg_net_r_multiple')):+.4f} | ${_float(row.get('portfolio_final_pnl')):+.2f} | ${_float(row.get('portfolio_max_drawdown')):.2f} | {_float(row.get('execution_order_ambiguity_rate')):.2%} | {_float(row.get('diagnostic_only_intrabar_rate')):.2%} |")
    lines.extend(["", "## By year", "", "| Year | Trades | Win | Avg R | $1k PnL | $1k max DD | Exec ambig | Diag-only |", "|---|---:|---:|---:|---:|---:|---:|---:|"])
    for year, item in _as_dict(gate.get("by_year")).items():
        row = _as_dict(item)
        lines.append(f"| {year} | {row.get('trade_count', 0)} | {_float(row.get('win_rate')):.2%} | {_float(row.get('avg_net_r_multiple')):+.4f} | ${_float(row.get('portfolio_final_pnl')):+.2f} | ${_float(row.get('portfolio_max_drawdown')):.2f} | {_float(row.get('execution_order_ambiguity_rate')):.2%} | {_float(row.get('diagnostic_only_intrabar_rate')):.2%} |")
    lines.extend(["", "## Next actions", ""])
    for item in _as_list(gate.get("next_actions")):
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Promotion gate for canonical Surveyor retest+DCA Arbiter constraints.")
    parser.add_argument("--report", default="artifacts/strategy_backtests/canonical_surveyor_dca_risk/dca_risk_report.json")
    parser.add_argument("--output-dir", default="artifacts/strategy_backtests/canonical_surveyor_promotion_gate")
    parser.add_argument("--min-trades", type=int, default=50)
    parser.add_argument("--max-ambiguity-rate", type=float, default=0.10, help="Maximum allowed execution-order ambiguity rate; diagnostic-only flags do not gate promotion.")
    parser.add_argument("--min-symbols-with-trades", type=int, default=6)
    parser.add_argument("--primary-target", default=DEFAULT_PRIMARY_VARIANT["target"], help="Primary final target label for gate, e.g. 2R or 4R")
    args = parser.parse_args(argv)

    report = json.loads(Path(args.report).read_text(encoding="utf-8"))
    expected_symbols = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "SOLUSDT", "TRXUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT"}
    variant = {**DEFAULT_PRIMARY_VARIANT, "target": str(args.primary_target)}
    gate = _gate_report(report, variant, expected_symbols=expected_symbols, min_trades=args.min_trades, max_ambiguity_rate=args.max_ambiguity_rate, min_symbols_with_trades=args.min_symbols_with_trades)
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    report_path = outdir / "promotion_gate_report.json"
    summary_path = outdir / "SUMMARY.md"
    report_path.write_text(json.dumps(gate, indent=2, sort_keys=True), encoding="utf-8")
    summary_path.write_text(_markdown(gate), encoding="utf-8")
    print(json.dumps({"ok": True, "decision": gate["decision"], "summary_path": str(summary_path), "report_path": str(report_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
