from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from market_arbiter.arbiter.dca_execution import DCA_PLANS, graduated_confluence_risk_pct, planned_dca_entries
from market_arbiter.arbiter.ohlcv_backtest import OHLCVBacktestConfig, _candidate_costs, _find_first_candle_after, _slip, parse_timestamp
from market_arbiter.arbiter.ohlcv_retest_adapter import load_market_candles_from_db, write_json
from market_arbiter.arbiter.strategy_backtest import build_foxian_retest_backtest_dataset
from market_arbiter.arbiter.take_profit import planned_take_profits
from market_arbiter.core.db import init_db
from market_arbiter.ops.canonical_surveyor_retest_backtest import CanonicalSurveyorConfig, DEFAULT_SYMBOLS, _ensure_liquidsniper_import, build_retest_profiles_for_symbol
from market_arbiter.ops.canonical_surveyor_retest_diagnostics import _as_dict, _as_list, _enrich_profiles, _float, _profile_event_id, _profile_zone, _summarize
from market_arbiter.ops.canonical_surveyor_stop_ablation import _dataset_for_policy
from market_arbiter.ops.canonical_surveyor_walk_forward import FOLDS, _apply_setup_filter, _candidate_ts, _dataset_subset, _parse_ts, _train_thresholds


PORTFOLIO_BASELINE_EQUITY = 1_000.0


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


def _ambiguity_signal(flags: Sequence[str]) -> dict[str, Any]:
    """Classify OHLC ambiguity into promotion-gating vs diagnostic signals.

    Execution-order ambiguity means the candle ordering can change the modeled
    fill/exit result. Diagnostic-only flags describe coarse daily movement that
    is worth reviewing but should not by itself block promotion. Unknown future
    flags are treated as execution-order ambiguity until explicitly classified.
    """

    normalized = sorted({str(flag) for flag in flags if str(flag)})
    execution = sorted(flag for flag in normalized if flag in EXECUTION_ORDER_AMBIGUITY_FLAGS)
    diagnostic = sorted(flag for flag in normalized if flag in DIAGNOSTIC_INTRABAR_FLAGS)
    unclassified = sorted(flag for flag in normalized if flag not in EXECUTION_ORDER_AMBIGUITY_FLAGS and flag not in DIAGNOSTIC_INTRABAR_FLAGS)
    gating = sorted(execution + unclassified)
    if gating:
        signal = "execution_order"
    elif diagnostic:
        signal = "diagnostic_only"
    else:
        signal = "none"
    return {
        "ambiguity_flags": normalized,
        "execution_order_ambiguity_flags": gating,
        "diagnostic_intrabar_flags": diagnostic,
        "unclassified_intrabar_flags": unclassified,
        "has_intrabar_ambiguity": bool(normalized),
        "has_execution_order_ambiguity": bool(gating),
        "has_diagnostic_intrabar_signal": bool(diagnostic),
        "intrabar_ambiguity_signal": signal,
    }


def _trade_ambiguity_signal(trade: Mapping[str, Any]) -> dict[str, Any]:
    flags = _as_list(trade.get("ambiguity_flags"))
    return _ambiguity_signal([str(flag) for flag in flags])


def _trade_sort_key(trade: Mapping[str, Any]) -> tuple[str, str, str]:
    return (str(trade.get("exit_ts") or ""), str(trade.get("entry_ts") or ""), str(trade.get("entry_event_id") or ""))


def _portfolio_metrics(trades: Sequence[Mapping[str, Any]], *, baseline_equity: float = PORTFOLIO_BASELINE_EQUITY) -> dict[str, Any]:
    """Closed-trade, non-compounded portfolio curve on a fixed baseline.

    Current simulators size each trade from fixed fractional risk rather than a
    compounding equity curve, so these metrics scale each trade's net return bps
    onto a standard $1,000 baseline and accumulate realized PnL by exit time.
    """

    equity = float(baseline_equity)
    peak_equity = equity
    cumulative_pnl = 0.0
    max_pnl = 0.0
    max_drawdown = 0.0
    curve = []
    for trade in sorted((_as_dict(t) for t in trades), key=_trade_sort_key):
        pnl = baseline_equity * _float(trade.get("net_return_bps")) / 10_000.0
        cumulative_pnl += pnl
        equity = baseline_equity + cumulative_pnl
        peak_equity = max(peak_equity, equity)
        max_pnl = max(max_pnl, cumulative_pnl)
        drawdown = peak_equity - equity
        max_drawdown = max(max_drawdown, drawdown)
        curve.append({
            "exit_ts": trade.get("exit_ts"),
            "entry_event_id": trade.get("entry_event_id"),
            "symbol": trade.get("symbol"),
            "pnl": pnl,
            "cumulative_pnl": cumulative_pnl,
            "equity": equity,
            "drawdown": drawdown,
        })
    return {
        "portfolio_baseline_equity": baseline_equity,
        "portfolio_final_equity": equity,
        "portfolio_final_pnl": cumulative_pnl,
        "portfolio_max_pnl": max_pnl,
        "portfolio_max_drawdown": max_drawdown,
        "portfolio_max_drawdown_pct_of_baseline": max_drawdown / baseline_equity if baseline_equity else None,
        "portfolio_max_drawdown_pct_from_peak": max_drawdown / peak_equity if peak_equity else None,
        "portfolio_curve_points": len(curve),
        "portfolio_curve": curve,
    }


def _summary_with_portfolio(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    summary = dict(_summarize(trades))
    summary.update(_portfolio_metrics(trades))
    summary.update(_ambiguity_metrics(trades))
    return summary


def _ambiguity_metrics(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    n = len(trades)
    if not n:
        return {
            "intrabar_ambiguity_count": 0,
            "intrabar_ambiguity_rate": 0.0,
            "execution_order_ambiguity_count": 0,
            "execution_order_ambiguity_rate": 0.0,
            "diagnostic_only_intrabar_count": 0,
            "diagnostic_only_intrabar_rate": 0.0,
        }
    signals = [_trade_ambiguity_signal(_as_dict(trade)) for trade in trades]
    any_ambiguity = [signal for signal in signals if signal.get("has_intrabar_ambiguity")]
    execution = [signal for signal in signals if signal.get("has_execution_order_ambiguity")]
    diagnostic_only = [signal for signal in signals if signal.get("intrabar_ambiguity_signal") == "diagnostic_only"]
    return {
        "intrabar_ambiguity_count": len(any_ambiguity),
        "intrabar_ambiguity_rate": len(any_ambiguity) / n,
        "execution_order_ambiguity_count": len(execution),
        "execution_order_ambiguity_rate": len(execution) / n,
        "diagnostic_only_intrabar_count": len(diagnostic_only),
        "diagnostic_only_intrabar_rate": len(diagnostic_only) / n,
    }


def _risk_pct_for(candidate: Mapping[str, Any], risk_model: str, features: Mapping[str, Any], thresholds: Mapping[str, Any]) -> float:
    if risk_model == "candidate_scaled":
        model = _as_dict(candidate.get("risk_model"))
        return max(0.0, _float(model.get("risk_pct"), 1.0))
    if risk_model == "flat_3pct":
        return 3.0
    if risk_model == "graduated_confluence":
        return _float(graduated_confluence_risk_pct(features, thresholds).get("risk_pct"), 1.0)
    raise ValueError(f"unknown risk_model={risk_model}")


def _filled(price: float, candle: Mapping[str, Any], *, side: str, is_market: bool) -> bool:
    if is_market:
        return True
    high = _float(candle.get("high"))
    low = _float(candle.get("low"))
    return low <= price <= high


def simulate_dca_candidate(
    candidate: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    *,
    features: Mapping[str, Any],
    thresholds: Mapping[str, Any],
    dca_plan: str,
    risk_model: str,
    target_rr: float,
    take_profit_plan: str = "tp_25_50_25",
    max_hold_bars: int = 30,
    config: OHLCVBacktestConfig | None = None,
) -> dict[str, Any]:
    cfg = config or OHLCVBacktestConfig(timeframe="1d", max_hold_bars=max_hold_bars, target_rr=target_rr)
    symbol = str(candidate.get("symbol") or "").upper()
    side = str(candidate.get("side") or "").lower()
    if side not in {"long", "short"}:
        return {"status": "skipped", "reason": "unsupported_side", "symbol": symbol, "candidate": dict(candidate)}
    try:
        signal_ts = parse_timestamp(candidate.get("entry_ts"))
    except Exception:
        return {"status": "skipped", "reason": "missing_entry_ts", "symbol": symbol, "candidate": dict(candidate)}
    entry_idx = _find_first_candle_after(candles, signal_ts)
    if entry_idx is None:
        return {"status": "skipped", "reason": "no_candle_after_entry_ts", "symbol": symbol, "candidate": dict(candidate)}
    stop = _float(candidate.get("invalidation_level_hint"), 0.0)
    if stop <= 0:
        return {"status": "skipped", "reason": "missing_stop", "symbol": symbol, "candidate": dict(candidate)}
    fee_bps, slippage_bps, funding_bps_per_8h = _candidate_costs(candidate, cfg)
    raw_entry = _float(candles[entry_idx].get("open"))
    market_entry = _slip(raw_entry, side=side, bps=slippage_bps, is_entry=True)
    full_low = _float(features.get("full_zone_low"), _float(features.get("zone_low")))
    full_high = _float(features.get("full_zone_high"), _float(features.get("zone_high")))
    dca_ladder = planned_dca_entries(side=side, first_entry_price=market_entry, zone_low=full_low, zone_high=full_high, plan_id=dca_plan)
    if dca_ladder.get("status") != "ok":
        return {"status": "skipped", "reason": dca_ladder.get("reason", "invalid_dca_plan"), "symbol": symbol, "candidate": dict(candidate)}
    entries = _as_list(dca_ladder.get("entries"))
    planned_prices = [_float(_as_dict(entry).get("entry_price")) for entry in entries]
    weights = tuple(_float(_as_dict(entry).get("weight")) for entry in entries)
    assert len(planned_prices) == len(weights)
    tp_plan = planned_take_profits(final_rr=target_rr, plan_id=take_profit_plan)
    if tp_plan.get("status") != "ok":
        return {"status": "skipped", "reason": tp_plan.get("reason", "invalid_take_profit_plan"), "symbol": symbol, "candidate": dict(candidate)}
    tp_tranches = [_as_dict(item) for item in _as_list(tp_plan.get("tranches"))]

    risk_pct = _risk_pct_for(candidate, risk_model, features, thresholds)
    risk_budget = cfg.initial_equity * risk_pct / 100.0
    tranche_risks = [risk_budget * weight for weight in weights]
    fills: list[dict[str, Any]] = []
    partial_exits: list[dict[str, Any]] = []
    ambiguity_flags: set[str] = set()
    dca_cancelled = False
    stop_moved_to_entry = False
    active_stop = stop
    locked_avg_entry: float | None = None
    tp_base_units: float | None = None
    remaining_units = 0.0
    next_tp_idx = 0
    max_exit_idx = min(len(candles) - 1, entry_idx + max(1, max_hold_bars))
    exit_idx = max_exit_idx
    exit_reason = "max_hold" if max_exit_idx < len(candles) - 1 else "series_end"
    raw_exit = _float(candles[exit_idx].get("close"))
    exit_price = raw_exit

    # First candle can fill market tranche and also hit stop/target under stop-first policy.
    for idx in range(entry_idx, max_exit_idx + 1):
        candle = candles[idx]
        newly_filled_tranches: list[int] = []
        if not dca_cancelled:
            for tranche_idx, (price, tranche_risk, weight) in enumerate(zip(planned_prices, tranche_risks, weights)):
                if any(fill["tranche_idx"] == tranche_idx for fill in fills):
                    continue
                if not _filled(price, candle, side=side, is_market=(tranche_idx == 0)):
                    continue
                fill_price = _slip(price, side=side, bps=slippage_bps, is_entry=True)
                risk_per_unit = fill_price - stop if side == "long" else stop - fill_price
                if risk_per_unit <= 0:
                    continue
                units = tranche_risk / risk_per_unit
                fills.append({
                    "tranche_idx": tranche_idx,
                    "fill_idx": idx,
                    "fill_ts": candle.get("timestamp"),
                    "weight": weight,
                    "risk_dollars": tranche_risk,
                    "entry_price": fill_price,
                    "units": units,
                    "risk_per_unit": risk_per_unit,
                })
                newly_filled_tranches.append(tranche_idx)
        if not fills:
            continue
        total_units = sum(fill["units"] for fill in fills)
        avg_entry = sum(fill["entry_price"] * fill["units"] for fill in fills) / max(total_units, 1e-9)
        if locked_avg_entry is not None:
            avg_entry = locked_avg_entry
        avg_risk_per_unit = avg_entry - stop if side == "long" else stop - avg_entry
        if avg_risk_per_unit <= 0:
            return {"status": "skipped", "reason": "non_positive_avg_risk", "symbol": symbol, "candidate": dict(candidate)}
        if tp_base_units is None:
            tp_base_units = total_units
            remaining_units = total_units
        elif not dca_cancelled:
            # Before first TP, new fills enlarge the position and TP base.
            tp_base_units = total_units
            remaining_units = total_units
        target = avg_entry + avg_risk_per_unit * target_rr if side == "long" else avg_entry - avg_risk_per_unit * target_rr
        high = _float(candle.get("high"))
        low = _float(candle.get("low"))
        stop_hit = low <= active_stop if side == "long" else high >= active_stop
        next_tp = tp_tranches[next_tp_idx] if next_tp_idx < len(tp_tranches) else None
        next_tp_rr = _float(next_tp.get("rr"), target_rr) if next_tp else target_rr
        next_tp_price = avg_entry + avg_risk_per_unit * next_tp_rr if side == "long" else avg_entry - avg_risk_per_unit * next_tp_rr
        target_hit = bool(next_tp and (high >= next_tp_price if side == "long" else low <= next_tp_price))
        if not stop_moved_to_entry and stop_hit and target_hit:
            ambiguity_flags.add("same_candle_stop_and_target")
        if target_hit and any(tranche_idx > 0 for tranche_idx in newly_filled_tranches):
            ambiguity_flags.add("same_candle_limit_fill_and_target")
        if stop_hit and any(tranche_idx > 0 for tranche_idx in newly_filled_tranches):
            ambiguity_flags.add("same_candle_limit_fill_and_stop")
        if not stop_moved_to_entry and stop_hit and target_hit:
            exit_idx = idx
            exit_reason = "stop_loss" if cfg.same_bar_fill_policy == "stop_first" else "target"
            raw_exit = active_stop if exit_reason == "stop_loss" else next_tp_price
            exit_price = _slip(raw_exit, side=side, bps=slippage_bps, is_entry=False)
            if exit_reason == "target":
                partial_exits.append({"tp_index": next_tp_idx, "exit_idx": idx, "exit_ts": candle.get("timestamp"), "rr": next_tp_rr, "weight": 1.0, "units": remaining_units, "exit_price": exit_price, "role": "ambiguous_full_target"})
                remaining_units = 0.0
            break
        if stop_hit:
            exit_idx = idx
            exit_reason = "breakeven_stop" if stop_moved_to_entry else "stop_loss"
            raw_exit = active_stop
            exit_price = _slip(raw_exit, side=side, bps=slippage_bps, is_entry=False)
            if remaining_units > 0:
                partial_exits.append({"tp_index": None, "exit_idx": idx, "exit_ts": candle.get("timestamp"), "rr": 0.0 if stop_moved_to_entry else -1.0, "weight": None, "units": remaining_units, "exit_price": exit_price, "role": exit_reason})
                remaining_units = 0.0
            break
        if target_hit:
            stop_was_moved_to_entry_at_candle_start = stop_moved_to_entry
            tp_hits_this_candle = 0
            while next_tp_idx < len(tp_tranches):
                tp = tp_tranches[next_tp_idx]
                rr = _float(tp.get("rr"), target_rr)
                tp_price = avg_entry + avg_risk_per_unit * rr if side == "long" else avg_entry - avg_risk_per_unit * rr
                hit = high >= tp_price if side == "long" else low <= tp_price
                if not hit:
                    break
                tp_hits_this_candle += 1
                exit_units = min(remaining_units, (tp_base_units or total_units) * _float(tp.get("weight")))
                if exit_units > 0:
                    partial_exits.append({"tp_index": next_tp_idx, "exit_idx": idx, "exit_ts": candle.get("timestamp"), "rr": rr, "weight": tp.get("weight"), "units": exit_units, "exit_price": _slip(tp_price, side=side, bps=slippage_bps, is_entry=False), "role": tp.get("role")})
                    remaining_units -= exit_units
                if next_tp_idx == 0:
                    dca_cancelled = True
                    stop_moved_to_entry = True
                    locked_avg_entry = avg_entry
                    active_stop = avg_entry
                    if any(tranche_idx > 0 for tranche_idx in newly_filled_tranches):
                        ambiguity_flags.add("same_candle_limit_fill_and_first_take_profit")
                next_tp_idx += 1
            if stop_was_moved_to_entry_at_candle_start and tp_hits_this_candle > 1:
                ambiguity_flags.add("same_candle_multiple_take_profits_after_breakeven")
            if remaining_units <= 1e-9 or next_tp_idx >= len(tp_tranches):
                exit_idx = idx
                exit_reason = "final_target"
                raw_exit = partial_exits[-1]["exit_price"] if partial_exits else target
                exit_price = raw_exit
                remaining_units = 0.0
                break
            if stop_moved_to_entry:
                be_hit_after_tp = low <= active_stop if side == "long" else high >= active_stop
                if be_hit_after_tp:
                    ambiguity_flags.add("same_candle_take_profit_and_breakeven_stop")
                    exit_idx = idx
                    exit_reason = "breakeven_stop"
                    raw_exit = active_stop
                    exit_price = _slip(raw_exit, side=side, bps=slippage_bps, is_entry=False)
                    partial_exits.append({"tp_index": None, "exit_idx": idx, "exit_ts": candle.get("timestamp"), "rr": 0.0, "weight": None, "units": remaining_units, "exit_price": exit_price, "role": "breakeven_stop"})
                    remaining_units = 0.0
                    break
    if not fills:
        return {"status": "skipped", "reason": "no_dca_fills", "symbol": symbol, "candidate": dict(candidate)}

    if remaining_units > 1e-9:
        # Max-hold / series-end close for anything left after partial TPs.
        exit_price = _slip(raw_exit, side=side, bps=slippage_bps, is_entry=False)
        partial_exits.append({"tp_index": None, "exit_idx": exit_idx, "exit_ts": candles[exit_idx].get("timestamp"), "rr": None, "weight": None, "units": remaining_units, "exit_price": exit_price, "role": exit_reason})
        remaining_units = 0.0
    duration_seconds = max(0, int(candles[exit_idx]["ts"]) - int(candles[entry_idx]["ts"]))
    funding_bps = funding_bps_per_8h * (duration_seconds / (8 * 60 * 60))
    pnl = 0.0
    gross_pnl = 0.0
    risk_dollars_filled = sum(fill["risk_dollars"] for fill in fills)
    notional = sum(fill["units"] * fill["entry_price"] for fill in fills)
    total_units = sum(fill["units"] for fill in fills)
    avg_entry = sum(fill["entry_price"] * fill["units"] for fill in fills) / max(total_units, 1e-9)
    entry_cost = sum(fill["entry_price"] * fill["units"] * fee_bps / 10_000.0 for fill in fills)
    funding_cost = notional * funding_bps / 10_000.0
    for exit_part in partial_exits:
        units = _float(exit_part.get("units"))
        part_exit_price = _float(exit_part.get("exit_price"))
        if side == "long":
            gross = (part_exit_price - avg_entry) * units
        else:
            gross = (avg_entry - part_exit_price) * units
        gross_pnl += gross
        pnl += gross - (part_exit_price * units * fee_bps / 10_000.0)
    pnl -= entry_cost + funding_cost
    equity_return_pct = pnl / cfg.initial_equity * 100.0 if cfg.initial_equity else None
    net_r = pnl / risk_dollars_filled if risk_dollars_filled else None
    net_return_bps = equity_return_pct * 100.0 if equity_return_pct is not None else 0.0
    avg_risk_per_unit = avg_entry - stop if side == "long" else stop - avg_entry
    risk_bps = avg_risk_per_unit / avg_entry * 10_000.0 if avg_entry else None
    target = avg_entry + avg_risk_per_unit * target_rr if side == "long" else avg_entry - avg_risk_per_unit * target_rr
    ambiguity = _ambiguity_signal(sorted(ambiguity_flags))
    return {
        "status": "closed",
        "symbol": symbol,
        "side": side,
        "entry_event_id": candidate.get("entry_event_id"),
        "zone_id": candidate.get("zone_id"),
        "entry_ts": candles[entry_idx]["timestamp"],
        "exit_ts": candles[exit_idx]["timestamp"],
        "entry_idx": entry_idx,
        "exit_idx": exit_idx,
        "entry_price": avg_entry,
        "stop_price": stop,
        "target_price": target,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "target_rr": target_rr,
        "take_profit_plan": take_profit_plan,
        "risk_pct": risk_pct,
        "risk_dollars": risk_budget,
        "risk_dollars_filled": risk_dollars_filled,
        "risk_budget_used_pct": risk_dollars_filled / risk_budget * 100.0 if risk_budget else 0.0,
        "filled_tranches": len(fills),
        "planned_tranches": len(weights),
        "dca_plan": dca_plan,
        "risk_model_id": risk_model,
        "gross_pnl": gross_pnl,
        "pnl": pnl,
        "equity_return_pct": equity_return_pct,
        "net_return_bps": net_return_bps,
        "net_r_multiple": net_r,
        "risk_bps": risk_bps,
        "holding_bars": exit_idx - entry_idx,
        "notional": notional,
        "position_units": total_units,
        "fills": fills,
        "partial_exits": partial_exits,
        "stop_moved_to_entry": stop_moved_to_entry,
        "dca_cancelled_after_first_tp": dca_cancelled,
        **ambiguity,
    }


def _run_dca_backtest(dataset: Mapping[str, Any], candles_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]], features_by_event: Mapping[str, Mapping[str, Any]], thresholds: Mapping[str, Any], *, dca_plan: str, risk_model: str, target_rr: float, take_profit_plan: str, max_hold_bars: int) -> dict[str, Any]:
    trades = []
    skipped = []
    for candidate in _as_list(dataset.get("trade_candidates")):
        cand = _as_dict(candidate)
        symbol = str(cand.get("symbol") or "").upper()
        event_id = str(cand.get("entry_event_id") or "")
        result = simulate_dca_candidate(cand, candles_by_symbol.get(symbol, []), features=features_by_event.get(event_id, {}), thresholds=thresholds, dca_plan=dca_plan, risk_model=risk_model, target_rr=target_rr, take_profit_plan=take_profit_plan, max_hold_bars=max_hold_bars)
        if result.get("status") == "closed":
            trades.append(result)
        else:
            skipped.append(result)
    by_symbol: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for trade in trades:
        by_symbol[str(trade.get("symbol") or "UNKNOWN")].append(trade)
    return {
        "contract": "dca_risk_backtest_report_v0",
        "config": {"dca_plan": dca_plan, "risk_model": risk_model, "target_rr": target_rr, "take_profit_plan": take_profit_plan, "max_hold_bars": max_hold_bars},
        "coverage": {"input_trade_candidates": len(_as_list(dataset.get("trade_candidates"))), "closed_trades": len(trades), "skipped_trades": len(skipped)},
        "summary": _summary_with_portfolio(trades),
        "by_symbol": {symbol: _summary_with_portfolio(rows) for symbol, rows in sorted(by_symbol.items())},
        "trades": trades,
        "skipped": skipped,
    }


def _summary_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Canonical Surveyor DCA + Risk Walk-Forward Ablation",
        "",
        "Compares flat 3% risk, candidate confluence-scaled risk, graduated confluence risk, DCA plans, and graduated 25/50/25 take profits. Total risk is the budget; unfilled DCA tranches leave risk unused. First TP moves stop to entry and cancels pending DCA. `Exec ambig` is the promotion-gating rate where daily candles cannot prove fill/exit ordering; `Diag ambig` is review-only coarse-candle movement.",
        "",
        "| Stop | Setup | Target | TP | Risk | DCA | Closed | Win | Avg R | $1k final PnL | $1k max PnL | $1k max DD | Exec ambig | Diag ambig | Folds |",
        "|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in sorted(report.get("aggregate", []), key=lambda r: (_float(_as_dict(r).get("summary", {}).get("avg_net_r_multiple")), _float(_as_dict(r).get("summary", {}).get("total_net_bps"))), reverse=True):
        s = _as_dict(row.get("summary"))
        lines.append(
            f"| {row.get('stop_policy')} | {row.get('setup_id')} | {row.get('target')} | {row.get('take_profit_plan')} | {row.get('risk_model')} | {row.get('dca_plan')} | {s.get('trade_count', 0)} | {s.get('win_rate', 0):.2%} | {s.get('avg_net_r_multiple', 0):+.4f} | ${s.get('portfolio_final_pnl', 0):+.2f} | ${s.get('portfolio_max_pnl', 0):+.2f} | ${s.get('portfolio_max_drawdown', 0):.2f} | {s.get('execution_order_ambiguity_rate', s.get('intrabar_ambiguity_rate', 0)):.1%} | {s.get('diagnostic_only_intrabar_rate', 0):.1%} | {s.get('folds_with_trades', 0)} |"
        )
    lines.extend([
        "",
        "## Reusable Arbiter artifacts",
        "",
        "- `market_arbiter.arbiter.setup_score:score_retest_setup`",
        "- `market_arbiter.arbiter.stop_policy:resolve_retest_stop`",
        "- `market_arbiter.arbiter.dca_execution:planned_dca_entries`",
        "- `market_arbiter.arbiter.take_profit:planned_take_profits`",
        "- `market_arbiter.arbiter.dca_execution:graduated_confluence_risk_pct`",
        "- `docs/ARBITER_RETEST_EXECUTION_CONTRACT_V1.md`",
        "",
        "## Interpretation",
        "",
    ])
    for item in report.get("interpretation", []):
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def _aggregate(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    trades = []
    for row in rows:
        trades.extend(_as_list(_as_dict(row.get("backtest")).get("trades")))
    s = _summary_with_portfolio(trades)
    if trades:
        s["avg_risk_pct"] = sum(_float(_as_dict(t).get("risk_pct")) for t in trades) / len(trades)
        s["avg_risk_used_pct"] = sum(_float(_as_dict(t).get("risk_budget_used_pct")) for t in trades) / len(trades)
        s["avg_filled_tranches"] = sum(_float(_as_dict(t).get("filled_tranches")) for t in trades) / len(trades)
        s.update(_ambiguity_metrics([_as_dict(t) for t in trades]))
    s["fold_count"] = len(rows)
    s["folds_with_trades"] = sum(1 for row in rows if _as_dict(_as_dict(row.get("backtest")).get("summary")).get("trade_count", 0))
    s["closed_trades"] = len(trades)
    return s


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="DCA and risk model walk-forward ablation for canonical Surveyor retests.")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS)
    parser.add_argument("--timeframe", default="1d")
    parser.add_argument("--provider-id", default="binance_public_data")
    parser.add_argument("--venue", default="binance_usdm_futures")
    parser.add_argument("--output-dir", default="artifacts/strategy_backtests/canonical_surveyor_dca_risk")
    parser.add_argument("--liquidsniper-root", default=None)
    parser.add_argument("--max-hold-bars", type=int, default=30)
    parser.add_argument("--targets", default="2,3", help="Comma-separated final RR targets to test, e.g. 2,3,4,5")
    parser.add_argument("--take-profit-plan", default="tp_25_50_25")
    args = parser.parse_args(argv)

    _ensure_liquidsniper_import(args.liquidsniper_root)
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    conn = init_db(args.db_path)
    try:
        candles_by_symbol = {symbol: load_market_candles_from_db(conn, symbol=symbol, timeframe=args.timeframe, provider_id=args.provider_id, venue=args.venue) for symbol in symbols}
    finally:
        conn.close()

    profiles: list[dict[str, Any]] = []
    for symbol in symbols:
        ps, _ = build_retest_profiles_for_symbol(symbol, candles_by_symbol[symbol], CanonicalSurveyorConfig(use_operator_core_bounds=True))
        profiles.extend(ps)
    base_dataset = build_foxian_retest_backtest_dataset(profiles)
    features_by_event = _enrich_profiles(profiles, candles_by_symbol)
    # Add full-zone bounds for DCA ladders.
    for profile in profiles:
        event_id = _profile_event_id(profile)
        zone = _profile_zone(profile)
        full = _as_dict(zone.get("full_zone_bounds"))
        if event_id in features_by_event:
            features_by_event[event_id]["full_zone_low"] = _float(full.get("low"), _float(zone.get("zone_low")))
            features_by_event[event_id]["full_zone_high"] = _float(full.get("high"), _float(zone.get("zone_high")))

    stop_specs = [
        {"stop_policy": "full_zone_5bps", "cap_risk_bps": None},
        {"stop_policy": "sweep_or_zone_adaptive", "cap_risk_bps": None},
    ]
    setup_ids = ["body_p50_selection_p50_family3", "body_p60_selection_p60_family3"]
    risk_models = ["candidate_scaled", "flat_3pct", "graduated_confluence"]
    dca_plans = ["single_100", "dca_50_50", "dca_20_30_50"]
    take_profit_plan = str(args.take_profit_plan)
    targets = [float(item.strip()) for item in str(args.targets).split(",") if item.strip()]
    fold_results: list[dict[str, Any]] = []

    for fold in FOLDS:
        train_end = _parse_ts(str(fold["train_end"]))
        test_start = _parse_ts(str(fold["test_start"]))
        test_end = _parse_ts(str(fold["test_end"]))
        train_dataset = _dataset_subset(base_dataset, lambda ev, train_end=train_end: (_candidate_ts(ev) is not None and int(_candidate_ts(ev) or 0) <= train_end))
        thresholds = _train_thresholds(_as_list(train_dataset.get("evaluations")), features_by_event)
        for stop_spec in stop_specs:
            stopped_dataset, stop_manifest = _dataset_for_policy(base_dataset, profiles, candles_by_symbol, policy=str(stop_spec["stop_policy"]), cap_risk_bps=stop_spec["cap_risk_bps"])
            stopped_test = _dataset_subset(stopped_dataset, lambda ev, s=test_start, e=test_end: (_candidate_ts(ev) is not None and s <= int(_candidate_ts(ev) or 0) <= e))
            for setup_id in setup_ids:
                setup_dataset = _apply_setup_filter(stopped_test, features_by_event, setup_id, thresholds)
                for target in targets:
                    for risk_model in risk_models:
                        for dca_plan in dca_plans:
                            backtest = _run_dca_backtest(setup_dataset, candles_by_symbol, features_by_event, thresholds, dca_plan=dca_plan, risk_model=risk_model, target_rr=target, take_profit_plan=take_profit_plan, max_hold_bars=args.max_hold_bars)
                            fold_results.append({
                                "fold_id": fold["fold_id"],
                                "train_end": fold["train_end"],
                                "test_start": fold["test_start"],
                                "test_end": fold["test_end"],
                                "thresholds": thresholds,
                                "stop_policy": stop_spec["stop_policy"],
                                "setup_id": setup_id,
                                "target": f"{target:g}R",
                                "take_profit_plan": take_profit_plan,
                                "risk_model": risk_model,
                                "dca_plan": dca_plan,
                                "input_candidates": len(_as_list(setup_dataset.get("trade_candidates"))),
                                "stop_manifest": stop_manifest,
                                "backtest": backtest,
                            })

    grouped: dict[tuple[str, str, str, str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in fold_results:
        grouped[(str(row["stop_policy"]), str(row["setup_id"]), str(row["target"]), str(row["take_profit_plan"]), str(row["risk_model"]), str(row["dca_plan"]))].append(row)
    aggregate = []
    for (stop_policy, setup_id, target, take_profit_plan, risk_model, dca_plan), rows in sorted(grouped.items()):
        aggregate.append({
            "stop_policy": stop_policy,
            "setup_id": setup_id,
            "target": target,
            "take_profit_plan": take_profit_plan,
            "risk_model": risk_model,
            "dca_plan": dca_plan,
            "summary": _aggregate(rows),
        })

    report = {
        "contract": "canonical_surveyor_dca_risk_walk_forward_v0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbols": symbols,
        "profile_count": len(profiles),
        "trade_candidate_count": len(_as_list(base_dataset.get("trade_candidates"))),
        "risk_models": risk_models,
        "dca_plans": dca_plans,
        "take_profit_plans": [take_profit_plan],
        "reusable_arbiter_artifacts": {
            "setup_score": "market_arbiter.arbiter.setup_score:score_retest_setup",
            "stop_policy": "market_arbiter.arbiter.stop_policy:resolve_retest_stop",
            "dca_ladder": "market_arbiter.arbiter.dca_execution:planned_dca_entries",
            "take_profit": "market_arbiter.arbiter.take_profit:planned_take_profits",
            "graduated_risk": "market_arbiter.arbiter.dca_execution:graduated_confluence_risk_pct",
            "contract_doc": "docs/ARBITER_RETEST_EXECUTION_CONTRACT_V1.md",
        },
        "fold_results": fold_results,
        "aggregate": aggregate,
        "interpretation": [
            "Prior backtests used single-entry only and no graduated TP; this report compares single-entry vs 50/50 and 20/30/50 DCA ladders with 25/50/25 take-profit mechanics.",
            "Flat 3% risk is included. Candidate-scaled risk is the existing confluence-scaled model. Graduated confluence risk is a new hypothesis based on body, selection score, and family confluence thresholds learned from train windows.",
            "DCA total risk is budgeted across tranches; unfilled tranches leave risk unused, so equity bps and R multiple can diverge from single-entry behavior.",
        ],
    }
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    write_json(outdir / "dca_risk_report.json", report)
    (outdir / "SUMMARY.md").write_text(_summary_markdown(report), encoding="utf-8")
    print(json.dumps({"ok": True, "summary_path": str(outdir / "SUMMARY.md"), "report_path": str(outdir / "dca_risk_report.json"), "profiles": len(profiles), "fold_rows": len(fold_results), "aggregate_rows": len(aggregate)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
