from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from market_arbiter.ops.canonical_surveyor_promotion_gate import DEFAULT_PRIMARY_VARIANT, _ambiguity_signal, _as_dict, _as_list, _float, _variant_rows


CONTRACT = "canonical_surveyor_pair_4h_resolution_probe_v0"


def _parse_ts(value: Any) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _load_candles(conn: sqlite3.Connection, *, symbol: str, timeframe: str, provider_id: str, venue: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT ts_open_ms, datetime(ts_open_ms/1000, 'unixepoch') || 'Z' AS timestamp,
               open, high, low, close, volume
        FROM market_candles
        WHERE symbol = ? AND timeframe = ? AND provider_id = ? AND venue = ?
          AND ts_open_ms >= ? AND ts_open_ms <= ?
        ORDER BY ts_open_ms ASC
        """,
        (symbol, timeframe, provider_id, venue, int(start.timestamp() * 1000), int(end.timestamp() * 1000)),
    ).fetchall()
    return [dict(row) for row in rows]


def _hit(price: float, candle: Mapping[str, Any]) -> bool:
    return _float(candle.get("low")) <= price <= _float(candle.get("high"))


def _tp_price(avg_entry: float, stop: float, side: str, rr: float) -> float:
    risk = avg_entry - stop if side == "long" else stop - avg_entry
    return avg_entry + risk * rr if side == "long" else avg_entry - risk * rr


def _stop_hit(stop: float, candle: Mapping[str, Any], side: str) -> bool:
    return _float(candle.get("low")) <= stop if side == "long" else _float(candle.get("high")) >= stop


def _target_hit(price: float, candle: Mapping[str, Any], side: str) -> bool:
    return _float(candle.get("high")) >= price if side == "long" else _float(candle.get("low")) <= price


def _pnl_r(side: str, fills: Sequence[Mapping[str, Any]], exits: Sequence[Mapping[str, Any]]) -> tuple[float, float]:
    total_units = sum(_float(fill.get("units")) for fill in fills)
    avg_entry = sum(_float(fill.get("entry_price")) * _float(fill.get("units")) for fill in fills) / max(total_units, 1e-9)
    pnl = 0.0
    for exit_part in exits:
        units = _float(exit_part.get("units"))
        price = _float(exit_part.get("exit_price"))
        pnl += ((price - avg_entry) if side == "long" else (avg_entry - price)) * units
    risk_filled = sum(_float(fill.get("risk_dollars")) for fill in fills)
    return pnl, pnl / risk_filled if risk_filled else 0.0


def _simulate_trade_on_4h(trade: Mapping[str, Any], candles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    side = str(trade.get("side") or "").lower()
    stop = _float(trade.get("stop_price"))
    target_rr = _float(trade.get("target_rr"), 4.0)
    source_fills = sorted((_as_dict(fill) for fill in _as_list(trade.get("fills"))), key=lambda fill: int(fill.get("tranche_idx", 0)))
    planned = [
        {
            "tranche_idx": int(fill.get("tranche_idx", idx)),
            "entry_price": _float(fill.get("entry_price")),
            "weight": _float(fill.get("weight")),
            "risk_dollars": _float(fill.get("risk_dollars")),
        }
        for idx, fill in enumerate(source_fills)
    ]
    tp_plan = [(1.0, 0.25, "first_protective_tp"), (2.0, 0.50, "second_tp"), (target_rr, 0.25, "final")]
    if target_rr <= 2.0:
        tp_plan = [(1.0, 0.25, "first_protective_tp"), (target_rr, 0.75, "final")]

    fills: list[dict[str, Any]] = []
    exits: list[dict[str, Any]] = []
    events: list[str] = []
    flags: set[str] = set()
    dca_cancelled = False
    stop_moved = False
    active_stop = stop
    locked_avg: float | None = None
    tp_base_units = 0.0
    remaining_units = 0.0
    next_tp_idx = 0

    if not candles:
        return {"status": "skipped", "reason": "no_4h_candles"}

    exit_reason = "max_hold"
    exit_ts = candles[-1].get("timestamp")
    for idx, candle in enumerate(candles):
        ts = candle.get("timestamp")
        newly: list[int] = []
        if not dca_cancelled:
            for plan in planned:
                if any(int(fill.get("tranche_idx")) == int(plan["tranche_idx"]) for fill in fills):
                    continue
                is_market = int(plan["tranche_idx"]) == 0
                if not is_market and not _hit(_float(plan["entry_price"]), candle):
                    continue
                risk_per_unit = (_float(plan["entry_price"]) - stop) if side == "long" else (stop - _float(plan["entry_price"]))
                if risk_per_unit <= 0:
                    continue
                fill = {**plan, "fill_ts": ts, "fill_idx_4h": idx, "units": _float(plan["risk_dollars"]) / risk_per_unit, "risk_per_unit": risk_per_unit}
                fills.append(fill)
                newly.append(int(plan["tranche_idx"]))
                events.append(f"{ts}: fill t{plan['tranche_idx']} @ {plan['entry_price']:.8g}")
        if not fills:
            continue

        total_units = sum(_float(fill.get("units")) for fill in fills)
        avg_entry = sum(_float(fill.get("entry_price")) * _float(fill.get("units")) for fill in fills) / max(total_units, 1e-9)
        if locked_avg is not None:
            avg_entry = locked_avg
        if not stop_moved:
            tp_base_units = total_units
            remaining_units = total_units
        stop_now = _stop_hit(active_stop, candle, side)
        next_rr = tp_plan[next_tp_idx][0] if next_tp_idx < len(tp_plan) else target_rr
        next_tp = _tp_price(avg_entry, stop, side, next_rr)
        target_now = next_tp_idx < len(tp_plan) and _target_hit(next_tp, candle, side)

        if not stop_moved and stop_now and target_now:
            flags.add("4h_same_candle_stop_and_target")
            exit_reason = "stop_loss"
            exits.append({"role": "stop_loss", "exit_ts": ts, "exit_idx_4h": idx, "exit_price": active_stop, "units": remaining_units, "rr": -1.0})
            events.append(f"{ts}: stop_loss @ {active_stop:.8g} before target under stop-first")
            remaining_units = 0.0
            exit_ts = ts
            break
        if stop_now:
            exit_reason = "breakeven_stop" if stop_moved else "stop_loss"
            exits.append({"role": exit_reason, "exit_ts": ts, "exit_idx_4h": idx, "exit_price": active_stop, "units": remaining_units, "rr": 0.0 if stop_moved else -1.0})
            events.append(f"{ts}: {exit_reason} @ {active_stop:.8g}")
            remaining_units = 0.0
            exit_ts = ts
            break
        if target_now:
            while next_tp_idx < len(tp_plan):
                rr, weight, role = tp_plan[next_tp_idx]
                price = _tp_price(avg_entry, stop, side, rr)
                if not _target_hit(price, candle, side):
                    break
                units = min(remaining_units, tp_base_units * weight)
                if units > 0:
                    exits.append({"role": role, "exit_ts": ts, "exit_idx_4h": idx, "exit_price": price, "units": units, "rr": rr, "weight": weight})
                    remaining_units -= units
                    events.append(f"{ts}: {role} {rr:g}R @ {price:.8g}")
                if next_tp_idx == 0:
                    dca_cancelled = True
                    stop_moved = True
                    locked_avg = avg_entry
                    active_stop = avg_entry
                    if any(item > 0 for item in newly):
                        flags.add("4h_same_candle_limit_fill_and_first_tp")
                next_tp_idx += 1
            if remaining_units <= 1e-9 or next_tp_idx >= len(tp_plan):
                exit_reason = "final_target"
                exit_ts = ts
                remaining_units = 0.0
                break
            if stop_moved and _stop_hit(active_stop, candle, side):
                flags.add("4h_same_candle_tp_and_breakeven_stop")
                exit_reason = "breakeven_stop"
                exits.append({"role": "breakeven_stop", "exit_ts": ts, "exit_idx_4h": idx, "exit_price": active_stop, "units": remaining_units, "rr": 0.0})
                events.append(f"{ts}: breakeven_stop @ {active_stop:.8g}")
                remaining_units = 0.0
                exit_ts = ts
                break

    if remaining_units > 1e-9:
        close = _float(candles[-1].get("close"))
        exits.append({"role": "max_hold", "exit_ts": candles[-1].get("timestamp"), "exit_idx_4h": len(candles) - 1, "exit_price": close, "units": remaining_units, "rr": None})
        events.append(f"{candles[-1].get('timestamp')}: max_hold @ {close:.8g}")
        exit_reason = "max_hold"
        exit_ts = candles[-1].get("timestamp")
        remaining_units = 0.0

    pnl, r_multiple = _pnl_r(side, fills, exits) if fills and exits else (0.0, 0.0)
    risk_used = sum(_float(fill.get("risk_dollars")) for fill in fills) / max(sum(_float(plan.get("risk_dollars")) for plan in planned), 1e-9) * 100.0
    risk_pct = _float(trade.get("risk_pct"))
    net_return_bps = r_multiple * risk_pct * (risk_used / 100.0) * 100.0
    return {
        "status": "closed" if exits else "skipped",
        "exit_reason": exit_reason,
        "exit_ts": exit_ts,
        "net_r_multiple": r_multiple,
        "net_return_bps": net_return_bps,
        "pnl_gross": pnl,
        "risk_budget_used_pct": risk_used,
        "filled_tranches": len(fills),
        "fills": fills,
        "partial_exits": exits,
        "remaining_4h_flags": sorted(flags),
        "events": events,
    }


def build_probe(report: Mapping[str, Any], *, symbol: str, db_path: str, provider_id: str, venue: str, timeframe: str, output_target: str) -> dict[str, Any]:
    variant = {**DEFAULT_PRIMARY_VARIANT, "target": output_target}
    rows = []
    for row in _variant_rows(report, variant):
        item = dict(row)
        item.update(_ambiguity_signal(item))
        if str(item.get("symbol") or "").upper() == symbol.upper() and item.get("has_execution_order_ambiguity"):
            rows.append(item)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    resolved = []
    try:
        for trade in sorted(rows, key=lambda item: str(item.get("entry_ts"))):
            start = _parse_ts(trade.get("entry_ts"))
            end = start + timedelta(days=30)
            candles = _load_candles(conn, symbol=symbol.upper(), timeframe=timeframe, provider_id=provider_id, venue=venue, start=start, end=end)
            lower = _simulate_trade_on_4h(trade, candles)
            resolved.append({
                "entry_event_id": trade.get("entry_event_id"),
                "daily": {
                    "entry_ts": trade.get("entry_ts"),
                    "exit_ts": trade.get("exit_ts"),
                    "exit_reason": trade.get("exit_reason"),
                    "net_r_multiple": trade.get("net_r_multiple"),
                    "net_return_bps": trade.get("net_return_bps"),
                    "risk_budget_used_pct": trade.get("risk_budget_used_pct"),
                    "filled_tranches": trade.get("filled_tranches"),
                    "ambiguity_flags": trade.get("ambiguity_flags"),
                },
                "lower_timeframe": lower,
                "delta": {
                    "net_r_multiple": _float(lower.get("net_r_multiple")) - _float(trade.get("net_r_multiple")),
                    "net_return_bps": _float(lower.get("net_return_bps")) - _float(trade.get("net_return_bps")),
                },
            })
    finally:
        conn.close()
    daily_bps = sum(_float(row["daily"].get("net_return_bps")) for row in resolved)
    lower_bps = sum(_float(row["lower_timeframe"].get("net_return_bps")) for row in resolved)
    return {
        "contract": CONTRACT,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "variant": variant,
        "symbol": symbol.upper(),
        "source": {"db_path": db_path, "provider_id": provider_id, "venue": venue, "timeframe": timeframe},
        "trade_count": len(resolved),
        "summary": {
            "daily_total_bps": daily_bps,
            "lower_timeframe_total_bps": lower_bps,
            "delta_bps": lower_bps - daily_bps,
            "daily_avg_r": sum(_float(row["daily"].get("net_r_multiple")) for row in resolved) / len(resolved) if resolved else 0.0,
            "lower_timeframe_avg_r": sum(_float(row["lower_timeframe"].get("net_r_multiple")) for row in resolved) / len(resolved) if resolved else 0.0,
            "remaining_lower_timeframe_ambiguous_count": sum(1 for row in resolved if row["lower_timeframe"].get("remaining_4h_flags")),
        },
        "trades": resolved,
    }


def _fmt(value: Any, digits: int = 4) -> str:
    return f"{_float(value):.{digits}f}"


def _markdown(report: Mapping[str, Any]) -> str:
    s = _as_dict(report.get("summary"))
    timeframe = str(_as_dict(report.get("source")).get("timeframe") or "lower-timeframe").upper()
    lines = [
        f"# {report.get('symbol')} {timeframe} Hard-Ambiguity Resolution Probe",
        "",
        "Targeted lower-timeframe replay for primary-variant daily trades that were hard execution-order ambiguous.",
        "",
        "## Summary",
        "",
        f"- Trades resolved: `{report.get('trade_count')}`",
        f"- Daily hard-ambiguous total: `{_fmt(s.get('daily_total_bps'), 2)} bps`, avg R `{_fmt(s.get('daily_avg_r'))}`",
        f"- {timeframe} resolved total: `{_fmt(s.get('lower_timeframe_total_bps'), 2)} bps`, avg R `{_fmt(s.get('lower_timeframe_avg_r'))}`",
        f"- Delta: `{_fmt(s.get('delta_bps'), 2)} bps`",
        f"- Remaining {timeframe} ambiguous trades: `{s.get('remaining_lower_timeframe_ambiguous_count')}`",
        "",
        "## Trades",
        "",
        f"| Entry | Daily exit/R/bps | {timeframe} exit/R/bps | Δ bps | {timeframe} flags |",
        "|---|---|---|---:|---|",
    ]
    for row in _as_list(report.get("trades")):
        row = _as_dict(row)
        daily = _as_dict(row.get("daily"))
        lower = _as_dict(row.get("lower_timeframe"))
        flags = ", ".join(str(flag) for flag in _as_list(lower.get("remaining_4h_flags")))
        lines.append(
            f"| {daily.get('entry_ts')} | {daily.get('exit_reason')} / {_fmt(daily.get('net_r_multiple'))} / {_fmt(daily.get('net_return_bps'), 2)} | "
            f"{lower.get('exit_reason')} / {_fmt(lower.get('net_r_multiple'))} / {_fmt(lower.get('net_return_bps'), 2)} | {_fmt(_as_dict(row.get('delta')).get('net_return_bps'), 2)} | `{flags}` |"
        )
    lines.extend(["", "## Read", ""])
    if _float(s.get("lower_timeframe_total_bps")) > _float(s.get("daily_total_bps")):
        lines.append(f"- {timeframe} replay improves this hard-ambiguous slice versus daily stop-first accounting.")
    else:
        lines.append(f"- {timeframe} replay does not improve this hard-ambiguous slice versus daily accounting.")
    if _float(s.get("lower_timeframe_total_bps")) > 0:
        lines.append(f"- The selected pair's hard-ambiguous slice is positive after {timeframe} resolution, so broader lower-timeframe validation is worth pursuing.")
    else:
        lines.append(f"- The selected pair's hard-ambiguous slice remains negative after {timeframe} resolution, so inspect before broadening.")
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Probe one symbol's hard ambiguous daily trades using imported lower-timeframe candles.")
    parser.add_argument("--report", default="artifacts/strategy_backtests/canonical_surveyor_final_target_sweep_2_5r/dca_risk_report.json")
    parser.add_argument("--symbol", default="ADAUSDT")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--provider-id", default="binance_public_data")
    parser.add_argument("--venue", default="binance_usdm_futures")
    parser.add_argument("--timeframe", default="4h")
    parser.add_argument("--primary-target", default="4R")
    parser.add_argument("--output-dir", default="artifacts/strategy_backtests/adausdt_4h_hard_ambiguity_probe")
    args = parser.parse_args(argv)
    source = json.loads(Path(args.report).read_text(encoding="utf-8"))
    probe = build_probe(source, symbol=args.symbol, db_path=args.db_path, provider_id=args.provider_id, venue=args.venue, timeframe=args.timeframe, output_target=args.primary_target)
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    report_path = outdir / "resolution_probe_report.json"
    summary_path = outdir / "SUMMARY.md"
    report_path.write_text(json.dumps(probe, indent=2, sort_keys=True), encoding="utf-8")
    summary_path.write_text(_markdown(probe), encoding="utf-8")
    print(json.dumps({"ok": True, "summary_path": str(summary_path), "report_path": str(report_path), "summary": probe["summary"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
