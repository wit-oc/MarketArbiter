from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from market_arbiter.ops.canonical_surveyor_promotion_gate import (
    DEFAULT_PRIMARY_VARIANT,
    _ambiguity_signal,
    _as_dict,
    _as_list,
    _float,
    _variant_rows,
)


CONTRACT = "canonical_surveyor_hard_ambiguity_inspection_v0"


def _parse_ts_ms(value: Any) -> int | None:
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return None


def _candle_for(conn: sqlite3.Connection, *, symbol: str, timeframe: str, provider_id: str, venue: str, timestamp: str) -> dict[str, Any] | None:
    ts_open_ms = _parse_ts_ms(timestamp)
    if ts_open_ms is None:
        return None
    row = conn.execute(
        """
        SELECT timestamp, open, high, low, close, volume
        FROM (
            SELECT
                datetime(ts_open_ms / 1000, 'unixepoch') || 'Z' AS timestamp,
                open, high, low, close, volume
            FROM market_candles
            WHERE symbol = ? AND timeframe = ? AND provider_id = ? AND venue = ? AND ts_open_ms = ?
        )
        """,
        (symbol, timeframe, provider_id, venue, ts_open_ms),
    ).fetchone()
    if row is None:
        return None
    candle = dict(row)
    high = _float(candle.get("high"))
    low = _float(candle.get("low"))
    mid = (high + low) / 2.0 if high or low else 0.0
    candle["range_pct"] = ((high - low) / mid * 100.0) if mid else None
    return candle


def _actions_by_timestamp(trade: Mapping[str, Any]) -> dict[str, list[str]]:
    actions: dict[str, list[str]] = {}
    for fill in _as_list(trade.get("fills")):
        fill = _as_dict(fill)
        ts = str(fill.get("fill_ts") or "")
        if not ts:
            continue
        actions.setdefault(ts, []).append(
            f"fill t{fill.get('tranche_idx')} @ {_float(fill.get('entry_price')):.8g} weight {_float(fill.get('weight')):.0%}"
        )
    for exit_part in _as_list(trade.get("partial_exits")):
        exit_part = _as_dict(exit_part)
        ts = str(exit_part.get("exit_ts") or "")
        if not ts:
            continue
        rr = exit_part.get("rr")
        rr_label = "" if rr is None else f" { _float(rr):g}R"
        actions.setdefault(ts, []).append(
            f"{exit_part.get('role')}{rr_label} @ {_float(exit_part.get('exit_price')):.8g}"
        )
    return actions


def _daily_context(conn: sqlite3.Connection, trade: Mapping[str, Any], *, timeframe: str, provider_id: str, venue: str) -> list[dict[str, Any]]:
    symbol = str(trade.get("symbol") or "").upper()
    actions = _actions_by_timestamp(trade)
    rows = []
    for ts in sorted(actions):
        candle = _candle_for(conn, symbol=symbol, timeframe=timeframe, provider_id=provider_id, venue=venue, timestamp=ts)
        rows.append({
            "timestamp": ts,
            "candle": candle,
            "actions": actions[ts],
        })
    return rows


def _trade_summary(trade: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "symbol": trade.get("symbol"),
        "side": trade.get("side"),
        "entry_ts": trade.get("entry_ts"),
        "exit_ts": trade.get("exit_ts"),
        "holding_bars": trade.get("holding_bars"),
        "exit_reason": trade.get("exit_reason"),
        "net_r_multiple": trade.get("net_r_multiple"),
        "net_return_bps": trade.get("net_return_bps"),
        "risk_bps": trade.get("risk_bps"),
        "risk_pct": trade.get("risk_pct"),
        "risk_budget_used_pct": trade.get("risk_budget_used_pct"),
        "filled_tranches": trade.get("filled_tranches"),
        "entry_price": trade.get("entry_price"),
        "stop_price": trade.get("stop_price"),
        "target_price": trade.get("target_price"),
        "ambiguity_flags": trade.get("ambiguity_flags"),
        "entry_event_id": trade.get("entry_event_id"),
    }


def _reason_for(label: str, trade: Mapping[str, Any]) -> str:
    flags = set(str(flag) for flag in _as_list(trade.get("ambiguity_flags")))
    if "same_candle_stop_and_target" in flags:
        return "Daily bar spans stop and target plus DCA fill(s); this is genuine ordering ambiguity and exactly what lower-timeframe replay must resolve."
    if "same_candle_take_profit_and_breakeven_stop" in flags:
        return "Daily bar spans a take-profit and the moved-to-entry stop; current accounting may overstate or understate realized PnL depending on intraday order."
    if "same_candle_limit_fill_and_first_take_profit" in flags:
        return "Daily bar fills deeper DCA and reaches first TP in the same candle; the position size/TP base may be wrong without lower-timeframe order."
    if "same_candle_limit_fill_and_stop" in flags:
        return "Daily bar fills a limit tranche and hits stop in the same candle; conservative accounting may be too punitive if stop happened first."
    return f"Selected as {label}."


def _hard_rows(report: Mapping[str, Any], variant: Mapping[str, Any]) -> list[dict[str, Any]]:
    out = []
    for row in _variant_rows(report, variant):
        item = dict(row)
        signal = _ambiguity_signal(item)
        item.update(signal)
        if signal.get("has_execution_order_ambiguity"):
            out.append(item)
    return out


def _pick_examples(rows: Sequence[Mapping[str, Any]]) -> list[tuple[str, Mapping[str, Any]]]:
    selected: list[tuple[str, Mapping[str, Any]]] = []
    used: set[str] = set()

    def add(label: str, candidates: Sequence[Mapping[str, Any]], *, reverse: bool = False) -> None:
        ordered = sorted(candidates, key=lambda row: _float(row.get("net_return_bps")), reverse=reverse)
        for row in ordered:
            key = str(row.get("entry_event_id") or f"{row.get('symbol')}:{row.get('entry_ts')}:{row.get('exit_ts')}")
            if key in used:
                continue
            selected.append((label, row))
            used.add(key)
            return

    add("worst_stop_loss", [r for r in rows if r.get("exit_reason") == "stop_loss"])
    add("same_bar_stop_loss", [r for r in rows if r.get("exit_reason") == "stop_loss" and r.get("entry_ts") == r.get("exit_ts")])
    add("limit_fill_stop_only", [r for r in rows if _as_list(r.get("ambiguity_flags")) == ["same_candle_limit_fill_and_stop"]])
    add("tp_and_breakeven_stop", [r for r in rows if "same_candle_take_profit_and_breakeven_stop" in _as_list(r.get("ambiguity_flags"))], reverse=True)
    add("best_final_target", [r for r in rows if r.get("exit_reason") == "final_target"], reverse=True)
    return selected


def build_inspection_report(
    source_report: Mapping[str, Any],
    variant: Mapping[str, Any],
    *,
    db_path: str,
    timeframe: str,
    provider_id: str,
    venue: str,
) -> dict[str, Any]:
    hard = _hard_rows(source_report, variant)
    picked = _pick_examples(hard)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        examples = []
        for label, trade in picked:
            examples.append({
                "label": label,
                "reason": _reason_for(label, trade),
                "trade": _trade_summary(trade),
                "daily_context": _daily_context(conn, trade, timeframe=timeframe, provider_id=provider_id, venue=venue),
            })
    finally:
        conn.close()
    by_exit: dict[str, int] = {}
    by_flagset: dict[str, int] = {}
    for row in hard:
        by_exit[str(row.get("exit_reason"))] = by_exit.get(str(row.get("exit_reason")), 0) + 1
        flagset = ", ".join(str(flag) for flag in _as_list(row.get("ambiguity_flags")))
        by_flagset[flagset] = by_flagset.get(flagset, 0) + 1
    return {
        "contract": CONTRACT,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "variant": dict(variant),
        "source": {
            "db_path": db_path,
            "timeframe": timeframe,
            "provider_id": provider_id,
            "venue": venue,
        },
        "hard_ambiguity_count": len(hard),
        "by_exit_reason": dict(sorted(by_exit.items())),
        "by_flagset": dict(sorted(by_flagset.items(), key=lambda item: item[1], reverse=True)),
        "examples": examples,
        "read": [
            "The hard-ambiguous cohort is not one uniform problem: most stop-loss cases are true stop/target/DCA ordering conflicts, while profitable BE/final-target cases often depend on DCA+TP ordering on a wide daily bar.",
            "Manual daily inspection cannot resolve these trades; it can only show whether the ambiguity is real enough to justify lower-timeframe replay.",
            "Because clean + diagnostic-only trades are strongly positive but hard-ambiguous trades decide the full result, targeted lower-timeframe replay on hard cases is justified before a full promotion claim.",
        ],
    }


def _fmt(value: Any, digits: int = 4) -> str:
    if value is None:
        return "n/a"
    return f"{_float(value):.{digits}f}"


def _markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Canonical Surveyor Hard Ambiguity Inspection",
        "",
        "Manual inspection packet for representative execution-order ambiguous trades. This does not resolve intrabar order; it identifies whether ambiguity is real enough to justify lower-timeframe replay.",
        "",
        "## Variant",
        "",
    ]
    for key, value in _as_dict(report.get("variant")).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend([
        "",
        "## Hard ambiguity shape",
        "",
        f"- Hard execution-order ambiguous trades: `{report.get('hard_ambiguity_count')}`",
        f"- By exit reason: `{json.dumps(report.get('by_exit_reason'), sort_keys=True)}`",
        "",
        "### By flagset",
        "",
    ])
    for flagset, count in _as_dict(report.get("by_flagset")).items():
        lines.append(f"- `{count}` × `{flagset}`")
    lines.extend(["", "## Examples", ""])
    for example in _as_list(report.get("examples")):
        example = _as_dict(example)
        trade = _as_dict(example.get("trade"))
        lines.extend([
            f"### {example.get('label')}: {trade.get('symbol')} {trade.get('side')} {trade.get('entry_ts')} → {trade.get('exit_ts')}",
            "",
            f"- Result: `{trade.get('exit_reason')}`, R `{_fmt(trade.get('net_r_multiple'))}`, bps `{_fmt(trade.get('net_return_bps'), 2)}`",
            f"- Risk geometry: risk bps `{_fmt(trade.get('risk_bps'), 2)}`, risk used `{_fmt(trade.get('risk_budget_used_pct'), 2)}%`, filled tranches `{trade.get('filled_tranches')}`",
            f"- Entry/stop/final target: `{_fmt(trade.get('entry_price'), 8)}` / `{_fmt(trade.get('stop_price'), 8)}` / `{_fmt(trade.get('target_price'), 8)}`",
            f"- Flags: `{', '.join(str(flag) for flag in _as_list(trade.get('ambiguity_flags')))}`",
            f"- Read: {example.get('reason')}",
            "",
            "| Date | O | H | L | C | Range | Actions |",
            "|---|---:|---:|---:|---:|---:|---|",
        ])
        for ctx in _as_list(example.get("daily_context")):
            ctx = _as_dict(ctx)
            candle = _as_dict(ctx.get("candle"))
            actions = "; ".join(str(action) for action in _as_list(ctx.get("actions")))
            lines.append(
                f"| {ctx.get('timestamp')} | {_fmt(candle.get('open'), 8)} | {_fmt(candle.get('high'), 8)} | {_fmt(candle.get('low'), 8)} | {_fmt(candle.get('close'), 8)} | {_fmt(candle.get('range_pct'), 2)}% | {actions} |"
            )
        lines.append("")
    lines.extend(["## Read", ""])
    for item in _as_list(report.get("read")):
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect representative hard ambiguous canonical Surveyor trades.")
    parser.add_argument("--report", default="artifacts/strategy_backtests/canonical_surveyor_final_target_sweep_2_5r/dca_risk_report.json")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--output-dir", default="artifacts/strategy_backtests/canonical_surveyor_hard_ambiguity_inspection_4r")
    parser.add_argument("--primary-target", default="4R")
    parser.add_argument("--timeframe", default="1d")
    parser.add_argument("--provider-id", default="binance_public_data")
    parser.add_argument("--venue", default="binance_usdm_futures")
    args = parser.parse_args(argv)

    source_report = json.loads(Path(args.report).read_text(encoding="utf-8"))
    variant = {**DEFAULT_PRIMARY_VARIANT, "target": str(args.primary_target)}
    report = build_inspection_report(
        source_report,
        variant,
        db_path=args.db_path,
        timeframe=args.timeframe,
        provider_id=args.provider_id,
        venue=args.venue,
    )
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    report_path = outdir / "hard_ambiguity_inspection_report.json"
    summary_path = outdir / "SUMMARY.md"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    summary_path.write_text(_markdown(report), encoding="utf-8")
    print(json.dumps({"ok": True, "summary_path": str(summary_path), "report_path": str(report_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
