from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


OHLCV_BACKTEST_REPORT_CONTRACT = "ohlcv_strategy_backtest_report_v0"
EVENT_STUDY_REPORT_CONTRACT = "ohlcv_event_study_report_v0"


@dataclass(frozen=True)
class OHLCVBacktestConfig:
    """Execution assumptions for the first OHLCV-backed simulator.

    The simulator is deliberately deterministic and conservative. It is meant
    to prove the data/simulation path across many pairs before we optimize the
    trading logic.
    """

    timeframe: str = "4h"
    max_hold_bars: int = 180
    target_rr: float | None = None
    same_bar_fill_policy: str = "stop_first"
    default_taker_fee_bps: float = 5.0
    default_slippage_bps: float = 2.0
    default_funding_bps_per_8h: float = 0.0
    event_study_horizons_bars: tuple[int, ...] = (1, 3, 6, 18, 42)
    initial_equity: float = 100_000.0
    default_risk_pct: float = 1.0


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_timestamp(value: Any) -> int:
    """Parse epoch seconds/ms or ISO8601 into epoch seconds."""

    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    if isinstance(value, (int, float)):
        numeric = float(value)
        return int(numeric / 1000 if numeric > 10_000_000_000 else numeric)
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("timestamp is required")
    try:
        numeric = float(raw)
        return int(numeric / 1000 if numeric > 10_000_000_000 else numeric)
    except ValueError:
        pass
    normalized = raw.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_ohlcv_rows(rows: Iterable[Mapping[str, Any]], *, symbol: str | None = None) -> list[dict[str, Any]]:
    """Normalize arbitrary OHLCV rows into sorted canonical candle dicts."""

    normalized: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        timestamp_value = row.get("ts") or row.get("timestamp") or row.get("time") or row.get("datetime") or row.get("date")
        try:
            ts = parse_timestamp(timestamp_value)
        except Exception as exc:  # pragma: no cover - message path tested through loader errors
            raise ValueError(f"invalid timestamp in OHLCV row {idx}: {timestamp_value!r}") from exc
        open_ = _float(row.get("open") or row.get("o"))
        high = _float(row.get("high") or row.get("h"))
        low = _float(row.get("low") or row.get("l"))
        close = _float(row.get("close") or row.get("c"))
        volume = _float(row.get("volume") or row.get("vol") or row.get("v"))
        if None in (open_, high, low, close):
            raise ValueError(f"OHLCV row {idx} missing open/high/low/close")
        if high < low:
            raise ValueError(f"OHLCV row {idx} has high < low")
        normalized.append(
            {
                "ts": ts,
                "timestamp": _format_ts(ts),
                "symbol": str(row.get("symbol") or symbol or ""),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume if volume is not None else 0.0,
            }
        )
    normalized.sort(key=lambda candle: candle["ts"])
    return normalized


def load_ohlcv_csv(path: str | Path, *, symbol: str | None = None) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        return normalize_ohlcv_rows(csv.DictReader(handle), symbol=symbol)


def load_ohlcv_json(path: str | Path, *, symbol: str | None = None) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, Mapping):
        rows = payload.get("candles") or payload.get("ohlcv") or payload.get("rows") or []
        symbol = str(payload.get("symbol") or symbol or "") or None
    else:
        rows = payload
    return normalize_ohlcv_rows(_as_list(rows), symbol=symbol)


def _symbol_from_path(path: Path, timeframe: str) -> str:
    stem = path.stem
    suffixes = (f".{timeframe}", f"_{timeframe}", f"-{timeframe}")
    for suffix in suffixes:
        if stem.endswith(suffix):
            return stem[: -len(suffix)]
    return stem


def load_ohlcv_directory(path: str | Path, *, timeframe: str = "4h", symbols: Sequence[str] | None = None) -> dict[str, list[dict[str, Any]]]:
    """Load per-symbol OHLCV files from a directory.

    Accepted names: `BTCUSDT.5m.csv`, `BTCUSDT_5m.csv`, `BTCUSDT.csv`
    and the same forms for JSON. If `symbols` is supplied, only those symbols
    are loaded and missing symbols are simply absent from the returned mapping.
    """

    root = Path(path)
    wanted = {symbol.upper() for symbol in symbols} if symbols else None
    loaded: dict[str, list[dict[str, Any]]] = {}
    for file_path in sorted(root.iterdir()):
        if not file_path.is_file() or file_path.suffix.lower() not in {".csv", ".json"}:
            continue
        symbol = _symbol_from_path(file_path, timeframe).upper()
        if wanted and symbol not in wanted:
            continue
        if file_path.suffix.lower() == ".csv":
            candles = load_ohlcv_csv(file_path, symbol=symbol)
        else:
            candles = load_ohlcv_json(file_path, symbol=symbol)
        if candles:
            loaded[symbol] = candles
    return loaded


def _find_first_candle_after(candles: Sequence[Mapping[str, Any]], ts: int) -> int | None:
    for idx, candle in enumerate(candles):
        if int(candle["ts"]) > ts:
            return idx
    return None


def _slip(price: float, *, side: str, bps: float, is_entry: bool) -> float:
    rate = bps / 10_000
    if side == "long":
        return price * (1 + rate) if is_entry else price * (1 - rate)
    return price * (1 - rate) if is_entry else price * (1 + rate)


def _candidate_costs(candidate: Mapping[str, Any], config: OHLCVBacktestConfig) -> tuple[float, float, float]:
    cost_model = _as_mapping(candidate.get("cost_model"))
    fee = _float(cost_model.get("taker_fee_bps"))
    slippage = _float(cost_model.get("slippage_bps"))
    funding = _float(cost_model.get("funding_bps_per_8h"))
    return (
        config.default_taker_fee_bps if fee is None else fee,
        config.default_slippage_bps if slippage is None else slippage,
        config.default_funding_bps_per_8h if funding is None else funding,
    )



def _candidate_risk_pct(candidate: Mapping[str, Any], config: OHLCVBacktestConfig) -> float:
    risk_model = _as_mapping(candidate.get("risk_model"))
    risk = _float(risk_model.get("risk_pct"))
    if risk is None:
        risk = _float(candidate.get("risk_pct") or candidate.get("risk_percent"))
    return config.default_risk_pct if risk is None else max(0.0, risk)

def _candidate_target_rr(candidate: Mapping[str, Any], config: OHLCVBacktestConfig) -> float:
    if config.target_rr is not None:
        return float(config.target_rr)
    rr_values = [_float(value) for value in _as_list(candidate.get("target_rr"))]
    rr_values = [value for value in rr_values if value is not None and value > 0]
    return float(rr_values[0]) if rr_values else 1.0


def _stop_price(candidate: Mapping[str, Any], *, side: str) -> float | None:
    invalidation = _float(candidate.get("invalidation_level_hint"))
    if invalidation is None or invalidation <= 0:
        return None
    buffer_bps = _float(candidate.get("stop_buffer_bps")) or 0.0
    if side == "long":
        return invalidation * (1 - buffer_bps / 10_000)
    return invalidation * (1 + buffer_bps / 10_000)


def simulate_trade_candidate(
    candidate: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    *,
    config: OHLCVBacktestConfig | None = None,
) -> dict[str, Any]:
    """Simulate one trade candidate against one symbol's OHLCV candles."""

    cfg = config or OHLCVBacktestConfig()
    symbol = str(candidate.get("symbol") or "").upper()
    side = str(candidate.get("side") or "").lower()
    if side not in {"long", "short"}:
        return {"status": "skipped", "reason": "unsupported_side", "symbol": symbol, "candidate": dict(candidate)}
    if not candles:
        return {"status": "skipped", "reason": "missing_ohlcv", "symbol": symbol, "candidate": dict(candidate)}
    try:
        signal_ts = parse_timestamp(candidate.get("entry_ts"))
    except Exception:
        return {"status": "skipped", "reason": "missing_entry_ts", "symbol": symbol, "candidate": dict(candidate)}
    entry_idx = _find_first_candle_after(candles, signal_ts)
    if entry_idx is None:
        return {"status": "skipped", "reason": "no_candle_after_entry_ts", "symbol": symbol, "candidate": dict(candidate)}

    fee_bps, slippage_bps, funding_bps_per_8h = _candidate_costs(candidate, cfg)
    entry_candle = candles[entry_idx]
    raw_entry = float(entry_candle["open"])
    entry_price = _slip(raw_entry, side=side, bps=slippage_bps, is_entry=True)
    stop = _stop_price(candidate, side=side)
    if stop is None:
        return {"status": "skipped", "reason": "missing_invalidation_level", "symbol": symbol, "candidate": dict(candidate)}
    risk_per_unit = entry_price - stop if side == "long" else stop - entry_price
    if risk_per_unit <= 0:
        return {
            "status": "skipped",
            "reason": "non_positive_risk",
            "symbol": symbol,
            "entry_price": entry_price,
            "stop_price": stop,
            "candidate": dict(candidate),
        }

    rr = _candidate_target_rr(candidate, cfg)
    target = entry_price + risk_per_unit * rr if side == "long" else entry_price - risk_per_unit * rr
    max_exit_idx = min(len(candles) - 1, entry_idx + max(1, cfg.max_hold_bars))
    exit_idx = max_exit_idx
    exit_reason = "max_hold" if max_exit_idx < len(candles) - 1 else "series_end"
    raw_exit = float(candles[exit_idx]["close"])

    for idx in range(entry_idx, max_exit_idx + 1):
        candle = candles[idx]
        high = float(candle["high"])
        low = float(candle["low"])
        if side == "long":
            stop_hit = low <= stop
            target_hit = high >= target
        else:
            stop_hit = high >= stop
            target_hit = low <= target
        if stop_hit and target_hit:
            exit_idx = idx
            exit_reason = "stop_loss" if cfg.same_bar_fill_policy == "stop_first" else "target"
            raw_exit = stop if exit_reason == "stop_loss" else target
            break
        if stop_hit:
            exit_idx = idx
            exit_reason = "stop_loss"
            raw_exit = stop
            break
        if target_hit:
            exit_idx = idx
            exit_reason = "target"
            raw_exit = target
            break

    exit_price = _slip(raw_exit, side=side, bps=slippage_bps, is_entry=False)
    gross_return_bps = ((exit_price - entry_price) / entry_price * 10_000) if side == "long" else ((entry_price - exit_price) / entry_price * 10_000)
    duration_seconds = max(0, int(candles[exit_idx]["ts"]) - int(entry_candle["ts"]))
    funding_bps = funding_bps_per_8h * (duration_seconds / (8 * 60 * 60))
    total_fee_bps = fee_bps * 2
    net_return_bps = gross_return_bps - total_fee_bps - funding_bps
    risk_bps = risk_per_unit / entry_price * 10_000
    risk_pct = _candidate_risk_pct(candidate, cfg)
    risk_dollars = cfg.initial_equity * risk_pct / 100
    position_units = risk_dollars / risk_per_unit if risk_per_unit else 0.0
    notional = position_units * entry_price
    pnl = notional * net_return_bps / 10_000

    return {
        "status": "closed",
        "symbol": symbol,
        "side": side,
        "entry_event_id": candidate.get("entry_event_id"),
        "zone_id": candidate.get("zone_id"),
        "entry_ts": entry_candle["timestamp"],
        "exit_ts": candles[exit_idx]["timestamp"],
        "entry_idx": entry_idx,
        "exit_idx": exit_idx,
        "entry_price": entry_price,
        "stop_price": stop,
        "target_price": target,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "target_rr": rr,
        "gross_return_bps": gross_return_bps,
        "fee_bps": total_fee_bps,
        "slippage_bps_per_side": slippage_bps,
        "funding_bps": funding_bps,
        "net_return_bps": net_return_bps,
        "risk_bps": risk_bps,
        "net_r_multiple": net_return_bps / risk_bps if risk_bps else None,
        "holding_bars": exit_idx - entry_idx,
        "risk_pct": risk_pct,
        "risk_dollars": risk_dollars,
        "position_units": position_units,
        "notional": notional,
        "pnl": pnl,
        "equity_return_pct": pnl / cfg.initial_equity * 100 if cfg.initial_equity else None,
    }


def _max_drawdown_bps(closed_trades: Sequence[Mapping[str, Any]]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for trade in sorted(closed_trades, key=lambda item: str(item.get("exit_ts") or "")):
        equity += float(trade.get("net_return_bps") or 0.0)
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return max_dd


def _summarize_closed_trades(closed_trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    count = len(closed_trades)
    wins = [trade for trade in closed_trades if float(trade.get("net_return_bps") or 0.0) > 0]
    losses = [trade for trade in closed_trades if float(trade.get("net_return_bps") or 0.0) <= 0]
    total = sum(float(trade.get("net_return_bps") or 0.0) for trade in closed_trades)
    r_values = [float(trade.get("net_r_multiple")) for trade in closed_trades if trade.get("net_r_multiple") is not None]
    return {
        "trade_count": count,
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate": len(wins) / count if count else 0.0,
        "total_net_bps": total,
        "avg_net_bps": total / count if count else 0.0,
        "avg_net_r_multiple": sum(r_values) / len(r_values) if r_values else 0.0,
        "max_drawdown_bps": _max_drawdown_bps(closed_trades),
    }


def _symbol_summary(closed_trades: Sequence[Mapping[str, Any]], skipped: Sequence[Mapping[str, Any]], ohlcv_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    symbols = sorted({str(trade.get("symbol") or "").upper() for trade in closed_trades} | {str(trade.get("symbol") or "").upper() for trade in skipped} | set(ohlcv_by_symbol))
    summary: dict[str, Any] = {}
    for symbol in symbols:
        symbol_closed = [trade for trade in closed_trades if str(trade.get("symbol") or "").upper() == symbol]
        symbol_skipped = [trade for trade in skipped if str(trade.get("symbol") or "").upper() == symbol]
        summary[symbol] = {
            **_summarize_closed_trades(symbol_closed),
            "skipped_count": len(symbol_skipped),
            "candle_count": len(ohlcv_by_symbol.get(symbol, [])),
        }
    return summary


def run_ohlcv_backtest(
    backtest_dataset: Mapping[str, Any],
    ohlcv_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    config: OHLCVBacktestConfig | None = None,
) -> dict[str, Any]:
    """Run all emitted strategy trade candidates across many OHLCV series."""

    cfg = config or OHLCVBacktestConfig()
    trades: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for candidate in _as_list(backtest_dataset.get("trade_candidates")):
        symbol = str(_as_mapping(candidate).get("symbol") or "").upper()
        result = simulate_trade_candidate(_as_mapping(candidate), ohlcv_by_symbol.get(symbol, []), config=cfg)
        if result.get("status") == "closed":
            trades.append(result)
        else:
            skipped.append(result)
    return {
        "contract": OHLCV_BACKTEST_REPORT_CONTRACT,
        "ruleset_id": backtest_dataset.get("ruleset_id"),
        "config": {
            "timeframe": cfg.timeframe,
            "max_hold_bars": cfg.max_hold_bars,
            "target_rr": cfg.target_rr,
            "same_bar_fill_policy": cfg.same_bar_fill_policy,
            "event_study_horizons_bars": list(cfg.event_study_horizons_bars),
            "initial_equity": cfg.initial_equity,
            "default_risk_pct": cfg.default_risk_pct,
        },
        "coverage": {
            "input_trade_candidates": len(_as_list(backtest_dataset.get("trade_candidates"))),
            "symbols_with_ohlcv": len(ohlcv_by_symbol),
            "closed_trades": len(trades),
            "skipped_trades": len(skipped),
        },
        "summary": _summarize_closed_trades(trades),
        "by_symbol": _symbol_summary(trades, skipped, ohlcv_by_symbol),
        "trades": trades,
        "skipped": skipped,
    }


def run_event_study(
    backtest_dataset: Mapping[str, Any],
    ohlcv_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    config: OHLCVBacktestConfig | None = None,
) -> dict[str, Any]:
    """Compute forward close-to-close returns after strategy event rows."""

    cfg = config or OHLCVBacktestConfig()
    rows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for event in _as_list(backtest_dataset.get("event_study_rows")):
        event_map = _as_mapping(event)
        symbol = str(event_map.get("symbol") or "").upper()
        candles = list(ohlcv_by_symbol.get(symbol, []))
        try:
            event_ts = parse_timestamp(event_map.get("event_ts") or event_map.get("as_of_ts"))
        except Exception:
            skipped.append({"reason": "missing_event_ts", "event": dict(event_map), "symbol": symbol})
            continue
        start_idx = _find_first_candle_after(candles, event_ts)
        if start_idx is None:
            skipped.append({"reason": "no_candle_after_event_ts", "event": dict(event_map), "symbol": symbol})
            continue
        start_close = float(candles[start_idx]["close"])
        horizon_returns: dict[str, Any] = {}
        for horizon in cfg.event_study_horizons_bars:
            idx = start_idx + horizon
            if idx >= len(candles):
                horizon_returns[f"{horizon}_bars"] = None
                continue
            close = float(candles[idx]["close"])
            direction = 1 if event_map.get("side") == "long" else -1 if event_map.get("side") == "short" else 0
            horizon_returns[f"{horizon}_bars"] = ((close - start_close) / start_close * 10_000 * direction) if direction else None
        rows.append(
            {
                "symbol": symbol,
                "event_id": event_map.get("event_id"),
                "event_ts": candles[start_idx]["timestamp"],
                "side": event_map.get("side"),
                "zone_id": event_map.get("zone_id"),
                "confluence_score": event_map.get("confluence_score"),
                "forward_return_bps": horizon_returns,
            }
        )
    return {
        "contract": EVENT_STUDY_REPORT_CONTRACT,
        "ruleset_id": backtest_dataset.get("ruleset_id"),
        "horizons_bars": list(cfg.event_study_horizons_bars),
        "coverage": {"input_events": len(_as_list(backtest_dataset.get("event_study_rows"))), "rows": len(rows), "skipped": len(skipped)},
        "rows": rows,
        "skipped": skipped,
    }


def serialize_report(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True) + "\n"
