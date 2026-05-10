from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from market_arbiter.arbiter.ohlcv_backtest import normalize_ohlcv_rows
from market_arbiter.arbiter.strategy_backtest import build_foxian_retest_backtest_dataset
from market_arbiter.surveyor.sr_lifecycle import classify_sr_zone


FAST_OHLCV_RETEST_ADAPTER_CONTRACT = "fast_ohlcv_foxian_retest_adapter_v0"
FAST_OHLCV_RETEST_RUN_CONTRACT = "fast_ohlcv_foxian_retest_run_v0"


@dataclass(frozen=True)
class FastOHLCVRetestAdapterConfig:
    """Heuristic bridge from raw OHLCV to Foxian retest candidates.

    This is intentionally a fast research adapter, not the final Surveyor replay.
    It scans candles chronologically, creates prior-window S/R breakout zones,
    and emits the first confirmed retest back into the flipped zone.
    """

    lookback_bars: int = 30
    retest_window_bars: int = 42
    breakout_buffer_bps: float = 5.0
    zone_width_bps: float = 15.0
    zone_atr_fraction: float = 0.35
    min_zone_width_bps: float = 5.0
    max_active_zones: int = 50


def _format_ts_seconds(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _float(value: Any) -> float:
    return float(value)


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _zone_width(level: float, window: Sequence[Mapping[str, Any]], config: FastOHLCVRetestAdapterConfig) -> float:
    avg_range = _mean([_float(candle["high"]) - _float(candle["low"]) for candle in window])
    return max(
        level * config.min_zone_width_bps / 10_000,
        level * config.zone_width_bps / 10_000,
        avg_range * config.zone_atr_fraction,
    )


def _bias(window: Sequence[Mapping[str, Any]], current_close: float) -> str | None:
    closes = [_float(candle["close"]) for candle in window]
    if not closes:
        return None
    mean_close = _mean(closes)
    if current_close > mean_close:
        return "long"
    if current_close < mean_close:
        return "short"
    return None


def _profile_for_retest(
    *,
    symbol: str,
    timeframe: str,
    event_id: str,
    zone_id: str,
    side: str,
    event_candle: Mapping[str, Any],
    zone_low: float,
    zone_high: float,
    quality_score: float,
    structure_side: str | None,
    breakout_ts: int,
) -> dict[str, Any]:
    event_ts = event_candle["timestamp"]
    role = "support" if side == "long" else "resistance"
    zone_row = {
        "zone_id": zone_id,
        "current_role": role,
        "zone_low": zone_low,
        "zone_high": zone_high,
        "quality_score": quality_score,
        "selection_score": quality_score,
        "historical_context_score": quality_score,
        "formation_reaction_count": 3,
        "retest_count": 1,
        "breakout_ts": _format_ts_seconds(breakout_ts),
        "construction": "prior_window_breakout_then_first_retest",
    }
    zone_row.update(classify_sr_zone(zone_row, policy={"formation_required_for_candidate": True}))
    return {
        "meta": {
            "symbol": symbol,
            "timeframe": timeframe,
            "as_of_ts": event_ts,
            "source_bundle_id": event_id,
            "adapter_contract": FAST_OHLCV_RETEST_ADAPTER_CONTRACT,
        },
        "datasets": {
            "feed_state": {
                "status": "replay_only",
                "payload": {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "event_ts": event_ts,
                    "source": "ohlcv_fast_replay",
                },
            },
            "structure_state": {
                "status": "replay_only",
                "payload": {
                    "directional_bias": structure_side,
                },
            },
            "sr_zones": {
                "status": "replay_only",
                "payload": {
                    "zones": [
                        zone_row
                    ]
                },
            },
            "interaction_lifecycle": {
                "status": "replay_only",
                "payload": {
                    "events": [
                        {
                            "event_id": event_id,
                            "event_type": "confirmed_retest",
                            "confirmation": "confirmed",
                            "event_ts": event_ts,
                            "zone_id": zone_id,
                            "side": side,
                            "retest_index": 1,
                            "price": event_candle.get("close"),
                        }
                    ]
                },
            },
            "fib_context": {"status": "replay_only", "payload": {}},
            "dynamic_levels": {"status": "replay_only", "payload": {}},
        },
    }


def build_fast_ohlcv_retest_profiles(
    candles: Sequence[Mapping[str, Any]],
    *,
    symbol: str,
    timeframe: str = "4h",
    config: FastOHLCVRetestAdapterConfig | None = None,
) -> list[dict[str, Any]]:
    """Emit rough point-in-time profiles from OHLCV breakout/retest patterns."""

    cfg = config or FastOHLCVRetestAdapterConfig()
    normalized = normalize_ohlcv_rows(candles, symbol=symbol)
    active: list[dict[str, Any]] = []
    profiles: list[dict[str, Any]] = []
    emitted_zone_ids: set[str] = set()

    for idx, candle in enumerate(normalized):
        if idx < cfg.lookback_bars:
            continue
        ts = int(candle["ts"])
        close = _float(candle["close"])
        high = _float(candle["high"])
        low = _float(candle["low"])

        still_active: list[dict[str, Any]] = []
        for zone in active:
            if idx - int(zone["breakout_idx"]) > cfg.retest_window_bars:
                continue
            zone_low = float(zone["zone_low"])
            zone_high = float(zone["zone_high"])
            midpoint = (zone_low + zone_high) / 2
            touched = low <= zone_high and high >= zone_low
            confirmed = (zone["side"] == "long" and close > midpoint) or (zone["side"] == "short" and close < midpoint)
            if touched and confirmed and idx > int(zone["breakout_idx"]):
                event_id = f"fast-retest:{symbol}:{timeframe}:{ts}:{zone['side']}"
                profiles.append(
                    _profile_for_retest(
                        symbol=symbol,
                        timeframe=timeframe,
                        event_id=event_id,
                        zone_id=str(zone["zone_id"]),
                        side=str(zone["side"]),
                        event_candle=candle,
                        zone_low=zone_low,
                        zone_high=zone_high,
                        quality_score=float(zone["quality_score"]),
                        structure_side=str(zone["structure_side"]) if zone.get("structure_side") else None,
                        breakout_ts=int(zone["breakout_ts"]),
                    )
                )
                emitted_zone_ids.add(str(zone["zone_id"]))
                continue
            still_active.append(zone)
        active = still_active[-cfg.max_active_zones :]

        window = normalized[idx - cfg.lookback_bars : idx]
        prior_high = max(_float(item["high"]) for item in window)
        prior_low = min(_float(item["low"]) for item in window)
        current_bias = _bias(window, close)
        breakout_buffer_high = prior_high * cfg.breakout_buffer_bps / 10_000
        breakout_buffer_low = prior_low * cfg.breakout_buffer_bps / 10_000

        if close > prior_high + breakout_buffer_high:
            width = _zone_width(prior_high, window, cfg)
            zone_id = f"fast-zone:{symbol}:{timeframe}:{ts}:long:{round(prior_high, 8)}"
            if zone_id not in emitted_zone_ids:
                active.append(
                    {
                        "zone_id": zone_id,
                        "side": "long",
                        "zone_low": prior_high - width,
                        "zone_high": prior_high + width,
                        "quality_score": 0.8 if current_bias == "long" else 0.7,
                        "structure_side": current_bias,
                        "breakout_idx": idx,
                        "breakout_ts": ts,
                    }
                )
        elif close < prior_low - breakout_buffer_low:
            width = _zone_width(prior_low, window, cfg)
            zone_id = f"fast-zone:{symbol}:{timeframe}:{ts}:short:{round(prior_low, 8)}"
            if zone_id not in emitted_zone_ids:
                active.append(
                    {
                        "zone_id": zone_id,
                        "side": "short",
                        "zone_low": prior_low - width,
                        "zone_high": prior_low + width,
                        "quality_score": 0.8 if current_bias == "short" else 0.7,
                        "structure_side": current_bias,
                        "breakout_idx": idx,
                        "breakout_ts": ts,
                    }
                )
        active = active[-cfg.max_active_zones :]

    return profiles


def build_fast_ohlcv_retest_dataset(
    candles: Sequence[Mapping[str, Any]],
    *,
    symbol: str,
    timeframe: str = "4h",
    config: FastOHLCVRetestAdapterConfig | None = None,
) -> dict[str, Any]:
    profiles = build_fast_ohlcv_retest_profiles(candles, symbol=symbol, timeframe=timeframe, config=config)
    dataset = build_foxian_retest_backtest_dataset(profiles)
    dataset["source_adapter"] = {
        "contract": FAST_OHLCV_RETEST_ADAPTER_CONTRACT,
        "symbol": symbol,
        "timeframe": timeframe,
        "profile_count": len(profiles),
        "config": (config or FastOHLCVRetestAdapterConfig()).__dict__,
    }
    return dataset


def load_market_candles_from_db(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    timeframe: str,
    provider_id: str = "binance_public_data",
    venue: str | None = None,
) -> list[dict[str, Any]]:
    params: list[Any] = [provider_id, symbol, timeframe]
    venue_clause = ""
    if venue:
        venue_clause = " AND venue = ?"
        params.append(venue)
    rows = conn.execute(
        f"""
        SELECT ts_open_ms, open, high, low, close, volume
        FROM market_candles
        WHERE provider_id = ? AND symbol = ? AND timeframe = ?{venue_clause}
        ORDER BY ts_open_ms ASC;
        """,
        params,
    ).fetchall()
    return normalize_ohlcv_rows(
        [
            {
                "timestamp": int(ts_open_ms),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "symbol": symbol,
            }
            for ts_open_ms, open_, high, low, close, volume in rows
        ],
        symbol=symbol,
    )


def write_json(path: str | Path, payload: Mapping[str, Any]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
