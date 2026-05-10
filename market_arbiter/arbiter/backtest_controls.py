from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from market_arbiter.arbiter.ohlcv_backtest import parse_timestamp


ARBITER_BACKTEST_CONTROL_DATASET_CONTRACT_V1 = "arbiter_backtest_control_dataset_v1"


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


@dataclass(frozen=True)
class TimeShiftControlConfig:
    """Deterministic negative control for SR/retest backtests.

    A profitable SR/retest hypothesis should usually degrade when the same
    candidate is moved away from the observed retest timestamp. This control
    keeps symbol, side, stop, risk, and cost assumptions intact while shifting
    event/entry timestamps by a fixed candle count within the same symbol.

    It is intentionally independent of Surveyor level generation, so it can be
    wired before canonical SR bundles are ready.
    """

    shift_bars: int = 20
    control_id: str | None = None
    direction: str = "forward"


def _symbol_candles(ohlcv_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]], symbol: str) -> list[Mapping[str, Any]]:
    candles = list(ohlcv_by_symbol.get(symbol.upper()) or ohlcv_by_symbol.get(symbol) or [])
    return sorted(candles, key=lambda candle: int(candle["ts"]))


def _candle_index_at_or_after(candles: Sequence[Mapping[str, Any]], ts: int) -> int | None:
    for idx, candle in enumerate(candles):
        if int(candle["ts"]) >= ts:
            return idx
    return None


def _shift_timestamp(value: Any, candles: Sequence[Mapping[str, Any]], *, shift_bars: int) -> tuple[str | None, dict[str, Any]]:
    try:
        ts = parse_timestamp(value)
    except Exception:
        return None, {"status": "skipped", "reason": "invalid_timestamp", "source_ts": value}
    idx = _candle_index_at_or_after(candles, ts)
    if idx is None:
        return None, {"status": "skipped", "reason": "source_timestamp_after_series", "source_ts": value}
    shifted_idx = idx + shift_bars
    if shifted_idx < 0 or shifted_idx >= len(candles):
        return None, {
            "status": "skipped",
            "reason": "shift_out_of_range",
            "source_ts": value,
            "source_index": idx,
            "shift_bars": shift_bars,
            "candle_count": len(candles),
        }
    candle = candles[shifted_idx]
    return str(candle.get("timestamp") or candle.get("ts")), {
        "status": "shifted",
        "source_ts": value,
        "source_index": idx,
        "shifted_index": shifted_idx,
        "shift_bars": shift_bars,
    }


def build_time_shift_control_dataset(
    backtest_dataset: Mapping[str, Any],
    ohlcv_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    config: TimeShiftControlConfig | None = None,
) -> dict[str, Any]:
    """Return a deterministic time-shifted negative-control dataset.

    The output keeps the original dataset shape (`trade_candidates` and
    `event_study_rows`) so existing backtest runners can consume it directly.
    Rows that cannot be shifted inside their symbol's candle series are omitted
    and recorded under `control.skipped_rows`.
    """

    cfg = config or TimeShiftControlConfig()
    if cfg.shift_bars == 0:
        raise ValueError("shift_bars must be non-zero for a negative control")
    if cfg.direction not in {"forward", "backward"}:
        raise ValueError("direction must be 'forward' or 'backward'")
    signed_shift = abs(cfg.shift_bars) if cfg.direction == "forward" else -abs(cfg.shift_bars)
    control_id = cfg.control_id or f"time_shift_{cfg.direction}_{abs(cfg.shift_bars)}bars"

    event_rows_by_id = {
        str(_as_dict(row).get("event_id") or ""): _as_dict(row)
        for row in _as_list(backtest_dataset.get("event_study_rows"))
        if _as_dict(row).get("event_id")
    }
    shifted_events: dict[str, dict[str, Any]] = {}
    shifted_candidates: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []

    for raw_candidate in _as_list(backtest_dataset.get("trade_candidates")):
        candidate = deepcopy(_as_dict(raw_candidate))
        symbol = str(candidate.get("symbol") or "").upper()
        candles = _symbol_candles(ohlcv_by_symbol, symbol)
        if not candles:
            skipped_rows.append({"row_type": "trade_candidate", "reason": "missing_symbol_candles", "symbol": symbol, "source": candidate})
            continue
        shifted_entry_ts, shift_meta = _shift_timestamp(candidate.get("entry_ts"), candles, shift_bars=signed_shift)
        if not shifted_entry_ts:
            skipped_rows.append({"row_type": "trade_candidate", "symbol": symbol, **shift_meta, "source": candidate})
            continue

        source_event_id = str(candidate.get("entry_event_id") or "")
        shifted_event_id = f"{source_event_id}:{control_id}" if source_event_id else f"control-event:{symbol}:{len(shifted_candidates)}:{control_id}"
        candidate["entry_event_id"] = shifted_event_id
        candidate["entry_ts"] = shifted_entry_ts
        candidate["control"] = {
            "contract": ARBITER_BACKTEST_CONTROL_DATASET_CONTRACT_V1,
            "control_id": control_id,
            "control_type": "time_shift",
            "direction": cfg.direction,
            "shift_bars": abs(cfg.shift_bars),
            "source_entry_event_id": source_event_id,
            **shift_meta,
        }
        shifted_candidates.append(candidate)

        source_event = event_rows_by_id.get(source_event_id)
        if source_event:
            event = deepcopy(source_event)
            event_ts_key = "event_ts" if "event_ts" in event else "ts"
            shifted_event_ts, event_shift_meta = _shift_timestamp(event.get(event_ts_key), candles, shift_bars=signed_shift)
            if shifted_event_ts:
                event["event_id"] = shifted_event_id
                event[event_ts_key] = shifted_event_ts
                event["control"] = {
                    "contract": ARBITER_BACKTEST_CONTROL_DATASET_CONTRACT_V1,
                    "control_id": control_id,
                    "control_type": "time_shift",
                    "direction": cfg.direction,
                    "shift_bars": abs(cfg.shift_bars),
                    "source_event_id": source_event_id,
                    **event_shift_meta,
                }
                shifted_events[shifted_event_id] = event
            else:
                skipped_rows.append({"row_type": "event_study_row", "symbol": symbol, **event_shift_meta, "source": source_event})

    output = dict(backtest_dataset)
    output["contract"] = ARBITER_BACKTEST_CONTROL_DATASET_CONTRACT_V1
    output["source_contract"] = backtest_dataset.get("contract")
    output["control"] = {
        "contract": ARBITER_BACKTEST_CONTROL_DATASET_CONTRACT_V1,
        "control_id": control_id,
        "control_type": "time_shift",
        "direction": cfg.direction,
        "shift_bars": abs(cfg.shift_bars),
        "source_ruleset_id": backtest_dataset.get("ruleset_id"),
        "input_trade_candidates": len(_as_list(backtest_dataset.get("trade_candidates"))),
        "output_trade_candidates": len(shifted_candidates),
        "input_event_study_rows": len(_as_list(backtest_dataset.get("event_study_rows"))),
        "output_event_study_rows": len(shifted_events),
        "skipped_count": len(skipped_rows),
        "skipped_rows": skipped_rows,
    }
    output["event_study_rows"] = list(shifted_events.values())
    output["trade_candidates"] = shifted_candidates
    return output
