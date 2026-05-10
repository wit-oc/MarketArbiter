from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Mapping, Sequence

from market_arbiter.core.market_data import CandleDTO, CandleValidationError, normalize_candle, upsert_market_candles

from .blofin import BlofinPublicRestClient


FIVE_DAYS_MS = 5 * 24 * 60 * 60 * 1000
HISTORICAL_REPAIR_POLICY_CONTRACT = "blofin_historical_repair_policy_v1"

HISTORY_REQUIREMENTS = {
    "1w": 520,
    "1d": 1440,
    "4h": 1080,
    "5m": 1440,
}

_TIMEFRAME_TO_BAR = {
    "1w": "1W",
    "1d": "1D",
    "4h": "4H",
    "5m": "5m",
}


class RecoveryMode(str, Enum):
    CLEAN_STARTUP = "clean_startup"
    REPAIR = "repair"
    RESEED = "reseed"


class BlofinRecoveryBlockedError(RuntimeError):
    def __init__(self, result: Mapping[str, Any]):
        super().__init__(f"BloFin recovery blocked: {result.get('symbol')} mode={result.get('mode')}")
        self.result = dict(result)


@dataclass(frozen=True)
class BlofinHydrationPlan:
    symbol: str
    mode: RecoveryMode
    timeframe: str
    bar: str
    max_candles: int


def determine_recovery_mode(*, last_closed_ts_open_ms: int | None, now_ms: int, repair_horizon_ms: int = FIVE_DAYS_MS) -> RecoveryMode:
    if last_closed_ts_open_ms is None:
        return RecoveryMode.CLEAN_STARTUP
    gap_ms = max(0, int(now_ms) - int(last_closed_ts_open_ms))
    if gap_ms <= repair_horizon_ms:
        return RecoveryMode.REPAIR
    return RecoveryMode.RESEED


def build_history_plan(*, symbol: str, mode: RecoveryMode) -> list[BlofinHydrationPlan]:
    plans: list[BlofinHydrationPlan] = []
    if mode is RecoveryMode.REPAIR:
        timeframes: Sequence[str] = ("5m",)
    else:
        timeframes = ("1w", "1d", "4h", "5m")
    for timeframe in timeframes:
        plans.append(
            BlofinHydrationPlan(
                symbol=symbol,
                mode=mode,
                timeframe=timeframe,
                bar=_TIMEFRAME_TO_BAR[timeframe],
                max_candles=HISTORY_REQUIREMENTS[timeframe],
            )
        )
    return plans


def _delete_timeframe_window(conn: sqlite3.Connection, *, symbol: str, timeframe: str) -> None:
    conn.execute(
        "DELETE FROM market_candles WHERE venue = ? AND symbol = ? AND timeframe = ?;",
        ("blofin", symbol, timeframe),
    )
    conn.execute(
        "DELETE FROM feed_checkpoints WHERE venue = ? AND symbol = ? AND timeframe = ?;",
        ("blofin", symbol, timeframe),
    )


def _write_checkpoint(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    timeframe: str,
    candles: Sequence[CandleDTO],
    now_ms: int,
    trace_id: str,
    state: str,
    reason_code: str | None,
) -> None:
    last_open_ms = max((row.ts_open_ms for row in candles), default=None)
    conn.execute(
        """
        INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider_id, venue, symbol, timeframe)
        DO UPDATE SET
            last_ts_open_ms=excluded.last_ts_open_ms,
            last_success_ms=excluded.last_success_ms,
            last_attempt_ms=excluded.last_attempt_ms,
            failure_count=excluded.failure_count,
            state=excluded.state,
            last_reason_code=excluded.last_reason_code,
            trace_id=excluded.trace_id;
        """,
        (
            "blofin_rest",
            "blofin",
            symbol,
            timeframe,
            last_open_ms,
            now_ms if state == "ok" else None,
            now_ms,
            0 if state == "ok" else 1,
            state,
            reason_code,
            trace_id,
        ),
    )


def _write_health_event(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    timeframe: str,
    state: str,
    reason_codes: Sequence[str],
    now_ms: int,
    trace_id: str,
    metadata: Mapping[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO feed_health_events(provider_id, venue, symbol, timeframe, state, reason_codes_json, as_of_ms, trace_id, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "blofin_rest",
            "blofin",
            symbol,
            timeframe,
            state,
            json.dumps(list(reason_codes), sort_keys=True),
            now_ms,
            trace_id,
            json.dumps(dict(metadata), sort_keys=True) if metadata else None,
        ),
    )


def _invalid_sample(candle: CandleDTO, *, reason_code: str, message: str) -> dict[str, Any]:
    return {
        "timeframe": candle.timeframe,
        "ts_open_ms": candle.ts_open_ms,
        "ts_close_ms": candle.ts_close_ms,
        "open": str(candle.open),
        "high": str(candle.high),
        "low": str(candle.low),
        "close": str(candle.close),
        "volume": str(candle.volume),
        "reason_code": reason_code,
        "message": message,
    }


def _count_reason_codes(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        code = str(row.get("reason_code") or "UNKNOWN")
        counts[code] = counts.get(code, 0) + 1
    return counts


def _decimal_or_none(value: str | int | float | Decimal) -> Decimal | None:
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not dec.is_finite():
        return None
    return dec


def _repair_range_invalid_candle(candle: CandleDTO) -> CandleDTO | None:
    open_px = _decimal_or_none(candle.open)
    high_px = _decimal_or_none(candle.high)
    low_px = _decimal_or_none(candle.low)
    close_px = _decimal_or_none(candle.close)
    if None in {open_px, high_px, low_px, close_px}:
        return None

    repaired_high = max(high_px, open_px, close_px)
    repaired_low = min(low_px, open_px, close_px)
    if repaired_high == high_px and repaired_low == low_px:
        return None

    return CandleDTO(
        provider_id=candle.provider_id,
        venue=candle.venue,
        symbol=candle.symbol,
        timeframe=candle.timeframe,
        ts_open_ms=candle.ts_open_ms,
        ts_close_ms=candle.ts_close_ms,
        open=str(candle.open),
        high=format(repaired_high, "f"),
        low=format(repaired_low, "f"),
        close=str(candle.close),
        volume=str(candle.volume),
        dataset_version=str(candle.dataset_version),
        trace_id=str(candle.trace_id),
    )


def _evaluate_candle(candle: CandleDTO, *, ingest_ts_ms: int) -> tuple[CandleDTO | None, dict[str, Any] | None, dict[str, Any] | None]:
    try:
        normalize_candle(candle, ingest_ts_ms=ingest_ts_ms)
        return candle, None, None
    except CandleValidationError as exc:
        if exc.reason_code == "CANDLE_RANGE_INVALID":
            repaired = _repair_range_invalid_candle(candle)
            if repaired is not None:
                try:
                    normalize_candle(repaired, ingest_ts_ms=ingest_ts_ms)
                    return (
                        repaired,
                        {
                            "timeframe": candle.timeframe,
                            "ts_open_ms": candle.ts_open_ms,
                            "ts_close_ms": candle.ts_close_ms,
                            "reason_code": exc.reason_code,
                            "repair_kind": "range_envelope_expand",
                            "raw": {
                                "open": str(candle.open),
                                "high": str(candle.high),
                                "low": str(candle.low),
                                "close": str(candle.close),
                            },
                            "repaired": {
                                "open": str(repaired.open),
                                "high": str(repaired.high),
                                "low": str(repaired.low),
                                "close": str(repaired.close),
                            },
                        },
                        None,
                    )
                except CandleValidationError as repair_exc:
                    return None, None, _invalid_sample(repaired, reason_code=repair_exc.reason_code, message=str(repair_exc))
        return None, None, _invalid_sample(candle, reason_code=exc.reason_code, message=str(exc))


def _repair_quality_band(*, timeframe: str, fetched_count: int, repaired_count: int, unrepairable_count: int, accepted_count: int) -> str:
    if accepted_count == 0:
        return "blocked" if timeframe == "5m" else "degraded"
    if unrepairable_count > 0:
        return "blocked" if timeframe == "5m" else "degraded"
    if repaired_count == 0:
        return "clean"
    repair_rate = repaired_count / max(1, fetched_count)
    if repaired_count <= 1 or repair_rate <= 0.02:
        return "benign"
    if repair_rate <= 0.15:
        return "elevated"
    return "blocked" if timeframe == "5m" else "degraded"


def _quality_reason_code(quality_band: str) -> str | None:
    if quality_band == "clean":
        return None
    return {
        "benign": "HISTORICAL_REPAIR_BENIGN",
        "elevated": "HISTORICAL_REPAIR_ELEVATED",
        "degraded": "HISTORICAL_REPAIR_DEGRADED",
        "blocked": "HISTORICAL_REPAIR_BLOCKED",
    }.get(quality_band)


def _circuit_breaker_action(*, timeframe: str, quality_band: str) -> str:
    if quality_band in {"clean", "benign"}:
        return "accept"
    if quality_band == "elevated":
        return "warn"
    if timeframe == "5m" and quality_band == "blocked":
        return "block_symbol"
    return "quarantine_timeframe"


def _checkpoint_state(*, timeframe: str, quality_band: str) -> str:
    if timeframe == "5m" and quality_band == "blocked":
        return "blocked"
    if quality_band == "degraded":
        return "degraded"
    return "ok"


def _repair_summary(
    *,
    timeframe: str,
    fetched_candles: Sequence[CandleDTO],
    accepted_candles: Sequence[CandleDTO],
    repaired_rows: Sequence[Mapping[str, Any]],
    invalid_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    fetched_count = len(fetched_candles)
    repaired_count = len(repaired_rows)
    accepted_count = len(accepted_candles)
    source_valid_count = max(0, accepted_count - repaired_count)
    unrepairable_count = len(invalid_rows)
    quality_band = _repair_quality_band(
        timeframe=timeframe,
        fetched_count=fetched_count,
        repaired_count=repaired_count,
        unrepairable_count=unrepairable_count,
        accepted_count=accepted_count,
    )
    repair_rate = (repaired_count / fetched_count) if fetched_count else 0.0
    return {
        "contract": "blofin_repair_summary_v1",
        "repair_policy_contract": HISTORICAL_REPAIR_POLICY_CONTRACT,
        "timeframe": timeframe,
        "fetched_candles": fetched_count,
        "accepted_candles": accepted_count,
        "source_valid_candles": source_valid_count,
        "repaired_candles": repaired_count,
        "unrepairable_candles": unrepairable_count,
        "repair_rate": round(repair_rate, 6),
        "repair_reason_counts": _count_reason_codes(repaired_rows),
        "unrepairable_reason_counts": _count_reason_codes(invalid_rows),
        "quality_band": quality_band,
        "circuit_breaker_action": _circuit_breaker_action(timeframe=timeframe, quality_band=quality_band),
        "blocked": timeframe == "5m" and quality_band == "blocked",
    }


class BlofinGapRecoveryEngine:
    def __init__(self, conn: sqlite3.Connection, client: BlofinPublicRestClient) -> None:
        self.conn = conn
        self.client = client

    def recover_symbol(self, *, symbol: str, now_ms: int, trace_id: str, last_closed_ts_open_ms: int | None = None) -> dict[str, Any]:
        mode = determine_recovery_mode(last_closed_ts_open_ms=last_closed_ts_open_ms, now_ms=now_ms)
        plans = build_history_plan(symbol=symbol, mode=mode)
        results: list[dict[str, Any]] = []
        blocked_reason_codes: list[str] = []
        with self.conn:
            for plan in plans:
                _delete_timeframe_window(self.conn, symbol=symbol, timeframe=plan.timeframe)
                fetched_candles = list(
                    self.client.iter_history(
                        inst_id=symbol,
                        bar=plan.bar,
                        max_candles=plan.max_candles,
                        trace_id=trace_id,
                    )
                )
                accepted_candles: list[CandleDTO] = []
                repaired_rows: list[dict[str, Any]] = []
                invalid_rows: list[dict[str, Any]] = []
                for candle in fetched_candles:
                    accepted, repaired_row, invalid_row = _evaluate_candle(candle, ingest_ts_ms=now_ms)
                    if accepted is not None:
                        accepted_candles.append(accepted)
                    if repaired_row is not None:
                        repaired_rows.append(repaired_row)
                    if invalid_row is not None:
                        invalid_rows.append(invalid_row)

                summary = _repair_summary(
                    timeframe=plan.timeframe,
                    fetched_candles=fetched_candles,
                    accepted_candles=accepted_candles,
                    repaired_rows=repaired_rows,
                    invalid_rows=invalid_rows,
                )
                quality_band = str(summary["quality_band"])
                circuit_breaker_action = str(summary["circuit_breaker_action"])
                state = _checkpoint_state(timeframe=plan.timeframe, quality_band=quality_band)
                quality_reason = _quality_reason_code(quality_band)

                reason_codes = sorted(
                    set(
                        [
                            *summary["unrepairable_reason_counts"].keys(),
                            *( ["CANDLE_RANGE_REPAIRED"] if summary["repaired_candles"] else [] ),
                            *( [quality_reason] if quality_reason else [] ),
                        ]
                    )
                )
                if not accepted_candles and "NO_VALID_CANDLES" not in reason_codes:
                    reason_codes.append("NO_VALID_CANDLES")
                    reason_codes = sorted(set(reason_codes))

                if accepted_candles:
                    upsert_market_candles(self.conn, accepted_candles, ingest_ts_ms=now_ms)

                checkpoint_reason = next(
                    (
                        code
                        for code in [quality_reason, *summary["unrepairable_reason_counts"].keys()]
                        if code
                    ),
                    None,
                )
                _write_checkpoint(
                    self.conn,
                    symbol=symbol,
                    timeframe=plan.timeframe,
                    candles=accepted_candles,
                    now_ms=now_ms,
                    trace_id=trace_id,
                    state=state,
                    reason_code=checkpoint_reason,
                )

                metadata = {
                    "repair_policy_contract": HISTORICAL_REPAIR_POLICY_CONTRACT,
                    "quality_band": quality_band,
                    "circuit_breaker_action": circuit_breaker_action,
                    "repair_summary": summary,
                    "repair_provenance": {
                        "provider_id": "blofin_rest",
                        "venue": "blofin",
                        "symbol": symbol,
                        "timeframe": plan.timeframe,
                        "mode": mode.value,
                        "contains_repairs": bool(summary["repaired_candles"]),
                        "contains_unrepairable_rows": bool(summary["unrepairable_candles"]),
                        "trace_id": trace_id,
                    },
                }
                _write_health_event(
                    self.conn,
                    symbol=symbol,
                    timeframe=plan.timeframe,
                    state=state,
                    reason_codes=reason_codes,
                    now_ms=now_ms,
                    trace_id=trace_id,
                    metadata=metadata,
                )
                if circuit_breaker_action == "block_symbol":
                    blocked_reason_codes.extend(summary["unrepairable_reason_counts"].keys())
                    if quality_reason:
                        blocked_reason_codes.append(quality_reason)
                results.append(
                    {
                        "symbol": symbol,
                        "mode": mode.value,
                        "timeframe": plan.timeframe,
                        "bar": plan.bar,
                        "requested_candles": plan.max_candles,
                        "loaded_candles": len(accepted_candles),
                        "fetched_candles": len(fetched_candles),
                        "repaired_candles": len(repaired_rows),
                        "invalid_candles": len(invalid_rows),
                        "state": state,
                        "quality_band": quality_band,
                        "circuit_breaker_action": circuit_breaker_action,
                        "reason_codes": reason_codes,
                        "repair_summary": summary,
                        "repair_samples": repaired_rows[:3],
                        "invalid_samples": invalid_rows[:3],
                    }
                )

            overall_reason_codes = [f"recovery_mode:{mode.value}"]
            if blocked_reason_codes:
                overall_reason_codes.extend(sorted(set(blocked_reason_codes)))
            per_timeframe = {row["timeframe"]: row for row in results}
            overall_5m = per_timeframe.get("5m") or {}
            _write_health_event(
                self.conn,
                symbol=symbol,
                timeframe="5m",
                state=str(overall_5m.get("state") or ("blocked" if blocked_reason_codes else "ok")),
                reason_codes=overall_reason_codes,
                now_ms=now_ms,
                trace_id=trace_id,
                metadata={
                    "repair_policy_contract": HISTORICAL_REPAIR_POLICY_CONTRACT,
                    "quality_band": overall_5m.get("quality_band"),
                    "circuit_breaker_action": overall_5m.get("circuit_breaker_action"),
                    "repair_summary": overall_5m.get("repair_summary"),
                    "repair_provenance": {
                        "provider_id": "blofin_rest",
                        "venue": "blofin",
                        "symbol": symbol,
                        "timeframe": "5m",
                        "mode": mode.value,
                        "trace_id": trace_id,
                    },
                    "symbol_status": "blocked" if blocked_reason_codes else "ok",
                    "blocked_reason_codes": sorted(set(blocked_reason_codes)),
                },
            )
        return {
            "contract": "blofin_gap_recovery_result_v1",
            "repair_policy_contract": HISTORICAL_REPAIR_POLICY_CONTRACT,
            "symbol": symbol,
            "mode": mode.value,
            "status": "blocked" if blocked_reason_codes else "ok",
            "results": results,
            "blocked_reason_codes": sorted(set(blocked_reason_codes)),
            "trace_id": trace_id,
        }
