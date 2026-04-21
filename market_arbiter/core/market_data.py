"""Task 19 market-data provider contract + canonical candle persistence."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import sqlite3
from typing import Protocol, Sequence


ALLOWED_TIMEFRAMES = {"1m", "5m", "15m", "1h", "4h", "1d", "1w"}


class CandleValidationError(ValueError):
    """Raised when a candle violates Task 19 canonical contract rules."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True)
class CandleDTO:
    provider_id: str
    venue: str
    symbol: str
    timeframe: str
    ts_open_ms: int
    ts_close_ms: int
    open: str | int | float | Decimal
    high: str | int | float | Decimal
    low: str | int | float | Decimal
    close: str | int | float | Decimal
    volume: str | int | float | Decimal
    dataset_version: str
    trace_id: str


@dataclass(frozen=True)
class ProviderHealth:
    provider_id: str
    status: str
    reason_codes: list[str]
    rate_state: str
    as_of_ms: int


class MarketDataProvider(Protocol):
    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since_ms: int | None,
        limit: int,
    ) -> list[CandleDTO]: ...

    def fetch_funding(self, symbol: str, since_ms: int | None, limit: int) -> list[dict]: ...

    def fetch_open_interest(self, symbol: str, since_ms: int | None, limit: int) -> list[dict]: ...

    def provider_health(self) -> ProviderHealth: ...


def _normalize_decimal(value: str | int | float | Decimal) -> str:
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise CandleValidationError("CANDLE_VALUE_INVALID", f"invalid decimal value: {value}")

    if not dec.is_finite():
        raise CandleValidationError("CANDLE_VALUE_INVALID", f"non-finite decimal value: {value}")

    text = format(dec, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _normalize_timeframe(timeframe: str) -> str:
    normalized = str(timeframe).strip().lower()
    if normalized not in ALLOWED_TIMEFRAMES:
        raise CandleValidationError("TIMEFRAME_UNSUPPORTED", f"unsupported timeframe: {timeframe}")
    return normalized


@dataclass(frozen=True)
class CanonicalCandle:
    provider_id: str
    venue: str
    symbol: str
    timeframe: str
    ts_open_ms: int
    ts_close_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    ingest_ts_ms: int
    dataset_version: str
    trace_id: str


def normalize_candle(dto: CandleDTO, *, ingest_ts_ms: int) -> CanonicalCandle:
    timeframe = _normalize_timeframe(dto.timeframe)

    ts_open_ms = int(dto.ts_open_ms)
    ts_close_ms = int(dto.ts_close_ms)
    if ts_close_ms <= ts_open_ms:
        raise CandleValidationError("CANDLE_TS_INVALID", "close timestamp must be greater than open timestamp")

    o = _normalize_decimal(dto.open)
    h = _normalize_decimal(dto.high)
    l = _normalize_decimal(dto.low)
    c = _normalize_decimal(dto.close)
    v = _normalize_decimal(dto.volume)

    o_d, h_d, l_d, c_d = Decimal(o), Decimal(h), Decimal(l), Decimal(c)
    if h_d < max(o_d, c_d) or l_d > min(o_d, c_d):
        raise CandleValidationError("CANDLE_RANGE_INVALID", "ohlc range integrity violation")

    return CanonicalCandle(
        provider_id=str(dto.provider_id).strip(),
        venue=str(dto.venue).strip(),
        symbol=str(dto.symbol).strip(),
        timeframe=timeframe,
        ts_open_ms=ts_open_ms,
        ts_close_ms=ts_close_ms,
        open=o,
        high=h,
        low=l,
        close=c,
        volume=v,
        ingest_ts_ms=int(ingest_ts_ms),
        dataset_version=str(dto.dataset_version).strip(),
        trace_id=str(dto.trace_id).strip(),
    )


def upsert_market_candles(
    conn: sqlite3.Connection,
    candles: Sequence[CandleDTO],
    *,
    ingest_ts_ms: int,
) -> dict[str, int]:
    """Validate + persist canonical candle rows with deterministic conflict behavior."""
    normalized = [normalize_candle(dto, ingest_ts_ms=ingest_ts_ms) for dto in candles]
    normalized.sort(key=lambda c: c.ts_open_ms)

    inserted = 0
    idempotent = 0
    for candle in normalized:
        key = (
            candle.provider_id,
            candle.venue,
            candle.symbol,
            candle.timeframe,
            candle.ts_open_ms,
        )
        existing = conn.execute(
            """
            SELECT ts_close_ms, open, high, low, close, volume, dataset_version
            FROM market_candles
            WHERE provider_id = ? AND venue = ? AND symbol = ? AND timeframe = ? AND ts_open_ms = ?;
            """,
            key,
        ).fetchone()

        if existing:
            incoming = (
                candle.ts_close_ms,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.dataset_version,
            )
            if tuple(str(v) for v in existing) != tuple(str(v) for v in incoming):
                raise CandleValidationError(
                    "CANDLE_DUPLICATE_CONFLICT",
                    f"conflicting duplicate candle for key={key}",
                )
            idempotent += 1
            continue

        conn.execute(
            """
            INSERT INTO market_candles(
                provider_id, venue, symbol, timeframe,
                ts_open_ms, ts_close_ms,
                open, high, low, close, volume,
                ingest_ts_ms, dataset_version, trace_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                candle.provider_id,
                candle.venue,
                candle.symbol,
                candle.timeframe,
                candle.ts_open_ms,
                candle.ts_close_ms,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.ingest_ts_ms,
                candle.dataset_version,
                candle.trace_id,
            ),
        )
        inserted += 1

    return {"inserted": inserted, "idempotent": idempotent}
