"""Task 21 candle quality gates + timeframe aggregation policy."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from .market_data import CandleDTO, CandleValidationError


TIMEFRAME_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
    "1w": 604_800_000,
}


@dataclass(frozen=True)
class QualityGateResult:
    candles: list[CandleDTO]
    reason_codes: list[str]


def _d(value: str | int | float | Decimal) -> Decimal:
    return Decimal(str(value))


def _norm_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def enforce_candle_quality(
    candles: Sequence[CandleDTO],
    *,
    timeframe: str,
    now_ms: int,
    max_stale_bars: int = 3,
    check_stale: bool = True,
) -> QualityGateResult:
    """Apply deterministic quality gates and return deduplicated candles."""
    if timeframe not in TIMEFRAME_MS:
        raise CandleValidationError("TIMEFRAME_UNSUPPORTED", f"unsupported timeframe: {timeframe}")
    tf_ms = TIMEFRAME_MS[timeframe]

    by_open: dict[int, CandleDTO] = {}
    reason_codes: list[str] = []
    for candle in candles:
        existing = by_open.get(candle.ts_open_ms)
        if existing is None:
            by_open[candle.ts_open_ms] = candle
            continue

        incoming_sig = (
            str(candle.ts_close_ms),
            str(candle.open),
            str(candle.high),
            str(candle.low),
            str(candle.close),
            str(candle.volume),
            str(candle.dataset_version),
        )
        existing_sig = (
            str(existing.ts_close_ms),
            str(existing.open),
            str(existing.high),
            str(existing.low),
            str(existing.close),
            str(existing.volume),
            str(existing.dataset_version),
        )
        if incoming_sig != existing_sig:
            raise CandleValidationError("CANDLE_DUPLICATE_CONFLICT", f"conflicting duplicate ts_open_ms={candle.ts_open_ms}")
        if "CANDLE_DEDUPED" not in reason_codes:
            reason_codes.append("CANDLE_DEDUPED")

    deduped = sorted(by_open.values(), key=lambda c: c.ts_open_ms)
    if not deduped:
        raise CandleValidationError("CANDLE_GAP_DETECTED", "no candles available for expected window")

    for prev, cur in zip(deduped, deduped[1:]):
        delta = cur.ts_open_ms - prev.ts_open_ms
        if delta <= 0:
            raise CandleValidationError("CANDLE_TS_NOT_MONOTONIC", "timestamps must increase")
        if delta > tf_ms:
            raise CandleValidationError("CANDLE_GAP_DETECTED", f"missing candles between {prev.ts_open_ms} and {cur.ts_open_ms}")

    if check_stale:
        newest_open = deduped[-1].ts_open_ms
        stale_cutoff = now_ms - (tf_ms * max_stale_bars)
        if newest_open < stale_cutoff:
            raise CandleValidationError(
                "CANDLE_STALE_WINDOW",
                f"newest candle {newest_open} is stale vs cutoff {stale_cutoff}",
            )

    return QualityGateResult(candles=deduped, reason_codes=reason_codes)


def aggregate_timeframe_candles(
    candles: Sequence[CandleDTO],
    *,
    from_timeframe: str,
    to_timeframe: str,
    dataset_version: str,
    trace_id: str,
) -> list[CandleDTO]:
    """Derive higher-timeframe candles from lower-timeframe candles when windows are complete."""
    if from_timeframe not in TIMEFRAME_MS or to_timeframe not in TIMEFRAME_MS:
        raise CandleValidationError("TIMEFRAME_UNSUPPORTED", "unsupported aggregation timeframe")

    from_ms = TIMEFRAME_MS[from_timeframe]
    to_ms = TIMEFRAME_MS[to_timeframe]
    if to_ms <= from_ms or (to_ms % from_ms) != 0:
        raise CandleValidationError("AGGREGATION_TIMEFRAME_INVALID", "aggregation must be lower->higher with integer ratio")

    ratio = to_ms // from_ms
    ordered = sorted(candles, key=lambda c: c.ts_open_ms)

    buckets: dict[int, list[CandleDTO]] = {}
    for candle in ordered:
        if candle.timeframe != from_timeframe:
            raise CandleValidationError("AGGREGATION_SOURCE_TIMEFRAME_MISMATCH", "source candle timeframe mismatch")
        if candle.ts_open_ms % from_ms != 0:
            raise CandleValidationError("AGGREGATION_SOURCE_ALIGNMENT_INVALID", "source candle open not aligned")
        bucket_open = (candle.ts_open_ms // to_ms) * to_ms
        buckets.setdefault(bucket_open, []).append(candle)

    out: list[CandleDTO] = []
    for bucket_open in sorted(buckets):
        group = sorted(buckets[bucket_open], key=lambda c: c.ts_open_ms)
        if len(group) != ratio:
            continue
        expected = [bucket_open + (i * from_ms) for i in range(ratio)]
        got = [c.ts_open_ms for c in group]
        if got != expected:
            continue

        out.append(
            CandleDTO(
                provider_id=group[0].provider_id,
                venue=group[0].venue,
                symbol=group[0].symbol,
                timeframe=to_timeframe,
                ts_open_ms=bucket_open,
                ts_close_ms=bucket_open + to_ms,
                open=_norm_decimal(_d(group[0].open)),
                high=_norm_decimal(max(_d(c.high) for c in group)),
                low=_norm_decimal(min(_d(c.low) for c in group)),
                close=_norm_decimal(_d(group[-1].close)),
                volume=_norm_decimal(sum((_d(c.volume) for c in group), Decimal("0"))),
                dataset_version=dataset_version,
                trace_id=trace_id,
            )
        )

    return out
