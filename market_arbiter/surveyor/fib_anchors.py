from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from .phase1_contract import PHASE1_STRUCTURE_CONTRACT, run_phase1_structure_contract
from .structure import Pivot, detect_pivots


@dataclass(frozen=True)
class FibImpulseAnchor:
    bias_side: str
    available: bool
    source: str
    reason: str

    start_array_index: int | None
    end_array_index: int | None

    start_id: str | None
    end_id: str | None

    start_price: float | None
    end_price: float | None

    opposite_end_swept: bool


@dataclass(frozen=True)
class _AggBar:
    index: int
    open: float
    high: float
    low: float
    close: float


@dataclass
class Phase1FibAnchorContext:
    target_tf: str
    step: int
    agg_bars: list[_AggBar]
    phase1_bars: list[dict]
    swings: list[dict]


_TF_TO_MINUTES: dict[str, int] = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "45m": 45,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
    "1w": 10080,
}


def _bar_id(bars: Sequence, arr_idx: int, prefix: str) -> str:
    bar_idx = getattr(bars[arr_idx], "index", arr_idx)
    return f"{prefix}_{bar_idx}"


def _fallback_anchor_indices(bars: Sequence, end_idx: int, bias_side: str) -> int:
    if bias_side == "long":
        return min(range(0, end_idx + 1), key=lambda i: bars[i].low)
    return max(range(0, end_idx + 1), key=lambda i: bars[i].high)


def _opposite_end_swept(
    bars: Sequence,
    end_idx: int,
    bias_side: str,
    *,
    as_of_end_idx: int | None = None,
) -> bool:
    if end_idx is None or end_idx >= len(bars):
        return False

    last_idx = len(bars) - 1 if as_of_end_idx is None else min(as_of_end_idx, len(bars) - 1)
    if end_idx >= last_idx:
        return False

    lookahead = bars[end_idx + 1 : last_idx + 1]

    if bias_side == "long":
        end_candle_low = bars[end_idx].low
        return any(b.low < end_candle_low for b in lookahead)

    end_candle_high = bars[end_idx].high
    return any(b.high > end_candle_high for b in lookahead)


def _pick_anchor_pair(
    bars: Sequence,
    pivots: Iterable[Pivot],
    bias_side: str,
) -> FibImpulseAnchor:
    pivot_list = sorted(list(pivots), key=lambda p: p.index)
    highs = [p for p in pivot_list if p.kind == "high"]
    lows = [p for p in pivot_list if p.kind == "low"]

    if bias_side == "long":
        if not highs:
            return FibImpulseAnchor(
                bias_side=bias_side,
                available=False,
                source="none",
                reason="no_high_pivot",
                start_array_index=None,
                end_array_index=None,
                start_id=None,
                end_id=None,
                start_price=None,
                end_price=None,
                opposite_end_swept=False,
            )

        end = highs[-1]
        start_candidates = [p for p in lows if p.index < end.index]
        if start_candidates:
            start = start_candidates[-1]
            start_idx = start.index
            start_id = _bar_id(bars, start_idx, "pivot_low")
            start_price = start.price
            source = "pivot_pair"
        else:
            start_idx = _fallback_anchor_indices(bars, end.index, bias_side)
            start_id = _bar_id(bars, start_idx, "fallback_low")
            start_price = bars[start_idx].low
            source = "fallback_range_low"

        end_idx = end.index
        end_id = _bar_id(bars, end_idx, "pivot_high")
        end_price = end.price
    else:
        if not lows:
            return FibImpulseAnchor(
                bias_side=bias_side,
                available=False,
                source="none",
                reason="no_low_pivot",
                start_array_index=None,
                end_array_index=None,
                start_id=None,
                end_id=None,
                start_price=None,
                end_price=None,
                opposite_end_swept=False,
            )

        end = lows[-1]
        start_candidates = [p for p in highs if p.index < end.index]
        if start_candidates:
            start = start_candidates[-1]
            start_idx = start.index
            start_id = _bar_id(bars, start_idx, "pivot_high")
            start_price = start.price
            source = "pivot_pair"
        else:
            start_idx = _fallback_anchor_indices(bars, end.index, bias_side)
            start_id = _bar_id(bars, start_idx, "fallback_high")
            start_price = bars[start_idx].high
            source = "fallback_range_high"

        end_idx = end.index
        end_id = _bar_id(bars, end_idx, "pivot_low")
        end_price = end.price

    if start_idx is None or end_idx is None or start_idx >= end_idx:
        return FibImpulseAnchor(
            bias_side=bias_side,
            available=False,
            source=source,
            reason="invalid_anchor_order",
            start_array_index=None,
            end_array_index=None,
            start_id=None,
            end_id=None,
            start_price=None,
            end_price=None,
            opposite_end_swept=False,
        )

    return FibImpulseAnchor(
        bias_side=bias_side,
        available=True,
        source=source,
        reason="ok",
        start_array_index=start_idx,
        end_array_index=end_idx,
        start_id=start_id,
        end_id=end_id,
        start_price=start_price,
        end_price=end_price,
        opposite_end_swept=_opposite_end_swept(bars, end_idx, bias_side),
    )


def _unavailable(bias_side: str, reason: str, source: str = "none") -> FibImpulseAnchor:
    return FibImpulseAnchor(
        bias_side=bias_side,
        available=False,
        source=source,
        reason=reason,
        start_array_index=None,
        end_array_index=None,
        start_id=None,
        end_id=None,
        start_price=None,
        end_price=None,
        opposite_end_swept=False,
    )


def select_latest_impulse_anchor(
    bars: Sequence,
    bias_side: str,
    *,
    pivot_left: int = 2,
    pivot_right: int = 2,
) -> FibImpulseAnchor:
    if bias_side not in ("long", "short"):
        return _unavailable("none", "unsupported_bias")

    if len(bars) < 3:
        return _unavailable(bias_side, "insufficient_bars")

    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    pivots = detect_pivots(highs, lows, left=pivot_left, right=pivot_right)
    return _pick_anchor_pair(bars, pivots, bias_side)


def _tf_step(base_tf: str, target_tf: str) -> int | None:
    base = _TF_TO_MINUTES.get(base_tf)
    target = _TF_TO_MINUTES.get(target_tf)
    if base is None or target is None:
        return None
    if target < base:
        return None
    if target % base != 0:
        return None
    return target // base


def _aggregate_bars(bars: Sequence, step: int) -> list[_AggBar]:
    if step <= 1:
        return [
            _AggBar(
                index=getattr(b, "index", i),
                open=b.open,
                high=b.high,
                low=b.low,
                close=b.close,
            )
            for i, b in enumerate(bars)
        ]

    out: list[_AggBar] = []
    for i in range(0, len(bars), step):
        chunk = bars[i : i + step]
        if len(chunk) < step:
            continue
        out.append(
            _AggBar(
                index=getattr(chunk[-1], "index", i + len(chunk) - 1),
                open=chunk[0].open,
                high=max(b.high for b in chunk),
                low=min(b.low for b in chunk),
                close=chunk[-1].close,
            )
        )
    return out


def select_latest_impulse_anchor_for_timeframe(
    bars: Sequence,
    bias_side: str,
    *,
    base_tf: str,
    target_tf: str,
    pivot_left: int = 2,
    pivot_right: int = 2,
) -> FibImpulseAnchor:
    if bias_side not in ("long", "short"):
        return _unavailable("none", "unsupported_bias")

    step = _tf_step(base_tf, target_tf)
    if step is None:
        return _unavailable(bias_side, "unsupported_tf_ratio", source=f"{base_tf}->{target_tf}")

    agg = _aggregate_bars(bars, step)
    if len(agg) < 3:
        return _unavailable(bias_side, "insufficient_aggregated_bars", source=target_tf)

    anchor = select_latest_impulse_anchor(agg, bias_side, pivot_left=pivot_left, pivot_right=pivot_right)
    return FibImpulseAnchor(
        bias_side=anchor.bias_side,
        available=anchor.available,
        source=f"{target_tf}:{anchor.source}",
        reason=anchor.reason,
        start_array_index=anchor.start_array_index,
        end_array_index=anchor.end_array_index,
        start_id=anchor.start_id,
        end_id=anchor.end_id,
        start_price=anchor.start_price,
        end_price=anchor.end_price,
        opposite_end_swept=anchor.opposite_end_swept,
    )


def _phase1_bias_from_row(row: dict, fallback_bias_side: str) -> tuple[str, str]:
    direction = row.get("regime_direction")
    confidence = row.get("regime_confidence", "unknown")
    if direction == "bullish":
        return "long", confidence
    if direction == "bearish":
        return "short", confidence
    return fallback_bias_side, confidence


def build_phase1_contract_context_for_timeframe(
    bars: Sequence,
    *,
    base_tf: str,
    target_tf: str,
) -> Phase1FibAnchorContext | None:
    step = _tf_step(base_tf, target_tf)
    if step is None:
        return None

    agg = _aggregate_bars(bars, step)
    if len(agg) < 3:
        return Phase1FibAnchorContext(
            target_tf=target_tf,
            step=step,
            agg_bars=agg,
            phase1_bars=[],
            swings=[],
        )

    highs = [b.high for b in agg]
    lows = [b.low for b in agg]
    closes = [b.close for b in agg]
    phase1_bars, _, swings = run_phase1_structure_contract(highs, lows, closes)
    return Phase1FibAnchorContext(
        target_tf=target_tf,
        step=step,
        agg_bars=agg,
        phase1_bars=phase1_bars,
        swings=swings,
    )


def _select_phase1_pair(
    swings: list[dict],
    *,
    bias_side: str,
    as_of_agg_index: int,
) -> tuple[int, float, int, float] | None:
    eligible = [s for s in swings if int(s.get("index", -1)) <= as_of_agg_index]
    if not eligible:
        return None

    if bias_side == "long":
        highs = [s for s in eligible if s.get("kind") == "swing_high"]
        if not highs:
            return None
        end = highs[-1]
        lows = [s for s in eligible if s.get("kind") == "swing_low" and int(s.get("index", -1)) < int(end["index"])]
        if not lows:
            return None
        start = lows[-1]
    else:
        lows = [s for s in eligible if s.get("kind") == "swing_low"]
        if not lows:
            return None
        end = lows[-1]
        highs = [s for s in eligible if s.get("kind") == "swing_high" and int(s.get("index", -1)) < int(end["index"])]
        if not highs:
            return None
        start = highs[-1]

    start_idx = int(start["index"])
    end_idx = int(end["index"])
    start_price = float(start["price"])
    end_price = float(end["price"])
    return start_idx, start_price, end_idx, end_price


def select_phase1_contract_anchor_for_timeframe(
    context: Phase1FibAnchorContext | None,
    *,
    as_of_bar_count: int,
    fallback_bias_side: str,
) -> tuple[FibImpulseAnchor, str, str]:
    if context is None:
        return (
            _unavailable(fallback_bias_side, "unsupported_tf_ratio", source="none"),
            fallback_bias_side,
            "unknown",
        )

    if context.step <= 0:
        return (
            _unavailable(fallback_bias_side, "unsupported_tf_ratio", source=f"{context.target_tf}:phase1_contract"),
            fallback_bias_side,
            "unknown",
        )

    as_of_agg_index = (as_of_bar_count // context.step) - 1
    if as_of_agg_index < 0:
        return (
            _unavailable(fallback_bias_side, "insufficient_aggregated_bars", source=f"{context.target_tf}:phase1_contract"),
            fallback_bias_side,
            "unknown",
        )

    as_of_agg_index = min(as_of_agg_index, len(context.agg_bars) - 1)
    if as_of_agg_index >= len(context.phase1_bars):
        return (
            _unavailable(fallback_bias_side, "phase1_state_unavailable", source=f"{context.target_tf}:phase1_contract"),
            fallback_bias_side,
            "unknown",
        )

    bias_side, confidence = _phase1_bias_from_row(context.phase1_bars[as_of_agg_index], fallback_bias_side)
    pair = _select_phase1_pair(context.swings, bias_side=bias_side, as_of_agg_index=as_of_agg_index)
    if pair is None:
        return (
            _unavailable(bias_side, "phase1_anchor_pair_unavailable", source=f"{context.target_tf}:phase1_contract"),
            bias_side,
            confidence,
        )

    start_idx, start_price, end_idx, end_price = pair
    if start_idx >= end_idx:
        return (
            _unavailable(bias_side, "invalid_anchor_order", source=f"{context.target_tf}:phase1_contract"),
            bias_side,
            confidence,
        )

    start_prefix = "phase1_swing_low" if bias_side == "long" else "phase1_swing_high"
    end_prefix = "phase1_swing_high" if bias_side == "long" else "phase1_swing_low"

    anchor = FibImpulseAnchor(
        bias_side=bias_side,
        available=True,
        source=f"{context.target_tf}:phase1_contract:{PHASE1_STRUCTURE_CONTRACT}",
        reason="ok_phase1_contract",
        start_array_index=start_idx,
        end_array_index=end_idx,
        start_id=_bar_id(context.agg_bars, start_idx, start_prefix),
        end_id=_bar_id(context.agg_bars, end_idx, end_prefix),
        start_price=start_price,
        end_price=end_price,
        opposite_end_swept=_opposite_end_swept(
            context.agg_bars,
            end_idx,
            bias_side,
            as_of_end_idx=as_of_agg_index,
        ),
    )
    return anchor, bias_side, confidence


def tag_anchor_as_debug_fallback(anchor: FibImpulseAnchor, *, target_tf: str) -> FibImpulseAnchor:
    source = anchor.source
    suffix = source.split(":", 1)[1] if source.startswith(f"{target_tf}:") else source
    return FibImpulseAnchor(
        bias_side=anchor.bias_side,
        available=anchor.available,
        source=f"{target_tf}:debug_fallback:{suffix}",
        reason=f"debug_fallback:{anchor.reason}",
        start_array_index=anchor.start_array_index,
        end_array_index=anchor.end_array_index,
        start_id=anchor.start_id,
        end_id=anchor.end_id,
        start_price=anchor.start_price,
        end_price=anchor.end_price,
        opposite_end_swept=anchor.opposite_end_swept,
    )


def compute_fib_level_tap_history_for_timeframe(
    bars: Sequence,
    *,
    base_tf: str,
    target_tf: str,
    anchor: FibImpulseAnchor,
    level_0_618: float | None,
    level_0_705: float | None,
    level_0_786: float | None,
) -> dict:
    out = {
        "level_0_618_tapped_before": False,
        "level_0_705_tapped_before": False,
        "level_0_786_tapped_before": False,
        "level_0_618_tap_count_before": 0,
        "level_0_705_tap_count_before": 0,
        "level_0_786_tap_count_before": 0,
        "band_tapped_before": False,
        "band_tap_count_before": 0,
        "band_first_tap_index": None,
        "band_last_tap_index": None,
    }

    if not anchor.available or anchor.end_array_index is None:
        return out

    if level_0_618 is None or level_0_705 is None or level_0_786 is None:
        return out

    step = _tf_step(base_tf, target_tf)
    if step is None:
        return out

    agg = _aggregate_bars(bars, step)
    if not agg:
        return out

    start = anchor.end_array_index + 1
    if start >= len(agg) - 1:
        return out

    prior = agg[start:-1]
    if not prior:
        return out

    band_low = min(level_0_618, level_0_786)
    band_high = max(level_0_618, level_0_786)

    band_tap_indices: list[int] = []
    c618 = c705 = c786 = 0

    for b in prior:
        hit_618 = b.low <= level_0_618 <= b.high
        hit_705 = b.low <= level_0_705 <= b.high
        hit_786 = b.low <= level_0_786 <= b.high
        hit_band = (b.low <= band_high) and (b.high >= band_low)

        if hit_618:
            c618 += 1
        if hit_705:
            c705 += 1
        if hit_786:
            c786 += 1
        if hit_band:
            band_tap_indices.append(b.index)

    out["level_0_618_tapped_before"] = c618 > 0
    out["level_0_705_tapped_before"] = c705 > 0
    out["level_0_786_tapped_before"] = c786 > 0
    out["level_0_618_tap_count_before"] = c618
    out["level_0_705_tap_count_before"] = c705
    out["level_0_786_tap_count_before"] = c786
    out["band_tapped_before"] = len(band_tap_indices) > 0
    out["band_tap_count_before"] = len(band_tap_indices)
    out["band_first_tap_index"] = band_tap_indices[0] if band_tap_indices else None
    out["band_last_tap_index"] = band_tap_indices[-1] if band_tap_indices else None
    return out
