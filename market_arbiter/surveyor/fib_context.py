from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable, List


class FibTimeframe(str, Enum):
    H4 = "4h"
    D1 = "1d"
    W1 = "1w"


class FibState(str, Enum):
    UNAVAILABLE = "UNAVAILABLE"
    INACTIVE_NOT_READY = "INACTIVE_NOT_READY"
    INACTIVE_OUTSIDE_BAND = "INACTIVE_OUTSIDE_BAND"
    ACTIVE = "ACTIVE"
    DISARMED = "DISARMED"


class FibBandInteraction(str, Enum):
    NONE = "NONE"
    WICK = "WICK"
    BODY_CLOSE = "BODY_CLOSE"


class FibSubZone(str, Enum):
    NONE = "NONE"
    EARLY = "EARLY"
    STRONG = "STRONG"


class FibDisarmReason(str, Enum):
    NONE = "NONE"
    NEW_STRUCTURE_EVENT = "NEW_STRUCTURE_EVENT"
    PASSED_0_886 = "PASSED_0_886"
    ANCHOR_REPLACED = "ANCHOR_REPLACED"


@dataclass(frozen=True)
class FibConfig:
    active_min: float = 0.618
    early_max: float = 0.705
    active_max: float = 0.786
    disarm_ratio: float = 0.886

    score_1d: float = 4.0
    score_4h: float = 3.0
    score_1w_bonus: float = 2.0
    score_strong_bonus: float = 1.0
    score_overlap_bonus: float = 1.0


@dataclass(frozen=True)
class FibTimeframeState:
    timeframe: FibTimeframe
    as_of_index: int
    as_of_ts: str
    fib_state: FibState
    bias_side: str

    anchor_start_id: str | None
    anchor_end_id: str | None
    anchor_start_price: float | None
    anchor_end_price: float | None

    level_0_618: float | None
    level_0_705: float | None
    level_0_786: float | None
    level_0_886: float | None

    band_low: float | None
    band_high: float | None

    band_interaction: FibBandInteraction
    sub_zone: FibSubZone
    passed_0_886: bool
    disarm_reason: FibDisarmReason

    tf_score_contribution: float


@dataclass(frozen=True)
class FibContextState:
    as_of_index: int
    as_of_ts: str
    fib_quality_score: float
    active_timeframes: List[str]
    overlap_cluster: str
    has_1d_4h_overlap: bool
    has_1w_bonus_overlap: bool
    overall_state: str
    overall_reason: str
    timeframes: List[FibTimeframeState]


def _band_interaction(bar_high: float, bar_low: float, bar_close: float, band_low: float, band_high: float) -> FibBandInteraction:
    touched = (bar_low <= band_high) and (bar_high >= band_low)
    if not touched:
        return FibBandInteraction.NONE
    if band_low <= bar_close <= band_high:
        return FibBandInteraction.BODY_CLOSE
    return FibBandInteraction.WICK


def _retracement_ratio_at_price(bias_side: str, impulse_start: float, impulse_end: float, price: float) -> float:
    span = abs(impulse_end - impulse_start)
    if span <= 1e-12:
        return 0.0

    if bias_side == "long":
        return (impulse_end - price) / span

    return (price - impulse_end) / span


def _sub_zone_from_ratio(ratio: float, cfg: FibConfig) -> FibSubZone:
    if cfg.active_min <= ratio < cfg.early_max:
        return FibSubZone.EARLY
    if cfg.early_max <= ratio <= cfg.active_max:
        return FibSubZone.STRONG
    return FibSubZone.NONE


def compute_timeframe_state(
    *,
    timeframe: FibTimeframe,
    as_of_index: int,
    as_of_ts: str,
    bias_side: str,
    anchor_start_id: str | None,
    anchor_end_id: str | None,
    anchor_start_price: float | None,
    anchor_end_price: float | None,
    opposite_end_swept: bool,
    structure_superseded: bool,
    bar_high: float,
    bar_low: float,
    bar_close: float,
    cfg: FibConfig | None = None,
) -> FibTimeframeState:
    cfg = cfg or FibConfig()

    if anchor_start_price is None or anchor_end_price is None or abs(anchor_start_price - anchor_end_price) <= 1e-12:
        return FibTimeframeState(
            timeframe=timeframe,
            as_of_index=as_of_index,
            as_of_ts=as_of_ts,
            fib_state=FibState.UNAVAILABLE,
            bias_side="none",
            anchor_start_id=anchor_start_id,
            anchor_end_id=anchor_end_id,
            anchor_start_price=anchor_start_price,
            anchor_end_price=anchor_end_price,
            level_0_618=None,
            level_0_705=None,
            level_0_786=None,
            level_0_886=None,
            band_low=None,
            band_high=None,
            band_interaction=FibBandInteraction.NONE,
            sub_zone=FibSubZone.NONE,
            passed_0_886=False,
            disarm_reason=FibDisarmReason.NONE,
            tf_score_contribution=0.0,
        )

    if not opposite_end_swept:
        return FibTimeframeState(
            timeframe=timeframe,
            as_of_index=as_of_index,
            as_of_ts=as_of_ts,
            fib_state=FibState.INACTIVE_NOT_READY,
            bias_side=bias_side,
            anchor_start_id=anchor_start_id,
            anchor_end_id=anchor_end_id,
            anchor_start_price=anchor_start_price,
            anchor_end_price=anchor_end_price,
            level_0_618=None,
            level_0_705=None,
            level_0_786=None,
            level_0_886=None,
            band_low=None,
            band_high=None,
            band_interaction=FibBandInteraction.NONE,
            sub_zone=FibSubZone.NONE,
            passed_0_886=False,
            disarm_reason=FibDisarmReason.NONE,
            tf_score_contribution=0.0,
        )

    span = abs(anchor_end_price - anchor_start_price)
    if bias_side == "long":
        level_618 = anchor_end_price - (cfg.active_min * span)
        level_705 = anchor_end_price - (cfg.early_max * span)
        level_786 = anchor_end_price - (cfg.active_max * span)
        level_886 = anchor_end_price - (cfg.disarm_ratio * span)
        penetration_ratio = _retracement_ratio_at_price(bias_side, anchor_start_price, anchor_end_price, bar_low)
    else:
        level_618 = anchor_end_price + (cfg.active_min * span)
        level_705 = anchor_end_price + (cfg.early_max * span)
        level_786 = anchor_end_price + (cfg.active_max * span)
        level_886 = anchor_end_price + (cfg.disarm_ratio * span)
        penetration_ratio = _retracement_ratio_at_price(bias_side, anchor_start_price, anchor_end_price, bar_high)

    band_low = min(level_618, level_786)
    band_high = max(level_618, level_786)
    interaction = _band_interaction(bar_high, bar_low, bar_close, band_low, band_high)
    sub_zone = _sub_zone_from_ratio(penetration_ratio, cfg) if interaction != FibBandInteraction.NONE else FibSubZone.NONE

    passed_0886 = penetration_ratio > cfg.disarm_ratio
    disarm_reason = FibDisarmReason.NONE
    state = FibState.ACTIVE if interaction != FibBandInteraction.NONE else FibState.INACTIVE_OUTSIDE_BAND
    if structure_superseded:
        state = FibState.DISARMED
        disarm_reason = FibDisarmReason.NEW_STRUCTURE_EVENT
    elif passed_0886:
        state = FibState.DISARMED
        disarm_reason = FibDisarmReason.PASSED_0_886

    tf_score = 0.0
    if state == FibState.ACTIVE:
        if timeframe == FibTimeframe.D1:
            tf_score = cfg.score_1d
        elif timeframe == FibTimeframe.H4:
            tf_score = cfg.score_4h
        elif timeframe == FibTimeframe.W1:
            tf_score = cfg.score_1w_bonus
        if sub_zone == FibSubZone.STRONG:
            tf_score += cfg.score_strong_bonus

    return FibTimeframeState(
        timeframe=timeframe,
        as_of_index=as_of_index,
        as_of_ts=as_of_ts,
        fib_state=state,
        bias_side=bias_side,
        anchor_start_id=anchor_start_id,
        anchor_end_id=anchor_end_id,
        anchor_start_price=anchor_start_price,
        anchor_end_price=anchor_end_price,
        level_0_618=level_618,
        level_0_705=level_705,
        level_0_786=level_786,
        level_0_886=level_886,
        band_low=band_low,
        band_high=band_high,
        band_interaction=interaction,
        sub_zone=sub_zone,
        passed_0_886=passed_0886,
        disarm_reason=disarm_reason,
        tf_score_contribution=tf_score,
    )


def aggregate_fib_context(
    *,
    as_of_index: int,
    as_of_ts: str,
    timeframe_states: Iterable[FibTimeframeState],
    cfg: FibConfig | None = None,
) -> FibContextState:
    cfg = cfg or FibConfig()
    states = list(timeframe_states)

    active = [s for s in states if s.fib_state == FibState.ACTIVE]
    active_tfs = [s.timeframe.value for s in active]
    has_1d = any(s.timeframe == FibTimeframe.D1 for s in active)
    has_4h = any(s.timeframe == FibTimeframe.H4 for s in active)
    has_1w = any(s.timeframe == FibTimeframe.W1 for s in active)

    score = sum(s.tf_score_contribution for s in active)
    if has_1d and has_4h:
        score += cfg.score_overlap_bonus
    score = max(0.0, min(10.0, score))

    if len(active) == 0:
        overlap = "none"
        overall_state = "INACTIVE"
        overall_reason = "no_active_fib_timeframes"
    elif len(active) == 1:
        overlap = "single"
        overall_state = "ACTIVE"
        overall_reason = "single_timeframe_fib"
    elif len(active) == 2:
        overlap = "dual"
        overall_state = "ACTIVE"
        overall_reason = "multi_timeframe_fib_overlap"
    else:
        overlap = "triple"
        overall_state = "ACTIVE"
        overall_reason = "triple_timeframe_fib_overlap"

    if not active and any(s.fib_state == FibState.DISARMED for s in states):
        overall_state = "DISARMED"
        overall_reason = "fib_disarmed"

    return FibContextState(
        as_of_index=as_of_index,
        as_of_ts=as_of_ts,
        fib_quality_score=score,
        active_timeframes=active_tfs,
        overlap_cluster=overlap,
        has_1d_4h_overlap=(has_1d and has_4h),
        has_1w_bonus_overlap=has_1w,
        overall_state=overall_state,
        overall_reason=overall_reason,
        timeframes=states,
    )
