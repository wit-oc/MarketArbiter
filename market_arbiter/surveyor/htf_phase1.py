from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

RegimeDirection = Literal["bullish", "bearish"]
RegimeConfidence = Literal["confirmed", "transitional"]
TransitionReason = Literal["bootstrap", "choch_detected", "bos_confirmed"]


@dataclass(frozen=True)
class SwingPoint:
    kind: Literal["high", "low"]
    pivot_index: int
    confirmed_index: int
    price: float


def detect_swings(highs: list[float], lows: list[float], *, left: int = 2, right: int = 2) -> list[SwingPoint]:
    if not highs or not lows or len(highs) != len(lows):
        return []
    n = len(highs)
    swings: list[SwingPoint] = []
    for i in range(left, n - right):
        hi = highs[i]
        lo = lows[i]
        if all(hi >= highs[j] for j in range(i - left, i + right + 1) if j != i):
            swings.append(SwingPoint("high", i, i + right, hi))
        if all(lo <= lows[j] for j in range(i - left, i + right + 1) if j != i):
            swings.append(SwingPoint("low", i, i + right, lo))
    swings.sort(key=lambda s: (s.confirmed_index, 0 if s.kind == "high" else 1, s.pivot_index))
    return swings


def run_phase1_htf_structure(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    *,
    left: int = 2,
    right: int = 2,
    initial_direction: RegimeDirection = "bullish",
    n_init: int = 25,
    break_min_frac_of_candle: float = 0.20,
    choch_break_min_frac_of_candle: float | None = None,
    strict_gating: bool = False,
    bos_require_fresh_cross: bool = True,
    enable_continuation_break: bool = True,
) -> tuple[list[dict], list[dict], list[dict]]:
    if not (len(highs) == len(lows) == len(closes)):
        raise ValueError("highs/lows/closes must have equal length")
    if not highs:
        return [], [], []

    n = len(closes)
    init_len = min(max(1, n_init), n)
    choch_frac = break_min_frac_of_candle if choch_break_min_frac_of_candle is None else choch_break_min_frac_of_candle

    def ema_direction(series: list[float], period: int = 12) -> RegimeDirection:
        alpha = 2.0 / (period + 1.0)
        ema = series[0]
        for px in series[1:]:
            ema = alpha * px + (1.0 - alpha) * ema
        return "bullish" if series[-1] >= ema else "bearish"

    seed_direction = ema_direction(closes[:init_len]) if n >= 2 else initial_direction

    init_high = max(highs[:init_len])
    init_high_idx = highs[:init_len].index(init_high)
    init_low = min(lows[:init_len])
    init_low_idx = lows[:init_len].index(init_low)

    direction: RegimeDirection = seed_direction
    confidence: RegimeConfidence = "confirmed"
    regime_reason: TransitionReason = "bootstrap"

    protected_low = init_low
    protected_low_idx = init_low_idx
    protected_high = init_high
    protected_high_idx = init_high_idx

    validated_high: float | None = init_high if direction == "bullish" else None
    validated_high_idx: int | None = init_high_idx if direction == "bullish" else None
    validated_low: float | None = init_low if direction == "bearish" else None
    validated_low_idx: int | None = init_low_idx if direction == "bearish" else None

    cand_high = highs[0]
    cand_high_idx = 0
    cand_high_low = lows[0]
    cand_high_low_idx = 0

    cand_low = lows[0]
    cand_low_idx = 0
    cand_low_high = highs[0]
    cand_low_high_idx = 0

    last_choch_protected_low_idx: int | None = None
    last_choch_protected_high_idx: int | None = None
    last_bos_bull_ref_idx: int | None = None
    last_bos_bear_ref_idx: int | None = None

    cb_active = False
    cb_dir: RegimeDirection | None = None
    cb_level: float | None = None
    cb_from_idx: int | None = None

    active_choch_level: float | None = None
    active_choch_index: int | None = None

    bars_log: list[dict] = []
    events_log: list[dict] = []
    swings_log: list[dict] = []

    def candle_range(i: int) -> float:
        return max(1e-9, highs[i] - lows[i])

    def emit(i: int, event: str, price: float, *, anchor_idx: int | None = None, reason: TransitionReason | None = None):
        events_log.append(
            {
                "index": i,
                "event": event,
                "price": price,
                "regime_direction": direction,
                "regime_confidence": confidence,
                "transition_reason": reason,
                "anchor_index": i if anchor_idx is None else anchor_idx,
            }
        )

    for i in range(n):
        h, l, c = highs[i], lows[i], closes[i]
        prev_close = closes[i - 1] if i > 0 else closes[0]

        ev_bos = False
        ev_choch = False
        ev_cb = False
        transition_reason: TransitionReason | None = None
        bos_blocked_reason = "none"
        choch_blocked_reason = "none"

        if direction == "bullish":
            if h > cand_high:
                cand_high = h
                cand_high_idx = i
                cand_high_low = l
                cand_high_low_idx = i
                emit(i, "candidate_swing_high", h)

            bull_gate_open = not (validated_high_idx is None or cand_high_idx != validated_high_idx)
            gate_allows = bull_gate_open if strict_gating else True

            if i > cand_high_idx and l <= cand_high_low and h <= cand_high and validated_high_idx != cand_high_idx:
                validated_high = cand_high
                validated_high_idx = cand_high_idx
                swings_log.append({"kind": "swing_high", "index": cand_high_idx, "price": cand_high})
                emit(i, "swing_high_validated_by_sweep", cand_high, anchor_idx=cand_high_idx)

            bos_attempt = validated_high is not None and c > validated_high
            bos_conviction = (
                validated_high is not None and (c - validated_high) >= break_min_frac_of_candle * candle_range(i)
            )
            bos_fresh = validated_high is not None and c > validated_high and prev_close <= validated_high
            bos_deduped = validated_high_idx is not None and last_bos_bull_ref_idx == validated_high_idx

            if bos_attempt and not bos_conviction:
                bos_blocked_reason = "weak_close"
            elif bos_attempt and bos_require_fresh_cross and not bos_fresh:
                bos_blocked_reason = "no_fresh_cross"
            elif bos_attempt and bos_deduped:
                bos_blocked_reason = "deduped"
            elif bos_attempt and strict_gating and not gate_allows:
                bos_blocked_reason = "gate_closed"

            bos_signal = bool(
                bos_attempt
                and bos_conviction
                and (bos_fresh if bos_require_fresh_cross else True)
                and not bos_deduped
            )

            if bos_signal and strict_gating and not gate_allows and enable_continuation_break:
                cb_active = True
                cb_dir = "bullish"
                cb_level = validated_high
                cb_from_idx = validated_high_idx
                ev_cb = True
                emit(i, "cb_pending", c, anchor_idx=validated_high_idx)

            promote_cb = bool(
                cb_active
                and cb_dir == "bullish"
                and bull_gate_open
                and validated_high_idx is not None
                and cb_from_idx == validated_high_idx
                and not bos_deduped
            )

            fire_bos = bos_signal and gate_allows or promote_cb
            if fire_bos and validated_high is not None and validated_high_idx is not None:
                start = validated_high_idx
                window = lows[start : i + 1]
                lock_low = min(window)
                lock_idx = start + window.index(lock_low)
                protected_low = lock_low
                protected_low_idx = lock_idx
                last_bos_bull_ref_idx = validated_high_idx

                swings_log.append({"kind": "swing_low", "index": lock_idx, "price": lock_low})
                ev_bos = True
                emit(i, "bos_confirmed", c, anchor_idx=validated_high_idx, reason="bos_confirmed")
                emit(i, "swing_low_locked", lock_low, anchor_idx=lock_idx)

                confidence = "confirmed"
                regime_reason = "bos_confirmed"
                transition_reason = "bos_confirmed"
                active_choch_level = None
                active_choch_index = None
                cb_active = False
                cb_dir = None
                cb_level = None
                cb_from_idx = None

            choch_attempt = c < protected_low
            choch_conviction = (protected_low - c) >= choch_frac * candle_range(i)
            choch_deduped = last_choch_protected_low_idx == protected_low_idx

            if choch_attempt and not choch_conviction:
                choch_blocked_reason = "weak_close"
            elif choch_attempt and strict_gating and not gate_allows:
                choch_blocked_reason = "gate_closed"
            elif choch_attempt and choch_deduped:
                choch_blocked_reason = "deduped"

            if choch_attempt and choch_conviction and not choch_deduped and gate_allows:
                last_choch_protected_low_idx = protected_low_idx
                choch_from_price = protected_low
                choch_from_idx = protected_low_idx

                direction = "bearish"
                confidence = "transitional"
                regime_reason = "choch_detected"
                transition_reason = "choch_detected"
                active_choch_level = choch_from_price
                active_choch_index = i
                ev_choch = True
                emit(i, "choch_detected", c, anchor_idx=choch_from_idx, reason="choch_detected")

                window = highs[choch_from_idx : i + 1]
                protected_high = max(window)
                protected_high_idx = choch_from_idx + window.index(protected_high)
                emit(i, "swing_high_locked", protected_high, anchor_idx=protected_high_idx)

                cand_low = l
                cand_low_idx = i
                cand_low_high = h
                cand_low_high_idx = i
                validated_low = None
                validated_low_idx = None

                if cb_active and cb_dir == "bearish":
                    cb_active = False
                    cb_dir = None
                    cb_level = None
                    cb_from_idx = None

        else:  # bearish
            if l < cand_low:
                cand_low = l
                cand_low_idx = i
                cand_low_high = h
                cand_low_high_idx = i
                emit(i, "candidate_swing_low", l)

            bear_gate_open = not (validated_low_idx is None or cand_low_idx != validated_low_idx)
            gate_allows = bear_gate_open if strict_gating else True

            if i > cand_low_idx and h >= cand_low_high and l >= cand_low and validated_low_idx != cand_low_idx:
                validated_low = cand_low
                validated_low_idx = cand_low_idx
                swings_log.append({"kind": "swing_low", "index": cand_low_idx, "price": cand_low})
                emit(i, "swing_low_validated_by_sweep", cand_low, anchor_idx=cand_low_idx)

            bos_attempt = validated_low is not None and c < validated_low
            bos_conviction = validated_low is not None and (validated_low - c) >= break_min_frac_of_candle * candle_range(i)
            bos_fresh = validated_low is not None and c < validated_low and prev_close >= validated_low
            bos_deduped = validated_low_idx is not None and last_bos_bear_ref_idx == validated_low_idx

            if bos_attempt and not bos_conviction:
                bos_blocked_reason = "weak_close"
            elif bos_attempt and bos_require_fresh_cross and not bos_fresh:
                bos_blocked_reason = "no_fresh_cross"
            elif bos_attempt and bos_deduped:
                bos_blocked_reason = "deduped"
            elif bos_attempt and strict_gating and not gate_allows:
                bos_blocked_reason = "gate_closed"

            bos_signal = bool(
                bos_attempt
                and bos_conviction
                and (bos_fresh if bos_require_fresh_cross else True)
                and not bos_deduped
            )

            if bos_signal and strict_gating and not gate_allows and enable_continuation_break:
                cb_active = True
                cb_dir = "bearish"
                cb_level = validated_low
                cb_from_idx = validated_low_idx
                ev_cb = True
                emit(i, "cb_pending", c, anchor_idx=validated_low_idx)

            promote_cb = bool(
                cb_active
                and cb_dir == "bearish"
                and bear_gate_open
                and validated_low_idx is not None
                and cb_from_idx == validated_low_idx
                and not bos_deduped
            )

            fire_bos = bos_signal and gate_allows or promote_cb
            if fire_bos and validated_low is not None and validated_low_idx is not None:
                start = validated_low_idx
                window = highs[start : i + 1]
                lock_high = max(window)
                lock_idx = start + window.index(lock_high)
                protected_high = lock_high
                protected_high_idx = lock_idx
                last_bos_bear_ref_idx = validated_low_idx

                swings_log.append({"kind": "swing_high", "index": lock_idx, "price": lock_high})
                ev_bos = True
                emit(i, "bos_confirmed", c, anchor_idx=validated_low_idx, reason="bos_confirmed")
                emit(i, "swing_high_locked", lock_high, anchor_idx=lock_idx)

                confidence = "confirmed"
                regime_reason = "bos_confirmed"
                transition_reason = "bos_confirmed"
                active_choch_level = None
                active_choch_index = None
                cb_active = False
                cb_dir = None
                cb_level = None
                cb_from_idx = None

            choch_attempt = c > protected_high
            choch_conviction = (c - protected_high) >= choch_frac * candle_range(i)
            choch_deduped = last_choch_protected_high_idx == protected_high_idx

            if choch_attempt and not choch_conviction:
                choch_blocked_reason = "weak_close"
            elif choch_attempt and strict_gating and not gate_allows:
                choch_blocked_reason = "gate_closed"
            elif choch_attempt and choch_deduped:
                choch_blocked_reason = "deduped"

            if choch_attempt and choch_conviction and not choch_deduped and gate_allows:
                last_choch_protected_high_idx = protected_high_idx
                choch_from_price = protected_high
                choch_from_idx = protected_high_idx

                direction = "bullish"
                confidence = "transitional"
                regime_reason = "choch_detected"
                transition_reason = "choch_detected"
                active_choch_level = choch_from_price
                active_choch_index = i
                ev_choch = True
                emit(i, "choch_detected", c, anchor_idx=choch_from_idx, reason="choch_detected")

                window = lows[choch_from_idx : i + 1]
                protected_low = min(window)
                protected_low_idx = choch_from_idx + window.index(protected_low)
                emit(i, "swing_low_locked", protected_low, anchor_idx=protected_low_idx)

                cand_high = h
                cand_high_idx = i
                cand_high_low = l
                cand_high_low_idx = i
                validated_high = None
                validated_high_idx = None

                if cb_active and cb_dir == "bullish":
                    cb_active = False
                    cb_dir = None
                    cb_level = None
                    cb_from_idx = None

        bars_log.append(
            {
                "index": i,
                "close": c,
                "regime_direction": direction,
                "regime_confidence": confidence,
                "regime_reason": regime_reason,
                "transition_reason": transition_reason,
                "protected_high": protected_high,
                "protected_high_idx": protected_high_idx,
                "protected_low": protected_low,
                "protected_low_idx": protected_low_idx,
                "validated_high": validated_high,
                "validated_high_idx": validated_high_idx,
                "validated_low": validated_low,
                "validated_low_idx": validated_low_idx,
                "cand_high": cand_high,
                "cand_high_idx": cand_high_idx,
                "cand_low": cand_low,
                "cand_low_idx": cand_low_idx,
                "active_choch_level": active_choch_level,
                "active_choch_index": active_choch_index,
                "bos_check": {
                    "event": ev_bos,
                    "blocked_reason": bos_blocked_reason,
                    "strict_gating": strict_gating,
                    "fresh_cross_required": bos_require_fresh_cross,
                    "cb_active": cb_active,
                },
                "choch_check": {
                    "event": ev_choch,
                    "blocked_reason": choch_blocked_reason,
                    "strict_gating": strict_gating,
                },
                "cb_check": {
                    "event": ev_cb,
                    "active": cb_active,
                    "dir": cb_dir,
                    "level": cb_level,
                    "from_idx": cb_from_idx,
                },
            }
        )

    return bars_log, events_log, swings_log
