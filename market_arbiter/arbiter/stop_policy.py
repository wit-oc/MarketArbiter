from __future__ import annotations

from typing import Any, Mapping


ARBITER_STOP_POLICY_CONTRACT_V1 = "arbiter_retest_stop_policy_v1"


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def adaptive_stop_buffer(*, reference_price: float, atr: float | None, atr_fraction: float = 0.15, min_bps: float = 10.0) -> float:
    """Return a doctrine-friendly stop buffer outside a zone.

    The buffer should allow ordinary deviation/liquidity hunts, but not create a
    massive invalidation gap by itself. Separate risk geometry guards should cap
    bad trades after this resolver emits a stop.
    """

    atr_part = max(float(atr or 0.0), 0.0) * atr_fraction
    bps_part = max(float(reference_price), 0.0) * min_bps / 10_000.0
    return max(atr_part, bps_part)


def resolve_retest_stop(
    *,
    side: str,
    zone: Mapping[str, Any],
    event_candle: Mapping[str, Any],
    atr: float | None = None,
    policy: str = "full_zone_5bps",
) -> dict[str, Any]:
    """Resolve invalidation stop for an SR retest candidate.

    Supported policies are deliberately small and replay-proven:
    - `full_zone_5bps`: just outside the full SR zone.
    - `full_zone_adaptive`: full SR zone plus ATR/min-bps buffer.
    - `sweep_or_zone_adaptive`: if the retest candle sweeps the full zone and
      reclaims, use the sweep wick plus a smaller adaptive buffer; otherwise use
      full-zone adaptive fallback.
    """

    normalized_side = str(side or "").lower()
    if normalized_side not in {"long", "short"}:
        return {"contract": ARBITER_STOP_POLICY_CONTRACT_V1, "status": "reject", "reason": "unsupported_side"}

    full = zone.get("full_zone_bounds") if isinstance(zone.get("full_zone_bounds"), Mapping) else {}
    zone_low = _float(full.get("low"), _float(zone.get("zone_low")))
    zone_high = _float(full.get("high"), _float(zone.get("zone_high")))
    if zone_high <= zone_low:
        return {"contract": ARBITER_STOP_POLICY_CONTRACT_V1, "status": "reject", "reason": "invalid_zone_bounds"}

    close = _float(event_candle.get("close"), (zone_low + zone_high) / 2.0)
    low = _float(event_candle.get("low"), close)
    high = _float(event_candle.get("high"), close)
    source = ""
    if policy == "full_zone_5bps":
        stop = zone_low * (1 - 5.0 / 10_000.0) if normalized_side == "long" else zone_high * (1 + 5.0 / 10_000.0)
        source = "full_boundary_5bps"
    elif policy == "full_zone_adaptive":
        buffer = adaptive_stop_buffer(reference_price=close, atr=atr, atr_fraction=0.15, min_bps=10.0)
        stop = zone_low - buffer if normalized_side == "long" else zone_high + buffer
        source = "full_boundary_adaptive"
    elif policy == "sweep_or_zone_adaptive":
        sweep_buffer = adaptive_stop_buffer(reference_price=close, atr=atr, atr_fraction=0.10, min_bps=5.0)
        if normalized_side == "long" and low < zone_low:
            stop = low - sweep_buffer
            source = "sweep_wick_low"
        elif normalized_side == "short" and high > zone_high:
            stop = high + sweep_buffer
            source = "sweep_wick_high"
        else:
            buffer = adaptive_stop_buffer(reference_price=close, atr=atr, atr_fraction=0.15, min_bps=10.0)
            stop = zone_low - buffer if normalized_side == "long" else zone_high + buffer
            source = "fallback_full_boundary_adaptive"
    else:
        return {"contract": ARBITER_STOP_POLICY_CONTRACT_V1, "status": "reject", "reason": "unknown_policy", "policy": policy}

    return {
        "contract": ARBITER_STOP_POLICY_CONTRACT_V1,
        "status": "ok",
        "policy": policy,
        "side": normalized_side,
        "stop": stop,
        "stop_source": source,
        "zone_bounds": {"low": zone_low, "high": zone_high},
    }
