from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

try:
    from enum import StrEnum
except ImportError:  # pragma: no cover - Python < 3.11 compatibility
    from enum import Enum

    class StrEnum(str, Enum):
        def __str__(self) -> str:
            return self.value

from typing import Any, Iterable, Mapping, MutableMapping, Sequence


class LifecycleStatus(StrEnum):
    ACTIVE = "active"
    DEGRADED = "degraded"
    WATCH_ONLY = "watch_only"
    INVALIDATED = "invalidated"
    FLIPPED_PENDING = "flipped_pending"
    SUPERSEDED = "superseded"
    EXPIRED = "expired"
    BLOCKED = "blocked"


class ConfidenceTier(StrEnum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    X = "X"


class DecisionEligibility(StrEnum):
    CANDIDATE_ELIGIBLE = "candidate_eligible"
    WATCH_ELIGIBLE = "watch_eligible"
    WATCH_ONLY = "watch_only"
    DISPLAY_ONLY = "display_only"
    REJECT = "reject"


HARD_STATUSES = {
    LifecycleStatus.INVALIDATED,
    LifecycleStatus.FLIPPED_PENDING,
    LifecycleStatus.EXPIRED,
    LifecycleStatus.BLOCKED,
}

SUPPORT_ROLES = {"support", "flip_support"}
RESISTANCE_ROLES = {"resistance", "flip_resistance"}

DEFAULT_POLICY: dict[str, Any] = {
    "min_bps_by_symbol": 10.0,
    "atr_buffer_fraction": 0.25,
    "width_buffer_fraction": 0.10,
    "min_formation_reactions": 3,
    "formation_required_for_candidate": True,
    "allow_flip_pending_on_confirmed_break": True,
    "max_failed_retests": 2,
    "max_total_retests_for_candidate_use": 3,
    "strong_historical_context": 0.70,
    "weak_historical_context": 0.35,
    "suppress_overlapping_same_side": True,
    "overlap_ratio_for_suppression": 0.50,
}


class SRLifecycleError(ValueError):
    """Raised when SR lifecycle inputs cannot be normalized."""


def classify_sr_zone(
    zone: Mapping[str, Any],
    *,
    current_price: float | None = None,
    confirmation: Mapping[str, Any] | None = None,
    feed_state: Mapping[str, Any] | None = None,
    confluence: Mapping[str, Any] | None = None,
    policy: Mapping[str, Any] | None = None,
    overlap_suppressed: bool = False,
    superseded_by_zone_id: str | None = None,
) -> dict[str, Any]:
    """Return the V0 lifecycle/quality/visual metadata blocks for one SR zone.

    The classifier is deliberately pure and mapping-based so it can sit between
    today's loose Surveyor rows and a later persisted SR schema. It separates
    hard role invalidation from confidence degradation, records reason codes, and
    derives downstream overlay metadata rather than making Pine or Arbiter repeat
    the SR rules.
    """

    cfg = _policy(policy)
    confirmation = confirmation or {}
    feed_state = feed_state or {}
    confluence = confluence or {}

    zone_id = _first_non_empty(zone, "zone_id", "id", "fingerprint")
    role = _normalize_role(_first_non_empty(zone, "role", "current_role", "kind", default="unknown"))
    zone_low, zone_high = _zone_bounds(zone)
    mid = _coerce_float(_first_non_empty(zone, "mid", default=None))
    if mid is None and zone_low is not None and zone_high is not None:
        mid = (zone_low + zone_high) / 2.0

    now = _first_non_empty(zone, "last_evaluated_at", default=None)
    reason_codes: list[str] = []
    degradation_reasons: list[str] = []
    invalidation_reasons: list[str] = []

    hard_status: LifecycleStatus | None = None
    current_role_valid = True
    invalidated_by: str | None = None
    invalidated_at: str | None = None
    flip_candidate = False

    if not zone_id:
        hard_status = LifecycleStatus.BLOCKED
        _add_reason(reason_codes, invalidation_reasons, "blocked_missing_zone_id")
    if zone_low is None or zone_high is None or zone_low >= zone_high:
        hard_status = LifecycleStatus.BLOCKED
        _add_reason(reason_codes, invalidation_reasons, "blocked_missing_zone_bounds")

    feed_status = str(_first_non_empty(feed_state, "status", "state", default="")).lower()
    feed_quality = str(_first_non_empty(feed_state, "quality", "freshness_state", default="")).lower()
    if feed_status in {"blocked", "circuit_breaker", "failed"}:
        hard_status = LifecycleStatus.BLOCKED
        _add_reason(reason_codes, invalidation_reasons, "blocked_feed_quality")
    elif feed_status in {"degraded", "partial", "repair_pending"} or feed_quality in {
        "degraded",
        "partial",
        "stale",
        "elevated",
    }:
        _add_reason(reason_codes, degradation_reasons, "degraded_feed_quality_elevated")

    if _truthy(_first_non_empty(zone, "future_leakage_risk", default=False)):
        hard_status = LifecycleStatus.BLOCKED
        _add_reason(reason_codes, invalidation_reasons, "blocked_future_leakage_risk")

    if _truthy(_first_non_empty(zone, "live_mode", default=False)) and str(
        _first_non_empty(zone, "provenance_mode", "source_mode", default="")
    ).lower() in {"replay", "replay_only", "synthetic_replay"}:
        hard_status = LifecycleStatus.BLOCKED
        _add_reason(reason_codes, invalidation_reasons, "blocked_replay_only_for_live_mode")

    formation_reaction_count = _coerce_int(
        _first_non_empty(zone, "formation_reaction_count", "reaction_count", default=None)
    )
    historical_context_score = _normalize_unit_score(
        _first_non_empty(zone, "historical_context_score", "source_context_score", default=None)
    )
    if formation_reaction_count is None:
        _add_reason(reason_codes, degradation_reasons, "degraded_missing_formation_evidence")
    elif formation_reaction_count < int(cfg["min_formation_reactions"]):
        if cfg["formation_required_for_candidate"]:
            _add_reason(reason_codes, degradation_reasons, "blocked_insufficient_reaction_history")
        else:
            _add_reason(reason_codes, degradation_reasons, "degraded_missing_formation_evidence")
    if historical_context_score is not None and historical_context_score < float(cfg["weak_historical_context"]):
        _add_reason(reason_codes, degradation_reasons, "degraded_weak_historical_context")

    retest_count = _coerce_int(_first_non_empty(zone, "retest_count", default=0)) or 0
    failed_retest_count = _coerce_int(_first_non_empty(zone, "failed_retest_count", default=0)) or 0
    if failed_retest_count >= int(cfg["max_failed_retests"]):
        hard_status = LifecycleStatus.EXPIRED
        _add_reason(reason_codes, invalidation_reasons, "expired_failed_retest_limit")
    elif retest_count == 2:
        _add_reason(reason_codes, degradation_reasons, "degraded_second_retest")
    elif retest_count == 3:
        _add_reason(reason_codes, degradation_reasons, "degraded_third_retest")
    elif retest_count > int(cfg["max_total_retests_for_candidate_use"]):
        _add_reason(reason_codes, degradation_reasons, "degraded_excessive_retests")

    close = _coerce_float(_first_non_empty(confirmation, "close", "confirmed_close", default=None))
    wick_low = _coerce_float(_first_non_empty(confirmation, "low", "wick_low", default=None))
    wick_high = _coerce_float(_first_non_empty(confirmation, "high", "wick_high", default=None))
    buffer = _invalidation_buffer(zone_low, zone_high, mid, confirmation, cfg)
    if hard_status is None and close is not None and zone_low is not None and zone_high is not None:
        if role in SUPPORT_ROLES and close < zone_low - buffer:
            reason = _confirmed_break_reason("support", confirmation)
            hard_status, flip_candidate = _break_status(cfg)
            current_role_valid = False
            invalidated_by = reason
            invalidated_at = _first_non_empty(confirmation, "closed_at", "timestamp", default=now) or _utc_now()
            _add_reason(reason_codes, invalidation_reasons, reason)
        elif role in RESISTANCE_ROLES and close > zone_high + buffer:
            reason = _confirmed_break_reason("resistance", confirmation)
            hard_status, flip_candidate = _break_status(cfg)
            current_role_valid = False
            invalidated_by = reason
            invalidated_at = _first_non_empty(confirmation, "closed_at", "timestamp", default=now) or _utc_now()
            _add_reason(reason_codes, invalidation_reasons, reason)

    wick_breach_reclaimed = _truthy(
        _first_non_empty(confirmation, "wick_breach_reclaimed", "wick_reclaimed", default=False)
    )
    if not wick_breach_reclaimed and close is not None and zone_low is not None and zone_high is not None:
        if role in SUPPORT_ROLES and wick_low is not None and wick_low < zone_low - buffer and close >= zone_low:
            wick_breach_reclaimed = True
        elif role in RESISTANCE_ROLES and wick_high is not None and wick_high > zone_high + buffer and close <= zone_high:
            wick_breach_reclaimed = True
    if hard_status is None and wick_breach_reclaimed:
        _add_reason(reason_codes, degradation_reasons, "degraded_wick_breach_reclaimed")

    repeated_wick_breaches = _coerce_int(
        _first_non_empty(confirmation, "repeated_wick_breach_count", "wick_breach_count", default=0)
    ) or 0
    if hard_status is None and repeated_wick_breaches >= 2:
        _add_reason(reason_codes, degradation_reasons, "degraded_repeated_wick_breaches")

    fakeout_severity = str(_first_non_empty(confirmation, "fakeout_severity", default="none") or "none").lower()
    if hard_status is None and fakeout_severity == "severe":
        _add_reason(reason_codes, degradation_reasons, "degraded_severe_fakeout_wick")
        _add_reason(reason_codes, degradation_reasons, "watch_only_failed_breakout_context")

    if _confluence_state(confluence, "fib") in {"missing", "stale"}:
        _add_reason(reason_codes, degradation_reasons, "degraded_missing_fib_confluence")
    if _confluence_state(confluence, "dynamic_levels") in {"missing", "stale"}:
        _add_reason(reason_codes, degradation_reasons, "degraded_missing_dynamic_level_confluence")
    if _confluence_state(confluence, "htf_structure") in {"misaligned", "stale"}:
        _add_reason(reason_codes, degradation_reasons, "degraded_htf_structure_misaligned")

    if superseded_by_zone_id:
        hard_status = LifecycleStatus.SUPERSEDED
        _add_reason(reason_codes, degradation_reasons, "degraded_overlapping_zone_cluster")

    score = _confidence_score(
        zone,
        historical_context_score=historical_context_score,
        retest_count=retest_count,
        reason_codes=reason_codes,
        degradation_reasons=degradation_reasons,
        invalidation_reasons=invalidation_reasons,
        confluence=confluence,
    )

    status = _status_from_reasons(hard_status, degradation_reasons, reason_codes)
    if status in HARD_STATUSES:
        score = 0.0
        current_role_valid = False
    tier = _tier_for_status_score(status, score)
    decision_eligibility = _eligibility(status, tier, reason_codes)
    visual = _visual_metadata(
        status=status,
        tier=tier,
        score=score,
        source_rank=_coerce_int(_first_non_empty(zone, "source_rank", default=None)),
        overlap_suppressed=overlap_suppressed,
    )
    if overlap_suppressed:
        _add_reason(reason_codes, degradation_reasons, "degraded_overlapping_zone_cluster")
        visual["show_on_overlay"] = False
        visual["suppression_reason"] = "degraded_overlapping_zone_cluster"

    return {
        "zone_id": zone_id,
        "lifecycle": {
            "status": str(status),
            "current_role_valid": current_role_valid,
            "invalidated_at": invalidated_at,
            "invalidated_by": invalidated_by,
            "flip_candidate": flip_candidate,
            "superseded_by_zone_id": superseded_by_zone_id,
            "last_touch_at": _first_non_empty(zone, "last_touch_at", default=None),
            "formation_reaction_count": formation_reaction_count or 0,
            "formation_span_bars": _coerce_int(_first_non_empty(zone, "formation_span_bars", default=0)) or 0,
            "formation_first_seen_at": _first_non_empty(zone, "formation_first_seen_at", default=None),
            "historical_context_score": historical_context_score or 0.0,
            "retest_count": retest_count,
            "failed_retest_count": failed_retest_count,
        },
        "quality": {
            "confidence_score": round(score, 4),
            "confidence_tier": str(tier),
            "decision_eligibility": str(decision_eligibility),
            "reason_codes": reason_codes,
            "degradation_reasons": degradation_reasons,
            "invalidation_reasons": invalidation_reasons,
            "confirmation_timeframe": _first_non_empty(confirmation, "timeframe", "confirmation_timeframe", default=None),
            "confirmation_candle_count": _coerce_int(
                _first_non_empty(confirmation, "confirmation_candle_count", "candle_count", default=0)
            )
            or 0,
            "break_quality": _first_non_empty(confirmation, "break_quality", default=None),
            "wick_breach_size_bps": _coerce_float(
                _first_non_empty(confirmation, "wick_breach_size_bps", default=None)
            ),
            "fakeout_severity": fakeout_severity,
        },
        "visual": visual,
    }


def classify_sr_zones(
    zones: Sequence[Mapping[str, Any]],
    *,
    current_price: float | None = None,
    confirmations_by_zone_id: Mapping[str, Mapping[str, Any]] | None = None,
    feed_state: Mapping[str, Any] | None = None,
    confluence_by_zone_id: Mapping[str, Mapping[str, Any]] | None = None,
    policy: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Classify zones and deterministically suppress lower-priority overlap clutter."""

    cfg = _policy(policy)
    confirmations_by_zone_id = confirmations_by_zone_id or {}
    confluence_by_zone_id = confluence_by_zone_id or {}

    rows: list[dict[str, Any]] = []
    for zone in zones:
        zone_id = _first_non_empty(zone, "zone_id", "id", "fingerprint")
        rows.append(
            {
                "zone": zone,
                "metadata": classify_sr_zone(
                    zone,
                    current_price=current_price,
                    confirmation=confirmations_by_zone_id.get(str(zone_id), {}),
                    feed_state=feed_state,
                    confluence=confluence_by_zone_id.get(str(zone_id), {}),
                    policy=cfg,
                ),
            }
        )

    if cfg["suppress_overlapping_same_side"]:
        suppressed_indexes = _overlap_suppressed_indexes(rows, cfg)
        for idx in suppressed_indexes:
            rows[idx]["metadata"] = classify_sr_zone(
                rows[idx]["zone"],
                current_price=current_price,
                confirmation=confirmations_by_zone_id.get(str(rows[idx]["metadata"].get("zone_id")), {}),
                feed_state=feed_state,
                confluence=confluence_by_zone_id.get(str(rows[idx]["metadata"].get("zone_id")), {}),
                policy=cfg,
                overlap_suppressed=True,
            )

    return [row["metadata"] for row in rows]


def attach_sr_lifecycle_metadata(
    zone: Mapping[str, Any],
    **kwargs: Any,
) -> dict[str, Any]:
    """Return a deep-copied zone row with V0 metadata blocks attached."""

    row = deepcopy(dict(zone))
    row.update(classify_sr_zone(zone, **kwargs))
    return row


def _policy(policy: Mapping[str, Any] | None) -> dict[str, Any]:
    cfg = dict(DEFAULT_POLICY)
    if policy:
        cfg.update(policy)
    return cfg


def _first_non_empty(mapping: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in mapping and mapping[key] not in (None, ""):
            return mapping[key]
    return default


def _zone_bounds(zone: Mapping[str, Any]) -> tuple[float | None, float | None]:
    bounds = zone.get("bounds") if isinstance(zone.get("bounds"), Mapping) else {}
    low = _coerce_float(
        _first_non_empty(zone, "zone_low", "low", default=_first_non_empty(bounds, "zone_low", "low", default=None))
    )
    high = _coerce_float(
        _first_non_empty(zone, "zone_high", "high", default=_first_non_empty(bounds, "zone_high", "high", default=None))
    )
    return low, high


def _normalize_role(value: Any) -> str:
    role = str(value or "unknown").lower()
    if role in {"s", "support", "bid", "demand"}:
        return "support"
    if role in {"r", "resistance", "ask", "supply"}:
        return "resistance"
    if role in {"flip_support", "support_flip"}:
        return "flip_support"
    if role in {"flip_resistance", "resistance_flip"}:
        return "flip_resistance"
    return role


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_unit_score(value: Any) -> float | None:
    score = _coerce_float(value)
    if score is None:
        return None
    if score > 1.0:
        score = score / 100.0
    return max(0.0, min(1.0, score))


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _add_reason(reason_codes: list[str], specific: list[str], reason: str) -> None:
    if reason not in reason_codes:
        reason_codes.append(reason)
    if reason not in specific:
        specific.append(reason)


def _invalidation_buffer(
    zone_low: float | None,
    zone_high: float | None,
    mid: float | None,
    confirmation: Mapping[str, Any],
    cfg: Mapping[str, Any],
) -> float:
    explicit = _coerce_float(_first_non_empty(confirmation, "invalidation_buffer", default=None))
    if explicit is not None:
        return max(0.0, explicit)
    if zone_low is None or zone_high is None:
        return 0.0
    width = max(zone_high - zone_low, 0.0)
    atr = _coerce_float(_first_non_empty(confirmation, "atr", "atr14", default=None))
    atr_component = (atr or 0.0) * float(cfg["atr_buffer_fraction"])
    bps_base = mid if mid is not None and mid > 0 else max(abs(zone_high), abs(zone_low), 1.0)
    bps_component = bps_base * (float(cfg["min_bps_by_symbol"]) / 10_000.0)
    return max(width * float(cfg["width_buffer_fraction"]), atr_component, bps_component)


def _confirmed_break_reason(side: str, confirmation: Mapping[str, Any]) -> str:
    break_quality = str(_first_non_empty(confirmation, "break_quality", default="single_close") or "single_close")
    if side == "support":
        if break_quality == "consolidated_close":
            return "invalidated_consolidated_close_below_support"
        return "invalidated_close_below_support_buffer"
    if break_quality == "consolidated_close":
        return "invalidated_consolidated_close_above_resistance"
    return "invalidated_close_above_resistance_buffer"


def _break_status(cfg: Mapping[str, Any]) -> tuple[LifecycleStatus, bool]:
    if cfg["allow_flip_pending_on_confirmed_break"]:
        return LifecycleStatus.FLIPPED_PENDING, True
    return LifecycleStatus.INVALIDATED, False


def _confluence_state(confluence: Mapping[str, Any], key: str) -> str:
    value = _first_non_empty(confluence, key, default="")
    if isinstance(value, Mapping):
        value = _first_non_empty(value, "status", "state", default="")
    return str(value or "").lower()


def _confidence_score(
    zone: Mapping[str, Any],
    *,
    historical_context_score: float | None,
    retest_count: int,
    reason_codes: Iterable[str],
    degradation_reasons: Iterable[str],
    invalidation_reasons: Iterable[str],
    confluence: Mapping[str, Any],
) -> float:
    if list(invalidation_reasons):
        return 0.0

    selection_score = _normalize_unit_score(_first_non_empty(zone, "selection_score", "score", default=None))
    source_rank = _coerce_int(_first_non_empty(zone, "source_rank", default=None))
    score = 0.50
    score += (historical_context_score if historical_context_score is not None else 0.35) * 0.25
    score += (selection_score if selection_score is not None else 0.50) * 0.15

    if source_rank is not None:
        score += max(0.0, 0.08 - max(source_rank - 1, 0) * 0.025)

    if retest_count == 1:
        score += 0.05

    if _confluence_state(confluence, "fib") == "present":
        score += 0.03
    if _confluence_state(confluence, "dynamic_levels") == "present":
        score += 0.03
    if _confluence_state(confluence, "htf_structure") in {"aligned", "present"}:
        score += 0.04

    penalties = {
        "blocked_insufficient_reaction_history": 0.30,
        "degraded_missing_formation_evidence": 0.18,
        "degraded_weak_historical_context": 0.14,
        "degraded_second_retest": 0.10,
        "degraded_third_retest": 0.20,
        "degraded_excessive_retests": 0.35,
        "degraded_wick_breach_reclaimed": 0.08,
        "degraded_repeated_wick_breaches": 0.16,
        "degraded_severe_fakeout_wick": 0.30,
        "watch_only_failed_breakout_context": 0.20,
        "degraded_feed_quality_elevated": 0.14,
        "degraded_missing_fib_confluence": 0.03,
        "degraded_missing_dynamic_level_confluence": 0.03,
        "degraded_htf_structure_misaligned": 0.10,
        "degraded_overlapping_zone_cluster": 0.08,
    }
    for reason in set(reason_codes) | set(degradation_reasons):
        score -= penalties.get(reason, 0.0)
    return max(0.0, min(1.0, score))


def _status_from_reasons(
    hard_status: LifecycleStatus | None,
    degradation_reasons: Sequence[str],
    reason_codes: Sequence[str],
) -> LifecycleStatus:
    if hard_status is not None:
        return hard_status
    if "watch_only_failed_breakout_context" in reason_codes or "degraded_excessive_retests" in reason_codes:
        return LifecycleStatus.WATCH_ONLY
    if degradation_reasons:
        return LifecycleStatus.DEGRADED
    return LifecycleStatus.ACTIVE


def _tier_for_status_score(status: LifecycleStatus, score: float) -> ConfidenceTier:
    if status in HARD_STATUSES:
        return ConfidenceTier.X
    if status == LifecycleStatus.WATCH_ONLY:
        return ConfidenceTier.D if score < 0.40 else ConfidenceTier.C
    if score >= 0.80:
        return ConfidenceTier.A
    if score >= 0.60:
        return ConfidenceTier.B
    if score >= 0.40:
        return ConfidenceTier.C
    return ConfidenceTier.D


def _eligibility(
    status: LifecycleStatus,
    tier: ConfidenceTier,
    reason_codes: Sequence[str],
) -> DecisionEligibility:
    if status in HARD_STATUSES:
        return DecisionEligibility.REJECT
    if "blocked_insufficient_reaction_history" in reason_codes:
        return DecisionEligibility.WATCH_ONLY
    if status == LifecycleStatus.WATCH_ONLY:
        return DecisionEligibility.WATCH_ONLY
    if tier in {ConfidenceTier.A, ConfidenceTier.B}:
        return DecisionEligibility.CANDIDATE_ELIGIBLE if tier == ConfidenceTier.A else DecisionEligibility.WATCH_ELIGIBLE
    if tier == ConfidenceTier.C:
        return DecisionEligibility.WATCH_ONLY
    return DecisionEligibility.DISPLAY_ONLY


def _visual_metadata(
    *,
    status: LifecycleStatus,
    tier: ConfidenceTier,
    score: float,
    source_rank: int | None,
    overlap_suppressed: bool,
) -> dict[str, Any]:
    color_by_tier = {
        ConfidenceTier.A: "sr_high",
        ConfidenceTier.B: "sr_medium",
        ConfidenceTier.C: "sr_low",
        ConfidenceTier.D: "sr_display",
        ConfidenceTier.X: "sr_invalid",
    }
    show = tier != ConfidenceTier.X and not overlap_suppressed
    priority_base = {ConfidenceTier.A: 400, ConfidenceTier.B: 300, ConfidenceTier.C: 200, ConfidenceTier.D: 100, ConfidenceTier.X: 0}[tier]
    rank_penalty = max((source_rank or 1) - 1, 0) * 10
    overlay_priority = max(0, priority_base + int(score * 100) - rank_penalty)
    return {
        "show_on_overlay": show,
        "overlay_priority": overlay_priority,
        "color_class": color_by_tier[tier],
        "label_density": "minimal" if tier in {ConfidenceTier.A, ConfidenceTier.B} else "detail_on_hover",
    }


def _overlap_suppressed_indexes(rows: Sequence[Mapping[str, Any]], cfg: Mapping[str, Any]) -> set[int]:
    suppressed: set[int] = set()
    threshold = float(cfg["overlap_ratio_for_suppression"])
    for i, left in enumerate(rows):
        if i in suppressed:
            continue
        for j in range(i + 1, len(rows)):
            if j in suppressed:
                continue
            right = rows[j]
            if not _same_visual_bucket(left["zone"], right["zone"]):
                continue
            if _overlap_ratio(left["zone"], right["zone"]) < threshold:
                continue
            left_priority = left["metadata"]["visual"]["overlay_priority"]
            right_priority = right["metadata"]["visual"]["overlay_priority"]
            if right_priority > left_priority:
                suppressed.add(i)
                break
            suppressed.add(j)
    return suppressed


def _same_visual_bucket(a: Mapping[str, Any], b: Mapping[str, Any]) -> bool:
    role_a = _normalize_role(_first_non_empty(a, "role", "current_role", "kind", default="unknown"))
    role_b = _normalize_role(_first_non_empty(b, "role", "current_role", "kind", default="unknown"))
    tf_a = str(_first_non_empty(a, "timeframe", default="")).lower()
    tf_b = str(_first_non_empty(b, "timeframe", default="")).lower()
    side_a = "support" if role_a in SUPPORT_ROLES else "resistance" if role_a in RESISTANCE_ROLES else role_a
    side_b = "support" if role_b in SUPPORT_ROLES else "resistance" if role_b in RESISTANCE_ROLES else role_b
    return side_a == side_b and tf_a == tf_b


def _overlap_ratio(a: Mapping[str, Any], b: Mapping[str, Any]) -> float:
    a_low, a_high = _zone_bounds(a)
    b_low, b_high = _zone_bounds(b)
    if None in {a_low, a_high, b_low, b_high}:
        return 0.0
    assert a_low is not None and a_high is not None and b_low is not None and b_high is not None
    intersection = max(0.0, min(a_high, b_high) - max(a_low, b_low))
    width = max(min(a_high - a_low, b_high - b_low), 1e-9)
    return intersection / width


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
