from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from market_arbiter.arbiter.setup_score import evaluate_sr_zone_eligibility


FOXIAN_RETEST_STRATEGY_SIGNAL_CONTRACT = "foxian_retest_strategy_signal_v0"
FOXIAN_RETEST_BACKTEST_DATASET_CONTRACT = "foxian_retest_backtest_dataset_v0"

_REQUIRED_DATASETS = ("feed_state", "structure_state", "sr_zones", "interaction_lifecycle")
_USABLE_DATASET_STATUSES = {"complete", "ok", "fresh", "replay_only", "partial", "warning", None, ""}
_LONG_TOKENS = {"long", "bull", "bullish", "up", "uptrend", "support", "demand"}
_SHORT_TOKENS = {"short", "bear", "bearish", "down", "downtrend", "resistance", "supply"}
_SUPPORT_ROLES = {"support", "demand", "support_zone", "demand_zone"}
_RESISTANCE_ROLES = {"resistance", "supply", "resistance_zone", "supply_zone"}


@dataclass(frozen=True)
class FoxianRetestStrategyConfig:
    """Deterministic knobs for the first backtestable Foxian retest ruleset.

    The defaults are intentionally boring: they create auditable signal packets
    from Surveyor state, not optimized trading claims.
    """

    min_confluence_score: int = 2
    taker_fee_bps: float = 5.0
    slippage_bps: float = 2.0
    funding_bps_per_8h: float = 0.0
    default_stop_buffer_bps: float = 5.0
    first_target_rr: float = 1.0
    second_target_rr: float = 2.0
    min_risk_pct: float = 1.0
    max_risk_pct: float = 5.0


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _text(value: Any) -> str:
    return str(value or "").strip().lower()


def _status_is_usable(envelope: Mapping[str, Any]) -> bool:
    status = _text(envelope.get("status"))
    return status in _USABLE_DATASET_STATUSES


def _walk_dicts(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for child in value:
            yield from _walk_dicts(child)


def _family(profile: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    return _as_mapping(_as_mapping(profile.get("datasets")).get(name))


def _first_present(mapping: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in mapping and mapping.get(key) not in (None, ""):
            return mapping.get(key)
    return None


def _classify_side_token(value: Any) -> str | None:
    token = _text(value).replace("_", "-")
    parts = {piece for chunk in token.split("/") for piece in chunk.replace("-", " ").split()}
    if token in _LONG_TOKENS or parts & _LONG_TOKENS:
        return "long"
    if token in _SHORT_TOKENS or parts & _SHORT_TOKENS:
        return "short"
    return None


def _extract_structure_side(structure_env: Mapping[str, Any]) -> str | None:
    for item in _walk_dicts(structure_env):
        for key in ("directional_bias", "bias", "trend", "regime", "market_structure", "side", "direction"):
            side = _classify_side_token(item.get(key))
            if side:
                return side
    return None


def _zone_identity(zone: Mapping[str, Any]) -> str | None:
    value = _first_present(zone, ("zone_id", "id", "object_id", "level_id", "surface_id", "name"))
    return str(value) if value not in (None, "") else None


def _zone_role(zone: Mapping[str, Any]) -> str | None:
    value = _first_present(zone, ("current_role", "role", "zone_role", "object_role", "type", "kind"))
    token = _text(value)
    if token in _SUPPORT_ROLES:
        return "support"
    if token in _RESISTANCE_ROLES:
        return "resistance"
    return token or None


def _zone_side(zone: Mapping[str, Any]) -> str | None:
    role = _zone_role(zone)
    if role == "support":
        return "long"
    if role == "resistance":
        return "short"
    return _classify_side_token(_first_present(zone, ("side", "direction", "bias")))


def _iter_zones(sr_env: Mapping[str, Any]) -> list[dict[str, Any]]:
    zones: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in _walk_dicts(sr_env):
        zone_id = _zone_identity(item)
        role = _zone_role(item)
        has_price_shape = any(key in item for key in ("zone_low", "zone_high", "low", "high", "level", "price"))
        if not zone_id and not (role and has_price_shape):
            continue
        normalized = dict(item)
        if zone_id:
            normalized["zone_id"] = zone_id
        if role:
            normalized["current_role"] = role
        identity = zone_id or f"{role}:{normalized.get('zone_low') or normalized.get('low')}:{normalized.get('zone_high') or normalized.get('high')}"
        if identity in seen:
            continue
        seen.add(identity)
        zones.append(normalized)
    return zones


def _event_identity(event: Mapping[str, Any]) -> str | None:
    value = _first_present(event, ("event_id", "interaction_id", "id"))
    return str(value) if value not in (None, "") else None


def _event_type(event: Mapping[str, Any]) -> str:
    return _text(_first_present(event, ("event_type", "event", "type", "state", "kind")))


def _event_is_retest(event: Mapping[str, Any]) -> bool:
    event_type = _event_type(event)
    if "retest" not in event_type:
        return False
    confirmation = _text(_first_present(event, ("confirmation", "confirmed", "status", "result", "outcome")))
    return confirmation not in {"false", "failed", "fail", "rejected", "invalid", "unconfirmed"}


def _iter_retest_events(lifecycle_env: Mapping[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in _walk_dicts(lifecycle_env):
        if not _event_is_retest(item):
            continue
        normalized = dict(item)
        event_id = _event_identity(normalized) or f"event:{len(events)}:{_event_type(normalized)}"
        if event_id in seen:
            continue
        seen.add(event_id)
        normalized["event_id"] = event_id
        events.append(normalized)
    return events


def _event_zone_id(event: Mapping[str, Any]) -> str | None:
    value = _first_present(event, ("zone_id", "object_id", "level_id", "surface_id"))
    if value not in (None, ""):
        return str(value)
    details = _as_mapping(event.get("details"))
    value = _first_present(details, ("zone_id", "object_id", "level_id", "surface_id"))
    return str(value) if value not in (None, "") else None


def _event_side(event: Mapping[str, Any]) -> str | None:
    for source in (event, _as_mapping(event.get("details"))):
        side = _classify_side_token(_first_present(source, ("side", "direction", "current_role", "zone_role", "breakout_direction")))
        if side:
            return side
    return None


def _match_zone(event: Mapping[str, Any], zones: Sequence[Mapping[str, Any]], fallback_side: str | None) -> Mapping[str, Any] | None:
    event_zone_id = _event_zone_id(event)
    if event_zone_id:
        for zone in zones:
            if _zone_identity(zone) == event_zone_id:
                return zone
    side = _event_side(event) or fallback_side
    if side:
        for zone in zones:
            if _zone_side(zone) == side:
                return zone
    return zones[0] if zones else None


def _numeric(mapping: Mapping[str, Any], keys: Sequence[str]) -> float | None:
    value = _first_present(mapping, keys)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _score_confluence(
    *,
    structure_side: str | None,
    signal_side: str | None,
    zone: Mapping[str, Any] | None,
    fib_env: Mapping[str, Any],
    dynamic_env: Mapping[str, Any],
    event: Mapping[str, Any],
) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    if structure_side and signal_side and structure_side == signal_side:
        score += 1
        reasons.append("structure_bias_aligned")
    elif structure_side and signal_side and structure_side != signal_side:
        reasons.append("structure_bias_opposes_signal")

    if zone is not None:
        score += 1
        reasons.append("qualified_sr_zone_present")
        sr_gate = evaluate_sr_zone_eligibility(zone)
        if sr_gate.get("confidence_tier") in {"A", "B"}:
            score += 1
            reasons.append("sr_lifecycle_confidence_tier_high")

    for item in _walk_dicts(fib_env):
        state = _text(_first_present(item, ("fib_state", "state", "status")))
        relation = _text(_first_present(item, ("relation", "zone_relation", "value_zone", "side")))
        if state == "active" or relation in {"discount", "premium", "value", "overlap"}:
            score += 1
            reasons.append("fib_context_available")
            break

    zone_id = _zone_identity(zone) if zone else None
    for item in _walk_dicts(dynamic_env):
        relation = _text(_first_present(item, ("zone_relation", "relation", "overlap")))
        object_id = str(_first_present(item, ("zone_id", "object_id", "level_id")) or "")
        if relation in {"overlap", "inside", "near", "touching"} or (zone_id and object_id == zone_id):
            score += 1
            reasons.append("dynamic_level_confluence")
            break

    if _text(_first_present(event, ("confirmation", "confirmed", "status", "result"))) in {"confirmed", "success", "accepted", "true"}:
        score += 1
        reasons.append("retest_confirmed")

    retest_index = _numeric(event, ("retest_index", "retest_number", "touch_number", "test_number"))
    if retest_index == 1:
        score += 1
        reasons.append("first_retest_bonus")

    return score, reasons


def _risk_model(config: FoxianRetestStrategyConfig, confluence_score: int, reasons: Sequence[str]) -> dict[str, Any]:
    """Map primary+secondary confluence into Redact's 1-5% research risk scale."""

    primary_score = int("qualified_sr_zone_present" in reasons) + int("retest_confirmed" in reasons)
    secondary_score = max(0, confluence_score - primary_score)
    risk_pct = min(config.max_risk_pct, max(config.min_risk_pct, config.min_risk_pct + secondary_score))
    if risk_pct >= 5.0:
        tier = "A+"
    elif risk_pct >= 4.0:
        tier = "A"
    elif risk_pct >= 3.0:
        tier = "B"
    elif risk_pct >= 2.0:
        tier = "C"
    else:
        tier = "base"
    return {
        "model": "confluence_scaled_fixed_fractional_v0",
        "risk_pct": risk_pct,
        "min_risk_pct": config.min_risk_pct,
        "max_risk_pct": config.max_risk_pct,
        "primary_score": primary_score,
        "secondary_score": secondary_score,
        "tier": tier,
    }


def _trade_template(
    *,
    config: FoxianRetestStrategyConfig,
    profile: Mapping[str, Any],
    event: Mapping[str, Any],
    zone: Mapping[str, Any] | None,
    side: str | None,
    confluence_score: int = 0,
    reason_codes: Sequence[str] = (),
) -> dict[str, Any]:
    zone_map = zone or {}
    if side == "long":
        invalidation_level = _numeric(zone_map, ("zone_low", "low", "bottom", "price_low"))
        stop_policy = "below_zone_low_plus_buffer"
    elif side == "short":
        invalidation_level = _numeric(zone_map, ("zone_high", "high", "top", "price_high"))
        stop_policy = "above_zone_high_plus_buffer"
    else:
        invalidation_level = None
        stop_policy = "unresolved_until_signal_side_known"

    details = _as_mapping(event.get("details"))
    sr_gate = evaluate_sr_zone_eligibility(zone_map)
    return {
        "symbol": _as_mapping(profile.get("meta")).get("symbol"),
        "side": side,
        "entry_policy": "next_bar_open_after_retest_confirmation",
        "entry_event_id": event.get("event_id"),
        "entry_ts": _first_present(event, ("event_ts", "timestamp", "ts")) or _first_present(details, ("event_ts", "timestamp", "ts")),
        "entry_price_hint": _first_present(event, ("entry_price", "price", "close")) or _first_present(details, ("entry_price", "price", "close")),
        "zone_id": _zone_identity(zone_map),
        "stop_policy": stop_policy,
        "invalidation_level_hint": invalidation_level,
        "stop_buffer_bps": config.default_stop_buffer_bps,
        "target_policy": "first_opposing_qualified_zone_else_fixed_rr_ladder",
        "target_rr": [config.first_target_rr, config.second_target_rr],
        "cost_model": {
            "taker_fee_bps": config.taker_fee_bps,
            "slippage_bps": config.slippage_bps,
            "funding_bps_per_8h": config.funding_bps_per_8h,
        },
        "confluence_model": {
            "score": confluence_score,
            "reason_codes": list(reason_codes),
        },
        "sr_lifecycle_gate": sr_gate,
        "risk_model": _risk_model(config, confluence_score, reason_codes),
    }


def evaluate_foxian_retest_strategy(
    profile: Mapping[str, Any],
    *,
    config: FoxianRetestStrategyConfig | None = None,
) -> dict[str, Any]:
    """Turn one point-in-time Surveyor `backtest_core` profile into a signal packet.

    This is the bridge from descriptive Surveyor state to backtestable Arbiter
    events. It does not simulate PnL; it emits deterministic candidate packets
    that a replay runner can later execute against OHLCV with fees/slippage.
    """

    cfg = config or FoxianRetestStrategyConfig()
    meta = dict(_as_mapping(profile.get("meta")))
    missing = [name for name in _REQUIRED_DATASETS if name not in _as_mapping(profile.get("datasets"))]
    reason_codes: list[str] = []
    if missing:
        reason_codes.append("missing_required_surveyor_families")
        return {
            "contract": FOXIAN_RETEST_STRATEGY_SIGNAL_CONTRACT,
            "ruleset_id": "foxian_retest_flip_confluence_v0",
            "verdict": "reject",
            "symbol": meta.get("symbol"),
            "as_of_ts": meta.get("as_of_ts"),
            "reason_codes": reason_codes,
            "missing_families": missing,
            "event_study_row": None,
            "trade_candidate": None,
        }

    feed_env = _family(profile, "feed_state")
    structure_env = _family(profile, "structure_state")
    sr_env = _family(profile, "sr_zones")
    lifecycle_env = _family(profile, "interaction_lifecycle")
    fib_env = _family(profile, "fib_context")
    dynamic_env = _family(profile, "dynamic_levels")

    unusable = [name for name, env in (("feed_state", feed_env), ("structure_state", structure_env), ("sr_zones", sr_env), ("interaction_lifecycle", lifecycle_env)) if not _status_is_usable(env)]
    if unusable:
        reason_codes.append("unusable_surveyor_family_status")
        return {
            "contract": FOXIAN_RETEST_STRATEGY_SIGNAL_CONTRACT,
            "ruleset_id": "foxian_retest_flip_confluence_v0",
            "verdict": "reject",
            "symbol": meta.get("symbol"),
            "as_of_ts": meta.get("as_of_ts"),
            "reason_codes": reason_codes,
            "unusable_families": unusable,
            "event_study_row": None,
            "trade_candidate": None,
        }

    zones = _iter_zones(sr_env)
    retest_events = _iter_retest_events(lifecycle_env)
    structure_side = _extract_structure_side(structure_env)

    if not zones:
        reason_codes.append("no_qualified_sr_zone")
    if not retest_events:
        reason_codes.append("no_retest_event")

    if not zones or not retest_events:
        return {
            "contract": FOXIAN_RETEST_STRATEGY_SIGNAL_CONTRACT,
            "ruleset_id": "foxian_retest_flip_confluence_v0",
            "verdict": "watch" if not retest_events else "reject",
            "symbol": meta.get("symbol"),
            "as_of_ts": meta.get("as_of_ts"),
            "reason_codes": reason_codes,
            "event_study_row": None,
            "trade_candidate": None,
        }

    event = retest_events[0]
    zone = _match_zone(event, zones, structure_side)
    sr_gate = evaluate_sr_zone_eligibility(zone or {})
    reason_codes.extend([reason for reason in sr_gate.get("reason_codes", []) if reason not in reason_codes])
    signal_side = _event_side(event) or _zone_side(zone or {}) or structure_side
    confluence_score, confluence_reasons = _score_confluence(
        structure_side=structure_side,
        signal_side=signal_side,
        zone=zone,
        fib_env=fib_env,
        dynamic_env=dynamic_env,
        event=event,
    )
    reason_codes.extend(confluence_reasons)
    if confluence_score < cfg.min_confluence_score:
        reason_codes.append("confluence_score_below_minimum")

    raw_verdict = "candidate" if confluence_score >= cfg.min_confluence_score and signal_side in {"long", "short"} else "watch"
    if sr_gate.get("verdict") == "reject":
        verdict = "reject"
    elif raw_verdict == "candidate" and sr_gate.get("candidate_allowed"):
        verdict = "candidate"
    else:
        verdict = "watch"
    event_study_row = {
        "ruleset_id": "foxian_retest_flip_confluence_v0",
        "symbol": meta.get("symbol"),
        "as_of_ts": meta.get("as_of_ts"),
        "source_bundle_id": meta.get("source_bundle_id"),
        "event_id": event.get("event_id"),
        "event_type": _event_type(event),
        "event_ts": _first_present(event, ("event_ts", "timestamp", "ts")),
        "zone_id": _zone_identity(zone or {}),
        "zone_role": _zone_role(zone or {}),
        "side": signal_side,
        "structure_side": structure_side,
        "confluence_score": confluence_score,
        "sr_lifecycle_status": sr_gate.get("lifecycle_status"),
        "sr_confidence_tier": sr_gate.get("confidence_tier"),
        "sr_decision_eligibility": sr_gate.get("decision_eligibility"),
        "sr_candidate_allowed": sr_gate.get("candidate_allowed"),
        "sr_watch_allowed": sr_gate.get("watch_allowed"),
        "sr_display_allowed": sr_gate.get("display_allowed"),
        "verdict": verdict,
        "reason_codes": list(reason_codes),
    }

    return {
        "contract": FOXIAN_RETEST_STRATEGY_SIGNAL_CONTRACT,
        "ruleset_id": "foxian_retest_flip_confluence_v0",
        "verdict": verdict,
        "symbol": meta.get("symbol"),
        "as_of_ts": meta.get("as_of_ts"),
        "reason_codes": list(reason_codes),
        "event_study_row": event_study_row,
        "trade_candidate": _trade_template(
            config=cfg,
            profile=profile,
            event=event,
            zone=zone,
            side=signal_side,
            confluence_score=confluence_score,
            reason_codes=reason_codes,
        ) if verdict == "candidate" else None,
    }


def build_foxian_retest_backtest_dataset(
    profiles: Iterable[Mapping[str, Any]],
    *,
    config: FoxianRetestStrategyConfig | None = None,
) -> dict[str, Any]:
    """Evaluate a sequence of point-in-time profiles into replay-ready rows."""

    evaluations = [evaluate_foxian_retest_strategy(profile, config=config) for profile in profiles]
    return {
        "contract": FOXIAN_RETEST_BACKTEST_DATASET_CONTRACT,
        "ruleset_id": "foxian_retest_flip_confluence_v0",
        "evaluations": evaluations,
        "event_study_rows": [row for evaluation in evaluations if (row := evaluation.get("event_study_row"))],
        "trade_candidates": [candidate for evaluation in evaluations if (candidate := evaluation.get("trade_candidate"))],
    }
