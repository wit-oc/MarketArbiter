from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Sequence
from uuid import uuid4

from market_arbiter.surveyor.phase1_contract import (
    PHASE1_STRUCTURE_CONTRACT,
    PHASE1_STRUCTURE_PROFILE_CANONICAL,
    normalize_phase1_structure_profile,
    phase1_structure_contract_config,
    run_phase1_structure_contract_from_candles,
)

from .dynamic_levels import DynamicLevelPacket, dynamic_level_packet_to_dict
from .fib_context import FibContextState, FibDisarmReason, FibTimeframeState
from .sr_lifecycle import classify_sr_zones

SURVEYOR_PACKET_CONTRACT = "surveyor_packet_contract_v1"
SURVEYOR_DATA_FEED_CONTRACT = "surveyor_data_feed_contract_v1"
SURVEYOR_STRUCTURE_SERVICE_CONTRACT = "surveyor_structure_service_contract_v1"
SURVEYOR_LIFECYCLE_CONTRACT = "surveyor_interaction_lifecycle_contract_v1"
SURVEYOR_SR_CONTRACT_FALLBACK = "authoritative_levels_view_v1"
SURVEYOR_FIB_CONTRACT_FALLBACK = "fib_context_state_v1"

REQUIRED_SURVEYOR_TIMEFRAMES = ("1W", "1D", "4H", "5m")
_TIMEFRAME_SECONDS = {
    "5m": 5 * 60,
    "4H": 4 * 60 * 60,
    "1D": 24 * 60 * 60,
    "1W": 7 * 24 * 60 * 60,
}


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_timeframe(timeframe: str | None) -> str:
    raw = str(timeframe or "").strip()
    upper = raw.upper()
    if upper in {"1W", "1D", "4H"}:
        return upper
    if raw.lower() in {"5m", "5min", "5minute"}:
        return "5m"
    return raw or "unknown"


def _coerce_epoch_seconds(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        if token.isdigit() or (token.startswith("-") and token[1:].isdigit()):
            return int(token)
        try:
            return int(float(token))
        except ValueError:
            try:
                parsed = datetime.fromisoformat(token.replace("Z", "+00:00"))
            except ValueError:
                return None
            return int(parsed.timestamp())
    return None


def _epoch_to_iso(value: Any) -> str | None:
    epoch = _coerce_epoch_seconds(value)
    if epoch is None:
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _extract_candle_timestamp(candle: Mapping[str, Any] | None) -> int | None:
    if not isinstance(candle, Mapping):
        return None
    for key in ("timestamp", "ts", "open_time", "close_time", "datetime"):
        epoch = _coerce_epoch_seconds(candle.get(key))
        if epoch is not None:
            return epoch
    return None


def _to_plain_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {str(k): _to_plain(v) for k, v in value.items()}
    if is_dataclass(value):
        return {str(k): _to_plain(v) for k, v in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(k): _to_plain(v) for k, v in value.items()}
    raise TypeError(f"expected mapping-like value, got {type(value).__name__}")


def _to_plain(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {str(k): _to_plain(v) for k, v in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(k): _to_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_plain(item) for item in value]
    return value


def _structure_event_id(*, timeframe: str, contract: str, event_name: str, index: Any, anchor_index: Any, price: Any) -> str:
    return f"structure:{timeframe}:{contract}:{event_name}:{index}:{anchor_index}:{price}"


def _structure_swing_id(*, timeframe: str, contract: str, kind: str, index: Any, price: Any) -> str:
    return f"swing:{timeframe}:{contract}:{kind}:{index}:{price}"


def _derive_level_interactions(dynamic_levels: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    payload = _to_plain_dict(dynamic_levels)
    levels = payload.get("levels") if isinstance(payload.get("levels"), list) else []
    out: list[dict[str, Any]] = []
    for level in levels:
        if not isinstance(level, Mapping):
            continue
        if not bool(level.get("available")):
            continue
        timeframe = _normalize_timeframe(level.get("timeframe"))
        level_name = str(level.get("level_name") or "unknown")
        level_id = f"dynamic:{timeframe}:{level_name}"
        event_ts = _epoch_to_iso(level.get("timeframe_bar_ts") or payload.get("feed_bar_ts") or payload.get("as_of_ts"))
        out.append(
            {
                "interaction_id": f"interaction:{level_id}:{event_ts or 'na'}",
                "module": "dynamic_levels",
                "timeframe": timeframe,
                "object_type": "level",
                "object_id": level_id,
                "event_type": "level_state_observed",
                "event_ts": event_ts,
                "source_event_id": payload.get("source_event_id"),
                "source_swing_id": payload.get("source_swing_id"),
                "source_contract_version": payload.get("source_contract_version"),
                "details": {
                    "level_value": level.get("level_value"),
                    "price_side": level.get("price_side"),
                    "distance_abs": level.get("distance_abs"),
                    "distance_pct": level.get("distance_pct"),
                    "zone_relation": level.get("zone_relation"),
                },
            }
        )
    return out


def build_market_data_timeframe_entry(
    *,
    symbol: str,
    timeframe: str,
    candles: Sequence[Mapping[str, Any]] | None = None,
    feed_provider: str,
    dataset_mode: str,
    dataset_id: str,
    latest_open_time: Any = None,
    latest_close_time: Any = None,
    latest_ingested_at: Any = None,
    freshness_state: str | None = None,
    freshness_reason: str | None = None,
    history_start_time: Any = None,
    history_end_time: Any = None,
) -> dict[str, Any]:
    tf = _normalize_timeframe(timeframe)
    candle_rows = list(candles or [])
    bar_count = len(candle_rows)
    first_ts = _extract_candle_timestamp(candle_rows[0]) if candle_rows else None
    last_ts = _extract_candle_timestamp(candle_rows[-1]) if candle_rows else None
    duration = _TIMEFRAME_SECONDS.get(tf)

    derived_open = _coerce_epoch_seconds(latest_open_time)
    if derived_open is None:
        derived_open = last_ts
    derived_close = _coerce_epoch_seconds(latest_close_time)
    if derived_close is None and derived_open is not None and duration is not None:
        derived_close = derived_open + duration
    derived_history_start = _coerce_epoch_seconds(history_start_time)
    if derived_history_start is None:
        derived_history_start = first_ts
    derived_history_end = _coerce_epoch_seconds(history_end_time)
    if derived_history_end is None:
        derived_history_end = derived_close or last_ts

    resolved_freshness = freshness_state or ("partial" if bar_count == 0 else ("replay_only" if dataset_mode == "certified_replay" else "fresh"))
    resolved_reason = freshness_reason or (
        "missing_timeframe_input"
        if bar_count == 0
        else ("certified_replay_dataset" if dataset_mode == "certified_replay" else "operational_dataset")
    )

    return {
        "feed_provider": feed_provider,
        "symbol": symbol,
        "timeframe": tf,
        "dataset_mode": dataset_mode,
        "dataset_id": dataset_id,
        "latest_open_time": _epoch_to_iso(derived_open),
        "latest_close_time": _epoch_to_iso(derived_close),
        "latest_ingested_at": _epoch_to_iso(latest_ingested_at) or _now_utc_iso(),
        "freshness_state": resolved_freshness,
        "freshness_reason": resolved_reason,
        "bar_count_available": bar_count,
        "history_start_time": _epoch_to_iso(derived_history_start),
        "history_end_time": _epoch_to_iso(derived_history_end),
    }


def build_market_data_section(
    *,
    symbol: str,
    provider: str,
    dataset_mode: str,
    dataset_id: str,
    timeframe_inputs: Mapping[str, Mapping[str, Any]] | None,
    packet_as_of_ts: Any = None,
) -> dict[str, Any]:
    normalized_inputs = { _normalize_timeframe(tf): dict(payload) for tf, payload in (timeframe_inputs or {}).items() }
    timeframes: dict[str, Any] = {}
    for tf in REQUIRED_SURVEYOR_TIMEFRAMES:
        payload = normalized_inputs.get(tf, {})
        timeframes[tf] = build_market_data_timeframe_entry(
            symbol=symbol,
            timeframe=tf,
            candles=payload.get("candles"),
            feed_provider=str(payload.get("feed_provider") or provider),
            dataset_mode=str(payload.get("dataset_mode") or dataset_mode),
            dataset_id=str(payload.get("dataset_id") or dataset_id),
            latest_open_time=payload.get("latest_open_time"),
            latest_close_time=payload.get("latest_close_time"),
            latest_ingested_at=payload.get("latest_ingested_at"),
            freshness_state=payload.get("freshness_state"),
            freshness_reason=payload.get("freshness_reason"),
            history_start_time=payload.get("history_start_time"),
            history_end_time=payload.get("history_end_time"),
        )

    latest_close_times = [
        row.get("latest_close_time")
        for row in timeframes.values()
        if isinstance(row, Mapping) and row.get("latest_close_time")
    ]
    resolved_as_of_ts = _epoch_to_iso(packet_as_of_ts)
    if resolved_as_of_ts is None:
        resolved_as_of_ts = max(latest_close_times) if latest_close_times else _now_utc_iso()

    return {
        "contract": SURVEYOR_DATA_FEED_CONTRACT,
        "provider": provider,
        "symbol": symbol,
        "packet_as_of_ts": resolved_as_of_ts,
        "dataset_mode": dataset_mode,
        "dataset_id": dataset_id,
        "timeframes": timeframes,
    }


def build_structure_timeframe_surface(
    *,
    timeframe: str,
    candles: Sequence[Mapping[str, Any]] | None,
    feed_provider: str | None = None,
    dataset_mode: str | None = None,
    dataset_id: str | None = None,
    profile: str | None = None,
) -> dict[str, Any]:
    tf = _normalize_timeframe(timeframe)
    candle_rows = list(candles or [])
    normalized_profile = normalize_phase1_structure_profile(profile)
    cfg = phase1_structure_contract_config(candle_count=len(candle_rows), profile=normalized_profile)
    contract_version = str(cfg["contract"])

    if not candle_rows:
        return {
            "timeframe": tf,
            "contract_version": contract_version,
            "contract_profile": normalized_profile,
            "regime_direction": None,
            "regime_confidence": None,
            "regime_reason": None,
            "transition_reason": None,
            "protected_high": None,
            "protected_high_idx": None,
            "protected_low": None,
            "protected_low_idx": None,
            "validated_high": None,
            "validated_high_idx": None,
            "validated_low": None,
            "validated_low_idx": None,
            "active_choch_level": None,
            "active_choch_index": None,
            "latest_bar_index": None,
            "latest_bar_close": None,
            "events": [],
            "swings": [],
            "source_event_id": None,
            "source_swing_id": None,
            "source_contract_version": contract_version,
            "source_contract_profile": normalized_profile,
            "source_bar_index": None,
            "source_bar_ts": None,
            "feed_provider": feed_provider,
            "dataset_mode": dataset_mode,
            "dataset_id": dataset_id,
            "status": "missing_candles",
        }

    bars, events, swings = run_phase1_structure_contract_from_candles(candle_rows, profile=normalized_profile)
    latest_bar = bars[-1] if bars else {}

    normalized_events: list[dict[str, Any]] = []
    for idx, event in enumerate(events):
        bar_index = int(event.get("index") or 0)
        anchor_index = event.get("anchor_index")
        price = event.get("price")
        event_ts = _epoch_to_iso(_extract_candle_timestamp(candle_rows[bar_index]) if 0 <= bar_index < len(candle_rows) else None)
        event_id = _structure_event_id(
            timeframe=tf,
            contract=contract_version,
            event_name=str(event.get("event") or "unknown"),
            index=bar_index,
            anchor_index=anchor_index,
            price=price,
        )
        normalized_events.append(
            {
                "event_id": event_id,
                "index": bar_index,
                "event": event.get("event"),
                "price": price,
                "regime_direction": event.get("regime_direction"),
                "regime_confidence": event.get("regime_confidence"),
                "transition_reason": event.get("transition_reason"),
                "anchor_index": anchor_index,
                "timeframe": tf,
                "event_ts": event_ts,
                "source_contract_version": contract_version,
                "sequence": idx,
            }
        )

    normalized_swings: list[dict[str, Any]] = []
    for idx, swing in enumerate(swings):
        kind_raw = str(swing.get("kind") or "")
        kind = kind_raw.replace("swing_", "") if kind_raw.startswith("swing_") else kind_raw
        swing_index = int(swing.get("index") or 0)
        swing_price = swing.get("price")
        swing_id = _structure_swing_id(
            timeframe=tf,
            contract=contract_version,
            kind=kind or "unknown",
            index=swing_index,
            price=swing_price,
        )
        normalized_swings.append(
            {
                "swing_id": swing_id,
                "kind": kind,
                "index": swing_index,
                "price": swing_price,
                "timeframe": tf,
                "swing_ts": _epoch_to_iso(
                    _extract_candle_timestamp(candle_rows[swing_index]) if 0 <= swing_index < len(candle_rows) else None
                ),
                "source_contract_version": contract_version,
                "sequence": idx,
            }
        )

    source_bar_index = latest_bar.get("index")
    source_bar_ts = None
    if isinstance(source_bar_index, int) and 0 <= source_bar_index < len(candle_rows):
        source_bar_ts = _epoch_to_iso(_extract_candle_timestamp(candle_rows[source_bar_index]))

    return {
        "timeframe": tf,
        "contract_version": contract_version,
        "contract_profile": normalized_profile,
        "regime_direction": latest_bar.get("regime_direction"),
        "regime_confidence": latest_bar.get("regime_confidence"),
        "regime_reason": latest_bar.get("regime_reason"),
        "transition_reason": latest_bar.get("transition_reason"),
        "protected_high": latest_bar.get("protected_high"),
        "protected_high_idx": latest_bar.get("protected_high_idx"),
        "protected_low": latest_bar.get("protected_low"),
        "protected_low_idx": latest_bar.get("protected_low_idx"),
        "validated_high": latest_bar.get("validated_high"),
        "validated_high_idx": latest_bar.get("validated_high_idx"),
        "validated_low": latest_bar.get("validated_low"),
        "validated_low_idx": latest_bar.get("validated_low_idx"),
        "active_choch_level": latest_bar.get("active_choch_level"),
        "active_choch_index": latest_bar.get("active_choch_index"),
        "latest_bar_index": latest_bar.get("index"),
        "latest_bar_close": latest_bar.get("close"),
        "events": normalized_events,
        "swings": normalized_swings,
        "source_event_id": normalized_events[-1]["event_id"] if normalized_events else None,
        "source_swing_id": normalized_swings[-1]["swing_id"] if normalized_swings else None,
        "source_contract_version": contract_version,
        "source_contract_profile": normalized_profile,
        "source_bar_index": source_bar_index,
        "source_bar_ts": source_bar_ts,
        "feed_provider": feed_provider,
        "dataset_mode": dataset_mode,
        "dataset_id": dataset_id,
        "status": "ok",
        "contract_config": cfg,
    }


def build_structure_section(
    *,
    candles_by_tf: Mapping[str, Sequence[Mapping[str, Any]]] | None,
    market_data_section: Mapping[str, Any],
    profile: str | None = PHASE1_STRUCTURE_PROFILE_CANONICAL,
) -> dict[str, Any]:
    market_timeframes = market_data_section.get("timeframes") if isinstance(market_data_section, Mapping) else {}
    normalized_candles = { _normalize_timeframe(tf): list(rows) for tf, rows in (candles_by_tf or {}).items() }

    surfaces: dict[str, Any] = {}
    for tf in REQUIRED_SURVEYOR_TIMEFRAMES:
        feed_meta = market_timeframes.get(tf, {}) if isinstance(market_timeframes, Mapping) else {}
        surfaces[tf] = build_structure_timeframe_surface(
            timeframe=tf,
            candles=normalized_candles.get(tf),
            feed_provider=feed_meta.get("feed_provider"),
            dataset_mode=feed_meta.get("dataset_mode"),
            dataset_id=feed_meta.get("dataset_id"),
            profile=profile,
        )

    return {
        "contract": SURVEYOR_STRUCTURE_SERVICE_CONTRACT,
        "timeframes": surfaces,
    }


def build_sr_section(
    *,
    authoritative_view: Mapping[str, Any] | None,
    ladders: Mapping[str, Any] | None = None,
    lifecycle: Mapping[str, Any] | None = None,
    source_contract_version: str | None = None,
) -> dict[str, Any]:
    view = _to_plain_dict(authoritative_view)
    timeframe_views = view.get("timeframes") if isinstance(view.get("timeframes"), Mapping) else {}

    selected_surfaces: dict[str, Any] = {}
    levels_by_timeframe: dict[str, list[dict[str, Any]]] = {}
    selector_surfaces: dict[str, Any] = {}
    for tf, payload in timeframe_views.items():
        normalized_tf = _normalize_timeframe(tf)
        groups = payload.get("groups") if isinstance(payload.get("groups"), Mapping) else {}
        levels: list[dict[str, Any]] = []
        normalized_groups: dict[str, list[dict[str, Any]]] = {}
        for group_name, rows in groups.items():
            row_list = [_to_plain(row) for row in (rows if isinstance(rows, list) else [])]
            for row in row_list:
                row.setdefault("timeframe", normalized_tf)
            normalized_groups[str(group_name)] = row_list
            levels.extend(row_list)
        if levels:
            for row, metadata in zip(levels, classify_sr_zones(levels)):
                row.update(metadata)
        selected_surfaces[normalized_tf] = {
            "contract": payload.get("contract") or view.get("contract") or SURVEYOR_SR_CONTRACT_FALLBACK,
            "tf": normalized_tf,
            "selector_surface": payload.get("selector_surface"),
            "group_perspective": payload.get("group_perspective"),
            "entry": payload.get("entry"),
            "groups": normalized_groups,
        }
        levels_by_timeframe[normalized_tf] = levels
        selector_surfaces[normalized_tf] = payload.get("selector_surface")

    return {
        "selected_surfaces": selected_surfaces,
        "levels_by_timeframe": levels_by_timeframe,
        "ladders": _to_plain(ladders or {}),
        "lifecycle": _to_plain(lifecycle or {}),
        "source_selector_surface": selector_surfaces,
        "source_contract_version": source_contract_version or view.get("contract") or SURVEYOR_SR_CONTRACT_FALLBACK,
    }


def build_fib_section(
    *,
    fib_context: FibContextState | Mapping[str, Any],
    timeframe_states: Sequence[FibTimeframeState | Mapping[str, Any]],
    tap_history: Mapping[str, Any] | None = None,
    anchor_provenance: Mapping[str, Any] | None = None,
    source_event_id: str | None = None,
    source_swing_id: str | None = None,
    source_contract_version: str | None = None,
) -> dict[str, Any]:
    context_payload = _to_plain_dict(fib_context)
    tap_map = { _normalize_timeframe(tf): _to_plain(payload) for tf, payload in (tap_history or {}).items() }
    anchor_map = { _normalize_timeframe(tf): _to_plain(payload) for tf, payload in (anchor_provenance or {}).items() }

    contexts_by_timeframe: dict[str, Any] = {}
    active_contexts: list[dict[str, Any]] = []
    preferred_event_id = source_event_id
    preferred_swing_id = source_swing_id

    for state in timeframe_states:
        payload = _to_plain_dict(state)
        tf_value = payload.get("timeframe")
        if isinstance(tf_value, Mapping):
            tf_value = tf_value.get("value")
        tf = _normalize_timeframe(str(tf_value or payload.get("timeframe") or "unknown"))
        fib_context_id = f"fib:{context_payload.get('as_of_ts') or 'na'}:{tf}"
        state_name = str(payload.get("fib_state") or "")
        structure_superseded = str(payload.get("disarm_reason") or "") == FibDisarmReason.NEW_STRUCTURE_EVENT.value
        row = {
            "fib_context_id": fib_context_id,
            "timeframe": tf,
            "anchor_a": {
                "id": payload.get("anchor_start_id"),
                "price": payload.get("anchor_start_price"),
            },
            "anchor_b": {
                "id": payload.get("anchor_end_id"),
                "price": payload.get("anchor_end_price"),
            },
            "direction": payload.get("bias_side"),
            "active": state_name == "ACTIVE",
            "state": state_name,
            "levels": {
                "0.618": payload.get("level_0_618"),
                "0.705": payload.get("level_0_705"),
                "0.786": payload.get("level_0_786"),
                "0.886": payload.get("level_0_886"),
                "band_low": payload.get("band_low"),
                "band_high": payload.get("band_high"),
            },
            "tap_state": tap_map.get(tf, {}),
            "structure_superseded": structure_superseded,
            "band_interaction": payload.get("band_interaction"),
            "sub_zone": payload.get("sub_zone"),
            "disarm_reason": payload.get("disarm_reason"),
            "tf_score_contribution": payload.get("tf_score_contribution"),
        }
        contexts_by_timeframe[tf] = row
        if row["active"]:
            active_contexts.append(row)
        provenance = anchor_map.get(tf, {})
        if preferred_event_id is None:
            preferred_event_id = provenance.get("source_event_id")
        if preferred_swing_id is None:
            preferred_swing_id = provenance.get("source_swing_id")

    return {
        "contexts_by_timeframe": contexts_by_timeframe,
        "active_contexts": active_contexts,
        "anchor_provenance": anchor_map,
        "tap_history": tap_map,
        "source_event_id": preferred_event_id,
        "source_swing_id": preferred_swing_id,
        "source_contract_version": source_contract_version or SURVEYOR_FIB_CONTRACT_FALLBACK,
        "summary": {
            "as_of_index": context_payload.get("as_of_index"),
            "as_of_ts": context_payload.get("as_of_ts"),
            "fib_quality_score": context_payload.get("fib_quality_score"),
            "active_timeframes": context_payload.get("active_timeframes"),
            "overlap_cluster": context_payload.get("overlap_cluster"),
            "has_1d_4h_overlap": context_payload.get("has_1d_4h_overlap"),
            "has_1w_bonus_overlap": context_payload.get("has_1w_bonus_overlap"),
            "overall_state": context_payload.get("overall_state"),
            "overall_reason": context_payload.get("overall_reason"),
        },
    }


def build_dynamic_levels_section(dynamic_levels: DynamicLevelPacket | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(dynamic_levels, DynamicLevelPacket):
        return dynamic_level_packet_to_dict(dynamic_levels)
    return _to_plain_dict(dynamic_levels)


def build_interaction_lifecycle_section(
    *,
    structure_section: Mapping[str, Any],
    dynamic_levels: Mapping[str, Any] | None = None,
    zone_interactions: Sequence[Mapping[str, Any]] | None = None,
    level_interactions: Sequence[Mapping[str, Any]] | None = None,
    retests: Sequence[Mapping[str, Any]] | None = None,
    breaches: Sequence[Mapping[str, Any]] | None = None,
    state_changes: Sequence[Mapping[str, Any]] | None = None,
    as_of_ts: Any = None,
) -> dict[str, Any]:
    structure_payload = _to_plain_dict(structure_section)
    derived_state_changes: list[dict[str, Any]] = []
    timestamps: dict[str, Any] = {"packet_as_of_ts": _epoch_to_iso(as_of_ts)}

    timeframe_map = structure_payload.get("timeframes") if isinstance(structure_payload.get("timeframes"), Mapping) else {}
    for tf, surface in timeframe_map.items():
        surface_dict = _to_plain_dict(surface)
        timestamps[str(tf)] = surface_dict.get("source_bar_ts")
        for event in surface_dict.get("events", []):
            if not isinstance(event, Mapping):
                continue
            derived_state_changes.append(
                {
                    "interaction_id": event.get("event_id"),
                    "module": "structure",
                    "timeframe": _normalize_timeframe(tf),
                    "object_type": "structure_event",
                    "object_id": event.get("event_id"),
                    "event_type": event.get("event"),
                    "event_ts": event.get("event_ts"),
                    "source_event_id": event.get("event_id"),
                    "source_swing_id": surface_dict.get("source_swing_id"),
                    "source_contract_version": surface_dict.get("source_contract_version"),
                    "details": {
                        "price": event.get("price"),
                        "anchor_index": event.get("anchor_index"),
                        "transition_reason": event.get("transition_reason"),
                    },
                }
            )

    dynamic_payload = _to_plain_dict(dynamic_levels)
    if dynamic_payload:
        timestamps["dynamic_levels"] = _epoch_to_iso(dynamic_payload.get("feed_bar_ts") or dynamic_payload.get("as_of_ts"))

    derived_level_interactions = _derive_level_interactions(dynamic_payload if dynamic_payload else None)

    return {
        "contract": SURVEYOR_LIFECYCLE_CONTRACT,
        "zone_interactions": [_to_plain(row) for row in (zone_interactions or [])],
        "level_interactions": [_to_plain(row) for row in (level_interactions or [])] + derived_level_interactions,
        "retests": [_to_plain(row) for row in (retests or [])],
        "breaches": [_to_plain(row) for row in (breaches or [])],
        "state_changes": derived_state_changes + [_to_plain(row) for row in (state_changes or [])],
        "timestamps": timestamps,
    }


def build_contract_versions(
    *,
    structure_section: Mapping[str, Any],
    sr_section: Mapping[str, Any],
    fib_section: Mapping[str, Any],
    dynamic_levels_section: Mapping[str, Any],
) -> dict[str, Any]:
    structure_payload = _to_plain_dict(structure_section)
    sr_payload = _to_plain_dict(sr_section)
    fib_payload = _to_plain_dict(fib_section)
    dynamic_payload = _to_plain_dict(dynamic_levels_section)

    structure_versions = {
        str(surface.get("source_contract_version") or surface.get("contract_version") or "unknown")
        for surface in (structure_payload.get("timeframes") or {}).values()
        if isinstance(surface, Mapping)
    }
    structure_contract = ",".join(sorted(structure_versions)) if structure_versions else PHASE1_STRUCTURE_CONTRACT

    return {
        "packet_contract": SURVEYOR_PACKET_CONTRACT,
        "feed_contract": SURVEYOR_DATA_FEED_CONTRACT,
        "structure_contract": structure_contract,
        "sr_contract": sr_payload.get("source_contract_version") or SURVEYOR_SR_CONTRACT_FALLBACK,
        "fib_contract": fib_payload.get("source_contract_version") or SURVEYOR_FIB_CONTRACT_FALLBACK,
        "dynamic_levels_contract": dynamic_payload.get("source_contract_version") or dynamic_payload.get("contract") or dynamic_payload.get("source_contract_version") or "unknown",
        "lifecycle_contract": SURVEYOR_LIFECYCLE_CONTRACT,
    }


def determine_packet_status(
    *,
    market_data: Mapping[str, Any],
    structure: Mapping[str, Any],
    sr: Mapping[str, Any],
    fib: Mapping[str, Any],
    dynamic_levels: Mapping[str, Any],
    interaction_lifecycle: Mapping[str, Any],
) -> str:
    if not market_data or not structure or not interaction_lifecycle:
        return "degraded"

    market_timeframes = market_data.get("timeframes") if isinstance(market_data.get("timeframes"), Mapping) else {}
    structure_timeframes = structure.get("timeframes") if isinstance(structure.get("timeframes"), Mapping) else {}

    missing_market = [tf for tf in REQUIRED_SURVEYOR_TIMEFRAMES if tf not in market_timeframes]
    missing_structure = [tf for tf in REQUIRED_SURVEYOR_TIMEFRAMES if tf not in structure_timeframes]
    incomplete_market = [
        tf
        for tf in REQUIRED_SURVEYOR_TIMEFRAMES
        if isinstance(market_timeframes.get(tf), Mapping) and str(market_timeframes[tf].get("freshness_state") or "") == "partial"
    ]
    incomplete_structure = [
        tf
        for tf in REQUIRED_SURVEYOR_TIMEFRAMES
        if isinstance(structure_timeframes.get(tf), Mapping) and str(structure_timeframes[tf].get("status") or "") != "ok"
    ]

    has_sr = bool(sr.get("selected_surfaces"))
    has_fib = bool(fib.get("contexts_by_timeframe"))
    has_dynamic = bool(dynamic_levels.get("levels"))

    if missing_market or missing_structure or incomplete_market or incomplete_structure or not has_sr or not has_fib or not has_dynamic:
        return "partial"
    return "complete"


def assemble_surveyor_packet(
    *,
    symbol: str,
    intended_direction_context: str,
    build_mode: str,
    market_data: Mapping[str, Any],
    structure: Mapping[str, Any],
    sr: Mapping[str, Any],
    fib: Mapping[str, Any],
    dynamic_levels: Mapping[str, Any],
    interaction_lifecycle: Mapping[str, Any] | None = None,
    packet_id: str | None = None,
    as_of_ts: Any = None,
) -> dict[str, Any]:
    market_payload = _to_plain_dict(market_data)
    structure_payload = _to_plain_dict(structure)
    sr_payload = _to_plain_dict(sr)
    fib_payload = _to_plain_dict(fib)
    dynamic_payload = _to_plain_dict(dynamic_levels)

    resolved_as_of_ts = (
        _epoch_to_iso(as_of_ts)
        or market_payload.get("packet_as_of_ts")
        or _epoch_to_iso(dynamic_payload.get("as_of_ts"))
        or _now_utc_iso()
    )
    lifecycle_payload = _to_plain_dict(
        interaction_lifecycle
        or build_interaction_lifecycle_section(
            structure_section=structure_payload,
            dynamic_levels=dynamic_payload,
            as_of_ts=resolved_as_of_ts,
        )
    )
    contract_versions = build_contract_versions(
        structure_section=structure_payload,
        sr_section=sr_payload,
        fib_section=fib_payload,
        dynamic_levels_section=dynamic_payload,
    )
    packet_status = determine_packet_status(
        market_data=market_payload,
        structure=structure_payload,
        sr=sr_payload,
        fib=fib_payload,
        dynamic_levels=dynamic_payload,
        interaction_lifecycle=lifecycle_payload,
    )

    return {
        "meta": {
            "packet_id": packet_id or f"surveyor:{symbol}:{uuid4().hex}",
            "symbol": symbol,
            "as_of_ts": resolved_as_of_ts,
            "intended_direction_context": intended_direction_context,
            "build_mode": build_mode,
            "packet_status": packet_status,
        },
        "market_data": market_payload,
        "structure": structure_payload,
        "sr": sr_payload,
        "fib": fib_payload,
        "dynamic_levels": dynamic_payload,
        "interaction_lifecycle": lifecycle_payload,
        "contract_versions": contract_versions,
    }
