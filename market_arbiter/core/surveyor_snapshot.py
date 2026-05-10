from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping, Sequence


from market_arbiter.ops.surveyor_feed_runner import FeedRunnerConfig, collect_status


from market_arbiter.surveyor.dynamic_levels import build_dynamic_level_packet
from market_arbiter.surveyor.fib_anchors import (
    build_phase1_contract_context_for_timeframe,
    compute_fib_level_tap_history_for_timeframe,
    select_phase1_contract_anchor_for_timeframe,
)
from market_arbiter.surveyor.fib_context import FibConfig, FibTimeframe, aggregate_fib_context, compute_timeframe_state
from market_arbiter.surveyor.phase1_contract import PHASE1_STRUCTURE_PROFILE_CANONICAL
from market_arbiter.surveyor.surveyor_packet import (
    REQUIRED_SURVEYOR_TIMEFRAMES,
    assemble_surveyor_packet,
    build_dynamic_levels_section,
    build_fib_section,
    build_market_data_section,
    build_sr_section,
    build_structure_section,
)
from market_arbiter.surveyor.zones import Zone, ZoneKind

from .pair_analytics import load_candles_from_csv
from .sr_universe import resolve_market_structure_csv


SURVEYOR_PRIMARY_PROVIDER = "OKX"
SURVEYOR_PRIMARY_PROVIDER_ID = "ccxt"
SURVEYOR_PRIMARY_VENUE = "okx"
_STORE_TF = {"1W": "1w", "1D": "1d", "4H": "4h", "5m": "5m"}
_TF_SECONDS = {"1W": 7 * 24 * 60 * 60, "1D": 24 * 60 * 60, "4H": 4 * 60 * 60, "5m": 5 * 60}
_REPLAY_LIMITS = {"1D": 800, "4H": 1200}
_RUNNER_TIMEFRAMES = ["5m", "4h", "1d", "1w"]
_BUNDLE_CONTRACT = "surveyor_unified_dataset_bundle_v1"


@dataclass(frozen=True)
class SurveyorBar:
    index: int
    open: float
    high: float
    low: float
    close: float
    timestamp: int | None = None
    volume: float | None = None


def _utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _symbol_aliases(symbol: str) -> list[str]:
    base = str(symbol or "").upper().strip()
    aliases = [base]
    if base.endswith("USDT"):
        aliases.append(f"{base[:-4]}/USDT")
    return list(dict.fromkeys(alias for alias in aliases if alias))


def _feed_provider_label(provider_id: str | None, venue: str | None) -> str:
    venue_text = str(venue or "").strip()
    provider_text = str(provider_id or "").strip()
    if venue_text.lower() == SURVEYOR_PRIMARY_VENUE:
        return SURVEYOR_PRIMARY_PROVIDER
    return venue_text.upper() or provider_text.upper() or SURVEYOR_PRIMARY_PROVIDER


def _json_object(value: Any) -> dict[str, Any] | None:
    if not value:
        return None
    try:
        payload = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return dict(payload) if isinstance(payload, Mapping) else None


def _load_feed_checkpoint(conn: sqlite3.Connection, symbol: str, timeframe: str) -> dict[str, Any] | None:
    aliases = _symbol_aliases(symbol)
    placeholders = ",".join("?" for _ in aliases)
    row = conn.execute(
        f"""
        SELECT provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms,
               failure_count, state, last_reason_code, trace_id
        FROM feed_checkpoints
        WHERE timeframe = ? AND symbol IN ({placeholders})
        ORDER BY CASE WHEN venue = ? THEN 0 ELSE 1 END, last_attempt_ms DESC
        LIMIT 1;
        """,
        (timeframe, *aliases, SURVEYOR_PRIMARY_VENUE),
    ).fetchone()
    if not row:
        return None
    return {
        "provider_id": row[0],
        "venue": row[1],
        "symbol": row[2],
        "timeframe": row[3],
        "last_ts_open_ms": row[4],
        "last_success_ms": row[5],
        "last_attempt_ms": row[6],
        "failure_count": row[7],
        "state": row[8],
        "last_reason_code": row[9],
        "trace_id": row[10],
    }


def _load_latest_feed_health_event(conn: sqlite3.Connection, symbol: str, timeframe: str) -> dict[str, Any] | None:
    aliases = _symbol_aliases(symbol)
    placeholders = ",".join("?" for _ in aliases)
    row = conn.execute(
        f"""
        SELECT provider_id, venue, symbol, timeframe, state, reason_codes_json, as_of_ms, trace_id, metadata_json
        FROM feed_health_events
        WHERE timeframe = ? AND symbol IN ({placeholders})
        ORDER BY
            CASE WHEN venue = ? THEN 0 ELSE 1 END,
            CASE WHEN metadata_json IS NOT NULL AND metadata_json != '' THEN 0 ELSE 1 END,
            as_of_ms DESC,
            id DESC
        LIMIT 1;
        """,
        (timeframe, *aliases, SURVEYOR_PRIMARY_VENUE),
    ).fetchone()
    if not row:
        return None
    return {
        "provider_id": row[0],
        "venue": row[1],
        "symbol": row[2],
        "timeframe": row[3],
        "state": row[4],
        "reason_codes": json.loads(row[5]) if row[5] else [],
        "as_of_ms": row[6],
        "trace_id": row[7],
        "metadata": _json_object(row[8]),
    }


def _load_market_candles(conn: sqlite3.Connection, symbol: str, timeframe: str, *, limit: int) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    aliases = _symbol_aliases(symbol)
    placeholders = ",".join("?" for _ in aliases)
    rows = conn.execute(
        f"""
        SELECT provider_id, venue, symbol, timeframe, ts_open_ms, ts_close_ms,
               open, high, low, close, volume, ingest_ts_ms, dataset_version, trace_id
        FROM market_candles
        WHERE timeframe = ? AND symbol IN ({placeholders})
        ORDER BY CASE WHEN venue = ? THEN 0 ELSE 1 END, ts_open_ms DESC
        LIMIT ?;
        """,
        (timeframe, *aliases, SURVEYOR_PRIMARY_VENUE, int(limit)),
    ).fetchall()
    if not rows:
        return [], None

    ordered = list(reversed(rows))
    candles = [
        {
            "timestamp": int(row[4] // 1000),
            "open_time": int(row[4] // 1000),
            "close_time": int(row[5] // 1000),
            "open": float(row[6]),
            "high": float(row[7]),
            "low": float(row[8]),
            "close": float(row[9]),
            "volume": float(row[10]),
        }
        for row in ordered
    ]
    meta = {
        "provider_id": ordered[-1][0],
        "venue": ordered[-1][1],
        "symbol": ordered[-1][2],
        "timeframe": ordered[-1][3],
        "latest_open_ms": ordered[-1][4],
        "latest_close_ms": ordered[-1][5],
        "latest_ingest_ms": ordered[-1][11],
        "dataset_version": ordered[-1][12],
        "trace_id": ordered[-1][13],
        "history_start_ms": ordered[0][4],
        "history_end_ms": ordered[-1][5],
        "bar_count": len(ordered),
    }
    return candles, meta


def _freshness_state(*, timeframe: str, checkpoint: Mapping[str, Any] | None, latest_close_ms: int | None, now_ms: int) -> tuple[str, str]:
    if latest_close_ms is None:
        return "partial", "missing_timeframe_input"

    cp_state = str((checkpoint or {}).get("state") or "").strip().lower()
    if cp_state in {"blocked", "degraded", "tripped", "resync_required"}:
        return "stale", str((checkpoint or {}).get("last_reason_code") or cp_state)

    threshold_ms = _TF_SECONDS[timeframe] * 1000 * 3
    age_ms = max(0, int(now_ms) - int(latest_close_ms))
    if age_ms > threshold_ms:
        return "stale", "checkpoint_or_candle_age_exceeded"
    return "fresh", str((checkpoint or {}).get("state") or "scheduler_ok")


def load_surveyor_timeframe_inputs(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    allow_replay_fallback: bool = True,
    now_ms: int | None = None,
) -> dict[str, dict[str, Any]]:
    resolved_now_ms = int(now_ms or _utc_now_ms())
    inputs: dict[str, dict[str, Any]] = {}
    for tf in REQUIRED_SURVEYOR_TIMEFRAMES:
        store_tf = _STORE_TF[tf]
        limit = 2500 if tf == "5m" else (400 if tf == "1W" else 800)
        checkpoint = _load_feed_checkpoint(conn, symbol, store_tf)
        health_event = _load_latest_feed_health_event(conn, symbol, store_tf)
        candles, meta = _load_market_candles(conn, symbol, store_tf, limit=limit)
        health_metadata = dict((health_event or {}).get("metadata") or {})
        repair_summary = dict(health_metadata.get("repair_summary") or {}) if isinstance(health_metadata.get("repair_summary"), Mapping) else None
        if candles and meta:
            freshness_state, freshness_reason = _freshness_state(
                timeframe=tf,
                checkpoint=checkpoint,
                latest_close_ms=meta.get("latest_close_ms"),
                now_ms=resolved_now_ms,
            )
            provider = _feed_provider_label(meta.get("provider_id"), meta.get("venue"))
            dataset_version = str(meta.get("dataset_version") or "market_candles_v1")
            trace_id = str(meta.get("trace_id") or "unknown")
            inputs[tf] = {
                "candles": candles,
                "feed_provider": provider,
                "dataset_mode": "live",
                "dataset_id": f"market_candles:{dataset_version}:{trace_id}",
                "latest_open_time": int(meta["latest_open_ms"] // 1000),
                "latest_close_time": int(meta["latest_close_ms"] // 1000),
                "latest_ingested_at": int(meta["latest_ingest_ms"] // 1000),
                "freshness_state": freshness_state,
                "freshness_reason": freshness_reason,
                "history_start_time": int(meta["history_start_ms"] // 1000),
                "history_end_time": int(meta["history_end_ms"] // 1000),
                "feed_status": checkpoint,
                "feed_health_event": health_event,
                "quality_band": health_metadata.get("quality_band"),
                "circuit_breaker_action": health_metadata.get("circuit_breaker_action"),
                "repair_policy_contract": health_metadata.get("repair_policy_contract"),
                "repair_provenance": dict(health_metadata.get("repair_provenance") or {}) if isinstance(health_metadata.get("repair_provenance"), Mapping) else None,
                "repair_summary": repair_summary,
                "source_kind": "market_candles",
            }
            continue

        if allow_replay_fallback and tf in {"1D", "4H"}:
            csv_path = resolve_market_structure_csv(symbol, tf)
            if csv_path and Path(csv_path).exists():
                replay_candles = load_candles_from_csv(csv_path, limit=_REPLAY_LIMITS[tf])
                if replay_candles:
                    inputs[tf] = {
                        "candles": replay_candles,
                        "feed_provider": SURVEYOR_PRIMARY_PROVIDER,
                        "dataset_mode": "certified_replay",
                        "dataset_id": str(csv_path),
                        "freshness_state": "replay_only",
                        "freshness_reason": "csv_replay_fallback",
                        "source_kind": "csv_replay",
                    }
                    continue

        inputs[tf] = {
            "candles": [],
            "feed_provider": SURVEYOR_PRIMARY_PROVIDER,
            "dataset_mode": "live",
            "dataset_id": f"missing:{symbol}:{tf}",
            "freshness_state": "partial",
            "freshness_reason": str((checkpoint or {}).get("last_reason_code") or (checkpoint or {}).get("state") or "no_store_or_replay_source"),
            "feed_status": checkpoint,
            "feed_health_event": health_event,
            "quality_band": health_metadata.get("quality_band"),
            "circuit_breaker_action": health_metadata.get("circuit_breaker_action"),
            "repair_policy_contract": health_metadata.get("repair_policy_contract"),
            "repair_provenance": dict(health_metadata.get("repair_provenance") or {}) if isinstance(health_metadata.get("repair_provenance"), Mapping) else None,
            "repair_summary": repair_summary,
            "source_kind": "missing",
        }

    return inputs


def _build_bars_from_candles(candles: Sequence[Mapping[str, Any]]) -> tuple[list[SurveyorBar], list[int], list[float]]:
    bars: list[SurveyorBar] = []
    timestamps: list[int] = []
    volumes: list[float] = []
    for idx, candle in enumerate(candles):
        ts = int(candle.get("timestamp") or candle.get("open_time") or candle.get("close_time") or 0)
        vol = _safe_float(candle.get("volume")) or 0.0
        bars.append(
            SurveyorBar(
                index=idx,
                open=float(candle.get("open") or 0.0),
                high=float(candle.get("high") or 0.0),
                low=float(candle.get("low") or 0.0),
                close=float(candle.get("close") or 0.0),
                timestamp=ts,
                volume=vol,
            )
        )
        timestamps.append(ts)
        volumes.append(vol)
    return bars, timestamps, volumes


def _pick_intended_direction(structure_section: Mapping[str, Any]) -> str:
    timeframes = structure_section.get("timeframes") if isinstance(structure_section.get("timeframes"), Mapping) else {}
    for tf in ("4H", "1D", "1W", "5m"):
        payload = timeframes.get(tf) if isinstance(timeframes, Mapping) else None
        direction = str((payload or {}).get("regime_direction") or "").strip().lower()
        if direction in {"bullish", "bearish"}:
            return direction
    return "unknown"


def _pick_selected_zone(authoritative_view: Mapping[str, Any] | None, ladders: Mapping[str, Any] | None, current_price: float | None) -> Zone | None:
    candidates: list[dict[str, Any]] = []
    if isinstance(authoritative_view, Mapping):
        timeframe_views = authoritative_view.get("timeframes") if isinstance(authoritative_view.get("timeframes"), Mapping) else {}
        for tf in ("4H", "1D"):
            payload = timeframe_views.get(tf) if isinstance(timeframe_views, Mapping) else None
            groups = payload.get("groups") if isinstance(payload, Mapping) else {}
            for key in ("contains_price", "below_price", "above_price"):
                rows = groups.get(key) if isinstance(groups, Mapping) else None
                if isinstance(rows, list):
                    candidates.extend([row for row in rows if isinstance(row, dict)])

    if not candidates and isinstance(ladders, Mapping):
        for key in ("nearest_support", "nearest_resistance", "next_support", "next_resistance"):
            row = ladders.get(key)
            if isinstance(row, dict):
                candidates.append(row)

    if not candidates:
        return None

    def _distance(zone: Mapping[str, Any]) -> float:
        bounds = zone.get("bounds") if isinstance(zone.get("bounds"), Mapping) else zone
        low = _safe_float(bounds.get("low") if isinstance(bounds, Mapping) else None)
        high = _safe_float(bounds.get("high") if isinstance(bounds, Mapping) else None)
        mid = _safe_float(bounds.get("mid") if isinstance(bounds, Mapping) else None)
        center = mid if mid is not None else ((low + high) / 2.0 if low is not None and high is not None else None)
        if center is None or current_price is None:
            return float("inf")
        return abs(center - current_price)

    chosen = min(candidates, key=_distance)
    bounds = chosen.get("bounds") if isinstance(chosen.get("bounds"), Mapping) else chosen
    low = _safe_float(bounds.get("low") if isinstance(bounds, Mapping) else None)
    high = _safe_float(bounds.get("high") if isinstance(bounds, Mapping) else None)
    if low is None or high is None:
        return None
    role = str(chosen.get("current_role") or chosen.get("kind") or chosen.get("zone_kind") or "").strip().lower()
    kind = ZoneKind.SUPPORT if role == "support" else ZoneKind.RESISTANCE
    return Zone(
        id=str(chosen.get("zone_id") or f"zone:{kind.value}:{low}:{high}"),
        kind=kind,
        low=float(low),
        high=float(high),
        created_at=0,
    )


def _build_fib_and_dynamic_sections(
    *,
    symbol: str,
    candles_5m: Sequence[Mapping[str, Any]],
    structure_section: Mapping[str, Any],
    authoritative_view: Mapping[str, Any] | None,
    ladders: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not candles_5m:
        return {}, {}

    bars, timestamps, volumes = _build_bars_from_candles(candles_5m)
    if not bars:
        return {}, {}

    latest_bar = bars[-1]
    structure_5m = (structure_section.get("timeframes") or {}).get("5m") if isinstance(structure_section.get("timeframes"), Mapping) else {}
    intended_direction = _pick_intended_direction(structure_section)
    default_bias_side = "long" if intended_direction != "bearish" else "short"
    as_of_bar_count = len(bars)
    as_of_ts = str(latest_bar.timestamp or 0)

    fib_cfg = FibConfig()
    anchor_rows: list[tuple[str, FibTimeframe, Any, str, str]] = []
    for target_tf, fib_tf in (("4h", FibTimeframe.H4), ("1d", FibTimeframe.D1), ("1w", FibTimeframe.W1)):
        ctx = build_phase1_contract_context_for_timeframe(bars, base_tf="5m", target_tf=target_tf)
        anchor, bias_side, confidence = select_phase1_contract_anchor_for_timeframe(
            ctx,
            as_of_bar_count=as_of_bar_count,
            fallback_bias_side=default_bias_side,
        )
        anchor_rows.append((target_tf, fib_tf, anchor, bias_side, confidence))

    fib_states = [
        compute_timeframe_state(
            timeframe=fib_tf,
            as_of_index=latest_bar.index,
            as_of_ts=as_of_ts,
            bias_side=bias_side,
            anchor_start_id=anchor.start_id,
            anchor_end_id=anchor.end_id,
            anchor_start_price=anchor.start_price,
            anchor_end_price=anchor.end_price,
            opposite_end_swept=anchor.opposite_end_swept,
            structure_superseded=False,
            bar_high=latest_bar.high,
            bar_low=latest_bar.low,
            bar_close=latest_bar.close,
            cfg=fib_cfg,
        )
        for _target_tf, fib_tf, anchor, bias_side, _confidence in anchor_rows
    ]
    fib_context = aggregate_fib_context(
        as_of_index=latest_bar.index,
        as_of_ts=as_of_ts,
        timeframe_states=fib_states,
        cfg=fib_cfg,
    )

    tap_history: dict[str, Any] = {}
    anchor_provenance: dict[str, Any] = {}
    for target_tf, _fib_tf, anchor, bias_side, confidence in anchor_rows:
        state = next((row for row in fib_states if row.timeframe.value == target_tf), None)
        if state is None:
            continue
        tap_history[target_tf] = compute_fib_level_tap_history_for_timeframe(
            bars,
            base_tf="5m",
            target_tf=target_tf,
            anchor=anchor,
            level_0_618=state.level_0_618,
            level_0_705=state.level_0_705,
            level_0_786=state.level_0_786,
        )
        anchor_provenance[target_tf] = {
            **asdict(anchor),
            "bias_side": bias_side,
            "phase1_confidence": confidence,
            "base_timeframe": "5m",
            "target_timeframe": target_tf,
        }

    fib_section = build_fib_section(
        fib_context=fib_context,
        timeframe_states=fib_states,
        tap_history=tap_history,
        anchor_provenance=anchor_provenance,
        source_event_id=structure_5m.get("source_event_id"),
        source_swing_id=structure_5m.get("source_swing_id"),
        source_contract_version=structure_5m.get("source_contract_version"),
    )

    selected_zone = _pick_selected_zone(authoritative_view, ladders, latest_bar.close)
    dynamic_packet = build_dynamic_level_packet(
        bars,
        as_of_bar_index=len(bars) - 1,
        symbol=symbol,
        base_tf="5m",
        intended_direction=intended_direction,
        selected_zone=selected_zone,
        timestamps=timestamps,
        volumes=volumes,
        feed_provider=SURVEYOR_PRIMARY_PROVIDER,
        feed_provenance_note="surveyor_ui.market_candles",
        source_event_id=structure_5m.get("source_event_id"),
        source_swing_id=structure_5m.get("source_swing_id"),
        source_contract_version=structure_5m.get("source_contract_version"),
        fib_context_id=f"fib:{fib_context.as_of_ts}",
    )
    dynamic_section = build_dynamic_levels_section(dynamic_packet)
    dynamic_section["symbol"] = dynamic_section.get("symbol") or ""
    return fib_section, dynamic_section


def _connection_db_path(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("PRAGMA database_list;").fetchone()
    if not row:
        return None
    path = row[2]
    return str(path) if path else None


def _runner_status_for_symbol(conn: sqlite3.Connection, *, symbol: str) -> dict[str, Any] | None:
    db_path = _connection_db_path(conn)
    if not db_path:
        return None
    return collect_status(
        FeedRunnerConfig(
            db_path=db_path,
            symbols=[symbol],
            timeframes=list(_RUNNER_TIMEFRAMES),
        )
    )


def _dataset_status(*, missing: bool = False, stale: bool = False, unavailable: bool = False, degraded: bool = False, replay_only: bool = False) -> str:
    if degraded:
        return "degraded"
    if stale:
        return "stale"
    if unavailable:
        return "unavailable"
    if missing:
        return "partial"
    if replay_only:
        return "replay_only"
    return "complete"


def _build_feed_dataset(market_data: Mapping[str, Any], runner_status: Mapping[str, Any] | None) -> dict[str, Any]:
    timeframes = market_data.get("timeframes") if isinstance(market_data.get("timeframes"), Mapping) else {}
    available = [tf for tf in REQUIRED_SURVEYOR_TIMEFRAMES if int((timeframes.get(tf) or {}).get("bar_count_available") or 0) > 0]
    missing = [tf for tf in REQUIRED_SURVEYOR_TIMEFRAMES if tf not in available]
    freshness_summary = {"fresh": [], "stale": [], "partial": [], "replay_only": []}
    quality_band_summary = {"clean": [], "benign": [], "elevated": [], "degraded": [], "blocked": [], "unknown": []}
    repair_totals = {
        "fetched_candles": 0,
        "accepted_candles": 0,
        "repaired_candles": 0,
        "unrepairable_candles": 0,
    }
    issue_reasons: list[dict[str, Any]] = []
    for tf in REQUIRED_SURVEYOR_TIMEFRAMES:
        payload = timeframes.get(tf) or {}
        freshness = str(payload.get("freshness_state") or "partial")
        freshness_summary.setdefault(freshness, []).append(tf)
        quality_band = str(payload.get("quality_band") or "unknown")
        quality_band_summary.setdefault(quality_band, []).append(tf)
        repair_summary = payload.get("repair_summary") if isinstance(payload.get("repair_summary"), Mapping) else {}
        for key in repair_totals:
            repair_totals[key] += int(repair_summary.get(key) or 0)
        if freshness in {"stale", "partial"}:
            issue_reasons.append(
                {
                    "family": "feed_state",
                    "timeframe": tf,
                    "issue_kind": "upstream_feed_input",
                    "reason": payload.get("freshness_reason"),
                    "dataset_id": payload.get("dataset_id"),
                }
            )
        if quality_band in {"elevated", "degraded", "blocked"}:
            issue_reasons.append(
                {
                    "family": "feed_state",
                    "timeframe": tf,
                    "issue_kind": "historical_repair_quality",
                    "reason": quality_band,
                    "repair_rate": repair_summary.get("repair_rate"),
                    "repaired_candles": repair_summary.get("repaired_candles"),
                    "unrepairable_candles": repair_summary.get("unrepairable_candles"),
                    "circuit_breaker_action": payload.get("circuit_breaker_action"),
                }
            )

    status = _dataset_status(
        missing=bool(missing),
        stale=bool(freshness_summary.get("stale")),
        degraded=bool(quality_band_summary.get("degraded") or quality_band_summary.get("blocked")),
        unavailable=len(available) == 0,
        replay_only=len(available) > 0 and len(available) == len(freshness_summary.get("replay_only", [])),
    )
    return {
        "family": "feed_state",
        "contract_version": market_data.get("contract") or "surveyor_feed_state_dataset_v1",
        "status": status,
        "summary": {
            "available_timeframes": available,
            "missing_timeframes": missing,
            "freshness_summary": freshness_summary,
            "quality_band_summary": quality_band_summary,
            "repair_totals": repair_totals,
            "continuity_state": (runner_status or {}).get("continuity_state", "unknown"),
            "issue_count": len(issue_reasons),
        },
        "timeframes": dict(timeframes),
        "provenance": {
            "feed_provider": market_data.get("provider") or SURVEYOR_PRIMARY_PROVIDER,
            "dataset_mode": market_data.get("dataset_mode"),
            "dataset_id": market_data.get("dataset_id"),
            "runner_state_path": (runner_status or {}).get("state_path"),
        },
        "payload": dict(market_data),
        "issues": issue_reasons,
    }


def _build_structure_dataset(structure: Mapping[str, Any], feed_dataset: Mapping[str, Any]) -> dict[str, Any]:
    surfaces = structure.get("timeframes") if isinstance(structure.get("timeframes"), Mapping) else {}
    feed_timeframes = ((feed_dataset.get("payload") or {}).get("timeframes") if isinstance(feed_dataset.get("payload"), Mapping) else {}) or {}
    issues: list[dict[str, Any]] = []
    suspected_code_defect_timeframes: list[str] = []
    missing_input_timeframes: list[str] = []
    ok_timeframes: list[str] = []

    for tf in REQUIRED_SURVEYOR_TIMEFRAMES:
        surface = surfaces.get(tf) or {}
        feed_tf = feed_timeframes.get(tf) or {}
        feed_bars = int(feed_tf.get("bar_count_available") or 0)
        surface_status = str(surface.get("status") or "missing")
        if surface_status == "ok":
            ok_timeframes.append(tf)
            continue
        if feed_bars > 0:
            suspected_code_defect_timeframes.append(tf)
            issues.append(
                {
                    "family": "structure_state",
                    "timeframe": tf,
                    "issue_kind": "suspected_code_defect",
                    "reason": "structure_missing_despite_feed_input",
                    "feed_bar_count": feed_bars,
                    "feed_freshness_state": feed_tf.get("freshness_state"),
                    "surface_status": surface_status,
                }
            )
        else:
            missing_input_timeframes.append(tf)
            issues.append(
                {
                    "family": "structure_state",
                    "timeframe": tf,
                    "issue_kind": "upstream_missing_input",
                    "reason": (feed_tf.get("freshness_reason") or "missing_feed_input"),
                    "surface_status": surface_status,
                }
            )

    status = _dataset_status(
        missing=bool(missing_input_timeframes),
        degraded=bool(suspected_code_defect_timeframes),
        unavailable=len(ok_timeframes) == 0 and not suspected_code_defect_timeframes,
    )
    return {
        "family": "structure_state",
        "contract_version": structure.get("contract") or "surveyor_structure_state_dataset_v1",
        "status": status,
        "summary": {
            "ok_timeframes": ok_timeframes,
            "missing_input_timeframes": missing_input_timeframes,
            "suspected_code_defect_timeframes": suspected_code_defect_timeframes,
            "issue_count": len(issues),
        },
        "timeframes": dict(surfaces),
        "provenance": {
            "upstream_family": "feed_state",
        },
        "payload": dict(structure),
        "issues": issues,
    }


def _build_sr_dataset(sr: Mapping[str, Any]) -> dict[str, Any]:
    surfaces = sr.get("selected_surfaces") if isinstance(sr.get("selected_surfaces"), Mapping) else {}
    levels_by_tf = sr.get("levels_by_timeframe") if isinstance(sr.get("levels_by_timeframe"), Mapping) else {}
    available_timeframes = [tf for tf, rows in levels_by_tf.items() if isinstance(rows, list) and rows]
    lifecycle_summary = _summarize_sr_lifecycle(levels_by_tf)
    status = _dataset_status(unavailable=len(available_timeframes) == 0)
    issues = []
    if not available_timeframes:
        issues.append(
            {
                "family": "sr_zones",
                "issue_kind": "unavailable_family_input",
                "reason": "authoritative_view_not_provided",
            }
        )
    return {
        "family": "sr_zones",
        "contract_version": sr.get("source_contract_version") or "surveyor_sr_dataset_v1",
        "status": status,
        "summary": {
            "available_timeframes": sorted(available_timeframes),
            "selector_surfaces": dict(sr.get("source_selector_surface") or {}),
            "metadata_zone_count": lifecycle_summary["metadata_zone_count"],
            "lifecycle_status_counts": lifecycle_summary["lifecycle_status_counts"],
            "confidence_tier_counts": lifecycle_summary["confidence_tier_counts"],
            "decision_eligibility_counts": lifecycle_summary["decision_eligibility_counts"],
            "hard_invalid_or_blocked_zone_count": lifecycle_summary["hard_invalid_or_blocked_zone_count"],
            "rejected_zone_count": lifecycle_summary["rejected_zone_count"],
            "issue_count": len(issues),
        },
        "timeframes": dict(surfaces),
        "provenance": {
            "source_contract_version": sr.get("source_contract_version"),
        },
        "payload": dict(sr),
        "issues": issues,
    }


def _summarize_sr_lifecycle(levels_by_tf: Mapping[str, Any]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    tier_counts: dict[str, int] = {}
    eligibility_counts: dict[str, int] = {}
    metadata_zone_count = 0
    hard_statuses = {"invalidated", "flipped_pending", "expired", "blocked", "superseded"}
    hard_count = 0
    rejected_count = 0

    for rows in levels_by_tf.values():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            lifecycle = row.get("lifecycle") if isinstance(row.get("lifecycle"), Mapping) else {}
            quality = row.get("quality") if isinstance(row.get("quality"), Mapping) else {}
            visual = row.get("visual") if isinstance(row.get("visual"), Mapping) else {}
            if not lifecycle and not quality and not visual:
                continue
            metadata_zone_count += 1
            status = str(lifecycle.get("status") or "unknown")
            tier = str(quality.get("confidence_tier") or "unknown")
            eligibility = str(quality.get("decision_eligibility") or "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1
            tier_counts[tier] = tier_counts.get(tier, 0) + 1
            eligibility_counts[eligibility] = eligibility_counts.get(eligibility, 0) + 1
            if status in hard_statuses:
                hard_count += 1
            if eligibility == "reject":
                rejected_count += 1

    return {
        "metadata_zone_count": metadata_zone_count,
        "lifecycle_status_counts": status_counts,
        "confidence_tier_counts": tier_counts,
        "decision_eligibility_counts": eligibility_counts,
        "hard_invalid_or_blocked_zone_count": hard_count,
        "rejected_zone_count": rejected_count,
    }


def _build_fib_dataset(fib: Mapping[str, Any], feed_dataset: Mapping[str, Any]) -> dict[str, Any]:
    contexts = fib.get("contexts_by_timeframe") if isinstance(fib.get("contexts_by_timeframe"), Mapping) else {}
    feed_timeframes = ((feed_dataset.get("payload") or {}).get("timeframes") if isinstance(feed_dataset.get("payload"), Mapping) else {}) or {}
    feed_5m = feed_timeframes.get("5m") or {}
    issues: list[dict[str, Any]] = []

    if int(feed_5m.get("bar_count_available") or 0) == 0:
        status = "unavailable"
        issues.append(
            {
                "family": "fib_context",
                "timeframe": "5m",
                "issue_kind": "upstream_missing_input",
                "reason": feed_5m.get("freshness_reason") or "missing_5m_input",
            }
        )
    elif not contexts:
        status = "degraded"
        issues.append(
            {
                "family": "fib_context",
                "timeframe": "5m",
                "issue_kind": "suspected_code_defect",
                "reason": "fib_missing_despite_5m_feed_input",
            }
        )
    else:
        status = "complete"

    return {
        "family": "fib_context",
        "contract_version": fib.get("source_contract_version") or "surveyor_fib_context_dataset_v1",
        "status": status,
        "summary": {
            "available_timeframes": sorted(contexts.keys()),
            "active_timeframes": list((fib.get("summary") or {}).get("active_timeframes") or []),
            "issue_count": len(issues),
        },
        "timeframes": dict(contexts),
        "provenance": {
            "source_event_id": fib.get("source_event_id"),
            "source_swing_id": fib.get("source_swing_id"),
            "source_contract_version": fib.get("source_contract_version"),
        },
        "payload": dict(fib),
        "issues": issues,
    }


def _build_dynamic_levels_dataset(dynamic: Mapping[str, Any], feed_dataset: Mapping[str, Any]) -> dict[str, Any]:
    levels = dynamic.get("levels") if isinstance(dynamic.get("levels"), list) else []
    feed_timeframes = ((feed_dataset.get("payload") or {}).get("timeframes") if isinstance(feed_dataset.get("payload"), Mapping) else {}) or {}
    feed_5m = feed_timeframes.get("5m") or {}
    issues: list[dict[str, Any]] = []
    if int(feed_5m.get("bar_count_available") or 0) == 0:
        status = "unavailable"
        issues.append(
            {
                "family": "dynamic_levels",
                "timeframe": "5m",
                "issue_kind": "upstream_missing_input",
                "reason": feed_5m.get("freshness_reason") or "missing_5m_input",
            }
        )
    elif not levels:
        status = "degraded"
        issues.append(
            {
                "family": "dynamic_levels",
                "timeframe": "5m",
                "issue_kind": "suspected_code_defect",
                "reason": "dynamic_levels_missing_despite_5m_feed_input",
            }
        )
    else:
        status = "complete"

    return {
        "family": "dynamic_levels",
        "contract_version": dynamic.get("source_contract_version") or dynamic.get("contract") or "surveyor_dynamic_levels_dataset_v1",
        "status": status,
        "summary": {
            "level_count": len(levels),
            "issue_count": len(issues),
        },
        "timeframes": {
            str(level.get("timeframe") or "unknown"): level
            for level in levels
            if isinstance(level, Mapping)
        },
        "provenance": {
            "source_event_id": dynamic.get("source_event_id"),
            "source_swing_id": dynamic.get("source_swing_id"),
            "source_contract_version": dynamic.get("source_contract_version"),
        },
        "payload": dict(dynamic),
        "issues": issues,
    }


def _build_interaction_lifecycle_dataset(lifecycle: Mapping[str, Any], structure_dataset: Mapping[str, Any]) -> dict[str, Any]:
    state_changes = lifecycle.get("state_changes") if isinstance(lifecycle.get("state_changes"), list) else []
    level_interactions = lifecycle.get("level_interactions") if isinstance(lifecycle.get("level_interactions"), list) else []
    zone_interactions = lifecycle.get("zone_interactions") if isinstance(lifecycle.get("zone_interactions"), list) else []
    structure_status = str(structure_dataset.get("status") or "partial")
    issues: list[dict[str, Any]] = []
    if structure_status == "degraded":
        status = "degraded"
        issues.append(
            {
                "family": "interaction_lifecycle",
                "issue_kind": "downstream_of_structure_failure",
                "reason": "structure_state_degraded",
            }
        )
    elif structure_status in {"partial", "unavailable", "stale"}:
        status = "partial"
    else:
        status = "complete"
    return {
        "family": "interaction_lifecycle",
        "contract_version": lifecycle.get("contract") or "surveyor_interaction_lifecycle_dataset_v1",
        "status": status,
        "summary": {
            "state_change_count": len(state_changes),
            "level_interaction_count": len(level_interactions),
            "zone_interaction_count": len(zone_interactions),
            "issue_count": len(issues),
        },
        "timeframes": {},
        "provenance": {
            "upstream_family": "structure_state",
        },
        "payload": dict(lifecycle),
        "issues": issues,
    }


def _bundle_coverage(feed_dataset: Mapping[str, Any]) -> dict[str, Any]:
    summary = dict((feed_dataset.get("summary") or {}).get("freshness_summary") or {})
    for key in ("fresh", "stale", "partial", "replay_only"):
        summary.setdefault(key, [])
    return {
        "required_timeframes": list(REQUIRED_SURVEYOR_TIMEFRAMES),
        "available_timeframes": list((feed_dataset.get("summary") or {}).get("available_timeframes") or []),
        "missing_timeframes": list((feed_dataset.get("summary") or {}).get("missing_timeframes") or []),
        "freshness_summary": summary,
    }


def _bundle_status(datasets: Mapping[str, Mapping[str, Any]]) -> str:
    statuses = [str((payload or {}).get("status") or "partial") for payload in datasets.values()]
    if any(status in {"degraded", "stale"} for status in statuses):
        return "degraded"
    if any(status in {"partial", "unavailable", "replay_only"} for status in statuses):
        return "partial"
    return "complete"


def build_surveyor_dataset_bundle(
    *,
    packet: Mapping[str, Any],
    runner_status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    meta = packet.get("meta") if isinstance(packet.get("meta"), Mapping) else {}
    feed_dataset = _build_feed_dataset(packet.get("market_data") or {}, runner_status)
    structure_dataset = _build_structure_dataset(packet.get("structure") or {}, feed_dataset)
    datasets = {
        "feed_state": feed_dataset,
        "structure_state": structure_dataset,
        "sr_zones": _build_sr_dataset(packet.get("sr") or {}),
        "fib_context": _build_fib_dataset(packet.get("fib") or {}, feed_dataset),
        "dynamic_levels": _build_dynamic_levels_dataset(packet.get("dynamic_levels") or {}, feed_dataset),
        "interaction_lifecycle": _build_interaction_lifecycle_dataset(packet.get("interaction_lifecycle") or {}, structure_dataset),
    }
    issues = []
    for payload in datasets.values():
        issues.extend(list(payload.get("issues") or []))
    return {
        "meta": {
            "bundle_contract": _BUNDLE_CONTRACT,
            "bundle_id": f"surveyor_bundle:{meta.get('symbol') or 'unknown'}:{meta.get('packet_id') or 'na'}",
            "symbol": meta.get("symbol"),
            "as_of_ts": meta.get("as_of_ts"),
            "build_mode": meta.get("build_mode"),
            "bundle_status": _bundle_status(datasets),
            "primary_feed_provider": ((packet.get("market_data") or {}).get("provider") or SURVEYOR_PRIMARY_PROVIDER),
            "continuity_state": (runner_status or {}).get("continuity_state", "unknown"),
            "intended_direction_context": meta.get("intended_direction_context"),
            "legacy_packet_status": meta.get("packet_status"),
        },
        "coverage": _bundle_coverage(feed_dataset),
        "datasets": datasets,
        "delivery_profiles": {
            "ui_full": ["feed_state", "structure_state", "sr_zones", "fib_context", "dynamic_levels", "interaction_lifecycle"],
            "arbiter_core": ["feed_state", "structure_state", "sr_zones", "fib_context", "dynamic_levels"],
            "backtest_core": ["feed_state", "structure_state", "sr_zones", "fib_context", "dynamic_levels", "interaction_lifecycle"],
        },
        "diagnostics": {
            "issue_count": len(issues),
            "issues": issues,
        },
    }


def build_surveyor_packet_snapshot(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    authoritative_view: Mapping[str, Any] | None,
    ladders: Mapping[str, Any] | None = None,
    allow_replay_fallback: bool = True,
) -> dict[str, Any]:
    runner_status = _runner_status_for_symbol(conn, symbol=symbol)
    timeframe_inputs = load_surveyor_timeframe_inputs(
        conn,
        symbol=symbol,
        allow_replay_fallback=allow_replay_fallback,
    )
    market_data = build_market_data_section(
        symbol=symbol,
        provider=SURVEYOR_PRIMARY_PROVIDER,
        dataset_mode="mixed",
        dataset_id=f"surveyor_ui:{symbol}",
        timeframe_inputs=timeframe_inputs,
    )
    for tf, payload in timeframe_inputs.items():
        if tf not in market_data.get("timeframes", {}):
            continue
        extras = {
            "quality_band": payload.get("quality_band"),
            "circuit_breaker_action": payload.get("circuit_breaker_action"),
            "repair_policy_contract": payload.get("repair_policy_contract"),
            "repair_provenance": payload.get("repair_provenance"),
            "repair_summary": payload.get("repair_summary"),
        }
        market_data["timeframes"][tf].update({key: value for key, value in extras.items() if value is not None})
    candles_by_tf = {
        tf: payload.get("candles")
        for tf, payload in timeframe_inputs.items()
        if isinstance(payload.get("candles"), list) and payload.get("candles")
    }
    structure = build_structure_section(
        candles_by_tf=candles_by_tf,
        market_data_section=market_data,
        profile=PHASE1_STRUCTURE_PROFILE_CANONICAL,
    )
    sr = build_sr_section(
        authoritative_view=authoritative_view,
        ladders=ladders,
        source_contract_version=(authoritative_view or {}).get("contract") if isinstance(authoritative_view, Mapping) else None,
    )
    fib, dynamic = _build_fib_and_dynamic_sections(
        symbol=symbol,
        candles_5m=timeframe_inputs.get("5m", {}).get("candles") or [],
        structure_section=structure,
        authoritative_view=authoritative_view,
        ladders=ladders,
    )
    packet = assemble_surveyor_packet(
        symbol=symbol,
        intended_direction_context=_pick_intended_direction(structure),
        build_mode="audit_ui",
        market_data=market_data,
        structure=structure,
        sr=sr,
        fib=fib,
        dynamic_levels=dynamic,
    )
    packet["meta"]["ui_source"] = "MarketArbiter/market_arbiter/web/app.py"
    packet["meta"]["packet_contract"] = "surveyor_packet_contract_v1"
    packet["meta"]["continuity_state"] = (runner_status or {}).get("continuity_state", "unknown")
    packet["bundle"] = build_surveyor_dataset_bundle(packet=packet, runner_status=runner_status)
    packet["diagnostics"] = dict(packet["bundle"].get("diagnostics") or {})
    return packet
