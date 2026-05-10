from __future__ import annotations

import argparse
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from market_arbiter.ops.surveyor_feed_runner import (
    DEFAULT_BACKFILL_PAGE_LIMIT,
    DEFAULT_CLOSE_LAG_MS,
    DEFAULT_LOOP_SLEEP_MS,
    DEFAULT_MAX_BACKFILL_BARS,
    FEED_WORKSET_MANIFEST_CONTRACT,
    _normalize_timeframes,
)


ONBOARDING_MANIFEST_CONTRACT = "surveyor_symbol_onboarding_manifest_v1"
ONBOARDING_RENDER_RESULT_CONTRACT = "surveyor_symbol_onboarding_render_result_v1"
ONBOARDING_STATE_CHANGE_CONTRACT = "surveyor_symbol_onboarding_state_change_v1"
ROLLOUT_REPORT_CONTRACT = "surveyor_rollout_report_v1"
ADVANCE_CHECK_CONTRACT = "surveyor_stage_advance_check_v1"
SOAK_EVIDENCE_CONTRACT = "surveyor_stage_soak_evidence_v1"
ACTIVE_STAGE_STATES = {"soaking", "promoted"}
ALLOWED_STAGE_STATES = {
    "proposed",
    "validated",
    "hydrating",
    "canary",
    "soaking",
    "promoted",
    "paused",
    "rejected",
}
DEFAULT_TIMEFRAMES = ["5m", "4h", "1d", "1w"]
DEFAULT_REPORT_PATH = "artifacts/feed_rollout/latest_report.json"
DEFAULT_ADVANCE_CHECK_PATH = "artifacts/feed_rollout/latest_advance_check.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class OnboardingSymbol:
    symbol: str
    enabled: bool
    stage_state: str
    cohort: str | None = None
    priority: int = 999
    shard_hint: str | None = None
    last_review_status: str | None = None
    notes: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def workset_symbol(self) -> str:
        return symbol_to_workset_symbol(self.symbol)

    @property
    def is_active(self) -> bool:
        return bool(self.enabled) and self.stage_state in ACTIVE_STAGE_STATES


@dataclass(frozen=True)
class OnboardingManifest:
    path: Path
    environment: str
    db_path: str
    workset_output_path: str
    control: Mapping[str, Any]
    provider_policy: Mapping[str, Any]
    stages: list[Mapping[str, Any]]
    symbols: list[OnboardingSymbol]
    timeframes: list[str]
    loop_sleep_ms: int
    close_lag_ms: int
    backfill_page_limit: int
    max_backfill_bars: int
    request_spacing_ms: int
    raw: Mapping[str, Any]

    @property
    def stage_ids(self) -> set[str]:
        return {str(stage.get("id") or "").strip() for stage in self.stages if str(stage.get("id") or "").strip()}

    @property
    def current_stage(self) -> str | None:
        value = self.control.get("stage") if isinstance(self.control, Mapping) else None
        normalized = str(value or "").strip()
        return normalized or None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"onboarding manifest not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid onboarding manifest JSON: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("onboarding manifest must be a JSON object")
    return payload


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp.replace(path)


def _repo_relative_path(manifest: OnboardingManifest, path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    if manifest.path.parent.name == "configs":
        return manifest.path.parent.parent / candidate
    return manifest.path.parent / candidate


def _bool_value(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _int_value(value: Any, *, default: int) -> int:
    if value is None or value == "":
        return default
    return int(value)


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        raise ValueError("symbol is required")
    return symbol


def symbol_to_workset_symbol(symbol: str) -> str:
    """Return the compact symbol shape currently accepted by surveyor_feed_runner.

    The onboarding manifest uses venue-style symbols such as BTC-USDT. The
    existing runner workset contract still uses compact symbols such as BTCUSDT.
    Keeping this conversion in one place avoids leaking venue-specific spelling
    into the generated active workset.
    """

    normalized = _clean_symbol(symbol).replace("/", "").replace("-", "")
    if not normalized.endswith("USDT"):
        raise ValueError(f"unsupported onboarding symbol for workset rendering: {symbol}")
    return normalized


def _parse_symbol(payload: Mapping[str, Any]) -> OnboardingSymbol:
    symbol = _clean_symbol(payload.get("symbol"))
    stage_state = str(payload.get("stage_state") or "proposed").strip().lower()
    if stage_state not in ALLOWED_STAGE_STATES:
        raise ValueError(f"unsupported stage_state for {symbol}: {stage_state}")
    return OnboardingSymbol(
        symbol=symbol,
        enabled=_bool_value(payload.get("enabled"), default=False),
        stage_state=stage_state,
        cohort=str(payload.get("cohort") or "").strip() or None,
        priority=_int_value(payload.get("priority"), default=999),
        shard_hint=str(payload.get("shard_hint") or "").strip() or None,
        last_review_status=str(payload.get("last_review_status") or "").strip() or None,
        notes=str(payload.get("notes") or "").strip() or None,
        raw=dict(payload),
    )


def load_onboarding_manifest(path: str | Path) -> OnboardingManifest:
    manifest_path = Path(path)
    payload = _read_json(manifest_path)
    contract = str(payload.get("contract") or "").strip()
    if contract != ONBOARDING_MANIFEST_CONTRACT:
        raise ValueError(f"unsupported onboarding manifest contract: {contract or 'missing'}")

    raw_symbols = payload.get("symbols") or []
    if not isinstance(raw_symbols, list):
        raise ValueError("onboarding manifest symbols must be a list")
    symbols = [_parse_symbol(row) for row in raw_symbols if isinstance(row, Mapping)]
    if len(symbols) != len(raw_symbols):
        raise ValueError("each onboarding manifest symbol entry must be an object")

    timeframes = _normalize_timeframes(payload.get("timeframes") or DEFAULT_TIMEFRAMES)
    return OnboardingManifest(
        path=manifest_path,
        environment=str(payload.get("environment") or "demo"),
        db_path=str(payload.get("db_path") or "data/market_arbiter.sqlite"),
        workset_output_path=str(payload.get("workset_output_path") or "configs/generated/surveyor_feed_workset.intraday.json"),
        control=dict(payload.get("control") or {}),
        provider_policy=dict(payload.get("provider_policy") or {}),
        stages=list(payload.get("stages") or []),
        symbols=symbols,
        timeframes=timeframes,
        loop_sleep_ms=_int_value(payload.get("loop_sleep_ms"), default=DEFAULT_LOOP_SLEEP_MS),
        close_lag_ms=_int_value(payload.get("close_lag_ms"), default=DEFAULT_CLOSE_LAG_MS),
        backfill_page_limit=_int_value(payload.get("backfill_page_limit"), default=DEFAULT_BACKFILL_PAGE_LIMIT),
        max_backfill_bars=_int_value(payload.get("max_backfill_bars"), default=DEFAULT_MAX_BACKFILL_BARS),
        request_spacing_ms=_int_value(payload.get("request_spacing_ms"), default=0),
        raw=payload,
    )


def validate_onboarding_manifest(manifest: OnboardingManifest) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []

    seen: dict[str, int] = {}
    for symbol in manifest.symbols:
        try:
            workset_symbol = symbol.workset_symbol
        except ValueError as exc:
            errors.append(str(exc))
            continue
        seen[workset_symbol] = seen.get(workset_symbol, 0) + 1

    duplicates = sorted(symbol for symbol, count in seen.items() if count > 1)
    for symbol in duplicates:
        errors.append(f"duplicate symbol after normalization: {symbol}")

    if manifest.current_stage and manifest.stage_ids and manifest.current_stage not in manifest.stage_ids:
        errors.append(f"control.stage is not declared in stages: {manifest.current_stage}")

    active_symbols = [symbol for symbol in manifest.symbols if symbol.is_active]
    if not active_symbols:
        warnings.append("no symbols are active; rendered workset will be empty")

    allow_prod = _bool_value(manifest.provider_policy.get("allow_prod"), default=False)
    if manifest.environment.lower() == "prod" and not allow_prod:
        errors.append("environment is prod but provider_policy.allow_prod is false")

    return {
        "ok": not errors,
        "errors": errors,
        "warnings": warnings,
        "summary": {
            "symbols_total": len(manifest.symbols),
            "symbols_active": len(active_symbols),
            "stage": manifest.current_stage,
            "timeframes": list(manifest.timeframes),
        },
    }


def render_active_workset(manifest: OnboardingManifest) -> dict[str, Any]:
    validation = validate_onboarding_manifest(manifest)
    if not validation["ok"]:
        raise ValueError("cannot render invalid onboarding manifest: " + "; ".join(validation["errors"]))

    active = sorted(
        (symbol for symbol in manifest.symbols if symbol.is_active),
        key=lambda row: (row.priority, row.workset_symbol),
    )
    symbols = [row.workset_symbol for row in active]
    workset: dict[str, Any] = {
        "contract": FEED_WORKSET_MANIFEST_CONTRACT,
        "db_path": manifest.db_path,
        "symbols": symbols,
        "timeframes": list(manifest.timeframes),
        "loop_sleep_ms": int(manifest.loop_sleep_ms),
        "close_lag_ms": int(manifest.close_lag_ms),
        "backfill_page_limit": int(manifest.backfill_page_limit),
        "max_backfill_bars": int(manifest.max_backfill_bars),
        "request_spacing_ms": int(manifest.request_spacing_ms),
        "onboarding_manifest_path": str(manifest.path),
        "onboarding_stage": manifest.current_stage,
        "source_symbols": [row.symbol for row in active],
        "shards": _render_shards(active),
    }
    source_routes = manifest.provider_policy.get("source_routes")
    if isinstance(source_routes, Mapping) and source_routes:
        workset["source_routes"] = dict(source_routes)
    provider_policy_path = manifest.provider_policy.get("provider_policy_path")
    if provider_policy_path:
        workset["provider_policy_path"] = str(provider_policy_path)
    return workset


def _render_shards(symbols: Sequence[OnboardingSymbol]) -> dict[str, list[str]]:
    shards: dict[str, list[str]] = {}
    for symbol in symbols:
        shard = symbol.shard_hint or "default"
        shards.setdefault(shard, []).append(symbol.workset_symbol)
    return {key: list(value) for key, value in sorted(shards.items())}


def _resolve_output_path(manifest: OnboardingManifest, output_path: str | Path | None = None) -> Path:
    return _repo_relative_path(manifest, output_path or manifest.workset_output_path)


def write_active_workset(manifest: OnboardingManifest, output_path: str | Path | None = None) -> dict[str, Any]:
    destination = _resolve_output_path(manifest, output_path)
    workset = render_active_workset(manifest)
    _write_json(destination, workset)
    return {
        "contract": ONBOARDING_RENDER_RESULT_CONTRACT,
        "manifest_path": str(manifest.path),
        "workset_output_path": str(destination),
        "active_symbols": list(workset["symbols"]),
        "active_symbol_count": len(workset["symbols"]),
        "stage": manifest.current_stage,
        "workset": workset,
    }


def _load_mutable_payload(path: str | Path) -> dict[str, Any]:
    manifest_path = Path(path)
    payload = _read_json(manifest_path)
    contract = str(payload.get("contract") or "").strip()
    if contract != ONBOARDING_MANIFEST_CONTRACT:
        raise ValueError(f"unsupported onboarding manifest contract: {contract or 'missing'}")
    return payload


def _find_symbol_entry(payload: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    wanted = symbol_to_workset_symbol(symbol)
    raw_symbols = payload.get("symbols") or []
    if not isinstance(raw_symbols, list):
        raise ValueError("onboarding manifest symbols must be a list")
    for row in raw_symbols:
        if not isinstance(row, dict):
            continue
        try:
            if symbol_to_workset_symbol(str(row.get("symbol") or "")) == wanted:
                return row
        except ValueError:
            continue
    raise ValueError(f"symbol not found in onboarding manifest: {symbol}")


def save_onboarding_payload(path: str | Path, payload: Mapping[str, Any]) -> OnboardingManifest:
    manifest_path = Path(path)
    _write_json(manifest_path, payload)
    return load_onboarding_manifest(manifest_path)


def admit_symbol(
    manifest_path: str | Path,
    *,
    symbol: str,
    stage: str | None = None,
    render: bool = True,
    now_fn: Any = _now_ms,
) -> dict[str, Any]:
    payload = _load_mutable_payload(manifest_path)
    manifest = load_onboarding_manifest(manifest_path)
    target_stage = str(stage or manifest.current_stage or "").strip()
    if target_stage and manifest.stage_ids and target_stage not in manifest.stage_ids:
        raise ValueError(f"stage is not declared in onboarding manifest: {target_stage}")
    row = _find_symbol_entry(payload, symbol)
    row["enabled"] = True
    row["stage_state"] = "soaking"
    if target_stage:
        row["admitted_stage"] = target_stage
    now_ms = int(now_fn())
    row["admitted_at_ms"] = now_ms
    row["last_state_change_ms"] = now_ms
    updated = save_onboarding_payload(manifest_path, payload)
    render_result = write_active_workset(updated) if render else None
    return {
        "contract": ONBOARDING_STATE_CHANGE_CONTRACT,
        "action": "admit",
        "manifest_path": str(manifest_path),
        "symbol": _clean_symbol(symbol),
        "stage": target_stage or None,
        "stage_state": "soaking",
        "enabled": True,
        "render_result": render_result,
    }


def pause_symbol(
    manifest_path: str | Path,
    *,
    symbol: str,
    reason: str,
    render: bool = True,
    now_fn: Any = _now_ms,
) -> dict[str, Any]:
    payload = _load_mutable_payload(manifest_path)
    row = _find_symbol_entry(payload, symbol)
    row["enabled"] = False
    row["stage_state"] = "paused"
    row["pause_reason"] = str(reason or "operator_pause").strip() or "operator_pause"
    row["last_state_change_ms"] = int(now_fn())
    updated = save_onboarding_payload(manifest_path, payload)
    render_result = write_active_workset(updated) if render else None
    return {
        "contract": ONBOARDING_STATE_CHANGE_CONTRACT,
        "action": "pause",
        "manifest_path": str(manifest_path),
        "symbol": _clean_symbol(symbol),
        "stage_state": "paused",
        "enabled": False,
        "reason": row["pause_reason"],
        "render_result": render_result,
    }


def build_rollout_report(manifest: OnboardingManifest, *, now_fn: Any = _now_ms) -> dict[str, Any]:
    validation = validate_onboarding_manifest(manifest)
    state_counts: dict[str, int] = {}
    for symbol in manifest.symbols:
        state_counts[symbol.stage_state] = state_counts.get(symbol.stage_state, 0) + 1
    active = [symbol for symbol in manifest.symbols if symbol.is_active]
    recommendation = "pause_required" if not validation["ok"] else "hold"
    notes = list(validation["warnings"])
    if validation["errors"]:
        notes.extend(validation["errors"])
    return {
        "contract": ROLLOUT_REPORT_CONTRACT,
        "as_of_ms": int(now_fn()),
        "manifest_path": str(manifest.path),
        "stage": manifest.current_stage,
        "recommendation": recommendation,
        "summary": {
            "symbols_total": len(manifest.symbols),
            "symbols_active": len(active),
            "symbols_green": sum(1 for symbol in active if symbol.last_review_status in {"pass", "green", "ok"}),
            "symbols_degraded": sum(1 for symbol in active if symbol.last_review_status in {"degraded", "conditional"}),
            "symbols_tripped": sum(1 for symbol in active if symbol.last_review_status in {"fail", "tripped"}),
            "symbols_paused": state_counts.get("paused", 0),
            "state_counts": state_counts,
        },
        "provider_safety": {
            "rest_429_events": 0,
            "rest_403_events": 0,
            "rapid_failure_cooldowns": 0,
            "source": "manifest_only_until_feed_events_are_wired",
        },
        "recompute": {
            "blocked_manifests": 0,
            "pending_manifests": 0,
            "source": "manifest_only_until_recompute_queue_is_wired",
        },
        "notes": notes,
    }


def write_rollout_report(
    manifest: OnboardingManifest,
    *,
    output_path: str | Path = DEFAULT_REPORT_PATH,
    now_fn: Any = _now_ms,
) -> dict[str, Any]:
    destination = _repo_relative_path(manifest, output_path)
    report = build_rollout_report(manifest, now_fn=now_fn)
    _write_json(destination, report)
    return {**report, "report_path": str(destination)}


def _current_stage_symbols(manifest: OnboardingManifest) -> list[OnboardingSymbol]:
    current_stage = manifest.current_stage
    if not current_stage:
        return []
    rows: list[OnboardingSymbol] = []
    for symbol in manifest.symbols:
        admitted_stage = str(symbol.raw.get("admitted_stage") or "").strip()
        if admitted_stage == current_stage:
            rows.append(symbol)
            continue
        if symbol.is_active:
            rows.append(symbol)
    return rows


def _symbol_admitted_at_ms(symbol: OnboardingSymbol) -> int | None:
    return _optional_int(symbol.raw.get("admitted_at_ms")) or _optional_int(symbol.raw.get("last_state_change_ms"))


def _unresolved_provider_freeze_events(manifest: OnboardingManifest) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    raw_sources = [
        manifest.provider_policy.get("provider_freeze_events"),
        manifest.provider_policy.get("freeze_events"),
        manifest.provider_policy.get("provider_incidents"),
    ]
    for raw in raw_sources:
        if not isinstance(raw, list):
            continue
        for row in raw:
            if not isinstance(row, Mapping):
                continue
            state = str(row.get("state") or row.get("status") or "open").strip().lower()
            resolved = bool(row.get("resolved")) or state in {"resolved", "closed", "cleared"}
            if not resolved:
                events.append(dict(row))
    return events


def build_soak_evidence(manifest: OnboardingManifest, *, now_fn: Any = _now_ms) -> dict[str, Any]:
    now_ms = int(now_fn())
    current_stage = manifest.current_stage
    stage = next((row for row in manifest.stages if str(row.get("id") or "") == str(current_stage or "")), None)
    min_soak_hours = int((stage or {}).get("min_soak_hours") or 0)
    min_soak_ms = min_soak_hours * 60 * 60 * 1000
    active_stage_symbols = [symbol for symbol in _current_stage_symbols(manifest) if symbol.is_active]

    active_rows: list[dict[str, Any]] = []
    elapsed_values: list[int] = []
    for symbol in active_stage_symbols:
        admitted_at_ms = _symbol_admitted_at_ms(symbol)
        elapsed_ms = max(0, now_ms - admitted_at_ms) if admitted_at_ms is not None else None
        if elapsed_ms is not None:
            elapsed_values.append(elapsed_ms)
        active_rows.append(
            {
                "symbol": symbol.symbol,
                "workset_symbol": symbol.workset_symbol,
                "stage_state": symbol.stage_state,
                "admitted_stage": symbol.raw.get("admitted_stage") or current_stage,
                "admitted_at_ms": admitted_at_ms,
                "elapsed_ms": elapsed_ms,
                "min_soak_elapsed": bool(elapsed_ms is not None and elapsed_ms >= min_soak_ms) if min_soak_ms else True,
                "last_review_status": symbol.last_review_status,
            }
        )

    stage_rows = _current_stage_symbols(manifest)
    paused_events = [
        {
            "symbol": symbol.symbol,
            "stage_state": symbol.stage_state,
            "reason": symbol.raw.get("pause_reason") or symbol.notes or "paused",
            "last_state_change_ms": _optional_int(symbol.raw.get("last_state_change_ms")),
        }
        for symbol in stage_rows
        if symbol.stage_state == "paused"
    ]
    blocked_events = [
        {
            "symbol": symbol.symbol,
            "stage_state": symbol.stage_state,
            "reason": symbol.raw.get("block_reason")
            or symbol.raw.get("blocked_reason")
            or symbol.raw.get("last_review_status")
            or "blocked",
            "last_state_change_ms": _optional_int(symbol.raw.get("last_state_change_ms")),
        }
        for symbol in stage_rows
        if symbol.stage_state == "rejected" or str(symbol.raw.get("last_review_status") or "").lower() in {"fail", "failed", "tripped", "blocked"}
    ]
    provider_freeze_events = _unresolved_provider_freeze_events(manifest)

    min_elapsed_ms = min(elapsed_values) if elapsed_values else None
    return {
        "contract": SOAK_EVIDENCE_CONTRACT,
        "as_of_ms": now_ms,
        "manifest_path": str(manifest.path),
        "stage": current_stage,
        "stage_min_soak_hours": min_soak_hours,
        "stage_min_soak_ms": min_soak_ms,
        "active_stage_symbol_count": len(active_rows),
        "active_symbols": active_rows,
        "min_active_symbol_elapsed_ms": min_elapsed_ms,
        "minimum_soak_elapsed": bool(min_elapsed_ms is not None and min_elapsed_ms >= min_soak_ms) if min_soak_ms else True,
        "paused_events": paused_events,
        "blocked_events": blocked_events,
        "provider_freeze_events": provider_freeze_events,
        "auto_widening_enabled": False,
        "advance_mode": "manual_review_only",
    }


def build_advance_check(manifest: OnboardingManifest, *, now_fn: Any = _now_ms) -> dict[str, Any]:
    now_ms = int(now_fn())
    report = build_rollout_report(manifest, now_fn=lambda: now_ms)
    soak_evidence = build_soak_evidence(manifest, now_fn=lambda: now_ms)
    blockers: list[str] = []
    pause_required = False
    validation = validate_onboarding_manifest(manifest)
    if not validation["ok"]:
        blockers.extend(validation["errors"])
        pause_required = True
    active_count = int(report["summary"]["symbols_active"])
    current_stage = manifest.current_stage
    stage = next((row for row in manifest.stages if str(row.get("id") or "") == str(current_stage or "")), None)
    if stage:
        target_symbols = int(stage.get("target_symbols") or 0)
        if target_symbols and active_count > target_symbols:
            blockers.append(f"active symbol count {active_count} exceeds stage target {target_symbols}")
        min_soak_hours = int(stage.get("min_soak_hours") or 0)
        if min_soak_hours:
            if not soak_evidence["active_symbols"]:
                blockers.append(f"soak has not started for stage {current_stage}; requires {min_soak_hours}h minimum")
            elif soak_evidence["min_active_symbol_elapsed_ms"] is None:
                blockers.append(f"soak admission timestamps missing for stage {current_stage}; requires {min_soak_hours}h minimum")
            elif not soak_evidence["minimum_soak_elapsed"]:
                elapsed_ms = int(soak_evidence["min_active_symbol_elapsed_ms"] or 0)
                elapsed_hours = elapsed_ms / (60 * 60 * 1000)
                blockers.append(f"minimum soak not elapsed: {elapsed_hours:.2f}h/{min_soak_hours}h")
    else:
        blockers.append("current stage is not declared")
        pause_required = True
    if soak_evidence["paused_events"]:
        symbols = ", ".join(row["symbol"] for row in soak_evidence["paused_events"])
        blockers.append(f"paused symbols in current stage: {symbols}")
        pause_required = True
    if soak_evidence["blocked_events"]:
        symbols = ", ".join(row["symbol"] for row in soak_evidence["blocked_events"])
        blockers.append(f"blocked symbols in current stage: {symbols}")
        pause_required = True
    if soak_evidence["provider_freeze_events"]:
        providers = ", ".join(str(row.get("provider_id") or row.get("provider") or "unknown") for row in soak_evidence["provider_freeze_events"])
        blockers.append(f"provider freeze events open: {providers}")
        pause_required = True
    recommendation = "pause_required" if pause_required else ("ready_for_review" if not blockers else "hold")
    return {
        "contract": ADVANCE_CHECK_CONTRACT,
        "as_of_ms": now_ms,
        "manifest_path": str(manifest.path),
        "stage": current_stage,
        "recommendation": recommendation,
        "blockers": blockers,
        "report_summary": report["summary"],
        "soak_evidence": soak_evidence,
        "auto_widening_enabled": False,
        "advance_mode": "manual_review_only",
    }


def write_advance_check(
    manifest: OnboardingManifest,
    *,
    output_path: str | Path = DEFAULT_ADVANCE_CHECK_PATH,
    now_fn: Any = _now_ms,
) -> dict[str, Any]:
    destination = _repo_relative_path(manifest, output_path)
    check = build_advance_check(manifest, now_fn=now_fn)
    _write_json(destination, check)
    return {**check, "advance_check_path": str(destination)}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Surveyor symbol onboarding control-plane utility.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="Validate onboarding manifest shape and rollout safety basics.")
    validate.add_argument("--manifest", required=True)

    render = subparsers.add_parser("render-workset", aliases=["render"], help="Render active feed workset from onboarding state.")
    render.add_argument("--manifest", required=True)
    render.add_argument("--output", default=None)

    admit = subparsers.add_parser("admit", help="Admit a symbol into the current stage cohort and re-render workset.")
    admit.add_argument("--manifest", required=True)
    admit.add_argument("--symbol", required=True)
    admit.add_argument("--stage", default=None)
    admit.add_argument("--no-render", action="store_true")

    pause = subparsers.add_parser("pause", help="Pause a symbol and re-render workset without it.")
    pause.add_argument("--manifest", required=True)
    pause.add_argument("--symbol", required=True)
    pause.add_argument("--reason", required=True)
    pause.add_argument("--no-render", action="store_true")

    report = subparsers.add_parser("report", help="Write local rollout report artifact from manifest state.")
    report.add_argument("--manifest", required=True)
    report.add_argument("--output", default=DEFAULT_REPORT_PATH)

    advance = subparsers.add_parser("advance-check", help="Write local stage-advance recommendation artifact.")
    advance.add_argument("--manifest", required=True)
    advance.add_argument("--output", default=DEFAULT_ADVANCE_CHECK_PATH)
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    manifest = load_onboarding_manifest(args.manifest)

    if args.command == "validate":
        payload = {
            "contract": "surveyor_symbol_onboarding_validation_v1",
            "manifest_path": str(manifest.path),
            **validate_onboarding_manifest(manifest),
        }
    elif args.command in {"render-workset", "render"}:
        payload = write_active_workset(manifest, output_path=args.output)
    elif args.command == "admit":
        payload = admit_symbol(args.manifest, symbol=args.symbol, stage=args.stage, render=not args.no_render)
    elif args.command == "pause":
        payload = pause_symbol(args.manifest, symbol=args.symbol, reason=args.reason, render=not args.no_render)
    elif args.command == "report":
        payload = write_rollout_report(manifest, output_path=args.output)
    elif args.command == "advance-check":
        payload = write_advance_check(manifest, output_path=args.output)
    else:  # pragma: no cover - argparse prevents this.
        raise ValueError(f"unsupported command: {args.command}")

    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
