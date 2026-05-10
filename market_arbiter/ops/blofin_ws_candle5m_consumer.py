from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import random
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Sequence

from market_arbiter.core.db import init_db
from market_arbiter.feed import (
    BLOFIN_WS_PROVIDER_ID,
    BlofinEnvironment,
    BlofinFirewallBanError,
    BlofinGapRecoveryEngine,
    BlofinPublicRestClient,
    BlofinRecoveryBlockedError,
    BlofinPublicWsClient,
    BlofinRateLimitError,
    BlofinWsCandle5mIngestor,
)


BLOFIN_WS_CONSUMER_STATUS_CONTRACT = "blofin_ws_candle5m_consumer_status_v1"
BLOFIN_WS_CONSUMER_STATE_CONTRACT = "blofin_ws_candle5m_consumer_state_v1"
BLOFIN_WS_CONFIRM_CANARY_CONTRACT = "blofin_ws_confirm_canary_result_v1"
SUPERVISED_BLOFIN_WS_CONSUMER_CONTRACT = "supervised_blofin_ws_candle5m_consumer_v1"
DEFAULT_STATUS_STALE_AFTER_MS = 60 * 1000
DEFAULT_CONSUMER_HEARTBEAT_SECONDS = 15.0
DEFAULT_SYMBOLS = ["BTC-USDT"]
DEFAULT_DISCONNECT_RING_SIZE = 20


def _now_ms() -> int:
    return int(time.time() * 1000)


def _symbols_from_csv(value: str) -> list[str]:
    return [item.strip().upper() for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class BlofinWsCandle5mConsumerConfig:
    db_path: str
    symbols: list[str]
    environment: str = BlofinEnvironment.DEMO.value
    requests_per_minute: int = 120
    state_path: str | None = None


@dataclass(frozen=True)
class BlofinWsReconnectPolicy:
    base_delay_seconds: float = 15.0
    max_delay_seconds: float = 300.0
    multiplier: float = 2.0
    jitter_ratio: float = 0.1
    stable_connection_window_seconds: float = 180.0
    rapid_failure_window_seconds: float = 600.0
    rapid_failure_threshold: int = 3
    rapid_failure_cooldown_seconds: float = 900.0
    rate_limit_delay_seconds: float = 60.0
    firewall_ban_delay_seconds: float = 300.0


@dataclass
class BlofinWsReconnectState:
    consecutive_failures: int = 0
    recent_failure_ts_ms: list[int] = field(default_factory=list)

    def note_failure(
        self,
        *,
        now_ms: int,
        connected_started_ms: int | None,
        policy: BlofinWsReconnectPolicy,
        exc: Exception,
        random_fn: Callable[[float, float], float],
    ) -> float:
        stable_ms = int(max(0.0, policy.stable_connection_window_seconds) * 1000)
        if connected_started_ms is not None and (int(now_ms) - int(connected_started_ms)) >= stable_ms:
            self.consecutive_failures = 1
            self.recent_failure_ts_ms = []
        else:
            self.consecutive_failures += 1

        cutoff_ms = int(now_ms) - int(max(0.0, policy.rapid_failure_window_seconds) * 1000)
        self.recent_failure_ts_ms = [ts for ts in self.recent_failure_ts_ms if ts >= cutoff_ms]
        self.recent_failure_ts_ms.append(int(now_ms))

        exponent = max(0, self.consecutive_failures - 1)
        delay_seconds = min(
            max(policy.base_delay_seconds, 0.0) * (policy.multiplier ** exponent),
            max(policy.max_delay_seconds, 0.0),
        )
        if len(self.recent_failure_ts_ms) >= max(1, policy.rapid_failure_threshold):
            delay_seconds = max(delay_seconds, max(policy.rapid_failure_cooldown_seconds, 0.0))
        if isinstance(exc, BlofinRateLimitError):
            delay_seconds = max(delay_seconds, max(policy.rate_limit_delay_seconds, 0.0))
        if isinstance(exc, BlofinFirewallBanError):
            delay_seconds = max(delay_seconds, max(policy.firewall_ban_delay_seconds, 0.0))

        if delay_seconds > 0 and policy.jitter_ratio > 0:
            jitter_window = delay_seconds * policy.jitter_ratio
            lower = max(policy.base_delay_seconds, delay_seconds - jitter_window)
            upper = delay_seconds + jitter_window
            delay_seconds = random_fn(lower, upper)
        return float(delay_seconds)


def _reason_code_for_exception(exc: Exception) -> str:
    if isinstance(exc, BlofinRecoveryBlockedError):
        return "REST_RECOVERY_BLOCKED"
    if isinstance(exc, BlofinFirewallBanError):
        return "REST_FIREWALL_BAN"
    if isinstance(exc, BlofinRateLimitError):
        return "REST_RATE_LIMIT"
    return "WS_DISCONNECTED"


def _state_path_for_db(db_path: str, state_path: str | None = None) -> Path:
    if state_path:
        return Path(state_path)
    db_file = Path(db_path)
    return db_file.with_name(f"{db_file.stem}.blofin_ws_candle5m_consumer_state.json")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _payload_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    arg = payload.get("arg") if isinstance(payload.get("arg"), Mapping) else {}
    data = payload.get("data")
    if isinstance(data, list):
        data_count = len(data)
    elif isinstance(data, Mapping):
        data_count = 1
    else:
        data_count = 0
    return {
        "event": payload.get("event"),
        "channel": arg.get("channel") or payload.get("channel"),
        "instId": arg.get("instId") or payload.get("instId"),
        "has_data": data_count > 0,
        "data_count": data_count,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp.replace(path)


def _bounded_events(existing: Sequence[Mapping[str, Any]] | None, event: Mapping[str, Any], *, limit: int = DEFAULT_DISCONNECT_RING_SIZE) -> list[dict[str, Any]]:
    rows = [dict(row) for row in (existing or []) if isinstance(row, Mapping)]
    rows.append(dict(event))
    return rows[-max(1, int(limit)):]


def _ws_diagnostics(ws_client: Any | None) -> dict[str, Any]:
    if ws_client is None:
        return {}
    diagnostic_state = getattr(ws_client, "diagnostic_state", None)
    if callable(diagnostic_state):
        try:
            payload = diagnostic_state()
            return dict(payload) if isinstance(payload, Mapping) else {"raw": str(payload)}
        except Exception as exc:  # pragma: no cover - diagnostics must never mask the primary error
            return {"diagnostic_error_type": exc.__class__.__name__, "diagnostic_error": str(exc)}
    return {}


def _write_terminal_state(
    config: BlofinWsCandle5mConsumerConfig,
    *,
    state: str,
    reason: str,
    now_ms: int | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    prior = _read_json(_state_path_for_db(config.db_path, config.state_path)) or {}
    return _write_consumer_state(
        config,
        now_ms=int(now_ms if now_ms is not None else _now_ms()),
        state=state,
        reconnect_attempts=int(prior.get("reconnect_attempts") or 0),
        messages_seen=int(prior.get("messages_seen") or 0),
        latest_disconnect_reason=prior.get("latest_disconnect_reason"),
        latest_backoff_seconds=prior.get("latest_backoff_seconds"),
        last_successful_ingest_ms=prior.get("last_successful_ingest_ms"),
        last_recovery_ms=prior.get("last_recovery_ms"),
        extra={
            "terminal_reason": reason,
            "data_messages": int(prior.get("data_messages") or 0),
            "control_messages": int(prior.get("control_messages") or 0),
            "received_rows": int(prior.get("received_rows") or 0),
            "unconfirmed_rows": int(prior.get("unconfirmed_rows") or 0),
            "confirmed_messages": int(prior.get("confirmed_messages") or 0),
            "confirmed_inserted": int(prior.get("confirmed_inserted") or 0),
            "last_payload_summary": prior.get("last_payload_summary"),
            "disconnect_events_recent": prior.get("disconnect_events_recent") or [],
            **dict(extra or {}),
        },
    )


def _install_signal_terminal_state_writer(config: BlofinWsCandle5mConsumerConfig) -> None:
    def _handler(signum, _frame):
        signal_name = signal.Signals(signum).name
        _write_terminal_state(
            config,
            state="terminated",
            reason="signal",
            extra={"signal": signal_name, "signal_number": int(signum)},
        )
        raise SystemExit(128 + int(signum))

    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, _handler)


def _checkpoint_row(conn, *, symbol: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id
        FROM feed_checkpoints
        WHERE provider_id = ? AND venue = ? AND symbol = ? AND timeframe = ?;
        """,
        (BLOFIN_WS_PROVIDER_ID, "blofin", symbol, "5m"),
    ).fetchone()
    if not row:
        return None
    return {
        "last_ts_open_ms": row[0],
        "last_success_ms": row[1],
        "last_attempt_ms": row[2],
        "failure_count": row[3],
        "state": row[4],
        "last_reason_code": row[5],
        "trace_id": row[6],
    }


def _write_consumer_state(
    config: BlofinWsCandle5mConsumerConfig,
    *,
    now_ms: int,
    state: str,
    reconnect_attempts: int,
    messages_seen: int,
    latest_disconnect_reason: str | None = None,
    latest_backoff_seconds: float | None = None,
    last_successful_ingest_ms: int | None = None,
    last_recovery_ms: int | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state_path = _state_path_for_db(config.db_path, config.state_path)
    existing = _read_json(state_path) or {}
    payload: dict[str, Any] = {
        "contract": BLOFIN_WS_CONSUMER_STATE_CONTRACT,
        "db_path": config.db_path,
        "state_path": str(state_path),
        "environment": config.environment,
        "symbols": list(config.symbols),
        "timeframe": "5m",
        "state": state,
        "updated_ms": int(now_ms),
        "reconnect_attempts": int(reconnect_attempts),
        "messages_seen": int(messages_seen),
        "latest_disconnect_reason": latest_disconnect_reason if latest_disconnect_reason is not None else existing.get("latest_disconnect_reason"),
        "latest_backoff_seconds": latest_backoff_seconds if latest_backoff_seconds is not None else existing.get("latest_backoff_seconds"),
        "last_successful_ingest_ms": (
            int(last_successful_ingest_ms)
            if last_successful_ingest_ms is not None
            else existing.get("last_successful_ingest_ms")
        ),
        "last_recovery_ms": int(last_recovery_ms) if last_recovery_ms is not None else existing.get("last_recovery_ms"),
    }
    for preserved_key in (
        "latest_exception_type",
        "latest_exception_message",
        "latest_disconnect_event",
        "disconnect_events_recent",
        "terminal_reason",
        "supervisor_last_outcome",
        "supervisor_recent_outcomes",
    ):
        if preserved_key in existing:
            payload[preserved_key] = existing[preserved_key]
    if extra:
        payload.update(extra)
    _write_json(state_path, payload)
    return payload


def _reported_feed_state(raw_state: str, status_source: str) -> str:
    if status_source == "stale_state" and raw_state in {"starting", "recovering", "connecting", "live", "backing_off"}:
        return "stale"
    return raw_state


def load_last_blofin_5m_checkpoint(conn, *, symbol: str) -> int | None:
    row = conn.execute(
        """
        SELECT MAX(last_ts_open_ms)
        FROM feed_checkpoints
        WHERE venue = ? AND symbol = ? AND timeframe = ? AND state = ?;
        """,
        ("blofin", symbol, "5m", "ok"),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def mark_symbols_repair_pending(
    conn,
    *,
    symbols: Sequence[str],
    now_ms: int,
    trace_id: str,
    reason_code: str = "WS_DISCONNECTED",
    metadata: Mapping[str, Any] | None = None,
) -> None:
    with conn:
        for symbol in symbols:
            last_ts_open_ms = load_last_blofin_5m_checkpoint(conn, symbol=symbol)
            conn.execute(
                """
                INSERT INTO feed_checkpoints(
                    provider_id, venue, symbol, timeframe,
                    last_ts_open_ms, last_success_ms, last_attempt_ms,
                    failure_count, state, last_reason_code, trace_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider_id, venue, symbol, timeframe)
                DO UPDATE SET
                    last_ts_open_ms=excluded.last_ts_open_ms,
                    last_attempt_ms=excluded.last_attempt_ms,
                    failure_count=feed_checkpoints.failure_count + 1,
                    state=excluded.state,
                    last_reason_code=excluded.last_reason_code,
                    trace_id=excluded.trace_id;
                """,
                (
                    BLOFIN_WS_PROVIDER_ID,
                    "blofin",
                    symbol,
                    "5m",
                    last_ts_open_ms,
                    None,
                    int(now_ms),
                    1,
                    "repair_pending",
                    reason_code,
                    trace_id,
                ),
            )
            conn.execute(
                """
                INSERT INTO feed_health_events(provider_id, venue, symbol, timeframe, state, reason_codes_json, as_of_ms, trace_id, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    BLOFIN_WS_PROVIDER_ID,
                    "blofin",
                    symbol,
                    "5m",
                    "repair_pending",
                    json.dumps([reason_code], sort_keys=True),
                    int(now_ms),
                    trace_id,
                    json.dumps(dict(metadata), sort_keys=True) if metadata else None,
                ),
            )


async def run_live(
    config: BlofinWsCandle5mConsumerConfig,
    *,
    rest_client: BlofinPublicRestClient | None = None,
    ws_client: BlofinPublicWsClient | None = None,
    rest_client_factory: Callable[[], BlofinPublicRestClient] | None = None,
    ws_client_factory: Callable[[], BlofinPublicWsClient] | None = None,
    now_fn: Callable[[], int] = _now_ms,
    sleep_fn: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    random_fn: Callable[[float, float], float] = random.uniform,
    reconnect_policy: BlofinWsReconnectPolicy | None = None,
    max_messages: int | None = None,
    max_confirmed_messages: int | None = None,
    max_reconnects: int | None = None,
    heartbeat_timeout_seconds: float = DEFAULT_CONSUMER_HEARTBEAT_SECONDS,
) -> dict[str, Any]:
    environment = BlofinEnvironment(config.environment)
    conn = init_db(config.db_path)
    resolved_rest = rest_client or (rest_client_factory or (lambda: BlofinPublicRestClient(environment=environment, requests_per_minute=config.requests_per_minute)))()
    ingestor = BlofinWsCandle5mIngestor(conn)
    policy = reconnect_policy or BlofinWsReconnectPolicy()
    reconnect_state = BlofinWsReconnectState()
    startup_recoveries: list[dict[str, Any]] = []
    recovery_runs: list[dict[str, Any]] = []
    ingests: list[dict[str, Any]] = []
    messages_seen = 0
    data_messages = 0
    control_messages = 0
    received_rows = 0
    unconfirmed_rows = 0
    confirmed_messages = 0
    confirmed_inserted = 0
    last_payload_summary: dict[str, Any] | None = None
    reconnect_attempts = 0
    backoff_delays_seconds: list[float] = []
    disconnect_events: list[dict[str, Any]] = []
    current_ws: BlofinPublicWsClient | None = None
    state_path = _state_path_for_db(config.db_path, config.state_path)

    try:
        _write_consumer_state(
            config,
            now_ms=int(now_fn()),
            state="starting",
            reconnect_attempts=reconnect_attempts,
            messages_seen=messages_seen,
        )
        while True:
            connected_started_ms: int | None = None
            try:
                recovery = BlofinGapRecoveryEngine(conn, resolved_rest)
                recovery_now_ms = int(now_fn())
                _write_consumer_state(
                    config,
                    now_ms=recovery_now_ms,
                    state="recovering",
                    reconnect_attempts=reconnect_attempts,
                    messages_seen=messages_seen,
                )
                recovery_results: list[dict[str, Any]] = []
                for symbol in config.symbols:
                    trace_id = f"blofin-ws-startup:{symbol}:{recovery_now_ms}"
                    result = recovery.recover_symbol(
                        symbol=symbol,
                        now_ms=recovery_now_ms,
                        trace_id=trace_id,
                        last_closed_ts_open_ms=load_last_blofin_5m_checkpoint(conn, symbol=symbol),
                    )
                    recovery_results.append(result)
                    if str(result.get("status") or "ok") != "ok":
                        raise BlofinRecoveryBlockedError(result)
                if not startup_recoveries:
                    startup_recoveries = list(recovery_results)
                recovery_runs.append(
                    {
                        "attempt": reconnect_attempts + 1,
                        "as_of_ms": recovery_now_ms,
                        "results": recovery_results,
                    }
                )
                _write_consumer_state(
                    config,
                    now_ms=recovery_now_ms,
                    state="connecting",
                    reconnect_attempts=reconnect_attempts,
                    messages_seen=messages_seen,
                    last_recovery_ms=recovery_now_ms,
                )
                current_ws = ws_client or (ws_client_factory or (lambda: BlofinPublicWsClient(environment=environment)))()
                await current_ws.connect()
                connected_started_ms = int(now_fn())
                await current_ws.subscribe_candle_5m(config.symbols)
                _write_consumer_state(
                    config,
                    now_ms=connected_started_ms,
                    state="live",
                    reconnect_attempts=reconnect_attempts,
                    messages_seen=messages_seen,
                    last_recovery_ms=recovery_now_ms,
                )

                while True:
                    if max_messages is not None and messages_seen >= max_messages:
                        break
                    if max_confirmed_messages is not None and confirmed_messages >= max_confirmed_messages:
                        break
                    try:
                        payload = await asyncio.wait_for(current_ws.recv(), timeout=max(0.1, heartbeat_timeout_seconds))
                    except asyncio.TimeoutError:
                        heartbeat_now_ms = int(now_fn())
                        _write_consumer_state(
                            config,
                            now_ms=heartbeat_now_ms,
                            state="live",
                            reconnect_attempts=reconnect_attempts,
                            messages_seen=messages_seen,
                            last_recovery_ms=recovery_now_ms,
                        )
                        continue
                    messages_seen += 1
                    received_ts_ms = int(now_fn())
                    trace_id = f"blofin-ws-live:message-{messages_seen}:{received_ts_ms}"
                    last_payload_summary = _payload_summary(payload)
                    if last_payload_summary["has_data"]:
                        data_messages += 1
                    else:
                        control_messages += 1
                    _write_consumer_state(
                        config,
                        now_ms=received_ts_ms,
                        state="live",
                        reconnect_attempts=reconnect_attempts,
                        messages_seen=messages_seen,
                        last_recovery_ms=recovery_now_ms,
                        extra={
                            "data_messages": data_messages,
                            "control_messages": control_messages,
                            "received_rows": received_rows,
                            "unconfirmed_rows": unconfirmed_rows,
                            "confirmed_messages": confirmed_messages,
                            "confirmed_inserted": confirmed_inserted,
                            "last_payload_summary": last_payload_summary,
                        },
                    )
                    ingest_result = ingestor.ingest_payload(payload, received_ts_ms=received_ts_ms, trace_id=trace_id)
                    ingests.append(ingest_result)
                    payload_rows = int(ingest_result.get("received") or 0)
                    payload_confirmed = int(ingest_result.get("confirmed") or 0)
                    received_rows += payload_rows
                    unconfirmed_rows += max(0, payload_rows - payload_confirmed)
                    confirmed_messages += payload_confirmed
                    confirmed_inserted += int(ingest_result.get("inserted") or 0)
                    _write_consumer_state(
                        config,
                        now_ms=received_ts_ms,
                        state="live",
                        reconnect_attempts=reconnect_attempts,
                        messages_seen=messages_seen,
                        last_successful_ingest_ms=received_ts_ms,
                        last_recovery_ms=recovery_now_ms,
                        extra={
                            "data_messages": data_messages,
                            "control_messages": control_messages,
                            "received_rows": received_rows,
                            "unconfirmed_rows": unconfirmed_rows,
                            "confirmed_messages": confirmed_messages,
                            "confirmed_inserted": confirmed_inserted,
                            "last_payload_summary": last_payload_summary,
                        },
                    )

                reconnect_state = BlofinWsReconnectState()
                _write_consumer_state(
                    config,
                    now_ms=int(now_fn()),
                    state="stopped",
                    reconnect_attempts=reconnect_attempts,
                    messages_seen=messages_seen,
                    extra={
                        "data_messages": data_messages,
                        "control_messages": control_messages,
                        "received_rows": received_rows,
                        "unconfirmed_rows": unconfirmed_rows,
                        "confirmed_messages": confirmed_messages,
                        "confirmed_inserted": confirmed_inserted,
                        "last_payload_summary": last_payload_summary,
                    },
                )
                return {
                    "contract": BLOFIN_WS_CONSUMER_STATUS_CONTRACT,
                    "environment": environment.value,
                    "symbols": list(config.symbols),
                    "state_path": str(state_path),
                    "startup_recoveries": startup_recoveries,
                    "recovery_runs": recovery_runs,
                    "messages_seen": messages_seen,
                    "data_messages": data_messages,
                    "control_messages": control_messages,
                    "received_rows": received_rows,
                    "unconfirmed_rows": unconfirmed_rows,
                    "confirmed_messages": confirmed_messages,
                    "confirmed_inserted": confirmed_inserted,
                    "reconnect_attempts": reconnect_attempts,
                    "backoff_delays_seconds": backoff_delays_seconds,
                    "disconnect_events": disconnect_events,
                    "ingests": ingests,
                }
            except asyncio.CancelledError:
                now_ms = int(now_fn())
                _write_consumer_state(
                    config,
                    now_ms=now_ms,
                    state="interrupted",
                    reconnect_attempts=reconnect_attempts,
                    messages_seen=messages_seen,
                    extra={
                        "data_messages": data_messages,
                        "control_messages": control_messages,
                        "received_rows": received_rows,
                        "unconfirmed_rows": unconfirmed_rows,
                        "confirmed_messages": confirmed_messages,
                        "confirmed_inserted": confirmed_inserted,
                        "last_payload_summary": last_payload_summary,
                        "latest_exception_type": "CancelledError",
                    },
                )
                raise
            except Exception as exc:
                now_ms = int(now_fn())
                reconnect_attempts += 1
                reason_code = _reason_code_for_exception(exc)
                delay_seconds = reconnect_state.note_failure(
                    now_ms=now_ms,
                    connected_started_ms=connected_started_ms,
                    policy=policy,
                    exc=exc,
                    random_fn=random_fn,
                )
                backoff_delays_seconds.append(delay_seconds)
                disconnect_event = {
                    "attempt": reconnect_attempts,
                    "as_of_ms": now_ms,
                    "reason_code": reason_code,
                    "exception_type": exc.__class__.__name__,
                    "message": str(exc),
                    "backoff_delay_seconds": delay_seconds,
                    "connected_started_ms": connected_started_ms,
                    "messages_seen": messages_seen,
                    "data_messages": data_messages,
                    "confirmed_messages": confirmed_messages,
                    "last_payload_summary": last_payload_summary,
                    "ws_diagnostics": _ws_diagnostics(current_ws),
                }
                disconnect_events.append(disconnect_event)
                existing_state = _read_json(state_path) or {}
                recent_disconnect_events = _bounded_events(
                    existing_state.get("disconnect_events_recent") or [],
                    disconnect_event,
                )
                mark_symbols_repair_pending(
                    conn,
                    symbols=config.symbols,
                    now_ms=now_ms,
                    trace_id=f"blofin-ws-disconnect:{now_ms}",
                    reason_code=reason_code,
                    metadata={"disconnect_event": disconnect_event},
                )
                _write_consumer_state(
                    config,
                    now_ms=now_ms,
                    state="backing_off",
                    reconnect_attempts=reconnect_attempts,
                    messages_seen=messages_seen,
                    latest_disconnect_reason=reason_code,
                    latest_backoff_seconds=delay_seconds,
                    extra={
                        "data_messages": data_messages,
                        "control_messages": control_messages,
                        "received_rows": received_rows,
                        "unconfirmed_rows": unconfirmed_rows,
                        "confirmed_messages": confirmed_messages,
                        "confirmed_inserted": confirmed_inserted,
                        "last_payload_summary": last_payload_summary,
                        "latest_exception_type": exc.__class__.__name__,
                        "latest_exception_message": str(exc),
                        "latest_disconnect_event": disconnect_event,
                        "disconnect_events_recent": recent_disconnect_events,
                    },
                )
                if current_ws is not None:
                    await current_ws.close()
                    current_ws = None
                if max_reconnects is not None and reconnect_attempts > max_reconnects:
                    _write_consumer_state(
                        config,
                        now_ms=now_ms,
                        state="failed",
                        reconnect_attempts=reconnect_attempts,
                        messages_seen=messages_seen,
                        latest_disconnect_reason=reason_code,
                        latest_backoff_seconds=delay_seconds,
                        extra={
                            "data_messages": data_messages,
                            "control_messages": control_messages,
                            "received_rows": received_rows,
                            "unconfirmed_rows": unconfirmed_rows,
                            "confirmed_messages": confirmed_messages,
                            "confirmed_inserted": confirmed_inserted,
                            "last_payload_summary": last_payload_summary,
                            "latest_exception_type": exc.__class__.__name__,
                            "latest_exception_message": str(exc),
                            "latest_disconnect_event": disconnect_event,
                            "disconnect_events_recent": recent_disconnect_events,
                        },
                    )
                    raise
                await sleep_fn(delay_seconds)
    finally:
        if current_ws is not None:
            await current_ws.close()
        if rest_client is None:
            resolved_rest.close()
        conn.close()


async def run_confirm_canary(
    config: BlofinWsCandle5mConsumerConfig,
    *,
    rest_client: BlofinPublicRestClient | None = None,
    ws_client: BlofinPublicWsClient | None = None,
    rest_client_factory: Callable[[], BlofinPublicRestClient] | None = None,
    ws_client_factory: Callable[[], BlofinPublicWsClient] | None = None,
    now_fn: Callable[[], int] = _now_ms,
    sleep_fn: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    random_fn: Callable[[float, float], float] = random.uniform,
    reconnect_policy: BlofinWsReconnectPolicy | None = None,
    max_runtime_seconds: float = 390.0,
    required_confirmed_messages: int = 1,
    max_reconnects: int | None = None,
    heartbeat_timeout_seconds: float = DEFAULT_CONSUMER_HEARTBEAT_SECONDS,
) -> dict[str, Any]:
    started_ms = int(now_fn())
    try:
        result = await asyncio.wait_for(
            run_live(
                config,
                rest_client=rest_client,
                ws_client=ws_client,
                rest_client_factory=rest_client_factory,
                ws_client_factory=ws_client_factory,
                now_fn=now_fn,
                sleep_fn=sleep_fn,
                random_fn=random_fn,
                reconnect_policy=reconnect_policy,
                max_confirmed_messages=max(1, int(required_confirmed_messages)),
                max_reconnects=max_reconnects,
                heartbeat_timeout_seconds=heartbeat_timeout_seconds,
            ),
            timeout=max(0.1, float(max_runtime_seconds)),
        )
        return {
            "contract": BLOFIN_WS_CONFIRM_CANARY_CONTRACT,
            "status": "ok",
            "started_ms": started_ms,
            "finished_ms": int(now_fn()),
            "max_runtime_seconds": float(max_runtime_seconds),
            "required_confirmed_messages": max(1, int(required_confirmed_messages)),
            **result,
        }
    except asyncio.TimeoutError:
        now_ms = int(now_fn())
        prior_state = _read_json(_state_path_for_db(config.db_path, config.state_path)) or {}
        diagnostic_counts = {
            "messages_seen": int(prior_state.get("messages_seen") or 0),
            "reconnect_attempts": int(prior_state.get("reconnect_attempts") or 0),
            "data_messages": int(prior_state.get("data_messages") or 0),
            "control_messages": int(prior_state.get("control_messages") or 0),
            "received_rows": int(prior_state.get("received_rows") or 0),
            "unconfirmed_rows": int(prior_state.get("unconfirmed_rows") or 0),
            "confirmed_messages": int(prior_state.get("confirmed_messages") or 0),
            "confirmed_inserted": int(prior_state.get("confirmed_inserted") or 0),
            "last_payload_summary": prior_state.get("last_payload_summary"),
        }
        _write_consumer_state(
            config,
            now_ms=now_ms,
            state="canary_timeout",
            reconnect_attempts=diagnostic_counts["reconnect_attempts"],
            messages_seen=diagnostic_counts["messages_seen"],
            latest_disconnect_reason="WAITING_FOR_CONFIRMED_CANDLE",
            extra={"latest_exception_type": "TimeoutError", "canary_timeout_seconds": float(max_runtime_seconds), **diagnostic_counts},
        )
        return {
            "contract": BLOFIN_WS_CONFIRM_CANARY_CONTRACT,
            "status": "timeout",
            "started_ms": started_ms,
            "finished_ms": now_ms,
            "max_runtime_seconds": float(max_runtime_seconds),
            "required_confirmed_messages": max(1, int(required_confirmed_messages)),
            "reason": "WAITING_FOR_CONFIRMED_CANDLE",
            "state_path": str(_state_path_for_db(config.db_path, config.state_path)),
            **diagnostic_counts,
        }
    except Exception as exc:
        now_ms = int(now_fn())
        prior_state = _read_json(_state_path_for_db(config.db_path, config.state_path)) or {}
        diagnostic_counts = {
            "messages_seen": int(prior_state.get("messages_seen") or 0),
            "reconnect_attempts": int(prior_state.get("reconnect_attempts") or 0),
            "data_messages": int(prior_state.get("data_messages") or 0),
            "control_messages": int(prior_state.get("control_messages") or 0),
            "received_rows": int(prior_state.get("received_rows") or 0),
            "unconfirmed_rows": int(prior_state.get("unconfirmed_rows") or 0),
            "confirmed_messages": int(prior_state.get("confirmed_messages") or 0),
            "confirmed_inserted": int(prior_state.get("confirmed_inserted") or 0),
            "last_payload_summary": prior_state.get("last_payload_summary"),
        }
        _write_consumer_state(
            config,
            now_ms=now_ms,
            state="canary_failed",
            reconnect_attempts=diagnostic_counts["reconnect_attempts"],
            messages_seen=diagnostic_counts["messages_seen"],
            latest_disconnect_reason=_reason_code_for_exception(exc),
            extra={"latest_exception_type": exc.__class__.__name__, "canary_error": str(exc), **diagnostic_counts},
        )
        return {
            "contract": BLOFIN_WS_CONFIRM_CANARY_CONTRACT,
            "status": "error",
            "started_ms": started_ms,
            "finished_ms": now_ms,
            "max_runtime_seconds": float(max_runtime_seconds),
            "required_confirmed_messages": max(1, int(required_confirmed_messages)),
            "reason": _reason_code_for_exception(exc),
            "error_type": exc.__class__.__name__,
            "message": str(exc),
            "state_path": str(_state_path_for_db(config.db_path, config.state_path)),
            **diagnostic_counts,
        }


async def run_supervised(
    config: BlofinWsCandle5mConsumerConfig,
    *,
    restart_delay_seconds: float = 30.0,
    max_supervisor_runs: int | None = None,
    now_fn: Callable[[], int] = _now_ms,
    sleep_fn: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    max_reconnects: int | None = None,
    heartbeat_timeout_seconds: float = DEFAULT_CONSUMER_HEARTBEAT_SECONDS,
) -> dict[str, Any]:
    supervisor_started_ms = int(now_fn())
    run_index = 0
    outcomes: list[dict[str, Any]] = []
    while True:
        run_index += 1
        run_started_ms = int(now_fn())
        _write_terminal_state(
            config,
            state="supervisor_starting_child",
            reason="supervisor_child_start",
            now_ms=run_started_ms,
            extra={
                "supervisor_contract": SUPERVISED_BLOFIN_WS_CONSUMER_CONTRACT,
                "supervisor_started_ms": supervisor_started_ms,
                "supervisor_run_index": run_index,
            },
        )
        try:
            result = await run_live(
                config,
                now_fn=now_fn,
                sleep_fn=sleep_fn,
                max_reconnects=max_reconnects,
                heartbeat_timeout_seconds=heartbeat_timeout_seconds,
            )
            outcome = {
                "run_index": run_index,
                "status": "returned",
                "started_ms": run_started_ms,
                "finished_ms": int(now_fn()),
                "messages_seen": int(result.get("messages_seen") or 0),
                "confirmed_messages": int(result.get("confirmed_messages") or 0),
                "reconnect_attempts": int(result.get("reconnect_attempts") or 0),
            }
        except asyncio.CancelledError:
            _write_terminal_state(
                config,
                state="supervisor_interrupted",
                reason="CancelledError",
                extra={
                    "supervisor_contract": SUPERVISED_BLOFIN_WS_CONSUMER_CONTRACT,
                    "supervisor_run_index": run_index,
                    "latest_exception_type": "CancelledError",
                },
            )
            raise
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                _write_terminal_state(
                    config,
                    state="supervisor_terminated",
                    reason=exc.__class__.__name__,
                    extra={
                        "supervisor_contract": SUPERVISED_BLOFIN_WS_CONSUMER_CONTRACT,
                        "supervisor_run_index": run_index,
                        "latest_exception_type": exc.__class__.__name__,
                        "latest_exception_message": str(exc),
                    },
                )
                raise
            outcome = {
                "run_index": run_index,
                "status": "error",
                "started_ms": run_started_ms,
                "finished_ms": int(now_fn()),
                "exception_type": exc.__class__.__name__,
                "message": str(exc),
            }
        outcomes.append(outcome)
        recent_outcomes = outcomes[-10:]
        _write_terminal_state(
            config,
            state="supervisor_waiting_to_restart",
            reason=str(outcome.get("status") or "child_finished"),
            extra={
                "supervisor_contract": SUPERVISED_BLOFIN_WS_CONSUMER_CONTRACT,
                "supervisor_started_ms": supervisor_started_ms,
                "supervisor_run_index": run_index,
                "supervisor_last_outcome": outcome,
                "supervisor_recent_outcomes": recent_outcomes,
                "supervisor_restart_delay_seconds": float(restart_delay_seconds),
            },
        )
        if max_supervisor_runs is not None and run_index >= int(max_supervisor_runs):
            return {
                "contract": SUPERVISED_BLOFIN_WS_CONSUMER_CONTRACT,
                "status": "stopped_after_max_runs",
                "started_ms": supervisor_started_ms,
                "finished_ms": int(now_fn()),
                "runs": recent_outcomes,
            }
        await sleep_fn(max(0.0, float(restart_delay_seconds)))


def collect_status(
    config: BlofinWsCandle5mConsumerConfig,
    *,
    now_fn: Callable[[], int] = _now_ms,
    stale_after_ms: int = DEFAULT_STATUS_STALE_AFTER_MS,
) -> dict[str, Any]:
    now_ms = int(now_fn())
    state_path = _state_path_for_db(config.db_path, config.state_path)
    consumer_state = _read_json(state_path)
    if consumer_state is None:
        status_source = "no_state"
        state_age_ms = None
        feed_state = "unknown"
    else:
        updated_ms = consumer_state.get("updated_ms")
        state_age_ms = (now_ms - int(updated_ms)) if isinstance(updated_ms, int) else None
        status_source = "live_state" if state_age_ms is not None and state_age_ms <= int(stale_after_ms) else "stale_state"
        feed_state = _reported_feed_state(str(consumer_state.get("state") or "unknown"), status_source)

    conn = init_db(config.db_path)
    try:
        symbols: list[dict[str, Any]] = []
        for symbol in config.symbols:
            checkpoint = _checkpoint_row(conn, symbol=symbol)
            symbols.append(
                {
                    "symbol": symbol,
                    "timeframe": "5m",
                    "checkpoint_state": checkpoint.get("state") if checkpoint else "missing",
                    "last_reason_code": checkpoint.get("last_reason_code") if checkpoint else "NO_CHECKPOINT",
                    "failure_count": checkpoint.get("failure_count") if checkpoint else 0,
                    "last_ts_open_ms": checkpoint.get("last_ts_open_ms") if checkpoint else None,
                    "last_success_ms": checkpoint.get("last_success_ms") if checkpoint else None,
                    "last_attempt_ms": checkpoint.get("last_attempt_ms") if checkpoint else None,
                    "trace_id": checkpoint.get("trace_id") if checkpoint else None,
                }
            )
    finally:
        conn.close()

    summary = {
        "configured_symbols": len(symbols),
        "ok_symbols": sum(1 for row in symbols if row["checkpoint_state"] == "ok"),
        "issue_symbols": sum(1 for row in symbols if row["checkpoint_state"] not in {"ok", "missing"}),
        "missing_symbols": sum(1 for row in symbols if row["checkpoint_state"] == "missing"),
    }
    return {
        "contract": BLOFIN_WS_CONSUMER_STATUS_CONTRACT,
        "db_path": config.db_path,
        "state_path": str(state_path),
        "environment": config.environment,
        "symbols": list(config.symbols),
        "timeframe": "5m",
        "status_source": status_source,
        "state_age_ms": state_age_ms,
        "stale_after_ms": int(stale_after_ms),
        "feed_state": feed_state,
        "latest_disconnect_reason": (consumer_state or {}).get("latest_disconnect_reason"),
        "reconnect_attempts": int((consumer_state or {}).get("reconnect_attempts") or 0),
        "latest_backoff_seconds": (consumer_state or {}).get("latest_backoff_seconds"),
        "last_successful_ingest_ms": (consumer_state or {}).get("last_successful_ingest_ms"),
        "last_recovery_ms": (consumer_state or {}).get("last_recovery_ms"),
        "messages_seen": int((consumer_state or {}).get("messages_seen") or 0),
        "data_messages": int((consumer_state or {}).get("data_messages") or 0),
        "control_messages": int((consumer_state or {}).get("control_messages") or 0),
        "received_rows": int((consumer_state or {}).get("received_rows") or 0),
        "unconfirmed_rows": int((consumer_state or {}).get("unconfirmed_rows") or 0),
        "confirmed_messages": int((consumer_state or {}).get("confirmed_messages") or 0),
        "confirmed_inserted": int((consumer_state or {}).get("confirmed_inserted") or 0),
        "last_payload_summary": (consumer_state or {}).get("last_payload_summary"),
        "latest_exception_type": (consumer_state or {}).get("latest_exception_type"),
        "latest_exception_message": (consumer_state or {}).get("latest_exception_message"),
        "latest_disconnect_event": (consumer_state or {}).get("latest_disconnect_event"),
        "disconnect_events_recent": (consumer_state or {}).get("disconnect_events_recent") or [],
        "terminal_reason": (consumer_state or {}).get("terminal_reason"),
        "supervisor_last_outcome": (consumer_state or {}).get("supervisor_last_outcome"),
        "supervisor_recent_outcomes": (consumer_state or {}).get("supervisor_recent_outcomes") or [],
        "summary": summary,
        "consumer_state": consumer_state,
        "symbol_statuses": symbols,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the first BloFin public websocket candle5m live consumer.")
    parser.add_argument("--mode", choices=("run", "supervise", "status", "confirm-canary"), default="run")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--environment", choices=("demo", "prod"), default=BlofinEnvironment.DEMO.value)
    parser.add_argument("--requests-per-minute", type=int, default=120)
    parser.add_argument("--state-path", default=None)
    parser.add_argument("--max-runtime-seconds", type=float, default=390.0)
    parser.add_argument("--confirmed-target", type=int, default=1)
    parser.add_argument("--max-reconnects", type=int, default=None)
    parser.add_argument("--supervisor-restart-delay-seconds", type=float, default=30.0)
    parser.add_argument("--max-supervisor-runs", type=int, default=None)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    config = BlofinWsCandle5mConsumerConfig(
        db_path=args.db_path,
        symbols=_symbols_from_csv(args.symbols),
        environment=args.environment,
        requests_per_minute=int(args.requests_per_minute),
        state_path=args.state_path,
    )
    _install_signal_terminal_state_writer(config)
    if args.mode == "status":
        payload = collect_status(config)
    elif args.mode == "confirm-canary":
        payload = asyncio.run(
            run_confirm_canary(
                config,
                max_runtime_seconds=float(args.max_runtime_seconds),
                required_confirmed_messages=int(args.confirmed_target),
                max_reconnects=args.max_reconnects,
            )
        )
    elif args.mode == "supervise":
        payload = asyncio.run(
            run_supervised(
                config,
                restart_delay_seconds=float(args.supervisor_restart_delay_seconds),
                max_supervisor_runs=args.max_supervisor_runs,
                max_reconnects=args.max_reconnects,
            )
        )
    else:
        payload = asyncio.run(run_live(config))
    print(json.dumps({"config": asdict(config), **payload}, indent=2))


if __name__ == "__main__":
    main()
