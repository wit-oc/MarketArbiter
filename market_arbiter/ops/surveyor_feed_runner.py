from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from market_arbiter.core.db import init_db
from market_arbiter.core.market_data import CandleDTO, MarketDataProvider, upsert_market_candles
from market_arbiter.core.market_quality import TIMEFRAME_MS, aggregate_timeframe_candles
from market_arbiter.core.market_scheduler import MarketDataScheduler, SchedulerKey
from market_arbiter.feed.provider_policy import ProviderAccessGovernor, ProviderRolePolicy
from market_arbiter.feed.split_source_routing import FeedSourceRoute, RoutedMarketDataProvider, SplitSourceRoutingPlan


RUNNER_STATE_CONTRACT = "surveyor_feed_runner_state_v1"
RUNNER_STATUS_CONTRACT = "surveyor_feed_runner_status_v1"
FEED_WORKSET_MANIFEST_CONTRACT = "surveyor_feed_workset_manifest_v1"
FEED_CANARY_CONTRACT = "surveyor_feed_runner_canary_result_v1"
FEED_SHARD_STATUS_CONTRACT = "surveyor_feed_shard_status_v1"
DEFAULT_LOOP_SLEEP_MS = 15_000
DEFAULT_CLOSE_LAG_MS = 2_500
DEFAULT_BACKFILL_PAGE_LIMIT = 1_000
DEFAULT_MAX_BACKFILL_BARS = 2_000
DEFAULT_REQUEST_SPACING_MS = 0
_TIMEFRAME_ORDER = ["5m", "4h", "1d", "1w"]
_DERIVED_PROVIDER_ID = "local_aggregate"
_DERIVED_DATASET_PREFIX = "derived_candle_v1"
_SURVEYOR_TF_LABELS = {"5m": "5m", "4h": "4H", "1d": "1D", "1w": "1W"}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _market_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip()
    if "/" in raw:
        return raw.upper()
    normalized = raw.upper()
    if normalized.endswith("USDT"):
        return f"{normalized[:-4]}/USDT"
    raise ValueError(f"unsupported symbol format: {symbol}")


def _state_path_for_db(db_path: str, state_path: str | None = None) -> Path:
    if state_path:
        return Path(state_path)
    db_file = Path(db_path)
    return db_file.with_name(f"{db_file.stem}.surveyor_feed_runner_state.json")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp.replace(path)


def _normalize_symbols(symbols: Sequence[str]) -> list[str]:
    out: list[str] = []
    for symbol in symbols:
        value = str(symbol or "").strip().upper()
        if value:
            out.append(value)
    return list(dict.fromkeys(out))


def _normalize_timeframes(timeframes: Sequence[str]) -> list[str]:
    out: list[str] = []
    for timeframe in timeframes:
        value = str(timeframe or "").strip().lower()
        if not value:
            continue
        if value not in TIMEFRAME_MS:
            raise ValueError(f"unsupported timeframe: {timeframe}")
        out.append(value)
    ordered = [timeframe for timeframe in _TIMEFRAME_ORDER if timeframe in out]
    extras = [timeframe for timeframe in out if timeframe not in _TIMEFRAME_ORDER]
    return list(dict.fromkeys([*ordered, *extras]))


def _normalize_shards(symbols: Sequence[str], shards: Mapping[str, Sequence[str]] | None = None) -> dict[str, list[str]]:
    normalized_symbols = _normalize_symbols(symbols)
    known = set(normalized_symbols)
    if not shards:
        return {"default": normalized_symbols} if normalized_symbols else {}

    normalized_shards: dict[str, list[str]] = {}
    assigned: dict[str, str] = {}
    for raw_shard_id, raw_symbols in shards.items():
        shard_id = str(raw_shard_id or "").strip() or "default"
        shard_symbols = _normalize_symbols(raw_symbols or [])
        for symbol in shard_symbols:
            if symbol not in known:
                raise ValueError(f"shard {shard_id} references symbol outside workset: {symbol}")
            prior_shard = assigned.get(symbol)
            if prior_shard and prior_shard != shard_id:
                raise ValueError(f"symbol assigned to multiple shards: {symbol}")
            assigned[symbol] = shard_id
        if shard_symbols:
            normalized_shards.setdefault(shard_id, [])
            normalized_shards[shard_id].extend(shard_symbols)

    unassigned = [symbol for symbol in normalized_symbols if symbol not in assigned]
    if unassigned:
        normalized_shards.setdefault("default", [])
        normalized_shards["default"].extend(unassigned)

    return {shard_id: list(dict.fromkeys(values)) for shard_id, values in sorted(normalized_shards.items()) if values}


def _shard_for_symbol_map(symbols: Sequence[str], shards: Mapping[str, Sequence[str]] | None = None) -> dict[str, str]:
    normalized_shards = _normalize_shards(symbols, shards)
    return {symbol: shard_id for shard_id, shard_symbols in normalized_shards.items() for symbol in shard_symbols}


def build_feed_workset_manifest(config: "FeedRunnerConfig") -> dict[str, Any]:
    symbols = _normalize_symbols(config.symbols)
    manifest = {
        "contract": FEED_WORKSET_MANIFEST_CONTRACT,
        "db_path": config.db_path,
        "state_path": config.state_path,
        "symbols": symbols,
        "timeframes": _normalize_timeframes(config.timeframes),
        "shards": _normalize_shards(symbols, config.shards),
        "loop_sleep_ms": int(config.loop_sleep_ms),
        "close_lag_ms": int(config.close_lag_ms),
        "backfill_page_limit": int(config.backfill_page_limit),
        "max_backfill_bars": int(config.max_backfill_bars),
        "request_spacing_ms": int(config.request_spacing_ms),
        "manifest_path": config.manifest_path,
        "provider_policy_path": config.provider_policy_path,
    }
    if config.source_routes:
        manifest["source_routes"] = dict(config.source_routes)
    return manifest


def config_from_manifest(path: str, *, db_path: str | None = None, state_path: str | None = None) -> "FeedRunnerConfig":
    manifest_path = Path(path)
    payload = _read_json(manifest_path)
    if payload is None:
        raise ValueError(f"unable to read feed workset manifest: {path}")
    contract = str(payload.get("contract") or "")
    if contract != FEED_WORKSET_MANIFEST_CONTRACT:
        raise ValueError(f"unsupported feed workset manifest contract: {contract or 'missing'}")
    return FeedRunnerConfig(
        db_path=str(db_path or payload.get("db_path") or "data/market_arbiter.sqlite"),
        symbols=_normalize_symbols(payload.get("symbols") or []),
        timeframes=_normalize_timeframes(payload.get("timeframes") or []),
        shards=_normalize_shards(payload.get("symbols") or [], payload.get("shards") or None),
        loop_sleep_ms=int(payload.get("loop_sleep_ms") or DEFAULT_LOOP_SLEEP_MS),
        close_lag_ms=int(payload.get("close_lag_ms") or DEFAULT_CLOSE_LAG_MS),
        backfill_page_limit=int(payload.get("backfill_page_limit") or DEFAULT_BACKFILL_PAGE_LIMIT),
        max_backfill_bars=int(payload.get("max_backfill_bars") or DEFAULT_MAX_BACKFILL_BARS),
        request_spacing_ms=int(payload.get("request_spacing_ms") or DEFAULT_REQUEST_SPACING_MS),
        state_path=str(state_path or payload.get("state_path")) if (state_path or payload.get("state_path")) else None,
        manifest_path=str(manifest_path),
        provider_policy_path=str(payload.get("provider_policy_path")) if payload.get("provider_policy_path") else None,
        source_routes=dict(payload.get("source_routes") or {}) or None,
    )


def _trace_id(symbol: str, timeframe: str, *, run_label: str, cycle_index: int, now_ms: int) -> str:
    return f"surveyor-feed-runner:{run_label}:cycle-{cycle_index}:{symbol}:{timeframe}:{now_ms}"


def _symbol_aliases(symbol: str) -> list[str]:
    raw = str(symbol or "").strip()
    if not raw:
        return []
    aliases = [raw]
    try:
        aliases.append(_market_symbol(raw))
    except ValueError:
        pass
    normalized = raw.upper().replace("/", "")
    if normalized.endswith("USDT"):
        aliases.append(normalized)
    return list(dict.fromkeys(alias for alias in aliases if alias))


def _load_candles_for_window(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    timeframe: str,
    start_open_ms: int,
    end_open_ms: int,
    preferred_provider_id: str | None = None,
    venue: str = "okx",
) -> list[CandleDTO]:
    aliases = _symbol_aliases(symbol)
    placeholders = ",".join("?" for _ in aliases)
    rows = conn.execute(
        f"""
        SELECT provider_id, venue, symbol, timeframe, ts_open_ms, ts_close_ms,
               open, high, low, close, volume, dataset_version, trace_id
        FROM market_candles
        WHERE timeframe = ?
          AND symbol IN ({placeholders})
          AND ts_open_ms >= ?
          AND ts_open_ms < ?
          AND venue = ?
        ORDER BY ts_open_ms ASC,
                 CASE WHEN provider_id = ? THEN 0 ELSE 1 END,
                 ingest_ts_ms DESC;
        """,
        (timeframe, *aliases, int(start_open_ms), int(end_open_ms), venue, preferred_provider_id or ""),
    ).fetchall()

    by_open: dict[int, CandleDTO] = {}
    for row in rows:
        ts_open_ms = int(row[4])
        if ts_open_ms in by_open:
            continue
        by_open[ts_open_ms] = CandleDTO(
            provider_id=str(row[0]),
            venue=str(row[1]),
            symbol=str(row[2]),
            timeframe=str(row[3]),
            ts_open_ms=ts_open_ms,
            ts_close_ms=int(row[5]),
            open=str(row[6]),
            high=str(row[7]),
            low=str(row[8]),
            close=str(row[9]),
            volume=str(row[10]),
            dataset_version=str(row[11]),
            trace_id=str(row[12]),
        )
    return [by_open[key] for key in sorted(by_open)]


def _upsert_feed_checkpoint(
    conn: sqlite3.Connection,
    *,
    provider_id: str,
    venue: str,
    symbol: str,
    timeframe: str,
    last_ts_open_ms: int,
    now_ms: int,
    reason_code: str | None,
    trace_id: str,
) -> None:
    conn.execute(
        """
        INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider_id, venue, symbol, timeframe)
        DO UPDATE SET
            last_ts_open_ms=excluded.last_ts_open_ms,
            last_success_ms=excluded.last_success_ms,
            last_attempt_ms=excluded.last_attempt_ms,
            failure_count=excluded.failure_count,
            state=excluded.state,
            last_reason_code=excluded.last_reason_code,
            trace_id=excluded.trace_id;
        """,
        (
            provider_id,
            venue,
            symbol,
            timeframe,
            int(last_ts_open_ms),
            int(now_ms),
            int(now_ms),
            0,
            "ok",
            reason_code,
            trace_id,
        ),
    )


def materialize_derived_timeframe(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    timeframe: str,
    close_ts_ms: int,
    trace_id: str,
    now_ms: int,
) -> dict[str, Any]:
    target_tf = str(timeframe or "").strip().lower()
    if target_tf not in {"4h", "1d", "1w"}:
        return {
            "status": "blocked",
            "reason": "unsupported_derived_timeframe",
            "next_action": "use_4h_1d_or_1w",
            "details": {"timeframe": target_tf},
        }

    to_ms = TIMEFRAME_MS[target_tf]
    bucket_open = int(close_ts_ms) - to_ms
    source = _load_candles_for_window(
        conn,
        symbol=symbol,
        timeframe="5m",
        start_open_ms=bucket_open,
        end_open_ms=int(close_ts_ms),
        preferred_provider_id="ccxt",
        venue="okx",
    )
    expected_bars = to_ms // TIMEFRAME_MS["5m"]
    derived = aggregate_timeframe_candles(
        source,
        from_timeframe="5m",
        to_timeframe=target_tf,
        dataset_version=f"{_DERIVED_DATASET_PREFIX}:{target_tf}",
        trace_id=trace_id,
    )
    if len(derived) != 1:
        return {
            "status": "blocked",
            "reason": "missing_source_bars",
            "next_action": "reload_authoritative_5m_window_then_retry",
            "details": {
                "timeframe": target_tf,
                "source_bar_count_expected": int(expected_bars),
                "source_bar_count_observed": len(source),
                "window_start_ms": bucket_open,
                "window_end_ms": int(close_ts_ms),
            },
        }

    aggregate = derived[0]
    stored = CandleDTO(
        provider_id=_DERIVED_PROVIDER_ID,
        venue=aggregate.venue,
        symbol=aggregate.symbol,
        timeframe=aggregate.timeframe,
        ts_open_ms=aggregate.ts_open_ms,
        ts_close_ms=aggregate.ts_close_ms,
        open=aggregate.open,
        high=aggregate.high,
        low=aggregate.low,
        close=aggregate.close,
        volume=aggregate.volume,
        dataset_version=aggregate.dataset_version,
        trace_id=aggregate.trace_id,
    )
    summary = upsert_market_candles(conn, [stored], ingest_ts_ms=int(now_ms))
    _upsert_feed_checkpoint(
        conn,
        provider_id=_DERIVED_PROVIDER_ID,
        venue=aggregate.venue,
        symbol=aggregate.symbol,
        timeframe=target_tf,
        last_ts_open_ms=aggregate.ts_open_ms,
        now_ms=int(now_ms),
        reason_code="DERIVED_AGGREGATE_READY",
        trace_id=trace_id,
    )
    conn.commit()
    return {
        "status": "completed",
        "reason": "derived_aggregate_materialized",
        "next_action": None,
        "details": {
            "timeframe": target_tf,
            "ts_open_ms": aggregate.ts_open_ms,
            "ts_close_ms": aggregate.ts_close_ms,
            "source_bar_count_expected": int(expected_bars),
            "source_bar_count_observed": len(source),
            "inserted": int(summary.get("inserted") or 0),
            "idempotent": int(summary.get("idempotent") or 0),
            "provider_id": _DERIVED_PROVIDER_ID,
            "dataset_version": aggregate.dataset_version,
        },
        "trace_id": trace_id,
    }


def recompute_surveyor_family_snapshot(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    timeframe: str,
    trace_id: str,
    authoritative_view: Mapping[str, Any] | None = None,
    ladders: Mapping[str, Any] | None = None,
    allow_replay_fallback: bool = False,
) -> dict[str, Any]:
    from market_arbiter.core.surveyor_snapshot import build_surveyor_packet_snapshot

    target_tf = _SURVEYOR_TF_LABELS.get(str(timeframe or "").strip().lower(), str(timeframe or ""))
    packet = build_surveyor_packet_snapshot(
        conn,
        symbol=symbol,
        authoritative_view=authoritative_view,
        ladders=ladders,
        allow_replay_fallback=allow_replay_fallback,
    )
    market_tf = dict((packet.get("market_data") or {}).get("timeframes") or {}).get(target_tf) or {}
    structure_tf = dict((packet.get("structure") or {}).get("timeframes") or {}).get(target_tf) or {}
    bar_count = int(market_tf.get("bar_count_available") or 0)
    freshness_state = str(market_tf.get("freshness_state") or "missing")
    structure_status = str(structure_tf.get("status") or "missing")

    if bar_count <= 0:
        return {
            "status": "blocked",
            "reason": "missing_timeframe_input",
            "next_action": "materialize_or_reload_timeframe_input_then_retry",
            "details": {
                "target_timeframe": target_tf,
                "packet_status": (packet.get("meta") or {}).get("packet_status"),
                "bundle_status": ((packet.get("bundle") or {}).get("meta") or {}).get("bundle_status"),
            },
            "trace_id": trace_id,
        }

    if structure_status not in {"ok", "supporting_only"} and freshness_state == "fresh":
        return {
            "status": "blocked",
            "reason": "structure_missing_despite_feed_input",
            "next_action": "inspect_snapshot_build_for_target_timeframe",
            "details": {
                "target_timeframe": target_tf,
                "freshness_state": freshness_state,
                "structure_status": structure_status,
                "packet_status": (packet.get("meta") or {}).get("packet_status"),
            },
            "trace_id": trace_id,
        }

    return {
        "status": "completed",
        "reason": "surveyor_snapshot_recomputed",
        "next_action": None,
        "details": {
            "target_timeframe": target_tf,
            "packet_status": (packet.get("meta") or {}).get("packet_status"),
            "bundle_status": ((packet.get("bundle") or {}).get("meta") or {}).get("bundle_status"),
            "continuity_state": (packet.get("meta") or {}).get("continuity_state"),
            "freshness_state": freshness_state,
            "structure_status": structure_status,
            "bar_count_available": bar_count,
        },
        "trace_id": trace_id,
    }


def build_live_recompute_task_runner(
    conn: sqlite3.Connection,
    *,
    authoritative_view_loader: Callable[[str], Mapping[str, Any] | None] | None = None,
    ladders_loader: Callable[[str], Mapping[str, Any] | None] | None = None,
    now_fn: Callable[[], int] = _now_ms,
    allow_replay_fallback: bool = False,
) -> Callable[[Mapping[str, Any]], Mapping[str, Any]]:
    def _runner(task: Mapping[str, Any]) -> Mapping[str, Any]:
        action = str(task.get("action") or "")
        symbol = str(task.get("symbol") or "")
        timeframe = str(task.get("timeframe") or "")
        trace_id = str(task.get("trace_id") or "")
        close_ts_ms = int(task.get("close_ts_ms") or 0) or None

        if action.startswith("materialize_") and action.endswith("_aggregate"):
            if close_ts_ms is None:
                return {
                    "status": "blocked",
                    "reason": "manifest_close_ts_missing",
                    "next_action": "rebuild_close_manifest",
                    "details": {"manifest_id": task.get("manifest_id"), "action": action},
                    "trace_id": trace_id,
                }
            return materialize_derived_timeframe(
                conn,
                symbol=symbol,
                timeframe=timeframe,
                close_ts_ms=close_ts_ms,
                trace_id=trace_id,
                now_ms=int(now_fn()),
            )

        if action == "recompute_surveyor_families":
            authoritative_view = authoritative_view_loader(symbol) if authoritative_view_loader else None
            ladders = ladders_loader(symbol) if ladders_loader else None
            return recompute_surveyor_family_snapshot(
                conn,
                symbol=symbol,
                timeframe=timeframe,
                trace_id=trace_id,
                authoritative_view=authoritative_view,
                ladders=ladders,
                allow_replay_fallback=allow_replay_fallback,
            )

        return {
            "status": "blocked",
            "reason": "unsupported_recompute_action",
            "next_action": "register_task_handler",
            "details": {"action": action},
            "trace_id": trace_id,
        }

    return _runner


def _close_provider(provider: MarketDataProvider) -> None:
    close_fn = getattr(provider, "close", None)
    if callable(close_fn):
        close_fn()
        return

    exchange = getattr(provider, "exchange", None)
    exchange_close = getattr(exchange, "close", None)
    if callable(exchange_close):
        exchange_close()


def _default_provider_factory() -> MarketDataProvider:
    import ccxt

    from market_arbiter.ops.surveyor_feed_refresh import OkxCcxtProvider

    exchange = ccxt.okx({"enableRateLimit": True})
    exchange.load_markets()
    return OkxCcxtProvider(exchange)


def _checkpoint_row(conn, *, provider_id: str, venue: str, symbol: str, timeframe: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id
        FROM feed_checkpoints
        WHERE provider_id = ? AND venue = ? AND symbol = ? AND timeframe = ?;
        """,
        (provider_id, venue, symbol, timeframe),
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


def _build_key_status(*, symbol: str, timeframe: str, checkpoint: dict[str, Any] | None, now_ms: int) -> dict[str, Any]:
    tf_ms = TIMEFRAME_MS[timeframe]
    last_ts_open_ms = checkpoint.get("last_ts_open_ms") if checkpoint else None
    latest_close_ms = (int(last_ts_open_ms) + tf_ms) if last_ts_open_ms is not None else None
    freshness_ms = (now_ms - latest_close_ms) if latest_close_ms is not None else None
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "state": checkpoint.get("state") if checkpoint else "missing",
        "last_reason_code": checkpoint.get("last_reason_code") if checkpoint else "NO_CHECKPOINT",
        "failure_count": checkpoint.get("failure_count") if checkpoint else 0,
        "last_ts_open_ms": last_ts_open_ms,
        "latest_close_ms": latest_close_ms,
        "last_success_ms": checkpoint.get("last_success_ms") if checkpoint else None,
        "last_attempt_ms": checkpoint.get("last_attempt_ms") if checkpoint else None,
        "freshness_ms": freshness_ms,
        "trace_id": checkpoint.get("trace_id") if checkpoint else None,
    }


def _close_ts_ms(last_ts_open_ms: int | None, timeframe: str) -> int | None:
    if last_ts_open_ms is None:
        return None
    return int(last_ts_open_ms) + int(TIMEFRAME_MS[timeframe])


def _closed_timeframes_for_close_ts(close_ts_ms: int) -> list[str]:
    closed = ["5m"]
    for timeframe in ("4h", "1d", "1w"):
        if close_ts_ms % TIMEFRAME_MS[timeframe] == 0:
            closed.append(timeframe)
    return closed


def build_close_manifest(*, symbol: str, venue: str, close_ts_ms: int, trace_id: str) -> dict[str, Any]:
    normalized_symbol = _market_symbol(symbol)
    closed_timeframes = _closed_timeframes_for_close_ts(int(close_ts_ms))
    manifest_id = f"close:{normalized_symbol}:{int(close_ts_ms)}:{trace_id}"
    return {
        "contract": "surveyor_close_manifest_v1",
        "manifest_id": manifest_id,
        "symbol": normalized_symbol,
        "venue": venue,
        "trigger_timeframe": "5m",
        "closed_timeframes": closed_timeframes,
        "close_ts_ms": int(close_ts_ms),
        "trace_id": trace_id,
    }


def build_recompute_tasks(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    manifest_id = str(manifest.get("manifest_id") or "")
    symbol = str(manifest.get("symbol") or "")
    trace_id = str(manifest.get("trace_id") or "")
    close_ts_ms = int(manifest.get("close_ts_ms") or 0)
    closed_timeframes = [str(value).lower() for value in (manifest.get("closed_timeframes") or [])]

    tasks: list[dict[str, Any]] = []
    depends_on: list[str] = []
    if "5m" in closed_timeframes:
        task_id = f"{manifest_id}:recompute:5m"
        tasks.append(
            {
                "contract": "surveyor_recompute_task_v1",
                "task_id": task_id,
                "manifest_id": manifest_id,
                "symbol": symbol,
                "timeframe": "5m",
                "action": "recompute_surveyor_families",
                "close_ts_ms": close_ts_ms,
                "depends_on": [],
                "trace_id": trace_id,
            }
        )
        depends_on = [task_id]

    for timeframe in ("4h", "1d", "1w"):
        if timeframe not in closed_timeframes:
            continue
        materialize_id = f"{manifest_id}:materialize:{timeframe}"
        tasks.append(
            {
                "contract": "surveyor_recompute_task_v1",
                "task_id": materialize_id,
                "manifest_id": manifest_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "action": f"materialize_{timeframe}_aggregate",
                "close_ts_ms": close_ts_ms,
                "depends_on": list(depends_on),
                "trace_id": trace_id,
            }
        )
        recompute_id = f"{manifest_id}:recompute:{timeframe}"
        tasks.append(
            {
                "contract": "surveyor_recompute_task_v1",
                "task_id": recompute_id,
                "manifest_id": manifest_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "action": "recompute_surveyor_families",
                "close_ts_ms": close_ts_ms,
                "depends_on": [materialize_id],
                "trace_id": trace_id,
            }
        )
        depends_on = [recompute_id]
    return tasks


@dataclass
class SymbolRecomputeLockManager:
    held_symbols: dict[str, str] | None = None
    _guard: threading.RLock = field(default_factory=threading.RLock, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.held_symbols is None:
            self.held_symbols = {}

    def acquire(self, *, symbol: str, manifest_id: str) -> bool:
        with self._guard:
            owner = self.held_symbols.get(symbol)
            if owner and owner != manifest_id:
                return False
            self.held_symbols[symbol] = manifest_id
            return True

    def release(self, *, symbol: str, manifest_id: str) -> None:
        with self._guard:
            if self.held_symbols.get(symbol) == manifest_id:
                self.held_symbols.pop(symbol, None)


def _default_recompute_task_runner(task: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": "completed",
        "reason": "task_runner_not_provided",
        "next_action": None,
        "details": {
            "note": "execution path verified without a concrete recompute handler",
            "action": task.get("action"),
        },
    }


def execute_recompute_manifest(
    manifest: Mapping[str, Any],
    tasks: Sequence[Mapping[str, Any]],
    *,
    task_runner: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    lock_manager: SymbolRecomputeLockManager | None = None,
) -> dict[str, Any]:
    manifest_id = str(manifest.get("manifest_id") or "")
    symbol = str(manifest.get("symbol") or "")
    trace_id = str(manifest.get("trace_id") or "")
    runner = task_runner or _default_recompute_task_runner
    locks = lock_manager or SymbolRecomputeLockManager()

    if not locks.acquire(symbol=symbol, manifest_id=manifest_id):
        return {
            "contract": "surveyor_recompute_result_v1",
            "manifest_id": manifest_id,
            "symbol": symbol,
            "status": "blocked",
            "failed_step": "symbol_lock_unavailable",
            "reason": "symbol_recompute_already_active",
            "next_action": "retry_manifest_later",
            "trace_id": trace_id,
            "task_results": [],
        }

    task_results: list[dict[str, Any]] = []
    completed_task_ids: set[str] = set()
    try:
        for task in tasks:
            task_id = str(task.get("task_id") or "")
            missing_deps = [dep for dep in task.get("depends_on") or [] if dep not in completed_task_ids]
            if missing_deps:
                return {
                    "contract": "surveyor_recompute_result_v1",
                    "manifest_id": manifest_id,
                    "symbol": symbol,
                    "status": "blocked",
                    "failed_step": task.get("action"),
                    "reason": "missing_task_dependency",
                    "next_action": "rebuild_manifest_dependencies",
                    "trace_id": trace_id,
                    "task_results": task_results,
                }

            raw_result = dict(runner(task) or {})
            task_status = str(raw_result.get("status") or "completed")
            task_result = {
                "contract": "surveyor_recompute_task_result_v1",
                "task_id": task_id,
                "manifest_id": manifest_id,
                "symbol": symbol,
                "timeframe": task.get("timeframe"),
                "action": task.get("action"),
                "status": task_status,
                "reason": raw_result.get("reason"),
                "next_action": raw_result.get("next_action"),
                "details": dict(raw_result.get("details") or {}),
                "trace_id": str(raw_result.get("trace_id") or task.get("trace_id") or trace_id),
            }
            task_results.append(task_result)
            if task_status != "completed":
                return {
                    "contract": "surveyor_recompute_result_v1",
                    "manifest_id": manifest_id,
                    "symbol": symbol,
                    "status": "blocked",
                    "failed_step": task.get("action"),
                    "reason": task_result.get("reason") or "task_failed",
                    "next_action": task_result.get("next_action") or "inspect_failed_step",
                    "trace_id": trace_id,
                    "task_results": task_results,
                }
            completed_task_ids.add(task_id)

        return {
            "contract": "surveyor_recompute_result_v1",
            "manifest_id": manifest_id,
            "symbol": symbol,
            "status": "completed",
            "failed_step": None,
            "reason": None,
            "next_action": None,
            "trace_id": trace_id,
            "task_results": task_results,
        }
    finally:
        locks.release(symbol=symbol, manifest_id=manifest_id)


def execute_close_manifests(
    close_manifests: Sequence[Mapping[str, Any]],
    *,
    task_builder: Callable[[Mapping[str, Any]], list[dict[str, Any]]] = build_recompute_tasks,
    task_runner: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    lock_manager: SymbolRecomputeLockManager | None = None,
    max_workers: int = 1,
) -> list[dict[str, Any]]:
    locks = lock_manager or SymbolRecomputeLockManager()
    manifests = list(close_manifests)
    if not manifests:
        return []

    worker_count = max(1, int(max_workers or 1))

    def _execute_one(manifest: Mapping[str, Any]) -> dict[str, Any]:
        tasks = task_builder(manifest)
        return execute_recompute_manifest(
            manifest,
            tasks,
            task_runner=task_runner,
            lock_manager=locks,
        )

    if worker_count == 1 or len(manifests) == 1:
        return [_execute_one(manifest) for manifest in manifests]

    # Worker-pool concurrency is allowed across symbols, but a single symbol's
    # close manifests must remain serialized. Build one lane per symbol, process
    # each lane in input order, and bound concurrently active lanes by max_workers.
    lanes_by_symbol: dict[str, list[tuple[int, Mapping[str, Any]]]] = {}
    for index, manifest in enumerate(manifests):
        symbol = str(manifest.get("symbol") or "")
        lanes_by_symbol.setdefault(symbol, []).append((index, manifest))

    results: list[dict[str, Any] | None] = [None] * len(manifests)

    def _execute_lane(lane: Sequence[tuple[int, Mapping[str, Any]]]) -> list[tuple[int, dict[str, Any]]]:
        lane_results: list[tuple[int, dict[str, Any]]] = []
        for index, manifest in lane:
            lane_results.append((index, _execute_one(manifest)))
        return lane_results

    with ThreadPoolExecutor(max_workers=min(worker_count, len(lanes_by_symbol))) as executor:
        futures = [executor.submit(_execute_lane, lane) for lane in lanes_by_symbol.values()]
        for future in as_completed(futures):
            for index, result in future.result():
                results[index] = result

    return [result or {} for result in results]


def _close_manifests_for_results(
    conn,
    results: Sequence[dict[str, Any]],
    *,
    emitted_manifest_ids: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    manifests: list[dict[str, Any]] = []
    seen: set[str] = {str(value) for value in (emitted_manifest_ids or []) if str(value or "").strip()}
    for row in results:
        if str(row.get("timeframe") or "") != "5m":
            continue
        if str(row.get("state") or "") != "ok":
            continue
        checkpoint = _checkpoint_row(
            conn,
            provider_id=str(row.get("provider_id") or "ccxt"),
            venue=str(row.get("venue") or "okx"),
            symbol=str(row.get("symbol") or ""),
            timeframe="5m",
        )
        close_ts_ms = _close_ts_ms(checkpoint.get("last_ts_open_ms") if checkpoint else None, "5m")
        if close_ts_ms is None:
            continue
        if close_ts_ms > int(row.get("as_of_ms") or 0):
            continue
        manifest = build_close_manifest(
            symbol=str(row.get("symbol") or ""),
            venue=str(row.get("venue") or "okx"),
            close_ts_ms=close_ts_ms,
            trace_id=str(row.get("trace_id") or "trace"),
        )
        manifest_id = str(manifest["manifest_id"])
        if manifest_id in seen:
            continue
        seen.add(manifest_id)
        manifests.append(manifest)
    return manifests


def _continuity_state(*, runner_state: dict[str, Any] | None, key_statuses: Sequence[dict[str, Any]], now_ms: int) -> str:
    if not key_statuses:
        return "unknown"

    if all(row.get("state") == "missing" for row in key_statuses):
        return "unknown"

    has_issues = any(row.get("state") != "ok" for row in key_statuses)
    if runner_state is None:
        return "mixed" if has_issues else "one_shot_refresh"

    mode = str(runner_state.get("mode") or "unknown")
    if mode == "once":
        return "mixed" if has_issues else "one_shot_refresh"

    if mode == "loop":
        liveness = _runner_liveness(runner_state=runner_state, now_ms=now_ms)
        if liveness["state"] == "live":
            return "mixed" if has_issues else "live_continuous"
        if liveness["state"] == "stale":
            return "stale_loop"
        return "mixed"

    return "mixed" if has_issues else "unknown"


def _runner_liveness(*, runner_state: dict[str, Any] | None, now_ms: int) -> dict[str, Any]:
    if runner_state is None:
        return {
            "state": "unknown",
            "reason_code": "RUNNER_STATE_MISSING",
            "last_cycle_completed_ms": None,
            "last_cycle_age_ms": None,
            "stale_after_ms": None,
            "loop_sleep_ms": None,
            "mode": None,
        }

    mode = str(runner_state.get("mode") or "unknown")
    loop_sleep_ms = int(runner_state.get("loop_sleep_ms") or DEFAULT_LOOP_SLEEP_MS)
    stale_after_ms = max(loop_sleep_ms * 3, 60_000)
    last_cycle_ms = runner_state.get("last_cycle_completed_ms")
    if not isinstance(last_cycle_ms, int):
        return {
            "state": "unknown",
            "reason_code": "RUNNER_LAST_CYCLE_MISSING",
            "last_cycle_completed_ms": None,
            "last_cycle_age_ms": None,
            "stale_after_ms": stale_after_ms if mode == "loop" else None,
            "loop_sleep_ms": loop_sleep_ms if mode == "loop" else None,
            "mode": mode,
        }
    age_ms = max(0, int(now_ms) - int(last_cycle_ms))
    if mode != "loop":
        return {
            "state": "not_continuous",
            "reason_code": "RUNNER_NOT_LOOP_MODE",
            "last_cycle_completed_ms": int(last_cycle_ms),
            "last_cycle_age_ms": age_ms,
            "stale_after_ms": None,
            "loop_sleep_ms": loop_sleep_ms,
            "mode": mode,
        }
    if age_ms <= stale_after_ms:
        return {
            "state": "live",
            "reason_code": None,
            "last_cycle_completed_ms": int(last_cycle_ms),
            "last_cycle_age_ms": age_ms,
            "stale_after_ms": stale_after_ms,
            "loop_sleep_ms": loop_sleep_ms,
            "mode": mode,
        }
    return {
        "state": "stale",
        "reason_code": "RUNNER_STALE",
        "last_cycle_completed_ms": int(last_cycle_ms),
        "last_cycle_age_ms": age_ms,
        "stale_after_ms": stale_after_ms,
        "loop_sleep_ms": loop_sleep_ms,
        "mode": mode,
    }


def _summarize_results(results: Sequence[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    inserted = 0
    for row in results:
        state = str(row.get("state") or "unknown")
        counts[state] = counts.get(state, 0) + 1
        inserted += int(row.get("inserted") or 0)
    return {
        "configured_keys": len(results),
        "state_counts": counts,
        "inserted": inserted,
    }


def _summarize_shards(
    results: Sequence[Mapping[str, Any]],
    *,
    symbols: Sequence[str],
    timeframes: Sequence[str],
    shards: Mapping[str, Sequence[str]] | None = None,
    now_ms: int,
    runner_liveness: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    configured_shards = _normalize_shards(symbols, shards)
    configured_timeframes = _normalize_timeframes(timeframes)
    shard_statuses: list[dict[str, Any]] = []
    overall_state = "ok" if configured_shards else "unknown"
    for shard_id, shard_symbols in configured_shards.items():
        rows = [row for row in results if str(row.get("shard_id") or "default") == shard_id]
        state_counts: dict[str, int] = {}
        issue_symbols: set[str] = set()
        degraded_symbols: set[str] = set()
        repair_pending_symbols: set[str] = set()
        reason_codes: set[str] = set()
        last_confirmed_close_ms: int | None = None
        for row in rows:
            state = str(row.get("state") or "unknown")
            state_counts[state] = state_counts.get(state, 0) + 1
            symbol = str(row.get("symbol") or "")
            if state != "ok":
                issue_symbols.add(symbol)
            if state in {"degraded", "tripped"}:
                degraded_symbols.add(symbol)
            if state == "resync_required":
                repair_pending_symbols.add(symbol)
            for reason in row.get("reason_codes") or []:
                reason_codes.add(str(reason))
            if str(row.get("timeframe") or "") == "5m" and state == "ok":
                latest_close_ms = row.get("latest_close_ms") or row.get("last_ts_open_ms")
                if latest_close_ms is not None:
                    last_confirmed_close_ms = max(last_confirmed_close_ms or 0, int(latest_close_ms))
        if any(state in state_counts for state in ("tripped", "resync_required")):
            shard_state = "repair_pending"
        elif any(state != "ok" for state in state_counts):
            shard_state = "degraded"
        elif rows:
            shard_state = "ok"
        else:
            shard_state = "missing"

        if shard_state == "repair_pending":
            overall_state = "repair_pending"
        elif shard_state == "degraded" and overall_state == "ok":
            overall_state = "degraded"
        elif shard_state == "missing" and overall_state == "ok":
            overall_state = "missing"

        shard_statuses.append(
            {
                "contract": FEED_SHARD_STATUS_CONTRACT,
                "shard_id": shard_id,
                "configured_symbols": list(shard_symbols),
                "configured_keys": len(shard_symbols) * len(configured_timeframes),
                "observed_keys": len(rows),
                "state": shard_state,
                "state_counts": state_counts,
                "issue_symbols": sorted(issue_symbols),
                "degraded_symbols": sorted(degraded_symbols),
                "repair_pending_symbols": sorted(repair_pending_symbols),
                "reason_codes": sorted(reason_codes),
                "last_confirmed_close_ms": last_confirmed_close_ms,
                "as_of_ms": int(now_ms),
            }
        )

    reason_codes: list[str] = []
    if runner_liveness and runner_liveness.get("state") == "stale":
        reason_codes.append(str(runner_liveness.get("reason_code") or "RUNNER_STALE"))
        if overall_state == "ok":
            overall_state = "stale"

    return {
        "contract": FEED_SHARD_STATUS_CONTRACT,
        "overall_state": overall_state,
        "reason_codes": reason_codes,
        "shard_count": len(shard_statuses),
        "shards": shard_statuses,
    }


def _write_runner_state(
    *,
    config: "FeedRunnerConfig",
    mode: str,
    cycle_index: int,
    cycle_results: Sequence[dict[str, Any]],
    close_manifests: Sequence[Mapping[str, Any]],
    recompute_tasks: Sequence[Mapping[str, Any]],
    now_ms: int,
) -> dict[str, Any]:
    state_path = _state_path_for_db(config.db_path, config.state_path)
    existing = _read_json(state_path) or {}
    existing_manifest_ids = [str(value) for value in (existing.get("emitted_manifest_ids") or []) if str(value or "").strip()]
    current_manifest_ids = [str(row.get("manifest_id") or "") for row in close_manifests if str(row.get("manifest_id") or "").strip()]
    payload = {
        "contract": RUNNER_STATE_CONTRACT,
        "mode": mode,
        "db_path": config.db_path,
        "state_path": str(state_path),
        "workset_manifest": build_feed_workset_manifest(config),
        "symbols": list(config.symbols),
        "timeframes": list(config.timeframes),
        "loop_sleep_ms": int(config.loop_sleep_ms),
        "close_lag_ms": int(config.close_lag_ms),
        "backfill_page_limit": int(config.backfill_page_limit),
        "max_backfill_bars": int(config.max_backfill_bars),
        "cycles_completed": int(existing.get("cycles_completed") or 0) + 1,
        "last_cycle_completed_ms": now_ms,
        "last_cycle_index": cycle_index,
        "last_cycle_summary": _summarize_results(cycle_results),
        "last_shard_status": _summarize_shards(
            cycle_results,
            symbols=config.symbols,
            timeframes=config.timeframes,
            shards=config.shards,
            now_ms=now_ms,
        ),
        "last_cycle_results": list(cycle_results),
        "last_close_manifests": list(close_manifests),
        "last_recompute_tasks": list(recompute_tasks),
        "emitted_manifest_ids": list(dict.fromkeys([*existing_manifest_ids, *current_manifest_ids]))[-200:],
    }
    _write_json(state_path, payload)
    return payload


def _has_derived_candle(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    timeframe: str,
    close_ts_ms: int,
    venue: str = "okx",
) -> bool:
    aliases = _symbol_aliases(symbol)
    placeholders = ",".join("?" for _ in aliases)
    row = conn.execute(
        f"""
        SELECT 1
        FROM market_candles
        WHERE provider_id = ?
          AND venue = ?
          AND timeframe = ?
          AND ts_close_ms = ?
          AND symbol IN ({placeholders})
        LIMIT 1;
        """,
        (_DERIVED_PROVIDER_ID, venue, timeframe, int(close_ts_ms), *aliases),
    ).fetchone()
    return row is not None


def run_canary(
    config: FeedRunnerConfig,
    *,
    provider: MarketDataProvider | None = None,
    provider_factory: Callable[[], MarketDataProvider] | None = None,
    recompute_task_runner: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    now_fn: Callable[[], int] = _now_ms,
    require_manifest: bool = False,
) -> dict[str, Any]:
    started_ms = int(now_fn())
    payload = run_once(
        config,
        provider=provider,
        provider_factory=provider_factory,
        recompute_task_runner=recompute_task_runner,
        now_fn=now_fn,
    )
    finished_ms = int(now_fn())

    results = list(payload.get("results") or [])
    close_manifests = list(payload.get("close_manifests") or [])
    recompute_results = {
        str(row.get("manifest_id") or ""): row
        for row in (payload.get("recompute_results") or [])
        if str(row.get("manifest_id") or "").strip()
    }
    ok_5m_results = [
        row for row in results if str(row.get("timeframe") or "") == "5m" and str(row.get("state") or "") == "ok"
    ]
    checks: list[dict[str, Any]] = []
    status = "ok"
    reason = "canary_ok"

    if not ok_5m_results:
        status = "blocked"
        reason = "missing_ok_5m_cycle"

    if require_manifest and not close_manifests and status == "ok":
        status = "blocked"
        reason = "no_close_manifest_emitted"

    conn = init_db(config.db_path)
    try:
        for manifest in close_manifests:
            manifest_id = str(manifest.get("manifest_id") or "")
            close_ts_ms = int(manifest.get("close_ts_ms") or 0)
            closed_timeframes = [str(value).lower() for value in (manifest.get("closed_timeframes") or [])]
            recompute_result = recompute_results.get(manifest_id) or {}
            derived_checks = {
                timeframe: _has_derived_candle(
                    conn,
                    symbol=str(manifest.get("symbol") or ""),
                    timeframe=timeframe,
                    close_ts_ms=close_ts_ms,
                    venue=str(manifest.get("venue") or "okx"),
                )
                for timeframe in closed_timeframes
                if timeframe in {"4h", "1d", "1w"}
            }
            checks.append(
                {
                    "manifest_id": manifest_id,
                    "symbol": manifest.get("symbol"),
                    "close_ts_ms": close_ts_ms,
                    "closed_timeframes": closed_timeframes,
                    "recompute_status": recompute_result.get("status"),
                    "verified_derived_aggregates": derived_checks,
                    "task_statuses": [
                        {
                            "timeframe": task.get("timeframe"),
                            "action": task.get("action"),
                            "status": task.get("status"),
                        }
                        for task in (recompute_result.get("task_results") or [])
                    ],
                }
            )

            if str(recompute_result.get("status") or "") != "completed" and status == "ok":
                status = "blocked"
                reason = str(recompute_result.get("reason") or "recompute_failed")
            if not all(derived_checks.values()) and status == "ok":
                status = "blocked"
                reason = "derived_aggregate_verification_failed"
    finally:
        conn.close()

    return {
        "contract": FEED_CANARY_CONTRACT,
        "status": status,
        "reason": reason,
        "started_ms": started_ms,
        "finished_ms": finished_ms,
        "config": asdict(config),
        "summary": {
            "ok_5m_results": len(ok_5m_results),
            "close_manifest_count": len(close_manifests),
            "recompute_result_count": len(recompute_results),
        },
        "checks": checks,
        "run_once_payload": payload,
    }


def _scheduler_keys(symbols: Sequence[str], timeframes: Sequence[str], *, route: FeedSourceRoute | None = None) -> list[SchedulerKey]:
    keys: list[SchedulerKey] = []
    provider_id = route.write_provider_id if route else "ccxt"
    venue = route.venue if route else "okx"
    for symbol in symbols:
        market_symbol = _market_symbol(symbol)
        for timeframe in timeframes:
            normalized_tf = str(timeframe).strip().lower()
            if normalized_tf not in TIMEFRAME_MS:
                raise ValueError(f"unsupported timeframe: {timeframe}")
            keys.append(SchedulerKey(provider_id, venue, market_symbol, normalized_tf))
    return keys


@dataclass(frozen=True)
class ShardedSchedulerKey:
    shard_id: str
    key: SchedulerKey


def _sharded_scheduler_keys(config: "FeedRunnerConfig") -> list[ShardedSchedulerKey]:
    symbol_to_shard = _shard_for_symbol_map(config.symbols, config.shards)
    rows: list[ShardedSchedulerKey] = []
    for key in _scheduler_keys(config.symbols, config.timeframes, route=_rest_history_route_for_config(config)):
        workset_symbol = key.symbol.replace("/", "")
        rows.append(ShardedSchedulerKey(shard_id=symbol_to_shard.get(workset_symbol, "default"), key=key))
    return rows


def _run_cycle_set(
    scheduler: MarketDataScheduler,
    *,
    keys: Sequence[SchedulerKey] | Sequence[ShardedSchedulerKey],
    cycle_index: int,
    run_label: str,
    now_fn: Callable[[], int],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for row in keys:
        if isinstance(row, ShardedSchedulerKey):
            shard_id = row.shard_id
            key = row.key
        else:
            shard_id = "default"
            key = row
        cycle_now_ms = int(now_fn())
        trace_id = _trace_id(key.symbol, key.timeframe, run_label=run_label, cycle_index=cycle_index, now_ms=cycle_now_ms)
        result = scheduler.run_cycle(key, now_ms=cycle_now_ms, trace_id=trace_id)
        result["shard_id"] = shard_id
        results.append(result)
    return results


@dataclass(frozen=True)
class FeedRunnerConfig:
    db_path: str
    symbols: list[str]
    timeframes: list[str]
    shards: dict[str, list[str]] | None = None
    loop_sleep_ms: int = DEFAULT_LOOP_SLEEP_MS
    close_lag_ms: int = DEFAULT_CLOSE_LAG_MS
    backfill_page_limit: int = DEFAULT_BACKFILL_PAGE_LIMIT
    max_backfill_bars: int = DEFAULT_MAX_BACKFILL_BARS
    request_spacing_ms: int = DEFAULT_REQUEST_SPACING_MS
    state_path: str | None = None
    manifest_path: str | None = None
    provider_policy_path: str | None = None
    source_routes: dict[str, dict[str, Any]] | None = None


def _split_source_plan_for_config(config: FeedRunnerConfig) -> SplitSourceRoutingPlan | None:
    if not config.source_routes:
        return None
    return SplitSourceRoutingPlan.from_payload({"routes": config.source_routes})


def _rest_history_route_for_config(config: FeedRunnerConfig) -> FeedSourceRoute | None:
    plan = _split_source_plan_for_config(config)
    return plan.route("rest_history") if plan else None


def _routed_provider_for_config(config: FeedRunnerConfig, provider: MarketDataProvider) -> MarketDataProvider:
    route = _rest_history_route_for_config(config)
    if route is None:
        return provider
    return RoutedMarketDataProvider(provider, route)


def _provider_governor_for_config(config: FeedRunnerConfig) -> ProviderAccessGovernor | None:
    if not config.provider_policy_path:
        return None
    return ProviderAccessGovernor(
        policy=ProviderRolePolicy.from_path(config.provider_policy_path),
        script_id="market_arbiter.ops.surveyor_feed_runner",
    )


def run_once(
    config: FeedRunnerConfig,
    *,
    provider: MarketDataProvider | None = None,
    provider_factory: Callable[[], MarketDataProvider] | None = None,
    recompute_task_runner: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    now_fn: Callable[[], int] = _now_ms,
) -> dict[str, Any]:
    conn = init_db(config.db_path)
    resolved_provider = provider or (provider_factory or _default_provider_factory)()
    routed_provider = _routed_provider_for_config(config, resolved_provider)
    cycle_now_ms = int(now_fn())
    run_label = "once"
    existing_state = _read_json(_state_path_for_db(config.db_path, config.state_path)) or {}
    try:
        scheduler = MarketDataScheduler(
            conn,
            routed_provider,
            close_lag_ms=config.close_lag_ms,
            backfill_page_limit=config.backfill_page_limit,
            max_backfill_bars=config.max_backfill_bars,
            request_spacing_ms=config.request_spacing_ms,
            provider_governor=_provider_governor_for_config(config),
            request_class="rest_history",
        )
        results = _run_cycle_set(
            scheduler,
            keys=_sharded_scheduler_keys(config),
            cycle_index=1,
            run_label=run_label,
            now_fn=now_fn,
        )
        conn.commit()
        close_manifests = _close_manifests_for_results(
            conn,
            results,
            emitted_manifest_ids=existing_state.get("emitted_manifest_ids") or [],
        )
        recompute_tasks = [task for manifest in close_manifests for task in build_recompute_tasks(manifest)]
        task_runner = recompute_task_runner or build_live_recompute_task_runner(conn)
        recompute_results = execute_close_manifests(close_manifests, task_runner=task_runner) if close_manifests else []
        state = _write_runner_state(
            config=config,
            mode="once",
            cycle_index=1,
            cycle_results=results,
            close_manifests=close_manifests,
            recompute_tasks=recompute_tasks,
            now_ms=cycle_now_ms,
        )
        return {
            "contract": RUNNER_STATUS_CONTRACT,
            "mode": "once",
            "config": asdict(config),
            "summary": _summarize_results(results),
            "results": results,
            "close_manifests": close_manifests,
            "recompute_tasks": recompute_tasks,
            "recompute_results": recompute_results,
            "runner_state": state,
        }
    finally:
        conn.close()
        if provider is None:
            _close_provider(resolved_provider)


def run_loop(
    config: FeedRunnerConfig,
    *,
    provider: MarketDataProvider | None = None,
    provider_factory: Callable[[], MarketDataProvider] | None = None,
    recompute_task_runner: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    now_fn: Callable[[], int] = _now_ms,
    sleep_fn: Callable[[float], None] = time.sleep,
    max_cycles: int | None = None,
) -> dict[str, Any]:
    conn = init_db(config.db_path)
    resolved_provider = provider or (provider_factory or _default_provider_factory)()
    routed_provider = _routed_provider_for_config(config, resolved_provider)
    run_label = "loop"
    cycle_index = 0
    latest_results: list[dict[str, Any]] = []
    latest_state: dict[str, Any] | None = None
    latest_manifests: list[dict[str, Any]] = []
    latest_tasks: list[dict[str, Any]] = []
    latest_recompute_results: list[dict[str, Any]] = []
    emitted_manifest_ids = [
        str(value)
        for value in ((_read_json(_state_path_for_db(config.db_path, config.state_path)) or {}).get("emitted_manifest_ids") or [])
        if str(value or "").strip()
    ]
    try:
        scheduler = MarketDataScheduler(
            conn,
            routed_provider,
            close_lag_ms=config.close_lag_ms,
            backfill_page_limit=config.backfill_page_limit,
            max_backfill_bars=config.max_backfill_bars,
            request_spacing_ms=config.request_spacing_ms,
            provider_governor=_provider_governor_for_config(config),
            request_class="rest_history",
        )
        while True:
            cycle_index += 1
            latest_results = _run_cycle_set(
                scheduler,
                keys=_sharded_scheduler_keys(config),
                cycle_index=cycle_index,
                run_label=run_label,
                now_fn=now_fn,
            )
            conn.commit()
            latest_manifests = _close_manifests_for_results(conn, latest_results, emitted_manifest_ids=emitted_manifest_ids)
            latest_tasks = [task for manifest in latest_manifests for task in build_recompute_tasks(manifest)]
            task_runner = recompute_task_runner or build_live_recompute_task_runner(conn)
            latest_recompute_results = execute_close_manifests(latest_manifests, task_runner=task_runner) if latest_manifests else []
            emitted_manifest_ids = list(dict.fromkeys([*emitted_manifest_ids, *[str(row.get("manifest_id") or "") for row in latest_manifests if str(row.get("manifest_id") or "").strip()]]))[-200:]
            latest_state = _write_runner_state(
                config=config,
                mode="loop",
                cycle_index=cycle_index,
                cycle_results=latest_results,
                close_manifests=latest_manifests,
                recompute_tasks=latest_tasks,
                now_ms=int(now_fn()),
            )
            if max_cycles is not None and cycle_index >= max_cycles:
                break
            sleep_fn(max(0, config.loop_sleep_ms) / 1000.0)
        return {
            "contract": RUNNER_STATUS_CONTRACT,
            "mode": "loop",
            "config": asdict(config),
            "summary": _summarize_results(latest_results),
            "results": latest_results,
            "close_manifests": latest_manifests,
            "recompute_tasks": latest_tasks,
            "recompute_results": latest_recompute_results,
            "runner_state": latest_state or {},
        }
    finally:
        conn.close()
        if provider is None:
            _close_provider(resolved_provider)


def collect_status(config: FeedRunnerConfig, *, now_fn: Callable[[], int] = _now_ms) -> dict[str, Any]:
    now_ms = int(now_fn())
    state_path = _state_path_for_db(config.db_path, config.state_path)
    runner_state = _read_json(state_path)

    conn = init_db(config.db_path)
    try:
        key_statuses: list[dict[str, Any]] = []
        for row in _sharded_scheduler_keys(config):
            key = row.key
            checkpoint = _checkpoint_row(
                conn,
                provider_id=key.provider_id,
                venue=key.venue,
                symbol=key.symbol,
                timeframe=key.timeframe,
            )
            status = _build_key_status(
                symbol=key.symbol,
                timeframe=key.timeframe,
                checkpoint=checkpoint,
                now_ms=now_ms,
            )
            status["shard_id"] = row.shard_id
            key_statuses.append(status)
    finally:
        conn.close()

    runner_liveness = _runner_liveness(runner_state=runner_state, now_ms=now_ms)
    continuity_state = _continuity_state(runner_state=runner_state, key_statuses=key_statuses, now_ms=now_ms)
    summary = {
        "configured_keys": len(key_statuses),
        "ok_keys": sum(1 for row in key_statuses if row["state"] == "ok"),
        "issue_keys": sum(1 for row in key_statuses if row["state"] not in {"ok", "missing"}),
        "missing_keys": sum(1 for row in key_statuses if row["state"] == "missing"),
        "runner_liveness_state": runner_liveness["state"],
    }
    shard_status = _summarize_shards(
        key_statuses,
        symbols=config.symbols,
        timeframes=config.timeframes,
        shards=config.shards,
        now_ms=now_ms,
        runner_liveness=runner_liveness,
    )
    return {
        "contract": RUNNER_STATUS_CONTRACT,
        "db_path": config.db_path,
        "state_path": str(state_path),
        "symbols": list(config.symbols),
        "timeframes": list(config.timeframes),
        "workset_manifest": build_feed_workset_manifest(config),
        "continuity_state": continuity_state,
        "runner_liveness": runner_liveness,
        "summary": summary,
        "shard_status": shard_status,
        "runner_state": runner_state,
        "keys": key_statuses,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Surveyor feed ingestion once, continuously, or inspect status.")
    parser.add_argument("--mode", choices=("once", "loop", "status", "canary"), default="once")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--timeframes", default="5m,4h,1d,1w")
    parser.add_argument("--loop-sleep-ms", type=int, default=DEFAULT_LOOP_SLEEP_MS)
    parser.add_argument("--close-lag-ms", type=int, default=DEFAULT_CLOSE_LAG_MS)
    parser.add_argument("--backfill-page-limit", type=int, default=DEFAULT_BACKFILL_PAGE_LIMIT)
    parser.add_argument("--max-backfill-bars", type=int, default=DEFAULT_MAX_BACKFILL_BARS)
    parser.add_argument("--state-path", default=None)
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument("--provider-policy", default=None)
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.manifest_path:
        config = config_from_manifest(
            args.manifest_path,
            db_path=args.db_path,
            state_path=args.state_path,
        )
    else:
        config = FeedRunnerConfig(
            db_path=str(args.db_path or "data/market_arbiter.sqlite"),
            symbols=_normalize_symbols(args.symbols.split(",")),
            timeframes=_normalize_timeframes(args.timeframes.split(",")),
            loop_sleep_ms=int(args.loop_sleep_ms),
            close_lag_ms=int(args.close_lag_ms),
            backfill_page_limit=int(args.backfill_page_limit),
            max_backfill_bars=int(args.max_backfill_bars),
            state_path=args.state_path,
            manifest_path=args.manifest_path,
            provider_policy_path=args.provider_policy,
        )

    if args.provider_policy:
        config = FeedRunnerConfig(**(asdict(config) | {"provider_policy_path": args.provider_policy}))

    if args.mode == "status":
        payload = collect_status(config)
    elif args.mode == "canary":
        payload = run_canary(config)
    elif args.mode == "loop":
        payload = run_loop(config)
    else:
        payload = run_once(config)

    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
