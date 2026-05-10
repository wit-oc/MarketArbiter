from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import aiohttp


FEED_BAKEOFF_CLOSE_EVENT_CONTRACT = "feed_bakeoff_close_event_v1"
FEED_BAKEOFF_SMOKE_SUMMARY_CONTRACT = "feed_bakeoff_phase_a_smoke_summary_v1"
FEED_BAKEOFF_LIVE_SUMMARY_CONTRACT = "feed_bakeoff_phase_b_live_summary_v1"

TIMEFRAME_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
    "1w": 604_800_000,
}

_BLOFIN_BARS = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "4h": "4H",
    "1d": "1D",
    "1w": "1W",
}
_BYBIT_INTERVALS = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "4h": "240",
    "1d": "D",
    "1w": "W",
}
_OKX_CHANNELS = {
    "1m": "candle1m",
    "3m": "candle3m",
    "5m": "candle5m",
    "15m": "candle15m",
    "30m": "candle30m",
    "1h": "candle1H",
    "4h": "candle4H",
    "1d": "candle1D",
    "1w": "candle1W",
}

PROVIDER_DOCS = {
    "blofin": "https://docs.blofin.com/index.html",
    "bybit": "https://bybit-exchange.github.io/docs/v5/websocket/public/kline",
    "okx": "https://www.okx.com/docs-v5/en/",
}

PROVIDER_WS_URLS = {
    "blofin": "wss://openapi.blofin.com/ws/public",
    "bybit": "wss://stream.bybit.com/v5/public/linear",
    # OKX candlestick channels are served on the business WebSocket path.
    "okx": "wss://ws.okx.com:8443/ws/v5/business",
}


@dataclass(frozen=True)
class FeedBakeoffEvent:
    contract: str
    run_id: str
    provider: str
    symbol: str
    provider_symbol: str
    timeframe: str
    ts_open_ms: int | None
    ts_close_ms: int | None
    event_kind: str
    provider_event_ts_ms: int | None
    received_ts_ms: int
    close_latency_ms: int | None
    open: str | None
    high: str | None
    low: str | None
    close: str | None
    volume_base: str | None
    volume_quote: str | None
    raw_ref: str
    detail: str | None = None

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FeedBakeoffProvider:
    id: str
    ws_url: str
    docs_url: str

    def provider_symbol(self, symbol: str) -> str:
        canonical = str(symbol or "").strip().upper().replace("/", "-")
        if self.id == "bybit":
            return canonical.replace("-", "")
        return canonical

    def subscribe_payload(self, symbols: Sequence[str], timeframe: str) -> Mapping[str, Any]:
        tf = normalize_timeframe(timeframe)
        provider_symbols = [self.provider_symbol(symbol) for symbol in symbols]
        if self.id == "blofin":
            channel = f"candle{_BLOFIN_BARS[tf]}"
            return {"op": "subscribe", "args": [{"channel": channel, "instId": symbol} for symbol in provider_symbols]}
        if self.id == "bybit":
            interval = _BYBIT_INTERVALS[tf]
            return {"op": "subscribe", "args": [f"kline.{interval}.{symbol}" for symbol in provider_symbols]}
        if self.id == "okx":
            channel = _OKX_CHANNELS[tf]
            return {"op": "subscribe", "args": [{"channel": channel, "instId": symbol} for symbol in provider_symbols]}
        raise ValueError(f"unsupported bakeoff provider: {self.id}")

    def parse_payload(
        self,
        payload: Mapping[str, Any],
        *,
        run_id: str,
        symbols_by_provider: Mapping[str, str],
        timeframe: str,
        received_ts_ms: int,
        raw_ref: str,
    ) -> list[FeedBakeoffEvent]:
        if self.id == "blofin":
            return _parse_blofin_payload(payload, run_id=run_id, symbols_by_provider=symbols_by_provider, timeframe=timeframe, received_ts_ms=received_ts_ms, raw_ref=raw_ref)
        if self.id == "bybit":
            return _parse_bybit_payload(payload, run_id=run_id, symbols_by_provider=symbols_by_provider, timeframe=timeframe, received_ts_ms=received_ts_ms, raw_ref=raw_ref)
        if self.id == "okx":
            return _parse_okx_payload(payload, run_id=run_id, symbols_by_provider=symbols_by_provider, timeframe=timeframe, received_ts_ms=received_ts_ms, raw_ref=raw_ref)
        return []


def _now_ms() -> int:
    return int(time.time() * 1000)


def normalize_timeframe(timeframe: str) -> str:
    value = str(timeframe or "").strip().lower()
    if value not in TIMEFRAME_MS:
        raise ValueError(f"unsupported bakeoff timeframe: {timeframe}")
    return value


def enabled_provider_ids(config: Mapping[str, Any], *, include_disabled: bool = False) -> list[str]:
    providers = config.get("providers") or []
    ids: list[str] = []
    for row in providers:
        if not isinstance(row, Mapping):
            continue
        provider_id = str(row.get("id") or "").strip().lower()
        if not provider_id or provider_id == "paid_normalized_candidate":
            continue
        if include_disabled or bool(row.get("enabled_by_default")):
            ids.append(provider_id)
    return list(dict.fromkeys(ids))


def build_provider(provider_id: str, *, ws_url: str | None = None) -> FeedBakeoffProvider:
    normalized = str(provider_id or "").strip().lower()
    if normalized not in PROVIDER_WS_URLS:
        raise ValueError(f"unsupported bakeoff provider: {provider_id}")
    return FeedBakeoffProvider(
        id=normalized,
        ws_url=ws_url or PROVIDER_WS_URLS[normalized],
        docs_url=PROVIDER_DOCS[normalized],
    )


def _close_latency(received_ts_ms: int, ts_close_ms: int | None, event_kind: str) -> int | None:
    if event_kind != "closed_candle" or ts_close_ms is None:
        return None
    return int(received_ts_ms) - int(ts_close_ms)


def _event(
    *,
    run_id: str,
    provider: str,
    symbol: str,
    provider_symbol: str,
    timeframe: str,
    ts_open_ms: int | None,
    event_kind: str,
    provider_event_ts_ms: int | None,
    received_ts_ms: int,
    open: Any = None,
    high: Any = None,
    low: Any = None,
    close: Any = None,
    volume_base: Any = None,
    volume_quote: Any = None,
    raw_ref: str,
    detail: str | None = None,
) -> FeedBakeoffEvent:
    tf = normalize_timeframe(timeframe)
    ts_close_ms = int(ts_open_ms) + TIMEFRAME_MS[tf] if ts_open_ms is not None else None
    return FeedBakeoffEvent(
        contract=FEED_BAKEOFF_CLOSE_EVENT_CONTRACT,
        run_id=run_id,
        provider=provider,
        symbol=symbol,
        provider_symbol=provider_symbol,
        timeframe=tf,
        ts_open_ms=int(ts_open_ms) if ts_open_ms is not None else None,
        ts_close_ms=ts_close_ms,
        event_kind=event_kind,
        provider_event_ts_ms=int(provider_event_ts_ms) if provider_event_ts_ms is not None else None,
        received_ts_ms=int(received_ts_ms),
        close_latency_ms=_close_latency(int(received_ts_ms), ts_close_ms, event_kind),
        open=str(open) if open is not None else None,
        high=str(high) if high is not None else None,
        low=str(low) if low is not None else None,
        close=str(close) if close is not None else None,
        volume_base=str(volume_base) if volume_base is not None else None,
        volume_quote=str(volume_quote) if volume_quote is not None else None,
        raw_ref=raw_ref,
        detail=detail,
    )


def _canonical(symbols_by_provider: Mapping[str, str], provider_symbol: str) -> str:
    return symbols_by_provider.get(provider_symbol) or symbols_by_provider.get(provider_symbol.upper()) or provider_symbol


def _parse_blofin_payload(
    payload: Mapping[str, Any],
    *,
    run_id: str,
    symbols_by_provider: Mapping[str, str],
    timeframe: str,
    received_ts_ms: int,
    raw_ref: str,
) -> list[FeedBakeoffEvent]:
    arg = payload.get("arg") if isinstance(payload.get("arg"), Mapping) else {}
    channel = str(arg.get("channel") or "")
    if not channel.startswith("candle"):
        return []
    provider_symbol = str(arg.get("instId") or "")
    if not provider_symbol:
        return []
    rows = payload.get("data") or []
    if isinstance(rows, Mapping):
        rows = [rows]
    events: list[FeedBakeoffEvent] = []
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, Mapping):
            ts_open_ms = int(row.get("ts") or row.get("t") or row.get("ts_open_ms"))
            open_value = row.get("open") or row.get("o")
            high_value = row.get("high") or row.get("h")
            low_value = row.get("low") or row.get("l")
            close_value = row.get("close") or row.get("c")
            volume_base = row.get("volCurrency") or row.get("vol_base")
            volume_quote = row.get("volCurrencyQuote") or row.get("vol_quote")
            confirm = str(row.get("confirm") or "0")
        elif isinstance(row, (list, tuple)) and len(row) >= 6:
            ts_open_ms = int(row[0])
            open_value, high_value, low_value, close_value = row[1], row[2], row[3], row[4]
            volume_base = row[6] if len(row) > 6 else row[5]
            volume_quote = row[7] if len(row) > 7 else None
            confirm = str(row[8]) if len(row) > 8 else "0"
        else:
            continue
        events.append(
            _event(
                run_id=run_id,
                provider="blofin",
                symbol=_canonical(symbols_by_provider, provider_symbol),
                provider_symbol=provider_symbol,
                timeframe=timeframe,
                ts_open_ms=ts_open_ms,
                event_kind="closed_candle" if confirm == "1" else "working_candle",
                provider_event_ts_ms=None,
                received_ts_ms=received_ts_ms,
                open=open_value,
                high=high_value,
                low=low_value,
                close=close_value,
                volume_base=volume_base,
                volume_quote=volume_quote,
                raw_ref=raw_ref,
            )
        )
    return events


def _parse_bybit_payload(
    payload: Mapping[str, Any],
    *,
    run_id: str,
    symbols_by_provider: Mapping[str, str],
    timeframe: str,
    received_ts_ms: int,
    raw_ref: str,
) -> list[FeedBakeoffEvent]:
    topic = str(payload.get("topic") or "")
    if not topic.startswith("kline."):
        return []
    parts = topic.split(".")
    provider_symbol = parts[-1] if parts else ""
    provider_event_ts_ms = payload.get("ts")
    events: list[FeedBakeoffEvent] = []
    data = payload.get("data") or []
    for row in data if isinstance(data, list) else []:
        if not isinstance(row, Mapping):
            continue
        ts_open_ms = int(row.get("start"))
        events.append(
            _event(
                run_id=run_id,
                provider="bybit",
                symbol=_canonical(symbols_by_provider, provider_symbol),
                provider_symbol=provider_symbol,
                timeframe=timeframe,
                ts_open_ms=ts_open_ms,
                event_kind="closed_candle" if bool(row.get("confirm")) else "working_candle",
                provider_event_ts_ms=int(row.get("timestamp") or provider_event_ts_ms) if (row.get("timestamp") or provider_event_ts_ms) is not None else None,
                received_ts_ms=received_ts_ms,
                open=row.get("open"),
                high=row.get("high"),
                low=row.get("low"),
                close=row.get("close"),
                volume_base=row.get("volume"),
                volume_quote=row.get("turnover"),
                raw_ref=raw_ref,
            )
        )
    return events


def _parse_okx_payload(
    payload: Mapping[str, Any],
    *,
    run_id: str,
    symbols_by_provider: Mapping[str, str],
    timeframe: str,
    received_ts_ms: int,
    raw_ref: str,
) -> list[FeedBakeoffEvent]:
    arg = payload.get("arg") if isinstance(payload.get("arg"), Mapping) else {}
    channel = str(arg.get("channel") or "")
    if not channel.startswith("candle"):
        return []
    provider_symbol = str(arg.get("instId") or "")
    data = payload.get("data") or []
    events: list[FeedBakeoffEvent] = []
    for row in data if isinstance(data, list) else []:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        ts_open_ms = int(row[0])
        confirm = str(row[8]) if len(row) > 8 else "0"
        events.append(
            _event(
                run_id=run_id,
                provider="okx",
                symbol=_canonical(symbols_by_provider, provider_symbol),
                provider_symbol=provider_symbol,
                timeframe=timeframe,
                ts_open_ms=ts_open_ms,
                event_kind="closed_candle" if confirm == "1" else "working_candle",
                provider_event_ts_ms=None,
                received_ts_ms=received_ts_ms,
                open=row[1],
                high=row[2],
                low=row[3],
                close=row[4],
                volume_base=row[6] if len(row) > 6 else row[5],
                volume_quote=row[7] if len(row) > 7 else None,
                raw_ref=raw_ref,
            )
        )
    return events


class JsonlWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a", encoding="utf-8")
        self.count = 0

    def write(self, payload: Mapping[str, Any]) -> str:
        self.count += 1
        ref = f"{self.path.name}:{self.count}"
        self._handle.write(json.dumps(dict(payload), sort_keys=True) + "\n")
        self._handle.flush()
        return ref

    def close(self) -> None:
        self._handle.close()


def _percentile(sorted_values: Sequence[int], percentile: float) -> int | None:
    if not sorted_values:
        return None
    index = min(len(sorted_values) - 1, max(0, round((len(sorted_values) - 1) * percentile)))
    return int(sorted_values[index])


def _event_identity_values(event: FeedBakeoffEvent) -> tuple[str | None, str | None, str | None, str | None, str | None, str | None]:
    return (event.open, event.high, event.low, event.close, event.volume_base, event.volume_quote)


def _summarize_events(events: Sequence[FeedBakeoffEvent], *, provider_id: str, symbols: Sequence[str], target_closes_per_symbol: int) -> dict[str, Any]:
    by_symbol: dict[str, dict[str, Any]] = {symbol: {"closed_candles": 0, "working_candles": 0, "errors": 0, "latencies_ms": []} for symbol in symbols}
    seen_closes: dict[tuple[str, int], FeedBakeoffEvent] = {}
    duplicates = 0
    conflicts = 0
    unique_closed = 0
    for event in events:
        bucket = by_symbol.setdefault(event.symbol, {"closed_candles": 0, "working_candles": 0, "errors": 0, "latencies_ms": []})
        if event.event_kind == "closed_candle":
            bucket["closed_candles"] += 1
            if event.close_latency_ms is not None:
                bucket["latencies_ms"].append(event.close_latency_ms)
            key = (event.symbol, int(event.ts_open_ms or -1))
            previous = seen_closes.get(key)
            if previous:
                duplicates += 1
                if _event_identity_values(previous) != _event_identity_values(event):
                    conflicts += 1
            else:
                seen_closes[key] = event
                unique_closed += 1
        elif event.event_kind == "working_candle":
            bucket["working_candles"] += 1
        elif event.event_kind == "error":
            bucket["errors"] += 1
    for bucket in by_symbol.values():
        latencies = sorted(int(value) for value in bucket.pop("latencies_ms"))
        if latencies:
            bucket["close_latency_ms"] = {
                "p50": latencies[len(latencies) // 2],
                "p95": _percentile(latencies, 0.95),
                "max": latencies[-1],
            }
        else:
            bucket["close_latency_ms"] = None
    passed = all(row["closed_candles"] >= target_closes_per_symbol for row in by_symbol.values()) and conflicts == 0
    return {
        "provider": provider_id,
        "symbols": by_symbol,
        "unique_closed_candles": unique_closed,
        "duplicates": duplicates,
        "conflicts": conflicts,
        "passed_target_closes": passed,
    }


def load_events_jsonl(path: Path) -> list[FeedBakeoffEvent]:
    events: list[FeedBakeoffEvent] = []
    if not path.exists():
        return events
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, Mapping):
                continue
            events.append(FeedBakeoffEvent(**row))
    return events


def summarize_provider_event_file(
    *,
    provider_id: str,
    event_path: Path,
    symbols: Sequence[str],
    timeframe: str,
    target_closes_per_symbol: int,
    started_ms: int | None = None,
    as_of_ms: int | None = None,
) -> dict[str, Any]:
    events = load_events_jsonl(event_path)
    summary = _summarize_events(events, provider_id=provider_id, symbols=symbols, target_closes_per_symbol=target_closes_per_symbol)
    tf_ms = TIMEFRAME_MS[normalize_timeframe(timeframe)]
    expected_by_runtime = None
    if started_ms is not None and as_of_ms is not None and as_of_ms > started_ms:
        # One interval is subtracted so an in-progress candle is not counted as missing.
        expected_by_runtime = max(0, int((as_of_ms - started_ms - tf_ms) // tf_ms))
    for symbol, row in summary["symbols"].items():
        closes = sorted({int(event.ts_open_ms) for event in events if event.event_kind == "closed_candle" and event.symbol == symbol and event.ts_open_ms is not None})
        missing_between_observed = 0
        if len(closes) >= 2:
            expected = set(range(closes[0], closes[-1] + tf_ms, tf_ms))
            missing_between_observed = len(expected.difference(closes))
        row["first_close_open_ms"] = closes[0] if closes else None
        row["last_close_open_ms"] = closes[-1] if closes else None
        row["missing_between_observed"] = missing_between_observed
        if expected_by_runtime is not None:
            row["expected_closed_by_runtime"] = expected_by_runtime
            row["runtime_shortfall"] = max(0, expected_by_runtime - int(row.get("closed_candles", 0)))
    summary["timeframe"] = normalize_timeframe(timeframe)
    summary["event_count"] = len(events)
    summary["expected_closed_by_runtime"] = expected_by_runtime
    return summary


async def run_provider_smoke(
    provider: FeedBakeoffProvider,
    *,
    run_id: str,
    symbols: Sequence[str],
    timeframe: str,
    duration_seconds: int,
    target_closes_per_symbol: int,
    artifact_dir: Path,
    session_timeout_seconds: float = 30.0,
    idle_reconnect_seconds: float | None = None,
    max_reconnects: int | None = None,
    reconnect_base_delay_seconds: float = 1.0,
    reconnect_max_delay_seconds: float = 60.0,
    stop_on_target: bool = True,
    summary_contract: str = FEED_BAKEOFF_SMOKE_SUMMARY_CONTRACT,
    summary_filename: str = "phase_a_smoke.json",
) -> dict[str, Any]:
    tf = normalize_timeframe(timeframe)
    normalized_symbols = [str(symbol).strip().upper().replace("/", "-") for symbol in symbols if str(symbol).strip()]
    symbols_by_provider = {provider.provider_symbol(symbol): symbol for symbol in normalized_symbols}
    provider_dir = artifact_dir / provider.id
    events_path = provider_dir / "close_events.jsonl"
    raw_path = provider_dir / "raw_messages.jsonl"
    event_writer = JsonlWriter(events_path)
    raw_writer = JsonlWriter(raw_path)
    events: list[FeedBakeoffEvent] = []
    started_ms = _now_ms()
    deadline = time.monotonic() + int(duration_seconds)
    status = "completed"
    error_detail: str | None = None
    connection_attempts = 0
    reconnects = 0
    closed_reconnects = 0
    idle_reconnects = 0
    error_reconnects = 0
    target_met = False
    last_reconnect_reason: str | None = None
    idle_threshold = float(idle_reconnect_seconds if idle_reconnect_seconds is not None else max(session_timeout_seconds * 3, TIMEFRAME_MS[tf] / 1000 / 2))

    def _write_runtime_error(detail: str) -> None:
        event = _event(
            run_id=run_id,
            provider=provider.id,
            symbol="*",
            provider_symbol="*",
            timeframe=tf,
            ts_open_ms=None,
            event_kind="error",
            provider_event_ts_ms=None,
            received_ts_ms=_now_ms(),
            raw_ref="runtime",
            detail=detail,
        )
        events.append(event)
        event_writer.write(event.to_json())

    try:
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=session_timeout_seconds, sock_read=session_timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while time.monotonic() < deadline and not target_met:
                connection_attempts += 1
                reconnect_reason: str | None = None
                try:
                    async with session.ws_connect(provider.ws_url, heartbeat=20) as ws:
                        last_message_monotonic = time.monotonic()
                        raw_writer.write(
                            {
                                "attempt": connection_attempts,
                                "event_kind": "subscribe_sent",
                                "payload": provider.subscribe_payload(normalized_symbols, tf),
                                "received_ts_ms": _now_ms(),
                            }
                        )
                        await ws.send_json(provider.subscribe_payload(normalized_symbols, tf))
                        while time.monotonic() < deadline:
                            remaining = max(0.1, deadline - time.monotonic())
                            try:
                                message = await ws.receive(timeout=min(session_timeout_seconds, remaining))
                            except (TimeoutError, asyncio.TimeoutError):
                                if time.monotonic() >= deadline:
                                    break
                                idle_seconds = time.monotonic() - last_message_monotonic
                                if idle_seconds >= idle_threshold:
                                    reconnect_reason = f"idle_timeout_{round(idle_seconds, 1)}s"
                                    idle_reconnects += 1
                                    raw_writer.write(
                                        {
                                            "attempt": connection_attempts,
                                            "event_kind": "idle_reconnect",
                                            "idle_seconds": round(idle_seconds, 3),
                                            "idle_threshold_seconds": idle_threshold,
                                            "received_ts_ms": _now_ms(),
                                        }
                                    )
                                    break
                                continue
                            received_ts_ms = _now_ms()
                            if message.type == aiohttp.WSMsgType.TEXT:
                                last_message_monotonic = time.monotonic()
                                text = str(message.data)
                                raw_ref = raw_writer.write({"attempt": connection_attempts, "received_ts_ms": received_ts_ms, "message": text})
                                if text == "pong":
                                    continue
                                try:
                                    payload = json.loads(text)
                                except json.JSONDecodeError:
                                    continue
                                parsed = provider.parse_payload(payload, run_id=run_id, symbols_by_provider=symbols_by_provider, timeframe=tf, received_ts_ms=received_ts_ms, raw_ref=raw_ref)
                                for event in parsed:
                                    events.append(event)
                                    event_writer.write(event.to_json())
                                summary_so_far = _summarize_events(events, provider_id=provider.id, symbols=normalized_symbols, target_closes_per_symbol=target_closes_per_symbol)
                                if stop_on_target and summary_so_far["passed_target_closes"]:
                                    target_met = True
                                    break
                            elif message.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR}:
                                reconnect_reason = str(ws.exception() or message.extra or message.data or message.type)
                                closed_reconnects += 1
                                raw_writer.write(
                                    {
                                        "attempt": connection_attempts,
                                        "event_kind": "connection_closed",
                                        "reason": reconnect_reason,
                                        "received_ts_ms": received_ts_ms,
                                    }
                                )
                                break
                except Exception as exc:  # noqa: BLE001 - artifact must record provider failures.
                    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)) and time.monotonic() >= deadline:
                        break
                    reconnect_reason = f"{type(exc).__name__}: {exc}"
                    error_reconnects += 1
                    _write_runtime_error(reconnect_reason)

                if target_met or time.monotonic() >= deadline:
                    break
                if reconnect_reason is None:
                    continue
                last_reconnect_reason = reconnect_reason
                if max_reconnects is not None and reconnects >= max_reconnects:
                    status = "failed"
                    error_detail = f"max_reconnects_exceeded after {reconnects} reconnects; last_reason={last_reconnect_reason}"
                    _write_runtime_error(error_detail)
                    break
                reconnects += 1
                delay_seconds = min(float(reconnect_max_delay_seconds), float(reconnect_base_delay_seconds) * (2 ** min(reconnects - 1, 6)))
                raw_writer.write(
                    {
                        "attempt": connection_attempts,
                        "delay_seconds": delay_seconds,
                        "event_kind": "reconnect_scheduled",
                        "reason": reconnect_reason,
                        "reconnects": reconnects,
                        "received_ts_ms": _now_ms(),
                    }
                )
                await asyncio.sleep(min(delay_seconds, max(0.0, deadline - time.monotonic())))
    except Exception as exc:  # noqa: BLE001 - artifact must record provider failures.
        if isinstance(exc, (TimeoutError, asyncio.TimeoutError)) and time.monotonic() >= deadline:
            status = "completed"
            error_detail = None
        else:
            status = "failed"
            error_detail = f"{type(exc).__name__}: {exc}"
            _write_runtime_error(error_detail)
    finally:
        event_writer.close()
        raw_writer.close()

    ended_ms = _now_ms()
    summary = {
        "contract": summary_contract,
        "run_id": run_id,
        "provider": provider.id,
        "docs_url": provider.docs_url,
        "ws_url": provider.ws_url,
        "timeframe": tf,
        "symbols": normalized_symbols,
        "target_closes_per_symbol": int(target_closes_per_symbol),
        "duration_seconds_requested": int(duration_seconds),
        "started_ms": started_ms,
        "ended_ms": ended_ms,
        "elapsed_seconds": round((ended_ms - started_ms) / 1000, 3),
        "status": status,
        "error_detail": error_detail,
        "connection_attempts": connection_attempts,
        "reconnects": reconnects,
        "closed_reconnects": closed_reconnects,
        "idle_reconnects": idle_reconnects,
        "error_reconnects": error_reconnects,
        "last_reconnect_reason": last_reconnect_reason,
        "idle_reconnect_seconds": idle_threshold,
        "event_path": str(events_path),
        "raw_path": str(raw_path),
        "summary": _summarize_events(events, provider_id=provider.id, symbols=normalized_symbols, target_closes_per_symbol=target_closes_per_symbol),
    }
    summary_path = provider_dir / summary_filename
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


async def run_smoke(
    *,
    run_id: str,
    provider_ids: Sequence[str],
    symbols: Sequence[str],
    timeframe: str,
    duration_seconds: int,
    target_closes_per_symbol: int,
    artifact_root: Path,
    stop_on_target: bool = True,
    combined_contract: str = "feed_bakeoff_phase_a_smoke_combined_v1",
    combined_filename: str = "phase_a_smoke_combined.json",
    provider_summary_contract: str = FEED_BAKEOFF_SMOKE_SUMMARY_CONTRACT,
    provider_summary_filename: str = "phase_a_smoke.json",
) -> dict[str, Any]:
    artifact_dir = artifact_root / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    providers = [build_provider(provider_id) for provider_id in provider_ids]
    results = await asyncio.gather(
        *[
            run_provider_smoke(
                provider,
                run_id=run_id,
                symbols=symbols,
                timeframe=timeframe,
                duration_seconds=duration_seconds,
                target_closes_per_symbol=target_closes_per_symbol,
                artifact_dir=artifact_dir,
                stop_on_target=stop_on_target,
                summary_contract=provider_summary_contract,
                summary_filename=provider_summary_filename,
            )
            for provider in providers
        ]
    )
    status = "pass" if all(result["summary"]["passed_target_closes"] and result["status"] == "completed" for result in results) else "needs_review"
    combined = {
        "contract": combined_contract,
        "run_id": run_id,
        "artifact_dir": str(artifact_dir),
        "status": status,
        "providers": results,
    }
    (artifact_dir / combined_filename).write_text(json.dumps(combined, indent=2, sort_keys=True), encoding="utf-8")
    return combined


def render_smoke_markdown(combined: Mapping[str, Any]) -> str:
    lines = [f"# Feed bakeoff Phase A smoke — {combined.get('run_id')}", "", f"Status: `{combined.get('status')}`", ""]
    for result in combined.get("providers") or []:
        if not isinstance(result, Mapping):
            continue
        lines.append(f"## {result.get('provider')}")
        lines.append(f"- provider status: `{result.get('status')}`")
        if result.get("error_detail"):
            lines.append(f"- error: `{result.get('error_detail')}`")
        lines.append(f"- events: `{result.get('event_path')}`")
        summary = result.get("summary") if isinstance(result.get("summary"), Mapping) else {}
        lines.append(f"- duplicates/conflicts: `{summary.get('duplicates', 0)}` / `{summary.get('conflicts', 0)}`")
        symbols = summary.get("symbols") if isinstance(summary.get("symbols"), Mapping) else {}
        for symbol, row in symbols.items():
            if not isinstance(row, Mapping):
                continue
            lines.append(
                f"  - {symbol}: closed={row.get('closed_candles', 0)} working={row.get('working_candles', 0)} latency={row.get('close_latency_ms')}"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
