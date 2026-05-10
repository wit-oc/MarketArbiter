from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterator, Mapping, Sequence

import aiohttp
import requests

from market_arbiter.core.market_data import CandleDTO, upsert_market_candles


BLOFIN_REST_DOC_LIMIT = 1440
BLOFIN_REST_SAFE_LIMIT = 500
BLOFIN_REST_SAFE_REQUESTS_PER_MINUTE = 120
BLOFIN_FIREWALL_TEMP_BAN_SECONDS = 5 * 60
BLOFIN_WS_CHANNEL_CANDLE_5M = "candle5m"
BLOFIN_WS_PROVIDER_ID = "blofin_ws"
BLOFIN_WS_CANDLE_5M_DATASET_VERSION = "blofin_5m_v1"
BLOFIN_WS_CANDLE_5M_RAW_CONTRACT = "blofin_ws_candle_5m_raw_v1"


class BlofinRestError(RuntimeError):
    pass


class BlofinRateLimitError(BlofinRestError):
    pass


class BlofinFirewallBanError(BlofinRestError):
    pass


class BlofinWebSocketError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        message_type: str | None = None,
        close_code: int | None = None,
        ws_exception: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message_type = message_type
        self.close_code = close_code
        self.ws_exception = ws_exception


class BlofinEnvironment(str, Enum):
    DEMO = "demo"
    PROD = "prod"

    @property
    def rest_base_url(self) -> str:
        if self is BlofinEnvironment.DEMO:
            return "https://demo-trading-openapi.blofin.com"
        return "https://openapi.blofin.com"

    @property
    def ws_public_url(self) -> str:
        if self is BlofinEnvironment.DEMO:
            return "wss://demo-trading-openapi.blofin.com/ws/public"
        return "wss://openapi.blofin.com/ws/public"


_BAR_TO_TIMEFRAME = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "1H": "1h",
    "4H": "4h",
    "1D": "1d",
    "1W": "1w",
}

_TIMEFRAME_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
    "1w": 604_800_000,
}


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class BlofinRestGovernor:
    requests_per_minute: int = BLOFIN_REST_SAFE_REQUESTS_PER_MINUTE
    firewall_ban_backoff_seconds: int = BLOFIN_FIREWALL_TEMP_BAN_SECONDS
    sleep_fn: Any = time.sleep
    _request_timestamps: list[float] = field(default_factory=list)
    _banned_until_ts: float = 0.0

    def acquire(self) -> None:
        now = time.time()
        if self._banned_until_ts > now:
            self.sleep_fn(max(0.0, self._banned_until_ts - now))
            now = time.time()
        window_start = now - 60.0
        self._request_timestamps = [ts for ts in self._request_timestamps if ts >= window_start]
        if len(self._request_timestamps) >= self.requests_per_minute:
            sleep_for = max(0.0, self._request_timestamps[0] + 60.0 - now)
            if sleep_for > 0:
                self.sleep_fn(sleep_for)
            now = time.time()
            window_start = now - 60.0
            self._request_timestamps = [ts for ts in self._request_timestamps if ts >= window_start]
        self._request_timestamps.append(time.time())

    def note_429(self) -> None:
        now = time.time()
        self._request_timestamps = [ts for ts in self._request_timestamps if now - ts <= 60.0]
        if self._request_timestamps:
            self._banned_until_ts = max(self._banned_until_ts, self._request_timestamps[0] + 60.0)

    def note_403(self) -> None:
        self._banned_until_ts = max(self._banned_until_ts, time.time() + self.firewall_ban_backoff_seconds)


def _safe_limit(limit: int) -> int:
    return max(1, min(int(limit), BLOFIN_REST_SAFE_LIMIT))


def _normalize_bar(bar: str) -> str:
    token = str(bar or "").strip()
    if token in _BAR_TO_TIMEFRAME:
        return token
    upper = token.upper()
    if upper in _BAR_TO_TIMEFRAME:
        return upper
    lower = token.lower()
    if lower in _BAR_TO_TIMEFRAME:
        return lower
    raise ValueError(f"unsupported BloFin bar: {bar}")


def _timeframe_for_bar(bar: str) -> str:
    normalized = _normalize_bar(bar)
    return _BAR_TO_TIMEFRAME[normalized]


def _bar_ms(bar: str) -> int:
    timeframe = _timeframe_for_bar(bar)
    return _TIMEFRAME_MS[timeframe]


def _normalize_candle_row(*, inst_id: str, bar: str, row: Sequence[Any], trace_id: str, dataset_version: str) -> CandleDTO:
    if len(row) < 6:
        raise ValueError(f"unexpected BloFin candle row length: {row}")
    ts_open_ms = int(row[0])
    timeframe = _timeframe_for_bar(bar)
    return CandleDTO(
        provider_id="blofin_rest",
        venue="blofin",
        symbol=str(inst_id),
        timeframe=timeframe,
        ts_open_ms=ts_open_ms,
        ts_close_ms=ts_open_ms + _bar_ms(bar),
        open=str(row[1]),
        high=str(row[2]),
        low=str(row[3]),
        close=str(row[4]),
        volume=str(row[5]),
        dataset_version=dataset_version,
        trace_id=trace_id,
    )


def _string_at(row: Sequence[Any], index: int, default: str = "") -> str:
    if index >= len(row):
        return default
    return str(row[index])


@dataclass(frozen=True)
class BlofinWsCandle5mRaw:
    contract: str
    venue: str
    inst_id: str
    channel: str
    timeframe: str
    ts_open_ms: int
    ts_close_ms: int
    open: str
    high: str
    low: str
    close: str
    vol_contract: str
    vol_base: str
    vol_quote: str
    confirm: str
    exchange_event_ts_ms: int | None
    received_ts_ms: int
    ingest_trace_id: str

    @property
    def is_confirmed(self) -> bool:
        return self.confirm == "1"

    def to_candle_dto(self) -> CandleDTO:
        return CandleDTO(
            provider_id=BLOFIN_WS_PROVIDER_ID,
            venue="blofin",
            symbol=self.inst_id,
            timeframe="5m",
            ts_open_ms=self.ts_open_ms,
            ts_close_ms=self.ts_close_ms,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.vol_contract,
            dataset_version=BLOFIN_WS_CANDLE_5M_DATASET_VERSION,
            trace_id=self.ingest_trace_id,
        )


def _normalize_ws_candle_row(
    *,
    inst_id: str,
    row: Sequence[Any] | Mapping[str, Any],
    received_ts_ms: int,
    trace_id: str,
) -> BlofinWsCandle5mRaw:
    if isinstance(row, Mapping):
        ts_open_ms = int(row.get("ts") or row.get("t") or row.get("ts_open_ms"))
        open_value = str(row.get("open") or row.get("o"))
        high_value = str(row.get("high") or row.get("h"))
        low_value = str(row.get("low") or row.get("l"))
        close_value = str(row.get("close") or row.get("c"))
        vol_contract = str(row.get("vol") or row.get("vol_contract") or row.get("volume") or "0")
        vol_base = str(row.get("volCurrency") or row.get("vol_base") or "")
        vol_quote = str(row.get("volCurrencyQuote") or row.get("vol_quote") or "")
        confirm = str(row.get("confirm") or "0")
    else:
        if len(row) < 6:
            raise ValueError(f"unexpected BloFin websocket candle row length: {row}")
        ts_open_ms = int(row[0])
        open_value = _string_at(row, 1)
        high_value = _string_at(row, 2)
        low_value = _string_at(row, 3)
        close_value = _string_at(row, 4)
        vol_contract = _string_at(row, 5, "0")
        vol_base = _string_at(row, 6)
        vol_quote = _string_at(row, 7)
        confirm = _string_at(row, 8, "0")

    return BlofinWsCandle5mRaw(
        contract=BLOFIN_WS_CANDLE_5M_RAW_CONTRACT,
        venue="blofin",
        inst_id=str(inst_id),
        channel=BLOFIN_WS_CHANNEL_CANDLE_5M,
        timeframe="5m",
        ts_open_ms=ts_open_ms,
        ts_close_ms=ts_open_ms + _TIMEFRAME_MS["5m"],
        open=open_value,
        high=high_value,
        low=low_value,
        close=close_value,
        vol_contract=vol_contract,
        vol_base=vol_base,
        vol_quote=vol_quote,
        confirm=confirm,
        exchange_event_ts_ms=None,
        received_ts_ms=int(received_ts_ms),
        ingest_trace_id=trace_id,
    )


def parse_ws_candle_5m_payload(
    payload: Mapping[str, Any],
    *,
    received_ts_ms: int | None = None,
    trace_id: str | None = None,
) -> list[BlofinWsCandle5mRaw]:
    arg = payload.get("arg") if isinstance(payload.get("arg"), Mapping) else {}
    channel = str(arg.get("channel") or payload.get("channel") or "")
    if channel and channel != BLOFIN_WS_CHANNEL_CANDLE_5M:
        return []

    inst_id = str(arg.get("instId") or payload.get("instId") or "")
    raw_data = payload.get("data") or []
    if isinstance(raw_data, Mapping):
        data = [raw_data]
    elif isinstance(raw_data, list):
        data = raw_data
    else:
        data = []
    if not inst_id:
        return []

    resolved_received = int(received_ts_ms if received_ts_ms is not None else _now_ms())
    resolved_trace = trace_id or f"blofin-ws:{inst_id}:5m:{resolved_received}"
    return [
        _normalize_ws_candle_row(
            inst_id=inst_id,
            row=row,
            received_ts_ms=resolved_received,
            trace_id=resolved_trace,
        )
        for row in data
        if isinstance(row, (list, tuple, dict))
    ]


def confirmed_ws_candle_5m_dtos(
    payload: Mapping[str, Any],
    *,
    received_ts_ms: int | None = None,
    trace_id: str | None = None,
) -> list[CandleDTO]:
    return [
        row.to_candle_dto()
        for row in parse_ws_candle_5m_payload(payload, received_ts_ms=received_ts_ms, trace_id=trace_id)
        if row.is_confirmed
    ]


@dataclass(frozen=True)
class BlofinCandlePage:
    inst_id: str
    bar: str
    candles: list[CandleDTO]
    raw_rows: list[list[Any]]

    @property
    def oldest_open_ms(self) -> int | None:
        return min((row.ts_open_ms for row in self.candles), default=None)

    @property
    def newest_open_ms(self) -> int | None:
        return max((row.ts_open_ms for row in self.candles), default=None)


class BlofinPublicRestClient:
    def __init__(
        self,
        *,
        environment: BlofinEnvironment = BlofinEnvironment.DEMO,
        session: requests.Session | None = None,
        requests_per_minute: int = BLOFIN_REST_SAFE_REQUESTS_PER_MINUTE,
        user_agent: str = "MarketArbiter/0.1 blofin-rest",
        timeout_seconds: float = 20.0,
        sleep_fn=time.sleep,
    ) -> None:
        self.environment = environment
        self.base_url = environment.rest_base_url.rstrip("/")
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": user_agent, "Accept": "application/json"})
        self.timeout_seconds = float(timeout_seconds)
        self.sleep_fn = sleep_fn
        self.governor = BlofinRestGovernor(
            requests_per_minute=max(1, int(requests_per_minute)),
            sleep_fn=sleep_fn,
        )

    def _get(self, path: str, *, params: Mapping[str, Any]) -> dict[str, Any]:
        self.governor.acquire()
        url = f"{self.base_url}{path}"
        response = self._session.get(url, params=params, timeout=self.timeout_seconds)
        if response.status_code == 429:
            self.governor.note_429()
            raise BlofinRateLimitError(f"BloFin rate limit hit for {path}")
        if response.status_code == 403:
            self.governor.note_403()
            raise BlofinFirewallBanError(f"BloFin firewall restriction hit for {path}")
        if response.status_code >= 400:
            raise BlofinRestError(f"BloFin REST error {response.status_code} for {path}: {response.text[:200]}")
        payload = response.json()
        if str(payload.get("code", "0")) not in {"0", "", "None"}:
            raise BlofinRestError(f"BloFin API returned code={payload.get('code')} msg={payload.get('msg')}")
        return payload

    def fetch_instruments(self) -> list[dict[str, Any]]:
        payload = self._get("/api/v1/market/instruments", params={})
        data = payload.get("data") or []
        return [row for row in data if isinstance(row, dict)]

    def fetch_candles_page(
        self,
        *,
        inst_id: str,
        bar: str,
        limit: int,
        after: int | None = None,
        before: int | None = None,
        trace_id: str | None = None,
    ) -> BlofinCandlePage:
        normalized_bar = _normalize_bar(bar)
        params: dict[str, Any] = {
            "instId": inst_id,
            "bar": normalized_bar,
            "limit": _safe_limit(limit),
        }
        if after is not None:
            params["after"] = int(after)
        if before is not None:
            params["before"] = int(before)
        payload = self._get("/api/v1/market/candles", params=params)
        raw_rows = [list(row) for row in (payload.get("data") or []) if isinstance(row, (list, tuple))]
        dataset_version = f"blofin_rest_{_timeframe_for_bar(normalized_bar)}_v1"
        resolved_trace = trace_id or f"blofin-rest:{inst_id}:{normalized_bar}:{_now_ms()}"
        candles = [
            _normalize_candle_row(inst_id=inst_id, bar=normalized_bar, row=row, trace_id=resolved_trace, dataset_version=dataset_version)
            for row in raw_rows
        ]
        return BlofinCandlePage(inst_id=inst_id, bar=normalized_bar, candles=candles, raw_rows=raw_rows)

    def iter_history(
        self,
        *,
        inst_id: str,
        bar: str,
        max_candles: int,
        trace_id: str | None = None,
    ) -> Iterator[CandleDTO]:
        normalized_bar = _normalize_bar(bar)
        remaining = max(0, int(max_candles))
        after: int | None = None
        yielded: set[int] = set()
        while remaining > 0:
            page = self.fetch_candles_page(
                inst_id=inst_id,
                bar=normalized_bar,
                limit=min(remaining, BLOFIN_REST_SAFE_LIMIT),
                after=after,
                trace_id=trace_id,
            )
            if not page.candles:
                break
            page_rows = sorted(page.candles, key=lambda row: row.ts_open_ms)
            for candle in page_rows:
                if candle.ts_open_ms in yielded:
                    continue
                yielded.add(candle.ts_open_ms)
                yield candle
                remaining -= 1
                if remaining <= 0:
                    break
            oldest = page.oldest_open_ms
            if oldest is None:
                break
            next_after = oldest - 1
            if after is not None and next_after >= after:
                break
            after = next_after

    def close(self) -> None:
        self._session.close()


class BlofinPublicWsClient:
    def __init__(
        self,
        *,
        environment: BlofinEnvironment = BlofinEnvironment.DEMO,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.environment = environment
        self.url = environment.ws_public_url
        self._session = session
        self._owns_session = session is None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._last_message_type: str | None = None
        self._last_close_code: int | None = None
        self._last_exception: str | None = None

    async def connect(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(headers={"User-Agent": "MarketArbiter/0.1 blofin-ws"})
        self._ws = await self._session.ws_connect(self.url, heartbeat=25)

    async def subscribe_candle_5m(self, symbols: Sequence[str]) -> None:
        if self._ws is None:
            raise RuntimeError("websocket not connected")
        args = [{"channel": BLOFIN_WS_CHANNEL_CANDLE_5M, "instId": symbol} for symbol in symbols]
        await self._ws.send_json({"op": "subscribe", "args": args})

    async def recv(self) -> dict[str, Any]:
        if self._ws is None:
            raise RuntimeError("websocket not connected")
        msg = await self._ws.receive()
        self._last_message_type = str(msg.type)
        if msg.type == aiohttp.WSMsgType.TEXT:
            payload = json.loads(msg.data)
            return payload if isinstance(payload, dict) else {"raw": payload}
        if msg.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED}:
            self._last_close_code = self._ws.close_code
            exception = self._ws.exception()
            self._last_exception = str(exception) if exception else None
            raise BlofinWebSocketError(
                "websocket closed",
                message_type=str(msg.type),
                close_code=self._last_close_code,
                ws_exception=self._last_exception,
            )
        if msg.type == aiohttp.WSMsgType.ERROR:
            self._last_close_code = self._ws.close_code
            exception = self._ws.exception()
            self._last_exception = str(exception) if exception else None
            raise BlofinWebSocketError(
                "websocket error",
                message_type=str(msg.type),
                close_code=self._last_close_code,
                ws_exception=self._last_exception,
            )
        return {"type": str(msg.type), "data": getattr(msg, 'data', None)}

    def diagnostic_state(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "environment": self.environment.value,
            "last_message_type": self._last_message_type,
            "close_code": self._last_close_code if self._last_close_code is not None else (self._ws.close_code if self._ws is not None else None),
            "ws_exception": self._last_exception,
        }

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None


class BlofinWsCandle5mIngestor:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def ingest_payload(
        self,
        payload: Mapping[str, Any],
        *,
        received_ts_ms: int | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        resolved_received = int(received_ts_ms if received_ts_ms is not None else _now_ms())
        rows = parse_ws_candle_5m_payload(payload, received_ts_ms=resolved_received, trace_id=trace_id)
        candles = [row.to_candle_dto() for row in rows if row.is_confirmed]
        if not candles:
            return {
                "provider_id": BLOFIN_WS_PROVIDER_ID,
                "venue": "blofin",
                "timeframe": "5m",
                "received": len(rows),
                "confirmed": 0,
                "inserted": 0,
                "idempotent": 0,
                "trace_id": trace_id,
            }

        with self.conn:
            summary = upsert_market_candles(self.conn, candles, ingest_ts_ms=resolved_received)
            for symbol in sorted({row.symbol for row in candles}):
                symbol_candles = [row for row in candles if row.symbol == symbol]
                newest = max(row.ts_open_ms for row in symbol_candles)
                resolved_trace = symbol_candles[-1].trace_id
                self._write_checkpoint(
                    symbol=symbol,
                    last_ts_open_ms=newest,
                    now_ms=resolved_received,
                    trace_id=resolved_trace,
                )
                self._write_health_event(
                    symbol=symbol,
                    now_ms=resolved_received,
                    trace_id=resolved_trace,
                )

        return {
            "provider_id": BLOFIN_WS_PROVIDER_ID,
            "venue": "blofin",
            "timeframe": "5m",
            "received": len(rows),
            "confirmed": len(candles),
            "inserted": summary["inserted"],
            "idempotent": summary["idempotent"],
            "trace_id": trace_id or (candles[-1].trace_id if candles else None),
        }

    def _write_checkpoint(self, *, symbol: str, last_ts_open_ms: int, now_ms: int, trace_id: str) -> None:
        self.conn.execute(
            """
            INSERT INTO feed_checkpoints(
                provider_id, venue, symbol, timeframe,
                last_ts_open_ms, last_success_ms, last_attempt_ms,
                failure_count, state, last_reason_code, trace_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                BLOFIN_WS_PROVIDER_ID,
                "blofin",
                symbol,
                "5m",
                int(last_ts_open_ms),
                int(now_ms),
                int(now_ms),
                0,
                "ok",
                None,
                trace_id,
            ),
        )

    def _write_health_event(self, *, symbol: str, now_ms: int, trace_id: str) -> None:
        self.conn.execute(
            """
            INSERT INTO feed_health_events(
                provider_id, venue, symbol, timeframe,
                state, reason_codes_json, as_of_ms, trace_id, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                BLOFIN_WS_PROVIDER_ID,
                "blofin",
                symbol,
                "5m",
                "ok",
                json.dumps(["ws_confirmed_candle"], sort_keys=True),
                int(now_ms),
                trace_id,
                None,
            ),
        )


async def _demo() -> None:  # pragma: no cover
    client = BlofinPublicWsClient()
    await client.connect()
    try:
        await client.subscribe_candle_5m(["BTC-USDT"])
        while True:
            print(await client.recv())
    finally:
        await client.close()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(_demo())
