from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass

import ccxt

from market_arbiter.core.db import init_db
from market_arbiter.core.market_data import CandleDTO, ProviderHealth, upsert_market_candles
from market_arbiter.core.market_scheduler import (
    ProviderRateLimitError,
    ProviderTimeoutError,
    ProviderUnavailableError,
    ProviderUpstreamError,
)


_TIMEFRAME_SECONDS = {"5m": 5 * 60, "4h": 4 * 60 * 60, "1d": 24 * 60 * 60, "1w": 7 * 24 * 60 * 60}
_DEFAULT_MAX_BACKFILL_BARS = {"5m": 2500, "4h": 800, "1d": 500, "1w": 300}
_OKX_LIMIT = 300
_CLOSE_LAG_MS = 2500


def _now_ms() -> int:
    return int(time.time() * 1000)


def _trace_id(symbol: str, timeframe: str) -> str:
    return f"surveyor-feed:{symbol}:{timeframe}:{_now_ms()}"


def _market_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip()
    if "/" in raw:
        return raw.upper()
    normalized = raw.upper()
    if normalized.endswith("USDT"):
        return f"{normalized[:-4]}/USDT"
    raise ValueError(f"unsupported symbol format: {symbol}")


@dataclass
class OkxCcxtProvider:
    exchange: ccxt.Exchange

    def fetch_ohlcv(self, symbol: str, timeframe: str, since_ms: int | None, limit: int) -> list[CandleDTO]:
        market_symbol = _market_symbol(symbol)
        try:
            rows = self.exchange.fetch_ohlcv(
                market_symbol,
                timeframe=timeframe,
                since=since_ms,
                limit=min(int(limit), _OKX_LIMIT),
            )
        except ccxt.RateLimitExceeded as exc:
            raise ProviderRateLimitError(str(exc)) from exc
        except ccxt.RequestTimeout as exc:
            raise ProviderTimeoutError(str(exc)) from exc
        except ccxt.ExchangeNotAvailable as exc:
            raise ProviderUnavailableError(str(exc)) from exc
        except ccxt.NetworkError as exc:
            raise ProviderUpstreamError(str(exc)) from exc
        except ccxt.ExchangeError as exc:
            raise ProviderUpstreamError(str(exc)) from exc

        dataset_version = f"okx_ccxt_{timeframe}_v1"
        trace_id = _trace_id(symbol, timeframe)
        return [
            CandleDTO(
                provider_id="ccxt",
                venue="okx",
                symbol=market_symbol,
                timeframe=timeframe,
                ts_open_ms=int(row[0]),
                ts_close_ms=int(row[0]) + (_TIMEFRAME_SECONDS[timeframe] * 1000),
                open=str(row[1]),
                high=str(row[2]),
                low=str(row[3]),
                close=str(row[4]),
                volume=str(row[5]),
                dataset_version=dataset_version,
                trace_id=trace_id,
            )
            for row in rows
        ]

    def fetch_funding(self, symbol: str, since_ms: int | None, limit: int) -> list[dict]:
        return []

    def fetch_open_interest(self, symbol: str, since_ms: int | None, limit: int) -> list[dict]:
        return []

    def provider_health(self) -> ProviderHealth:
        return ProviderHealth(
            provider_id="ccxt",
            status="ok",
            reason_codes=[],
            rate_state="exchange_rate_limit",
            as_of_ms=_now_ms(),
        )


def _fetch_recent_closed_window(
    provider: OkxCcxtProvider,
    *,
    symbol: str,
    timeframe: str,
    now_ms: int,
    max_bars: int,
) -> list[CandleDTO]:
    earliest_ms = now_ms - (_TIMEFRAME_SECONDS[timeframe] * 1000 * max_bars)
    cursor = earliest_ms
    out: list[CandleDTO] = []
    seen_opens: set[int] = set()
    while len(out) < max_bars:
        batch = provider.fetch_ohlcv(symbol, timeframe, cursor, min(_OKX_LIMIT, max_bars - len(out)))
        if not batch:
            break
        batch = [row for row in batch if row.ts_close_ms + _CLOSE_LAG_MS <= now_ms and row.ts_open_ms not in seen_opens]
        if not batch:
            break
        out.extend(batch)
        for row in batch:
            seen_opens.add(row.ts_open_ms)
        cursor = batch[-1].ts_open_ms + 1
        if len(batch) < _OKX_LIMIT:
            break
        time.sleep(max(provider.exchange.rateLimit, 350) / 1000.0)
    return out[-max_bars:]


def _save_checkpoint(conn, *, symbol: str, timeframe: str, last_ts_open_ms: int | None, now_ms: int, state: str, reason_code: str | None, trace_id: str) -> None:
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
        ("ccxt", "okx", _market_symbol(symbol), timeframe, last_ts_open_ms, now_ms if state == "ok" else None, now_ms, 0 if state == "ok" else 1, state, reason_code, trace_id),
    )
    conn.execute(
        """
        INSERT INTO feed_health_events(provider_id, venue, symbol, timeframe, state, reason_codes_json, as_of_ms, trace_id, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        ("ccxt", "okx", _market_symbol(symbol), timeframe, state, json.dumps([] if reason_code is None else [reason_code]), now_ms, trace_id, None),
    )


def run_refresh(*, db_path: str, symbols: list[str], timeframes: list[str]) -> list[dict]:
    conn = init_db(db_path)
    exchange = ccxt.okx({"enableRateLimit": True})
    exchange.load_markets()
    provider = OkxCcxtProvider(exchange)
    outputs: list[dict] = []
    try:
        for symbol in symbols:
            for timeframe in timeframes:
                now_ms = _now_ms()
                trace_id = _trace_id(symbol, timeframe)
                candles = _fetch_recent_closed_window(
                    provider,
                    symbol=symbol,
                    timeframe=timeframe,
                    now_ms=now_ms,
                    max_bars=_DEFAULT_MAX_BACKFILL_BARS[timeframe],
                )
                reason_code: str | None = None
                state = "ok"
                inserted = 0
                idempotent = 0
                last_ts_open_ms = max((row.ts_open_ms for row in candles), default=None)
                latest_close_ms = max((row.ts_close_ms for row in candles), default=None)
                if not candles:
                    state = "degraded"
                    reason_code = "NO_CLOSED_CANDLES"
                elif latest_close_ms is not None and now_ms - latest_close_ms > (_TIMEFRAME_SECONDS[timeframe] * 1000 * 3):
                    state = "degraded"
                    reason_code = "CANDLE_STALE_WINDOW"
                with conn:
                    if candles:
                        summary = upsert_market_candles(conn, candles, ingest_ts_ms=now_ms)
                        inserted = int(summary["inserted"])
                        idempotent = int(summary["idempotent"])
                    _save_checkpoint(
                        conn,
                        symbol=symbol,
                        timeframe=timeframe,
                        last_ts_open_ms=last_ts_open_ms,
                        now_ms=now_ms,
                        state=state,
                        reason_code=reason_code,
                        trace_id=trace_id,
                    )
                freshness_ms = (now_ms - latest_close_ms) if latest_close_ms is not None else (_TIMEFRAME_SECONDS[timeframe] * 1000 * _DEFAULT_MAX_BACKFILL_BARS[timeframe])
                outputs.append(
                    {
                        "provider_id": "ccxt",
                        "venue": "okx",
                        "symbol": _market_symbol(symbol),
                        "timeframe": timeframe,
                        "freshness_ms": freshness_ms,
                        "gap_bars": 0,
                        "state": state,
                        "reason_codes": [] if reason_code is None else [reason_code],
                        "as_of_ms": now_ms,
                        "trace_id": trace_id,
                        "inserted": inserted,
                        "idempotent": idempotent,
                        "bars_fetched": len(candles),
                    }
                )
                time.sleep(max(exchange.rateLimit, 350) / 1000.0)
    finally:
        conn.close()
        close_fn = getattr(exchange, "close", None)
        if callable(close_fn):
            close_fn()
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh Surveyor canonical OKX feed into market_candles.")
    parser.add_argument("--db-path", default="data/liquidsniper.sqlite")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--timeframes", default="5m,4h,1d,1w")
    args = parser.parse_args()

    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    timeframes = [item.strip().lower() for item in args.timeframes.split(",") if item.strip()]
    outputs = run_refresh(db_path=args.db_path, symbols=symbols, timeframes=timeframes)
    print(json.dumps({"ok": True, "runs": outputs}, indent=2))


if __name__ == "__main__":
    main()
