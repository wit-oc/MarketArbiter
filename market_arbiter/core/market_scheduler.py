"""Task 20/23 candle scheduler + reliability controls + feed-health logging."""

from __future__ import annotations

from dataclasses import dataclass
import json
import random
import sqlite3
import time
from typing import Callable

from .market_data import CandleValidationError, MarketDataProvider, upsert_market_candles
from .market_quality import TIMEFRAME_MS, enforce_candle_quality
from market_arbiter.feed.provider_policy import (
    ProviderAccessGovernor,
    ProviderCooldownError,
    ProviderIpFrozenError,
    ProviderPolicyBlockedError,
)


class ProviderTimeoutError(RuntimeError): ...


class ProviderUnavailableError(RuntimeError): ...


class ProviderRateLimitError(RuntimeError): ...


class ProviderUpstreamError(RuntimeError): ...


@dataclass(frozen=True)
class SchedulerKey:
    provider_id: str
    venue: str
    symbol: str
    timeframe: str


@dataclass
class CircuitState:
    failures: list[int]
    tripped_until_ms: int = 0


class CircuitBreaker:
    def __init__(self, threshold: int = 5, window_ms: int = 120_000, cooldown_ms: int = 60_000) -> None:
        self.threshold = threshold
        self.window_ms = window_ms
        self.cooldown_ms = cooldown_ms
        self._states: dict[tuple[str, str], CircuitState] = {}

    def _state(self, provider_id: str, venue: str) -> CircuitState:
        return self._states.setdefault((provider_id, venue), CircuitState(failures=[]))

    def is_tripped(self, provider_id: str, venue: str, now_ms: int) -> bool:
        return self._state(provider_id, venue).tripped_until_ms > now_ms

    def record_failure(self, provider_id: str, venue: str, now_ms: int) -> bool:
        state = self._state(provider_id, venue)
        state.failures = [ts for ts in state.failures if now_ms - ts <= self.window_ms]
        state.failures.append(now_ms)
        if len(state.failures) >= self.threshold:
            state.tripped_until_ms = now_ms + self.cooldown_ms
            return True
        return False

    def record_success(self, provider_id: str, venue: str, now_ms: int) -> None:
        state = self._state(provider_id, venue)
        state.failures.clear()
        if state.tripped_until_ms <= now_ms:
            state.tripped_until_ms = 0


class RateBudgetManager:
    def __init__(self, *, max_tokens: int = 120, interval_ms: int = 60_000) -> None:
        self.max_tokens = max(1, int(max_tokens))
        self.interval_ms = max(1, int(interval_ms))
        self._calls: dict[tuple[str, str], list[int]] = {}

    def consume(self, provider_id: str, venue: str, now_ms: int) -> bool:
        key = (provider_id, venue)
        calls = [ts for ts in self._calls.get(key, []) if now_ms - ts <= self.interval_ms]
        if len(calls) >= self.max_tokens:
            self._calls[key] = calls
            return False
        calls.append(now_ms)
        self._calls[key] = calls
        return True


class MarketDataScheduler:
    def __init__(
        self,
        conn: sqlite3.Connection,
        provider: MarketDataProvider,
        *,
        close_lag_ms: int = 2500,
        backfill_page_limit: int = 1000,
        max_backfill_bars: int = 2000,
        breaker: CircuitBreaker | None = None,
        rate_budget: RateBudgetManager | None = None,
        retry_attempts: int = 3,
        retry_base_delay_ms: int = 250,
        retry_max_delay_ms: int = 2000,
        request_spacing_ms: int = 0,
        rand: Callable[[], float] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
        provider_governor: ProviderAccessGovernor | None = None,
        request_class: str = "rest_history",
    ) -> None:
        self.conn = conn
        self.provider = provider
        self.close_lag_ms = close_lag_ms
        self.backfill_page_limit = backfill_page_limit
        self.max_backfill_bars = max_backfill_bars
        self.breaker = breaker or CircuitBreaker()
        self.rate_budget = rate_budget or RateBudgetManager()
        self.retry_attempts = max(1, int(retry_attempts))
        self.retry_base_delay_ms = max(1, int(retry_base_delay_ms))
        self.retry_max_delay_ms = max(1, int(retry_max_delay_ms))
        self.request_spacing_ms = max(0, int(request_spacing_ms))
        self._last_request_monotonic_ms = 0.0
        self.rand = rand or random.random
        self.sleep_fn = sleep_fn or time.sleep
        self.provider_governor = provider_governor
        self.request_class = request_class

    def run_cycle(self, key: SchedulerKey, *, now_ms: int, trace_id: str) -> dict:
        tf_ms = TIMEFRAME_MS[key.timeframe]
        cp = self._get_checkpoint(key)

        if self.breaker.is_tripped(key.provider_id, key.venue, now_ms):
            self._save_checkpoint(key, now_ms=now_ms, state="tripped", reason_code="PROVIDER_UPSTREAM_ERROR", trace_id=trace_id)
            return self._snapshot(key, cp_last_ts=(cp["last_ts_open_ms"] if cp else None), now_ms=now_ms, state="tripped", reason_codes=["PROVIDER_UPSTREAM_ERROR"], trace_id=trace_id)

        try:
            if cp is None or cp["state"] == "resync_required":
                cp_last_ts = self._backfill(key, cp_last_ts=(cp["last_ts_open_ms"] if cp else None), now_ms=now_ms, trace_id=trace_id)
            else:
                cp_last_ts = cp["last_ts_open_ms"]

            expected_next_open = ((cp_last_ts or (now_ms - tf_ms)) // tf_ms) * tf_ms + tf_ms
            if expected_next_open + self.close_lag_ms > now_ms:
                state = cp["state"] if cp else "ok"
                if cp is not None and state in {"degraded", "tripped"}:
                    state = "ok"
                    self.breaker.record_success(key.provider_id, key.venue, now_ms)
                    self._save_checkpoint(key, now_ms=now_ms, state="ok", reason_code=None, trace_id=trace_id, last_ts_open_ms=cp_last_ts, failure_count=0)
                return self._snapshot(key, cp_last_ts=cp_last_ts, now_ms=now_ms, state=state, reason_codes=[], trace_id=trace_id)

            since_ms = (cp_last_ts + tf_ms) if cp_last_ts is not None else None
            candles = self._fetch_ohlcv_with_retry(key, since_ms=since_ms, limit=1000, now_ms=now_ms)
            candles = self._closed_candles(key, candles, now_ms=now_ms)
            if cp_last_ts is not None:
                candles = [candle for candle in candles if int(candle.ts_open_ms) > int(cp_last_ts)]
            if not candles:
                freshness = (now_ms - cp_last_ts) if cp_last_ts is not None else (self.max_backfill_bars * tf_ms)
                if freshness > (tf_ms * 3):
                    self._save_checkpoint(key, now_ms=now_ms, state="degraded", reason_code="CANDLE_STALE_WINDOW", trace_id=trace_id, last_ts_open_ms=cp_last_ts)
                    return self._snapshot(key, cp_last_ts=cp_last_ts, now_ms=now_ms, state="degraded", reason_codes=["CANDLE_STALE_WINDOW"], trace_id=trace_id)
                return self._snapshot(key, cp_last_ts=cp_last_ts, now_ms=now_ms, state=(cp["state"] if cp else "ok"), reason_codes=[], trace_id=trace_id)

            quality = enforce_candle_quality(candles, timeframe=key.timeframe, now_ms=now_ms, check_stale=False)
            summary = upsert_market_candles(self.conn, quality.candles, ingest_ts_ms=now_ms)
            quality_reason_codes = list(quality.reason_codes)
            newest = max((c.ts_open_ms for c in quality.candles), default=cp_last_ts)
            if newest is None:
                self._save_checkpoint(key, now_ms=now_ms, state="resync_required", reason_code="CANDLE_GAP_DETECTED", trace_id=trace_id)
                return self._snapshot(key, cp_last_ts=cp_last_ts, now_ms=now_ms, state="resync_required", reason_codes=["CANDLE_GAP_DETECTED"], trace_id=trace_id)

            oldest = min((c.ts_open_ms for c in quality.candles), default=newest)
            if cp_last_ts is not None and oldest != cp_last_ts + tf_ms:
                self._save_checkpoint(key, now_ms=now_ms, state="resync_required", reason_code="CANDLE_GAP_DETECTED", trace_id=trace_id, last_ts_open_ms=cp_last_ts)
                return self._snapshot(key, cp_last_ts=cp_last_ts, now_ms=now_ms, state="resync_required", reason_codes=["CANDLE_GAP_DETECTED"], trace_id=trace_id)

            latest_close_ms = newest + tf_ms
            if now_ms - latest_close_ms > (tf_ms * 3):
                self._save_checkpoint(key, now_ms=now_ms, state="degraded", reason_code="CANDLE_STALE_WINDOW", trace_id=trace_id, last_ts_open_ms=newest, failure_count=0)
                return self._snapshot(
                    key,
                    cp_last_ts=newest,
                    now_ms=now_ms,
                    state="degraded",
                    reason_codes=[*quality_reason_codes, "CANDLE_STALE_WINDOW"],
                    trace_id=trace_id,
                    inserted=summary["inserted"],
                )

            self.breaker.record_success(key.provider_id, key.venue, now_ms)
            self._save_checkpoint(key, now_ms=now_ms, state="ok", reason_code=None, trace_id=trace_id, last_ts_open_ms=newest, failure_count=0)
            return self._snapshot(
                key,
                cp_last_ts=newest,
                now_ms=now_ms,
                state="ok",
                reason_codes=quality_reason_codes,
                trace_id=trace_id,
                inserted=summary["inserted"],
            )
        except ProviderPolicyBlockedError:
            self._save_checkpoint(key, now_ms=now_ms, state="blocked", reason_code="PROVIDER_POLICY_BLOCKED", trace_id=trace_id, last_ts_open_ms=(cp["last_ts_open_ms"] if cp else None))
            return self._snapshot(key, cp_last_ts=(cp["last_ts_open_ms"] if cp else None), now_ms=now_ms, state="blocked", reason_codes=["PROVIDER_POLICY_BLOCKED"], trace_id=trace_id)
        except ProviderCooldownError:
            self._save_checkpoint(key, now_ms=now_ms, state="degraded", reason_code="PROVIDER_COOLDOWN", trace_id=trace_id, last_ts_open_ms=(cp["last_ts_open_ms"] if cp else None))
            return self._snapshot(key, cp_last_ts=(cp["last_ts_open_ms"] if cp else None), now_ms=now_ms, state="degraded", reason_codes=["PROVIDER_COOLDOWN"], trace_id=trace_id)
        except ProviderIpFrozenError:
            self._save_checkpoint(key, now_ms=now_ms, state="frozen", reason_code="PROVIDER_IP_FROZEN", trace_id=trace_id, last_ts_open_ms=(cp["last_ts_open_ms"] if cp else None))
            return self._snapshot(key, cp_last_ts=(cp["last_ts_open_ms"] if cp else None), now_ms=now_ms, state="frozen", reason_codes=["PROVIDER_IP_FROZEN"], trace_id=trace_id)
        except ProviderRateLimitError:
            self._on_failure(key, now_ms=now_ms, state="degraded", reason_code="PROVIDER_RATE_LIMITED", trace_id=trace_id)
            return self._snapshot(key, cp_last_ts=(cp["last_ts_open_ms"] if cp else None), now_ms=now_ms, state="degraded", reason_codes=["PROVIDER_RATE_LIMITED"], trace_id=trace_id)
        except ProviderTimeoutError:
            self._on_failure(key, now_ms=now_ms, state="degraded", reason_code="PROVIDER_TIMEOUT", trace_id=trace_id)
            return self._snapshot(key, cp_last_ts=(cp["last_ts_open_ms"] if cp else None), now_ms=now_ms, state="degraded", reason_codes=["PROVIDER_TIMEOUT"], trace_id=trace_id)
        except ProviderUnavailableError:
            self._on_failure(key, now_ms=now_ms, state="degraded", reason_code="PROVIDER_UNREACHABLE", trace_id=trace_id)
            return self._snapshot(key, cp_last_ts=(cp["last_ts_open_ms"] if cp else None), now_ms=now_ms, state="degraded", reason_codes=["PROVIDER_UNREACHABLE"], trace_id=trace_id)
        except ProviderUpstreamError:
            tripped = self._on_failure(key, now_ms=now_ms, state="degraded", reason_code="PROVIDER_UPSTREAM_ERROR", trace_id=trace_id)
            state = "tripped" if tripped else "degraded"
            return self._snapshot(key, cp_last_ts=(cp["last_ts_open_ms"] if cp else None), now_ms=now_ms, state=state, reason_codes=["PROVIDER_UPSTREAM_ERROR"], trace_id=trace_id)
        except CandleValidationError as e:
            if e.reason_code == "CANDLE_GAP_DETECTED":
                self._save_checkpoint(
                    key,
                    now_ms=now_ms,
                    state="resync_required",
                    reason_code=e.reason_code,
                    trace_id=trace_id,
                    last_ts_open_ms=(cp["last_ts_open_ms"] if cp else None),
                )
                return self._snapshot(
                    key,
                    cp_last_ts=(cp["last_ts_open_ms"] if cp else None),
                    now_ms=now_ms,
                    state="resync_required",
                    reason_codes=[e.reason_code],
                    trace_id=trace_id,
                )
            tripped = self._on_failure(key, now_ms=now_ms, state="degraded", reason_code=e.reason_code, trace_id=trace_id)
            state = "tripped" if tripped else "degraded"
            return self._snapshot(key, cp_last_ts=(cp["last_ts_open_ms"] if cp else None), now_ms=now_ms, state=state, reason_codes=[e.reason_code], trace_id=trace_id)

    def _fetch_ohlcv_with_retry(self, key: SchedulerKey, *, since_ms: int | None, limit: int, now_ms: int):
        if self.provider_governor is not None:
            self.provider_governor.check_request(
                provider_id=key.venue,
                venue=key.venue,
                request_class=self.request_class,
                now_ms=now_ms,
            )

        if not self.rate_budget.consume(key.provider_id, key.venue, now_ms):
            if self.provider_governor is not None:
                self.provider_governor.record_http_status(provider_id=key.venue, venue=key.venue, status_code=429, now_ms=now_ms)
            raise ProviderRateLimitError("budget_exceeded")

        attempt = 0
        while True:
            try:
                self._throttle_provider_request()
                return self.provider.fetch_ohlcv(key.symbol, key.timeframe, since_ms, limit)
            except (ProviderRateLimitError, ProviderTimeoutError, ProviderUnavailableError, ProviderUpstreamError) as e:
                if self.provider_governor is not None:
                    governor_reason = self.provider_governor.record_exception(provider_id=key.venue, venue=key.venue, error=e, now_ms=now_ms)
                    if governor_reason == "PROVIDER_IP_FROZEN":
                        raise ProviderIpFrozenError(str(e)) from e
                attempt += 1
                if attempt >= self.retry_attempts:
                    raise e
                backoff = min(self.retry_max_delay_ms, self.retry_base_delay_ms * (2 ** (attempt - 1)))
                jitter = int(backoff * self.rand() * 0.25)
                self.sleep_fn((backoff + jitter) / 1000.0)

    def _throttle_provider_request(self) -> None:
        if self.request_spacing_ms <= 0:
            return
        now_monotonic_ms = time.monotonic() * 1000.0
        elapsed_ms = now_monotonic_ms - self._last_request_monotonic_ms
        wait_ms = self.request_spacing_ms - elapsed_ms
        if self._last_request_monotonic_ms > 0 and wait_ms > 0:
            self.sleep_fn(wait_ms / 1000.0)
            now_monotonic_ms = time.monotonic() * 1000.0
        self._last_request_monotonic_ms = now_monotonic_ms

    def _closed_candles(self, key: SchedulerKey, candles, *, now_ms: int):
        tf_ms = TIMEFRAME_MS[key.timeframe]
        return [
            candle
            for candle in candles
            if int(candle.ts_open_ms) + tf_ms + self.close_lag_ms <= now_ms
        ]

    def _backfill(self, key: SchedulerKey, *, cp_last_ts: int | None, now_ms: int, trace_id: str) -> int | None:
        tf_ms = TIMEFRAME_MS[key.timeframe]
        earliest = now_ms - (self.max_backfill_bars * tf_ms)
        since_ms = cp_last_ts if cp_last_ts is not None else earliest
        fetched = 0
        newest: int | None = cp_last_ts

        while fetched < self.max_backfill_bars:
            batch_limit = min(self.backfill_page_limit, self.max_backfill_bars - fetched)
            candles = self._fetch_ohlcv_with_retry(key, since_ms=since_ms, limit=batch_limit, now_ms=now_ms)
            candles = self._closed_candles(key, candles, now_ms=now_ms)
            if not candles:
                break
            quality = enforce_candle_quality(candles, timeframe=key.timeframe, now_ms=now_ms, check_stale=False)
            upsert_market_candles(self.conn, quality.candles, ingest_ts_ms=now_ms)
            newest_batch = max(c.ts_open_ms for c in quality.candles)
            newest = max(newest or newest_batch, newest_batch)
            fetched += len(quality.candles)
            if newest_batch < since_ms:
                break
            since_ms = newest_batch + tf_ms
            if (newest_batch + tf_ms + self.close_lag_ms) > now_ms:
                break

        state = "ok" if newest is not None else "resync_required"
        reason = None if newest is not None else "CANDLE_GAP_DETECTED"
        self._save_checkpoint(key, now_ms=now_ms, state=state, reason_code=reason, trace_id=trace_id, last_ts_open_ms=newest, failure_count=0)
        return newest

    def _on_failure(self, key: SchedulerKey, *, now_ms: int, state: str, reason_code: str, trace_id: str) -> bool:
        cp = self._get_checkpoint(key)
        failure_count = (cp["failure_count"] if cp else 0) + 1
        tripped = self.breaker.record_failure(key.provider_id, key.venue, now_ms)
        effective_state = "tripped" if tripped else state
        self._save_checkpoint(
            key,
            now_ms=now_ms,
            state=effective_state,
            reason_code=reason_code,
            trace_id=trace_id,
            last_ts_open_ms=(cp["last_ts_open_ms"] if cp else None),
            failure_count=failure_count,
        )
        return tripped

    def _get_checkpoint(self, key: SchedulerKey) -> dict | None:
        row = self.conn.execute(
            """
            SELECT last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id
            FROM feed_checkpoints
            WHERE provider_id = ? AND venue = ? AND symbol = ? AND timeframe = ?;
            """,
            (key.provider_id, key.venue, key.symbol, key.timeframe),
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

    def _save_checkpoint(
        self,
        key: SchedulerKey,
        *,
        now_ms: int,
        state: str,
        reason_code: str | None,
        trace_id: str,
        last_ts_open_ms: int | None = None,
        failure_count: int | None = None,
    ) -> None:
        cp = self._get_checkpoint(key)
        last_ts = last_ts_open_ms if last_ts_open_ms is not None else (cp["last_ts_open_ms"] if cp else None)
        failures = failure_count if failure_count is not None else (cp["failure_count"] if cp else 0)
        last_success = now_ms if state == "ok" else (cp["last_success_ms"] if cp else None)
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
                key.provider_id,
                key.venue,
                key.symbol,
                key.timeframe,
                last_ts,
                last_success,
                now_ms,
                failures,
                state,
                reason_code,
                trace_id,
            ),
        )

    def _write_health_event(self, key: SchedulerKey, *, state: str, reason_codes: list[str], now_ms: int, trace_id: str) -> None:
        self.conn.execute(
            """
            INSERT INTO feed_health_events(
                provider_id, venue, symbol, timeframe,
                state, reason_codes_json, as_of_ms, trace_id, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                key.provider_id,
                key.venue,
                key.symbol,
                key.timeframe,
                state,
                json.dumps(reason_codes, sort_keys=True),
                now_ms,
                trace_id,
                None,
            ),
        )

    def _snapshot(
        self,
        key: SchedulerKey,
        *,
        cp_last_ts: int | None,
        now_ms: int,
        state: str,
        reason_codes: list[str],
        trace_id: str,
        inserted: int = 0,
    ) -> dict:
        self._write_health_event(key, state=state, reason_codes=reason_codes, now_ms=now_ms, trace_id=trace_id)
        tf_ms = TIMEFRAME_MS[key.timeframe]
        freshness = (now_ms - cp_last_ts) if cp_last_ts is not None else (self.max_backfill_bars * tf_ms)
        gap_bars = max(0, (freshness // tf_ms) - 1)
        return {
            "provider_id": key.provider_id,
            "venue": key.venue,
            "symbol": key.symbol,
            "timeframe": key.timeframe,
            "last_ts_open_ms": cp_last_ts,
            "latest_close_ms": (cp_last_ts + tf_ms) if cp_last_ts is not None else None,
            "freshness_ms": freshness,
            "gap_bars": int(gap_bars),
            "state": state,
            "reason_codes": reason_codes,
            "as_of_ms": now_ms,
            "trace_id": trace_id,
            "inserted": inserted,
        }
