from __future__ import annotations

from dataclasses import dataclass

from market_arbiter.core.db import init_db
from market_arbiter.core.market_data import CandleDTO
from market_arbiter.core.market_scheduler import MarketDataScheduler, ProviderRateLimitError, ProviderUpstreamError, SchedulerKey
from market_arbiter.feed.provider_policy import ProviderAccessGovernor, ProviderRolePolicy


POLICY_PATH = "configs/surveyor_feed_provider_policy.v1.json"


@dataclass
class CountingProvider:
    error: Exception | None = None
    calls: int = 0

    def fetch_ohlcv(self, symbol: str, timeframe: str, since_ms: int | None, limit: int):
        self.calls += 1
        if self.error:
            raise self.error
        return []

    def fetch_funding(self, symbol: str, since_ms: int | None, limit: int):
        return []

    def fetch_open_interest(self, symbol: str, since_ms: int | None, limit: int):
        return []

    def provider_health(self):
        raise NotImplementedError


class HttpUpstreamError(ProviderUpstreamError):
    def __init__(self, status_code: int):
        super().__init__(str(status_code))
        self.status_code = status_code


def test_accepted_split_source_provider_role_matrix():
    policy = ProviderRolePolicy.from_path(POLICY_PATH)

    assert policy.decide(provider_id="bybit", request_class="live_ws").allowed is True
    assert policy.decide(provider_id="okx", request_class="rest_history").allowed is True
    assert policy.decide(provider_id="okx", request_class="gap_repair").allowed is True
    assert policy.decide(provider_id="okx", request_class="live_ws").allowed is True

    bybit_rest = policy.decide(provider_id="bybit", request_class="rest_history")
    assert bybit_rest.allowed is False
    assert bybit_rest.reason_code == "PROVIDER_ROLE_BLOCKED"
    assert bybit_rest.role_state == "disabled_from_this_host"

    ad_hoc = policy.decide(
        provider_id="okx",
        request_class="rest_history",
        script_id="scripts/ad_hoc_provider_probe.py",
    )
    assert ad_hoc.allowed is False
    assert ad_hoc.reason_code == "PROVIDER_SCRIPT_OUT_OF_ROLLOUT_BOUNDS"


def test_bybit_rest_history_policy_blocks_before_provider_touch(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = CountingProvider()
    governor = ProviderAccessGovernor(
        policy=ProviderRolePolicy.from_path(POLICY_PATH),
        script_id="market_arbiter.ops.surveyor_feed_runner",
    )
    scheduler = MarketDataScheduler(conn, provider, provider_governor=governor, request_class="rest_history")
    key = SchedulerKey("ccxt", "bybit", "BTC/USDT", "1m")

    out = scheduler.run_cycle(key, now_ms=200_000, trace_id="trace-bybit-block")

    assert provider.calls == 0
    assert out["state"] == "blocked"
    assert out["reason_codes"] == ["PROVIDER_POLICY_BLOCKED"]
    cp = conn.execute("SELECT state, last_reason_code FROM feed_checkpoints").fetchone()
    assert cp == ("blocked", "PROVIDER_POLICY_BLOCKED")


def test_429_sets_provider_cooldown_and_next_probe_does_not_touch_provider(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = CountingProvider(error=ProviderRateLimitError("429"))
    governor = ProviderAccessGovernor(
        policy=ProviderRolePolicy.from_path(POLICY_PATH),
        script_id="market_arbiter.ops.surveyor_feed_runner",
        rate_limit_cooldown_ms=60_000,
    )
    scheduler = MarketDataScheduler(
        conn,
        provider,
        provider_governor=governor,
        request_class="rest_history",
        retry_attempts=1,
    )
    key = SchedulerKey("ccxt", "okx", "BTC/USDT", "1m")

    first = scheduler.run_cycle(key, now_ms=200_000, trace_id="trace-429-a")
    second = scheduler.run_cycle(key, now_ms=201_000, trace_id="trace-429-b")

    assert provider.calls == 1
    assert first["state"] == "degraded"
    assert first["reason_codes"] == ["PROVIDER_RATE_LIMITED"]
    assert second["state"] == "degraded"
    assert second["reason_codes"] == ["PROVIDER_COOLDOWN"]
    assert governor.snapshot()["cooldown_until_ms"] == {"okx/okx": 260_000}


def test_403_freezes_provider_ip_path_and_next_probe_does_not_touch_provider(tmp_path):
    conn = init_db(str(tmp_path / "db.sqlite"))
    provider = CountingProvider(error=HttpUpstreamError(403))
    governor = ProviderAccessGovernor(
        policy=ProviderRolePolicy.from_path(POLICY_PATH),
        script_id="market_arbiter.ops.surveyor_feed_runner",
        ip_freeze_ms=3_600_000,
    )
    scheduler = MarketDataScheduler(
        conn,
        provider,
        provider_governor=governor,
        request_class="rest_history",
        retry_attempts=1,
    )
    key = SchedulerKey("ccxt", "okx", "BTC/USDT", "1m")

    first = scheduler.run_cycle(key, now_ms=200_000, trace_id="trace-403-a")
    second = scheduler.run_cycle(key, now_ms=201_000, trace_id="trace-403-b")

    assert provider.calls == 1
    assert first["state"] == "frozen"
    assert first["reason_codes"] == ["PROVIDER_IP_FROZEN"]
    assert second["state"] == "frozen"
    assert second["reason_codes"] == ["PROVIDER_IP_FROZEN"]
    assert governor.snapshot()["frozen_until_ms"] == {"okx/okx": 3_800_000}
