from __future__ import annotations

from dataclasses import dataclass
import json

from market_arbiter.core.db import init_db
from market_arbiter.core.market_data import CandleDTO
from market_arbiter.feed.provider_policy import ProviderRolePolicy
from market_arbiter.feed.split_source_routing import (
    SplitSourceRoutingPlan,
    default_split_source_routes,
    tag_candles_for_route,
    validate_cohort_routes,
)
from market_arbiter.ops.surveyor_feed_runner import FeedRunnerConfig, config_from_manifest, run_once
from market_arbiter.ops.surveyor_symbol_onboarding import load_onboarding_manifest, render_active_workset


POLICY_PATH = "configs/surveyor_feed_provider_policy.v1.json"
COHORT = ["BTC-USDT", "ETH-USDT", "OKB-USDT", "SOL-USDT", "DOGE-USDT", "XRP-USDT", "BCH-USDT", "1INCH-USDT", "AAVE-USDT", "ADA-USDT"]


@dataclass
class RouteProbeProvider:
    batches: list[list[CandleDTO]]
    okx_rest_calls: int = 0
    bybit_rest_calls: int = 0

    def fetch_ohlcv(self, symbol: str, timeframe: str, since_ms: int | None, limit: int):
        self.okx_rest_calls += 1
        if not self.batches:
            return []
        return self.batches.pop(0)

    def fetch_bybit_rest_ohlcv(self, symbol: str, timeframe: str, since_ms: int | None, limit: int):
        self.bybit_rest_calls += 1
        raise AssertionError("Bybit REST must not be used for split-source repair/history")

    def fetch_funding(self, symbol: str, since_ms: int | None, limit: int):
        return []

    def fetch_open_interest(self, symbol: str, since_ms: int | None, limit: int):
        return []

    def provider_health(self):
        raise NotImplementedError


def _raw_okx_candle(ts_open_ms: int, *, symbol: str = "BTC/USDT", trace_id: str = "trace") -> CandleDTO:
    return CandleDTO(
        provider_id="raw_ccxt",
        venue="raw_okx",
        symbol=symbol,
        timeframe="5m",
        ts_open_ms=ts_open_ms,
        ts_close_ms=ts_open_ms + 300_000,
        open="100",
        high="101",
        low="99",
        close="100.5",
        volume="10",
        dataset_version="stub_history_v1",
        trace_id=trace_id,
    )


def test_split_source_plan_matches_accepted_provider_roles_for_10_symbol_cohort():
    policy = ProviderRolePolicy.from_path(POLICY_PATH)
    plan = SplitSourceRoutingPlan.from_provider_policy(policy)
    validation = validate_cohort_routes(symbols=COHORT, plan=plan, policy=policy)

    assert validation["ok"] is True
    assert validation["symbol_count"] == 10
    assert validation["checks"] == {
        "required_routes_allowed": True,
        "bybit_rest_history_blocked": True,
        "bybit_gap_repair_blocked": True,
        "rest_history_route_provider": "okx",
        "gap_repair_route_provider": "okx",
    }
    assertions = validation["plan"]["safety_assertions"]
    assert assertions["bybit_live_ws_primary"] is True
    assert assertions["okx_ws_shadow_fallback"] is True
    assert assertions["bybit_rest_history_not_configured"] is True


def test_route_tagging_stamps_persisted_okx_rest_history_identity():
    plan = SplitSourceRoutingPlan.from_payload(default_split_source_routes())
    route = plan.route("rest_history")

    tagged = tag_candles_for_route([_raw_okx_candle(300_000)], route)

    assert tagged[0].provider_id == "okx_rest"
    assert tagged[0].venue == "okx"
    assert tagged[0].dataset_version == "okx_rest_history:stub_history_v1"


def test_run_once_split_source_uses_okx_rest_history_and_never_bybit_rest(tmp_path):
    config = FeedRunnerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        state_path=str(tmp_path / "runner_state.json"),
        symbols=["BTCUSDT"],
        timeframes=["5m"],
        provider_policy_path=POLICY_PATH,
        source_routes=default_split_source_routes(),
    )
    provider = RouteProbeProvider(batches=[[_raw_okx_candle(300_000), _raw_okx_candle(600_000)]])

    payload = run_once(config, provider=provider, now_fn=lambda: 1_000_000, recompute_task_runner=lambda _task: {"status": "completed"})

    assert provider.okx_rest_calls >= 1
    assert provider.bybit_rest_calls == 0
    assert payload["results"][0]["provider_id"] == "okx_rest"
    assert payload["results"][0]["venue"] == "okx"

    conn = init_db(config.db_path)
    try:
        rows = conn.execute(
            "SELECT provider_id, venue, dataset_version FROM market_candles WHERE timeframe = '5m' ORDER BY ts_open_ms"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [
        ("okx_rest", "okx", "okx_rest_history:stub_history_v1"),
        ("okx_rest", "okx", "okx_rest_history:stub_history_v1"),
    ]


def test_workset_manifest_roundtrips_split_source_routes(tmp_path):
    manifest_path = tmp_path / "onboarding.json"
    manifest_path.write_text(
        json.dumps(
            {
                "contract": "surveyor_symbol_onboarding_manifest_v1",
                "environment": "demo",
                "db_path": str(tmp_path / "db.sqlite"),
                "workset_output_path": str(tmp_path / "workset.json"),
                "control": {"stage": "small_cohort_soak"},
                "provider_policy": {
                    "allow_prod": False,
                    "provider_policy_path": POLICY_PATH,
                    "source_routes": default_split_source_routes(),
                },
                "stages": [{"id": "small_cohort_soak", "target_symbols": 10, "min_soak_hours": 72}],
                "symbols": [
                    {"symbol": symbol, "enabled": True, "stage_state": "soaking", "priority": idx, "shard_hint": "ws-a"}
                    for idx, symbol in enumerate(COHORT, start=1)
                ],
                "timeframes": ["5m"],
            }
        ),
        encoding="utf-8",
    )

    workset = render_active_workset(load_onboarding_manifest(manifest_path))
    workset_path = tmp_path / "workset.json"
    workset_path.write_text(json.dumps(workset), encoding="utf-8")
    config = config_from_manifest(str(workset_path))

    assert workset["provider_policy_path"] == POLICY_PATH
    assert workset["source_routes"]["live_ws_primary"]["provider_id"] == "bybit"
    assert workset["source_routes"]["rest_history"]["provider_id"] == "okx"
    assert len(config.symbols) == 10
    assert config.source_routes == default_split_source_routes()
