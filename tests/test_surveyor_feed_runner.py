from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass

from market_arbiter.core.db import init_db
from market_arbiter.core.market_data import CandleDTO
from market_arbiter.core.market_scheduler import ProviderRateLimitError, ProviderUpstreamError
from market_arbiter.ops.surveyor_feed_refresh import OkxCcxtProvider
from market_arbiter.ops.surveyor_feed_runner import (
    FEED_SHARD_STATUS_CONTRACT,
    FEED_WORKSET_MANIFEST_CONTRACT,
    FeedRunnerConfig,
    SymbolRecomputeLockManager,
    build_live_recompute_task_runner,
    build_recompute_tasks,
    build_close_manifest,
    collect_status,
    config_from_manifest,
    execute_close_manifests,
    execute_recompute_manifest,
    materialize_derived_timeframe,
    run_canary,
    run_loop,
    run_once,
)


@dataclass
class StubProvider:
    batches: list[list[CandleDTO]]
    error: Exception | None = None

    def fetch_ohlcv(self, symbol: str, timeframe: str, since_ms: int | None, limit: int):
        if self.error:
            raise self.error
        if not self.batches:
            return []
        return self.batches.pop(0)

    def fetch_funding(self, symbol: str, since_ms: int | None, limit: int):
        return []

    def fetch_open_interest(self, symbol: str, since_ms: int | None, limit: int):
        return []

    def provider_health(self):
        raise NotImplementedError


def _candle(ts_open_ms: int, trace_id: str = "trace") -> CandleDTO:
    return CandleDTO(
        provider_id="ccxt",
        venue="okx",
        symbol="BTC/USDT",
        timeframe="5m",
        ts_open_ms=ts_open_ms,
        ts_close_ms=ts_open_ms + 300_000,
        open="100",
        high="101",
        low="99",
        close="100.5",
        volume="10",
        dataset_version="runner_test_v1",
        trace_id=trace_id,
    )


def _seed_candles(conn, *, symbol: str, timeframe: str, count: int, start_ms: int = 0) -> None:
    step_ms = {"5m": 300_000, "4h": 14_400_000, "1d": 86_400_000, "1w": 604_800_000}[timeframe]
    candles = []
    for idx in range(count):
        open_ms = start_ms + (idx * step_ms)
        base = 100 + idx
        candles.append(
            CandleDTO(
                provider_id="ccxt" if timeframe == "5m" else "local_aggregate",
                venue="okx",
                symbol=symbol,
                timeframe=timeframe,
                ts_open_ms=open_ms,
                ts_close_ms=open_ms + step_ms,
                open=str(base),
                high=str(base + 2),
                low=str(base - 1),
                close=str(base + 1),
                volume="10",
                dataset_version=f"seed_{timeframe}",
                trace_id=f"seed:{timeframe}:{idx}",
            )
        )
    from market_arbiter.core.market_data import upsert_market_candles

    with conn:
        upsert_market_candles(conn, candles, ingest_ts_ms=start_ms + (count * step_ms))


def test_run_once_writes_runner_state_and_status(tmp_path):
    config = FeedRunnerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        state_path=str(tmp_path / "runner_state.json"),
        symbols=["BTCUSDT"],
        timeframes=["5m"],
    )
    provider = StubProvider(batches=[[ _candle(300_000), _candle(600_000) ]])

    payload = run_once(config, provider=provider, now_fn=lambda: 850_000)
    status = collect_status(config, now_fn=lambda: 850_000)

    assert payload["mode"] == "once"
    assert payload["summary"]["state_counts"] == {"ok": 1}
    assert len(payload["close_manifests"]) == 1
    assert payload["close_manifests"][0]["close_ts_ms"] == 600_000
    assert len(payload["recompute_tasks"]) == 1
    assert payload["runner_state"]["mode"] == "once"
    assert payload["runner_state"]["cycles_completed"] == 1
    assert payload["runner_state"]["workset_manifest"]["contract"] == FEED_WORKSET_MANIFEST_CONTRACT
    assert status["continuity_state"] == "one_shot_refresh"
    assert status["summary"]["ok_keys"] == 1
    assert status["keys"][0]["state"] == "ok"


def test_run_loop_marks_status_live_continuous(tmp_path):
    config = FeedRunnerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        state_path=str(tmp_path / "runner_state.json"),
        symbols=["BTCUSDT"],
        timeframes=["5m"],
        loop_sleep_ms=15_000,
    )
    provider = StubProvider(batches=[[ _candle(300_000), _candle(600_000) ]])

    payload = run_loop(
        config,
        provider=provider,
        now_fn=lambda: 850_000,
        sleep_fn=lambda _seconds: None,
        max_cycles=1,
    )
    status = collect_status(config, now_fn=lambda: 860_000)

    assert payload["mode"] == "loop"
    assert payload["runner_state"]["mode"] == "loop"
    assert payload["runner_state"]["cycles_completed"] == 1
    assert status["continuity_state"] == "live_continuous"
    assert status["summary"]["ok_keys"] == 1


def test_config_from_manifest_loads_workset(tmp_path):
    manifest_path = tmp_path / "feed_workset.json"
    manifest_path.write_text(
        json.dumps(
            {
                "contract": FEED_WORKSET_MANIFEST_CONTRACT,
                "db_path": str(tmp_path / "from_manifest.sqlite"),
                "symbols": ["BTCUSDT", "ETHUSDT"],
                "timeframes": ["1d", "5m", "4h", "1w"],
                "shards": {"ws-a": ["BTCUSDT"], "ws-b": ["ETHUSDT"]},
                "loop_sleep_ms": 20000,
                "close_lag_ms": 5000,
                "backfill_page_limit": 200,
                "max_backfill_bars": 400,
            }
        ),
        encoding="utf-8",
    )

    config = config_from_manifest(str(manifest_path))

    assert config.db_path.endswith("from_manifest.sqlite")
    assert config.symbols == ["BTCUSDT", "ETHUSDT"]
    assert config.timeframes == ["5m", "4h", "1d", "1w"]
    assert config.shards == {"ws-a": ["BTCUSDT"], "ws-b": ["ETHUSDT"]}
    assert config.loop_sleep_ms == 20000
    assert config.manifest_path == str(manifest_path)


def test_collect_status_reports_per_shard_health(tmp_path):
    db_path = tmp_path / "market_arbiter.sqlite"
    conn = init_db(str(db_path))
    with conn:
        conn.execute(
            """
            INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("ccxt", "okx", "BTC/USDT", "5m", 600_000, 850_000, 850_000, 0, "ok", None, "trace:btc"),
        )
        conn.execute(
            """
            INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("ccxt", "okx", "ETH/USDT", "5m", 300_000, 300_000, 850_000, 1, "degraded", "PROVIDER_RATE_LIMITED", "trace:eth"),
        )
    conn.close()
    config = FeedRunnerConfig(
        db_path=str(db_path),
        symbols=["BTCUSDT", "ETHUSDT"],
        timeframes=["5m"],
        shards={"ws-a": ["BTCUSDT"], "ws-b": ["ETHUSDT"]},
    )

    status = collect_status(config, now_fn=lambda: 900_000)

    assert status["workset_manifest"]["shards"] == {"ws-a": ["BTCUSDT"], "ws-b": ["ETHUSDT"]}
    assert status["shard_status"]["contract"] == FEED_SHARD_STATUS_CONTRACT
    assert status["shard_status"]["overall_state"] == "degraded"
    shards = {row["shard_id"]: row for row in status["shard_status"]["shards"]}
    assert shards["ws-a"]["state"] == "ok"
    assert shards["ws-a"]["last_confirmed_close_ms"] == 900_000
    assert shards["ws-b"]["state"] == "degraded"
    assert shards["ws-b"]["degraded_symbols"] == ["ETH/USDT"]


def test_run_once_persists_last_shard_status(tmp_path):
    config = FeedRunnerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        state_path=str(tmp_path / "runner_state.json"),
        symbols=["BTCUSDT", "ETHUSDT"],
        timeframes=["5m"],
        shards={"ws-a": ["BTCUSDT"], "ws-b": ["ETHUSDT"]},
    )
    provider = StubProvider(batches=[[_candle(300_000), _candle(600_000)], []])

    payload = run_once(config, provider=provider, now_fn=lambda: 850_000)

    assert [row["shard_id"] for row in payload["results"]] == ["ws-a", "ws-b"]
    assert payload["runner_state"]["last_shard_status"]["shard_count"] == 2
    shards = {row["shard_id"]: row for row in payload["runner_state"]["last_shard_status"]["shards"]}
    assert shards["ws-a"]["state"] == "ok"
    assert shards["ws-b"]["state"] == "degraded"


def test_run_once_emits_close_manifest_and_recompute_tasks(tmp_path):
    config = FeedRunnerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        state_path=str(tmp_path / "runner_state.json"),
        symbols=["BTCUSDT"],
        timeframes=["5m"],
    )
    provider = StubProvider(batches=[[_candle(300_000), _candle(600_000)]])

    payload = run_once(config, provider=provider, now_fn=lambda: 1_000_000)

    assert len(payload["close_manifests"]) == 1
    manifest = payload["close_manifests"][0]
    assert manifest["contract"] == "surveyor_close_manifest_v1"
    assert manifest["symbol"] == "BTC/USDT"
    assert manifest["closed_timeframes"] == ["5m"]
    assert manifest["close_ts_ms"] == 900_000
    assert [task["action"] for task in payload["recompute_tasks"]] == ["recompute_surveyor_families"]
    assert payload["recompute_tasks"][0]["timeframe"] == "5m"
    assert len(payload["recompute_results"]) == 1
    assert payload["recompute_results"][0]["status"] == "completed"


def test_build_recompute_tasks_for_stacked_close_orders_dependencies():
    manifest = build_close_manifest(symbol="BTCUSDT", venue="blofin", close_ts_ms=604_800_000, trace_id="trace-1")

    tasks = build_recompute_tasks(manifest)

    assert [task["action"] for task in tasks] == [
        "recompute_surveyor_families",
        "materialize_4h_aggregate",
        "recompute_surveyor_families",
        "materialize_1d_aggregate",
        "recompute_surveyor_families",
        "materialize_1w_aggregate",
        "recompute_surveyor_families",
    ]
    assert tasks[1]["depends_on"] == [tasks[0]["task_id"]]
    assert tasks[2]["depends_on"] == [tasks[1]["task_id"]]
    assert tasks[-1]["timeframe"] == "1w"


def test_execute_recompute_manifest_runs_tasks_in_order(tmp_path):
    manifest = build_close_manifest(symbol="BTCUSDT", venue="blofin", close_ts_ms=604_800_000, trace_id="trace-1")
    tasks = build_recompute_tasks(manifest)
    seen: list[tuple[str, str]] = []

    def runner(task):
        seen.append((str(task.get("timeframe")), str(task.get("action"))))
        return {"status": "completed"}

    result = execute_recompute_manifest(manifest, tasks, task_runner=runner)

    assert result["status"] == "completed"
    assert seen == [
        ("5m", "recompute_surveyor_families"),
        ("4h", "materialize_4h_aggregate"),
        ("4h", "recompute_surveyor_families"),
        ("1d", "materialize_1d_aggregate"),
        ("1d", "recompute_surveyor_families"),
        ("1w", "materialize_1w_aggregate"),
        ("1w", "recompute_surveyor_families"),
    ]


def test_materialize_derived_timeframe_persists_4h_candle(tmp_path):
    conn = init_db(str(tmp_path / "market_arbiter.sqlite"))
    _seed_candles(conn, symbol="BTC/USDT", timeframe="5m", count=48)

    result = materialize_derived_timeframe(
        conn,
        symbol="BTCUSDT",
        timeframe="4h",
        close_ts_ms=14_400_000,
        trace_id="trace-agg",
        now_ms=14_400_500,
    )

    row = conn.execute(
        "SELECT provider_id, timeframe, ts_open_ms, ts_close_ms FROM market_candles WHERE provider_id = 'local_aggregate' AND timeframe = '4h'"
    ).fetchone()
    checkpoint = conn.execute(
        "SELECT last_ts_open_ms, state, last_reason_code FROM feed_checkpoints WHERE provider_id = 'local_aggregate' AND timeframe = '4h'"
    ).fetchone()
    conn.close()

    assert result["status"] == "completed"
    assert row == ("local_aggregate", "4h", 0, 14_400_000)
    assert checkpoint == (0, "ok", "DERIVED_AGGREGATE_READY")


def test_execute_close_manifests_with_live_runner_materializes_4h_and_recomputes(tmp_path):
    conn = init_db(str(tmp_path / "market_arbiter.sqlite"))
    _seed_candles(conn, symbol="BTC/USDT", timeframe="5m", count=48)
    manifest = build_close_manifest(symbol="BTCUSDT", venue="okx", close_ts_ms=14_400_000, trace_id="trace-1")

    results = execute_close_manifests([manifest], task_runner=build_live_recompute_task_runner(conn))

    aggregate_count = conn.execute(
        "SELECT COUNT(*) FROM market_candles WHERE provider_id = 'local_aggregate' AND timeframe = '4h'"
    ).fetchone()[0]
    conn.close()

    assert results[0]["status"] == "completed"
    assert aggregate_count == 1


def test_run_canary_returns_proof_for_stacked_boundary(tmp_path):
    config = FeedRunnerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        state_path=str(tmp_path / "runner_state.json"),
        symbols=["BTCUSDT"],
        timeframes=["5m"],
        backfill_page_limit=2_500,
        max_backfill_bars=2_500,
    )
    provider = StubProvider(
        batches=[[ _candle(idx * 300_000, trace_id=f"trace:{idx}") for idx in range(2_016) ]]
    )

    result = run_canary(
        config,
        provider=provider,
        now_fn=lambda: 605_000_000,
        require_manifest=True,
    )

    assert result["status"] == "ok"
    assert result["summary"]["ok_5m_results"] == 1
    assert result["summary"]["close_manifest_count"] == 1
    assert result["checks"][0]["closed_timeframes"] == ["5m", "4h", "1d", "1w"]
    assert result["checks"][0]["verified_derived_aggregates"] == {"4h": True, "1d": True, "1w": True}
    assert result["checks"][0]["recompute_status"] == "completed"


def test_execute_recompute_manifest_blocks_when_symbol_lock_held():
    manifest = build_close_manifest(symbol="BTCUSDT", venue="blofin", close_ts_ms=900_000, trace_id="trace-1")
    tasks = build_recompute_tasks(manifest)
    locks = SymbolRecomputeLockManager()
    assert locks.acquire(symbol="BTC/USDT", manifest_id="other-manifest") is True

    result = execute_recompute_manifest(manifest, tasks, task_runner=lambda _task: {"status": "completed"}, lock_manager=locks)

    assert result["status"] == "blocked"
    assert result["failed_step"] == "symbol_lock_unavailable"
    assert result["next_action"] == "retry_manifest_later"


def test_execute_recompute_manifest_stops_downstream_after_failure():
    manifest = build_close_manifest(symbol="BTCUSDT", venue="blofin", close_ts_ms=604_800_000, trace_id="trace-1")
    tasks = build_recompute_tasks(manifest)
    seen: list[str] = []

    def runner(task):
        action = str(task.get("action"))
        seen.append(action)
        if action == "materialize_4h_aggregate":
            return {"status": "blocked", "reason": "missing_source_bars", "next_action": "reload_authoritative_5m_window_then_retry"}
        return {"status": "completed"}

    result = execute_recompute_manifest(manifest, tasks, task_runner=runner)

    assert result["status"] == "blocked"
    assert result["failed_step"] == "materialize_4h_aggregate"
    assert seen == ["recompute_surveyor_families", "materialize_4h_aggregate"]


def test_execute_close_manifests_uses_bounded_worker_pool_across_symbols():
    worker_limit = 3
    manifests = [
        build_close_manifest(symbol="BTCUSDT", venue="okx", close_ts_ms=900_000 + idx * 300_000, trace_id=f"trace-btc-{idx}")
        for idx in range(3)
    ]
    manifests.extend(
        build_close_manifest(symbol=symbol, venue="okx", close_ts_ms=900_000, trace_id=f"trace-{symbol}")
        for symbol in ["ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT"]
    )
    state = {
        "active_total": 0,
        "max_active_total": 0,
        "active_by_symbol": {},
        "same_symbol_violations": 0,
        "seen_by_symbol": [],
    }
    guard = threading.Condition()

    def runner(task):
        symbol = str(task.get("symbol"))
        with guard:
            active_for_symbol = int(state["active_by_symbol"].get(symbol, 0))
            if active_for_symbol:
                state["same_symbol_violations"] += 1
            state["active_by_symbol"][symbol] = active_for_symbol + 1
            state["active_total"] += 1
            state["max_active_total"] = max(int(state["max_active_total"]), int(state["active_total"]))
            state["seen_by_symbol"].append((symbol, task.get("manifest_id")))
            guard.notify_all()
            if int(state["max_active_total"]) < worker_limit:
                guard.wait_for(lambda: int(state["max_active_total"]) >= worker_limit, timeout=1.0)
        time.sleep(0.01)
        with guard:
            state["active_total"] -= 1
            state["active_by_symbol"][symbol] -= 1
            guard.notify_all()
        return {"status": "completed"}

    results = execute_close_manifests(manifests, task_runner=runner, max_workers=worker_limit)

    assert [result["status"] for result in results] == ["completed"] * len(manifests)
    assert state["same_symbol_violations"] == 0
    assert state["max_active_total"] == worker_limit
    btc_seen = [manifest_id for symbol, manifest_id in state["seen_by_symbol"] if symbol == "BTC/USDT"]
    assert btc_seen == [manifest["manifest_id"] for manifest in manifests[:3]]


def test_run_once_executes_recompute_when_handler_supplied(tmp_path):
    config = FeedRunnerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        state_path=str(tmp_path / "runner_state.json"),
        symbols=["BTCUSDT"],
        timeframes=["5m"],
    )
    provider = StubProvider(batches=[[_candle(300_000), _candle(600_000)]])

    payload = run_once(
        config,
        provider=provider,
        now_fn=lambda: 1_000_000,
        recompute_task_runner=lambda _task: {"status": "completed"},
    )

    assert len(payload["recompute_results"]) == 1
    assert payload["recompute_results"][0]["status"] == "completed"


def test_collect_status_reports_mixed_when_runner_hits_error(tmp_path):
    config = FeedRunnerConfig(
        db_path=str(tmp_path / "market_arbiter.sqlite"),
        state_path=str(tmp_path / "runner_state.json"),
        symbols=["BTCUSDT"],
        timeframes=["5m"],
    )
    provider = StubProvider(batches=[], error=ProviderRateLimitError("429"))

    run_once(config, provider=provider, now_fn=lambda: 850_000)
    status = collect_status(config, now_fn=lambda: 850_000)

    assert status["continuity_state"] == "mixed"
    assert status["summary"]["issue_keys"] == 1
    assert status["keys"][0]["state"] == "degraded"


def test_collect_status_marks_stale_loop_even_when_keys_are_ok(tmp_path):
    db_path = tmp_path / "market_arbiter.sqlite"
    state_path = tmp_path / "runner_state.json"
    conn = init_db(str(db_path))
    with conn:
        conn.execute(
            """
            INSERT INTO feed_checkpoints(provider_id, venue, symbol, timeframe, last_ts_open_ms, last_success_ms, last_attempt_ms, failure_count, state, last_reason_code, trace_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("ccxt", "okx", "BTC/USDT", "5m", 600_000, 850_000, 850_000, 0, "ok", None, "trace:btc"),
        )
    conn.close()
    state_path.write_text(
        json.dumps(
            {
                "contract": "surveyor_feed_runner_state_v1",
                "mode": "loop",
                "loop_sleep_ms": 15_000,
                "last_cycle_completed_ms": 850_000,
            }
        ),
        encoding="utf-8",
    )
    config = FeedRunnerConfig(
        db_path=str(db_path),
        state_path=str(state_path),
        symbols=["BTCUSDT"],
        timeframes=["5m"],
    )

    status = collect_status(config, now_fn=lambda: 1_000_000)

    assert status["continuity_state"] == "stale_loop"
    assert status["runner_liveness"]["state"] == "stale"
    assert status["summary"]["ok_keys"] == 1
    assert status["summary"]["runner_liveness_state"] == "stale"
    assert status["shard_status"]["overall_state"] == "stale"
    assert status["shard_status"]["reason_codes"] == ["RUNNER_STALE"]


def test_okx_provider_translates_ccxt_network_error_to_scheduler_error():
    import ccxt

    class NetworkErrorExchange:
        def fetch_ohlcv(self, *_args, **_kwargs):
            raise ccxt.NetworkError("connection reset by peer")

    provider = OkxCcxtProvider(NetworkErrorExchange())

    try:
        provider.fetch_ohlcv("BTCUSDT", "5m", None, 10)
    except ProviderUpstreamError as exc:
        assert "connection reset" in str(exc)
    else:
        raise AssertionError("expected ProviderUpstreamError")
