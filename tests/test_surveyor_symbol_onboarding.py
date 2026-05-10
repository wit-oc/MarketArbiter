from __future__ import annotations

import json
import subprocess
import sys

import pytest

from market_arbiter.ops.surveyor_feed_runner import FEED_WORKSET_MANIFEST_CONTRACT
from market_arbiter.ops.surveyor_symbol_onboarding import (
    ADVANCE_CHECK_CONTRACT,
    ONBOARDING_MANIFEST_CONTRACT,
    ROLLOUT_REPORT_CONTRACT,
    SOAK_EVIDENCE_CONTRACT,
    admit_symbol,
    build_advance_check,
    build_rollout_report,
    build_soak_evidence,
    load_onboarding_manifest,
    pause_symbol,
    render_active_workset,
    validate_onboarding_manifest,
    write_active_workset,
)


def _manifest_payload(tmp_path):
    return {
        "contract": ONBOARDING_MANIFEST_CONTRACT,
        "environment": "demo",
        "db_path": str(tmp_path / "market_arbiter.sqlite"),
        "workset_output_path": str(tmp_path / "generated" / "workset.json"),
        "control": {"stage": "single_pair_soak"},
        "provider_policy": {"allow_prod": False, "rest_requests_per_minute_cap": 90},
        "stages": [{"id": "single_pair_soak", "target_symbols": 1, "min_soak_hours": 72}],
        "symbols": [
            {
                "symbol": "BTC-USDT",
                "enabled": True,
                "stage_state": "promoted",
                "cohort": "core",
                "priority": 2,
                "shard_hint": "ws-a",
            },
            {
                "symbol": "ETH-USDT",
                "enabled": True,
                "stage_state": "soaking",
                "cohort": "core",
                "priority": 1,
                "shard_hint": "ws-a",
            },
            {
                "symbol": "SOL-USDT",
                "enabled": True,
                "stage_state": "proposed",
                "cohort": "candidate",
                "priority": 3,
                "shard_hint": "ws-b",
            },
            {
                "symbol": "DOGE-USDT",
                "enabled": False,
                "stage_state": "promoted",
                "priority": 4,
                "shard_hint": "ws-b",
            },
        ],
    }


def test_render_active_workset_from_onboarding_manifest(tmp_path):
    manifest_path = tmp_path / "onboarding.json"
    manifest_path.write_text(json.dumps(_manifest_payload(tmp_path)), encoding="utf-8")

    manifest = load_onboarding_manifest(manifest_path)
    workset = render_active_workset(manifest)

    assert workset["contract"] == FEED_WORKSET_MANIFEST_CONTRACT
    assert workset["db_path"].endswith("market_arbiter.sqlite")
    assert workset["symbols"] == ["ETHUSDT", "BTCUSDT"]
    assert workset["source_symbols"] == ["ETH-USDT", "BTC-USDT"]
    assert workset["timeframes"] == ["5m", "4h", "1d", "1w"]
    assert workset["shards"] == {"ws-a": ["ETHUSDT", "BTCUSDT"]}
    assert workset["onboarding_stage"] == "single_pair_soak"


def test_write_active_workset_creates_runner_manifest(tmp_path):
    manifest_path = tmp_path / "onboarding.json"
    payload = _manifest_payload(tmp_path)
    output_path = tmp_path / "generated" / "rendered.json"
    payload["workset_output_path"] = str(output_path)
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    result = write_active_workset(load_onboarding_manifest(manifest_path))
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert result["active_symbol_count"] == 2
    assert result["workset_output_path"] == str(output_path)
    assert written["contract"] == FEED_WORKSET_MANIFEST_CONTRACT
    assert written["symbols"] == ["ETHUSDT", "BTCUSDT"]


def test_validate_rejects_duplicate_normalized_symbols(tmp_path):
    payload = _manifest_payload(tmp_path)
    payload["symbols"].append({"symbol": "BTCUSDT", "enabled": True, "stage_state": "soaking"})
    manifest_path = tmp_path / "onboarding.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    validation = validate_onboarding_manifest(load_onboarding_manifest(manifest_path))

    assert validation["ok"] is False
    assert "duplicate symbol after normalization: BTCUSDT" in validation["errors"]


def test_validate_rejects_prod_without_explicit_allowance(tmp_path):
    payload = _manifest_payload(tmp_path)
    payload["environment"] = "prod"
    manifest_path = tmp_path / "onboarding.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    validation = validate_onboarding_manifest(load_onboarding_manifest(manifest_path))

    assert validation["ok"] is False
    assert "environment is prod but provider_policy.allow_prod is false" in validation["errors"]


def test_admit_and_pause_update_manifest_and_render_workset(tmp_path):
    manifest_path = tmp_path / "onboarding.json"
    output_path = tmp_path / "generated" / "workset.json"
    payload = _manifest_payload(tmp_path)
    payload["workset_output_path"] = str(output_path)
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    admit = admit_symbol(manifest_path, symbol="SOL-USDT", stage="single_pair_soak", now_fn=lambda: 1234)
    admitted_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    sol = next(row for row in admitted_payload["symbols"] if row["symbol"] == "SOL-USDT")
    rendered = json.loads(output_path.read_text(encoding="utf-8"))

    assert admit["action"] == "admit"
    assert sol["enabled"] is True
    assert sol["stage_state"] == "soaking"
    assert sol["admitted_stage"] == "single_pair_soak"
    assert sol["last_state_change_ms"] == 1234
    assert rendered["symbols"] == ["ETHUSDT", "BTCUSDT", "SOLUSDT"]

    pause = pause_symbol(manifest_path, symbol="ETHUSDT", reason="operator_test", now_fn=lambda: 5678)
    paused_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    eth = next(row for row in paused_payload["symbols"] if row["symbol"] == "ETH-USDT")
    rendered = json.loads(output_path.read_text(encoding="utf-8"))

    assert pause["action"] == "pause"
    assert eth["enabled"] is False
    assert eth["stage_state"] == "paused"
    assert eth["pause_reason"] == "operator_test"
    assert eth["last_state_change_ms"] == 5678
    assert rendered["symbols"] == ["BTCUSDT", "SOLUSDT"]


def test_report_and_advance_check_are_local_artifacts(tmp_path):
    manifest_path = tmp_path / "onboarding.json"
    manifest_path.write_text(json.dumps(_manifest_payload(tmp_path)), encoding="utf-8")
    manifest = load_onboarding_manifest(manifest_path)

    report = build_rollout_report(manifest, now_fn=lambda: 111)
    check = build_advance_check(manifest, now_fn=lambda: 222)

    assert report["contract"] == ROLLOUT_REPORT_CONTRACT
    assert report["as_of_ms"] == 111
    assert report["summary"]["symbols_total"] == 4
    assert report["summary"]["symbols_active"] == 2
    assert report["provider_safety"]["source"] == "manifest_only_until_feed_events_are_wired"
    assert check["contract"] == ADVANCE_CHECK_CONTRACT
    assert check["as_of_ms"] == 222
    assert check["recommendation"] == "hold"
    assert "active symbol count 2 exceeds stage target 1" in check["blockers"]
    assert "soak admission timestamps missing for stage single_pair_soak; requires 72h minimum" in check["blockers"]
    assert check["soak_evidence"]["contract"] == SOAK_EVIDENCE_CONTRACT
    assert check["auto_widening_enabled"] is False


def test_advance_check_holds_until_minimum_soak_elapsed(tmp_path):
    manifest_path = tmp_path / "onboarding.json"
    payload = _manifest_payload(tmp_path)
    payload["symbols"] = [
        {
            "symbol": "BTC-USDT",
            "enabled": True,
            "stage_state": "soaking",
            "admitted_stage": "single_pair_soak",
            "admitted_at_ms": 1_000,
        }
    ]
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    check = build_advance_check(load_onboarding_manifest(manifest_path), now_fn=lambda: 71 * 60 * 60 * 1000 + 1_000)

    assert check["recommendation"] == "hold"
    assert check["soak_evidence"]["minimum_soak_elapsed"] is False
    assert check["soak_evidence"]["active_symbols"][0]["min_soak_elapsed"] is False
    assert check["blockers"] == ["minimum soak not elapsed: 71.00h/72h"]
    assert check["advance_mode"] == "manual_review_only"


def test_advance_check_recommends_manual_review_after_clean_minimum_soak(tmp_path):
    manifest_path = tmp_path / "onboarding.json"
    payload = _manifest_payload(tmp_path)
    payload["symbols"] = [
        {
            "symbol": "BTC-USDT",
            "enabled": True,
            "stage_state": "soaking",
            "admitted_stage": "single_pair_soak",
            "admitted_at_ms": 1_000,
        }
    ]
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    check = build_advance_check(load_onboarding_manifest(manifest_path), now_fn=lambda: 72 * 60 * 60 * 1000 + 1_000)
    evidence = build_soak_evidence(load_onboarding_manifest(manifest_path), now_fn=lambda: 72 * 60 * 60 * 1000 + 1_000)

    assert evidence["contract"] == SOAK_EVIDENCE_CONTRACT
    assert evidence["minimum_soak_elapsed"] is True
    assert check["recommendation"] == "ready_for_review"
    assert check["blockers"] == []
    assert check["auto_widening_enabled"] is False


def test_advance_check_surfaces_paused_blocked_and_provider_freeze_states(tmp_path):
    manifest_path = tmp_path / "onboarding.json"
    payload = _manifest_payload(tmp_path)
    payload["provider_policy"] = {
        "allow_prod": False,
        "provider_freeze_events": [{"provider_id": "bybit", "reason": "403_country_block", "state": "open"}],
    }
    payload["symbols"] = [
        {
            "symbol": "BTC-USDT",
            "enabled": False,
            "stage_state": "paused",
            "admitted_stage": "single_pair_soak",
            "pause_reason": "operator_pause",
        },
        {
            "symbol": "ETH-USDT",
            "enabled": True,
            "stage_state": "soaking",
            "admitted_stage": "single_pair_soak",
            "admitted_at_ms": 1_000,
            "last_review_status": "blocked",
        },
    ]
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    check = build_advance_check(load_onboarding_manifest(manifest_path), now_fn=lambda: 73 * 60 * 60 * 1000 + 1_000)

    assert check["recommendation"] == "pause_required"
    assert "paused symbols in current stage: BTC-USDT" in check["blockers"]
    assert "blocked symbols in current stage: ETH-USDT" in check["blockers"]
    assert "provider freeze events open: bybit" in check["blockers"]
    assert check["soak_evidence"]["paused_events"][0]["symbol"] == "BTC-USDT"
    assert check["soak_evidence"]["blocked_events"][0]["symbol"] == "ETH-USDT"


def test_cli_render_workset(tmp_path):
    manifest_path = tmp_path / "onboarding.json"
    output_path = tmp_path / "generated" / "workset.json"
    payload = _manifest_payload(tmp_path)
    payload["workset_output_path"] = str(output_path)
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_arbiter.ops.surveyor_symbol_onboarding",
            "render-workset",
            "--manifest",
            str(manifest_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    result = json.loads(completed.stdout)
    assert result["active_symbols"] == ["ETHUSDT", "BTCUSDT"]
    assert output_path.exists()


def test_load_rejects_bad_contract(tmp_path):
    manifest_path = tmp_path / "onboarding.json"
    manifest_path.write_text(json.dumps({"contract": "wrong", "symbols": []}), encoding="utf-8")

    with pytest.raises(ValueError, match="unsupported onboarding manifest contract"):
        load_onboarding_manifest(manifest_path)
