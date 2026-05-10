from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_arbiter.ops.surveyor_bundle_export import (
    export_surveyor_bundle_profile,
    load_surveyor_bundle_for_export,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "surveyor_bundles"


def test_load_bundle_accepts_packet_with_top_level_bundle(tmp_path) -> None:
    bundle = json.loads((FIXTURE_DIR / "complete_live.json").read_text(encoding="utf-8"))
    packet_path = tmp_path / "packet.json"
    packet_path.write_text(json.dumps({"bundle": bundle}, sort_keys=True), encoding="utf-8")

    loaded = load_surveyor_bundle_for_export(bundle_path=str(packet_path))

    assert loaded["meta"]["bundle_id"] == "surveyor_bundle:BTCUSDT:fixture-live-complete"


def test_export_writes_profile_selected_payload_from_bundle_file(tmp_path) -> None:
    output_path = tmp_path / "arbiter_core.json"

    payload = export_surveyor_bundle_profile(
        bundle_path=str(FIXTURE_DIR / "complete_live.json"),
        profile_id="arbiter_core",
        output_path=str(output_path),
    )

    exported = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["export"]["output_path"] == str(output_path)
    assert exported["profile_id"] == "arbiter_core"
    assert exported["profile_status"] == "complete"
    assert exported["selection"]["included_families"] == [
        "feed_state",
        "structure_state",
        "sr_zones",
        "fib_context",
        "dynamic_levels",
    ]
    assert "interaction_lifecycle" not in exported["datasets"]


def test_export_can_emit_invalid_payload_when_operator_allows_invalid(tmp_path) -> None:
    bundle = json.loads((FIXTURE_DIR / "complete_live.json").read_text(encoding="utf-8"))
    del bundle["datasets"]["sr_zones"]
    bundle_path = tmp_path / "missing_sr.json"
    output_path = tmp_path / "invalid.json"
    bundle_path.write_text(json.dumps(bundle, sort_keys=True), encoding="utf-8")

    payload = export_surveyor_bundle_profile(
        bundle_path=str(bundle_path),
        profile_id="arbiter_core",
        strict=False,
        output_path=str(output_path),
    )

    exported = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["profile_status"] == "invalid"
    assert exported["selection"]["missing_required_families"] == ["sr_zones"]


def test_export_can_embed_acceptance_verdict_for_consumer_handoff(tmp_path) -> None:
    output_path = tmp_path / "arbiter_core_with_acceptance.json"

    payload = export_surveyor_bundle_profile(
        bundle_path=str(FIXTURE_DIR / "complete_live.json"),
        profile_id="arbiter_core",
        include_acceptance=True,
        output_path=str(output_path),
    )

    exported = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["acceptance"]["acceptance_status"] == "accepted"
    assert exported["acceptance"]["contract"] == "surveyor_profile_acceptance_v1"
    assert exported["acceptance"]["decision_mode"] == "live_decision_candidate"
    assert exported["acceptance"]["reason_counts"] == {"reject": 0, "warn": 0}


def test_export_embeds_rejected_acceptance_for_allowed_invalid_payload(tmp_path) -> None:
    bundle = json.loads((FIXTURE_DIR / "complete_live.json").read_text(encoding="utf-8"))
    del bundle["datasets"]["sr_zones"]
    bundle_path = tmp_path / "missing_sr.json"
    output_path = tmp_path / "invalid_with_acceptance.json"
    bundle_path.write_text(json.dumps(bundle, sort_keys=True), encoding="utf-8")

    payload = export_surveyor_bundle_profile(
        bundle_path=str(bundle_path),
        profile_id="arbiter_core",
        strict=False,
        include_acceptance=True,
        output_path=str(output_path),
    )

    exported = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["profile_status"] == "invalid"
    assert exported["acceptance"]["acceptance_status"] == "rejected"
    assert exported["acceptance"]["decision_mode"] == "rejected"
    assert exported["acceptance"]["reason_counts"]["reject"] >= 1


def test_load_bundle_requires_symbol_for_db_snapshot(tmp_path) -> None:
    with pytest.raises(ValueError, match="--symbol is required"):
        load_surveyor_bundle_for_export(db_path=str(tmp_path / "market_arbiter.sqlite"))
