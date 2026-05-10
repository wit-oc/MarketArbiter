from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_arbiter.core.surveyor_bundle_profile import (
    BundleProfileSelectionError,
    SURVEYOR_BUNDLE_PROFILE_PAYLOAD_CONTRACT,
    select_surveyor_bundle_profile,
    serialize_surveyor_bundle_profile,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "surveyor_bundle_profiles"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_arbiter_core_profile_selects_declared_families_without_rewriting_envelopes() -> None:
    bundle = _load_fixture("complete_live_bundle.json")

    payload = select_surveyor_bundle_profile(bundle, profile_id="arbiter_core")

    assert payload["contract"] == SURVEYOR_BUNDLE_PROFILE_PAYLOAD_CONTRACT
    assert payload["profile_id"] == "arbiter_core"
    assert payload["profile_status"] == "complete"
    assert payload["meta"]["source_bundle_id"] == "fixture:complete-live"
    assert payload["selection"]["included_families"] == [
        "feed_state",
        "structure_state",
        "sr_zones",
        "fib_context",
        "dynamic_levels",
    ]
    assert "interaction_lifecycle" not in payload["datasets"]
    assert payload["datasets"]["feed_state"] == bundle["datasets"]["feed_state"]
    assert payload["datasets"]["feed_state"] is not bundle["datasets"]["feed_state"]
    assert payload["selection"]["family_contract_versions"]["sr_zones"] == "sr_zones_v1"


def test_unknown_profile_fails_closed_with_available_profile_hint() -> None:
    bundle = _load_fixture("complete_live_bundle.json")

    with pytest.raises(BundleProfileSelectionError) as error:
        select_surveyor_bundle_profile(bundle, profile_id="not_a_profile")

    assert error.value.profile_id == "not_a_profile"
    assert error.value.missing_required_families == []
    assert "available profiles" in str(error.value)
    assert "arbiter_core" in str(error.value)


def test_arbiter_core_profile_fails_closed_when_required_family_missing() -> None:
    bundle = _load_fixture("complete_live_bundle.json")
    del bundle["datasets"]["sr_zones"]

    with pytest.raises(BundleProfileSelectionError) as error:
        select_surveyor_bundle_profile(bundle, profile_id="arbiter_core")

    assert error.value.profile_id == "arbiter_core"
    assert error.value.missing_required_families == ["sr_zones"]
    assert "missing required families: sr_zones" in str(error.value)


def test_backtest_core_profile_requires_interaction_lifecycle_family() -> None:
    bundle = _load_fixture("replay_mode_bundle.json")
    del bundle["datasets"]["interaction_lifecycle"]

    with pytest.raises(BundleProfileSelectionError) as error:
        select_surveyor_bundle_profile(bundle, profile_id="backtest_core")

    assert error.value.profile_id == "backtest_core"
    assert error.value.missing_required_families == ["interaction_lifecycle"]


def test_non_strict_selection_can_emit_invalid_payload_for_operator_diagnostics() -> None:
    bundle = _load_fixture("partial_degraded_bundle.json")
    del bundle["datasets"]["feed_state"]

    payload = select_surveyor_bundle_profile(bundle, profile_id="arbiter_core", strict=False)

    assert payload["profile_status"] == "invalid"
    assert payload["selection"]["missing_required_families"] == ["feed_state"]
    assert payload["diagnostics"]["profile_errors"] == [
        {
            "issue_kind": "missing_required_families",
            "profile_id": "arbiter_core",
            "families": ["feed_state"],
        }
    ]


def test_serializer_is_deterministic_and_round_trippable() -> None:
    bundle = _load_fixture("replay_mode_bundle.json")
    payload = select_surveyor_bundle_profile(bundle, profile_id="backtest_core")

    serialized = serialize_surveyor_bundle_profile(payload)

    assert serialized.endswith("\n")
    assert json.loads(serialized) == payload
    assert '"profile_id": "backtest_core"' in serialized
