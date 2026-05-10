from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from market_arbiter.core.surveyor_bundle_profile import select_surveyor_bundle_profile
from market_arbiter.core.surveyor_profile_acceptance import (
    ACCEPTED,
    ACCEPTED_WITH_CAUTION,
    REJECTED,
    SURVEYOR_PROFILE_ACCEPTANCE_CONTRACT,
    evaluate_surveyor_profile_acceptance,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "surveyor_bundle_profiles"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _profile_payload(fixture_name: str, profile_id: str) -> dict:
    return select_surveyor_bundle_profile(_load_fixture(fixture_name), profile_id=profile_id)


def _reason_codes(result: dict) -> set[str]:
    return {reason["reason_code"] for reason in result["reasons"]}


def test_arbiter_core_accepts_complete_live_payload() -> None:
    payload = _profile_payload("complete_live_bundle.json", "arbiter_core")

    result = evaluate_surveyor_profile_acceptance(payload)

    assert result["contract"] == SURVEYOR_PROFILE_ACCEPTANCE_CONTRACT
    assert result["profile_id"] == "arbiter_core"
    assert result["acceptance_status"] == ACCEPTED
    assert result["decision_mode"] == "live_decision_candidate"
    assert result["caution_level"] == "none"
    assert result["reason_counts"] == {"reject": 0, "warn": 0}


def test_arbiter_core_rejects_missing_required_family_even_from_diagnostic_payload() -> None:
    bundle = _load_fixture("complete_live_bundle.json")
    del bundle["datasets"]["sr_zones"]
    payload = select_surveyor_bundle_profile(bundle, profile_id="arbiter_core", strict=False)

    result = evaluate_surveyor_profile_acceptance(payload)

    assert result["acceptance_status"] == REJECTED
    assert result["decision_mode"] == "rejected"
    assert result["caution_level"] == "high"
    assert {"missing_required_families", "required_family_absent"}.issubset(_reason_codes(result))


def test_arbiter_core_rejects_degraded_required_family() -> None:
    bundle = _load_fixture("complete_live_bundle.json")
    bundle["datasets"]["feed_state"]["status"] = "degraded"
    payload = select_surveyor_bundle_profile(bundle, profile_id="arbiter_core")

    result = evaluate_surveyor_profile_acceptance(payload)

    assert result["acceptance_status"] == REJECTED
    assert result["reason_counts"]["reject"] == 1
    reason = result["reasons"][0]
    assert reason["reason_code"] == "required_family_not_usable"
    assert reason["family"] == "feed_state"
    assert reason["value"] == "degraded"


def test_arbiter_core_allows_optional_family_issue_as_watch_only_caution() -> None:
    bundle = _load_fixture("complete_live_bundle.json")
    bundle["datasets"]["dynamic_levels"]["status"] = "stale"
    payload = select_surveyor_bundle_profile(bundle, profile_id="arbiter_core")

    result = evaluate_surveyor_profile_acceptance(payload)

    assert result["acceptance_status"] == ACCEPTED_WITH_CAUTION
    assert result["decision_mode"] == "live_watch_only"
    assert result["caution_level"] == "medium"
    assert _reason_codes(result) == {"optional_family_caution"}


def test_backtest_core_accepts_replay_mode_payload_with_replay_only_interaction_family() -> None:
    payload = _profile_payload("replay_mode_bundle.json", "backtest_core")

    result = evaluate_surveyor_profile_acceptance(payload)

    assert result["acceptance_status"] == ACCEPTED
    assert result["decision_mode"] == "backtest_candidate"
    assert result["meta"]["build_mode"] == "replay"
    assert result["reason_counts"] == {"reject": 0, "warn": 0}


def test_arbiter_core_rejects_replay_mode_payload_for_live_decision_candidate() -> None:
    payload = _profile_payload("replay_mode_bundle.json", "arbiter_core")

    result = evaluate_surveyor_profile_acceptance(payload)

    assert result["acceptance_status"] == REJECTED
    assert result["decision_mode"] == "rejected"
    assert {"build_mode_not_allowed", "source_bundle_replay_only"}.issubset(_reason_codes(result))


def test_unknown_profile_policy_rejects_without_guessing_consumer_rules() -> None:
    payload = deepcopy(_profile_payload("complete_live_bundle.json", "ui_full"))
    payload["profile_id"] = "experimental_profile"

    result = evaluate_surveyor_profile_acceptance(payload)

    assert result["acceptance_status"] == REJECTED
    assert "unknown_profile_policy" in _reason_codes(result)
