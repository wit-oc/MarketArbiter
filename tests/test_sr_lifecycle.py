from __future__ import annotations

from market_arbiter.surveyor.sr_lifecycle import classify_sr_zone, classify_sr_zones


def _zone(**overrides):
    zone = {
        "zone_id": "btc-support-4h",
        "current_role": "support",
        "timeframe": "4h",
        "bounds": {"low": 100.0, "high": 102.0},
        "formation_reaction_count": 3,
        "historical_context_score": 0.80,
        "selection_score": 0.90,
        "source_rank": 1,
        "retest_count": 1,
        "failed_retest_count": 0,
    }
    zone.update(overrides)
    return zone


def test_confirmed_close_below_support_hard_invalidates_when_flip_disabled():
    metadata = classify_sr_zone(
        _zone(),
        confirmation={"close": 99.70, "break_quality": "single_close", "closed_at": "2026-05-09T18:00:00Z"},
        policy={"allow_flip_pending_on_confirmed_break": False},
    )

    assert metadata["lifecycle"]["status"] == "invalidated"
    assert metadata["lifecycle"]["current_role_valid"] is False
    assert metadata["quality"]["confidence_tier"] == "X"
    assert metadata["quality"]["decision_eligibility"] == "reject"
    assert "invalidated_close_below_support_buffer" in metadata["quality"]["invalidation_reasons"]
    assert metadata["visual"]["show_on_overlay"] is False


def test_wick_breach_reclaim_degrades_without_invalidating():
    metadata = classify_sr_zone(
        _zone(),
        confirmation={"low": 99.50, "close": 100.40},
    )

    assert metadata["lifecycle"]["status"] == "degraded"
    assert metadata["lifecycle"]["current_role_valid"] is True
    assert metadata["lifecycle"]["invalidated_by"] is None
    assert "degraded_wick_breach_reclaimed" in metadata["quality"]["degradation_reasons"]
    assert metadata["quality"]["decision_eligibility"] != "reject"
    assert metadata["visual"]["show_on_overlay"] is True


def test_retest_count_decay_maps_to_expected_tiers_and_eligibility():
    base = {
        "historical_context_score": 0.60,
        "selection_score": 0.60,
        "source_rank": 2,
    }

    first = classify_sr_zone(_zone(**base, retest_count=1))
    second = classify_sr_zone(_zone(**base, retest_count=2))
    third = classify_sr_zone(_zone(**base, retest_count=3))
    fourth = classify_sr_zone(_zone(**base, retest_count=4))

    assert first["lifecycle"]["status"] == "active"
    assert first["quality"]["confidence_tier"] == "A"
    assert first["quality"]["decision_eligibility"] == "candidate_eligible"

    assert second["lifecycle"]["status"] == "degraded"
    assert "degraded_second_retest" in second["quality"]["degradation_reasons"]
    assert second["quality"]["confidence_tier"] == "B"
    assert second["quality"]["decision_eligibility"] == "watch_eligible"

    assert third["lifecycle"]["status"] == "degraded"
    assert "degraded_third_retest" in third["quality"]["degradation_reasons"]
    assert third["quality"]["confidence_tier"] == "C"
    assert third["quality"]["decision_eligibility"] == "watch_only"

    assert fourth["lifecycle"]["status"] == "watch_only"
    assert "degraded_excessive_retests" in fourth["quality"]["degradation_reasons"]
    assert fourth["quality"]["confidence_tier"] == "C"
    assert fourth["quality"]["decision_eligibility"] == "watch_only"

    scores = [
        first["quality"]["confidence_score"],
        second["quality"]["confidence_score"],
        third["quality"]["confidence_score"],
        fourth["quality"]["confidence_score"],
    ]
    assert scores == sorted(scores, reverse=True)


def test_missing_bounds_hard_blocks_zone():
    metadata = classify_sr_zone(_zone(bounds={}))

    assert metadata["lifecycle"]["status"] == "blocked"
    assert metadata["lifecycle"]["current_role_valid"] is False
    assert metadata["quality"]["confidence_tier"] == "X"
    assert metadata["quality"]["decision_eligibility"] == "reject"
    assert "blocked_missing_zone_bounds" in metadata["quality"]["invalidation_reasons"]


def test_overlap_display_suppression_hides_lower_priority_same_side_zone():
    zones = [
        _zone(zone_id="strong-support", bounds={"low": 100.0, "high": 102.0}, source_rank=1, selection_score=0.95),
        _zone(zone_id="weak-support", bounds={"low": 100.50, "high": 102.50}, source_rank=4, selection_score=0.55),
    ]

    strong, weak = classify_sr_zones(zones)

    assert strong["zone_id"] == "strong-support"
    assert strong["visual"]["show_on_overlay"] is True
    assert weak["zone_id"] == "weak-support"
    assert weak["visual"]["show_on_overlay"] is False
    assert weak["visual"]["suppression_reason"] == "degraded_overlapping_zone_cluster"
    assert "degraded_overlapping_zone_cluster" in weak["quality"]["degradation_reasons"]


def test_confirmed_break_transitions_to_flip_pending_by_default():
    metadata = classify_sr_zone(
        _zone(zone_id="btc-resistance-4h", current_role="resistance", bounds={"low": 110.0, "high": 112.0}),
        confirmation={"close": 112.30, "break_quality": "single_close", "closed_at": "2026-05-09T18:00:00Z"},
    )

    assert metadata["lifecycle"]["status"] == "flipped_pending"
    assert metadata["lifecycle"]["current_role_valid"] is False
    assert metadata["lifecycle"]["flip_candidate"] is True
    assert metadata["lifecycle"]["invalidated_by"] == "invalidated_close_above_resistance_buffer"
    assert metadata["quality"]["decision_eligibility"] == "reject"
    assert "invalidated_close_above_resistance_buffer" in metadata["quality"]["invalidation_reasons"]
