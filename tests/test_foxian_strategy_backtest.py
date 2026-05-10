from __future__ import annotations

from market_arbiter.arbiter.strategy_backtest import (
    FOXIAN_RETEST_BACKTEST_DATASET_CONTRACT,
    FOXIAN_RETEST_STRATEGY_SIGNAL_CONTRACT,
    build_foxian_retest_backtest_dataset,
    evaluate_foxian_retest_strategy,
)


def _profile(*, include_retest: bool = True) -> dict:
    lifecycle_events = []
    if include_retest:
        lifecycle_events.append(
            {
                "event_id": "evt:btc:4h-zone-1:first-retest",
                "event_type": "zone_retest_confirmed",
                "event_ts": "2026-04-27T15:00:00Z",
                "zone_id": "zone:BTCUSDT:4H:support:1",
                "side": "long",
                "confirmation": "confirmed",
                "retest_index": 1,
                "price": 63250.0,
            }
        )
    return {
        "contract": "surveyor_bundle_profile_payload_v1",
        "profile_id": "backtest_core",
        "profile_status": "complete",
        "meta": {
            "symbol": "BTCUSDT",
            "as_of_ts": "2026-04-27T15:00:00Z",
            "source_bundle_id": "fixture:foxian-retest",
            "build_mode": "replay",
            "continuity_state": "replay",
        },
        "datasets": {
            "feed_state": {
                "family": "feed_state",
                "contract_version": "feed_state_v1",
                "status": "complete",
                "payload": {"dataset_mode": "replay"},
            },
            "structure_state": {
                "family": "structure_state",
                "contract_version": "structure_state_v1",
                "status": "complete",
                "payload": {
                    "timeframes": {
                        "1D": {"directional_bias": "bullish"},
                        "4H": {"directional_bias": "bullish"},
                    }
                },
            },
            "sr_zones": {
                "family": "sr_zones",
                "contract_version": "sr_zones_v1",
                "status": "complete",
                "payload": {
                    "selected_surfaces": {
                        "4H": {
                            "zones": [
                                {
                                    "zone_id": "zone:BTCUSDT:4H:support:1",
                                    "timeframe": "4H",
                                    "current_role": "support",
                                    "origin_kind": "flip_zone",
                                    "zone_low": 63000.0,
                                    "zone_high": 63500.0,
                                    "quality_score": 0.82,
                                    "lifecycle": {"status": "active", "current_role_valid": True},
                                    "quality": {
                                        "confidence_score": 0.86,
                                        "confidence_tier": "A",
                                        "decision_eligibility": "candidate_eligible",
                                        "reason_codes": [],
                                        "degradation_reasons": [],
                                        "invalidation_reasons": [],
                                    },
                                    "visual": {"show_on_overlay": True, "overlay_priority": 486, "color_class": "sr_high"},
                                }
                            ]
                        }
                    }
                },
            },
            "fib_context": {
                "family": "fib_context",
                "contract_version": "fib_context_v1",
                "status": "complete",
                "payload": {"contexts_by_timeframe": {"4H": {"fib_state": "active", "value_zone": "discount"}}},
            },
            "dynamic_levels": {
                "family": "dynamic_levels",
                "contract_version": "dynamic_levels_v1",
                "status": "complete",
                "payload": {"levels": [{"level_id": "dyn:vwap", "timeframe": "5m", "zone_relation": "overlap"}]},
            },
            "interaction_lifecycle": {
                "family": "interaction_lifecycle",
                "contract_version": "interaction_lifecycle_v1",
                "status": "complete",
                "payload": {"state_changes": lifecycle_events},
            },
        },
        "diagnostics": {"profile_errors": []},
    }


def test_evaluate_foxian_retest_strategy_emits_trade_candidate_from_backtest_core_profile() -> None:
    signal = evaluate_foxian_retest_strategy(_profile())

    assert signal["contract"] == FOXIAN_RETEST_STRATEGY_SIGNAL_CONTRACT
    assert signal["verdict"] == "candidate"
    assert signal["symbol"] == "BTCUSDT"
    assert signal["event_study_row"]["side"] == "long"
    assert signal["event_study_row"]["zone_id"] == "zone:BTCUSDT:4H:support:1"
    assert signal["event_study_row"]["confluence_score"] >= 4
    assert signal["trade_candidate"]["entry_policy"] == "next_bar_open_after_retest_confirmation"
    assert signal["trade_candidate"]["stop_policy"] == "below_zone_low_plus_buffer"
    assert signal["trade_candidate"]["cost_model"] == {
        "taker_fee_bps": 5.0,
        "slippage_bps": 2.0,
        "funding_bps_per_8h": 0.0,
    }
    assert signal["trade_candidate"]["risk_model"]["model"] == "confluence_scaled_fixed_fractional_v0"
    assert signal["trade_candidate"]["risk_model"]["risk_pct"] == 5.0
    assert signal["trade_candidate"]["sr_lifecycle_gate"]["candidate_allowed"] is True
    assert signal["event_study_row"]["sr_decision_eligibility"] == "candidate_eligible"
    assert "first_retest_bonus" in signal["trade_candidate"]["confluence_model"]["reason_codes"]
    assert "sr_lifecycle_confidence_tier_high" in signal["reason_codes"]


def test_evaluate_foxian_retest_strategy_stays_watch_when_sr_metadata_is_watch_only() -> None:
    profile = _profile()
    zone = profile["datasets"]["sr_zones"]["payload"]["selected_surfaces"]["4H"]["zones"][0]
    zone["lifecycle"] = {"status": "watch_only", "current_role_valid": True}
    zone["quality"] = {"confidence_tier": "C", "decision_eligibility": "watch_only", "reason_codes": ["degraded_third_retest"]}

    signal = evaluate_foxian_retest_strategy(profile)

    assert signal["verdict"] == "watch"
    assert signal["trade_candidate"] is None
    assert signal["event_study_row"]["sr_watch_allowed"] is True
    assert "sr_lifecycle_watch_only" in signal["reason_codes"]


def test_evaluate_foxian_retest_strategy_stays_watch_when_sr_metadata_is_display_only() -> None:
    profile = _profile()
    zone = profile["datasets"]["sr_zones"]["payload"]["selected_surfaces"]["4H"]["zones"][0]
    zone["lifecycle"] = {"status": "degraded", "current_role_valid": True}
    zone["quality"] = {"confidence_tier": "D", "decision_eligibility": "display_only", "reason_codes": ["degraded_missing_formation_evidence"]}

    signal = evaluate_foxian_retest_strategy(profile)

    assert signal["verdict"] == "watch"
    assert signal["trade_candidate"] is None
    assert signal["event_study_row"]["sr_display_allowed"] is True
    assert "sr_lifecycle_display_only" in signal["reason_codes"]


def test_evaluate_foxian_retest_strategy_rejects_when_sr_metadata_rejects() -> None:
    profile = _profile()
    zone = profile["datasets"]["sr_zones"]["payload"]["selected_surfaces"]["4H"]["zones"][0]
    zone["lifecycle"] = {"status": "invalidated", "current_role_valid": False}
    zone["quality"] = {"confidence_tier": "X", "decision_eligibility": "reject", "reason_codes": ["invalidated_close_below_support_buffer"]}
    zone["visual"] = {"show_on_overlay": False, "overlay_priority": 0, "color_class": "sr_invalid"}

    signal = evaluate_foxian_retest_strategy(profile)

    assert signal["verdict"] == "reject"
    assert signal["trade_candidate"] is None
    assert signal["event_study_row"]["sr_candidate_allowed"] is False
    assert "sr_lifecycle_reject" in signal["reason_codes"]


def test_evaluate_foxian_retest_strategy_stays_watch_without_retest_event() -> None:
    signal = evaluate_foxian_retest_strategy(_profile(include_retest=False))

    assert signal["verdict"] == "watch"
    assert signal["trade_candidate"] is None
    assert "no_retest_event" in signal["reason_codes"]


def test_evaluate_foxian_retest_strategy_rejects_missing_required_family() -> None:
    profile = _profile()
    del profile["datasets"]["sr_zones"]

    signal = evaluate_foxian_retest_strategy(profile)

    assert signal["verdict"] == "reject"
    assert signal["missing_families"] == ["sr_zones"]
    assert signal["event_study_row"] is None


def test_build_foxian_retest_backtest_dataset_splits_event_rows_and_trade_candidates() -> None:
    dataset = build_foxian_retest_backtest_dataset([_profile(), _profile(include_retest=False)])

    assert dataset["contract"] == FOXIAN_RETEST_BACKTEST_DATASET_CONTRACT
    assert len(dataset["evaluations"]) == 2
    assert len(dataset["event_study_rows"]) == 1
    assert len(dataset["trade_candidates"]) == 1
