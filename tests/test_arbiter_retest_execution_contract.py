from __future__ import annotations

from market_arbiter.arbiter.dca_execution import (
    ARBITER_DCA_EXECUTION_CONTRACT_V1,
    graduated_confluence_risk_pct,
    planned_dca_entries,
)
from market_arbiter.arbiter.ohlcv_backtest import OHLCVBacktestConfig
from market_arbiter.arbiter.setup_score import ARBITER_SETUP_SCORE_CONTRACT_V1, ARBITER_SR_ELIGIBILITY_CONTRACT_V1, RetestSetupThresholds, evaluate_sr_zone_eligibility, score_retest_setup
from market_arbiter.arbiter.stop_policy import ARBITER_STOP_POLICY_CONTRACT_V1, resolve_retest_stop
from market_arbiter.arbiter.take_profit import ARBITER_TAKE_PROFIT_CONTRACT_V1, planned_take_profits
from market_arbiter.ops.canonical_surveyor_dca_risk_ablation import simulate_dca_candidate


BASE_CANDIDATE = {
    "symbol": "BTCUSDT",
    "side": "long",
    "entry_event_id": "event-1",
    "entry_ts": "2025-12-31T23:00:00Z",
    "zone_id": "zone-1",
    "invalidation_level_hint": 90.0,
    "cost_model": {"taker_fee_bps": 0.0, "slippage_bps": 0.0, "funding_bps_per_8h": 0.0},
    "risk_model": {"risk_pct": 3.0},
}


def test_retest_setup_score_separates_arbiter_tradeability_from_surveyor_level_quality() -> None:
    thresholds = RetestSetupThresholds(body_ratio_min=0.65, selection_score_min=8.0, min_merge_family_count=3)

    passed = score_retest_setup({"body_ratio": 0.72, "selection_score": 9.0, "merge_family_count": 4}, thresholds)
    rejected = score_retest_setup({"body_ratio": 0.40, "selection_score": 9.0, "merge_family_count": 4}, thresholds)

    assert passed["contract"] == ARBITER_SETUP_SCORE_CONTRACT_V1
    assert passed["verdict"] == "candidate"
    assert passed["setup_score"] == 3.0
    assert rejected["verdict"] == "reject"
    assert "retest_body_displacement_below_threshold" in rejected["reason_codes"]


def test_sr_lifecycle_gate_maps_surveyor_metadata_to_arbiter_and_sentinel_behavior() -> None:
    candidate = evaluate_sr_zone_eligibility(
        {
            "zone_id": "zone-1",
            "lifecycle": {"status": "active"},
            "quality": {"confidence_tier": "A", "decision_eligibility": "candidate_eligible", "reason_codes": []},
            "visual": {"show_on_overlay": True},
        }
    )
    watch = evaluate_sr_zone_eligibility(
        {
            "zone_id": "zone-2",
            "lifecycle": {"status": "watch_only"},
            "quality": {"confidence_tier": "C", "decision_eligibility": "watch_only", "reason_codes": ["degraded_third_retest"]},
            "visual": {"show_on_overlay": True},
        }
    )
    display = evaluate_sr_zone_eligibility(
        {
            "zone_id": "zone-3",
            "lifecycle": {"status": "degraded"},
            "quality": {"confidence_tier": "D", "decision_eligibility": "display_only", "reason_codes": []},
            "visual": {"show_on_overlay": True},
        }
    )
    reject = evaluate_sr_zone_eligibility(
        {
            "zone_id": "zone-4",
            "lifecycle": {"status": "invalidated"},
            "quality": {"confidence_tier": "X", "decision_eligibility": "reject", "reason_codes": ["invalidated_close_below_support_buffer"]},
            "visual": {"show_on_overlay": False},
        }
    )

    assert candidate["contract"] == ARBITER_SR_ELIGIBILITY_CONTRACT_V1
    assert candidate["verdict"] == "candidate"
    assert candidate["candidate_allowed"] is True
    assert watch["verdict"] == "watch"
    assert watch["candidate_allowed"] is False and watch["watch_allowed"] is True
    assert display["verdict"] == "display"
    assert display["display_allowed"] is True and display["watch_allowed"] is False
    assert reject["verdict"] == "reject"
    assert reject["candidate_allowed"] is False and reject["display_allowed"] is False


def test_stop_policy_resolves_full_zone_and_sweep_aware_invalidation() -> None:
    zone = {"full_zone_bounds": {"low": 95.0, "high": 105.0}}

    full = resolve_retest_stop(side="long", zone=zone, event_candle={"low": 97, "high": 103, "close": 101}, policy="full_zone_5bps")
    sweep = resolve_retest_stop(side="long", zone=zone, event_candle={"low": 94, "high": 103, "close": 101}, atr=2.0, policy="sweep_or_zone_adaptive")
    short = resolve_retest_stop(side="short", zone=zone, event_candle={"low": 97, "high": 106, "close": 100}, atr=2.0, policy="sweep_or_zone_adaptive")

    assert full["contract"] == ARBITER_STOP_POLICY_CONTRACT_V1
    assert full["stop"] < 95.0
    assert sweep["stop_source"] == "sweep_wick_low"
    assert sweep["stop"] < 94.0
    assert short["stop_source"] == "sweep_wick_high"
    assert short["stop"] > 106.0


def test_planned_dca_entries_encode_reusable_ladder_and_unused_risk_semantics() -> None:
    long_ladder = planned_dca_entries(side="long", first_entry_price=100.0, zone_low=90.0, zone_high=110.0, plan_id="dca_20_30_50")
    short_ladder = planned_dca_entries(side="short", first_entry_price=100.0, zone_low=90.0, zone_high=110.0, plan_id="dca_50_50")

    assert long_ladder["contract"] == ARBITER_DCA_EXECUTION_CONTRACT_V1
    assert [row["weight"] for row in long_ladder["entries"]] == [0.2, 0.3, 0.5]
    assert [row["entry_price"] for row in long_ladder["entries"]] == [100.0, 100.0, 90.0]
    assert short_ladder["entries"][1]["entry_price"] == 100.0
    assert "unfilled_tranches_leave_risk_unused" in long_ladder["risk_budget_semantics"]


def test_graduated_confluence_risk_pct_caps_and_explains_risk() -> None:
    risk = graduated_confluence_risk_pct(
        {"body_ratio": 0.75, "selection_score": 9.5, "merge_family_count": 4},
        {"body_p50": 0.55, "body_p60": 0.70, "selection_p50": 8.0, "selection_p60": 9.0},
    )

    assert risk["risk_model"] == "graduated_confluence_v1"
    assert risk["risk_pct"] == 4.0
    assert "body_and_selection_p60_plus_0_5pct" in risk["reason_codes"]


def test_dca_simulation_allocates_total_trade_risk_and_leaves_unfilled_tranche_unused() -> None:
    candidate = dict(BASE_CANDIDATE)
    candles = [
        {"timestamp": "2026-01-01T00:00:00Z", "ts": 1767225600, "open": 100.0, "high": 110.0, "low": 99.0, "close": 105.0},
        {"timestamp": "2026-01-02T00:00:00Z", "ts": 1767312000, "open": 105.0, "high": 121.0, "low": 104.0, "close": 120.0},
    ]

    result = simulate_dca_candidate(
        candidate,
        candles,
        features={"full_zone_low": 90.0, "full_zone_high": 100.0},
        thresholds={},
        dca_plan="dca_50_50",
        risk_model="candidate_scaled",
        target_rr=2.0,
        take_profit_plan="single_final",
        max_hold_bars=2,
        config=OHLCVBacktestConfig(default_taker_fee_bps=0.0, default_slippage_bps=0.0, default_funding_bps_per_8h=0.0),
    )

    assert result["status"] == "closed"
    assert result["exit_reason"] == "final_target"
    assert result["filled_tranches"] == 1
    assert result["planned_tranches"] == 2
    assert result["risk_dollars"] == 3000.0
    assert result["risk_dollars_filled"] == 1500.0
    assert result["risk_budget_used_pct"] == 50.0


def test_dca_simulation_fills_all_tranches_and_uses_full_budget_when_zone_boundary_trades() -> None:
    candles = [
        {"timestamp": "2026-01-01T00:00:00Z", "ts": 1767225600, "open": 100.0, "high": 106.0, "low": 89.5, "close": 95.0},
        {"timestamp": "2026-01-02T00:00:00Z", "ts": 1767312000, "open": 95.0, "high": 111.0, "low": 94.0, "close": 110.0},
    ]

    candidate = {**BASE_CANDIDATE, "invalidation_level_hint": 89.0}
    result = simulate_dca_candidate(
        candidate,
        candles,
        features={"full_zone_low": 90.0, "full_zone_high": 100.0},
        thresholds={},
        dca_plan="dca_20_30_50",
        risk_model="candidate_scaled",
        target_rr=2.0,
        take_profit_plan="single_final",
        max_hold_bars=2,
        config=OHLCVBacktestConfig(default_taker_fee_bps=0.0, default_slippage_bps=0.0, default_funding_bps_per_8h=0.0),
    )

    assert result["status"] == "closed"
    assert result["filled_tranches"] == 3
    assert result["risk_budget_used_pct"] == 100.0
    assert [fill["weight"] for fill in result["fills"]] == [0.2, 0.3, 0.5]


def test_dca_simulation_is_conservative_when_stop_and_target_share_a_candle() -> None:
    candles = [
        {"timestamp": "2026-01-01T00:00:00Z", "ts": 1767225600, "open": 100.0, "high": 121.0, "low": 89.0, "close": 105.0},
    ]

    result = simulate_dca_candidate(
        dict(BASE_CANDIDATE),
        candles,
        features={"full_zone_low": 90.0, "full_zone_high": 100.0},
        thresholds={},
        dca_plan="single_100",
        risk_model="candidate_scaled",
        target_rr=2.0,
        take_profit_plan="single_final",
        max_hold_bars=1,
        config=OHLCVBacktestConfig(default_taker_fee_bps=0.0, default_slippage_bps=0.0, default_funding_bps_per_8h=0.0),
    )

    assert result["exit_reason"] == "stop_loss"
    assert "same_candle_stop_and_target" in result["ambiguity_flags"]
    assert result["has_intrabar_ambiguity"] is True
    assert result["has_execution_order_ambiguity"] is True
    assert result["intrabar_ambiguity_signal"] == "execution_order"


def test_dca_simulation_flags_same_candle_limit_fill_and_target_ambiguity() -> None:
    candles = [
        {"timestamp": "2026-01-01T00:00:00Z", "ts": 1767225600, "open": 100.0, "high": 111.0, "low": 94.0, "close": 105.0},
    ]

    result = simulate_dca_candidate(
        dict(BASE_CANDIDATE),
        candles,
        features={"full_zone_low": 90.0, "full_zone_high": 100.0},
        thresholds={},
        dca_plan="dca_50_50",
        risk_model="candidate_scaled",
        target_rr=2.0,
        take_profit_plan="single_final",
        max_hold_bars=1,
        config=OHLCVBacktestConfig(default_taker_fee_bps=0.0, default_slippage_bps=0.0, default_funding_bps_per_8h=0.0),
    )

    assert result["exit_reason"] == "final_target"
    assert result["filled_tranches"] == 2
    assert "same_candle_limit_fill_and_target" in result["ambiguity_flags"]
    assert result["has_execution_order_ambiguity"] is True


def test_short_dca_simulation_uses_short_ladder_and_unused_risk() -> None:
    candidate = {**BASE_CANDIDATE, "side": "short", "invalidation_level_hint": 110.0}
    candles = [
        {"timestamp": "2026-01-01T00:00:00Z", "ts": 1767225600, "open": 100.0, "high": 101.0, "low": 90.0, "close": 95.0},
        {"timestamp": "2026-01-02T00:00:00Z", "ts": 1767312000, "open": 95.0, "high": 96.0, "low": 79.0, "close": 80.0},
    ]

    result = simulate_dca_candidate(
        candidate,
        candles,
        features={"full_zone_low": 100.0, "full_zone_high": 110.0},
        thresholds={},
        dca_plan="dca_50_50",
        risk_model="candidate_scaled",
        target_rr=2.0,
        take_profit_plan="single_final",
        max_hold_bars=2,
        config=OHLCVBacktestConfig(default_taker_fee_bps=0.0, default_slippage_bps=0.0, default_funding_bps_per_8h=0.0),
    )

    assert result["status"] == "closed"
    assert result["exit_reason"] == "final_target"
    assert result["filled_tranches"] == 1
    assert result["risk_budget_used_pct"] == 50.0


def test_planned_take_profits_merges_final_tranche_at_2r() -> None:
    two_r = planned_take_profits(final_rr=2.0)
    three_r = planned_take_profits(final_rr=3.0)

    assert two_r["contract"] == ARBITER_TAKE_PROFIT_CONTRACT_V1
    assert [(row["rr"], row["weight"]) for row in two_r["tranches"]] == [(1.0, 0.25), (2.0, 0.75)]
    assert [(row["rr"], row["weight"]) for row in three_r["tranches"]] == [(1.0, 0.25), (2.0, 0.50), (3.0, 0.25)]
    assert "cancel_pending_dca_entries" in two_r["first_tp_actions"]


def test_first_take_profit_cancels_pending_dca_and_moves_stop_to_entry() -> None:
    candles = [
        {"timestamp": "2026-01-01T00:00:00Z", "ts": 1767225600, "open": 100.0, "high": 111.0, "low": 100.5, "close": 110.0},
        {"timestamp": "2026-01-02T00:00:00Z", "ts": 1767312000, "open": 110.0, "high": 112.0, "low": 94.0, "close": 95.0},
    ]

    result = simulate_dca_candidate(
        dict(BASE_CANDIDATE),
        candles,
        features={"full_zone_low": 90.0, "full_zone_high": 100.0},
        thresholds={},
        dca_plan="dca_20_30_50",
        risk_model="candidate_scaled",
        target_rr=3.0,
        max_hold_bars=2,
        config=OHLCVBacktestConfig(default_taker_fee_bps=0.0, default_slippage_bps=0.0, default_funding_bps_per_8h=0.0),
    )

    assert result["exit_reason"] == "breakeven_stop"
    assert result["filled_tranches"] == 1
    assert result["dca_cancelled_after_first_tp"] is True
    assert result["stop_moved_to_entry"] is True
    assert result["partial_exits"][0]["rr"] == 1.0
    assert result["partial_exits"][0]["units"] == 15.0
    assert result["partial_exits"][1]["role"] == "breakeven_stop"
    assert result["net_r_multiple"] == 0.25


def test_graduated_take_profit_exits_25_50_25_at_1r_2r_final() -> None:
    candles = [
        {"timestamp": "2026-01-01T00:00:00Z", "ts": 1767225600, "open": 100.0, "high": 111.0, "low": 100.5, "close": 110.0},
        {"timestamp": "2026-01-02T00:00:00Z", "ts": 1767312000, "open": 110.0, "high": 121.0, "low": 109.0, "close": 120.0},
        {"timestamp": "2026-01-03T00:00:00Z", "ts": 1767398400, "open": 120.0, "high": 131.0, "low": 119.0, "close": 130.0},
    ]

    result = simulate_dca_candidate(
        dict(BASE_CANDIDATE),
        candles,
        features={"full_zone_low": 90.0, "full_zone_high": 100.0},
        thresholds={},
        dca_plan="dca_20_30_50",
        risk_model="candidate_scaled",
        target_rr=3.0,
        max_hold_bars=3,
        config=OHLCVBacktestConfig(default_taker_fee_bps=0.0, default_slippage_bps=0.0, default_funding_bps_per_8h=0.0),
    )

    assert result["exit_reason"] == "final_target"
    assert [exit_part["rr"] for exit_part in result["partial_exits"]] == [1.0, 2.0, 3.0]
    assert [exit_part["units"] for exit_part in result["partial_exits"]] == [15.0, 30.0, 15.0]
    assert result["net_r_multiple"] == 2.0


def test_multiple_take_profits_after_breakeven_is_diagnostic_not_execution_ambiguity() -> None:
    candles = [
        {"timestamp": "2026-01-01T00:00:00Z", "ts": 1767225600, "open": 100.0, "high": 111.0, "low": 100.5, "close": 110.0},
        {"timestamp": "2026-01-02T00:00:00Z", "ts": 1767312000, "open": 110.0, "high": 131.0, "low": 109.0, "close": 130.0},
    ]

    result = simulate_dca_candidate(
        dict(BASE_CANDIDATE),
        candles,
        features={"full_zone_low": 90.0, "full_zone_high": 100.0},
        thresholds={},
        dca_plan="dca_20_30_50",
        risk_model="candidate_scaled",
        target_rr=3.0,
        max_hold_bars=2,
        config=OHLCVBacktestConfig(default_taker_fee_bps=0.0, default_slippage_bps=0.0, default_funding_bps_per_8h=0.0),
    )

    assert result["exit_reason"] == "final_target"
    assert "same_candle_multiple_take_profits_after_breakeven" in result["ambiguity_flags"]
    assert result["has_intrabar_ambiguity"] is True
    assert result["has_execution_order_ambiguity"] is False
    assert result["has_diagnostic_intrabar_signal"] is True
    assert result["intrabar_ambiguity_signal"] == "diagnostic_only"


def test_portfolio_metrics_use_1000_non_compounded_baseline() -> None:
    from market_arbiter.ops.canonical_surveyor_dca_risk_ablation import _portfolio_metrics

    metrics = _portfolio_metrics([
        {"entry_event_id": "a", "exit_ts": "2026-01-01T00:00:00Z", "net_return_bps": 100.0},
        {"entry_event_id": "b", "exit_ts": "2026-01-02T00:00:00Z", "net_return_bps": -50.0},
        {"entry_event_id": "c", "exit_ts": "2026-01-03T00:00:00Z", "net_return_bps": 200.0},
    ])

    assert metrics["portfolio_baseline_equity"] == 1000.0
    assert metrics["portfolio_final_pnl"] == 25.0
    assert metrics["portfolio_max_pnl"] == 25.0
    assert metrics["portfolio_max_drawdown"] == 5.0
    assert metrics["portfolio_max_drawdown_pct_of_baseline"] == 0.005
