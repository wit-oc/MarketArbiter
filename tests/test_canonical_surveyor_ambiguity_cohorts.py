from __future__ import annotations

from market_arbiter.ops.canonical_surveyor_ambiguity_cohorts import build_cohort_report
from market_arbiter.ops.canonical_surveyor_promotion_gate import DEFAULT_PRIMARY_VARIANT


def _trade(event_id: str, r: float, *, flags: list[str] | None = None, risk_pct: float = 4.0, risk_used: float = 50.0) -> dict:
    return {
        "entry_event_id": event_id,
        "symbol": "BTCUSDT",
        "side": "long",
        "entry_ts": "2026-01-01T00:00:00Z",
        "exit_ts": "2026-01-02T00:00:00Z",
        "exit_reason": "final_target" if r > 0 else "stop_loss",
        "net_r_multiple": r,
        "net_return_bps": r * risk_pct * (risk_used / 100.0) * 100.0,
        "risk_pct": risk_pct,
        "risk_budget_used_pct": risk_used,
        "target_rr": 4.0,
        "take_profit_plan": "tp_25_50_25",
        "ambiguity_flags": flags or [],
    }


def test_ambiguity_cohort_report_splits_clean_diagnostic_and_execution_order_bounds() -> None:
    variant = {**DEFAULT_PRIMARY_VARIANT, "target": "4R"}
    source = {
        "symbols": ["BTCUSDT"],
        "fold_results": [
            {
                **variant,
                "fold_id": "wf_test",
                "backtest": {
                    "trades": [
                        _trade("clean", 1.0),
                        _trade("diagnostic", 2.0, flags=["same_candle_multiple_take_profits_after_breakeven"]),
                        _trade("hard", 0.5, flags=["same_candle_stop_and_target"]),
                    ]
                },
            }
        ],
    }

    report = build_cohort_report(source, variant)

    assert report["cohorts"]["clean"]["trade_count"] == 1
    assert report["cohorts"]["diagnostic_only"]["trade_count"] == 1
    assert report["cohorts"]["non_execution_order"]["trade_count"] == 2
    assert report["cohorts"]["execution_order_ambiguous"]["trade_count"] == 1
    assert report["bounds"]["current_all"]["avg_net_r_multiple"] == (1.0 + 2.0 + 0.5) / 3
    assert report["bounds"]["conservative_all"]["avg_net_r_multiple"] == (1.0 + 2.0 - 1.0) / 3
    assert report["bounds"]["optimistic_all"]["avg_net_r_multiple"] == (1.0 + 2.0 + 2.25) / 3
