"""Arbiter decision-layer package.

Surveyor remains descriptive. Arbiter modules consume point-in-time Surveyor
profiles and emit deterministic decision/simulation surfaces.
"""

from .backtest_controls import (
    ARBITER_BACKTEST_CONTROL_DATASET_CONTRACT_V1,
    TimeShiftControlConfig,
    build_time_shift_control_dataset,
)
from .ohlcv_backtest import (
    EVENT_STUDY_REPORT_CONTRACT,
    OHLCV_BACKTEST_REPORT_CONTRACT,
    OHLCVBacktestConfig,
    load_ohlcv_csv,
    load_ohlcv_directory,
    load_ohlcv_json,
    run_event_study,
    run_ohlcv_backtest,
    simulate_trade_candidate,
)
from .dca_execution import (
    ARBITER_DCA_EXECUTION_CONTRACT_V1,
    graduated_confluence_risk_pct,
    planned_dca_entries,
)
from .ohlcv_retest_adapter import (
    FAST_OHLCV_RETEST_ADAPTER_CONTRACT,
    FAST_OHLCV_RETEST_RUN_CONTRACT,
    FastOHLCVRetestAdapterConfig,
    build_fast_ohlcv_retest_dataset,
    build_fast_ohlcv_retest_profiles,
    load_market_candles_from_db,
)
from .setup_score import ARBITER_SETUP_SCORE_CONTRACT_V1, ARBITER_SR_ELIGIBILITY_CONTRACT_V1, RetestSetupThresholds, evaluate_sr_zone_eligibility, score_retest_setup
from .stop_policy import ARBITER_STOP_POLICY_CONTRACT_V1, adaptive_stop_buffer, resolve_retest_stop
from .take_profit import ARBITER_TAKE_PROFIT_CONTRACT_V1, planned_take_profits
from .strategy_backtest import (
    FOXIAN_RETEST_BACKTEST_DATASET_CONTRACT,
    FOXIAN_RETEST_STRATEGY_SIGNAL_CONTRACT,
    FoxianRetestStrategyConfig,
    build_foxian_retest_backtest_dataset,
    evaluate_foxian_retest_strategy,
)

__all__ = [
    "ARBITER_BACKTEST_CONTROL_DATASET_CONTRACT_V1",
    "ARBITER_DCA_EXECUTION_CONTRACT_V1",
    "ARBITER_SETUP_SCORE_CONTRACT_V1",
    "ARBITER_SR_ELIGIBILITY_CONTRACT_V1",
    "ARBITER_STOP_POLICY_CONTRACT_V1",
    "ARBITER_TAKE_PROFIT_CONTRACT_V1",
    "FAST_OHLCV_RETEST_ADAPTER_CONTRACT",
    "FAST_OHLCV_RETEST_RUN_CONTRACT",
    "FastOHLCVRetestAdapterConfig",
    "build_fast_ohlcv_retest_dataset",
    "build_fast_ohlcv_retest_profiles",
    "load_market_candles_from_db",
    "EVENT_STUDY_REPORT_CONTRACT",
    "FOXIAN_RETEST_BACKTEST_DATASET_CONTRACT",
    "FOXIAN_RETEST_STRATEGY_SIGNAL_CONTRACT",
    "OHLCV_BACKTEST_REPORT_CONTRACT",
    "OHLCVBacktestConfig",
    "RetestSetupThresholds",
    "TimeShiftControlConfig",
    "FoxianRetestStrategyConfig",
    "adaptive_stop_buffer",
    "build_foxian_retest_backtest_dataset",
    "build_time_shift_control_dataset",
    "evaluate_sr_zone_eligibility",
    "evaluate_foxian_retest_strategy",
    "graduated_confluence_risk_pct",
    "load_ohlcv_csv",
    "load_ohlcv_directory",
    "load_ohlcv_json",
    "run_event_study",
    "planned_dca_entries",
    "planned_take_profits",
    "resolve_retest_stop",
    "run_ohlcv_backtest",
    "score_retest_setup",
    "simulate_trade_candidate",
]
