from __future__ import annotations

import argparse
import json
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from market_arbiter.arbiter.ohlcv_backtest import OHLCVBacktestConfig, run_ohlcv_backtest
from market_arbiter.arbiter.ohlcv_retest_adapter import load_market_candles_from_db, write_json
from market_arbiter.arbiter.strategy_backtest import build_foxian_retest_backtest_dataset
from market_arbiter.core.db import init_db
from market_arbiter.ops.canonical_surveyor_retest_backtest import (
    CanonicalSurveyorConfig,
    DEFAULT_SYMBOLS,
    _ensure_liquidsniper_import,
    build_retest_profiles_for_symbol,
)
from market_arbiter.ops.canonical_surveyor_retest_diagnostics import (
    _as_dict,
    _as_list,
    _enrich_profiles,
    _float,
    _safe_quantile,
    _summarize,
)
from market_arbiter.ops.canonical_surveyor_stop_ablation import _dataset_for_policy


FOLDS = [
    {"fold_id": "wf_2023", "train_end": "2022-12-31T23:59:59Z", "test_start": "2023-01-01T00:00:00Z", "test_end": "2023-12-31T23:59:59Z"},
    {"fold_id": "wf_2024", "train_end": "2023-12-31T23:59:59Z", "test_start": "2024-01-01T00:00:00Z", "test_end": "2024-12-31T23:59:59Z"},
    {"fold_id": "wf_2025", "train_end": "2024-12-31T23:59:59Z", "test_start": "2025-01-01T00:00:00Z", "test_end": "2025-12-31T23:59:59Z"},
    {"fold_id": "wf_2026_q1", "train_end": "2025-12-31T23:59:59Z", "test_start": "2026-01-01T00:00:00Z", "test_end": "2026-03-31T23:59:59Z"},
]


def _parse_ts(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def _candidate_event_id(evaluation: Mapping[str, Any]) -> str:
    return str(_as_dict(evaluation.get("trade_candidate")).get("entry_event_id") or "")


def _candidate_ts(evaluation: Mapping[str, Any]) -> int | None:
    ts = _as_dict(evaluation.get("trade_candidate")).get("entry_ts")
    if not ts:
        return None
    try:
        return _parse_ts(str(ts))
    except Exception:
        return None


def _dataset_subset(dataset: Mapping[str, Any], predicate: Callable[[Mapping[str, Any]], bool]) -> dict[str, Any]:
    evaluations = [deepcopy(_as_dict(ev)) for ev in _as_list(dataset.get("evaluations")) if predicate(_as_dict(ev))]
    out = dict(dataset)
    out["evaluations"] = evaluations
    out["event_study_rows"] = [row for ev in evaluations if (row := _as_dict(ev).get("event_study_row"))]
    out["trade_candidates"] = [candidate for ev in evaluations if (candidate := _as_dict(ev).get("trade_candidate"))]
    return out


def _train_thresholds(train_evaluations: Sequence[Mapping[str, Any]], features_by_event: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    rows = []
    for ev in train_evaluations:
        event_id = _candidate_event_id(ev)
        row = features_by_event.get(event_id)
        if row:
            rows.append(row)
    body_p50 = _safe_quantile([_float(row.get("body_ratio"), float("nan")) for row in rows], 0.5)
    selection_p50 = _safe_quantile([_float(row.get("selection_score"), float("nan")) for row in rows], 0.5)
    body_p60 = _safe_quantile([_float(row.get("body_ratio"), float("nan")) for row in rows], 0.6)
    selection_p60 = _safe_quantile([_float(row.get("selection_score"), float("nan")) for row in rows], 0.6)
    return {
        "train_candidate_count": len(rows),
        "body_p50": body_p50,
        "selection_p50": selection_p50,
        "body_p60": body_p60,
        "selection_p60": selection_p60,
    }


def _passes_setup(row: Mapping[str, Any], setup_id: str, thresholds: Mapping[str, Any]) -> bool:
    body = _float(row.get("body_ratio"), float("nan"))
    selection = _float(row.get("selection_score"), float("nan"))
    family_count = int(_float(row.get("merge_family_count"), 0))
    if setup_id == "all":
        return True
    if setup_id == "body_p50_selection_p50_family3":
        return body >= _float(thresholds.get("body_p50"), float("inf")) and selection >= _float(thresholds.get("selection_p50"), float("inf")) and family_count >= 3
    if setup_id == "selection_p50_family3":
        return selection >= _float(thresholds.get("selection_p50"), float("inf")) and family_count >= 3
    if setup_id == "body_p50_selection_p50":
        return body >= _float(thresholds.get("body_p50"), float("inf")) and selection >= _float(thresholds.get("selection_p50"), float("inf"))
    if setup_id == "body_p60_selection_p60_family3":
        return body >= _float(thresholds.get("body_p60"), float("inf")) and selection >= _float(thresholds.get("selection_p60"), float("inf")) and family_count >= 3
    raise ValueError(f"unknown setup_id: {setup_id}")


def _apply_setup_filter(dataset: Mapping[str, Any], features_by_event: Mapping[str, Mapping[str, Any]], setup_id: str, thresholds: Mapping[str, Any]) -> dict[str, Any]:
    if setup_id == "all":
        return dict(dataset)

    def predicate(ev: Mapping[str, Any]) -> bool:
        row = features_by_event.get(_candidate_event_id(ev))
        return bool(row and _passes_setup(row, setup_id, thresholds))

    return _dataset_subset(dataset, predicate)


def _period_filter(start_ts: int, end_ts: int) -> Callable[[Mapping[str, Any]], bool]:
    def predicate(ev: Mapping[str, Any]) -> bool:
        ts = _candidate_ts(ev)
        return ts is not None and start_ts <= ts <= end_ts
    return predicate


def _aggregate_summaries(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    trade_rows = []
    for row in rows:
        for trade in _as_list(_as_dict(row.get("backtest")).get("trades")):
            trade_rows.append(_as_dict(trade))
    summary = _summarize(trade_rows)
    summary["fold_count"] = len(rows)
    summary["folds_with_trades"] = sum(1 for row in rows if _as_dict(_as_dict(row.get("backtest")).get("summary")).get("trade_count", 0))
    summary["closed_trades"] = len(trade_rows)
    return summary


def _by_symbol_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_symbol: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        for trade in _as_list(_as_dict(row.get("backtest")).get("trades")):
            trade_map = _as_dict(trade)
            by_symbol[str(trade_map.get("symbol") or "UNKNOWN")].append(trade_map)
    return {symbol: _summarize(trades) for symbol, trades in sorted(by_symbol.items())}


def _summary_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Canonical Surveyor Walk-Forward Ablation",
        "",
        "Expanding walk-forward test: thresholds are learned only from the train window, then applied to the next out-of-sample period.",
        "",
        "## Aggregate results",
        "",
        "| Stop policy | Setup | Target | Closed | Win | Avg R | Total bps | Folds with trades |",
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in sorted(report.get("aggregate", []), key=lambda r: (_float(_as_dict(r).get("summary", {}).get("avg_net_r_multiple")), _float(_as_dict(r).get("summary", {}).get("trade_count"))), reverse=True):
        summary = _as_dict(row.get("summary"))
        lines.append(
            f"| {row.get('stop_policy')} | {row.get('setup_id')} | {row.get('target')} | {summary.get('closed_trades', 0)} | {summary.get('win_rate', 0):.2%} | {summary.get('avg_net_r_multiple', 0):+.4f} | {summary.get('total_net_bps', 0):+.2f} | {summary.get('folds_with_trades', 0)} |"
        )
    lines.extend(["", "## Fold details", ""])
    for row in report.get("fold_results", []):
        summary = _as_dict(_as_dict(row.get("backtest")).get("summary"))
        lines.append(
            f"- `{row.get('fold_id')}` `{row.get('stop_policy')}` `{row.get('setup_id')}` `{row.get('target')}`: trades `{summary.get('trade_count', 0)}`, win `{summary.get('win_rate', 0):.2%}`, avgR `{summary.get('avg_net_r_multiple', 0):+.4f}`, bps `{summary.get('total_net_bps', 0):+.2f}`"
        )
    lines.extend(["", "## Interpretation", ""])
    for item in report.get("interpretation", []):
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Walk-forward canonical Surveyor stop/setup ablation.")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS)
    parser.add_argument("--timeframe", default="1d")
    parser.add_argument("--provider-id", default="binance_public_data")
    parser.add_argument("--venue", default="binance_usdm_futures")
    parser.add_argument("--output-dir", default="artifacts/strategy_backtests/canonical_surveyor_walk_forward")
    parser.add_argument("--liquidsniper-root", default=None)
    parser.add_argument("--max-hold-bars", type=int, default=30)
    args = parser.parse_args(argv)

    _ensure_liquidsniper_import(args.liquidsniper_root)
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    cfg = CanonicalSurveyorConfig(use_operator_core_bounds=True)
    conn = init_db(args.db_path)
    try:
        candles_by_symbol = {
            symbol: load_market_candles_from_db(conn, symbol=symbol, timeframe=args.timeframe, provider_id=args.provider_id, venue=args.venue)
            for symbol in symbols
        }
    finally:
        conn.close()

    profiles: list[dict[str, Any]] = []
    for symbol in symbols:
        symbol_profiles, _manifest = build_retest_profiles_for_symbol(symbol, candles_by_symbol[symbol], cfg)
        profiles.extend(symbol_profiles)
    base_dataset = build_foxian_retest_backtest_dataset(profiles)
    features_by_event = _enrich_profiles(profiles, candles_by_symbol)

    stop_specs = [
        {"stop_policy": "full_zone_5bps", "cap_risk_bps": None},
        {"stop_policy": "sweep_or_zone_adaptive", "cap_risk_bps": None},
        {"stop_policy": "core_5bps_baseline", "cap_risk_bps": None},
    ]
    setup_ids = [
        "all",
        "body_p50_selection_p50_family3",
        "selection_p50_family3",
        "body_p50_selection_p50",
        "body_p60_selection_p60_family3",
    ]
    targets = [2.0, 3.0]
    fold_results: list[dict[str, Any]] = []
    for fold in FOLDS:
        train_end = _parse_ts(str(fold["train_end"]))
        test_start = _parse_ts(str(fold["test_start"]))
        test_end = _parse_ts(str(fold["test_end"]))
        train_dataset = _dataset_subset(base_dataset, lambda ev, train_end=train_end: (_candidate_ts(ev) is not None and int(_candidate_ts(ev) or 0) <= train_end))
        thresholds = _train_thresholds(_as_list(train_dataset.get("evaluations")), features_by_event)
        for stop_spec in stop_specs:
            stopped_dataset, stop_manifest = _dataset_for_policy(
                base_dataset,
                profiles,
                candles_by_symbol,
                policy=str(stop_spec["stop_policy"]),
                cap_risk_bps=stop_spec["cap_risk_bps"],
            )
            stopped_test_dataset = _dataset_subset(stopped_dataset, _period_filter(test_start, test_end))
            for setup_id in setup_ids:
                setup_dataset = _apply_setup_filter(stopped_test_dataset, features_by_event, setup_id, thresholds)
                for target in targets:
                    backtest = run_ohlcv_backtest(
                        setup_dataset,
                        candles_by_symbol,
                        config=OHLCVBacktestConfig(timeframe=args.timeframe, max_hold_bars=args.max_hold_bars, target_rr=target),
                    )
                    fold_results.append({
                        "fold_id": fold["fold_id"],
                        "train_end": fold["train_end"],
                        "test_start": fold["test_start"],
                        "test_end": fold["test_end"],
                        "thresholds": thresholds,
                        "stop_policy": stop_spec["stop_policy"],
                        "cap_risk_bps": stop_spec["cap_risk_bps"],
                        "setup_id": setup_id,
                        "target": f"{target:g}R",
                        "stop_manifest": stop_manifest,
                        "input_candidates": len(_as_list(stopped_test_dataset.get("trade_candidates"))),
                        "filtered_candidates": len(_as_list(setup_dataset.get("trade_candidates"))),
                        "backtest": backtest,
                    })

    grouped: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in fold_results:
        grouped[(str(row["stop_policy"]), str(row["setup_id"]), str(row["target"]))].append(row)
    aggregate = []
    for (stop_policy, setup_id, target), rows in sorted(grouped.items()):
        aggregate.append({
            "stop_policy": stop_policy,
            "setup_id": setup_id,
            "target": target,
            "summary": _aggregate_summaries(rows),
            "by_symbol": _by_symbol_summary(rows),
        })

    report = {
        "contract": "canonical_surveyor_walk_forward_ablation_v0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbols": symbols,
        "timeframe": args.timeframe,
        "profile_count": len(profiles),
        "trade_candidate_count": len(_as_list(base_dataset.get("trade_candidates"))),
        "folds": FOLDS,
        "stop_specs": stop_specs,
        "setup_ids": setup_ids,
        "targets": ["2R", "3R"],
        "fold_results": fold_results,
        "aggregate": aggregate,
        "interpretation": [
            "This is the first out-of-sample check for the post-hoc setup/stop hypotheses; positive in-sample filters should be discounted unless they survive here.",
            "Thresholds are train-window medians/quantiles only, so each test period is not allowed to see its own body/selection distribution before filtering.",
            "If a variant is positive across multiple folds and symbols, it deserves promotion to an explicit Arbiter setup_score_v1 candidate; otherwise keep it as diagnostic only.",
        ],
    }
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    write_json(outdir / "walk_forward_report.json", report)
    (outdir / "SUMMARY.md").write_text(_summary_markdown(report), encoding="utf-8")
    print(json.dumps({
        "ok": True,
        "summary_path": str(outdir / "SUMMARY.md"),
        "report_path": str(outdir / "walk_forward_report.json"),
        "profiles": len(profiles),
        "trade_candidates": len(_as_list(base_dataset.get("trade_candidates"))),
        "aggregate_rows": len(aggregate),
        "fold_rows": len(fold_results),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
