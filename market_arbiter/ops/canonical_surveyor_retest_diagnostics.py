from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence

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


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _profile_event_id(profile: Mapping[str, Any]) -> str:
    events = _as_list(_as_dict(_as_dict(profile.get("datasets")).get("interaction_lifecycle")).get("payload", {}).get("events"))
    if events:
        return str(_as_dict(events[0]).get("event_id") or "")
    return str(_as_dict(profile.get("meta")).get("source_bundle_id") or "")


def _profile_zone(profile: Mapping[str, Any]) -> dict[str, Any]:
    sr = _as_dict(_as_dict(profile.get("datasets")).get("sr_zones"))
    zones = _as_list(_as_dict(sr.get("payload")).get("zones"))
    return _as_dict(zones[0]) if zones else {}


def _profile_event(profile: Mapping[str, Any]) -> dict[str, Any]:
    lifecycle = _as_dict(_as_dict(profile.get("datasets")).get("interaction_lifecycle"))
    events = _as_list(_as_dict(lifecycle.get("payload")).get("events"))
    return _as_dict(events[0]) if events else {}


def _candle_by_iso(candles: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    return {str(c.get("timestamp")): c for c in candles}


def _series_index_by_iso(candles: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return {str(c.get("timestamp")): idx for idx, c in enumerate(candles)}


def _sma(values: Sequence[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _atr(candles: Sequence[Mapping[str, Any]], idx: int, period: int = 14) -> float | None:
    if idx <= 0:
        return None
    start = max(1, idx - period + 1)
    trs = []
    for i in range(start, idx + 1):
        high = _float(candles[i].get("high"))
        low = _float(candles[i].get("low"))
        prev_close = _float(candles[i - 1].get("close"))
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return _sma(trs)


def _trend_features(candles: Sequence[Mapping[str, Any]], idx: int, side: str) -> dict[str, Any]:
    closes = [_float(c.get("close")) for c in candles]
    close = closes[idx]
    sma50 = _sma(closes[max(0, idx - 49) : idx + 1])
    sma200 = _sma(closes[max(0, idx - 199) : idx + 1])
    prior_20 = closes[idx - 20] if idx >= 20 else None
    ret20 = ((close / prior_20) - 1.0) if prior_20 and prior_20 > 0 else None
    aligned_50_200 = None
    aligned_ret20 = None
    if sma50 is not None and sma200 is not None:
        aligned_50_200 = (sma50 > sma200) if side == "long" else (sma50 < sma200)
    if ret20 is not None:
        aligned_ret20 = (ret20 > 0) if side == "long" else (ret20 < 0)
    return {
        "sma50": sma50,
        "sma200": sma200,
        "ret20": ret20,
        "trend_aligned_sma50_200": aligned_50_200,
        "trend_aligned_ret20": aligned_ret20,
    }


def _reaction_features(candle: Mapping[str, Any], *, side: str, low: float, high: float) -> dict[str, Any]:
    open_ = _float(candle.get("open"))
    close = _float(candle.get("close"))
    candle_high = _float(candle.get("high"))
    candle_low = _float(candle.get("low"))
    rng = max(candle_high - candle_low, 1e-9)
    body = abs(close - open_)
    body_ratio = body / rng
    if side == "long":
        reject_wick = (min(open_, close) - candle_low) / rng
        close_outside = close > high
        reclaim_mid = close > ((low + high) / 2.0)
        body_direction = close > open_
    else:
        reject_wick = (candle_high - max(open_, close)) / rng
        close_outside = close < low
        reclaim_mid = close < ((low + high) / 2.0)
        body_direction = close < open_
    return {
        "body_ratio": body_ratio,
        "reject_wick_ratio": reject_wick,
        "close_outside_zone": close_outside,
        "reclaim_mid": reclaim_mid,
        "body_direction_aligned": body_direction,
    }


def _enrich_profiles(profiles: Sequence[Mapping[str, Any]], candles_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, dict[str, Any]]:
    by_event: dict[str, dict[str, Any]] = {}
    candle_lookup = {symbol: _candle_by_iso(candles) for symbol, candles in candles_by_symbol.items()}
    index_lookup = {symbol: _series_index_by_iso(candles) for symbol, candles in candles_by_symbol.items()}
    for profile in profiles:
        event_id = _profile_event_id(profile)
        if not event_id:
            continue
        meta = _as_dict(profile.get("meta"))
        symbol = str(meta.get("symbol") or "").upper()
        zone = _profile_zone(profile)
        event = _profile_event(profile)
        event_ts = str(event.get("event_ts") or meta.get("as_of_ts") or "")
        candle = candle_lookup.get(symbol, {}).get(event_ts)
        idx = index_lookup.get(symbol, {}).get(event_ts)
        side = str(event.get("side") or "").lower()
        low = _float(zone.get("zone_low"))
        high = _float(zone.get("zone_high"))
        mid = _float(zone.get("zone_mid"), (low + high) / 2.0)
        features: dict[str, Any] = {
            "event_id": event_id,
            "symbol": symbol,
            "event_ts": event_ts,
            "side": side,
            "zone_id": zone.get("zone_id"),
            "zone_low": low,
            "zone_high": high,
            "zone_mid": mid,
            "zone_width_bps": ((high - low) / max(abs(mid), 1e-9)) * 10000.0 if mid > 0 else None,
            "selection_score": _float(zone.get("selection_score"), None),
            "strength_score": _float(zone.get("strength_score"), None),
            "quality_score": _float(zone.get("quality_score"), None),
            "merge_family_count": int(_float(zone.get("merge_family_count"), 0)),
            "candidate_sources": zone.get("candidate_sources") or zone.get("candidate_families") or [],
            "origin_kind": zone.get("origin_kind"),
            "bounds_kind": zone.get("bounds_kind"),
        }
        if candle is not None and idx is not None:
            features.update(_reaction_features(candle, side=side, low=low, high=high))
            features.update(_trend_features(candles_by_symbol[symbol], idx, side))
            atr = _atr(candles_by_symbol[symbol], idx, period=14)
            if atr:
                features["zone_width_atr"] = (high - low) / atr
                features["event_range_atr"] = (_float(candle.get("high")) - _float(candle.get("low"))) / atr
        by_event[event_id] = features
    return by_event


def _safe_quantile(values: list[float], q: float) -> float | None:
    values = sorted(v for v in values if math.isfinite(v))
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    idx = (len(values) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(values) - 1)
    frac = idx - lo
    return values[lo] * (1 - frac) + values[hi] * frac


def _summarize(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    n = len(rows)
    if not n:
        return {"trade_count": 0}
    wins = [r for r in rows if _float(r.get("net_r_multiple")) > 0]
    return {
        "trade_count": n,
        "win_rate": len(wins) / n,
        "avg_net_r_multiple": sum(_float(r.get("net_r_multiple")) for r in rows) / n,
        "total_net_bps": sum(_float(r.get("net_return_bps")) for r in rows),
        "avg_net_bps": sum(_float(r.get("net_return_bps")) for r in rows) / n,
        "median_risk_bps": _safe_quantile([_float(r.get("risk_bps")) for r in rows], 0.5),
    }


def _bucket(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, Any]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        value = row.get(key)
        if isinstance(value, bool):
            label = str(value).lower()
        elif value is None:
            label = "missing"
        elif isinstance(value, (int, float)):
            label = str(value)
        else:
            label = str(value)
        groups[label].append(row)
    return {label: _summarize(group) for label, group in sorted(groups.items())}


def _numeric_splits(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, Any]:
    vals = [_float(r.get(key), float("nan")) for r in rows]
    vals = [v for v in vals if math.isfinite(v)]
    thresholds = []
    for q in (0.25, 0.5, 0.75):
        val = _safe_quantile(vals, q)
        if val is not None:
            thresholds.append((f">=p{int(q*100)}:{val:.6g}", val))
    out = {}
    for label, threshold in thresholds:
        out[label] = _summarize([r for r in rows if _float(r.get(key), float("nan")) >= threshold])
    return out


def _passes_filter(row: Mapping[str, Any], filter_id: str) -> bool:
    if filter_id == "reaction_close_outside":
        return bool(row.get("close_outside_zone"))
    if filter_id == "reaction_aligned_body_and_close_outside":
        return bool(row.get("close_outside_zone")) and bool(row.get("body_direction_aligned"))
    if filter_id == "trend_sma50_200_aligned":
        return bool(row.get("trend_aligned_sma50_200"))
    if filter_id == "trend_ret20_aligned":
        return bool(row.get("trend_aligned_ret20"))
    if filter_id == "multi_family":
        return int(_float(row.get("merge_family_count"), 0)) >= 2
    if filter_id == "narrower_than_median_risk":
        return bool(row.get("risk_bps") is not None and row.get("_median_risk_bps") is not None and _float(row.get("risk_bps")) <= _float(row.get("_median_risk_bps")))
    if filter_id == "reaction_wick_top_half":
        return bool(row.get("reject_wick_ratio") is not None and row.get("_p50_reject_wick_ratio") is not None and _float(row.get("reject_wick_ratio")) >= _float(row.get("_p50_reject_wick_ratio")))
    if filter_id == "close_outside_and_trend":
        return bool(row.get("close_outside_zone")) and bool(row.get("trend_aligned_sma50_200"))
    if filter_id == "close_outside_and_multi_family":
        return bool(row.get("close_outside_zone")) and int(_float(row.get("merge_family_count"), 0)) >= 2
    if filter_id == "trend_and_multi_family":
        return bool(row.get("trend_aligned_sma50_200")) and int(_float(row.get("merge_family_count"), 0)) >= 2
    return True


def _filter_dataset(dataset: Mapping[str, Any], allowed_event_ids: set[str]) -> dict[str, Any]:
    evaluations = []
    for evaluation in _as_list(dataset.get("evaluations")):
        candidate = _as_dict(_as_dict(evaluation).get("trade_candidate"))
        if str(candidate.get("entry_event_id") or "") in allowed_event_ids:
            evaluations.append(evaluation)
    out = dict(dataset)
    out["evaluations"] = evaluations
    out["event_study_rows"] = [row for evaluation in evaluations if (row := _as_dict(evaluation).get("event_study_row"))]
    out["trade_candidates"] = [candidate for evaluation in evaluations if (candidate := _as_dict(evaluation).get("trade_candidate"))]
    return out


def _markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Canonical Surveyor Retest Diagnostics",
        "",
        "This pass diagnoses the canonical daily-major Surveyor retest replay before tuning. It treats Surveyor zones as level-quality evidence and tests Arbiter-side entry/filter hypotheses.",
        "",
        "## Baseline",
        "",
        "| Run | Closed | Win rate | Avg R | Total bps |",
        "|---|---:|---:|---:|---:|",
    ]
    for label, s in report["baseline"].items():
        lines.append(f"| {label} | {s['trade_count']} | {s['win_rate']:.2%} | {s['avg_net_r_multiple']:+.4f} | {s['total_net_bps']:+.2f} |")
    lines.extend(["", "## Ablations", "", "| Filter | Target | Kept | Win rate | Avg R | Total bps |", "|---|---|---:|---:|---:|---:|"])
    for item in report["ablations"]:
        s = item["summary"]
        lines.append(f"| {item['filter_id']} | {item['target']} | {s.get('trade_count',0)} | {s.get('win_rate',0):.2%} | {s.get('avg_net_r_multiple',0):+.4f} | {s.get('total_net_bps',0):+.2f} |")
    lines.extend(["", "## Strongest diagnostic buckets", ""])
    for key, bucket in report["buckets"].items():
        lines.append(f"### {key}")
        lines.append("")
        lines.append("| Bucket | Trades | Win rate | Avg R | Total bps |")
        lines.append("|---|---:|---:|---:|---:|")
        for label, s in bucket.items():
            lines.append(f"| {label} | {s.get('trade_count',0)} | {s.get('win_rate',0):.2%} | {s.get('avg_net_r_multiple',0):+.4f} | {s.get('total_net_bps',0):+.2f} |")
        lines.append("")
    lines.extend(["## Interpretation", ""])
    for item in report.get("interpretation", []):
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose canonical Surveyor retest backtest failure modes and explainable ablations.")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS)
    parser.add_argument("--timeframe", default="1d")
    parser.add_argument("--provider-id", default="binance_public_data")
    parser.add_argument("--venue", default="binance_usdm_futures")
    parser.add_argument("--liquidsniper-root", default=None)
    parser.add_argument("--output-dir", default="artifacts/strategy_backtests/canonical_surveyor_diagnostics")
    parser.add_argument("--targets", default="1,2")
    parser.add_argument("--max-hold-bars", type=int, default=30)
    parser.add_argument("--full-zone-bounds", action="store_true")
    args = parser.parse_args(argv)

    _ensure_liquidsniper_import(args.liquidsniper_root)
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    cfg = CanonicalSurveyorConfig(use_operator_core_bounds=not bool(args.full_zone_bounds))

    conn = init_db(args.db_path)
    try:
        candles_by_symbol = {
            symbol: load_market_candles_from_db(conn, symbol=symbol, timeframe=args.timeframe, provider_id=args.provider_id, venue=args.venue)
            for symbol in symbols
        }
    finally:
        conn.close()

    profiles = []
    manifests = {}
    for symbol in symbols:
        symbol_profiles, manifest = build_retest_profiles_for_symbol(symbol, candles_by_symbol[symbol], cfg)
        profiles.extend(symbol_profiles)
        manifests[symbol] = manifest

    dataset = build_foxian_retest_backtest_dataset(profiles)
    feature_by_event = _enrich_profiles(profiles, candles_by_symbol)
    reports = {}
    enriched_by_target: dict[str, list[dict[str, Any]]] = {}
    baseline = {}

    for target_raw in [item.strip() for item in str(args.targets).split(",") if item.strip()]:
        target = float(target_raw)
        label = f"{target:g}R"
        report = run_ohlcv_backtest(dataset, candles_by_symbol, config=OHLCVBacktestConfig(timeframe=args.timeframe, max_hold_bars=args.max_hold_bars, target_rr=target))
        reports[label] = report
        rows = []
        for trade in _as_list(report.get("trades")):
            event_id = str(_as_dict(trade).get("entry_event_id") or "")
            row = {**feature_by_event.get(event_id, {}), **_as_dict(trade)}
            rows.append(row)
        median_risk = _safe_quantile([_float(r.get("risk_bps")) for r in rows], 0.5)
        median_wick = _safe_quantile([_float(r.get("reject_wick_ratio"), float("nan")) for r in rows], 0.5)
        for row in rows:
            row["_median_risk_bps"] = median_risk
            row["_p50_reject_wick_ratio"] = median_wick
        enriched_by_target[label] = rows
        baseline[label] = _summarize(rows)

    # Use 2R as the main diagnostic surface because the prior daily rough test looked best there.
    diagnostic_rows = enriched_by_target.get("2R") or next(iter(enriched_by_target.values()))
    buckets = {
        "exit_reason_2R": _bucket(diagnostic_rows, "exit_reason"),
        "symbol_2R": _bucket(diagnostic_rows, "symbol"),
        "side_2R": _bucket(diagnostic_rows, "side"),
        "close_outside_zone_2R": _bucket(diagnostic_rows, "close_outside_zone"),
        "body_direction_aligned_2R": _bucket(diagnostic_rows, "body_direction_aligned"),
        "trend_aligned_sma50_200_2R": _bucket(diagnostic_rows, "trend_aligned_sma50_200"),
        "trend_aligned_ret20_2R": _bucket(diagnostic_rows, "trend_aligned_ret20"),
        "merge_family_count_2R": _bucket(diagnostic_rows, "merge_family_count"),
    }
    numeric = {
        "risk_bps_2R": _numeric_splits(diagnostic_rows, "risk_bps"),
        "reject_wick_ratio_2R": _numeric_splits(diagnostic_rows, "reject_wick_ratio"),
        "body_ratio_2R": _numeric_splits(diagnostic_rows, "body_ratio"),
        "selection_score_2R": _numeric_splits(diagnostic_rows, "selection_score"),
    }

    filter_ids = [
        "reaction_close_outside",
        "reaction_aligned_body_and_close_outside",
        "trend_sma50_200_aligned",
        "trend_ret20_aligned",
        "multi_family",
        "narrower_than_median_risk",
        "reaction_wick_top_half",
        "close_outside_and_trend",
        "close_outside_and_multi_family",
        "trend_and_multi_family",
    ]
    ablations = []
    # Use enriched 2R rows for filtering thresholds/booleans, then rerun both targets on filtered datasets.
    enriched_all = {str(r.get("entry_event_id") or ""): r for r in diagnostic_rows}
    for filter_id in filter_ids:
        allowed = {event_id for event_id, row in enriched_all.items() if _passes_filter(row, filter_id)}
        filtered_dataset = _filter_dataset(dataset, allowed)
        for target_raw in [item.strip() for item in str(args.targets).split(",") if item.strip()]:
            target = float(target_raw)
            label = f"{target:g}R"
            report = run_ohlcv_backtest(filtered_dataset, candles_by_symbol, config=OHLCVBacktestConfig(timeframe=args.timeframe, max_hold_bars=args.max_hold_bars, target_rr=target))
            ablations.append({
                "filter_id": filter_id,
                "target": label,
                "allowed_events": len(allowed),
                "summary": report.get("summary", {}),
                "coverage": report.get("coverage", {}),
            })

    interpretation = [
        "The baseline remains negative even with canonical daily-major zones, so the failure is not primarily level generation.",
        "If an ablation improves avg R while reducing trade count, treat it as an Arbiter entry/filter hypothesis, not as Surveyor SR scoring evidence.",
        "The most important next implementation boundary is to enrich Arbiter candidates with zone lifecycle/provenance fields so filters can be applied before trade construction rather than after simulation.",
    ]

    output = {
        "contract": "canonical_surveyor_retest_diagnostics_v0",
        "config": {**asdict(cfg), "targets": args.targets, "max_hold_bars": args.max_hold_bars, "full_zone_bounds": bool(args.full_zone_bounds)},
        "profiles": len(profiles),
        "trade_candidates": len(dataset.get("trade_candidates") or []),
        "baseline": baseline,
        "buckets": buckets,
        "numeric_splits": numeric,
        "ablations": ablations,
        "interpretation": interpretation,
        "symbol_manifests": manifests,
    }
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    report_path = outdir / "diagnostics_report.json"
    summary_path = outdir / "SUMMARY.md"
    write_json(report_path, output)
    summary_path.write_text(_markdown(output), encoding="utf-8")
    print(json.dumps({"ok": True, "report_path": str(report_path), "summary_path": str(summary_path), "profiles": len(profiles), "trade_candidates": len(dataset.get("trade_candidates") or [])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
