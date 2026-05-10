from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from copy import deepcopy
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
from market_arbiter.ops.canonical_surveyor_retest_diagnostics import (
    _as_dict,
    _as_list,
    _atr,
    _float,
    _profile_event,
    _profile_event_id,
    _profile_zone,
    _series_index_by_iso,
    _summarize,
)


def _full_bounds(zone: Mapping[str, Any], *, fallback_low: float, fallback_high: float) -> tuple[float, float]:
    full = _as_dict(zone.get("full_zone_bounds"))
    low = _float(full.get("low"), fallback_low)
    high = _float(full.get("high"), fallback_high)
    if high <= low:
        return fallback_low, fallback_high
    return low, high


def _adaptive_buffer(*, price: float, atr: float | None, atr_fraction: float = 0.15, min_bps: float = 10.0) -> float:
    atr_part = (atr or 0.0) * atr_fraction
    bps_part = price * min_bps / 10_000.0
    return max(atr_part, bps_part)


def _pivot_lows(candles: Sequence[Mapping[str, Any]], *, end_idx: int, lookback: int = 90, k: int = 2) -> list[tuple[int, float]]:
    out: list[tuple[int, float]] = []
    start = max(k, end_idx - lookback)
    stop = max(k, end_idx - k + 1)
    for idx in range(start, stop):
        low = _float(candles[idx].get("low"))
        window = [_float(candles[j].get("low")) for j in range(idx - k, idx + k + 1)]
        if low <= min(window):
            out.append((idx, low))
    return out


def _pivot_highs(candles: Sequence[Mapping[str, Any]], *, end_idx: int, lookback: int = 90, k: int = 2) -> list[tuple[int, float]]:
    out: list[tuple[int, float]] = []
    start = max(k, end_idx - lookback)
    stop = max(k, end_idx - k + 1)
    for idx in range(start, stop):
        high = _float(candles[idx].get("high"))
        window = [_float(candles[j].get("high")) for j in range(idx - k, idx + k + 1)]
        if high >= max(window):
            out.append((idx, high))
    return out


def _swing_stop(
    *,
    candles: Sequence[Mapping[str, Any]],
    event_idx: int,
    side: str,
    zone_low: float,
    zone_high: float,
    atr: float | None,
    price: float,
) -> tuple[float | None, str]:
    buffer = _adaptive_buffer(price=price, atr=atr, atr_fraction=0.10, min_bps=5.0)
    if side == "long":
        lows = _pivot_lows(candles, end_idx=event_idx, lookback=90, k=2)
        # Prefer the nearest structural low outside/below the zone; if unavailable,
        # allow a low inside the lower half of the zone as a weaker structure proxy.
        below = [(idx, low) for idx, low in lows if low <= zone_low]
        inside = [(idx, low) for idx, low in lows if zone_low < low <= ((zone_low + zone_high) / 2.0)]
        if below:
            _idx, level = max(below, key=lambda row: row[1])
            return level - buffer, "swing_below_zone"
        if inside:
            _idx, level = min(inside, key=lambda row: row[1])
            return level - buffer, "swing_inside_lower_half"
        return None, "no_swing"
    highs = _pivot_highs(candles, end_idx=event_idx, lookback=90, k=2)
    above = [(idx, high) for idx, high in highs if high >= zone_high]
    inside = [(idx, high) for idx, high in highs if ((zone_low + zone_high) / 2.0) <= high < zone_high]
    if above:
        _idx, level = min(above, key=lambda row: row[1])
        return level + buffer, "swing_above_zone"
    if inside:
        _idx, level = max(inside, key=lambda row: row[1])
        return level + buffer, "swing_inside_upper_half"
    return None, "no_swing"


def _entry_idx(candles: Sequence[Mapping[str, Any]], event_idx: int) -> int | None:
    idx = event_idx + 1
    return idx if idx < len(candles) else None


def _risk_bps(*, entry: float, stop: float, side: str) -> float:
    if entry <= 0 or stop <= 0:
        return float("inf")
    risk = entry - stop if side == "long" else stop - entry
    if risk <= 0:
        return float("inf")
    return risk / entry * 10_000.0


def _stop_for_policy(
    *,
    policy: str,
    candles: Sequence[Mapping[str, Any]],
    event_idx: int,
    side: str,
    zone: Mapping[str, Any],
    cap_risk_bps: float | None = None,
) -> tuple[float | None, dict[str, Any]]:
    event_candle = candles[event_idx]
    entry_i = _entry_idx(candles, event_idx)
    if entry_i is None:
        return None, {"skip_reason": "no_entry_candle"}
    entry = _float(candles[entry_i].get("open"))
    core_low = _float(zone.get("zone_low"))
    core_high = _float(zone.get("zone_high"))
    full_low, full_high = _full_bounds(zone, fallback_low=core_low, fallback_high=core_high)
    atr = _atr(candles, event_idx, period=14)
    ref_price = _float(event_candle.get("close"), entry)

    if policy == "core_5bps_baseline":
        stop = core_low * (1 - 5.0 / 10_000.0) if side == "long" else core_high * (1 + 5.0 / 10_000.0)
        source = "core_boundary_5bps"
    elif policy == "full_zone_5bps":
        stop = full_low * (1 - 5.0 / 10_000.0) if side == "long" else full_high * (1 + 5.0 / 10_000.0)
        source = "full_boundary_5bps"
    elif policy == "full_zone_adaptive":
        buffer = _adaptive_buffer(price=ref_price, atr=atr, atr_fraction=0.15, min_bps=10.0)
        stop = full_low - buffer if side == "long" else full_high + buffer
        source = "full_boundary_adaptive"
    elif policy.startswith("swing_or_zone"):
        swing, swing_source = _swing_stop(candles=candles, event_idx=event_idx, side=side, zone_low=full_low, zone_high=full_high, atr=atr, price=ref_price)
        if swing is not None:
            stop = swing
            source = swing_source
        else:
            buffer = _adaptive_buffer(price=ref_price, atr=atr, atr_fraction=0.15, min_bps=10.0)
            stop = full_low - buffer if side == "long" else full_high + buffer
            source = "fallback_full_boundary_adaptive"
    elif policy.startswith("sweep_or_zone"):
        buffer = _adaptive_buffer(price=ref_price, atr=atr, atr_fraction=0.10, min_bps=5.0)
        if side == "long" and _float(event_candle.get("low")) < full_low:
            stop = _float(event_candle.get("low")) - buffer
            source = "sweep_wick_low"
        elif side == "short" and _float(event_candle.get("high")) > full_high:
            stop = _float(event_candle.get("high")) + buffer
            source = "sweep_wick_high"
        else:
            buffer = _adaptive_buffer(price=ref_price, atr=atr, atr_fraction=0.15, min_bps=10.0)
            stop = full_low - buffer if side == "long" else full_high + buffer
            source = "fallback_full_boundary_adaptive"
    else:
        raise ValueError(f"unknown stop policy: {policy}")

    risk = _risk_bps(entry=entry, stop=stop, side=side)
    if not math.isfinite(risk):
        return None, {"skip_reason": "invalid_stop_risk", "risk_bps": risk, "stop_source": source}
    if cap_risk_bps is not None and risk > cap_risk_bps:
        return None, {"skip_reason": "risk_cap_exceeded", "risk_bps": risk, "cap_risk_bps": cap_risk_bps, "stop_source": source}
    return stop, {"stop_source": source, "risk_bps": risk, "atr": atr, "entry_open": entry}


def _dataset_for_policy(
    dataset: Mapping[str, Any],
    profiles: Sequence[Mapping[str, Any]],
    candles_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    policy: str,
    cap_risk_bps: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    profile_by_event = {_profile_event_id(profile): profile for profile in profiles}
    index_by_symbol = {symbol: _series_index_by_iso(candles) for symbol, candles in candles_by_symbol.items()}
    out = deepcopy(dict(dataset))
    kept_evaluations: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    source_counts: Counter[str] = Counter()
    risk_values: list[float] = []

    for evaluation in _as_list(dataset.get("evaluations")):
        ev = deepcopy(_as_dict(evaluation))
        candidate = _as_dict(ev.get("trade_candidate"))
        event_id = str(candidate.get("entry_event_id") or "")
        profile = profile_by_event.get(event_id)
        if not profile or not candidate:
            continue
        symbol = str(candidate.get("symbol") or "").upper()
        event = _profile_event(profile)
        event_ts = str(event.get("event_ts") or "")
        event_idx = index_by_symbol.get(symbol, {}).get(event_ts)
        if event_idx is None:
            skipped.append({"event_id": event_id, "skip_reason": "missing_event_idx"})
            continue
        zone = _profile_zone(profile)
        side = str(candidate.get("side") or "").lower()
        stop, meta = _stop_for_policy(policy=policy, candles=candles_by_symbol[symbol], event_idx=event_idx, side=side, zone=zone, cap_risk_bps=cap_risk_bps)
        if stop is None:
            skipped.append({"event_id": event_id, **meta})
            continue
        candidate["invalidation_level_hint"] = stop
        candidate["stop_buffer_bps"] = 0.0
        candidate["stop_policy"] = f"{policy}_exact_stop"
        candidate["stop_policy_meta"] = meta
        ev["trade_candidate"] = candidate
        kept_evaluations.append(ev)
        source_counts[str(meta.get("stop_source") or "unknown")] += 1
        risk_values.append(float(meta.get("risk_bps") or 0.0))

    out["evaluations"] = kept_evaluations
    out["event_study_rows"] = [row for ev in kept_evaluations if (row := _as_dict(ev).get("event_study_row"))]
    out["trade_candidates"] = [candidate for ev in kept_evaluations if (candidate := _as_dict(ev).get("trade_candidate"))]
    manifest = {
        "policy": policy,
        "cap_risk_bps": cap_risk_bps,
        "input_evaluations": len(_as_list(dataset.get("evaluations"))),
        "kept_evaluations": len(kept_evaluations),
        "policy_skipped": len(skipped),
        "skip_reasons": dict(Counter(str(row.get("skip_reason")) for row in skipped)),
        "stop_source_counts": dict(source_counts),
        "risk_bps_avg": sum(risk_values) / len(risk_values) if risk_values else None,
        "risk_bps_min": min(risk_values) if risk_values else None,
        "risk_bps_max": max(risk_values) if risk_values else None,
        "skipped_examples": skipped[:20],
    }
    return out, manifest


def _summary_md(report: Mapping[str, Any]) -> str:
    lines = [
        "# Canonical Surveyor Stop-Policy Ablation",
        "",
        "Same canonical daily-major Surveyor retest candidates; only invalidation/stop placement changes.",
        "",
        "| Policy | Cap risk bps | Target | Kept | Closed | Skipped by policy | Win | Avg R | Total bps | Avg risk bps | Stop sources |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in report["runs"]:
        s = row["backtest"].get("summary", {})
        cov = row["backtest"].get("coverage", {})
        manifest = row["manifest"]
        sources = ", ".join(f"{k}:{v}" for k, v in sorted(manifest.get("stop_source_counts", {}).items()))
        lines.append(
            f"| {row['policy']} | {row.get('cap_risk_bps') or ''} | {row['target']} | {manifest['kept_evaluations']} | {cov.get('closed_trades', 0)} | {manifest['policy_skipped']} | {s.get('win_rate', 0):.2%} | {s.get('avg_net_r_multiple', 0):+.4f} | {s.get('total_net_bps', 0):+.2f} | {(manifest.get('risk_bps_avg') or 0):.1f} | {sources} |"
        )
    lines.extend(["", "## Interpretation", ""])
    for item in report.get("interpretation", []):
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ablate Arbiter stop/invalidation policies on canonical Surveyor daily retests.")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS)
    parser.add_argument("--timeframe", default="1d")
    parser.add_argument("--provider-id", default="binance_public_data")
    parser.add_argument("--venue", default="binance_usdm_futures")
    parser.add_argument("--output-dir", default="artifacts/strategy_backtests/canonical_surveyor_stop_ablation")
    parser.add_argument("--liquidsniper-root", default=None)
    parser.add_argument("--targets", default="1,2")
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
    dataset = build_foxian_retest_backtest_dataset(profiles)

    policy_specs: list[tuple[str, float | None]] = [
        ("core_5bps_baseline", None),
        ("full_zone_5bps", None),
        ("full_zone_adaptive", None),
        ("swing_or_zone_adaptive", None),
        ("swing_or_zone_adaptive", 800.0),
        ("swing_or_zone_adaptive", 600.0),
        ("sweep_or_zone_adaptive", None),
        ("sweep_or_zone_adaptive", 800.0),
        ("sweep_or_zone_adaptive", 600.0),
    ]
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    runs: list[dict[str, Any]] = []
    for policy, cap in policy_specs:
        policy_dataset, manifest = _dataset_for_policy(dataset, profiles, candles_by_symbol, policy=policy, cap_risk_bps=cap)
        safe_cap = "uncapped" if cap is None else f"cap{cap:g}"
        policy_dir = outdir / f"{policy}_{safe_cap}"
        policy_dir.mkdir(parents=True, exist_ok=True)
        write_json(policy_dir / "dataset.json", policy_dataset)
        write_json(policy_dir / "manifest.json", manifest)
        for target_raw in [item.strip() for item in args.targets.split(",") if item.strip()]:
            target = float(target_raw)
            target_label = f"{target:g}R"
            backtest = run_ohlcv_backtest(
                policy_dataset,
                candles_by_symbol,
                config=OHLCVBacktestConfig(timeframe=args.timeframe, max_hold_bars=args.max_hold_bars, target_rr=target),
            )
            write_json(policy_dir / f"report_{target_label}.json", backtest)
            runs.append({
                "policy": policy,
                "cap_risk_bps": cap,
                "target": target_label,
                "manifest": manifest,
                "backtest": backtest,
                "artifact_dir": str(policy_dir),
            })

    report = {
        "contract": "canonical_surveyor_stop_policy_ablation_v0",
        "symbols": symbols,
        "timeframe": args.timeframe,
        "profiles": len(profiles),
        "input_trade_candidates": len(dataset.get("trade_candidates") or []),
        "runs": runs,
        "interpretation": [
            "This isolates stop/invalidation placement while keeping canonical Surveyor daily-major zones and retest events fixed.",
            "Capped policies answer whether avoiding massive invalidation gaps improves expectancy or merely discards too many trades.",
            "If a stop policy improves avg R without collapsing trade count, promote it into Arbiter stop_policy_v1 and then combine with setup-score filters in a walk-forward test.",
        ],
    }
    write_json(outdir / "stop_ablation_report.json", report)
    (outdir / "SUMMARY.md").write_text(_summary_md(report), encoding="utf-8")
    print(json.dumps({"ok": True, "summary_path": str(outdir / "SUMMARY.md"), "report_path": str(outdir / "stop_ablation_report.json"), "profiles": len(profiles), "trade_candidates": len(dataset.get("trade_candidates") or [])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
