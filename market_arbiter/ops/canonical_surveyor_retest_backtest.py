from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from market_arbiter.arbiter.ohlcv_backtest import OHLCVBacktestConfig, run_ohlcv_backtest
from market_arbiter.arbiter.ohlcv_retest_adapter import load_market_candles_from_db, write_json
from market_arbiter.arbiter.strategy_backtest import build_foxian_retest_backtest_dataset
from market_arbiter.core.db import init_db
from market_arbiter.surveyor.sr_lifecycle import classify_sr_zone


CANONICAL_SURVEYOR_RETEST_CONTRACT = "canonical_surveyor_sr_retest_replay_v0"
DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT,BNBUSDT,XRPUSDT,SOLUSDT,TRXUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT"


@dataclass(frozen=True)
class CanonicalSurveyorConfig:
    min_history_bars: int = 365
    discovery_cadence_bars: int = 7
    daily_cluster_eps: float = 1.10
    daily_reaction_atr_min: float = 0.60
    daily_min_meaningful_touches: int = 5
    daily_min_zone_separation_bps: float = 250.0
    daily_min_strength: float = 70.0
    daily_max_zones: int = 8
    daily_require_first_retest_quality: bool = True
    use_operator_core_bounds: bool = True


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _ensure_liquidsniper_import(liquidsniper_root: str | None) -> None:
    candidates: list[Path] = []
    if liquidsniper_root:
        candidates.append(Path(liquidsniper_root).expanduser().resolve())
    candidates.append((_repo_root().parent / "LiquidSniper").resolve())
    for root in candidates:
        if (root / "liquidsniper" / "core" / "zone_engine_v3.py").exists():
            sys.path.insert(0, str(root))
            intraday = root / "IntradayTrading"
            if intraday.exists():
                sys.path.insert(0, str(intraday))
            return
    raise RuntimeError("Could not locate canonical LiquidSniper Surveyor zone_engine_v3 source")


def _ts(candle: Mapping[str, Any]) -> int:
    return int(candle["ts"])


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _as_surveyor_candle(candle: Mapping[str, Any]) -> dict[str, Any]:
    ts = _ts(candle)
    return {
        "open": float(candle["open"]),
        "high": float(candle["high"]),
        "low": float(candle["low"]),
        "close": float(candle["close"]),
        "close_time": _iso(ts),
        "timestamp": _iso(ts),
        "ts": _iso(ts),
    }


def _zone_bounds(zone: Mapping[str, Any], *, use_core: bool) -> tuple[float, float, float, str]:
    if use_core and zone.get("core_low") is not None and zone.get("core_high") is not None:
        low = float(zone["core_low"])
        high = float(zone["core_high"])
        mid = float(zone.get("core_mid") or ((low + high) / 2.0))
        return low, high, mid, str(zone.get("core_definition") or "operator_core")
    low = float(zone.get("zone_low") or 0.0)
    high = float(zone.get("zone_high") or 0.0)
    mid = float(zone.get("zone_mid") or ((low + high) / 2.0))
    return low, high, mid, "full_zone_bounds"


def _int_default(value: Any, default: int) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _side_from_asof_price(price: float, low: float, high: float) -> str | None:
    if price > high:
        return "long"
    if price < low:
        return "short"
    return None


def _touched_and_confirmed(candle: Mapping[str, Any], *, low: float, high: float, mid: float, side: str) -> bool:
    candle_low = float(candle["low"])
    candle_high = float(candle["high"])
    close = float(candle["close"])
    touched = candle_low <= high and candle_high >= low
    if not touched:
        return False
    if side == "long":
        return close > mid
    return close < mid


def _profile_for_retest(
    *,
    symbol: str,
    timeframe: str,
    asof_candle: Mapping[str, Any],
    event_candle: Mapping[str, Any],
    zone: Mapping[str, Any],
    side: str,
    low: float,
    high: float,
    mid: float,
    bounds_kind: str,
    selector_surface: str,
) -> dict[str, Any]:
    event_ts = event_candle["timestamp"]
    event_id = f"canonical-surveyor-retest:{symbol}:{timeframe}:{_ts(event_candle)}:{side}:{zone.get('zone_id')}"
    role = "support" if side == "long" else "resistance"
    quality_score = max(0.0, min(1.0, float(zone.get("selection_score") or zone.get("strength_score") or 0.0) / 100.0))
    formation_reaction_count = _int_default(
        zone.get("formation_reaction_count") or zone.get("reaction_count") or zone.get("meaningful_touches") or zone.get("touch_count"),
        3,
    )
    zone_row = {
        "zone_id": str(zone.get("zone_id")),
        "current_role": role,
        "origin_kind": zone.get("origin_kind") or zone.get("zone_kind"),
        "zone_low": low,
        "zone_high": high,
        "zone_mid": mid,
        "quality_score": quality_score,
        "selection_score": zone.get("selection_score"),
        "strength_score": zone.get("strength_score"),
        "historical_context_score": quality_score,
        "formation_reaction_count": formation_reaction_count,
        "retest_count": 1,
        "candidate_sources": zone.get("candidate_sources"),
        "candidate_families": zone.get("candidate_families"),
        "merge_family_count": zone.get("merge_family_count"),
        "selector_surface": selector_surface,
        "bounds_kind": bounds_kind,
        "full_zone_bounds": {
            "low": zone.get("zone_low"),
            "mid": zone.get("zone_mid"),
            "high": zone.get("zone_high"),
        },
        "core_bounds": {
            "low": zone.get("core_low"),
            "mid": zone.get("core_mid"),
            "high": zone.get("core_high"),
            "definition": zone.get("core_definition"),
        },
    }
    zone_row.update(classify_sr_zone(zone_row, policy={"formation_required_for_candidate": True}))
    return {
        "meta": {
            "symbol": symbol,
            "timeframe": timeframe,
            "as_of_ts": event_ts,
            "source_bundle_id": event_id,
            "adapter_contract": CANONICAL_SURVEYOR_RETEST_CONTRACT,
        },
        "datasets": {
            "feed_state": {
                "status": "replay_only",
                "payload": {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "event_ts": event_ts,
                    "source": "binance_public_data_usdm_futures",
                    "asof_ts": asof_candle["timestamp"],
                },
            },
            "structure_state": {
                "status": "replay_only",
                "payload": {"directional_bias": None},
            },
            "sr_zones": {
                "status": "replay_only",
                "payload": {
                    "contract": "authoritative_levels_view_v1",
                    "source": "canonical_surveyor_zone_engine_v3_daily_major",
                    "selector_surface": selector_surface,
                    "zones": [
                        zone_row
                    ],
                },
            },
            "interaction_lifecycle": {
                "status": "replay_only",
                "payload": {
                    "events": [
                        {
                            "event_id": event_id,
                            "event_type": "confirmed_retest",
                            "confirmation": "confirmed",
                            "event_ts": event_ts,
                            "zone_id": str(zone.get("zone_id")),
                            "side": side,
                            "retest_index": 1,
                            "price": event_candle.get("close"),
                        }
                    ]
                },
            },
            "fib_context": {"status": "replay_only", "payload": {}},
            "dynamic_levels": {"status": "replay_only", "payload": {}},
        },
    }


def _build_daily_major_zones(symbol: str, candles: Sequence[Mapping[str, Any]], cfg: CanonicalSurveyorConfig) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from liquidsniper.core.zone_engine_v3 import (  # type: ignore
        build_base_candidates,
        build_reaction_candidates,
        build_structure_candidates,
        merge_candidate_zones,
        score_zone,
        select_daily_majors,
    )

    surveyor_candles = [_as_surveyor_candle(candle) for candle in candles]
    kwargs = {
        "cluster_eps": cfg.daily_cluster_eps,
        "reaction_atr_min": cfg.daily_reaction_atr_min,
        "min_meaningful_touches": cfg.daily_min_meaningful_touches,
    }
    structure = build_structure_candidates(symbol, "1D", surveyor_candles, **kwargs)
    base = build_base_candidates(symbol, "1D", surveyor_candles, **kwargs)
    reaction = build_reaction_candidates(symbol, "1D", surveyor_candles, **kwargs)
    merged = merge_candidate_zones(structure, base, reaction)
    last_price = float(candles[-1]["close"])
    scored = [score_zone(zone, last_price=last_price) for zone in merged]
    kept = select_daily_majors(
        scored,
        min_strength=cfg.daily_min_strength,
        min_zone_separation_bps=cfg.daily_min_zone_separation_bps,
        max_zones=cfg.daily_max_zones,
        strict_retest_quality=cfg.daily_require_first_retest_quality,
        reference_price=last_price,
    )
    stats = {
        "structure_candidates": len(structure),
        "base_candidates": len(base),
        "reaction_candidates": len(reaction),
        "merged_candidates": len(merged),
        "selected_daily_majors": len(kept),
    }
    return [dict(zone) for zone in kept], stats


def build_retest_profiles_for_symbol(symbol: str, candles: Sequence[Mapping[str, Any]], cfg: CanonicalSurveyorConfig) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    if len(candles) <= cfg.min_history_bars + 1:
        return profiles, {"symbol": symbol, "snapshots": snapshots, "status": "insufficient_history", "candles": len(candles)}

    for asof_idx in range(cfg.min_history_bars - 1, len(candles) - 1, cfg.discovery_cadence_bars):
        asof_candle = candles[asof_idx]
        asof_price = float(asof_candle["close"])
        zones, stats = _build_daily_major_zones(symbol, candles[: asof_idx + 1], cfg)
        next_asof_idx = min(len(candles) - 1, asof_idx + cfg.discovery_cadence_bars)
        emitted_for_window: set[str] = set()
        events_this_window = 0

        for event_idx in range(asof_idx + 1, next_asof_idx + 1):
            event_candle = candles[event_idx]
            candidates: list[tuple[float, dict[str, Any], str, float, float, float, str]] = []
            for zone in zones:
                low, high, mid, bounds_kind = _zone_bounds(zone, use_core=cfg.use_operator_core_bounds)
                if high <= low or mid <= 0:
                    continue
                side = _side_from_asof_price(asof_price, low, high)
                if side is None:
                    continue
                zone_window_key = f"{zone.get('zone_id')}:{side}"
                if zone_window_key in emitted_for_window:
                    continue
                if not _touched_and_confirmed(event_candle, low=low, high=high, mid=mid, side=side):
                    continue
                distance_bps = abs(asof_price - mid) / max(abs(asof_price), 1e-9) * 10000.0
                score = float(zone.get("selection_score") or zone.get("strength_score") or 0.0)
                candidates.append((score - (0.01 * distance_bps), zone, side, low, high, mid, bounds_kind))

            if not candidates:
                continue
            # Avoid stacked same-day trades: pick the highest-ranked canonical zone touched on this candle.
            _rank, zone, side, low, high, mid, bounds_kind = sorted(candidates, key=lambda row: row[0], reverse=True)[0]
            emitted_for_window.add(f"{zone.get('zone_id')}:{side}")
            events_this_window += 1
            profiles.append(
                _profile_for_retest(
                    symbol=symbol,
                    timeframe="1d",
                    asof_candle=asof_candle,
                    event_candle=event_candle,
                    zone=zone,
                    side=side,
                    low=low,
                    high=high,
                    mid=mid,
                    bounds_kind=bounds_kind,
                    selector_surface="daily_major",
                )
            )

        snapshots.append(
            {
                "asof_idx": asof_idx,
                "asof_ts": asof_candle["timestamp"],
                "asof_price": asof_price,
                "zones_selected": len(zones),
                "events_next_window": events_this_window,
                **stats,
            }
        )

    return profiles, {
        "symbol": symbol,
        "status": "ok",
        "candles": len(candles),
        "snapshot_count": len(snapshots),
        "profile_count": len(profiles),
        "snapshots": snapshots,
    }


def _summary_markdown(*, symbols: list[str], cfg: CanonicalSurveyorConfig, dataset: Mapping[str, Any], rr_reports: Mapping[str, Mapping[str, Any]], manifest_path: str, dataset_path: str) -> str:
    lines = [
        "# Canonical Surveyor Daily-Major SR Retest Backtest",
        "",
        f"Date: {datetime.now(timezone.utc).date().isoformat()}",
        "",
        f"Universe: `{', '.join(symbols)}`",
        "",
        "Source candles: Binance Public Data USD-M futures daily candles already imported into `data/market_arbiter.sqlite`.",
        "",
        "Zone source: canonical LiquidSniper/Surveyor `zone_engine_v3` daily-major process (`structure`, `base`, and `reaction` families -> merge/arbitrate -> `select_daily_majors`).",
        "",
        "Replay shape: point-in-time weekly SR discovery using only candles available as of the discovery date, then daily retest scan until the next weekly discovery. One highest-ranked touched zone per symbol/day is emitted to avoid stacked same-day trades.",
        "",
        "## Config",
        "",
        f"- min history bars before first discovery: `{cfg.min_history_bars}`",
        f"- discovery cadence bars: `{cfg.discovery_cadence_bars}`",
        f"- daily max zones: `{cfg.daily_max_zones}`",
        f"- min strength: `{cfg.daily_min_strength}`",
        f"- min zone separation bps: `{cfg.daily_min_zone_separation_bps}`",
        f"- use operator core bounds: `{cfg.use_operator_core_bounds}`",
        "",
        "## Coverage",
        "",
        f"- profiles/evaluations: `{len(dataset.get('evaluations') or [])}`",
        f"- trade candidates: `{len(dataset.get('trade_candidates') or [])}`",
        f"- manifest: `{manifest_path}`",
        f"- dataset: `{dataset_path}`",
        "",
        "## Results",
        "",
        "| Target | Closed | Win rate | Avg R | Total net bps | Max DD bps | Skipped | Report |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for label, report in rr_reports.items():
        summary = report.get("summary", {}) if isinstance(report, Mapping) else {}
        coverage = report.get("coverage", {}) if isinstance(report, Mapping) else {}
        path = report.get("_artifact_path", "") if isinstance(report, Mapping) else ""
        lines.append(
            "| {label} | {closed} | {win:.2%} | {avg_r:+.4f} | {bps:+.2f} | {dd:+.2f} | {skipped} | `{path}` |".format(
                label=label,
                closed=int(summary.get("trade_count") or 0),
                win=float(summary.get("win_rate") or 0.0),
                avg_r=float(summary.get("avg_net_r_multiple") or 0.0),
                bps=float(summary.get("total_net_bps") or 0.0),
                dd=float(summary.get("max_drawdown_bps") or 0.0),
                skipped=int(coverage.get("skipped_trades") or 0),
                path=path,
            )
        )
    lines.extend([
        "",
        "## Caveats",
        "",
        "- This is much closer to the defined Surveyor process than the rough prior-window OHLCV adapter, but it is still a replay bridge: it imports the canonical Surveyor zone code from the sibling `LiquidSniper` workspace instead of a fully ported MarketArbiter-native module.",
        "- It is point-in-time by weekly discovery window, not a final event-sourced Surveyor bundle replay with persisted `sr_zones` + `interaction_lifecycle` snapshots.",
        "- Results should be treated as research evidence until the canonical zone engine is ported/wired directly into MarketArbiter and covered by fixtures.",
    ])
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run canonical Surveyor V3 daily-major SR zones through Foxian retest OHLCV backtest.")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS)
    parser.add_argument("--timeframe", default="1d")
    parser.add_argument("--provider-id", default="binance_public_data")
    parser.add_argument("--venue", default="binance_usdm_futures")
    parser.add_argument("--output-dir", default="artifacts/strategy_backtests/canonical_surveyor_top10_1d")
    parser.add_argument("--liquidsniper-root", default=None)
    parser.add_argument("--min-history-bars", type=int, default=365)
    parser.add_argument("--discovery-cadence-bars", type=int, default=7)
    parser.add_argument("--max-hold-bars", type=int, default=30)
    parser.add_argument("--targets", default="1,2")
    parser.add_argument("--full-zone-bounds", action="store_true", help="Use full selected zone bounds instead of operator core bounds for retest/stop.")
    args = parser.parse_args(argv)

    _ensure_liquidsniper_import(args.liquidsniper_root)
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    cfg = CanonicalSurveyorConfig(
        min_history_bars=max(30, int(args.min_history_bars)),
        discovery_cadence_bars=max(1, int(args.discovery_cadence_bars)),
        use_operator_core_bounds=not bool(args.full_zone_bounds),
    )

    conn = init_db(args.db_path)
    try:
        ohlcv_by_symbol: dict[str, list[dict[str, Any]]] = {}
        profiles: list[dict[str, Any]] = []
        symbol_manifests: dict[str, Any] = {}
        for symbol in symbols:
            candles = load_market_candles_from_db(conn, symbol=symbol, timeframe=args.timeframe, provider_id=args.provider_id, venue=args.venue)
            ohlcv_by_symbol[symbol] = candles
            symbol_profiles, manifest = build_retest_profiles_for_symbol(symbol, candles, cfg)
            profiles.extend(symbol_profiles)
            symbol_manifests[symbol] = manifest
    finally:
        conn.close()

    dataset = build_foxian_retest_backtest_dataset(profiles)
    dataset["source_adapter"] = {
        "contract": CANONICAL_SURVEYOR_RETEST_CONTRACT,
        "symbols": symbols,
        "timeframe": args.timeframe,
        "provider_id": args.provider_id,
        "venue": args.venue,
        "config": cfg.__dict__,
        "profile_count": len(profiles),
        "canonical_source": "LiquidSniper/liquidsniper/core/zone_engine_v3.py",
    }

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "contract": CANONICAL_SURVEYOR_RETEST_CONTRACT,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": args.db_path,
        "symbols": symbols,
        "timeframe": args.timeframe,
        "provider_id": args.provider_id,
        "venue": args.venue,
        "config": cfg.__dict__,
        "symbols_detail": symbol_manifests,
    }
    manifest_path = output_dir / "canonical_surveyor_zone_manifest.json"
    dataset_path = output_dir / "canonical_surveyor_retest_dataset.json"
    write_json(manifest_path, manifest)
    write_json(dataset_path, dataset)

    rr_reports: dict[str, dict[str, Any]] = {}
    for target_raw in [item.strip() for item in str(args.targets).split(",") if item.strip()]:
        target = float(target_raw)
        label = f"{target:g}R"
        report = run_ohlcv_backtest(
            dataset,
            ohlcv_by_symbol,
            config=OHLCVBacktestConfig(timeframe=args.timeframe, max_hold_bars=int(args.max_hold_bars), target_rr=target),
        )
        report["source_adapter"] = dataset["source_adapter"]
        report_path = output_dir / f"canonical_surveyor_retest_report_rr{target:g}.json"
        write_json(report_path, report)
        report["_artifact_path"] = str(report_path)
        rr_reports[label] = report

    summary_path = output_dir / "SUMMARY.md"
    summary_path.write_text(
        _summary_markdown(
            symbols=symbols,
            cfg=cfg,
            dataset=dataset,
            rr_reports=rr_reports,
            manifest_path=str(manifest_path),
            dataset_path=str(dataset_path),
        ),
        encoding="utf-8",
    )
    print(json.dumps({
        "ok": True,
        "summary_path": str(summary_path),
        "manifest_path": str(manifest_path),
        "dataset_path": str(dataset_path),
        "profiles": len(profiles),
        "trade_candidates": len(dataset.get("trade_candidates") or []),
        "reports": {label: report.get("_artifact_path") for label, report in rr_reports.items()},
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
