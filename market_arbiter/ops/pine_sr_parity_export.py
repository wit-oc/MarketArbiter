"""Export golden Surveyor daily-major SR zones for Pine parity checks.

This command intentionally calls the canonical LiquidSniper Surveyor V3 zone path
locked in docs/PINE_SURVEYOR_SR_PARITY_MAP_V1.md.  It produces point-in-time
selected daily-major zones for fixed as-of dates so the TradingView Pine port can
compare selected bounds/ranks against Python output.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence


GOLDEN_CONTRACT = "pine_surveyor_sr_daily_major_golden_v1"
DEFAULT_ASOF_DATES = "2023-01-01,2024-01-01,2025-01-01,2026-03-31"
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_TIMEFRAME = "1d"
DEFAULT_PROVIDER_ID = "binance_public_data"
DEFAULT_VENUE = "binance_usdm_futures"
DEFAULT_OUTPUT_JSON = "artifacts/pine_sr_parity/btcusdt_1d_golden_zones.json"
DEFAULT_OUTPUT_MD = "artifacts/pine_sr_parity/btcusdt_1d_golden_zones_summary.md"


@dataclass(frozen=True)
class DailyMajorConfig:
    min_history_bars: int = 365
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


def _ensure_liquidsniper_import(liquidsniper_root: str | None) -> Path:
    candidates: list[Path] = []
    if liquidsniper_root:
        candidates.append(Path(liquidsniper_root).expanduser().resolve())
    candidates.append((_repo_root().parent / "LiquidSniper").resolve())
    candidates.append(Path("/Users/wit/.openclaw/workspace/LiquidSniper"))

    for root in candidates:
        if (root / "liquidsniper" / "core" / "zone_engine_v3.py").exists():
            sys.path.insert(0, str(root))
            intraday = root / "IntradayTrading"
            if intraday.exists():
                sys.path.insert(0, str(intraday))
            return root
    searched = ", ".join(str(path) for path in candidates)
    raise RuntimeError(f"Could not locate canonical LiquidSniper Surveyor source; searched: {searched}")


def _parse_utc_date(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise ValueError("empty as-of date")
    if "T" in text:
        text = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def _iso_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _as_float(value: Any) -> float:
    return float(value) if value is not None else math.nan


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return _jsonable(value.item())
        except Exception:
            pass
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    return value


def _load_market_candles(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    timeframe: str,
    provider_id: str,
    venue: str,
    asof_ts_ms: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            provider_id, venue, symbol, timeframe, ts_open_ms, ts_close_ms,
            open, high, low, close, volume, ingest_ts_ms, dataset_version, trace_id
        FROM market_candles
        WHERE symbol = ?
          AND timeframe = ?
          AND provider_id = ?
          AND venue = ?
          AND ts_open_ms <= ?
        ORDER BY ts_open_ms ASC;
        """,
        (symbol, timeframe, provider_id, venue, asof_ts_ms),
    ).fetchall()
    candles: list[dict[str, Any]] = []
    for row in rows:
        ts_open_ms = int(row[4])
        candle = {
            "provider_id": row[0],
            "venue": row[1],
            "symbol": row[2],
            "timeframe": row[3],
            "ts_open_ms": ts_open_ms,
            "ts_close_ms": int(row[5]),
            "ts": ts_open_ms // 1000,
            "timestamp": _iso_from_ms(ts_open_ms),
            "close_time": _iso_from_ms(int(row[5])),
            "open": float(row[6]),
            "high": float(row[7]),
            "low": float(row[8]),
            "close": float(row[9]),
            "volume": float(row[10]),
            "ingest_ts_ms": int(row[11]),
            "dataset_version": row[12],
            "trace_id": row[13],
        }
        candles.append(candle)
    return candles


def _surveyor_candles(candles: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "open": float(candle["open"]),
            "high": float(candle["high"]),
            "low": float(candle["low"]),
            "close": float(candle["close"]),
            "close_time": str(candle["close_time"]),
            "timestamp": str(candle["timestamp"]),
            "ts": str(candle["timestamp"]),
        }
        for candle in candles
    ]


def _zone_export(zone: Mapping[str, Any]) -> dict[str, Any]:
    full_low = _as_float(zone.get("zone_low"))
    full_high = _as_float(zone.get("zone_high"))
    core_low = zone.get("core_low")
    core_high = zone.get("core_high")
    exported = {
        "selector_rank": zone.get("selector_rank"),
        "selector_surface": zone.get("selector_surface"),
        "selector_status": zone.get("selector_status"),
        "selector_reason": zone.get("selector_reason"),
        "zone_id": zone.get("zone_id"),
        "status": zone.get("status"),
        "zone_kind": zone.get("zone_kind"),
        "origin_kind": zone.get("origin_kind"),
        "current_role": zone.get("current_role"),
        "relative_position": zone.get("relative_position"),
        "lifecycle_state": zone.get("lifecycle_state"),
        "full_zone_bounds": {
            "low": zone.get("zone_low"),
            "high": zone.get("zone_high"),
            "mid": zone.get("zone_mid"),
            "width": (full_high - full_low) if full_high >= full_low else None,
            "width_bps": zone.get("zone_width_bps"),
            "width_atr": zone.get("zone_width_atr"),
        },
        "operator_core_bounds": {
            "low": core_low,
            "high": core_high,
            "mid": zone.get("core_mid"),
            "definition": zone.get("core_definition"),
            "display_bounds_kind": zone.get("display_bounds_kind"),
        },
        "scores": {
            "strength_score": zone.get("strength_score"),
            "selection_score": zone.get("selection_score"),
            "reaction_score": zone.get("reaction_score"),
            "reaction_efficiency_score": zone.get("reaction_efficiency_score"),
            "carry_score": zone.get("carry_score"),
            "body_respect_score": zone.get("body_respect_score"),
            "family_confluence_bonus": zone.get("family_confluence_bonus"),
            "daily_major_provenance_weight": zone.get("daily_major_provenance_weight"),
            "retest_weight": zone.get("retest_weight"),
        },
        "families": {
            "candidate_family": zone.get("candidate_family"),
            "source_family": zone.get("source_family"),
            "source_family_primary": zone.get("source_family_primary"),
            "source_family_display": zone.get("source_family_display"),
            "candidate_sources": zone.get("candidate_sources"),
            "candidate_families": zone.get("candidate_families"),
            "merge_family_count": zone.get("merge_family_count"),
            "source_versions": zone.get("source_versions"),
            "generator_contracts": zone.get("generator_contracts"),
            "provenance_summary": zone.get("provenance_summary"),
        },
        "touch_reaction_fields": {
            "meaningful_touch_count": zone.get("meaningful_touch_count"),
            "first_touch_state": zone.get("first_touch_state"),
            "interaction_role": zone.get("interaction_role"),
            "interaction_buy": zone.get("interaction_buy"),
            "interaction_sell": zone.get("interaction_sell"),
        },
        "daily_major_diagnostics": zone.get("daily_major_diagnostics"),
        "daily_pocket": {
            "contract": zone.get("daily_pocket_contract"),
            "id": zone.get("daily_pocket_id"),
            "member_count": zone.get("daily_pocket_member_count"),
            "member_ids": zone.get("daily_pocket_member_ids"),
            "demoted_ids": zone.get("daily_pocket_demoted_ids"),
            "reason": zone.get("daily_pocket_reason"),
        },
        "arbitration_diagnostics": zone.get("arbitration_diagnostics"),
    }
    return _jsonable(exported)


def _build_daily_major_snapshot(
    *,
    symbol: str,
    candles: Sequence[Mapping[str, Any]],
    cfg: DailyMajorConfig,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from liquidsniper.core.zone_engine_v3 import (  # type: ignore
        build_base_candidates,
        build_reaction_candidates,
        build_structure_candidates,
        merge_candidate_zones,
        score_zone,
        select_daily_majors,
    )

    surveyor_candles = _surveyor_candles(candles)
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
        "scored_candidates": len(scored),
        "selected_daily_majors": len(kept),
    }
    ranked = sorted(kept, key=lambda zone: int(zone.get("selector_rank") or 9999))
    return [_zone_export(dict(zone)) for zone in ranked], stats


def _summary_markdown(dataset: Mapping[str, Any], json_path: Path) -> str:
    lines = [
        "# BTCUSDT 1D Surveyor SR Pine Parity Golden Zones",
        "",
        f"Generated at: `{dataset.get('generated_at')}`",
        f"JSON artifact: `{json_path}`",
        "",
        "These fixtures are point-in-time outputs from the canonical LiquidSniper Surveyor V3 daily-major SR path, using MarketArbiter's imported Binance USD-M futures daily candles.",
        "",
        "## Data-source caveat",
        "",
        str(dataset.get("data_source_caveat")),
        "",
        "## Config",
        "",
    ]
    cfg = dataset.get("config") or {}
    for key in sorted(cfg):
        lines.append(f"- `{key}`: `{cfg[key]}`")
    lines.extend([
        "",
        "## Snapshots",
        "",
        "| As-of date | Candle count | Last close | Candidates S/B/R/M | Selected | Zone ranks |",
        "|---|---:|---:|---:|---:|---|",
    ])
    for snapshot in dataset.get("snapshots") or []:
        counts = snapshot.get("candidate_counts") or {}
        ranks = []
        for zone in snapshot.get("zones") or []:
            bounds = zone.get("operator_core_bounds") or {}
            scores = zone.get("scores") or {}
            families = zone.get("families") or {}
            ranks.append(
                "#{rank} {kind} core {low:.2f}-{high:.2f} score {score:.2f} families {families}".format(
                    rank=int(zone.get("selector_rank") or 0),
                    kind=zone.get("zone_kind") or "zone",
                    low=float(bounds.get("low") or 0.0),
                    high=float(bounds.get("high") or 0.0),
                    score=float(scores.get("selection_score") or 0.0),
                    families=",".join(families.get("candidate_families") or []),
                )
            )
        lines.append(
            "| {date} | {candles} | {close:.2f} | {s}/{b}/{r}/{m} | {selected} | {ranks} |".format(
                date=snapshot.get("asof_date"),
                candles=int(snapshot.get("candle_count") or 0),
                close=float((snapshot.get("source_candle") or {}).get("close") or 0.0),
                s=int(counts.get("structure_candidates") or 0),
                b=int(counts.get("base_candidates") or 0),
                r=int(counts.get("reaction_candidates") or 0),
                m=int(counts.get("merged_candidates") or 0),
                selected=int(counts.get("selected_daily_majors") or 0),
                ranks="<br>".join(ranks),
            )
        )
    lines.extend([
        "",
        "## Pine parity use",
        "",
        "Compare Pine-selected daily-major operator-core bounds and selector ranks against the JSON `operator_core_bounds`/`selector_rank` values for each fixed as-of date. Full macro bounds are present under `full_zone_bounds` for debugging divergence.",
    ])
    return "\n".join(lines) + "\n"


def build_dataset(args: argparse.Namespace) -> dict[str, Any]:
    cfg = DailyMajorConfig(
        min_history_bars=max(30, int(args.min_history_bars)),
        daily_max_zones=max(1, int(args.max_zones)),
    )
    liquidsniper_root = _ensure_liquidsniper_import(args.liquidsniper_root)
    asof_dates = [_parse_utc_date(item) for item in str(args.asof_dates).split(",") if item.strip()]
    db_path = Path(args.db_path).expanduser()

    snapshots: list[dict[str, Any]] = []
    conn = sqlite3.connect(str(db_path))
    try:
        for asof_dt in asof_dates:
            asof_ts_ms = int(asof_dt.timestamp() * 1000)
            candles = _load_market_candles(
                conn,
                symbol=args.symbol,
                timeframe=args.timeframe,
                provider_id=args.provider_id,
                venue=args.venue,
                asof_ts_ms=asof_ts_ms,
            )
            if len(candles) < cfg.min_history_bars:
                raise RuntimeError(
                    f"{args.symbol} {args.timeframe} has only {len(candles)} candles by {asof_dt.date().isoformat()}; "
                    f"need at least {cfg.min_history_bars}"
                )
            zones, counts = _build_daily_major_snapshot(symbol=args.symbol, candles=candles, cfg=cfg)
            first = candles[0]
            last = candles[-1]
            snapshots.append(
                {
                    "asof_date": asof_dt.date().isoformat(),
                    "requested_asof_timestamp": asof_dt.isoformat().replace("+00:00", "Z"),
                    "resolved_asof_timestamp": last["timestamp"],
                    "resolved_asof_ts_open_ms": last["ts_open_ms"],
                    "candle_count": len(candles),
                    "source_candle_window": {
                        "first_ts": first["timestamp"],
                        "first_ts_open_ms": first["ts_open_ms"],
                        "last_ts": last["timestamp"],
                        "last_ts_open_ms": last["ts_open_ms"],
                    },
                    "source_candle": {
                        key: last[key]
                        for key in [
                            "provider_id",
                            "venue",
                            "symbol",
                            "timeframe",
                            "ts_open_ms",
                            "ts_close_ms",
                            "timestamp",
                            "close_time",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "dataset_version",
                            "trace_id",
                        ]
                    },
                    "candidate_counts": counts,
                    "zones": zones,
                }
            )
    finally:
        conn.close()

    return _jsonable(
        {
            "contract": GOLDEN_CONTRACT,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "symbol": args.symbol,
            "timeframe": args.timeframe,
            "provider_id": args.provider_id,
            "venue": args.venue,
            "requested_asof_dates": [dt.date().isoformat() for dt in asof_dates],
            "config": asdict(cfg),
            "canonical_source": {
                "liquidsniper_root": str(liquidsniper_root),
                "zone_engine": "liquidsniper/core/zone_engine_v3.py",
                "selector": "liquidsniper/core/zone_selectors.py:select_daily_majors",
                "sequence": "structure + base + reaction -> merge_candidate_zones -> score_zone -> select_daily_majors -> operator core",
            },
            "source_candles": {
                "db_path": str(db_path),
                "provider_id": args.provider_id,
                "venue": args.venue,
                "timeframe": args.timeframe,
            },
            "data_source_caveat": (
                "Golden zones are generated from MarketArbiter's local imported Binance Public Data USD-M futures candles. "
                "TradingView candle history/session/vendor rounding can differ; Pine parity checks should separate code-port bugs from data-source mismatch."
            ),
            "snapshots": snapshots,
        }
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export Surveyor daily-major SR golden zones for Pine parity.")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    parser.add_argument("--provider-id", default=DEFAULT_PROVIDER_ID)
    parser.add_argument("--venue", default=DEFAULT_VENUE)
    parser.add_argument("--asof-dates", default=DEFAULT_ASOF_DATES, help="Comma-separated UTC dates/timestamps.")
    parser.add_argument("--liquidsniper-root", default=None)
    parser.add_argument("--min-history-bars", type=int, default=DailyMajorConfig.min_history_bars)
    parser.add_argument("--max-zones", type=int, default=DailyMajorConfig.daily_max_zones)
    parser.add_argument("--output-json", default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-md", default=DEFAULT_OUTPUT_MD)
    args = parser.parse_args(argv)

    dataset = build_dataset(args)
    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(dataset, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_md.write_text(_summary_markdown(dataset, output_json), encoding="utf-8")
    print(
        json.dumps(
            {
                "ok": True,
                "json_path": str(output_json),
                "summary_path": str(output_md),
                "snapshots": len(dataset.get("snapshots") or []),
                "selected_zones": sum(len(snapshot.get("zones") or []) for snapshot in dataset.get("snapshots") or []),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
