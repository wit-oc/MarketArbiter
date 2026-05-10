from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from market_arbiter.feed.bakeoff import (
    FEED_BAKEOFF_LIVE_SUMMARY_CONTRACT,
    render_smoke_markdown,
    run_smoke,
)


PHASE_B_COMBINED_CONTRACT = "feed_bakeoff_phase_b_live_combined_v1"
PHASE_B_DEFAULT_SYMBOLS = [
    "BTC-USDT",
    "ETH-USDT",
    "SOL-USDT",
    "XRP-USDT",
    "DOGE-USDT",
    "BNB-USDT",
    "ADA-USDT",
    "LINK-USDT",
    "AVAX-USDT",
    "TON-USDT",
]


def _read_config(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _default_run_id() -> str:
    return "feed-bakeoff-phase-b-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")


def _phase_b_symbols(config: Mapping[str, Any]) -> list[str]:
    candidate_symbols = config.get("candidate_symbols") if isinstance(config.get("candidate_symbols"), Mapping) else {}
    configured = [str(symbol) for symbol in candidate_symbols.get("phase_b_live", [])]
    return configured or PHASE_B_DEFAULT_SYMBOLS


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase B 24h feed bakeoff live canary without canonical Surveyor writes.")
    parser.add_argument("--config", default="configs/feed_provider_bakeoff.v1.json")
    parser.add_argument("--providers", default="bybit,okx", help="Comma-separated provider ids. Default: bybit,okx")
    parser.add_argument("--symbols", help="Comma-separated canonical symbols. Defaults to config phase_b_live symbols.")
    parser.add_argument("--timeframe", default=None, help="Live candle timeframe. Defaults to config run_profile.base_timeframe.")
    parser.add_argument("--duration-seconds", type=int, default=None, help="Default: config phase_b_live_duration_hours * 3600")
    parser.add_argument("--target-closes-per-symbol", type=int, default=None, help="Default: duration/timeframe minus one close.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--allow-live-provider-calls", action="store_true", help="Required guardrail for outbound public WebSocket calls.")
    return parser.parse_args(argv)


def _timeframe_seconds(timeframe: str) -> int:
    return {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800}[timeframe.lower()]


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.allow_live_provider_calls:
        raise SystemExit("Refusing outbound provider WebSocket calls without --allow-live-provider-calls")
    config = _read_config(args.config)
    run_profile = config.get("run_profile") if isinstance(config.get("run_profile"), Mapping) else {}
    providers = _csv(args.providers)
    symbols = _csv(args.symbols) or _phase_b_symbols(config)
    timeframe = str(args.timeframe or run_profile.get("base_timeframe") or "5m").lower()
    duration_seconds = int(args.duration_seconds or (float(run_profile.get("phase_b_live_duration_hours") or 24) * 3600))
    target_closes = args.target_closes_per_symbol
    if target_closes is None:
        target_closes = max(1, duration_seconds // _timeframe_seconds(timeframe) - 1)
    artifact_root = Path(args.artifact_root or run_profile.get("artifact_root") or "artifacts/feed_bakeoff")
    run_id = args.run_id or _default_run_id()
    artifact_dir = artifact_root / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "contract": "feed_bakeoff_phase_b_live_metadata_v1",
        "run_id": run_id,
        "providers": providers,
        "symbols": symbols,
        "timeframe": timeframe,
        "duration_seconds": duration_seconds,
        "target_closes_per_symbol": target_closes,
        "artifact_dir": str(artifact_dir),
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "canonical_surveyor_writes_enabled": False,
    }
    (artifact_dir / "phase_b_live_metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")

    combined = asyncio.run(
        run_smoke(
            run_id=run_id,
            provider_ids=providers,
            symbols=symbols,
            timeframe=timeframe,
            duration_seconds=duration_seconds,
            target_closes_per_symbol=target_closes,
            artifact_root=artifact_root,
            stop_on_target=False,
            combined_contract=PHASE_B_COMBINED_CONTRACT,
            combined_filename="phase_b_live_summary.json",
            provider_summary_contract=FEED_BAKEOFF_LIVE_SUMMARY_CONTRACT,
            provider_summary_filename="phase_b_live.json",
        )
    )
    md = render_smoke_markdown(combined).replace("Phase A smoke", "Phase B live canary")
    (artifact_dir / "phase_b_live_summary.md").write_text(md, encoding="utf-8")
    print(json.dumps(combined, indent=2, sort_keys=True))
    return 0 if combined["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
