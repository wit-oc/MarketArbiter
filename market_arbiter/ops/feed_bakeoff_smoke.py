from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from market_arbiter.feed.bakeoff import enabled_provider_ids, render_smoke_markdown, run_smoke


def _read_config(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _default_run_id() -> str:
    return "feed-bakeoff-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")


def _symbols_from_config(config: Mapping[str, Any], phase: str) -> list[str]:
    candidate_symbols = config.get("candidate_symbols") if isinstance(config.get("candidate_symbols"), Mapping) else {}
    return [str(symbol) for symbol in candidate_symbols.get(phase, [])]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run provider-neutral feed bakeoff smoke tests without canonical Surveyor writes.")
    parser.add_argument("--config", default="configs/feed_provider_bakeoff.v1.json")
    parser.add_argument("--providers", help="Comma-separated provider ids. Defaults to enabled providers from config.")
    parser.add_argument("--symbols", help="Comma-separated canonical symbols. Defaults to config phase_a_smoke symbols.")
    parser.add_argument("--timeframe", default=None, help="Live candle timeframe. Defaults to config run_profile.base_timeframe.")
    parser.add_argument("--duration-seconds", type=int, default=None)
    parser.add_argument("--target-closes-per-symbol", type=int, default=3)
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--allow-live-provider-calls", action="store_true", help="Required guardrail for outbound public WebSocket calls.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.allow_live_provider_calls:
        raise SystemExit("Refusing outbound provider WebSocket calls without --allow-live-provider-calls")
    config = _read_config(args.config)
    run_profile = config.get("run_profile") if isinstance(config.get("run_profile"), Mapping) else {}
    providers = _csv(args.providers) or enabled_provider_ids(config)
    symbols = _csv(args.symbols) or _symbols_from_config(config, "phase_a_smoke")
    timeframe = args.timeframe or str(run_profile.get("base_timeframe") or "5m")
    duration_seconds = int(args.duration_seconds or int(run_profile.get("phase_a_smoke_duration_minutes") or 120) * 60)
    artifact_root = Path(args.artifact_root or run_profile.get("artifact_root") or "artifacts/feed_bakeoff")
    run_id = args.run_id or _default_run_id()

    combined = asyncio.run(
        run_smoke(
            run_id=run_id,
            provider_ids=providers,
            symbols=symbols,
            timeframe=timeframe,
            duration_seconds=duration_seconds,
            target_closes_per_symbol=args.target_closes_per_symbol,
            artifact_root=artifact_root,
        )
    )
    artifact_dir = Path(str(combined["artifact_dir"]))
    md = render_smoke_markdown(combined)
    (artifact_dir / "phase_a_smoke_summary.md").write_text(md, encoding="utf-8")
    print(json.dumps(combined, indent=2, sort_keys=True))
    return 0 if combined["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
