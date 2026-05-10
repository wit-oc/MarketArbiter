from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from market_arbiter.feed.bakeoff import render_smoke_markdown, summarize_provider_event_file


DEFAULT_ALERT_STALE_INTERVALS = 3


def _all_targets_passed(provider_results: Sequence[Mapping[str, Any]]) -> bool:
    if not provider_results:
        return False
    for provider in provider_results:
        summary = provider.get("summary")
        if not isinstance(summary, Mapping) or summary.get("passed_target_closes") is not True:
            return False
    return True


def _planned_duration_elapsed(*, metadata: Mapping[str, Any], as_of_ms: int) -> bool:
    started_at = metadata.get("started_at_utc")
    duration_seconds = metadata.get("duration_seconds")
    if not started_at or duration_seconds is None:
        return False
    try:
        from datetime import datetime

        started_ms = int(datetime.fromisoformat(str(started_at)).timestamp() * 1000)
        duration_ms = int(float(duration_seconds) * 1000)
    except Exception:  # noqa: BLE001
        return False
    return as_of_ms >= started_ms + duration_ms


def _csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _pid_running(pid: int | None) -> bool | None:
    if pid is None:
        return None
    try:
        subprocess.run(["kill", "-0", str(pid)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        return False


def _timeframe_ms(timeframe: str) -> int:
    return {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000, "1w": 604_800_000}[timeframe.lower()]


def summarize_run(
    *,
    artifact_dir: Path,
    providers: Sequence[str] | None = None,
    symbols: Sequence[str] | None = None,
    timeframe: str | None = None,
    target_closes_per_symbol: int | None = None,
    pid: int | None = None,
    stale_intervals: int = DEFAULT_ALERT_STALE_INTERVALS,
) -> dict[str, Any]:
    metadata_path = artifact_dir / "phase_b_live_metadata.json"
    metadata = _read_json(metadata_path) if metadata_path.exists() else {}
    resolved_providers = list(providers or metadata.get("providers") or [])
    resolved_symbols = list(symbols or metadata.get("symbols") or [])
    resolved_timeframe = str(timeframe or metadata.get("timeframe") or "5m")
    resolved_target = int(target_closes_per_symbol or metadata.get("target_closes_per_symbol") or 1)
    started_ms = None
    if metadata.get("started_at_utc"):
        # Runtime shortfall is advisory. If timestamp parsing fails, omit it instead of blocking status.
        try:
            from datetime import datetime

            started_ms = int(datetime.fromisoformat(str(metadata["started_at_utc"])).timestamp() * 1000)
        except Exception:  # noqa: BLE001
            started_ms = None
    as_of_ms = int(time.time() * 1000)
    provider_results = []
    issues: list[str] = []
    planned_duration_elapsed = _planned_duration_elapsed(metadata=metadata, as_of_ms=as_of_ms)
    for provider in resolved_providers:
        event_path = artifact_dir / provider / "close_events.jsonl"
        summary = summarize_provider_event_file(
            provider_id=provider,
            event_path=event_path,
            symbols=resolved_symbols,
            timeframe=resolved_timeframe,
            target_closes_per_symbol=resolved_target,
            started_ms=started_ms,
            as_of_ms=as_of_ms,
        )
        newest = max((row.get("last_close_open_ms") or 0 for row in summary["symbols"].values()), default=0)
        stale_ms = as_of_ms - int(newest) if newest else None
        target_passed = summary.get("passed_target_closes") is True
        provider_status = "ok"
        if summary["conflicts"]:
            provider_status = "critical"
            issues.append(f"{provider}: conflicting closed candles={summary['conflicts']}")
        elif newest and stale_ms is not None and stale_ms > (_timeframe_ms(resolved_timeframe) * stale_intervals) and not (target_passed and planned_duration_elapsed):
            provider_status = "warning"
            issues.append(f"{provider}: newest close is stale by {stale_ms // 1000}s")
        elif not newest:
            provider_status = "warning"
            issues.append(f"{provider}: no closed candles observed yet")
        for symbol, row in summary["symbols"].items():
            if row.get("missing_between_observed"):
                provider_status = "critical"
                issues.append(f"{provider}/{symbol}: missing intervals between observed closes={row['missing_between_observed']}")
        provider_results.append(
            {
                "provider": provider,
                "status": provider_status,
                "event_path": str(event_path),
                "summary": summary,
                "newest_close_stale_ms": stale_ms,
            }
        )
    running = _pid_running(pid)
    planned_completed = planned_duration_elapsed and _all_targets_passed(provider_results)
    if running is False and not planned_completed:
        issues.append(f"runner pid {pid} is not running")
    status = "critical" if any(row["status"] == "critical" for row in provider_results) else "warning" if issues else "ok"
    if running is False and not planned_completed:
        status = "critical"
    return {
        "contract": "feed_bakeoff_phase_b_status_v1",
        "artifact_dir": str(artifact_dir),
        "as_of_ms": as_of_ms,
        "runner_pid": pid,
        "runner_running": running,
        "runner_state": "completed" if planned_completed else "running" if running else "not_running" if running is False else "unknown",
        "planned_duration_elapsed": planned_duration_elapsed,
        "status": status,
        "issues": issues,
        "providers": provider_results,
    }


def render_status_markdown(status: Mapping[str, Any]) -> str:
    combined = {
        "run_id": Path(str(status.get("artifact_dir", ""))).name,
        "status": status.get("status"),
        "providers": [],
    }
    for provider in status.get("providers") or []:
        if not isinstance(provider, Mapping):
            continue
        combined["providers"].append(
            {
                "provider": provider.get("provider"),
                "status": provider.get("status"),
                "event_path": provider.get("event_path"),
                "summary": provider.get("summary"),
            }
        )
    lines = [render_smoke_markdown(combined).replace("Phase A smoke", "Phase B status").rstrip()]
    issues = status.get("issues") or []
    if issues:
        lines.append("\n## Issues")
        for issue in issues:
            lines.append(f"- {issue}")
    return "\n".join(lines) + "\n"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize a feed bakeoff Phase B canary artifact directory.")
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--providers")
    parser.add_argument("--symbols")
    parser.add_argument("--timeframe")
    parser.add_argument("--target-closes-per-symbol", type=int)
    parser.add_argument("--pid", type=int)
    parser.add_argument("--output-json")
    parser.add_argument("--output-md")
    parser.add_argument("--stale-intervals", type=int, default=DEFAULT_ALERT_STALE_INTERVALS)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    status = summarize_run(
        artifact_dir=Path(args.artifact_dir),
        providers=_csv(args.providers) or None,
        symbols=_csv(args.symbols) or None,
        timeframe=args.timeframe,
        target_closes_per_symbol=args.target_closes_per_symbol,
        pid=args.pid,
        stale_intervals=args.stale_intervals,
    )
    md = render_status_markdown(status)
    if args.output_json:
        Path(args.output_json).write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    if args.output_md:
        Path(args.output_md).write_text(md, encoding="utf-8")
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0 if status["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
