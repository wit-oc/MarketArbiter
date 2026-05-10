from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from market_arbiter.core.db import init_db
from market_arbiter.feed.binance_public_data import (
    BINANCE_PUBLIC_DATA_IMPORT_CONTRACT,
    build_monthly_archive_refs,
    default_start_for_years,
    download_archive,
    import_archive_results,
    local_archive_result,
    normalize_symbol,
    normalize_timeframe,
    parse_date,
    write_json,
)


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _last_closed_month_day(anchor: date) -> date:
    return date(anchor.year, anchor.month, 1) - timedelta(days=1)


def _split_csv(value: str, *, upper: bool = False, lower: bool = False) -> list[str]:
    items = [item.strip() for item in str(value or "").split(",") if item.strip()]
    if upper:
        return [item.upper() for item in items]
    if lower:
        return [item.lower() for item in items]
    return items


def build_import_plan(*, market: str, symbols: list[str], timeframes: list[str], start: date, end: date) -> dict:
    refs = []
    for symbol in symbols:
        for timeframe in timeframes:
            refs.extend(build_monthly_archive_refs(market=market, symbol=symbol, timeframe=timeframe, start=start, end=end))
    return {
        "contract": f"{BINANCE_PUBLIC_DATA_IMPORT_CONTRACT}_plan",
        "market": market,
        "symbols": symbols,
        "timeframes": timeframes,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "archive_count": len(refs),
        "archives": [
            {
                "symbol": ref.symbol,
                "timeframe": ref.timeframe,
                "granularity": ref.granularity,
                "period": ref.period,
                "url": ref.url,
                "checksum_url": ref.checksum_url,
                "filename": ref.filename,
            }
            for ref in refs
        ],
    }


def run(args: argparse.Namespace) -> dict:
    today = _utc_today()
    end = parse_date(args.end) if args.end else (_utc_today() if args.include_open_month else _last_closed_month_day(today))
    start = parse_date(args.start) if args.start else default_start_for_years(years=int(args.years), end=end)
    symbols = [normalize_symbol(symbol) for symbol in _split_csv(args.symbols, upper=True)]
    timeframes = [normalize_timeframe(timeframe) for timeframe in _split_csv(args.timeframes, lower=True)]
    refs = []
    for symbol in symbols:
        for timeframe in timeframes:
            refs.extend(build_monthly_archive_refs(market=args.market, symbol=symbol, timeframe=timeframe, start=start, end=end))

    plan = build_import_plan(market=args.market, symbols=symbols, timeframes=timeframes, start=start, end=end)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "binance_history_import_plan.json", plan)

    if not args.download and not args.import_to_db:
        return {"ok": True, "mode": "plan", "plan_path": str(output_dir / "binance_history_import_plan.json"), **plan}

    results = []
    missing = []
    for ref in refs:
        try:
            if args.download:
                results.append(
                    download_archive(
                        ref,
                        archive_root=args.archive_dir,
                        timeout=float(args.timeout_seconds),
                        allow_missing_checksum=bool(args.allow_missing_checksum),
                        skip_existing=not args.redownload,
                    )
                )
            else:
                # Reuse local cache only; checksum must have been saved by a prior download.
                results.append(local_archive_result(ref, archive_root=args.archive_dir))
        except Exception as exc:
            missing.append({"url": ref.url, "filename": ref.filename, "reason": type(exc).__name__, "message": str(exc)})
            if not args.skip_missing:
                raise

    download_manifest = {
        "contract": "binance_public_data_download_manifest_v1",
        "created_ts_ms": int(time.time() * 1000),
        "archive_root": args.archive_dir,
        "requested_archive_count": len(refs),
        "available_archive_count": len(results),
        "missing_archive_count": len(missing),
        "archives": [
            {
                "symbol": result.ref.symbol,
                "timeframe": result.ref.timeframe,
                "period": result.ref.period,
                "url": result.ref.url,
                "zip_path": str(result.zip_path),
                "checksum_path": str(result.checksum_path) if result.checksum_path else None,
                "checksum_status": result.checksum_status,
                "sha256": result.sha256,
                "bytes": result.bytes,
                "downloaded": result.downloaded,
            }
            for result in results
        ],
        "missing": missing,
    }
    write_json(output_dir / "binance_history_download_manifest.json", download_manifest)

    import_report = None
    if args.import_to_db:
        conn = init_db(args.db_path)
        try:
            with conn:
                import_report = import_archive_results(
                    conn,
                    results,
                    trace_id=args.trace_id,
                    derive_weekly_from_daily=bool(args.derive_weekly_from_daily),
                )
        finally:
            conn.close()
        write_json(output_dir / "binance_history_import_report.json", import_report)

    return {
        "ok": True,
        "mode": "download_import" if args.download and args.import_to_db else ("download" if args.download else "import"),
        "plan_path": str(output_dir / "binance_history_import_plan.json"),
        "download_manifest_path": str(output_dir / "binance_history_download_manifest.json"),
        "import_report_path": str(output_dir / "binance_history_import_report.json") if import_report else None,
        "requested_archive_count": len(refs),
        "available_archive_count": len(results),
        "missing_archive_count": len(missing),
        "inserted": ((import_report or {}).get("summary") or {}).get("inserted"),
        "idempotent": ((import_report or {}).get("summary") or {}).get("idempotent"),
        "continuity_statuses": [report.get("status") for report in ((import_report or {}).get("continuity_reports") or [])],
        "derived_continuity_statuses": [report.get("continuity", {}).get("status") for report in ((import_report or {}).get("derived_reports") or [])],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download and import Binance Public Data historical kline archives for backtesting.")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--market", choices=("usdm-futures", "spot"), default="usdm-futures")
    parser.add_argument("--symbols", default="BTCUSDT")
    parser.add_argument("--timeframes", default="4h,1d,1w")
    parser.add_argument("--years", type=int, default=5, help="Lookback used when --start is omitted.")
    parser.add_argument("--start", default=None, help="Inclusive YYYY-MM-DD start. Defaults to --years before --end.")
    parser.add_argument("--end", default=None, help="Inclusive YYYY-MM-DD end. Defaults to last closed monthly archive unless --include-open-month.")
    parser.add_argument("--include-open-month", action="store_true", help="Include the current UTC month in the monthly archive plan; usually unavailable until month closes.")
    parser.add_argument("--archive-dir", default="data/historical/binance_public_data")
    parser.add_argument("--output-dir", default="artifacts/historical_data/binance_btc_5y")
    parser.add_argument("--download", action="store_true", help="Fetch remote zip and checksum archives into --archive-dir.")
    parser.add_argument("--import-to-db", action="store_true", help="Import available archives into market_candles after checksum/provenance capture.")
    parser.add_argument("--skip-missing", action="store_true", help="Continue if an archive is absent; missing files are recorded in the manifest.")
    parser.add_argument("--allow-missing-checksum", action="store_true", help="Allow archives without checksum sidecars; not recommended for research artifacts.")
    parser.add_argument("--derive-weekly-from-daily", action="store_true", help="Derive a complete 1w research series from imported 1d candles; useful when direct Binance 1w monthly archives are sparse.")
    parser.add_argument("--redownload", action="store_true", help="Overwrite local cached archives/checksums.")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--trace-id", default=None)
    args = parser.parse_args(argv)

    result = run(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
