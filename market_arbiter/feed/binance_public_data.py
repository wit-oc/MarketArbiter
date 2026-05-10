from __future__ import annotations

import csv
import hashlib
import io
import json
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from market_arbiter.core.market_data import CandleDTO, upsert_market_candles


BINANCE_PUBLIC_DATA_BASE_URL = "https://data.binance.vision"
BINANCE_PUBLIC_DATA_PROVIDER_ID = "binance_public_data"
BINANCE_PUBLIC_DATA_DERIVED_PROVIDER_ID = "binance_public_data_derived"
BINANCE_PUBLIC_DATA_IMPORT_CONTRACT = "binance_public_data_historical_import_v1"
BINANCE_PUBLIC_DATA_CONTINUITY_CONTRACT = "historical_ohlcv_continuity_report_v1"

TIMEFRAME_MS = {
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
    "1w": 7 * 24 * 60 * 60 * 1000,
}

_MARKETS = {
    "usdm-futures": {
        "path": "futures/um",
        "venue": "binance_usdm_futures",
        "dataset_prefix": "binance_public_data_usdm_futures",
    },
    "spot": {
        "path": "spot",
        "venue": "binance_spot",
        "dataset_prefix": "binance_public_data_spot",
    },
}


@dataclass(frozen=True)
class BinanceArchiveRef:
    market: str
    symbol: str
    timeframe: str
    granularity: str
    period: str
    url: str
    checksum_url: str
    filename: str


@dataclass(frozen=True)
class BinanceArchiveResult:
    ref: BinanceArchiveRef
    zip_path: Path
    checksum_path: Path | None
    checksum_status: str
    sha256: str | None
    bytes: int
    downloaded: bool


def normalize_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper().replace("-", "").replace("/", "")
    if not normalized:
        raise ValueError("symbol is required")
    return normalized


def normalize_timeframe(timeframe: str) -> str:
    normalized = str(timeframe or "").strip().lower()
    if normalized not in TIMEFRAME_MS:
        raise ValueError(f"unsupported Binance historical timeframe for this importer: {timeframe}")
    return normalized


def market_config(market: str) -> Mapping[str, str]:
    normalized = str(market or "").strip().lower()
    if normalized not in _MARKETS:
        raise ValueError(f"unsupported Binance public-data market: {market}")
    return _MARKETS[normalized]


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def iter_months(start: date, end: date) -> Iterable[date]:
    cursor = _month_start(start)
    final = _month_start(end)
    while cursor <= final:
        yield cursor
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def default_start_for_years(*, years: int, end: date | None = None) -> date:
    end_date = end or datetime.now(timezone.utc).date()
    try:
        return date(end_date.year - int(years), end_date.month, end_date.day)
    except ValueError:
        # Handles leap-day anchors by using Feb 28 in the target year.
        return date(end_date.year - int(years), end_date.month, 28)


def build_archive_ref(*, market: str, symbol: str, timeframe: str, granularity: str, period: str) -> BinanceArchiveRef:
    market_key = str(market or "").strip().lower()
    cfg = market_config(market_key)
    sym = normalize_symbol(symbol)
    tf = normalize_timeframe(timeframe)
    gran = str(granularity or "").strip().lower()
    if gran not in {"monthly", "daily"}:
        raise ValueError(f"unsupported archive granularity: {granularity}")
    filename = f"{sym}-{tf}-{period}.zip"
    url = f"{BINANCE_PUBLIC_DATA_BASE_URL}/data/{cfg['path']}/{gran}/klines/{sym}/{tf}/{filename}"
    return BinanceArchiveRef(
        market=market_key,
        symbol=sym,
        timeframe=tf,
        granularity=gran,
        period=period,
        url=url,
        checksum_url=f"{url}.CHECKSUM",
        filename=filename,
    )


def build_monthly_archive_refs(*, market: str, symbol: str, timeframe: str, start: date, end: date) -> list[BinanceArchiveRef]:
    return [
        build_archive_ref(
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            granularity="monthly",
            period=month.strftime("%Y-%m"),
        )
        for month in iter_months(start, end)
    ]


def archive_local_path(root: str | Path, ref: BinanceArchiveRef) -> Path:
    return Path(root) / ref.market / ref.symbol / ref.timeframe / ref.granularity / ref.filename


def _download_url(url: str, path: Path, *, timeout: float) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(url, headers={"User-Agent": "MarketArbiter historical research importer"})
    with urllib.request.urlopen(request, timeout=timeout) as response:  # nosec B310 - configured public data source
        data = response.read()
    if path.exists() and path.read_bytes() == data:
        return False
    path.write_bytes(data)
    return True


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_checksum(text: str) -> str | None:
    for token in str(text or "").replace("*", " ").split():
        cleaned = token.strip()
        if len(cleaned) == 64 and all(char in "0123456789abcdefABCDEF" for char in cleaned):
            return cleaned.lower()
    return None


def local_archive_result(ref: BinanceArchiveRef, *, archive_root: str | Path) -> BinanceArchiveResult:
    zip_path = archive_local_path(archive_root, ref)
    checksum_path = zip_path.with_name(f"{zip_path.name}.CHECKSUM")
    if not zip_path.exists():
        raise FileNotFoundError(zip_path)
    actual = _sha256(zip_path)
    checksum_status = "not_checked"
    resolved_checksum_path: Path | None = None
    if checksum_path.exists():
        expected = parse_checksum(checksum_path.read_text(encoding="utf-8", errors="replace"))
        checksum_status = "pass" if expected == actual else "fail"
        if checksum_status == "fail":
            raise ValueError(f"checksum mismatch for {zip_path}: expected={expected} actual={actual}")
        resolved_checksum_path = checksum_path
    return BinanceArchiveResult(
        ref=ref,
        zip_path=zip_path,
        checksum_path=resolved_checksum_path,
        checksum_status=checksum_status,
        sha256=actual,
        bytes=zip_path.stat().st_size,
        downloaded=False,
    )


def download_archive(
    ref: BinanceArchiveRef,
    *,
    archive_root: str | Path,
    timeout: float = 30.0,
    allow_missing_checksum: bool = False,
    skip_existing: bool = True,
) -> BinanceArchiveResult:
    zip_path = archive_local_path(archive_root, ref)
    checksum_path = zip_path.with_name(f"{zip_path.name}.CHECKSUM")
    downloaded = False
    if not zip_path.exists() or not skip_existing:
        downloaded = _download_url(ref.url, zip_path, timeout=timeout)

    checksum_downloaded = False
    try:
        if not checksum_path.exists() or not skip_existing:
            checksum_downloaded = _download_url(ref.checksum_url, checksum_path, timeout=timeout)
    except urllib.error.HTTPError as exc:
        if exc.code == 404 and allow_missing_checksum:
            checksum_path = None
        else:
            raise

    actual = _sha256(zip_path)
    checksum_status = "not_checked"
    expected: str | None = None
    if checksum_path is not None and checksum_path.exists():
        expected = parse_checksum(checksum_path.read_text(encoding="utf-8", errors="replace"))
        checksum_status = "pass" if expected == actual else "fail"
        if checksum_status == "fail":
            raise ValueError(f"checksum mismatch for {zip_path}: expected={expected} actual={actual}")
    elif not allow_missing_checksum:
        raise FileNotFoundError(f"missing checksum for {zip_path}")

    return BinanceArchiveResult(
        ref=ref,
        zip_path=zip_path,
        checksum_path=checksum_path,
        checksum_status=checksum_status,
        sha256=actual,
        bytes=zip_path.stat().st_size,
        downloaded=downloaded or checksum_downloaded,
    )


def _row_is_header(row: Sequence[str]) -> bool:
    if not row:
        return True
    try:
        int(str(row[0]).strip())
        return False
    except ValueError:
        return True


def read_kline_zip(path: str | Path, *, market: str, symbol: str, timeframe: str, trace_id: str | None = None) -> list[CandleDTO]:
    zip_path = Path(path)
    cfg = market_config(market)
    sym = normalize_symbol(symbol)
    tf = normalize_timeframe(timeframe)
    step_ms = TIMEFRAME_MS[tf]
    dataset_version = f"{cfg['dataset_prefix']}_{tf}_v1"
    resolved_trace = trace_id or f"binance-public-data:{sym}:{tf}:{int(time.time() * 1000)}"
    candles: list[CandleDTO] = []
    with zipfile.ZipFile(zip_path) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"no CSV file found inside {zip_path}")
        with archive.open(csv_names[0]) as raw_handle:
            text_handle = io.TextIOWrapper(raw_handle, encoding="utf-8", newline="")
            for row in csv.reader(text_handle):
                if _row_is_header(row):
                    continue
                if len(row) < 6:
                    raise ValueError(f"malformed Binance kline row in {zip_path}: {row!r}")
                ts_open_ms = int(row[0])
                candles.append(
                    CandleDTO(
                        provider_id=BINANCE_PUBLIC_DATA_PROVIDER_ID,
                        venue=str(cfg["venue"]),
                        symbol=sym,
                        timeframe=tf,
                        ts_open_ms=ts_open_ms,
                        ts_close_ms=ts_open_ms + step_ms,
                        open=row[1],
                        high=row[2],
                        low=row[3],
                        close=row[4],
                        volume=row[5],
                        dataset_version=dataset_version,
                        trace_id=resolved_trace,
                    )
                )
    candles.sort(key=lambda candle: candle.ts_open_ms)
    return candles


def _week_start_ms(ts_open_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_open_ms / 1000, tz=timezone.utc)
    week_start = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc) - timedelta(days=dt.weekday())
    return int(week_start.timestamp() * 1000)


def derive_weekly_candles_from_daily(
    daily_candles: Sequence[CandleDTO],
    *,
    market: str,
    symbol: str,
    trace_id: str | None = None,
) -> list[CandleDTO]:
    cfg = market_config(market)
    sym = normalize_symbol(symbol)
    resolved_trace = trace_id or f"binance-public-data-derived:{sym}:1w:{int(time.time() * 1000)}"
    groups: dict[int, list[CandleDTO]] = {}
    for candle in sorted(daily_candles, key=lambda item: int(item.ts_open_ms)):
        groups.setdefault(_week_start_ms(int(candle.ts_open_ms)), []).append(candle)

    weekly: list[CandleDTO] = []
    for week_start, group in sorted(groups.items()):
        ordered = sorted(group, key=lambda item: int(item.ts_open_ms))
        highs = [float(item.high) for item in ordered]
        lows = [float(item.low) for item in ordered]
        volumes = [float(item.volume) for item in ordered]
        weekly.append(
            CandleDTO(
                provider_id=BINANCE_PUBLIC_DATA_DERIVED_PROVIDER_ID,
                venue=str(cfg["venue"]),
                symbol=sym,
                timeframe="1w",
                ts_open_ms=week_start,
                ts_close_ms=week_start + TIMEFRAME_MS["1w"],
                open=ordered[0].open,
                high=str(max(highs)),
                low=str(min(lows)),
                close=ordered[-1].close,
                volume=str(sum(volumes)),
                dataset_version=f"{cfg['dataset_prefix']}_1w_from_1d_v1",
                trace_id=resolved_trace,
            )
        )
    return weekly


def import_archive_results(
    conn,
    results: Sequence[BinanceArchiveResult],
    *,
    ingest_ts_ms: int | None = None,
    trace_id: str | None = None,
    derive_weekly_from_daily: bool = False,
) -> dict:
    now_ms = int(ingest_ts_ms or time.time() * 1000)
    by_symbol_tf: dict[tuple[str, str], list[CandleDTO]] = {}
    archives: list[dict] = []
    for result in results:
        ref = result.ref
        candles = read_kline_zip(result.zip_path, market=ref.market, symbol=ref.symbol, timeframe=ref.timeframe, trace_id=trace_id)
        by_symbol_tf.setdefault((ref.symbol, ref.timeframe), []).extend(candles)
        archives.append(
            {
                "market": ref.market,
                "symbol": ref.symbol,
                "timeframe": ref.timeframe,
                "granularity": ref.granularity,
                "period": ref.period,
                "url": ref.url,
                "zip_path": str(result.zip_path),
                "checksum_path": str(result.checksum_path) if result.checksum_path else None,
                "checksum_status": result.checksum_status,
                "sha256": result.sha256,
                "bytes": result.bytes,
            }
        )

    totals = {"inserted": 0, "idempotent": 0}
    continuity_reports = []
    derived_reports = []
    raw_deduped_by_symbol_tf: dict[tuple[str, str], list[CandleDTO]] = {}
    for (symbol, timeframe), candles in sorted(by_symbol_tf.items()):
        deduped = _dedupe_candles(candles)
        raw_deduped_by_symbol_tf[(symbol, timeframe)] = deduped
        summary = upsert_market_candles(conn, deduped, ingest_ts_ms=now_ms)
        totals["inserted"] += int(summary["inserted"])
        totals["idempotent"] += int(summary["idempotent"])
        continuity_reports.append(build_continuity_report(deduped, symbol=symbol, timeframe=timeframe))

    if derive_weekly_from_daily:
        for (symbol, timeframe), candles in sorted(raw_deduped_by_symbol_tf.items()):
            if timeframe != "1d":
                continue
            # The source market is per archive result. For this first importer run,
            # all archives in a symbol/timeframe group share one market.
            market = next((result.ref.market for result in results if result.ref.symbol == symbol and result.ref.timeframe == "1d"), "usdm-futures")
            weekly = derive_weekly_candles_from_daily(candles, market=market, symbol=symbol, trace_id=trace_id)
            weekly = _dedupe_candles(weekly)
            summary = upsert_market_candles(conn, weekly, ingest_ts_ms=now_ms)
            totals["inserted"] += int(summary["inserted"])
            totals["idempotent"] += int(summary["idempotent"])
            derived_reports.append(
                {
                    "derivation": "1w_from_1d",
                    "source_timeframe": "1d",
                    "target_timeframe": "1w",
                    "inserted": int(summary["inserted"]),
                    "idempotent": int(summary["idempotent"]),
                    "continuity": build_continuity_report(weekly, symbol=symbol, timeframe="1w"),
                }
            )

    return {
        "contract": BINANCE_PUBLIC_DATA_IMPORT_CONTRACT,
        "provider_id": BINANCE_PUBLIC_DATA_PROVIDER_ID,
        "ingest_ts_ms": now_ms,
        "archives": archives,
        "summary": {
            "archive_count": len(results),
            "symbol_timeframe_count": len(by_symbol_tf),
            **totals,
        },
        "continuity_reports": continuity_reports,
        "derived_reports": derived_reports,
    }


def _dedupe_candles(candles: Sequence[CandleDTO]) -> list[CandleDTO]:
    by_key: dict[tuple[str, str, int], CandleDTO] = {}
    for candle in candles:
        key = (candle.symbol, candle.timeframe, int(candle.ts_open_ms))
        existing = by_key.get(key)
        if existing is not None and (
            str(existing.open),
            str(existing.high),
            str(existing.low),
            str(existing.close),
            str(existing.volume),
        ) != (str(candle.open), str(candle.high), str(candle.low), str(candle.close), str(candle.volume)):
            raise ValueError(f"conflicting duplicate Binance candle for key={key}")
        by_key[key] = candle
    return [by_key[key] for key in sorted(by_key, key=lambda item: item[2])]


def build_continuity_report(candles: Sequence[CandleDTO], *, symbol: str, timeframe: str) -> dict:
    tf = normalize_timeframe(timeframe)
    step_ms = TIMEFRAME_MS[tf]
    ordered = sorted(candles, key=lambda candle: int(candle.ts_open_ms))
    duplicate_count = len(ordered) - len({int(candle.ts_open_ms) for candle in ordered})
    gaps = []
    previous: CandleDTO | None = None
    for candle in ordered:
        if previous is not None:
            expected = int(previous.ts_open_ms) + step_ms
            actual = int(candle.ts_open_ms)
            if actual > expected:
                missing = max(0, (actual - expected) // step_ms)
                gaps.append(
                    {
                        "after_ts_open_ms": int(previous.ts_open_ms),
                        "next_ts_open_ms": actual,
                        "missing_bars": missing,
                    }
                )
            elif actual < expected:
                gaps.append(
                    {
                        "after_ts_open_ms": int(previous.ts_open_ms),
                        "next_ts_open_ms": actual,
                        "overlap_ms": expected - actual,
                    }
                )
        previous = candle

    first_ts = int(ordered[0].ts_open_ms) if ordered else None
    last_ts = int(ordered[-1].ts_open_ms) if ordered else None
    expected_bars = ((last_ts - first_ts) // step_ms + 1) if first_ts is not None and last_ts is not None else 0
    missing_bars = sum(int(gap.get("missing_bars") or 0) for gap in gaps)
    return {
        "contract": BINANCE_PUBLIC_DATA_CONTINUITY_CONTRACT,
        "provider_id": BINANCE_PUBLIC_DATA_PROVIDER_ID,
        "symbol": normalize_symbol(symbol),
        "timeframe": tf,
        "first_ts_open_ms": first_ts,
        "last_ts_open_ms": last_ts,
        "bar_count": len(ordered),
        "expected_bar_count": expected_bars,
        "missing_bar_count": missing_bars,
        "duplicate_count": duplicate_count,
        "gap_count": len([gap for gap in gaps if gap.get("missing_bars")]),
        "overlap_count": len([gap for gap in gaps if gap.get("overlap_ms")]),
        "gaps": gaps[:50],
        "gaps_truncated": max(0, len(gaps) - 50),
        "status": "pass" if missing_bars == 0 and duplicate_count == 0 and not any(gap.get("overlap_ms") for gap in gaps) else "fail",
    }


def write_json(path: str | Path, payload: Mapping) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
