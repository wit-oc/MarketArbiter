from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

from market_arbiter.core.db import init_db
from market_arbiter.feed.binance_public_data import (
    BinanceArchiveResult,
    BINANCE_PUBLIC_DATA_IMPORT_CONTRACT,
    build_archive_ref,
    build_continuity_report,
    derive_weekly_candles_from_daily,
    import_archive_results,
    parse_checksum,
    read_kline_zip,
)
from market_arbiter.ops.binance_history_import import main as import_cli


def _write_zip(path: Path, rows: list[list[str]], *, header: bool = True) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = ""
    if header:
        body += "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n"
    body += "".join(",".join(row) + "\n" for row in rows)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(path.with_suffix(".csv").name, body)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_name(f"{path.name}.CHECKSUM").write_text(f"{digest}  {path.name}\n", encoding="utf-8")
    return digest


def _ref(period: str = "2021-01"):
    return build_archive_ref(market="usdm-futures", symbol="BTCUSDT", timeframe="4h", granularity="monthly", period=period)


def test_build_archive_ref_matches_binance_public_data_usdm_monthly_shape() -> None:
    ref = _ref("2021-05")

    assert ref.url == "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2021-05.zip"
    assert ref.checksum_url.endswith("BTCUSDT-4h-2021-05.zip.CHECKSUM")


def test_read_kline_zip_normalizes_headered_binance_rows(tmp_path: Path) -> None:
    zip_path = tmp_path / "BTCUSDT-4h-2021-01.zip"
    _write_zip(
        zip_path,
        [
            ["1609459200000", "100", "110", "90", "105", "123", "1609473599999", "0", "1", "0", "0", "0"],
            ["1609473600000", "105", "112", "101", "108", "124", "1609487999999", "0", "1", "0", "0", "0"],
        ],
    )

    candles = read_kline_zip(zip_path, market="usdm-futures", symbol="BTCUSDT", timeframe="4h", trace_id="test-trace")

    assert [candle.ts_open_ms for candle in candles] == [1609459200000, 1609473600000]
    assert candles[0].provider_id == "binance_public_data"
    assert candles[0].venue == "binance_usdm_futures"
    assert candles[0].ts_close_ms == 1609473600000
    assert candles[0].dataset_version == "binance_public_data_usdm_futures_4h_v1"
    assert candles[0].trace_id == "test-trace"


def test_derive_weekly_candles_from_daily_uses_monday_utc_ohlc_and_volume(tmp_path: Path) -> None:
    zip_path = tmp_path / "BTCUSDT-1d-2021-01.zip"
    _write_zip(
        zip_path,
        [
            ["1609718400000", "100", "110", "90", "105", "10", "1609804799999", "0", "1", "0", "0", "0"],
            ["1609804800000", "105", "112", "101", "108", "11", "1609891199999", "0", "1", "0", "0", "0"],
            ["1610323200000", "120", "130", "119", "125", "12", "1610409599999", "0", "1", "0", "0", "0"],
        ],
    )
    daily = read_kline_zip(zip_path, market="usdm-futures", symbol="BTCUSDT", timeframe="1d", trace_id="daily")

    weekly = derive_weekly_candles_from_daily(daily, market="usdm-futures", symbol="BTCUSDT", trace_id="derived")

    assert len(weekly) == 2
    assert weekly[0].provider_id == "binance_public_data_derived"
    assert weekly[0].timeframe == "1w"
    assert weekly[0].ts_open_ms == 1609718400000  # Monday 2021-01-04T00:00:00Z
    assert weekly[0].open == "100"
    assert weekly[0].high == "112.0"
    assert weekly[0].low == "90.0"
    assert weekly[0].close == "108"
    assert weekly[0].volume == "21.0"
    assert weekly[0].dataset_version == "binance_public_data_usdm_futures_1w_from_1d_v1"


def test_import_archive_results_persists_candles_with_checksum_provenance_and_continuity(tmp_path: Path) -> None:
    zip_path = tmp_path / "BTCUSDT-4h-2021-01.zip"
    digest = _write_zip(
        zip_path,
        [
            ["1609459200000", "100", "110", "90", "105", "123", "1609473599999", "0", "1", "0", "0", "0"],
            ["1609473600000", "105", "112", "101", "108", "124", "1609487999999", "0", "1", "0", "0", "0"],
        ],
    )
    result = BinanceArchiveResult(
        ref=_ref(),
        zip_path=zip_path,
        checksum_path=zip_path.with_name(f"{zip_path.name}.CHECKSUM"),
        checksum_status="pass",
        sha256=digest,
        bytes=zip_path.stat().st_size,
        downloaded=False,
    )
    conn = init_db(str(tmp_path / "market.sqlite"))
    try:
        with conn:
            report = import_archive_results(conn, [result], ingest_ts_ms=1_700_000_000_000, trace_id="import-test")
        rows = conn.execute(
            "SELECT provider_id, venue, symbol, timeframe, ts_open_ms, open, close, dataset_version, trace_id FROM market_candles ORDER BY ts_open_ms"
        ).fetchall()
    finally:
        conn.close()

    assert report["contract"] == BINANCE_PUBLIC_DATA_IMPORT_CONTRACT
    assert report["summary"] == {"archive_count": 1, "symbol_timeframe_count": 1, "inserted": 2, "idempotent": 0}
    assert report["archives"][0]["checksum_status"] == "pass"
    assert report["archives"][0]["sha256"] == digest
    assert report["continuity_reports"][0]["status"] == "pass"
    assert rows == [
        ("binance_public_data", "binance_usdm_futures", "BTCUSDT", "4h", 1609459200000, "100", "105", "binance_public_data_usdm_futures_4h_v1", "import-test"),
        ("binance_public_data", "binance_usdm_futures", "BTCUSDT", "4h", 1609473600000, "105", "108", "binance_public_data_usdm_futures_4h_v1", "import-test"),
    ]


def test_continuity_report_flags_missing_bar(tmp_path: Path) -> None:
    zip_path = tmp_path / "BTCUSDT-4h-2021-01.zip"
    _write_zip(
        zip_path,
        [
            ["1609459200000", "100", "110", "90", "105", "123", "1609473599999", "0", "1", "0", "0", "0"],
            ["1609488000000", "105", "112", "101", "108", "124", "1609502399999", "0", "1", "0", "0", "0"],
        ],
    )
    candles = read_kline_zip(zip_path, market="usdm-futures", symbol="BTCUSDT", timeframe="4h")

    report = build_continuity_report(candles, symbol="BTCUSDT", timeframe="4h")

    assert report["status"] == "fail"
    assert report["missing_bar_count"] == 1
    assert report["gaps"][0]["missing_bars"] == 1


def test_parse_checksum_accepts_binance_sidecar_shape() -> None:
    assert parse_checksum("abcd" * 16 + "  BTCUSDT-4h-2021-01.zip\n") == "abcd" * 16


def test_binance_history_import_cli_plan_only_writes_plan(tmp_path: Path) -> None:
    output_dir = tmp_path / "artifacts"

    code = import_cli(
        [
            "--symbols",
            "BTCUSDT",
            "--timeframes",
            "4h,1d",
            "--start",
            "2021-01-01",
            "--end",
            "2021-02-01",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert code == 0
    plan = json.loads((output_dir / "binance_history_import_plan.json").read_text(encoding="utf-8"))
    assert plan["archive_count"] == 4
    assert plan["archives"][0]["url"].startswith("https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/4h/")
