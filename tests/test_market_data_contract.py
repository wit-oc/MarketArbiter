from __future__ import annotations

from pathlib import Path

import pytest

from market_arbiter.core.db import init_db
from market_arbiter.core.market_data import CandleDTO, CandleValidationError, upsert_market_candles


def _candle(**overrides: object) -> CandleDTO:
    base: dict[str, object] = {
        "provider_id": "coinalyze",
        "venue": "blofin",
        "symbol": "BTCUSDT",
        "timeframe": "1h",
        "ts_open_ms": 1_700_000_000_000,
        "ts_close_ms": 1_700_000_003_600,
        "open": "100",
        "high": "110",
        "low": "90",
        "close": "105",
        "volume": "12.5",
        "dataset_version": "v1",
        "trace_id": "trace-abc",
    }
    base.update(overrides)
    return CandleDTO(**base)


def test_happy_path_ingest_is_deterministic_and_sorted(tmp_path: Path) -> None:
    conn = init_db(str(tmp_path / "ls.sqlite"))

    candles = [
        _candle(ts_open_ms=1_700_000_007_200, ts_close_ms=1_700_000_010_800, open="105", high="113", low="104", close="110"),
        _candle(ts_open_ms=1_700_000_003_600, ts_close_ms=1_700_000_007_200, open="101", high="112", low="99", close="104"),
    ]

    result = upsert_market_candles(conn, candles, ingest_ts_ms=1_700_000_011_000)
    rows = conn.execute(
        """
        SELECT ts_open_ms, ts_close_ms, open, high, low, close, volume
        FROM market_candles
        ORDER BY ts_open_ms;
        """
    ).fetchall()

    assert result == {"inserted": 2, "idempotent": 0}
    assert rows == [
        (1_700_000_003_600, 1_700_000_007_200, 101, 112, 99, 104, 12.5),
        (1_700_000_007_200, 1_700_000_010_800, 105, 113, 104, 110, 12.5),
    ]


def test_duplicate_idempotent_ingest_noops(tmp_path: Path) -> None:
    conn = init_db(str(tmp_path / "ls.sqlite"))
    candle = _candle()

    first = upsert_market_candles(conn, [candle], ingest_ts_ms=1_700_000_010_000)
    second = upsert_market_candles(conn, [candle], ingest_ts_ms=1_700_000_020_000)
    count = conn.execute("SELECT COUNT(*) FROM market_candles;").fetchone()[0]

    assert first == {"inserted": 1, "idempotent": 0}
    assert second == {"inserted": 0, "idempotent": 1}
    assert count == 1


def test_duplicate_conflict_rejected(tmp_path: Path) -> None:
    conn = init_db(str(tmp_path / "ls.sqlite"))

    upsert_market_candles(conn, [_candle()], ingest_ts_ms=1_700_000_010_000)
    with pytest.raises(CandleValidationError) as exc:
        upsert_market_candles(
            conn,
            [_candle(close="106")],
            ingest_ts_ms=1_700_000_020_000,
        )

    assert exc.value.reason_code == "CANDLE_DUPLICATE_CONFLICT"


@pytest.mark.parametrize(
    ("mutations", "reason"),
    [
        ({"open": "abc"}, "CANDLE_VALUE_INVALID"),
        ({"high": "100", "open": "120", "close": "110"}, "CANDLE_RANGE_INVALID"),
        ({"ts_close_ms": 1_700_000_000_000}, "CANDLE_TS_INVALID"),
        ({"timeframe": "2h"}, "TIMEFRAME_UNSUPPORTED"),
    ],
)
def test_invalid_candle_reasons_are_deterministic(
    tmp_path: Path,
    mutations: dict[str, object],
    reason: str,
) -> None:
    conn = init_db(str(tmp_path / "ls.sqlite"))

    with pytest.raises(CandleValidationError) as exc:
        upsert_market_candles(conn, [_candle(**mutations)], ingest_ts_ms=1_700_000_030_000)

    assert exc.value.reason_code == reason
