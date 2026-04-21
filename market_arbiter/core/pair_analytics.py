from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def load_candles_from_csv(path: str | Path, *, limit: int = 600) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append({
                "open": _as_float(row.get("open")),
                "high": _as_float(row.get("high")),
                "low": _as_float(row.get("low")),
                "close": _as_float(row.get("close")),
                "volume": _as_float(row.get("volume")),
                "timestamp": row.get("timestamp") or row.get("ts") or row.get("datetime"),
            })
    if limit > 0 and len(rows) > limit:
        rows = rows[-limit:]
    return rows
