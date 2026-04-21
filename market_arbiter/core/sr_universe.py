from __future__ import annotations

from pathlib import Path

DEFAULT_DATA_ROOT = Path("data/market_structure")
TIMEFRAME_FILE_KEYS = {"1D": "1d", "4H": "4h", "1H": "1h", "15M": "15m"}
SOURCE_PRIORITY = ("okx_ccxt", "blofin_ccxt", "blofin_derived_from_1h")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def symbol_to_asset(symbol: str) -> str:
    base = str(symbol or "").upper().strip()
    if base.endswith("USDT"):
        base = base[:-4]
    return "".join(ch for ch in base.lower() if ch.isalnum())


def resolve_market_structure_csv(symbol: str, tf: str, *, data_root: str | Path = DEFAULT_DATA_ROOT) -> Path | None:
    root = _repo_root() / Path(data_root)
    asset = symbol_to_asset(symbol)
    tf_key = TIMEFRAME_FILE_KEYS.get(str(tf).upper())
    if not asset or not tf_key:
        return None
    for source in SOURCE_PRIORITY:
        candidate = root / f"{asset}_{tf_key}_{source}_2022_to_now.csv"
        if candidate.exists():
            return candidate
    candidates = sorted(root.glob(f"{asset}_{tf_key}_*.csv"))
    return candidates[0] if candidates else None
