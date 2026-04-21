from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Mapping, Sequence

import pandas as pd

from .zones import Zone


class DynamicLevelName(str, Enum):
    YVWAP = "YVWAP"
    QVWAP = "QVWAP"
    RYVWAP = "RYVWAP"
    RQVWAP = "RQVWAP"
    EMA200 = "EMA200"
    EMA12 = "EMA12"


class PriceSide(str, Enum):
    ABOVE = "above"
    BELOW = "below"
    OVERLAPPING = "overlapping"


class ZoneRelation(str, Enum):
    ABOVE_ZONE = "above_zone"
    BELOW_ZONE = "below_zone"
    INSIDE_ZONE = "inside_zone"
    OVERLAPPING_ZONE = "overlapping_zone"
    NEAR_ZONE = "near_zone"
    FAR_FROM_ZONE = "far_from_zone"


@dataclass(frozen=True)
class DynamicLevelConfig:
    default_feed_provider: str = "OKX"
    price_overlap_bps: float = 5.0
    zone_overlap_bps: float = 5.0
    near_zone_bps: float = 25.0
    far_zone_bps: float = 150.0


@dataclass(frozen=True)
class DynamicLevelSurface:
    level_name: str
    timeframe: str
    available: bool
    level_value: float | None
    price_side: str | None
    distance_abs: float | None
    distance_pct: float | None
    zone_relation: str | None
    timeframe_bar_ts: int | None
    availability_reason: str | None


@dataclass(frozen=True)
class DynamicLevelPacket:
    symbol: str
    as_of_ts: str
    intended_direction: str
    current_price: float
    zone_id: str | None
    zone_low: float | None
    zone_high: float | None
    source_event_id: str | None
    source_swing_id: str | None
    source_contract_version: str | None
    fib_context_id: str | None
    feed_provider: str
    feed_timeframe: str
    feed_bar_ts: int | None
    feed_provenance_note: str
    levels: list[dict]


_TIMEFRAME_RULES = {
    "4h": "4h",
    "1d": "1d",
}


_TIMEFRAME_LEVELS = {
    "4h": (
        DynamicLevelName.YVWAP.value,
        DynamicLevelName.QVWAP.value,
        DynamicLevelName.EMA200.value,
        DynamicLevelName.EMA12.value,
    ),
    "1d": (
        DynamicLevelName.YVWAP.value,
        DynamicLevelName.QVWAP.value,
        DynamicLevelName.RYVWAP.value,
        DynamicLevelName.RQVWAP.value,
        DynamicLevelName.EMA200.value,
        DynamicLevelName.EMA12.value,
    ),
}


def classify_price_side(current_price: float, level_value: float, cfg: DynamicLevelConfig | None = None) -> str:
    cfg = cfg or DynamicLevelConfig()
    if current_price <= 1e-12:
        return PriceSide.OVERLAPPING.value
    distance_bps = abs(current_price - level_value) / abs(current_price) * 10_000.0
    if distance_bps <= cfg.price_overlap_bps:
        return PriceSide.OVERLAPPING.value
    return PriceSide.ABOVE.value if current_price > level_value else PriceSide.BELOW.value


def classify_zone_relation(
    level_value: float,
    zone_low: float | None,
    zone_high: float | None,
    cfg: DynamicLevelConfig | None = None,
) -> str:
    cfg = cfg or DynamicLevelConfig()
    if zone_low is None or zone_high is None:
        return ZoneRelation.FAR_FROM_ZONE.value

    low = min(zone_low, zone_high)
    high = max(zone_low, zone_high)
    ref = max(abs(level_value), abs((low + high) / 2.0), 1e-9)

    if low < level_value < high:
        return ZoneRelation.INSIDE_ZONE.value

    edge_distance_bps = min(abs(level_value - low), abs(level_value - high)) / ref * 10_000.0
    if edge_distance_bps <= cfg.zone_overlap_bps:
        return ZoneRelation.OVERLAPPING_ZONE.value
    if edge_distance_bps <= cfg.near_zone_bps:
        return ZoneRelation.NEAR_ZONE.value
    if edge_distance_bps >= cfg.far_zone_bps:
        return ZoneRelation.FAR_FROM_ZONE.value
    return ZoneRelation.BELOW_ZONE.value if level_value < low else ZoneRelation.ABOVE_ZONE.value


def _bars_to_frame(
    bars: Sequence,
    *,
    timestamps: Sequence[int] | None,
    volumes: Sequence[float] | None,
) -> pd.DataFrame:
    rows: list[dict] = []
    for i, bar in enumerate(bars):
        ts = timestamps[i] if timestamps is not None else getattr(bar, "timestamp", None)
        vol = volumes[i] if volumes is not None else getattr(bar, "volume", None)
        rows.append(
            {
                "index": getattr(bar, "index", i),
                "timestamp": ts,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": None if vol is None else float(vol),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if df["timestamp"].isna().any():
        raise ValueError("dynamic_levels requires timestamps for point-in-time computation")
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["ts"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    return df


def _aggregate(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    rule = _TIMEFRAME_RULES[timeframe]
    d = df.set_index("ts")
    out = pd.DataFrame()
    out["open"] = d["open"].resample(rule).first()
    out["high"] = d["high"].resample(rule).max()
    out["low"] = d["low"].resample(rule).min()
    out["close"] = d["close"].resample(rule).last()
    out["volume"] = d["volume"].resample(rule).sum(min_count=1)
    out = out.dropna(subset=["open", "high", "low", "close"]).reset_index()
    out["timestamp"] = (out["ts"].astype("int64") // 10**9).astype(int)
    return out


def _quarter_start(ts: pd.Timestamp) -> pd.Timestamp:
    month = ((ts.month - 1) // 3) * 3 + 1
    return pd.Timestamp(year=ts.year, month=month, day=1, tz="UTC")


def _anchored_vwap(
    agg: pd.DataFrame,
    *,
    as_of_ts: pd.Timestamp,
    period: str,
) -> tuple[float | None, str | None]:
    if agg.empty or "volume" not in agg.columns:
        return None, "missing_aggregate_bars"

    if period == "year":
        start = pd.Timestamp(year=as_of_ts.year, month=1, day=1, tz="UTC")
    elif period == "quarter":
        start = _quarter_start(as_of_ts)
    else:
        raise ValueError(f"unsupported period: {period}")

    subset = agg[(agg["ts"] >= start) & (agg["ts"] <= as_of_ts)].copy()
    if subset.empty:
        return None, "anchor_window_empty"
    first_ts = subset.iloc[0]["ts"]
    if first_ts > start:
        return None, "incomplete_anchor_history"
    if subset["volume"].isna().any():
        return None, "missing_volume"
    volume_total = float(subset["volume"].sum())
    if volume_total <= 1e-12:
        return None, "zero_volume"

    typical = (subset["high"] + subset["low"] + subset["close"]) / 3.0
    vwap = float((typical * subset["volume"]).sum() / volume_total)
    return vwap, None


def _rolling_vwap(
    agg: pd.DataFrame,
    *,
    length: int,
) -> tuple[float | None, str | None]:
    if agg.empty or "volume" not in agg.columns:
        return None, "missing_aggregate_bars"
    if len(agg) < length:
        return None, f"insufficient_history_for_rolling_vwap_{length}"

    subset = agg.iloc[-length:].copy()
    if subset["volume"].isna().any():
        return None, "missing_volume"
    volume_total = float(subset["volume"].sum())
    if volume_total <= 1e-12:
        return None, "zero_volume"

    typical = (subset["high"] + subset["low"] + subset["close"]) / 3.0
    vwap = float((typical * subset["volume"]).sum() / volume_total)
    return vwap, None


def _ema_value(agg: pd.DataFrame, span: int) -> tuple[float | None, str | None]:
    if len(agg) < span:
        return None, f"insufficient_history_for_ema_{span}"
    series = agg["close"].ewm(span=span, adjust=False).mean()
    return float(series.iloc[-1]), None


def _level_value_for_name(agg: pd.DataFrame, level_name: str) -> tuple[float | None, str | None]:
    as_of_ts = agg.iloc[-1]["ts"]
    if level_name == DynamicLevelName.YVWAP.value:
        return _anchored_vwap(agg, as_of_ts=as_of_ts, period="year")
    if level_name == DynamicLevelName.QVWAP.value:
        return _anchored_vwap(agg, as_of_ts=as_of_ts, period="quarter")
    if level_name == DynamicLevelName.RYVWAP.value:
        return _rolling_vwap(agg, length=365)
    if level_name == DynamicLevelName.RQVWAP.value:
        return _rolling_vwap(agg, length=90)
    if level_name == DynamicLevelName.EMA200.value:
        return _ema_value(agg, 200)
    if level_name == DynamicLevelName.EMA12.value:
        return _ema_value(agg, 12)
    raise ValueError(f"unsupported level: {level_name}")


def _build_surface(
    *,
    current_price: float,
    level_name: str,
    timeframe: str,
    agg: pd.DataFrame,
    zone_low: float | None,
    zone_high: float | None,
    cfg: DynamicLevelConfig,
) -> DynamicLevelSurface:
    level_value, availability_reason = _level_value_for_name(agg, level_name)
    if level_value is None:
        return DynamicLevelSurface(
            level_name=level_name,
            timeframe=timeframe,
            available=False,
            level_value=None,
            price_side=None,
            distance_abs=None,
            distance_pct=None,
            zone_relation=None,
            timeframe_bar_ts=int(agg.iloc[-1]["timestamp"]) if not agg.empty else None,
            availability_reason=availability_reason or "level_unavailable",
        )

    price_side = classify_price_side(current_price, level_value, cfg)
    distance_abs = abs(current_price - level_value)
    distance_pct = distance_abs / max(abs(current_price), 1e-9)
    zone_relation = classify_zone_relation(level_value, zone_low, zone_high, cfg)
    return DynamicLevelSurface(
        level_name=level_name,
        timeframe=timeframe,
        available=True,
        level_value=level_value,
        price_side=price_side,
        distance_abs=distance_abs,
        distance_pct=distance_pct,
        zone_relation=zone_relation,
        timeframe_bar_ts=int(agg.iloc[-1]["timestamp"]),
        availability_reason=None,
    )


def build_dynamic_level_packet(
    bars: Sequence,
    *,
    as_of_bar_index: int,
    symbol: str,
    base_tf: str,
    intended_direction: str = "unknown",
    selected_zone: Zone | None = None,
    timestamps: Sequence[int] | None = None,
    volumes: Sequence[float] | None = None,
    feed_provider: str | None = None,
    feed_provenance_note: str | None = None,
    source_event_id: str | None = None,
    source_swing_id: str | None = None,
    source_contract_version: str | None = None,
    fib_context_id: str | None = None,
    cfg: DynamicLevelConfig | None = None,
) -> DynamicLevelPacket:
    cfg = cfg or DynamicLevelConfig()
    prefix = list(bars[: as_of_bar_index + 1])
    if not prefix:
        raise ValueError("build_dynamic_level_packet requires at least one bar in range")

    ts_prefix = None if timestamps is None else list(timestamps[: as_of_bar_index + 1])
    vol_prefix = None if volumes is None else list(volumes[: as_of_bar_index + 1])
    df = _bars_to_frame(prefix, timestamps=ts_prefix, volumes=vol_prefix)
    if df.empty:
        raise ValueError("build_dynamic_level_packet requires non-empty bar data")

    current_bar = prefix[-1]
    current_price = float(current_bar.close)
    agg_4h = _aggregate(df, "4h")
    agg_1d = _aggregate(df, "1d")

    levels: list[DynamicLevelSurface] = []
    for timeframe, agg in (("4h", agg_4h), ("1d", agg_1d)):
        level_names = _TIMEFRAME_LEVELS[timeframe]
        if agg.empty:
            for level_name in level_names:
                levels.append(
                    DynamicLevelSurface(
                        level_name=level_name,
                        timeframe=timeframe,
                        available=False,
                        level_value=None,
                        price_side=None,
                        distance_abs=None,
                        distance_pct=None,
                        zone_relation=None,
                        timeframe_bar_ts=None,
                        availability_reason="aggregate_unavailable",
                    )
                )
            continue

        for level_name in level_names:
            levels.append(
                _build_surface(
                    current_price=current_price,
                    level_name=level_name,
                    timeframe=timeframe,
                    agg=agg,
                    zone_low=(selected_zone.low if selected_zone else None),
                    zone_high=(selected_zone.high if selected_zone else None),
                    cfg=cfg,
                )
            )

    as_of_ts_value = int(df.iloc[-1]["timestamp"])
    return DynamicLevelPacket(
        symbol=symbol,
        as_of_ts=str(as_of_ts_value),
        intended_direction=intended_direction,
        current_price=current_price,
        zone_id=(selected_zone.id if selected_zone else None),
        zone_low=(selected_zone.low if selected_zone else None),
        zone_high=(selected_zone.high if selected_zone else None),
        source_event_id=source_event_id,
        source_swing_id=source_swing_id,
        source_contract_version=source_contract_version,
        fib_context_id=fib_context_id,
        feed_provider=feed_provider or cfg.default_feed_provider,
        feed_timeframe=base_tf,
        feed_bar_ts=as_of_ts_value,
        feed_provenance_note=feed_provenance_note or "point_in_time_dynamic_level_reconstruction",
        levels=[asdict(level) for level in levels],
    )


def dynamic_level_packet_to_dict(packet: DynamicLevelPacket) -> dict:
    return asdict(packet)


def flatten_dynamic_level_packet(packet: DynamicLevelPacket) -> Mapping[str, object]:
    out: dict[str, object] = {
        "dynamic_as_of_ts": packet.as_of_ts,
        "dynamic_intended_direction": packet.intended_direction,
        "dynamic_current_price": packet.current_price,
        "dynamic_zone_id": packet.zone_id,
        "dynamic_zone_low": packet.zone_low,
        "dynamic_zone_high": packet.zone_high,
        "dynamic_source_event_id": packet.source_event_id,
        "dynamic_source_swing_id": packet.source_swing_id,
        "dynamic_source_contract_version": packet.source_contract_version,
        "dynamic_fib_context_id": packet.fib_context_id,
        "dynamic_feed_provider": packet.feed_provider,
        "dynamic_feed_timeframe": packet.feed_timeframe,
        "dynamic_feed_bar_ts": packet.feed_bar_ts,
        "dynamic_feed_provenance_note": packet.feed_provenance_note,
    }
    for level in packet.levels:
        prefix = f"dynamic_{level['timeframe']}_{level['level_name'].lower()}"
        out[f"{prefix}_available"] = level["available"]
        out[f"{prefix}_value"] = level["level_value"]
        out[f"{prefix}_price_side"] = level["price_side"]
        out[f"{prefix}_distance_abs"] = level["distance_abs"]
        out[f"{prefix}_distance_pct"] = level["distance_pct"]
        out[f"{prefix}_zone_relation"] = level["zone_relation"]
        out[f"{prefix}_timeframe_bar_ts"] = level["timeframe_bar_ts"]
        out[f"{prefix}_availability_reason"] = level["availability_reason"]
    return out
