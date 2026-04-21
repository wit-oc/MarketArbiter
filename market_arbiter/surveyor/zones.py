from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List


class ZoneKind(str, Enum):
    SUPPORT = "support"
    RESISTANCE = "resistance"


class ZoneState(str, Enum):
    ACTIVE = "active"
    FLIPPED = "flipped"
    INVALIDATED = "invalidated"


@dataclass
class Zone:
    id: str
    kind: ZoneKind
    low: float
    high: float
    created_at: int
    state: ZoneState = ZoneState.ACTIVE


class ZoneEngine:
    """Deterministic SR zone lifecycle manager.

    Notes:
    - Pivot detection is intentionally delegated to upstream signal prep.
    - This engine focuses on lifecycle operations: birth, merge, invalidate, flip.
    """

    def __init__(self, merge_overlap_ratio: float = 0.2):
        self.merge_overlap_ratio = merge_overlap_ratio
        self.zones: List[Zone] = []

    def add_zone(self, zone: Zone) -> Zone:
        for i, existing in enumerate(self.zones):
            if existing.kind != zone.kind or existing.state == ZoneState.INVALIDATED:
                continue
            if self._overlaps(existing, zone):
                merged = Zone(
                    id=existing.id,
                    kind=existing.kind,
                    low=min(existing.low, zone.low),
                    high=max(existing.high, zone.high),
                    created_at=min(existing.created_at, zone.created_at),
                    state=existing.state,
                )
                self.zones[i] = merged
                return merged
        self.zones.append(zone)
        return zone

    def invalidate_zone(self, zone_id: str) -> None:
        for z in self.zones:
            if z.id == zone_id:
                z.state = ZoneState.INVALIDATED
                return

    def flip_zone(self, zone_id: str) -> None:
        for z in self.zones:
            if z.id == zone_id:
                z.kind = ZoneKind.SUPPORT if z.kind == ZoneKind.RESISTANCE else ZoneKind.RESISTANCE
                z.state = ZoneState.FLIPPED
                return

    def active_zones(self) -> List[Zone]:
        return [z for z in self.zones if z.state in (ZoneState.ACTIVE, ZoneState.FLIPPED)]

    def _overlaps(self, a: Zone, b: Zone) -> bool:
        intersection = max(0.0, min(a.high, b.high) - max(a.low, b.low))
        width = max((a.high - a.low), (b.high - b.low), 1e-9)
        return (intersection / width) >= self.merge_overlap_ratio
