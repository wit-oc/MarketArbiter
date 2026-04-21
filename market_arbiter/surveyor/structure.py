from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List


class RegimeState(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"


class StructureBias(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


@dataclass
class Pivot:
    index: int
    price: float
    kind: str  # "high" | "low"


@dataclass
class StructurePoint:
    index: int
    bias: StructureBias
    transition: bool


@dataclass
class RegimeTransition:
    index: int
    from_regime: RegimeState
    to_regime: RegimeState
    reason: str


@dataclass
class RegimePoint:
    index: int
    regime: RegimeState
    choch_candidate: RegimeState | None
    bos_confirmed: bool
    transition: RegimeTransition | None


def detect_pivots(highs: List[float], lows: List[float], left: int = 2, right: int = 2) -> List[Pivot]:
    """Lookahead-safe pivot detection.

    Pivot at i is emitted only when i+right is available; callers should only use
    pivots up to current bar index to avoid leakage in streaming runs.
    """
    pivots: List[Pivot] = []
    n = len(highs)
    for i in range(left, n - right):
        h = highs[i]
        l = lows[i]
        is_high = all(h > highs[j] for j in range(i - left, i + right + 1) if j != i)
        is_low = all(l < lows[j] for j in range(i - left, i + right + 1) if j != i)
        if is_high:
            pivots.append(Pivot(index=i, price=h, kind="high"))
        if is_low:
            pivots.append(Pivot(index=i, price=l, kind="low"))
    return pivots


def classify_structure_from_pivots(pivots: List[Pivot]) -> List[StructurePoint]:
    points: List[StructurePoint] = []
    highs = [p for p in pivots if p.kind == "high"]
    lows = [p for p in pivots if p.kind == "low"]

    # Walk in time order and infer bias from last two highs + lows
    last_bias = StructureBias.NEUTRAL
    for p in sorted(pivots, key=lambda x: x.index):
        relevant_highs = [x for x in highs if x.index <= p.index][-2:]
        relevant_lows = [x for x in lows if x.index <= p.index][-2:]
        bias = StructureBias.NEUTRAL
        if len(relevant_highs) == 2 and len(relevant_lows) == 2:
            hh = relevant_highs[-1].price > relevant_highs[-2].price
            hl = relevant_lows[-1].price > relevant_lows[-2].price
            lh = relevant_highs[-1].price < relevant_highs[-2].price
            ll = relevant_lows[-1].price < relevant_lows[-2].price
            if hh and hl:
                bias = StructureBias.BULLISH
            elif lh and ll:
                bias = StructureBias.BEARISH
            else:
                bias = StructureBias.NEUTRAL

        transition = bias != last_bias and bias != StructureBias.NEUTRAL
        points.append(StructurePoint(index=p.index, bias=bias, transition=transition))
        last_bias = bias
    return points


def project_regime(points: List[StructurePoint], initial: RegimeState = RegimeState.BULLISH) -> List[RegimePoint]:
    """Persistent bullish/bearish regime with CHoCH->BoS flip confirmation.

    - Neutral structure points never force neutral regime.
    - Opposite non-neutral structure creates CHoCH candidate.
    - Regime flips only when that opposite side appears again (BoS confirm).
    """
    regime = initial
    choch_candidate: RegimeState | None = None
    out: List[RegimePoint] = []

    for p in points:
        transition: RegimeTransition | None = None
        bos_confirmed = False

        target: RegimeState | None = None
        if p.bias == StructureBias.BULLISH:
            target = RegimeState.BULLISH
        elif p.bias == StructureBias.BEARISH:
            target = RegimeState.BEARISH

        if target is None:
            pass
        elif target == regime:
            choch_candidate = None
        elif choch_candidate != target:
            choch_candidate = target
        else:
            transition = RegimeTransition(
                index=p.index,
                from_regime=regime,
                to_regime=target,
                reason="choch_candidate_then_bos_confirm",
            )
            regime = target
            choch_candidate = None
            bos_confirmed = True

        out.append(
            RegimePoint(
                index=p.index,
                regime=regime,
                choch_candidate=choch_candidate,
                bos_confirmed=bos_confirmed,
                transition=transition,
            )
        )

    return out
