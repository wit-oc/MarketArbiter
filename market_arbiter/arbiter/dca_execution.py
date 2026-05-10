from __future__ import annotations

from typing import Any, Mapping, Sequence


ARBITER_DCA_EXECUTION_CONTRACT_V1 = "arbiter_dca_execution_v1"
DCA_PLANS: dict[str, tuple[float, ...]] = {
    "single_100": (1.0,),
    "dca_50_50": (0.5, 0.5),
    "dca_20_30_50": (0.2, 0.3, 0.5),
}


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def planned_dca_entries(
    *,
    side: str,
    first_entry_price: float,
    zone_low: float,
    zone_high: float,
    plan_id: str,
) -> dict[str, Any]:
    """Return deterministic DCA limit ladder for an SR-zone retest.

    Semantics from the first walk-forward DCA research pass:
    - first tranche is market/next-open entry,
    - 50/50 second tranche sits at full-zone midpoint,
    - 20/30/50 second and third tranches sit at midpoint and far boundary,
    - total risk is the budget; unfilled tranches leave risk unused.
    """

    normalized_side = str(side or "").lower()
    if normalized_side not in {"long", "short"}:
        return {"contract": ARBITER_DCA_EXECUTION_CONTRACT_V1, "status": "reject", "reason": "unsupported_side"}
    weights = DCA_PLANS.get(plan_id)
    if weights is None:
        return {"contract": ARBITER_DCA_EXECUTION_CONTRACT_V1, "status": "reject", "reason": "unknown_plan", "plan_id": plan_id}
    low = float(zone_low)
    high = float(zone_high)
    if high <= low:
        return {"contract": ARBITER_DCA_EXECUTION_CONTRACT_V1, "status": "reject", "reason": "invalid_zone_bounds"}
    first = float(first_entry_price)
    mid = (low + high) / 2.0
    if plan_id == "single_100":
        prices = [first]
    elif normalized_side == "long" and plan_id == "dca_50_50":
        prices = [first, min(first, mid)]
    elif normalized_side == "long":
        prices = [first, min(first, mid), min(first, low)]
    elif plan_id == "dca_50_50":
        prices = [first, max(first, mid)]
    else:
        prices = [first, max(first, mid), max(first, high)]
    return {
        "contract": ARBITER_DCA_EXECUTION_CONTRACT_V1,
        "status": "ok",
        "plan_id": plan_id,
        "side": normalized_side,
        "entries": [
            {"tranche_index": idx, "weight": weight, "entry_price": price, "entry_type": "market" if idx == 0 else "limit"}
            for idx, (weight, price) in enumerate(zip(weights, prices))
        ],
        "risk_budget_semantics": "weights_allocate_total_trade_risk;unfilled_tranches_leave_risk_unused",
    }


def graduated_confluence_risk_pct(features: Mapping[str, Any], thresholds: Mapping[str, Any]) -> dict[str, Any]:
    """Return graduated risk percentage from Arbiter setup confluences.

    This is intentionally capped at 4% for the first reusable artifact. It keeps
    the flat 3% comparison available while allowing stronger confluence stacks
    to size up modestly in research.
    """

    body = _float(features.get("body_ratio"), float("nan"))
    selection = _float(features.get("selection_score"), float("nan"))
    family_count = int(_float(features.get("merge_family_count"), 0))
    risk = 1.0
    reasons = ["base_1pct"]
    if family_count >= 3:
        risk += 1.0
        reasons.append("family3_plus_1pct")
    if selection >= _float(thresholds.get("selection_p50"), float("inf")):
        risk += 0.75
        reasons.append("selection_p50_plus_0_75pct")
    if body >= _float(thresholds.get("body_p50"), float("inf")):
        risk += 0.75
        reasons.append("body_p50_plus_0_75pct")
    if selection >= _float(thresholds.get("selection_p60"), float("inf")) and body >= _float(thresholds.get("body_p60"), float("inf")):
        risk += 0.5
        reasons.append("body_and_selection_p60_plus_0_5pct")
    risk = max(1.0, min(4.0, risk))
    return {
        "contract": ARBITER_DCA_EXECUTION_CONTRACT_V1,
        "risk_model": "graduated_confluence_v1",
        "risk_pct": risk,
        "reason_codes": reasons,
        "cap_pct": 4.0,
    }
