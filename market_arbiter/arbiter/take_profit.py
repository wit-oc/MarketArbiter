from __future__ import annotations

from typing import Any


ARBITER_TAKE_PROFIT_CONTRACT_V1 = "arbiter_take_profit_policy_v1"


def planned_take_profits(*, final_rr: float, plan_id: str = "tp_25_50_25") -> dict[str, Any]:
    """Return deterministic partial take-profit ladder.

    Default doctrine:
    - 25% off at 1R.
    - 50% off at 2R.
    - final 25% at final target.

    If final target is exactly 2R, the 2R and final tranches merge, producing
    25% at 1R and 75% at 2R. If final target is below 2R, the remaining weight
    is merged into the final target after the 1R tranche when possible.
    """

    if plan_id not in {"single_final", "tp_25_50_25"}:
        return {"contract": ARBITER_TAKE_PROFIT_CONTRACT_V1, "status": "reject", "reason": "unknown_plan", "plan_id": plan_id}
    rr = float(final_rr)
    if rr <= 0:
        return {"contract": ARBITER_TAKE_PROFIT_CONTRACT_V1, "status": "reject", "reason": "invalid_final_rr", "final_rr": final_rr}
    if plan_id == "single_final":
        tranches = [{"tp_index": 0, "rr": rr, "weight": 1.0, "role": "final"}]
    elif rr <= 1.0:
        tranches = [{"tp_index": 0, "rr": rr, "weight": 1.0, "role": "final"}]
    elif rr < 2.0:
        tranches = [
            {"tp_index": 0, "rr": 1.0, "weight": 0.25, "role": "first_protective_tp"},
            {"tp_index": 1, "rr": rr, "weight": 0.75, "role": "final"},
        ]
    elif rr == 2.0:
        tranches = [
            {"tp_index": 0, "rr": 1.0, "weight": 0.25, "role": "first_protective_tp"},
            {"tp_index": 1, "rr": 2.0, "weight": 0.75, "role": "merged_second_and_final"},
        ]
    else:
        tranches = [
            {"tp_index": 0, "rr": 1.0, "weight": 0.25, "role": "first_protective_tp"},
            {"tp_index": 1, "rr": 2.0, "weight": 0.50, "role": "second_tp"},
            {"tp_index": 2, "rr": rr, "weight": 0.25, "role": "final"},
        ]
    return {
        "contract": ARBITER_TAKE_PROFIT_CONTRACT_V1,
        "status": "ok",
        "plan_id": plan_id,
        "final_rr": rr,
        "tranches": tranches,
        "first_tp_actions": ["move_stop_to_average_entry", "cancel_pending_dca_entries"],
    }
