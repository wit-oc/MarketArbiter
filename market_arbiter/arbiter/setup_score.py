from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


ARBITER_SETUP_SCORE_CONTRACT_V1 = "arbiter_retest_setup_score_v1"
ARBITER_SR_ELIGIBILITY_CONTRACT_V1 = "arbiter_sr_lifecycle_eligibility_v1"

_HARD_SR_STATUSES = {"blocked", "expired", "flipped_pending", "invalidated", "superseded"}
_CANDIDATE_ELIGIBILITIES = {"candidate_eligible"}
_WATCH_ELIGIBILITIES = {"watch_eligible", "watch_only"}
_DISPLAY_ELIGIBILITIES = {"display_only"}


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return str(value or "").strip().lower()


@dataclass(frozen=True)
class RetestSetupThresholds:
    """Train-window thresholds for Arbiter retest setup gating.

    These thresholds belong to Arbiter, not Surveyor. Surveyor supplies durable
    level facts; Arbiter decides whether the live/replay retest is actionable.
    """

    body_ratio_min: float
    selection_score_min: float
    min_merge_family_count: int = 3


def evaluate_sr_zone_eligibility(zone: Mapping[str, Any]) -> dict[str, Any]:
    """Gate Arbiter/Sentinel behavior from Surveyor SR lifecycle metadata.

    This intentionally reads the `lifecycle`, `quality`, and `visual` blocks
    emitted by Surveyor instead of re-scoring support/resistance validity in the
    Arbiter layer. Arbiter setup scores can still judge trade actionability, but
    only after this SR metadata gate allows candidate use.
    """

    lifecycle = _mapping(zone.get("lifecycle"))
    quality = _mapping(zone.get("quality"))
    visual = _mapping(zone.get("visual"))

    status = _text(lifecycle.get("status"))
    decision_eligibility = _text(quality.get("decision_eligibility"))
    confidence_tier = str(quality.get("confidence_tier") or "").strip().upper() or None
    visual_declared = "show_on_overlay" in visual
    display_allowed = bool(visual.get("show_on_overlay")) if visual_declared else False
    reason_codes = list(quality.get("reason_codes") or []) if isinstance(quality.get("reason_codes"), list) else []
    gate_reasons: list[str] = []

    if not lifecycle or not quality:
        gate_reasons.append("sr_lifecycle_metadata_missing")
        verdict = "watch"
        candidate_allowed = False
        watch_allowed = True
        display_allowed = False
    elif status in _HARD_SR_STATUSES or decision_eligibility == "reject":
        gate_reasons.append("sr_lifecycle_reject")
        verdict = "reject"
        candidate_allowed = False
        watch_allowed = False
        display_allowed = False
    elif decision_eligibility in _CANDIDATE_ELIGIBILITIES:
        gate_reasons.append("sr_lifecycle_candidate_eligible")
        verdict = "candidate"
        candidate_allowed = True
        watch_allowed = True
        display_allowed = display_allowed if visual_declared else True
    elif decision_eligibility in _WATCH_ELIGIBILITIES:
        gate_reasons.append(f"sr_lifecycle_{decision_eligibility}")
        verdict = "watch"
        candidate_allowed = False
        watch_allowed = True
        display_allowed = display_allowed if visual_declared else True
    elif decision_eligibility in _DISPLAY_ELIGIBILITIES:
        gate_reasons.append("sr_lifecycle_display_only")
        verdict = "display"
        candidate_allowed = False
        watch_allowed = False
        display_allowed = display_allowed if visual_declared else True
    else:
        gate_reasons.append("sr_lifecycle_unknown_eligibility")
        verdict = "watch"
        candidate_allowed = False
        watch_allowed = True
        display_allowed = False

    return {
        "contract": ARBITER_SR_ELIGIBILITY_CONTRACT_V1,
        "zone_id": zone.get("zone_id") or zone.get("id"),
        "verdict": verdict,
        "candidate_allowed": candidate_allowed,
        "watch_allowed": watch_allowed,
        "display_allowed": display_allowed,
        "lifecycle_status": status or None,
        "decision_eligibility": decision_eligibility or None,
        "confidence_tier": confidence_tier,
        "reason_codes": gate_reasons + reason_codes,
        "source": "surveyor_sr_lifecycle_metadata",
    }


def score_retest_setup(features: Mapping[str, Any], thresholds: RetestSetupThresholds) -> dict[str, Any]:
    """Score whether a Surveyor-described retest is actionable for Arbiter.

    The first reusable score mirrors the walk-forward hypothesis that survived:
    strong retest candle body/displacement, high Surveyor selection score, and
    cross-family zone confluence. It intentionally does not mutate Surveyor's
    level-quality score.
    """

    body_ratio = _float(features.get("body_ratio"), float("nan"))
    selection_score = _float(features.get("selection_score"), float("nan"))
    merge_family_count = int(_float(features.get("merge_family_count"), 0))

    body_pass = body_ratio >= thresholds.body_ratio_min
    selection_pass = selection_score >= thresholds.selection_score_min
    family_pass = merge_family_count >= thresholds.min_merge_family_count
    components = {
        "body_displacement": 1.0 if body_pass else 0.0,
        "surveyor_selection": 1.0 if selection_pass else 0.0,
        "family_confluence": 1.0 if family_pass else 0.0,
    }
    setup_score = sum(components.values())
    verdict = "candidate" if setup_score >= 3.0 else "reject"
    reason_codes: list[str] = []
    if body_pass:
        reason_codes.append("retest_body_displacement_pass")
    else:
        reason_codes.append("retest_body_displacement_below_threshold")
    if selection_pass:
        reason_codes.append("surveyor_selection_score_pass")
    else:
        reason_codes.append("surveyor_selection_score_below_threshold")
    if family_pass:
        reason_codes.append("zone_family_confluence_pass")
    else:
        reason_codes.append("zone_family_confluence_below_threshold")

    return {
        "contract": ARBITER_SETUP_SCORE_CONTRACT_V1,
        "verdict": verdict,
        "setup_score": setup_score,
        "components": components,
        "reason_codes": reason_codes,
        "features": {
            "body_ratio": body_ratio,
            "selection_score": selection_score,
            "merge_family_count": merge_family_count,
        },
        "thresholds": {
            "body_ratio_min": thresholds.body_ratio_min,
            "selection_score_min": thresholds.selection_score_min,
            "min_merge_family_count": thresholds.min_merge_family_count,
        },
    }
