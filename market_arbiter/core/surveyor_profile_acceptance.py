from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from market_arbiter.core.surveyor_bundle_profile import SURVEYOR_BUNDLE_PROFILE_PAYLOAD_CONTRACT


SURVEYOR_PROFILE_ACCEPTANCE_CONTRACT = "surveyor_profile_acceptance_v1"

ACCEPTED = "accepted"
ACCEPTED_WITH_CAUTION = "accepted_with_caution"
REJECTED = "rejected"

_CAUTION_RANK = {"none": 0, "low": 1, "medium": 2, "high": 3}
_BAD_REQUIRED_STATUSES = {"blocked", "degraded", "invalid", "partial", "stale", "unavailable", "unknown"}
_CAUTION_OPTIONAL_STATUSES = {"degraded", "partial", "replay_only", "stale", "unavailable", "unknown"}


@dataclass(frozen=True)
class ProfileAcceptancePolicy:
    profile_id: str
    required_families: tuple[str, ...]
    optional_families: tuple[str, ...] = ()
    allowed_build_modes: tuple[str, ...] = ("live", "mixed")
    reject_replay_only_required: bool = True
    accepted_decision_mode: str = "consumer_candidate"
    caution_decision_mode: str = "consumer_candidate_with_caution"


DEFAULT_PROFILE_ACCEPTANCE_POLICIES: dict[str, ProfileAcceptancePolicy] = {
    "arbiter_core": ProfileAcceptancePolicy(
        profile_id="arbiter_core",
        required_families=("feed_state", "structure_state", "sr_zones"),
        optional_families=("fib_context", "dynamic_levels"),
        allowed_build_modes=("live", "mixed"),
        reject_replay_only_required=True,
        accepted_decision_mode="live_decision_candidate",
        caution_decision_mode="live_watch_only",
    ),
    "backtest_core": ProfileAcceptancePolicy(
        profile_id="backtest_core",
        required_families=("feed_state", "structure_state", "sr_zones", "interaction_lifecycle"),
        optional_families=("fib_context", "dynamic_levels"),
        allowed_build_modes=("live", "mixed", "replay"),
        reject_replay_only_required=False,
        accepted_decision_mode="backtest_candidate",
        caution_decision_mode="backtest_candidate_with_caution",
    ),
}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _family_status(payload: Mapping[str, Any], family: str) -> str:
    family_payload = _as_mapping(_as_mapping(payload.get("datasets")).get(family))
    return str(family_payload.get("status") or "unknown")


def _reason(*, reason_code: str, severity: str, message: str, family: str | None = None, value: Any = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "reason_code": reason_code,
        "severity": severity,
        "message": message,
    }
    if family is not None:
        payload["family"] = family
    if value is not None:
        payload["value"] = value
    return payload


def _raise_caution(current: str, candidate: str) -> str:
    return candidate if _CAUTION_RANK[candidate] > _CAUTION_RANK[current] else current


def evaluate_surveyor_profile_acceptance(
    payload: Mapping[str, Any],
    *,
    policy: ProfileAcceptancePolicy | None = None,
) -> dict[str, Any]:
    """Classify whether a profile-selected Surveyor payload is usable by a consumer.

    This is a contract gate, not strategy logic. It fails closed for invalid or
    degraded required inputs, while allowing optional-family issues to surface as
    explicit caution instead of silently contaminating downstream decisions.
    """

    profile_id = str(payload.get("profile_id") or "")
    resolved_policy = policy or DEFAULT_PROFILE_ACCEPTANCE_POLICIES.get(profile_id)
    reasons: list[dict[str, Any]] = []
    caution_level = "none"

    unknown_policy = resolved_policy is None
    if unknown_policy:
        resolved_policy = ProfileAcceptancePolicy(profile_id=profile_id or "unknown", required_families=())
        reasons.append(
            _reason(
                reason_code="unknown_profile_policy",
                severity="reject",
                message=f"no acceptance policy exists for profile {profile_id or 'unknown'}",
                value=profile_id or None,
            )
        )

    if payload.get("contract") != SURVEYOR_BUNDLE_PROFILE_PAYLOAD_CONTRACT:
        reasons.append(
            _reason(
                reason_code="unexpected_payload_contract",
                severity="reject",
                message="payload is not a Surveyor bundle profile payload",
                value=payload.get("contract"),
            )
        )

    if profile_id != resolved_policy.profile_id and not unknown_policy:
        reasons.append(
            _reason(
                reason_code="policy_profile_mismatch",
                severity="reject",
                message=f"payload profile {profile_id or 'unknown'} does not match policy {resolved_policy.profile_id}",
                value=profile_id or None,
            )
        )

    meta = _as_mapping(payload.get("meta"))
    build_mode = str(meta.get("build_mode") or "unknown")
    if build_mode not in resolved_policy.allowed_build_modes:
        reasons.append(
            _reason(
                reason_code="build_mode_not_allowed",
                severity="reject",
                message=f"profile {resolved_policy.profile_id} does not allow build_mode {build_mode}",
                value=build_mode,
            )
        )

    selection = _as_mapping(payload.get("selection"))
    missing_required = [str(family) for family in (selection.get("missing_required_families") or [])]
    if payload.get("profile_status") == "invalid" or missing_required:
        reasons.append(
            _reason(
                reason_code="missing_required_families",
                severity="reject",
                message="profile payload is missing required families",
                value=missing_required,
            )
        )

    datasets = _as_mapping(payload.get("datasets"))
    for family in resolved_policy.required_families:
        if family not in datasets:
            reasons.append(
                _reason(
                    reason_code="required_family_absent",
                    severity="reject",
                    message=f"required family {family} is absent from payload",
                    family=family,
                )
            )
            continue
        status = _family_status(payload, family)
        if status in _BAD_REQUIRED_STATUSES:
            reasons.append(
                _reason(
                    reason_code="required_family_not_usable",
                    severity="reject",
                    message=f"required family {family} has unusable status {status}",
                    family=family,
                    value=status,
                )
            )
        elif status == "replay_only" and resolved_policy.reject_replay_only_required:
            reasons.append(
                _reason(
                    reason_code="required_family_replay_only",
                    severity="reject",
                    message=f"required family {family} is replay_only for a live consumer profile",
                    family=family,
                    value=status,
                )
            )

    for family in resolved_policy.optional_families:
        if family not in datasets:
            reasons.append(
                _reason(
                    reason_code="optional_family_absent",
                    severity="warn",
                    message=f"optional family {family} is absent from payload",
                    family=family,
                )
            )
            caution_level = _raise_caution(caution_level, "low")
            continue
        status = _family_status(payload, family)
        if status in _CAUTION_OPTIONAL_STATUSES:
            reasons.append(
                _reason(
                    reason_code="optional_family_caution",
                    severity="warn",
                    message=f"optional family {family} has caution status {status}",
                    family=family,
                    value=status,
                )
            )
            caution_level = _raise_caution(caution_level, "medium" if status in {"degraded", "stale", "unavailable"} else "low")

    source_status = str(meta.get("source_bundle_status") or "unknown")
    if source_status in {"degraded", "partial", "stale"}:
        reasons.append(
            _reason(
                reason_code="source_bundle_caution",
                severity="warn",
                message=f"source bundle status is {source_status}",
                value=source_status,
            )
        )
        caution_level = _raise_caution(caution_level, "medium")
    elif source_status == "replay_only" and resolved_policy.profile_id != "backtest_core":
        reasons.append(
            _reason(
                reason_code="source_bundle_replay_only",
                severity="reject",
                message="source bundle is replay_only for a non-backtest profile",
                value=source_status,
            )
        )

    reject_reasons = [reason for reason in reasons if reason.get("severity") == "reject"]
    warning_reasons = [reason for reason in reasons if reason.get("severity") == "warn"]
    if reject_reasons:
        acceptance_status = REJECTED
        decision_mode = "rejected"
        caution_level = "high"
    elif warning_reasons:
        acceptance_status = ACCEPTED_WITH_CAUTION
        decision_mode = resolved_policy.caution_decision_mode
    else:
        acceptance_status = ACCEPTED
        decision_mode = resolved_policy.accepted_decision_mode

    return {
        "contract": SURVEYOR_PROFILE_ACCEPTANCE_CONTRACT,
        "profile_id": profile_id or resolved_policy.profile_id,
        "acceptance_status": acceptance_status,
        "decision_mode": decision_mode,
        "caution_level": caution_level,
        "meta": {
            "source_bundle_id": meta.get("source_bundle_id"),
            "source_bundle_status": meta.get("source_bundle_status"),
            "build_mode": build_mode,
            "symbol": meta.get("symbol"),
        },
        "policy": {
            "profile_id": resolved_policy.profile_id,
            "required_families": list(resolved_policy.required_families),
            "optional_families": list(resolved_policy.optional_families),
            "allowed_build_modes": list(resolved_policy.allowed_build_modes),
            "reject_replay_only_required": resolved_policy.reject_replay_only_required,
        },
        "reason_counts": {
            "reject": len(reject_reasons),
            "warn": len(warning_reasons),
        },
        "reasons": reasons,
    }
