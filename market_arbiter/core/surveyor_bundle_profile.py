from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Mapping


SURVEYOR_BUNDLE_PROFILE_PAYLOAD_CONTRACT = "surveyor_bundle_profile_payload_v1"

DEFAULT_REQUIRED_FAMILIES: dict[str, list[str]] = {
    "ui_full": [],
    "arbiter_core": ["feed_state", "structure_state", "sr_zones"],
    "backtest_core": ["feed_state", "structure_state", "sr_zones", "interaction_lifecycle"],
}


@dataclass(frozen=True)
class BundleProfileSelectionError(ValueError):
    """Raised when a bundle profile cannot be selected safely."""

    profile_id: str
    missing_required_families: list[str]
    message: str

    def __str__(self) -> str:
        return self.message


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _profile_order(bundle: Mapping[str, Any], profile_id: str) -> list[str]:
    profiles = _as_mapping(bundle.get("delivery_profiles"))
    raw_order = profiles.get(profile_id)
    if not isinstance(raw_order, list):
        available = ", ".join(sorted(str(key) for key in profiles)) or "none"
        raise BundleProfileSelectionError(
            profile_id=profile_id,
            missing_required_families=[],
            message=f"unknown Surveyor bundle delivery profile: {profile_id}; available profiles: {available}",
        )
    return [str(family) for family in raw_order]


def _required_families(profile_id: str, required_families: list[str] | tuple[str, ...] | None) -> list[str]:
    if required_families is not None:
        return [str(family) for family in required_families]
    return list(DEFAULT_REQUIRED_FAMILIES.get(profile_id, []))


def select_surveyor_bundle_profile(
    bundle: Mapping[str, Any],
    *,
    profile_id: str,
    required_families: list[str] | tuple[str, ...] | None = None,
    strict: bool = True,
) -> dict[str, Any]:
    """Return a deterministic profile-selected Surveyor bundle payload.

    The selected payload preserves dataset-family envelopes exactly as emitted by
    the unified bundle. It only filters which families are included and records
    profile-selection metadata around that unmodified family content.
    """

    datasets = _as_mapping(bundle.get("datasets"))
    requested_families = _profile_order(bundle, profile_id)
    required = _required_families(profile_id, required_families)
    included_families = [family for family in requested_families if family in datasets]
    missing_profile_families = [family for family in requested_families if family not in datasets]
    missing_required_families = [family for family in required if family not in datasets]

    if strict and missing_required_families:
        raise BundleProfileSelectionError(
            profile_id=profile_id,
            missing_required_families=missing_required_families,
            message=(
                f"Surveyor bundle profile {profile_id} missing required families: "
                + ", ".join(missing_required_families)
            ),
        )

    selected_datasets = {family: deepcopy(datasets[family]) for family in included_families}
    meta = _as_mapping(bundle.get("meta"))
    diagnostics = _as_mapping(bundle.get("diagnostics"))
    family_contract_versions = {
        family: selected_datasets[family].get("contract_version")
        for family in included_families
        if isinstance(selected_datasets[family], Mapping)
    }

    profile_errors = []
    if missing_required_families:
        profile_errors.append(
            {
                "issue_kind": "missing_required_families",
                "profile_id": profile_id,
                "families": list(missing_required_families),
            }
        )

    if missing_required_families:
        profile_status = "invalid"
    elif missing_profile_families:
        profile_status = "partial"
    else:
        profile_status = "complete"

    return {
        "contract": SURVEYOR_BUNDLE_PROFILE_PAYLOAD_CONTRACT,
        "profile_id": profile_id,
        "profile_status": profile_status,
        "meta": {
            "source_bundle_contract": meta.get("bundle_contract"),
            "source_bundle_id": meta.get("bundle_id"),
            "source_bundle_status": meta.get("bundle_status"),
            "symbol": meta.get("symbol"),
            "as_of_ts": meta.get("as_of_ts"),
            "build_mode": meta.get("build_mode"),
            "primary_feed_provider": meta.get("primary_feed_provider"),
            "continuity_state": meta.get("continuity_state"),
        },
        "selection": {
            "requested_families": list(requested_families),
            "required_families": list(required),
            "included_families": list(included_families),
            "missing_profile_families": list(missing_profile_families),
            "missing_required_families": list(missing_required_families),
            "family_contract_versions": family_contract_versions,
        },
        "datasets": selected_datasets,
        "diagnostics": {
            "source_issue_count": int(diagnostics.get("issue_count") or 0),
            "source_issues": deepcopy(list(diagnostics.get("issues") or [])),
            "profile_errors": profile_errors,
        },
    }


def serialize_surveyor_bundle_profile(payload: Mapping[str, Any]) -> str:
    """Serialize a selected profile payload deterministically for fixtures/export."""

    return json.dumps(payload, indent=2, sort_keys=True) + "\n"
