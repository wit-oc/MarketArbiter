"""Provider-role and IP/rate guardrails for Surveyor feed rollout paths."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import re
from typing import Any, Mapping


PROVIDER_POLICY_CONTRACT = "surveyor_feed_provider_policy_v1"
PROVIDER_GOVERNOR_CONTRACT = "surveyor_feed_provider_governor_v1"
REQUEST_CLASS_TO_ROLE = {
    "live_ws": "live_websocket",
    "live_websocket": "live_websocket",
    "rest_history": "rest_history",
    "history": "rest_history",
    "gap_repair": "repair",
    "repair": "repair",
}
ALLOWED_ROLE_STATES = {"primary_conditional", "shadow_fallback_conditional", "canary_only"}
BLOCKED_ROLE_STATES = {"disabled_from_this_host", "not_canonical", "disabled", "blocked"}
DEFAULT_ROLLOUT_BOUNDS = {
    "live_websocket": {
        "market_arbiter.feed.bakeoff",
        "market_arbiter.ops.feed_bakeoff_phase_b_live",
    },
    "rest_history": {
        "market_arbiter.ops.surveyor_feed_runner",
        "market_arbiter.ops.surveyor_symbol_onboarding",
    },
    "repair": {
        "market_arbiter.ops.surveyor_feed_runner",
        "market_arbiter.ops.surveyor_symbol_onboarding",
    },
}


class ProviderPolicyError(RuntimeError):
    """Base class for local provider-governor policy decisions."""


class ProviderPolicyBlockedError(ProviderPolicyError):
    """Raised before any provider network touch that policy forbids."""


class ProviderCooldownError(ProviderPolicyError):
    """Raised while a provider is in local 429 cooldown."""


class ProviderIpFrozenError(ProviderPolicyError):
    """Raised while a provider/IP path is frozen after a 403-style block."""


@dataclass(frozen=True)
class ProviderDecision:
    provider_id: str
    venue: str
    request_class: str
    role: str
    role_state: str
    allowed: bool
    reason_code: str
    reason: str
    script_id: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "venue": self.venue,
            "request_class": self.request_class,
            "role": self.role,
            "role_state": self.role_state,
            "allowed": self.allowed,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "script_id": self.script_id,
        }


@dataclass(frozen=True)
class ProviderRolePolicy:
    payload: Mapping[str, Any]
    rollout_bounds: Mapping[str, set[str]] = field(default_factory=lambda: {key: set(values) for key, values in DEFAULT_ROLLOUT_BOUNDS.items()})

    @classmethod
    def from_path(cls, path: str | Path) -> "ProviderRolePolicy":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(payload=payload)

    @property
    def contract(self) -> str:
        return str(self.payload.get("contract") or "")

    def _role_state(self, provider_id: str, role: str) -> str:
        providers = self.payload.get("provider_roles") or {}
        provider = providers.get(provider_id) or {}
        return str(provider.get(role) or "missing")

    def decide(
        self,
        *,
        provider_id: str,
        venue: str | None = None,
        request_class: str,
        script_id: str | None = None,
    ) -> ProviderDecision:
        normalized_provider = str(provider_id or "").strip().lower()
        normalized_venue = str(venue or normalized_provider).strip().lower()
        normalized_request = str(request_class or "").strip().lower()
        role = REQUEST_CLASS_TO_ROLE.get(normalized_request, normalized_request)
        role_state = self._role_state(normalized_provider, role)

        if script_id:
            allowed_scripts = self.rollout_bounds.get(role) or set()
            if script_id not in allowed_scripts:
                return ProviderDecision(
                    provider_id=normalized_provider,
                    venue=normalized_venue,
                    request_class=normalized_request,
                    role=role,
                    role_state=role_state,
                    allowed=False,
                    reason_code="PROVIDER_SCRIPT_OUT_OF_ROLLOUT_BOUNDS",
                    reason=f"{script_id} is not approved to touch provider role {role}",
                    script_id=script_id,
                )

        if role_state in ALLOWED_ROLE_STATES:
            return ProviderDecision(
                provider_id=normalized_provider,
                venue=normalized_venue,
                request_class=normalized_request,
                role=role,
                role_state=role_state,
                allowed=True,
                reason_code="PROVIDER_ROLE_ALLOWED",
                reason=f"{normalized_provider}.{role} is {role_state}",
                script_id=script_id,
            )

        reason_code = "PROVIDER_ROLE_BLOCKED" if role_state in BLOCKED_ROLE_STATES else "PROVIDER_ROLE_MISSING"
        return ProviderDecision(
            provider_id=normalized_provider,
            venue=normalized_venue,
            request_class=normalized_request,
            role=role,
            role_state=role_state,
            allowed=False,
            reason_code=reason_code,
            reason=f"{normalized_provider}.{role} is {role_state}",
            script_id=script_id,
        )

    def require_allowed(self, **kwargs: Any) -> ProviderDecision:
        decision = self.decide(**kwargs)
        if not decision.allowed:
            raise ProviderPolicyBlockedError(decision.reason)
        return decision


@dataclass
class ProviderAccessGovernor:
    policy: ProviderRolePolicy
    script_id: str | None = None
    rate_limit_cooldown_ms: int = 60_000
    ip_freeze_ms: int = 3_600_000
    cooldown_until_ms: dict[tuple[str, str], int] = field(default_factory=dict)
    frozen_until_ms: dict[tuple[str, str], int] = field(default_factory=dict)
    decisions: list[dict[str, Any]] = field(default_factory=list)

    def _key(self, provider_id: str, venue: str | None = None) -> tuple[str, str]:
        provider = str(provider_id or "").strip().lower()
        return provider, str(venue or provider).strip().lower()

    def check_request(self, *, provider_id: str, venue: str | None = None, request_class: str, now_ms: int) -> ProviderDecision:
        provider, resolved_venue = self._key(provider_id, venue)
        decision = self.policy.decide(
            provider_id=provider,
            venue=resolved_venue,
            request_class=request_class,
            script_id=self.script_id,
        )
        payload = decision.as_dict() | {"as_of_ms": int(now_ms)}
        self.decisions.append(payload)
        if not decision.allowed:
            raise ProviderPolicyBlockedError(decision.reason)

        frozen_until = self.frozen_until_ms.get((provider, resolved_venue), 0)
        if frozen_until > now_ms:
            raise ProviderIpFrozenError(f"{provider}/{resolved_venue} frozen_until_ms={frozen_until}")

        cooldown_until = self.cooldown_until_ms.get((provider, resolved_venue), 0)
        if cooldown_until > now_ms:
            raise ProviderCooldownError(f"{provider}/{resolved_venue} cooldown_until_ms={cooldown_until}")
        return decision

    def record_http_status(self, *, provider_id: str, venue: str | None = None, status_code: int, now_ms: int) -> str | None:
        provider, resolved_venue = self._key(provider_id, venue)
        status = int(status_code)
        if status == 429:
            self.cooldown_until_ms[(provider, resolved_venue)] = int(now_ms) + int(self.rate_limit_cooldown_ms)
            return "PROVIDER_COOLDOWN"
        if status == 403:
            self.frozen_until_ms[(provider, resolved_venue)] = int(now_ms) + int(self.ip_freeze_ms)
            return "PROVIDER_IP_FROZEN"
        return None

    def record_exception(self, *, provider_id: str, venue: str | None = None, error: BaseException, now_ms: int) -> str | None:
        status = http_status_from_exception(error)
        if status is None:
            return None
        return self.record_http_status(provider_id=provider_id, venue=venue, status_code=status, now_ms=now_ms)

    def snapshot(self) -> dict[str, Any]:
        return {
            "contract": PROVIDER_GOVERNOR_CONTRACT,
            "script_id": self.script_id,
            "cooldown_until_ms": {f"{provider}/{venue}": until for (provider, venue), until in sorted(self.cooldown_until_ms.items())},
            "frozen_until_ms": {f"{provider}/{venue}": until for (provider, venue), until in sorted(self.frozen_until_ms.items())},
            "decisions": list(self.decisions),
        }


def http_status_from_exception(error: BaseException) -> int | None:
    for attr in ("status_code", "http_status", "code"):
        value = getattr(error, attr, None)
        if isinstance(value, int):
            return value
    response = getattr(error, "response", None)
    value = getattr(response, "status_code", None)
    if isinstance(value, int):
        return value
    text = str(error)
    match = re.search(r"\b(403|429|5\d\d)\b", text)
    return int(match.group(1)) if match else None
