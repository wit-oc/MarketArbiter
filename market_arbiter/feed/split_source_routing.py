"""Split-source routing contracts for Surveyor feed rollout writes.

The accepted top-100-readiness posture intentionally separates live and
history/repair roles: Bybit may be the live WebSocket primary while OKX owns
REST history/repair from this host.  This module keeps that role split explicit
in runner manifests and in persisted candle route tags.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Mapping, Sequence

from market_arbiter.core.market_data import CandleDTO, MarketDataProvider, ProviderHealth
from market_arbiter.feed.provider_policy import ProviderRolePolicy


SPLIT_SOURCE_ROUTING_CONTRACT = "surveyor_feed_split_source_routing_v1"


@dataclass(frozen=True)
class FeedSourceRoute:
    route_id: str
    role: str
    provider_id: str
    venue: str
    transport: str
    request_class: str
    write_provider_id: str
    dataset_prefix: str
    write_enabled: bool = True

    @classmethod
    def from_payload(cls, route_id: str, payload: Mapping[str, Any]) -> "FeedSourceRoute":
        provider_id = str(payload.get("provider_id") or payload.get("provider") or "").strip().lower()
        venue = str(payload.get("venue") or provider_id).strip().lower()
        role = str(payload.get("role") or route_id).strip().lower()
        request_class = str(payload.get("request_class") or _request_class_for_role(role)).strip().lower()
        write_provider_id = str(payload.get("write_provider_id") or f"{provider_id}_{payload.get('transport') or 'route'}").strip().lower()
        dataset_prefix = str(payload.get("dataset_prefix") or f"{write_provider_id}_{request_class}").strip()
        return cls(
            route_id=str(route_id).strip(),
            role=role,
            provider_id=provider_id,
            venue=venue,
            transport=str(payload.get("transport") or "unknown").strip().lower(),
            request_class=request_class,
            write_provider_id=write_provider_id,
            dataset_prefix=dataset_prefix,
            write_enabled=bool(payload.get("write_enabled", True)),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "route_id": self.route_id,
            "role": self.role,
            "provider_id": self.provider_id,
            "venue": self.venue,
            "transport": self.transport,
            "request_class": self.request_class,
            "write_provider_id": self.write_provider_id,
            "dataset_prefix": self.dataset_prefix,
            "write_enabled": self.write_enabled,
        }


def _request_class_for_role(role: str) -> str:
    normalized = str(role or "").strip().lower()
    if normalized in {"live_ws", "live_websocket", "live_ws_primary", "live_ws_shadow", "live_ws_shadow_fallback"}:
        return "live_ws"
    if normalized in {"repair", "gap_repair", "rest_repair"}:
        return "gap_repair"
    return "rest_history"


def default_split_source_routes() -> dict[str, dict[str, Any]]:
    return {
        "live_ws_primary": {
            "role": "live_ws_primary",
            "provider_id": "bybit",
            "venue": "bybit",
            "transport": "websocket",
            "request_class": "live_ws",
            "write_provider_id": "bybit_ws",
            "dataset_prefix": "bybit_live_ws",
            "write_enabled": True,
        },
        "rest_history": {
            "role": "rest_history",
            "provider_id": "okx",
            "venue": "okx",
            "transport": "rest",
            "request_class": "rest_history",
            "write_provider_id": "okx_rest",
            "dataset_prefix": "okx_rest_history",
            "write_enabled": True,
        },
        "gap_repair": {
            "role": "gap_repair",
            "provider_id": "okx",
            "venue": "okx",
            "transport": "rest",
            "request_class": "gap_repair",
            "write_provider_id": "okx_rest_repair",
            "dataset_prefix": "okx_gap_repair",
            "write_enabled": True,
        },
        "live_ws_shadow_fallback": {
            "role": "live_ws_shadow_fallback",
            "provider_id": "okx",
            "venue": "okx",
            "transport": "websocket",
            "request_class": "live_ws",
            "write_provider_id": "okx_ws",
            "dataset_prefix": "okx_shadow_ws",
            "write_enabled": True,
        },
    }


@dataclass(frozen=True)
class SplitSourceRoutingPlan:
    routes: dict[str, FeedSourceRoute]
    validation_decisions: list[dict[str, Any]]

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None = None) -> "SplitSourceRoutingPlan":
        source = dict(payload or {})
        raw_routes = source.get("routes") or source.get("source_routes") or source or default_split_source_routes()
        if not isinstance(raw_routes, Mapping):
            raise ValueError("source routes must be a mapping")
        routes = {
            str(route_id): FeedSourceRoute.from_payload(str(route_id), route_payload)
            for route_id, route_payload in raw_routes.items()
            if isinstance(route_payload, Mapping)
        }
        return cls(routes=routes, validation_decisions=[])

    @classmethod
    def from_provider_policy(cls, policy: ProviderRolePolicy) -> "SplitSourceRoutingPlan":
        plan = cls.from_payload(default_split_source_routes())
        decisions = [
            policy.decide(provider_id="bybit", request_class="live_ws"),
            policy.decide(provider_id="okx", request_class="rest_history"),
            policy.decide(provider_id="okx", request_class="gap_repair"),
            policy.decide(provider_id="okx", request_class="live_ws"),
            policy.decide(provider_id="bybit", request_class="rest_history"),
            policy.decide(provider_id="bybit", request_class="gap_repair"),
        ]
        return cls(routes=plan.routes, validation_decisions=[decision.as_dict() for decision in decisions])

    def route(self, route_id: str) -> FeedSourceRoute:
        try:
            return self.routes[route_id]
        except KeyError as exc:
            raise ValueError(f"missing split-source route: {route_id}") from exc

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract": SPLIT_SOURCE_ROUTING_CONTRACT,
            "routes": {route_id: route.as_dict() for route_id, route in sorted(self.routes.items())},
            "validation_decisions": list(self.validation_decisions),
            "safety_assertions": {
                "bybit_live_ws_primary": self.route("live_ws_primary").provider_id == "bybit",
                "okx_rest_history_primary": self.route("rest_history").provider_id == "okx",
                "okx_gap_repair_primary": self.route("gap_repair").provider_id == "okx",
                "okx_ws_shadow_fallback": self.route("live_ws_shadow_fallback").provider_id == "okx",
                "bybit_rest_history_not_configured": self.route("rest_history").provider_id != "bybit",
                "bybit_gap_repair_not_configured": self.route("gap_repair").provider_id != "bybit",
            },
        }


def tag_candles_for_route(candles: Sequence[CandleDTO], route: FeedSourceRoute) -> list[CandleDTO]:
    if not route.write_enabled:
        return []
    tagged: list[CandleDTO] = []
    for candle in candles:
        prior_version = str(candle.dataset_version or "untagged")
        dataset_version = prior_version if prior_version.startswith(f"{route.dataset_prefix}:") else f"{route.dataset_prefix}:{prior_version}"
        tagged.append(
            replace(
                candle,
                provider_id=route.write_provider_id,
                venue=route.venue,
                dataset_version=dataset_version,
            )
        )
    return tagged


class RoutedMarketDataProvider:
    """Provider wrapper that stamps persisted candles with an explicit source route."""

    def __init__(self, provider: MarketDataProvider, route: FeedSourceRoute) -> None:
        self.provider = provider
        self.route = route

    def fetch_ohlcv(self, symbol: str, timeframe: str, since_ms: int | None, limit: int) -> list[CandleDTO]:
        return tag_candles_for_route(
            self.provider.fetch_ohlcv(symbol, timeframe, since_ms, limit),
            self.route,
        )

    def fetch_funding(self, symbol: str, since_ms: int | None, limit: int) -> list[dict]:
        return self.provider.fetch_funding(symbol, since_ms, limit)

    def fetch_open_interest(self, symbol: str, since_ms: int | None, limit: int) -> list[dict]:
        return self.provider.fetch_open_interest(symbol, since_ms, limit)

    def provider_health(self) -> ProviderHealth:
        return self.provider.provider_health()


def validate_cohort_routes(*, symbols: Sequence[str], plan: SplitSourceRoutingPlan, policy: ProviderRolePolicy) -> dict[str, Any]:
    decisions = [
        policy.decide(provider_id=plan.route("live_ws_primary").provider_id, request_class=plan.route("live_ws_primary").request_class),
        policy.decide(provider_id=plan.route("rest_history").provider_id, request_class=plan.route("rest_history").request_class),
        policy.decide(provider_id=plan.route("gap_repair").provider_id, request_class=plan.route("gap_repair").request_class),
        policy.decide(provider_id=plan.route("live_ws_shadow_fallback").provider_id, request_class=plan.route("live_ws_shadow_fallback").request_class),
        policy.decide(provider_id="bybit", request_class="rest_history"),
        policy.decide(provider_id="bybit", request_class="gap_repair"),
    ]
    allowed_required = all(decision.allowed for decision in decisions[:4])
    bybit_rest_blocked = all(not decision.allowed for decision in decisions[4:])
    return {
        "contract": SPLIT_SOURCE_ROUTING_CONTRACT,
        "symbols": list(symbols),
        "symbol_count": len(list(symbols)),
        "plan": plan.as_dict(),
        "decisions": [decision.as_dict() for decision in decisions],
        "ok": allowed_required and bybit_rest_blocked,
        "checks": {
            "required_routes_allowed": allowed_required,
            "bybit_rest_history_blocked": not decisions[4].allowed,
            "bybit_gap_repair_blocked": not decisions[5].allowed,
            "rest_history_route_provider": plan.route("rest_history").provider_id,
            "gap_repair_route_provider": plan.route("gap_repair").provider_id,
        },
    }
