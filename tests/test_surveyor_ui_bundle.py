from __future__ import annotations

from market_arbiter.web.app import _issue_preview, _profile_family_order, _status_tone


def test_profile_family_order_uses_ui_full_and_appends_extra_families() -> None:
    bundle = {
        "datasets": {
            "sr_zones": {},
            "feed_state": {},
            "experimental_family": {},
        },
        "delivery_profiles": {
            "ui_full": ["feed_state", "structure_state", "sr_zones"],
        },
    }

    assert _profile_family_order(bundle) == ["feed_state", "sr_zones", "experimental_family"]


def test_status_tone_flags_family_health() -> None:
    assert _status_tone("complete") == "success"
    assert _status_tone("partial") == "warning"
    assert _status_tone("stale") == "error"
    assert _status_tone("degraded") == "error"
    assert _status_tone("replay_only") == "info"
    assert _status_tone("unexpected") == "info"


def test_issue_preview_is_compact_and_bounded() -> None:
    issues = [
        {"issue_kind": "upstream_feed_input", "timeframe": "5m", "reason": "stale"},
        {"issue_kind": "historical_repair_quality", "timeframe": "4H", "reason": "elevated"},
        {"issue_kind": "missing", "reason": "no_sr"},
        {"issue_kind": "extra", "timeframe": "1D"},
        {"issue_kind": "not_shown", "timeframe": "1W"},
    ]

    assert _issue_preview(issues) == [
        "upstream_feed_input · 5m · stale",
        "historical_repair_quality · 4H · elevated",
        "missing · no_sr",
        "extra · 1D",
    ]
