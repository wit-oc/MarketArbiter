from __future__ import annotations

import json
from typing import Any

try:
    import streamlit as st
except Exception:  # pragma: no cover
    st = None

from market_arbiter.core.db import init_db
from market_arbiter.core.surveyor_snapshot import build_surveyor_packet_snapshot
from market_arbiter.ops.blofin_ws_candle5m_consumer import BlofinWsCandle5mConsumerConfig, collect_status as collect_blofin_ws_status


_STATUS_TONE = {
    "complete": "success",
    "partial": "warning",
    "stale": "error",
    "degraded": "error",
    "unavailable": "error",
    "replay_only": "info",
}


def _status_tone(status: Any) -> str:
    return _STATUS_TONE.get(str(status or "").lower(), "info")


def _profile_family_order(bundle: dict[str, Any], profile: str = "ui_full") -> list[str]:
    datasets = bundle.get("datasets") if isinstance(bundle.get("datasets"), dict) else {}
    delivery_profiles = bundle.get("delivery_profiles") if isinstance(bundle.get("delivery_profiles"), dict) else {}
    profile_order = delivery_profiles.get(profile) if isinstance(delivery_profiles.get(profile), list) else []
    ordered = [family for family in profile_order if family in datasets]
    ordered.extend(family for family in datasets if family not in ordered)
    return ordered


def _issue_preview(issues: Any, *, limit: int = 4) -> list[str]:
    if not isinstance(issues, list):
        return []
    preview = []
    for issue in issues[:limit]:
        if not isinstance(issue, dict):
            continue
        parts = [str(issue.get("issue_kind") or "issue")]
        if issue.get("timeframe"):
            parts.append(str(issue["timeframe"]))
        if issue.get("reason"):
            parts.append(str(issue["reason"]))
        preview.append(" · ".join(parts))
    return preview


def _parse_json(raw: str) -> dict[str, Any] | None:
    raw = raw.strip()
    if not raw:
        return None
    return json.loads(raw)


def _compact_ms(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        seconds = int(value) / 1000.0
    except (TypeError, ValueError):
        return "n/a"
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{seconds / 60:.1f}m"


def _render_status_banner(label: str, status: Any, *, issue_count: int = 0) -> None:
    status_text = str(status or "unknown")
    message = f"{label}: `{status_text}`"
    if issue_count:
        message += f" · {issue_count} issue(s)"
    tone = _status_tone(status_text)
    if tone == "success":
        st.success(message)
    elif tone == "warning":
        st.warning(message)
    elif tone == "error":
        st.error(message)
    else:
        st.info(message)


def _render_bundle_overview(bundle: dict[str, Any]) -> None:
    meta = bundle.get("meta") if isinstance(bundle.get("meta"), dict) else {}
    diagnostics = bundle.get("diagnostics") if isinstance(bundle.get("diagnostics"), dict) else {}
    coverage = bundle.get("coverage") if isinstance(bundle.get("coverage"), dict) else {}

    st.subheader("Unified Surveyor bundle")
    _render_status_banner("Bundle", meta.get("bundle_status"), issue_count=int(diagnostics.get("issue_count") or 0))

    cols = st.columns(5)
    cols[0].metric("Symbol", meta.get("symbol") or "n/a")
    cols[1].metric("Contract", meta.get("bundle_contract") or "n/a")
    cols[2].metric("Build mode", meta.get("build_mode") or "n/a")
    cols[3].metric("Continuity", meta.get("continuity_state") or "unknown")
    cols[4].metric("Provider", meta.get("primary_feed_provider") or "n/a")

    coverage_cols = st.columns(4)
    coverage_cols[0].metric("Required TFs", len(coverage.get("required_timeframes") or []))
    coverage_cols[1].metric("Available TFs", len(coverage.get("available_timeframes") or []))
    coverage_cols[2].metric("Missing TFs", len(coverage.get("missing_timeframes") or []))
    freshness = coverage.get("freshness_summary") if isinstance(coverage.get("freshness_summary"), dict) else {}
    coverage_cols[3].metric("Fresh TFs", len(freshness.get("fresh") or []))

    with st.expander("Bundle meta / coverage", expanded=False):
        st.json({"meta": meta, "coverage": coverage, "delivery_profiles": bundle.get("delivery_profiles", {})})


def _render_family_card(family_name: str, family: dict[str, Any]) -> None:
    summary = family.get("summary") if isinstance(family.get("summary"), dict) else {}
    issues = family.get("issues") if isinstance(family.get("issues"), list) else []
    status = family.get("status") or "unknown"

    st.markdown(f"### `{family_name}`")
    _render_status_banner(family_name, status, issue_count=int(summary.get("issue_count") or len(issues)))

    metric_cols = st.columns(4)
    metric_cols[0].metric("Contract", family.get("contract_version") or "n/a")
    metric_cols[1].metric("Status", status)
    metric_cols[2].metric("Issues", int(summary.get("issue_count") or len(issues)))
    metric_cols[3].metric("Timeframe entries", len(family.get("timeframes") or {}))

    previews = _issue_preview(issues)
    if previews:
        st.caption(" · ".join(previews))

    tab_summary, tab_timeframes, tab_payload, tab_issues = st.tabs(["Summary", "Timeframes", "Payload", "Issues"])
    with tab_summary:
        st.json({"summary": summary, "provenance": family.get("provenance", {})})
    with tab_timeframes:
        st.json(family.get("timeframes", {}))
    with tab_payload:
        st.json(family.get("payload", {}))
    with tab_issues:
        st.json(issues)


def _render_bundle_families(bundle: dict[str, Any], *, profile: str = "ui_full") -> None:
    datasets = bundle.get("datasets") if isinstance(bundle.get("datasets"), dict) else {}
    if not datasets:
        st.warning("No bundle datasets are available in this packet.")
        return

    st.subheader(f"Dataset families · `{profile}`")
    family_order = _profile_family_order(bundle, profile=profile)
    family_tabs = st.tabs(family_order)
    for tab, family_name in zip(family_tabs, family_order):
        with tab:
            family = datasets.get(family_name) or {}
            _render_family_card(family_name, family)


def main() -> None:
    if st is None:  # pragma: no cover
        raise RuntimeError("streamlit is required to run the MarketArbiter UI")

    st.set_page_config(page_title="MarketArbiter Surveyor UI", layout="wide")
    st.title("MarketArbiter · Surveyor Packet Inspector")
    st.caption("Thin extracted UI for canonical feed state and Surveyor packet inspection.")

    db_path = st.text_input("SQLite DB path", value="data/market_arbiter.sqlite")
    blofin_symbols = st.text_input("BloFin live 5m symbols", value="BTC-USDT")
    blofin_state_path = st.text_input("BloFin status state path (optional)", value="")

    blofin_config = BlofinWsCandle5mConsumerConfig(
        db_path=db_path,
        symbols=[item.strip().upper() for item in blofin_symbols.split(",") if item.strip()],
        environment="demo",
        state_path=blofin_state_path.strip() or None,
    )
    blofin_status = collect_blofin_ws_status(blofin_config)

    st.subheader("BloFin live 5m feed")
    source_label = {
        "live_state": "live state",
        "stale_state": "stale state",
        "no_state": "no state",
    }.get(blofin_status["status_source"], blofin_status["status_source"])
    status_cols = st.columns(5)
    status_cols[0].metric("State", blofin_status["feed_state"])
    status_cols[1].metric("Source", source_label)
    status_cols[2].metric("Age", _compact_ms(blofin_status["state_age_ms"]))
    status_cols[3].metric("Reconnects", blofin_status["reconnect_attempts"])
    status_cols[4].metric("Backoff", _compact_ms((blofin_status["latest_backoff_seconds"] or 0) * 1000 if blofin_status["latest_backoff_seconds"] is not None else None))
    st.caption(
        "BloFin status is read from the live consumer state file and `feed_checkpoints`; "
        f"state file: {blofin_status['state_path']}"
    )
    with st.expander("BloFin feed status details", expanded=blofin_status["status_source"] != "live_state"):
        st.json(
            {
                "status_source": blofin_status["status_source"],
                "latest_disconnect_reason": blofin_status["latest_disconnect_reason"],
                "last_successful_ingest_ms": blofin_status["last_successful_ingest_ms"],
                "last_recovery_ms": blofin_status["last_recovery_ms"],
                "summary": blofin_status["summary"],
                "symbol_statuses": blofin_status["symbol_statuses"],
            }
        )

    symbol = st.text_input("Symbol", value="BTCUSDT")
    authoritative_raw = st.text_area("Authoritative view JSON (optional)", value="", height=180)
    ladders_raw = st.text_area("Ladders JSON (optional)", value="", height=140)
    allow_replay = st.checkbox("Allow replay fallback", value=True)

    if st.button("Build Surveyor packet"):
        conn = init_db(db_path)
        try:
            packet = build_surveyor_packet_snapshot(
                conn,
                symbol=symbol,
                authoritative_view=_parse_json(authoritative_raw),
                ladders=_parse_json(ladders_raw),
                allow_replay_fallback=allow_replay,
            )
        finally:
            conn.close()

        bundle = packet.get("bundle") if isinstance(packet.get("bundle"), dict) else {}
        _render_bundle_overview(bundle)
        _render_bundle_families(bundle, profile="ui_full")

        with st.expander("Legacy packet compatibility view", expanded=False):
            st.caption("Compatibility view only; the operator UI above is driven by the unified bundle datasets.")
            st.json(
                {
                    "meta": packet.get("meta", {}),
                    "market_data": packet.get("market_data", {}),
                    "diagnostics": packet.get("diagnostics", {}),
                }
            )
        with st.expander("Full raw Surveyor packet", expanded=False):
            st.json(packet)


if __name__ == "__main__":  # pragma: no cover
    main()
