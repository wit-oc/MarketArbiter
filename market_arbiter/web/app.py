from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import streamlit as st
except Exception:  # pragma: no cover
    st = None

from market_arbiter.core.db import init_db
from market_arbiter.core.surveyor_snapshot import build_surveyor_packet_snapshot


def _parse_json(raw: str) -> dict[str, Any] | None:
    raw = raw.strip()
    if not raw:
        return None
    return json.loads(raw)


def main() -> None:
    if st is None:  # pragma: no cover
        raise RuntimeError("streamlit is required to run the MarketArbiter UI")

    st.set_page_config(page_title="MarketArbiter Surveyor UI", layout="wide")
    st.title("MarketArbiter · Surveyor Packet Inspector")
    st.caption("Thin extracted UI for canonical feed state and Surveyor packet inspection.")

    db_path = st.text_input("SQLite DB path", value="data/market_arbiter.sqlite")
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

        st.subheader("Packet meta")
        st.json(packet.get("meta", {}))
        st.subheader("Market data")
        st.json(packet.get("market_data", {}))
        st.subheader("Surveyor packet")
        st.json(packet)


if __name__ == "__main__":  # pragma: no cover
    main()
