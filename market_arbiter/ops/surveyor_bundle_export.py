from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Mapping

from market_arbiter.core.db import init_db
from market_arbiter.core.surveyor_bundle_profile import (
    BundleProfileSelectionError,
    select_surveyor_bundle_profile,
    serialize_surveyor_bundle_profile,
)
from market_arbiter.core.surveyor_profile_acceptance import evaluate_surveyor_profile_acceptance
from market_arbiter.core.surveyor_snapshot import build_surveyor_packet_snapshot


DEFAULT_OUTPUT_DIR = "artifacts/surveyor_bundle_profiles"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"JSON input does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON input at {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON input must be an object: {path}")
    return payload


def _write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(content, encoding="utf-8")
    temp.replace(path)


def _default_output_path(*, profile_id: str, source_bundle_id: str | None) -> Path:
    safe_bundle = str(source_bundle_id or "unknown").replace(":", "_").replace("/", "_")
    return Path(DEFAULT_OUTPUT_DIR) / f"{profile_id}.{safe_bundle}.json"


def load_surveyor_bundle_for_export(
    *,
    bundle_path: str | None = None,
    db_path: str | None = None,
    symbol: str | None = None,
    authoritative_view_path: str | None = None,
    ladders_path: str | None = None,
    allow_replay_fallback: bool = True,
) -> dict[str, Any]:
    """Load a unified Surveyor bundle from a fixture/export file or live DB snapshot."""

    if bundle_path:
        payload = _read_json(Path(bundle_path))
        bundle = payload.get("bundle") if isinstance(payload.get("bundle"), Mapping) else payload
        if not isinstance(bundle, dict):
            raise ValueError(f"bundle input does not contain a bundle object: {bundle_path}")
        return bundle

    if not db_path:
        raise ValueError("either --bundle-path or --db-path is required")
    if not symbol:
        raise ValueError("--symbol is required when exporting from --db-path")

    authoritative_view = _read_json(Path(authoritative_view_path)) if authoritative_view_path else None
    ladders = _read_json(Path(ladders_path)) if ladders_path else None
    conn: sqlite3.Connection | None = None
    try:
        conn = init_db(db_path)
        packet = build_surveyor_packet_snapshot(
            conn,
            symbol=symbol,
            authoritative_view=authoritative_view,
            ladders=ladders,
            allow_replay_fallback=allow_replay_fallback,
        )
    finally:
        if conn is not None:
            conn.close()

    bundle = packet.get("bundle")
    if not isinstance(bundle, dict):
        raise ValueError("snapshot did not produce a Surveyor bundle")
    return bundle


def export_surveyor_bundle_profile(
    *,
    profile_id: str,
    bundle_path: str | None = None,
    db_path: str | None = None,
    symbol: str | None = None,
    authoritative_view_path: str | None = None,
    ladders_path: str | None = None,
    allow_replay_fallback: bool = True,
    strict: bool = True,
    include_acceptance: bool = False,
    output_path: str | None = None,
) -> dict[str, Any]:
    """Select and optionally write a profile payload for downstream consumers."""

    bundle = load_surveyor_bundle_for_export(
        bundle_path=bundle_path,
        db_path=db_path,
        symbol=symbol,
        authoritative_view_path=authoritative_view_path,
        ladders_path=ladders_path,
        allow_replay_fallback=allow_replay_fallback,
    )
    payload = select_surveyor_bundle_profile(bundle, profile_id=profile_id, strict=strict)
    if include_acceptance:
        payload["acceptance"] = evaluate_surveyor_profile_acceptance(payload)
    destination = Path(output_path) if output_path else _default_output_path(
        profile_id=profile_id,
        source_bundle_id=(payload.get("meta") or {}).get("source_bundle_id"),
    )
    _write_text_atomic(destination, serialize_surveyor_bundle_profile(payload))
    payload["export"] = {"output_path": str(destination)}
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export a profile-selected Surveyor bundle payload.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--bundle-path", help="Path to a unified bundle JSON file, or a packet JSON with a top-level bundle field.")
    source.add_argument("--db-path", help="SQLite DB path to build a fresh Surveyor packet snapshot from.")
    parser.add_argument("--symbol", help="Symbol to snapshot when using --db-path, e.g. BTCUSDT.")
    parser.add_argument("--profile", required=True, help="Delivery profile to export, e.g. ui_full, arbiter_core, backtest_core.")
    parser.add_argument("--output-path", help="Destination JSON path. Defaults under artifacts/surveyor_bundle_profiles/.")
    parser.add_argument("--authoritative-view-path", help="Optional authoritative levels view JSON for DB snapshot export.")
    parser.add_argument("--ladders-path", help="Optional ladder JSON for DB snapshot export.")
    parser.add_argument("--no-replay-fallback", action="store_true", help="Disable replay fallback when building from DB.")
    parser.add_argument("--allow-invalid", action="store_true", help="Emit diagnostic payload instead of failing on missing required families.")
    parser.add_argument("--include-acceptance", action="store_true", help="Embed a consumer acceptance verdict in the exported payload.")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    try:
        payload = export_surveyor_bundle_profile(
            profile_id=args.profile,
            bundle_path=args.bundle_path,
            db_path=args.db_path,
            symbol=args.symbol,
            authoritative_view_path=args.authoritative_view_path,
            ladders_path=args.ladders_path,
            allow_replay_fallback=not args.no_replay_fallback,
            strict=not args.allow_invalid,
            include_acceptance=args.include_acceptance,
            output_path=args.output_path,
        )
    except (BundleProfileSelectionError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2, sort_keys=True), file=sys.stderr)
        raise SystemExit(2) from exc

    print(
        json.dumps(
            {
                "ok": True,
                "profile_id": payload.get("profile_id"),
                "profile_status": payload.get("profile_status"),
                "included_families": (payload.get("selection") or {}).get("included_families"),
                "missing_required_families": (payload.get("selection") or {}).get("missing_required_families"),
                "acceptance_status": ((payload.get("acceptance") or {}).get("acceptance_status") if payload.get("acceptance") else None),
                "decision_mode": ((payload.get("acceptance") or {}).get("decision_mode") if payload.get("acceptance") else None),
                "output_path": (payload.get("export") or {}).get("output_path"),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
