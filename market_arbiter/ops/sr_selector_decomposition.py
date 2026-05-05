"""Decompose the canonical Surveyor daily-major selector into debug artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from market_arbiter.ops.pine_sr_parity_export import (
    DEFAULT_PROVIDER_ID,
    DEFAULT_SYMBOL,
    DEFAULT_TIMEFRAME,
    DEFAULT_VENUE,
    DailyMajorConfig,
    _ensure_liquidsniper_import,
    _jsonable,
    _load_market_candles,
    _parse_utc_date,
    _surveyor_candles,
)


DECOMPOSITION_CONTRACT = "surveyor_sr_selector_decomposition_v1"
DEFAULT_ASOF_DATE = "2026-05-04"
DEFAULT_OUTPUT_JSON = "artifacts/sr_selector_decomposition/btcusdt_1d_20260504_decomposition.json"
DEFAULT_OUTPUT_MD = "artifacts/sr_selector_decomposition/btcusdt_1d_20260504_decomposition.md"
DEFAULT_FOCUS_BANDS = "58000-62000:60k,63000-67000:65k,70000-78000:74k,83000-88000:85k,102000-112000:108k"


@dataclass(frozen=True)
class FocusBand:
    low: float
    high: float
    label: str


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_focus_bands(raw: str) -> list[FocusBand]:
    bands: list[FocusBand] = []
    for item in raw.split(","):
        text = item.strip()
        if not text:
            continue
        range_part, _, label = text.partition(":")
        low_text, _, high_text = range_part.partition("-")
        low = float(low_text.strip())
        high = float(high_text.strip())
        if high < low:
            low, high = high, low
        bands.append(FocusBand(low=low, high=high, label=(label.strip() or range_part.strip())))
    return bands


def _zone_bounds(zone: Mapping[str, Any]) -> tuple[float | None, float | None, float | None]:
    low = _safe_float(zone.get("zone_low"))
    high = _safe_float(zone.get("zone_high"))
    mid = _safe_float(zone.get("zone_mid"))
    if (low is None or high is None or mid is None) and isinstance(zone.get("full_zone_bounds"), Mapping):
        full_bounds = zone["full_zone_bounds"]
        low = low if low is not None else _safe_float(full_bounds.get("low"))
        high = high if high is not None else _safe_float(full_bounds.get("high"))
        mid = mid if mid is not None else _safe_float(full_bounds.get("mid"))
    if low is None and isinstance(zone.get("bounds"), Mapping):
        low = _safe_float(zone["bounds"].get("low"))
    if high is None and isinstance(zone.get("bounds"), Mapping):
        high = _safe_float(zone["bounds"].get("high"))
    if low is not None and high is not None and high < low:
        low, high = high, low
    if mid is None and low is not None and high is not None:
        mid = (low + high) / 2.0
    return low, high, mid


def _core_bounds(zone: Mapping[str, Any]) -> tuple[float | None, float | None, float | None]:
    low = _safe_float(zone.get("core_low"))
    high = _safe_float(zone.get("core_high"))
    mid = _safe_float(zone.get("core_mid"))
    if (low is None or high is None or mid is None) and isinstance(zone.get("operator_core_bounds"), Mapping):
        core_bounds = zone["operator_core_bounds"]
        low = low if low is not None else _safe_float(core_bounds.get("low"))
        high = high if high is not None else _safe_float(core_bounds.get("high"))
        mid = mid if mid is not None else _safe_float(core_bounds.get("mid"))
    if mid is None and low is not None and high is not None:
        mid = (low + high) / 2.0
    return low, high, mid


def _band_overlap(zone: Mapping[str, Any], band: FocusBand) -> bool:
    low, high, mid = _zone_bounds(zone)
    if low is not None and high is not None:
        return not (high < band.low or low > band.high)
    return mid is not None and band.low <= mid <= band.high


def _stage_id_set(zones: Sequence[Mapping[str, Any]]) -> set[str]:
    return {str(zone.get("zone_id") or "") for zone in zones if str(zone.get("zone_id") or "")}


def _best_demoter(zone_id: str, groups: Sequence[Mapping[str, Any]], demoted_key: str, representative_key: str = "zone_id") -> str | None:
    for group in groups:
        demoted = group.get(demoted_key)
        if isinstance(demoted, list) and zone_id in {str(item) for item in demoted}:
            representative_id = str(group.get(representative_key) or "")
            return representative_id or None
    return None


def _family_summary(zone: Mapping[str, Any]) -> dict[str, Any]:
    candidate_families = [str(item) for item in (zone.get("candidate_families") or [])]
    return {
        "candidate_family": zone.get("candidate_family"),
        "candidate_families": candidate_families,
        "candidate_sources": [str(item) for item in (zone.get("candidate_sources") or [])],
        "source_family": zone.get("source_family"),
        "source_family_primary": zone.get("source_family_primary"),
        "source_family_display": zone.get("source_family_display"),
        "merge_family_count": zone.get("merge_family_count"),
        "family_confluence_bonus": zone.get("family_confluence_bonus"),
        "provenance_summary": zone.get("provenance_summary"),
    }


def _candidate_export(
    zone: Mapping[str, Any],
    *,
    selected_ids: set[str],
    confirmed_ids: set[str],
    prefilter_ids: set[str],
    band_ids: set[str],
    collapsed_ids: set[str],
    diverse_ids: set[str],
    pocket_selected_ids: set[str],
    final_selected_ids: set[str],
    band_groups: Sequence[Mapping[str, Any]],
    pocket_groups: Sequence[Mapping[str, Any]],
    cfg: DailyMajorConfig,
) -> dict[str, Any]:
    zone_id = str(zone.get("zone_id") or "")
    low, high, mid = _zone_bounds(zone)
    core_low, core_high, core_mid = _core_bounds(zone)
    strength = _safe_float(zone.get("strength_score"))
    selection_score = _safe_float(zone.get("selection_score"))
    meaningful_touches = int(zone.get("meaningful_touch_count") or 0)

    debug_bucket = "selected"
    reason = str(zone.get("selector_reason") or "selected by canonical daily-major selector")
    if zone_id not in selected_ids:
        if str(zone.get("status") or "") != "confirmed":
            debug_bucket = "not_confirmed"
            reason = (
                f"rejected before daily-major selector: status={zone.get('status') or 'unknown'} "
                f"with {meaningful_touches} meaningful touches; minimum configured touches={cfg.daily_min_meaningful_touches}"
            )
        elif zone_id not in prefilter_ids:
            debug_bucket = "below_min_strength"
            reason = (
                f"rejected by min-strength prefilter: strength_score={strength or 0.0:.2f} "
                f"< {cfg.daily_min_strength:.2f}"
            )
        elif zone_id not in band_ids:
            debug_bucket = "local_band_demoted"
            reason = "rejected in local-band representative stage: stronger nearby candidate(s) won the band"
        elif zone_id not in collapsed_ids:
            demoter = _best_demoter(zone_id, band_groups, "local_cluster_demoted_ids")
            debug_bucket = "distance_collapse_demoted"
            if demoter:
                reason = f"rejected by distance collapse: too close to stronger representative `{demoter}`"
            else:
                reason = "rejected by distance collapse: too close to a stronger representative"
        elif zone_id not in diverse_ids:
            debug_bucket = "spatial_diversity_cut"
            reason = "rejected by spatial diversity stage: not chosen among the final spread of max zones"
        elif zone_id not in pocket_selected_ids:
            demoter = _best_demoter(zone_id, pocket_groups, "daily_pocket_demoted_ids")
            debug_bucket = "pocket_consolidated"
            if demoter:
                reason = f"rejected by pocket consolidation: grouped under stronger pocket representative `{demoter}`"
            else:
                reason = "rejected by pocket consolidation: grouped under a stronger pocket representative"
        else:
            debug_bucket = "coverage_replaced_or_unranked"
            reason = "survived pocketing but was displaced or outranked during current-regime coverage handling"

    return _jsonable(
        {
            "zone_id": zone.get("zone_id"),
            "status": zone.get("status"),
            "zone_kind": zone.get("zone_kind"),
            "origin_kind": zone.get("origin_kind"),
            "current_role": zone.get("current_role"),
            "relative_position": zone.get("relative_position"),
            "lifecycle_state": zone.get("lifecycle_state"),
            "first_touch_state": zone.get("first_touch_state"),
            "first_retest_pending": zone.get("first_retest_pending"),
            "first_retest_ts": zone.get("first_retest_ts"),
            "first_retest_result": zone.get("first_retest_result"),
            "meaningful_touch_count": meaningful_touches,
            "touch_count": zone.get("touch_count"),
            "full_zone_bounds": {
                "low": low,
                "high": high,
                "mid": mid,
                "width_bps": zone.get("zone_width_bps"),
                "width_atr": zone.get("zone_width_atr"),
            },
            "operator_core_bounds": {
                "low": core_low,
                "high": core_high,
                "mid": core_mid,
                "definition": zone.get("core_definition"),
                "display_bounds_kind": zone.get("display_bounds_kind"),
            },
            "scores": {
                "strength_score": strength,
                "selection_score": selection_score,
                "reaction_score": zone.get("reaction_score"),
                "reaction_efficiency_score": zone.get("reaction_efficiency_score"),
                "carry_score": zone.get("carry_score"),
                "body_respect_score": zone.get("body_respect_score"),
                "retest_weight": zone.get("retest_weight"),
                "daily_major_provenance_weight": zone.get("daily_major_provenance_weight"),
                "family_confluence_bonus": zone.get("family_confluence_bonus"),
            },
            "families": _family_summary(zone),
            "selector": {
                "selector_surface": zone.get("selector_surface"),
                "selector_status": zone.get("selector_status") if zone_id in final_selected_ids else None,
                "selector_rank": zone.get("selector_rank") if zone_id in final_selected_ids else None,
                "selector_reason": zone.get("selector_reason") if zone_id in final_selected_ids else None,
            },
            "stage_membership": {
                "confirmed": zone_id in confirmed_ids,
                "prefilter_min_strength": zone_id in prefilter_ids,
                "local_band_representative": zone_id in band_ids,
                "distance_collapse": zone_id in collapsed_ids,
                "spatial_diversity": zone_id in diverse_ids,
                "pocket_selected": zone_id in pocket_selected_ids,
                "final_selected": zone_id in final_selected_ids,
            },
            "selected_or_rejected_reason": reason,
            "debug_bucket": debug_bucket,
            "daily_major_diagnostics": zone.get("daily_major_diagnostics"),
            "daily_pocket": {
                "contract": zone.get("daily_pocket_contract"),
                "id": zone.get("daily_pocket_id"),
                "reason": zone.get("daily_pocket_reason"),
                "member_count": zone.get("daily_pocket_member_count"),
                "member_ids": zone.get("daily_pocket_member_ids"),
                "demoted_ids": zone.get("daily_pocket_demoted_ids"),
            },
            "local_cluster": {
                "id": zone.get("local_cluster_id"),
                "role": zone.get("local_cluster_role"),
                "member_count": zone.get("local_cluster_member_count"),
                "member_ids": zone.get("local_cluster_member_ids"),
                "demoted_ids": zone.get("local_cluster_demoted_ids"),
                "bounds": zone.get("local_cluster_bounds"),
            },
            "arbitration_diagnostics": zone.get("arbitration_diagnostics"),
        }
    )


def _short_zone_row(zone: Mapping[str, Any]) -> dict[str, Any]:
    low, high, mid = _zone_bounds(zone)
    core_low, core_high, core_mid = _core_bounds(zone)
    return _jsonable(
        {
            "zone_id": zone.get("zone_id"),
            "candidate_family": zone.get("candidate_family"),
            "candidate_families": zone.get("candidate_families"),
            "zone_kind": zone.get("zone_kind"),
            "status": zone.get("status"),
            "mid": mid,
            "full_low": low,
            "full_high": high,
            "core_low": core_low,
            "core_high": core_high,
            "core_mid": core_mid,
            "meaningful_touch_count": zone.get("meaningful_touch_count"),
            "strength_score": zone.get("strength_score"),
            "selection_score": zone.get("selection_score"),
            "selector_rank": zone.get("selector_rank"),
            "selector_status": zone.get("selector_status"),
            "selector_reason": zone.get("selector_reason"),
            "selected_or_rejected_reason": zone.get("selected_or_rejected_reason"),
            "debug_bucket": zone.get("debug_bucket"),
        }
    )


def _build_decomposition(
    *,
    symbol: str,
    candles: Sequence[Mapping[str, Any]],
    cfg: DailyMajorConfig,
) -> dict[str, Any]:
    from liquidsniper.core.zone_engine_v3 import (  # type: ignore
        build_base_candidates,
        build_reaction_candidates,
        build_structure_candidates,
        merge_candidate_zones,
        score_zone,
    )
    from liquidsniper.core.zone_selectors import (  # type: ignore
        _apply_daily_current_regime_coverage,
        _apply_daily_operator_core,
        _consolidate_daily_selected_pockets,
        _daily_pocket_rank_key,
        apply_daily_soft_retest_weights,
        collapse_zones_by_distance,
        select_daily_local_band_representatives,
        select_daily_majors,
        select_spatially_diverse_zones,
    )

    surveyor_candles = _surveyor_candles(candles)
    kwargs = {
        "cluster_eps": cfg.daily_cluster_eps,
        "reaction_atr_min": cfg.daily_reaction_atr_min,
        "min_meaningful_touches": cfg.daily_min_meaningful_touches,
    }
    structure = build_structure_candidates(symbol, "1D", surveyor_candles, **kwargs)
    base = build_base_candidates(symbol, "1D", surveyor_candles, **kwargs)
    reaction = build_reaction_candidates(symbol, "1D", surveyor_candles, **kwargs)
    merged = merge_candidate_zones(structure, base, reaction)
    last_price = float(candles[-1]["close"])
    scored = [score_zone(zone, last_price=last_price) for zone in merged]

    confirmed = [zone for zone in scored if zone.get("status") == "confirmed"]
    weighted = apply_daily_soft_retest_weights(confirmed, strict_mode=cfg.daily_require_first_retest_quality)
    prefilter = [zone for zone in weighted if float(zone.get("strength_score") or 0.0) >= cfg.daily_min_strength]
    band = select_daily_local_band_representatives(
        prefilter,
        max_zones=max(cfg.daily_max_zones * 2, cfg.daily_max_zones),
        min_zone_separation_bps=cfg.daily_min_zone_separation_bps,
    )
    collapsed = collapse_zones_by_distance(
        band,
        min_zone_separation_bps=cfg.daily_min_zone_separation_bps,
        max_zones_per_symbol=cfg.daily_max_zones,
    )
    diverse = select_spatially_diverse_zones(collapsed, max_zones=cfg.daily_max_zones)
    surfaced = [_apply_daily_operator_core(zone) for zone in diverse]
    pocketed = _consolidate_daily_selected_pockets(
        surfaced,
        min_zone_separation_bps=cfg.daily_min_zone_separation_bps,
        max_zones=cfg.daily_max_zones,
    )
    surfaced_candidates = [_apply_daily_operator_core(zone) for zone in prefilter]
    final = _apply_daily_current_regime_coverage(
        pocketed,
        candidates=surfaced_candidates,
        reference_price=last_price,
        max_zones=cfg.daily_max_zones,
    )
    ranked = sorted(final, key=lambda zone: _daily_pocket_rank_key(zone), reverse=True)
    rank_map = {str(zone.get("zone_id") or ""): idx for idx, zone in enumerate(ranked, start=1)}
    final_selected: list[dict[str, Any]] = []
    for zone in final:
        zz = dict(zone)
        zz["selector_surface"] = "daily_major"
        zz["selector_status"] = "kept"
        zz["selector_reason"] = zz.get("selector_reason") or zz.get("daily_pocket_reason") or "kept: daily macro anchor after pocket consolidation"
        zz["selector_rank"] = rank_map.get(str(zone.get("zone_id") or ""))
        final_selected.append(zz)

    canonical = select_daily_majors(
        scored,
        min_strength=cfg.daily_min_strength,
        min_zone_separation_bps=cfg.daily_min_zone_separation_bps,
        max_zones=cfg.daily_max_zones,
        strict_retest_quality=cfg.daily_require_first_retest_quality,
        reference_price=last_price,
    )

    canonical_ids = [str(zone.get("zone_id") or "") for zone in sorted(canonical, key=lambda zone: int(zone.get("selector_rank") or 9999))]
    decomposed_ids = [str(zone.get("zone_id") or "") for zone in sorted(final_selected, key=lambda zone: int(zone.get("selector_rank") or 9999))]

    confirmed_ids = _stage_id_set(weighted)
    prefilter_ids = _stage_id_set(prefilter)
    band_ids = _stage_id_set(band)
    collapsed_ids = _stage_id_set(collapsed)
    diverse_ids = _stage_id_set(diverse)
    pocket_selected_ids = _stage_id_set(pocketed)
    final_selected_ids = _stage_id_set(final_selected)

    selected_by_id = {str(zone.get("zone_id") or ""): zone for zone in final_selected}
    scored_exports: list[dict[str, Any]] = []
    for zone in weighted:
        zone_id = str(zone.get("zone_id") or "")
        enriched = dict(zone)
        if zone_id in selected_by_id:
            enriched.update(selected_by_id[zone_id])
        band_group_zone = next((row for row in band if str(row.get("zone_id") or "") == zone_id), None)
        if band_group_zone:
            enriched.update({
                "local_cluster_id": band_group_zone.get("local_cluster_id"),
                "local_cluster_role": band_group_zone.get("local_cluster_role"),
                "local_cluster_member_count": band_group_zone.get("local_cluster_member_count"),
                "local_cluster_member_ids": band_group_zone.get("local_cluster_member_ids"),
                "local_cluster_demoted_ids": band_group_zone.get("local_cluster_demoted_ids"),
                "local_cluster_bounds": band_group_zone.get("local_cluster_bounds"),
            })
        pocket_group_zone = next((row for row in pocketed if str(row.get("zone_id") or "") == zone_id), None)
        if pocket_group_zone:
            enriched.update({
                "daily_pocket_contract": pocket_group_zone.get("daily_pocket_contract"),
                "daily_pocket_id": pocket_group_zone.get("daily_pocket_id"),
                "daily_pocket_member_count": pocket_group_zone.get("daily_pocket_member_count"),
                "daily_pocket_member_ids": pocket_group_zone.get("daily_pocket_member_ids"),
                "daily_pocket_demoted_ids": pocket_group_zone.get("daily_pocket_demoted_ids"),
                "daily_pocket_reason": pocket_group_zone.get("daily_pocket_reason"),
            })
        exported = _candidate_export(
            enriched,
            selected_ids=final_selected_ids,
            confirmed_ids=confirmed_ids,
            prefilter_ids=prefilter_ids,
            band_ids=band_ids,
            collapsed_ids=collapsed_ids,
            diverse_ids=diverse_ids,
            pocket_selected_ids=pocket_selected_ids,
            final_selected_ids=final_selected_ids,
            band_groups=band,
            pocket_groups=pocketed,
            cfg=cfg,
        )
        scored_exports.append(exported)

    scored_exports.sort(
        key=lambda zone: (
            0 if zone.get("stage_membership", {}).get("final_selected") else 1,
            -float(zone.get("scores", {}).get("selection_score") or 0.0),
            float(zone.get("full_zone_bounds", {}).get("mid") or 0.0),
        )
    )

    return _jsonable(
        {
            "candidate_counts": {
                "structure_candidates": len(structure),
                "base_candidates": len(base),
                "reaction_candidates": len(reaction),
                "merged_candidates": len(merged),
                "scored_candidates": len(scored),
                "confirmed_candidates": len(weighted),
                "prefilter_candidates": len(prefilter),
                "local_band_candidates": len(band),
                "distance_collapsed_candidates": len(collapsed),
                "spatial_diversity_candidates": len(diverse),
                "pocket_selected_candidates": len(pocketed),
                "selected_daily_majors": len(final_selected),
            },
            "selector_stages": {
                "confirmed_ids": sorted(confirmed_ids),
                "prefilter_ids": sorted(prefilter_ids),
                "local_band_ids": sorted(band_ids),
                "distance_collapsed_ids": sorted(collapsed_ids),
                "spatial_diversity_ids": sorted(diverse_ids),
                "pocket_selected_ids": sorted(pocket_selected_ids),
                "selected_daily_major_ids": sorted(final_selected_ids),
            },
            "parity_check": {
                "matches_canonical_select_daily_majors": canonical_ids == decomposed_ids,
                "canonical_selected_zone_ids": canonical_ids,
                "decomposed_selected_zone_ids": decomposed_ids,
            },
            "family_candidates": {
                "structure": [_short_zone_row(zone) for zone in sorted(structure, key=lambda row: float(row.get("zone_mid") or 0.0))],
                "base": [_short_zone_row(zone) for zone in sorted(base, key=lambda row: float(row.get("zone_mid") or 0.0))],
                "reaction": [_short_zone_row(zone) for zone in sorted(reaction, key=lambda row: float(row.get("zone_mid") or 0.0))],
            },
            "merged_candidates": [_short_zone_row(zone) for zone in sorted(merged, key=lambda row: float(row.get("zone_mid") or 0.0))],
            "scored_candidates": scored_exports,
            "selected_daily_majors": [
                _short_zone_row(zone)
                for zone in sorted(final_selected, key=lambda row: int(row.get("selector_rank") or 9999))
            ],
        }
    )


def _focus_band_sections(dataset: Mapping[str, Any]) -> list[dict[str, Any]]:
    focus_bands = [
        FocusBand(
            low=float(row["low"]),
            high=float(row["high"]),
            label=str(row["label"]),
        )
        for row in (dataset.get("focus_bands") or [])
    ]
    scored = dataset.get("scored_candidates") or []
    sections: list[dict[str, Any]] = []
    for band in focus_bands:
        candidates = [zone for zone in scored if _band_overlap(zone, band)]
        candidates = sorted(
            candidates,
            key=lambda zone: (
                0 if zone.get("stage_membership", {}).get("final_selected") else 1,
                -float((zone.get("scores") or {}).get("selection_score") or 0.0),
            ),
        )
        sections.append(
            {
                "label": band.label,
                "low": band.low,
                "high": band.high,
                "candidate_count": len(candidates),
                "selected_count": sum(1 for zone in candidates if zone.get("stage_membership", {}).get("final_selected")),
                "candidates": candidates,
            }
        )
    return sections


def _markdown(dataset: Mapping[str, Any], json_path: Path) -> str:
    counts = dataset.get("candidate_counts") or {}
    last_candle = dataset.get("source_candle") or {}
    lines = [
        "# BTCUSDT 1D SR Selector Decomposition",
        "",
        f"Generated at: `{dataset.get('generated_at')}`",
        f"JSON artifact: `{json_path}`",
        "",
        "## Data-source caveat",
        "",
        str(dataset.get("data_source_caveat")),
        "",
        f"Requested as-of date: `{dataset.get('requested_asof_date')}`",
        f"Resolved source candle timestamp: `{dataset.get('resolved_asof_timestamp')}`",
        "",
        "## Counts",
        "",
        f"- Structure candidates: `{counts.get('structure_candidates')}`",
        f"- Base candidates: `{counts.get('base_candidates')}`",
        f"- Reaction candidates: `{counts.get('reaction_candidates')}`",
        f"- Merged candidates: `{counts.get('merged_candidates')}`",
        f"- Confirmed weighted candidates: `{counts.get('confirmed_candidates')}`",
        f"- Prefilter candidates: `{counts.get('prefilter_candidates')}`",
        f"- Selected daily majors: `{counts.get('selected_daily_majors')}`",
        "",
        "## Selected Daily Majors",
        "",
        "| Rank | Zone | Families | Core bounds | Full bounds | Touches | Strength | Selection | Reason |",
        "|---|---|---|---|---|---:|---:|---:|---|",
    ]
    for zone in dataset.get("selected_daily_majors") or []:
        families = ",".join(zone.get("candidate_families") or [])
        lines.append(
            "| {rank} | `{zone_id}` | `{families}` | {core_low:.2f}-{core_high:.2f} | {low:.2f}-{high:.2f} | {touches} | {strength:.2f} | {selection:.2f} | {reason} |".format(
                rank=int(zone.get("selector_rank") or 0),
                zone_id=zone.get("zone_id") or "",
                families=families,
                core_low=float(zone.get("core_low") or zone.get("full_low") or 0.0),
                core_high=float(zone.get("core_high") or zone.get("full_high") or 0.0),
                low=float(zone.get("full_low") or 0.0),
                high=float(zone.get("full_high") or 0.0),
                touches=int(zone.get("meaningful_touch_count") or 0),
                strength=float(zone.get("strength_score") or 0.0),
                selection=float(zone.get("selection_score") or 0.0),
                reason=zone.get("selected_or_rejected_reason") or zone.get("selector_reason") or "",
            )
        )

    lines.extend(
        [
            "",
            "## Focus Bands",
            "",
            f"Last close: `{float(last_candle.get('close') or 0.0):.2f}`",
            "",
        ]
    )
    for band in _focus_band_sections(dataset):
        lines.extend(
            [
                f"### {band['label']} `{band['low']:.0f}-{band['high']:.0f}`",
                "",
                f"- Overlapping scored candidates: `{band['candidate_count']}`",
                f"- Selected in band: `{band['selected_count']}`",
                "",
            ]
        )
        if not band["candidates"]:
            lines.extend(["No scored candidates overlapped this band.", ""])
            continue
        lines.extend(
            [
                "| Zone | Families | Full bounds | Core bounds | Touches | Strength | Selection | Bucket | Reason |",
                "|---|---|---|---|---:|---:|---:|---|---|",
            ]
        )
        for zone in band["candidates"]:
            families = ",".join(((zone.get("families") or {}).get("candidate_families") or []))
            full_bounds = zone.get("full_zone_bounds") or {}
            core_bounds = zone.get("operator_core_bounds") or {}
            scores = zone.get("scores") or {}
            lines.append(
                "| `{zone_id}` | `{families}` | {low:.2f}-{high:.2f} | {core_low:.2f}-{core_high:.2f} | {touches} | {strength:.2f} | {selection:.2f} | `{bucket}` | {reason} |".format(
                    zone_id=zone.get("zone_id") or "",
                    families=families,
                    low=float(full_bounds.get("low") or 0.0),
                    high=float(full_bounds.get("high") or 0.0),
                    core_low=float(core_bounds.get("low") or full_bounds.get("low") or 0.0),
                    core_high=float(core_bounds.get("high") or full_bounds.get("high") or 0.0),
                    touches=int(zone.get("meaningful_touch_count") or 0),
                    strength=float(scores.get("strength_score") or 0.0),
                    selection=float(scores.get("selection_score") or 0.0),
                    bucket=zone.get("debug_bucket") or "",
                    reason=zone.get("selected_or_rejected_reason") or "",
                )
            )
        lines.append("")

    parity = dataset.get("parity_check") or {}
    lines.extend(
        [
            "## Parity Check",
            "",
            f"- Decomposition matches canonical `select_daily_majors`: `{parity.get('matches_canonical_select_daily_majors')}`",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def build_dataset(args: argparse.Namespace) -> dict[str, Any]:
    cfg = DailyMajorConfig(
        min_history_bars=max(30, int(args.min_history_bars)),
        daily_max_zones=max(1, int(args.max_zones)),
    )
    focus_bands = _parse_focus_bands(args.focus_bands)
    liquidsniper_root = _ensure_liquidsniper_import(args.liquidsniper_root)
    asof_dt = _parse_utc_date(args.asof_date)
    db_path = Path(args.db_path).expanduser()

    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        candles = _load_market_candles(
            conn,
            symbol=args.symbol,
            timeframe=args.timeframe,
            provider_id=args.provider_id,
            venue=args.venue,
            asof_ts_ms=int(asof_dt.timestamp() * 1000),
        )
    finally:
        conn.close()

    if len(candles) < cfg.min_history_bars:
        raise RuntimeError(
            f"{args.symbol} {args.timeframe} has only {len(candles)} candles by {asof_dt.date().isoformat()}; "
            f"need at least {cfg.min_history_bars}"
        )

    decomposition = _build_decomposition(symbol=args.symbol, candles=candles, cfg=cfg)
    first = candles[0]
    last = candles[-1]
    return _jsonable(
        {
            "contract": DECOMPOSITION_CONTRACT,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "symbol": args.symbol,
            "timeframe": args.timeframe,
            "provider_id": args.provider_id,
            "venue": args.venue,
            "requested_asof_date": asof_dt.date().isoformat(),
            "requested_asof_timestamp": asof_dt.isoformat().replace("+00:00", "Z"),
            "resolved_asof_timestamp": last["timestamp"],
            "resolved_asof_ts_open_ms": last["ts_open_ms"],
            "config": asdict(cfg),
            "focus_bands": [asdict(band) for band in focus_bands],
            "canonical_source": {
                "liquidsniper_root": str(liquidsniper_root),
                "zone_engine": "liquidsniper/core/zone_engine_v3.py",
                "selector": "liquidsniper/core/zone_selectors.py:select_daily_majors",
                "sequence": "structure + base + reaction -> merge_candidate_zones -> score_zone -> select_daily_majors",
            },
            "source_candles": {
                "db_path": str(db_path),
                "first_ts": first["timestamp"],
                "last_ts": last["timestamp"],
                "candle_count": len(candles),
            },
            "source_candle": {
                key: last[key]
                for key in [
                    "provider_id",
                    "venue",
                    "symbol",
                    "timeframe",
                    "ts_open_ms",
                    "ts_close_ms",
                    "timestamp",
                    "close_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "dataset_version",
                    "trace_id",
                ]
            },
            "data_source_caveat": (
                "Requested 2026-05-04 currently resolves to the last local MarketArbiter candle on 2026-03-31. "
                "This decomposition reflects the local SQLite state, not a newer exchange candle. "
                "TradingView/vendor history differences should still be separated from selector logic."
            ),
            **decomposition,
        }
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Decompose the canonical Surveyor daily-major selector for debugging.")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    parser.add_argument("--provider-id", default=DEFAULT_PROVIDER_ID)
    parser.add_argument("--venue", default=DEFAULT_VENUE)
    parser.add_argument("--asof-date", default=DEFAULT_ASOF_DATE)
    parser.add_argument("--liquidsniper-root", default=None)
    parser.add_argument("--min-history-bars", type=int, default=DailyMajorConfig.min_history_bars)
    parser.add_argument("--max-zones", type=int, default=DailyMajorConfig.daily_max_zones)
    parser.add_argument("--focus-bands", default=DEFAULT_FOCUS_BANDS)
    parser.add_argument("--output-json", default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-md", default=DEFAULT_OUTPUT_MD)
    args = parser.parse_args(argv)

    dataset = build_dataset(args)
    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(dataset, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_md.write_text(_markdown(dataset, output_json), encoding="utf-8")
    print(
        json.dumps(
            {
                "ok": True,
                "json_path": str(output_json),
                "summary_path": str(output_md),
                "selected_daily_majors": len(dataset.get("selected_daily_majors") or []),
                "resolved_asof_timestamp": dataset.get("resolved_asof_timestamp"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
