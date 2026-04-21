from __future__ import annotations

from typing import Any, Sequence

from .htf_phase1 import run_phase1_htf_structure


PHASE1_STRUCTURE_PROFILE_CANONICAL = "canonical"
PHASE1_STRUCTURE_PROFILE_LEGACY = "legacy"

PHASE1_STRUCTURE_CONTRACT = "phase1_structure_contract_v1"
PHASE1_STRUCTURE_CONTRACT_LEGACY = "phase1_structure_contract_legacy_v0"
PHASE1_PIVOT_LEFT = 2
PHASE1_PIVOT_RIGHT = 2
PHASE1_N_INIT = 25
PHASE1_BOS_MIN_FRAC = 0.15
PHASE1_CHOCH_MIN_FRAC = 0.15
PHASE1_STRICT_GATING = False
PHASE1_BOS_REQUIRE_FRESH_CROSS = True
PHASE1_ENABLE_CONTINUATION_BREAK = True


def normalize_phase1_structure_profile(profile: str | None) -> str:
    normalized = str(profile or PHASE1_STRUCTURE_PROFILE_CANONICAL).strip().lower()
    if normalized in {PHASE1_STRUCTURE_PROFILE_LEGACY, "legacy_v0", "pre_unification", "0.20"}:
        return PHASE1_STRUCTURE_PROFILE_LEGACY
    return PHASE1_STRUCTURE_PROFILE_CANONICAL


def phase1_structure_contract_config(*, candle_count: int | None = None, profile: str | None = None) -> dict[str, Any]:
    normalized_profile = normalize_phase1_structure_profile(profile)
    bos_min_frac = 0.20 if normalized_profile == PHASE1_STRUCTURE_PROFILE_LEGACY else PHASE1_BOS_MIN_FRAC
    contract = PHASE1_STRUCTURE_CONTRACT_LEGACY if normalized_profile == PHASE1_STRUCTURE_PROFILE_LEGACY else PHASE1_STRUCTURE_CONTRACT
    return {
        "contract": contract,
        "profile": normalized_profile,
        "left": PHASE1_PIVOT_LEFT,
        "right": PHASE1_PIVOT_RIGHT,
        "n_init": min(PHASE1_N_INIT, int(candle_count)) if candle_count is not None and candle_count > 0 else PHASE1_N_INIT,
        "break_min_frac_of_candle": bos_min_frac,
        "choch_break_min_frac_of_candle": PHASE1_CHOCH_MIN_FRAC,
        "strict_gating": PHASE1_STRICT_GATING,
        "bos_require_fresh_cross": PHASE1_BOS_REQUIRE_FRESH_CROSS,
        "enable_continuation_break": PHASE1_ENABLE_CONTINUATION_BREAK,
    }


def run_phase1_structure_contract(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    *,
    profile: str | None = None,
):
    cfg = phase1_structure_contract_config(candle_count=len(closes), profile=profile)
    return run_phase1_htf_structure(
        list(highs),
        list(lows),
        list(closes),
        left=int(cfg["left"]),
        right=int(cfg["right"]),
        n_init=int(cfg["n_init"]),
        break_min_frac_of_candle=float(cfg["break_min_frac_of_candle"]),
        choch_break_min_frac_of_candle=float(cfg["choch_break_min_frac_of_candle"]),
        strict_gating=bool(cfg["strict_gating"]),
        bos_require_fresh_cross=bool(cfg["bos_require_fresh_cross"]),
        enable_continuation_break=bool(cfg["enable_continuation_break"]),
    )


def run_phase1_structure_contract_from_candles(candles: Sequence[dict[str, Any]], *, profile: str | None = None):
    highs = [float((row.get("high") if isinstance(row, dict) else None) or 0.0) for row in candles]
    lows = [float((row.get("low") if isinstance(row, dict) else None) or 0.0) for row in candles]
    closes = [float((row.get("close") if isinstance(row, dict) else None) or 0.0) for row in candles]
    return run_phase1_structure_contract(highs, lows, closes, profile=profile)
