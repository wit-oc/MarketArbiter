from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from market_arbiter.ops.canonical_surveyor_promotion_gate import (
    DEFAULT_PRIMARY_VARIANT,
    _ambiguity_signal,
    _as_dict,
    _as_list,
    _float,
    _summarize,
    _variant_rows,
)


CONTRACT = "canonical_surveyor_ambiguity_cohorts_v0"


def _risk_equity_pct(trade: Mapping[str, Any]) -> float:
    """Percent of baseline equity at risk after unused DCA tranches are left unused."""

    risk_pct = _float(trade.get("risk_pct"))
    risk_used_pct = _float(trade.get("risk_budget_used_pct"), 100.0)
    return risk_pct * risk_used_pct / 100.0


def _net_return_bps_from_r(trade: Mapping[str, Any], net_r_multiple: float) -> float:
    return net_r_multiple * _risk_equity_pct(trade) * 100.0


def _theoretical_tp_plan_r(trade: Mapping[str, Any]) -> float:
    target_rr = _float(trade.get("target_rr"), 2.0)
    plan = str(trade.get("take_profit_plan") or "tp_25_50_25")
    if plan == "single_final":
        return target_rr
    if plan == "tp_25_50_25":
        if target_rr <= 1.0:
            return target_rr
        if target_rr <= 2.0:
            return (0.25 * 1.0) + (0.75 * target_rr)
        return (0.25 * 1.0) + (0.50 * 2.0) + (0.25 * target_rr)
    return target_rr


def _stress_trade(trade: Mapping[str, Any], mode: str) -> dict[str, Any]:
    out = dict(trade)
    signal = _ambiguity_signal(out)
    out.update(signal)
    if not signal.get("has_execution_order_ambiguity"):
        out["stress_mode"] = "unchanged"
        return out

    current_r = _float(out.get("net_r_multiple"))
    if mode == "conservative":
        # Coarse smell-test: every hard-ambiguous trade resolves as a full loss
        # on the filled risk. Keep worse current outcomes if fees/slippage already
        # pushed them below -1R.
        stressed_r = min(current_r, -1.0)
    elif mode == "optimistic":
        # Coarse smell-test: every hard-ambiguous trade reaches the theoretical
        # TP-plan maximum for the configured final target. Keep better current
        # outcomes if the existing accounting already exceeds the simple plan.
        stressed_r = max(current_r, _theoretical_tp_plan_r(out))
    elif mode == "midpoint":
        stressed_r = (min(current_r, -1.0) + max(current_r, _theoretical_tp_plan_r(out))) / 2.0
    else:
        raise ValueError(f"unknown stress mode: {mode}")

    out["stress_mode"] = mode
    out["source_net_r_multiple"] = current_r
    out["source_net_return_bps"] = _float(out.get("net_return_bps"))
    out["net_r_multiple"] = stressed_r
    out["net_return_bps"] = _net_return_bps_from_r(out, stressed_r)
    return out


def _cohort_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    clean: list[dict[str, Any]] = []
    diagnostic_only: list[dict[str, Any]] = []
    execution_order: list[dict[str, Any]] = []
    non_execution_order: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        signal = _ambiguity_signal(item)
        item.update(signal)
        if signal.get("has_execution_order_ambiguity"):
            execution_order.append(item)
        elif signal.get("intrabar_ambiguity_signal") == "diagnostic_only":
            diagnostic_only.append(item)
            non_execution_order.append(item)
        else:
            clean.append(item)
            non_execution_order.append(item)
    return {
        "clean": clean,
        "diagnostic_only": diagnostic_only,
        "non_execution_order": non_execution_order,
        "execution_order_ambiguous": execution_order,
    }


def _examples(rows: Sequence[Mapping[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    def key(row: Mapping[str, Any]) -> tuple[float, str]:
        return (_float(row.get("net_return_bps")), str(row.get("entry_event_id") or ""))

    out = []
    for row in sorted(rows, key=key)[:limit]:
        out.append({
            "symbol": row.get("symbol"),
            "side": row.get("side"),
            "entry_ts": row.get("entry_ts"),
            "exit_ts": row.get("exit_ts"),
            "exit_reason": row.get("exit_reason"),
            "net_r_multiple": row.get("net_r_multiple"),
            "net_return_bps": row.get("net_return_bps"),
            "risk_pct": row.get("risk_pct"),
            "risk_budget_used_pct": row.get("risk_budget_used_pct"),
            "ambiguity_flags": row.get("ambiguity_flags"),
            "entry_event_id": row.get("entry_event_id"),
        })
    return out


def build_cohort_report(report: Mapping[str, Any], variant: Mapping[str, Any]) -> dict[str, Any]:
    rows = _variant_rows(report, variant)
    cohorts = _cohort_rows(rows)
    conservative_rows = [_stress_trade(row, "conservative") for row in rows]
    midpoint_rows = [_stress_trade(row, "midpoint") for row in rows]
    optimistic_rows = [_stress_trade(row, "optimistic") for row in rows]

    return {
        "contract": CONTRACT,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "variant": dict(variant),
        "method": {
            "purpose": "Cheap validation smell-test before lower-timeframe replay.",
            "clean": "Trades with no intrabar flags.",
            "diagnostic_only": "Trades with review-only intrabar flags; not promotion-gating by themselves.",
            "execution_order_ambiguous": "Hard ambiguous trades where OHLC ordering can change realized accounting.",
            "conservative_bound": "Hard ambiguous trades are stressed to a full -1R loss on filled risk; non-hard trades unchanged.",
            "midpoint_bound": "Hard ambiguous trades use the midpoint of the conservative and optimistic stress values; non-hard trades unchanged.",
            "optimistic_bound": "Hard ambiguous trades are stressed to the theoretical TP-plan max for the configured final target; non-hard trades unchanged.",
        },
        "cohorts": {name: _summarize(items) for name, items in cohorts.items()},
        "bounds": {
            "current_all": _summarize(rows),
            "conservative_all": _summarize(conservative_rows),
            "midpoint_all": _summarize(midpoint_rows),
            "optimistic_all": _summarize(optimistic_rows),
        },
        "examples": {
            "worst_execution_order_current": _examples(cohorts["execution_order_ambiguous"]),
        },
    }


def _fmt_money(value: Any) -> str:
    return f"${_float(value):+.2f}"


def _fmt_row(name: str, row: Mapping[str, Any]) -> str:
    return (
        f"| {name} | {row.get('trade_count', 0)} | {_float(row.get('win_rate')):.2%} | "
        f"{_float(row.get('avg_net_r_multiple')):+.4f} | {_float(row.get('total_net_bps')):+.2f} | "
        f"{_fmt_money(row.get('portfolio_final_pnl'))} | {_fmt_money(row.get('portfolio_max_drawdown'))} | "
        f"{_float(row.get('execution_order_ambiguity_rate')):.2%} | {_float(row.get('diagnostic_only_intrabar_rate')):.2%} |"
    )


def _markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Canonical Surveyor Ambiguity Cohort Bounds",
        "",
        "Cheap validation smell-test before lower-timeframe replay. Conservative/optimistic rows stress only hard execution-order ambiguous trades; clean and diagnostic-only trades remain unchanged.",
        "",
        "## Variant",
        "",
    ]
    for key, value in _as_dict(report.get("variant")).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend([
        "",
        "## Cohorts under current accounting",
        "",
        "| Cohort | Trades | Win | Avg R | Total bps | $1k PnL | $1k max DD | Exec ambig | Diag-only |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for name, label in [
        ("clean", "Clean"),
        ("diagnostic_only", "Diagnostic-only"),
        ("non_execution_order", "Clean + diagnostic-only"),
        ("execution_order_ambiguous", "Execution-order ambiguous"),
    ]:
        lines.append(_fmt_row(label, _as_dict(_as_dict(report.get("cohorts")).get(name))))
    lines.extend([
        "",
        "## Full-set stress bounds",
        "",
        "| Bound | Trades | Win | Avg R | Total bps | $1k PnL | $1k max DD | Exec ambig | Diag-only |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for name, label in [
        ("current_all", "Current accounting"),
        ("conservative_all", "Conservative hard-ambig"),
        ("midpoint_all", "Midpoint hard-ambig"),
        ("optimistic_all", "Optimistic hard-ambig"),
    ]:
        lines.append(_fmt_row(label, _as_dict(_as_dict(report.get("bounds")).get(name))))

    bounds = _as_dict(report.get("bounds"))
    conservative = _as_dict(bounds.get("conservative_all"))
    clean = _as_dict(_as_dict(report.get("cohorts")).get("clean"))
    midpoint = _as_dict(bounds.get("midpoint_all"))
    clean_positive = _float(clean.get("avg_net_r_multiple")) > 0 and _float(clean.get("portfolio_final_pnl")) > 0
    conservative_positive = _float(conservative.get("avg_net_r_multiple")) > 0 and _float(conservative.get("portfolio_final_pnl")) > 0
    midpoint_positive = _float(midpoint.get("avg_net_r_multiple")) > 0 and _float(midpoint.get("portfolio_final_pnl")) > 0
    if clean_positive and conservative_positive:
        read = "Clean and conservative full-set bounds are positive; lower-timeframe replay is worth pursuing."
    elif clean_positive and midpoint_positive:
        read = "Clean trades and midpoint bound are positive, but the conservative bound is weak; inspect hard-ambiguous examples before investing in lower-timeframe replay."
    elif clean_positive:
        read = "Clean trades are positive, but ambiguity stress dominates; inspect hard-ambiguous examples before lower-timeframe replay."
    else:
        read = "Clean trades are not positive enough; rethink the setup before lower-timeframe replay."
    lines.extend(["", "## Read", "", f"- {read}"])

    examples = _as_list(_as_dict(report.get("examples")).get("worst_execution_order_current"))
    if examples:
        lines.extend([
            "",
            "## Worst hard-ambiguous examples under current accounting",
            "",
            "| Symbol | Side | Entry | Exit | Reason | R | Bps | Flags |",
            "|---|---|---|---|---|---:|---:|---|",
        ])
        for item in examples:
            row = _as_dict(item)
            flags = ", ".join(str(flag) for flag in _as_list(row.get("ambiguity_flags")))
            lines.append(
                f"| {row.get('symbol')} | {row.get('side')} | {row.get('entry_ts')} | {row.get('exit_ts')} | {row.get('exit_reason')} | "
                f"{_float(row.get('net_r_multiple')):+.4f} | {_float(row.get('net_return_bps')):+.2f} | `{flags}` |"
            )
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build ambiguity cohort and stress-bound report for canonical Surveyor retests.")
    parser.add_argument("--report", default="artifacts/strategy_backtests/canonical_surveyor_final_target_sweep_2_5r/dca_risk_report.json")
    parser.add_argument("--output-dir", default="artifacts/strategy_backtests/canonical_surveyor_ambiguity_cohorts_4r")
    parser.add_argument("--primary-target", default="4R", help="Primary final target label, e.g. 2R or 4R")
    args = parser.parse_args(argv)

    source = json.loads(Path(args.report).read_text(encoding="utf-8"))
    variant = {**DEFAULT_PRIMARY_VARIANT, "target": str(args.primary_target)}
    cohort_report = build_cohort_report(source, variant)

    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    report_path = outdir / "ambiguity_cohort_report.json"
    summary_path = outdir / "SUMMARY.md"
    report_path.write_text(json.dumps(cohort_report, indent=2, sort_keys=True), encoding="utf-8")
    summary_path.write_text(_markdown(cohort_report), encoding="utf-8")
    print(json.dumps({"ok": True, "summary_path": str(summary_path), "report_path": str(report_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
