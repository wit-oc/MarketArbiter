from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
from typing import Any, Callable, Mapping, Sequence

from market_arbiter.arbiter.ohlcv_backtest import parse_timestamp


ARBITER_BACKTEST_SPLIT_REPORT_CONTRACT_V1 = "arbiter_backtest_split_report_v1"


@dataclass(frozen=True)
class BacktestFold:
    """Chronological train/test window for replay backtests.

    Fold boundaries are UTC ISO8601 strings or any value accepted by
    `parse_timestamp`. The split code only uses candidate/event timestamps that
    already exist in a replay dataset; it never inspects future candles or
    rebuilds Surveyor state.
    """

    fold_id: str
    train_end: str
    test_start: str
    test_end: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


DEFAULT_WALK_FORWARD_FOLDS: tuple[BacktestFold, ...] = (
    BacktestFold("wf_2023", "2022-12-31T23:59:59Z", "2023-01-01T00:00:00Z", "2023-12-31T23:59:59Z"),
    BacktestFold("wf_2024", "2023-12-31T23:59:59Z", "2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z"),
    BacktestFold("wf_2025", "2024-12-31T23:59:59Z", "2025-01-01T00:00:00Z", "2025-12-31T23:59:59Z"),
    BacktestFold("wf_2026_q1", "2025-12-31T23:59:59Z", "2026-01-01T00:00:00Z", "2026-03-31T23:59:59Z"),
)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _iso(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return str(value)
    except Exception:  # pragma: no cover - defensive only
        return None


def fold_from_mapping(value: Mapping[str, Any]) -> BacktestFold:
    return BacktestFold(
        fold_id=str(value["fold_id"]),
        train_end=str(value["train_end"]),
        test_start=str(value["test_start"]),
        test_end=str(value["test_end"]),
    )


def event_id_for_candidate(candidate: Mapping[str, Any]) -> str:
    return str(candidate.get("entry_event_id") or candidate.get("event_id") or "")


def event_id_for_row(row: Mapping[str, Any]) -> str:
    return str(row.get("event_id") or row.get("entry_event_id") or "")


def evaluation_entry_ts(evaluation: Mapping[str, Any]) -> str | None:
    """Return the timestamp that determines train/test membership.

    Trade-candidate entry timestamp is preferred because it is what the OHLCV
    simulator actually executes. Event-study-only rows fall back to event_ts so
    diagnostic datasets can still receive split manifests.
    """

    candidate_ts = _iso(_as_dict(evaluation.get("trade_candidate")).get("entry_ts"))
    if candidate_ts:
        return candidate_ts
    event = _as_dict(evaluation.get("event_study_row"))
    return _iso(event.get("event_ts") or event.get("ts"))


def candidate_entry_ts(candidate: Mapping[str, Any]) -> str | None:
    return _iso(candidate.get("entry_ts") or candidate.get("event_ts") or candidate.get("ts"))


def _timestamp_or_none(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return parse_timestamp(value)
    except Exception:
        return None


def _filter_evaluation_dataset(dataset: Mapping[str, Any], predicate: Callable[[int], bool]) -> dict[str, Any]:
    evaluations: list[dict[str, Any]] = []
    for raw_evaluation in _as_list(dataset.get("evaluations")):
        evaluation = deepcopy(_as_dict(raw_evaluation))
        ts = _timestamp_or_none(evaluation_entry_ts(evaluation))
        if ts is not None and predicate(ts):
            evaluations.append(evaluation)
    output = dict(dataset)
    output["evaluations"] = evaluations
    output["event_study_rows"] = [row for evaluation in evaluations if (row := _as_dict(evaluation.get("event_study_row")))]
    output["trade_candidates"] = [candidate for evaluation in evaluations if (candidate := _as_dict(evaluation.get("trade_candidate")))]
    return output


def _filter_candidate_dataset(dataset: Mapping[str, Any], predicate: Callable[[int], bool]) -> dict[str, Any]:
    candidates = []
    kept_event_ids: set[str] = set()
    for raw_candidate in _as_list(dataset.get("trade_candidates")):
        candidate = deepcopy(_as_dict(raw_candidate))
        ts = _timestamp_or_none(candidate_entry_ts(candidate))
        if ts is not None and predicate(ts):
            candidates.append(candidate)
            event_id = event_id_for_candidate(candidate)
            if event_id:
                kept_event_ids.add(event_id)
    event_rows = []
    for raw_row in _as_list(dataset.get("event_study_rows")):
        row = deepcopy(_as_dict(raw_row))
        event_id = event_id_for_row(row)
        row_ts = _timestamp_or_none(_iso(row.get("event_ts") or row.get("ts")))
        if (event_id and event_id in kept_event_ids) or (not kept_event_ids and row_ts is not None and predicate(row_ts)):
            event_rows.append(row)
    output = dict(dataset)
    output["event_study_rows"] = event_rows
    output["trade_candidates"] = candidates
    return output


def filter_dataset_by_entry_ts(dataset: Mapping[str, Any], predicate: Callable[[int], bool]) -> dict[str, Any]:
    """Return a dataset subset using only point-in-time entry/event timestamps."""

    if _as_list(dataset.get("evaluations")):
        return _filter_evaluation_dataset(dataset, predicate)
    return _filter_candidate_dataset(dataset, predicate)


def build_fold_datasets(dataset: Mapping[str, Any], fold: BacktestFold) -> dict[str, dict[str, Any]]:
    train_end = parse_timestamp(fold.train_end)
    test_start = parse_timestamp(fold.test_start)
    test_end = parse_timestamp(fold.test_end)
    if train_end >= test_start:
        raise ValueError(f"fold {fold.fold_id} train_end must be before test_start")
    if test_start > test_end:
        raise ValueError(f"fold {fold.fold_id} test_start must be <= test_end")
    return {
        "train": filter_dataset_by_entry_ts(dataset, lambda ts: ts <= train_end),
        "test": filter_dataset_by_entry_ts(dataset, lambda ts: test_start <= ts <= test_end),
    }


def _dataset_counts(dataset: Mapping[str, Any]) -> dict[str, int]:
    return {
        "evaluations": len(_as_list(dataset.get("evaluations"))),
        "event_study_rows": len(_as_list(dataset.get("event_study_rows"))),
        "trade_candidates": len(_as_list(dataset.get("trade_candidates"))),
    }


def _entry_ts_values(dataset: Mapping[str, Any]) -> list[int]:
    values: list[int] = []
    if _as_list(dataset.get("evaluations")):
        for evaluation in _as_list(dataset.get("evaluations")):
            ts = _timestamp_or_none(evaluation_entry_ts(_as_dict(evaluation)))
            if ts is not None:
                values.append(ts)
        return values
    for candidate in _as_list(dataset.get("trade_candidates")):
        ts = _timestamp_or_none(candidate_entry_ts(_as_dict(candidate)))
        if ts is not None:
            values.append(ts)
    return values


def _entry_event_ids(dataset: Mapping[str, Any]) -> set[str]:
    ids = set()
    if _as_list(dataset.get("evaluations")):
        for evaluation in _as_list(dataset.get("evaluations")):
            event_id = event_id_for_candidate(_as_dict(_as_dict(evaluation).get("trade_candidate")))
            if not event_id:
                event_id = event_id_for_row(_as_dict(_as_dict(evaluation).get("event_study_row")))
            if event_id:
                ids.add(event_id)
        return ids
    for candidate in _as_list(dataset.get("trade_candidates")):
        event_id = event_id_for_candidate(_as_dict(candidate))
        if event_id:
            ids.add(event_id)
    if not ids:
        for row in _as_list(dataset.get("event_study_rows")):
            event_id = event_id_for_row(_as_dict(row))
            if event_id:
                ids.add(event_id)
    return ids


def threshold_training_provenance(
    train_dataset: Mapping[str, Any],
    features_by_event: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Describe which train-window events can contribute to thresholds."""

    event_ids = sorted(_entry_event_ids(train_dataset))
    feature_event_ids = sorted(event_id for event_id in event_ids if event_id in features_by_event)
    missing_feature_event_ids = sorted(set(event_ids) - set(feature_event_ids))
    return {
        "train_event_ids": event_ids,
        "train_event_count": len(event_ids),
        "threshold_feature_event_ids": feature_event_ids,
        "threshold_feature_count": len(feature_event_ids),
        "missing_feature_event_ids": missing_feature_event_ids,
        "missing_feature_count": len(missing_feature_event_ids),
        "source": "train_window_only",
    }


def _missing_timestamp_count(dataset: Mapping[str, Any]) -> int:
    missing = 0
    if _as_list(dataset.get("evaluations")):
        for evaluation in _as_list(dataset.get("evaluations")):
            if _timestamp_or_none(evaluation_entry_ts(_as_dict(evaluation))) is None:
                missing += 1
        return missing
    for candidate in _as_list(dataset.get("trade_candidates")):
        if _timestamp_or_none(candidate_entry_ts(_as_dict(candidate))) is None:
            missing += 1
    return missing


def build_split_report(dataset: Mapping[str, Any], folds: Sequence[BacktestFold] = DEFAULT_WALK_FORWARD_FOLDS) -> dict[str, Any]:
    """Build an auditable chronological train/test split manifest."""

    input_counts = _dataset_counts(dataset)
    fold_rows: list[dict[str, Any]] = []
    for fold in folds:
        fold_datasets = build_fold_datasets(dataset, fold)
        train_ts = _entry_ts_values(fold_datasets["train"])
        test_ts = _entry_ts_values(fold_datasets["test"])
        train_end = parse_timestamp(fold.train_end)
        test_start = parse_timestamp(fold.test_start)
        test_end = parse_timestamp(fold.test_end)
        overlap_ids = sorted(_entry_event_ids(fold_datasets["train"]) & _entry_event_ids(fold_datasets["test"]))
        fold_rows.append(
            {
                **fold.to_dict(),
                "train": {
                    **_dataset_counts(fold_datasets["train"]),
                    "max_entry_ts": max(train_ts) if train_ts else None,
                    "after_train_end_count": sum(1 for ts in train_ts if ts > train_end),
                },
                "test": {
                    **_dataset_counts(fold_datasets["test"]),
                    "min_entry_ts": min(test_ts) if test_ts else None,
                    "max_entry_ts": max(test_ts) if test_ts else None,
                    "outside_test_window_count": sum(1 for ts in test_ts if ts < test_start or ts > test_end),
                },
                "overlap_entry_event_ids": overlap_ids,
                "chronology_ok": not overlap_ids
                and all(ts <= train_end for ts in train_ts)
                and all(test_start <= ts <= test_end for ts in test_ts),
            }
        )

    return {
        "contract": ARBITER_BACKTEST_SPLIT_REPORT_CONTRACT_V1,
        "source_contract": dataset.get("contract"),
        "source_ruleset_id": dataset.get("ruleset_id"),
        "input": {**input_counts, "missing_entry_timestamp_count": _missing_timestamp_count(dataset)},
        "fold_count": len(fold_rows),
        "folds": fold_rows,
        "all_chronology_ok": all(row["chronology_ok"] for row in fold_rows),
        "interpretation": [
            "Train/test split membership is based only on candidate entry_ts or event_study_row event_ts already present in the replay dataset.",
            "This report is an integrity manifest, not a performance claim; run OHLCV simulation separately on each test split before promotion.",
        ],
    }
