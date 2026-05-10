from __future__ import annotations

import pytest

from market_arbiter.arbiter.backtest_splits import (
    ARBITER_BACKTEST_SPLIT_REPORT_CONTRACT_V1,
    BacktestFold,
    build_fold_datasets,
    build_split_report,
    threshold_training_provenance,
)


def _evaluation(event_id: str, entry_ts: str) -> dict:
    return {
        "verdict": "candidate",
        "event_study_row": {
            "event_id": event_id,
            "event_ts": entry_ts,
            "symbol": "BTCUSDT",
            "side": "long",
        },
        "trade_candidate": {
            "entry_event_id": event_id,
            "entry_ts": entry_ts,
            "symbol": "BTCUSDT",
            "side": "long",
            "invalidation_level_hint": 90.0,
        },
    }


def _dataset() -> dict:
    evaluations = [
        _evaluation("evt-2022", "2022-06-01T00:00:00Z"),
        _evaluation("evt-2023", "2023-06-01T00:00:00Z"),
        _evaluation("evt-2024", "2024-06-01T00:00:00Z"),
    ]
    return {
        "contract": "foxian_retest_backtest_dataset_v0",
        "ruleset_id": "foxian_retest_flip_confluence_v0",
        "evaluations": evaluations,
        "event_study_rows": [row["event_study_row"] for row in evaluations],
        "trade_candidates": [row["trade_candidate"] for row in evaluations],
    }


def test_build_fold_datasets_assigns_train_and_test_without_leakage() -> None:
    fold = BacktestFold(
        fold_id="wf_2023",
        train_end="2022-12-31T23:59:59Z",
        test_start="2023-01-01T00:00:00Z",
        test_end="2023-12-31T23:59:59Z",
    )

    split = build_fold_datasets(_dataset(), fold)

    assert [row["entry_event_id"] for row in split["train"]["trade_candidates"]] == ["evt-2022"]
    assert [row["entry_event_id"] for row in split["test"]["trade_candidates"]] == ["evt-2023"]
    assert {row["event_id"] for row in split["train"]["event_study_rows"]}.isdisjoint(
        {row["event_id"] for row in split["test"]["event_study_rows"]}
    )


def test_build_split_report_proves_fold_chronology_and_overlap_counts() -> None:
    report = build_split_report(
        _dataset(),
        [
            BacktestFold(
                fold_id="wf_2023",
                train_end="2022-12-31T23:59:59Z",
                test_start="2023-01-01T00:00:00Z",
                test_end="2023-12-31T23:59:59Z",
            ),
            BacktestFold(
                fold_id="wf_2024",
                train_end="2023-12-31T23:59:59Z",
                test_start="2024-01-01T00:00:00Z",
                test_end="2024-12-31T23:59:59Z",
            ),
        ],
    )

    assert report["contract"] == ARBITER_BACKTEST_SPLIT_REPORT_CONTRACT_V1
    assert report["all_chronology_ok"] is True
    assert report["folds"][0]["train"]["trade_candidates"] == 1
    assert report["folds"][0]["test"]["trade_candidates"] == 1
    assert report["folds"][0]["overlap_entry_event_ids"] == []
    assert report["folds"][1]["train"]["trade_candidates"] == 2
    assert report["folds"][1]["test"]["trade_candidates"] == 1


def test_threshold_training_provenance_uses_train_window_events_only() -> None:
    fold = BacktestFold(
        fold_id="wf_2023",
        train_end="2022-12-31T23:59:59Z",
        test_start="2023-01-01T00:00:00Z",
        test_end="2023-12-31T23:59:59Z",
    )
    train = build_fold_datasets(_dataset(), fold)["train"]

    provenance = threshold_training_provenance(
        train,
        {
            "evt-2022": {"body_ratio": 0.7, "selection_score": 90.0},
            "evt-2023": {"body_ratio": 0.8, "selection_score": 95.0},
        },
    )

    assert provenance["source"] == "train_window_only"
    assert provenance["train_event_ids"] == ["evt-2022"]
    assert provenance["threshold_feature_event_ids"] == ["evt-2022"]
    assert "evt-2023" not in provenance["threshold_feature_event_ids"]


def test_build_fold_datasets_rejects_overlapping_boundaries() -> None:
    with pytest.raises(ValueError, match="train_end must be before test_start"):
        build_fold_datasets(
            _dataset(),
            BacktestFold(
                fold_id="bad",
                train_end="2023-01-01T00:00:00Z",
                test_start="2023-01-01T00:00:00Z",
                test_end="2023-12-31T23:59:59Z",
            ),
        )
