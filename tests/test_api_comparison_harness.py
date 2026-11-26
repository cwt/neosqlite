"""Tests for the API comparison harness's failure gates."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "examples"))

from api_comparison.reporter import CompatibilityReporter  # noqa: E402
from api_comparison.runner import (  # noqa: E402
    run_comparison_category,
    skip_budget_error,
)


def test_category_exception_is_recorded_as_failure():
    reporter = CompatibilityReporter()

    def boom():
        raise RuntimeError("broken comparison")

    run_comparison_category("Broken category", boom, reporter)

    assert reporter.total_tests == 1
    assert len(reporter.failed_tests) == 1
    assert reporter.failed_tests[0]["api"] == "CATEGORY_EXCEPTION"
    assert "broken comparison" in reporter.failed_tests[0]["error"]


def test_category_with_no_recorded_results_is_a_failure():
    reporter = CompatibilityReporter()

    def silent():
        return None

    run_comparison_category("Silent category", silent, reporter)

    assert reporter.total_tests == 1
    assert reporter.failed_tests[0]["api"] == "NO_RESULTS_RECORDED"


def test_category_with_results_is_not_flagged():
    reporter = CompatibilityReporter()

    def working():
        reporter.record_result("Working", "check", True)

    run_comparison_category("Working category", working, reporter)

    assert reporter.passed_tests == 1
    assert reporter.failed_tests == []


def test_stage_aliases_normalizes_facet_tie_order():
    from api_comparison import stage_aliases

    results = stage_aliases._run(
        lambda _pipeline: [
            {"x": [{"_id": "b", "count": 2}, {"_id": "a", "count": 2}]}
        ]
    )
    assert [d["_id"] for d in results["aliases in $facet"][0]["x"]] == [
        "a",
        "b",
    ]


def test_edge_case_parity_records_checks_without_mongodb(monkeypatch):
    from api_comparison import edge_case_parity

    local_reporter = CompatibilityReporter()
    monkeypatch.setattr(edge_case_parity, "reporter", local_reporter)
    monkeypatch.setattr(edge_case_parity, "get_mongo_client", lambda: None)

    edge_case_parity.compare_edge_case_parity()

    assert local_reporter.total_tests == 13
    assert local_reporter.failed_tests == []
    assert len(local_reporter.skipped_tests) == 13


def test_skip_budget_is_enforced(monkeypatch):
    reporter = CompatibilityReporter()
    reporter.record_result("X", "a", True, skip_reason="unavailable")
    reporter.record_result("X", "b", True, skip_reason="unavailable")

    monkeypatch.setenv("NEOSQLITE_COMPARISON_MAX_SKIPPED", "1")
    assert skip_budget_error(reporter) == "2 checks skipped, budget is 1"

    monkeypatch.setenv("NEOSQLITE_COMPARISON_MAX_SKIPPED", "2")
    assert skip_budget_error(reporter) is None
