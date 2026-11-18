"""Tests for the benchmark-only large-collection module."""

import sys
from pathlib import Path

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
sys.path.insert(0, str(EXAMPLES))

from api_comparison.large_collection import _doc_count, _documents  # noqa: E402


def test_doc_count_default_and_override(monkeypatch):
    monkeypatch.delenv("NEOSQLITE_BENCH_DOCS", raising=False)
    assert _doc_count() == 10000
    monkeypatch.setenv("NEOSQLITE_BENCH_DOCS", "250")
    assert _doc_count() == 250
    monkeypatch.setenv("NEOSQLITE_BENCH_DOCS", "0")
    assert _doc_count() == 1
    monkeypatch.setenv("NEOSQLITE_BENCH_DOCS", "garbage")
    assert _doc_count() == 10000


def test_documents_shape():
    docs = _documents(10)
    assert len(docs) == 10
    assert docs[0]["_id"] == 0
    assert docs[-1]["_id"] == 9
    assert docs[3]["group"] == "g3"
    assert docs[3]["value"] == 3
    assert docs[3]["active"] is False
    assert docs[4]["active"] is True
    assert len(docs[0]["tags"]) == 2


def test_benchmark_only_module_not_in_comparison_functions():
    from api_comparison import runner

    names = {name for name, _ in runner.COMPARISON_FUNCTIONS}
    bench_names = {name for name, _ in runner.BENCHMARK_ONLY_FUNCTIONS}
    assert bench_names == {
        "Large Collection (Reads & Updates)",
        "Large Collection (Index Operations)",
    }
    assert not (bench_names & names)
