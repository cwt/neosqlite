"""Tests for the SQLite parser-stack fallback in complex updates."""

import sqlite3
from types import SimpleNamespace

import neosqlite
from neosqlite.collection.query_helper import _sql_updates
from neosqlite.collection.query_helper.utils import (
    _check_sqlite_version,
    _supports_heap_parser_stack,
)

_COMPLEX_UPDATE = {
    "$rename": {"src": "dst", "bits": "bits"},
    "$bit": {"bits": {"and": 6, "or": 8, "xor": 1}},
    "$currentDate": {"ts": True},
    "$min": {"lo": 5},
    "$max": {"hi": 20},
    "$pop": {"nums": -1},
}


def test_heap_parser_stack_flag_matches_sqlite_version():
    assert _supports_heap_parser_stack() == _check_sqlite_version("3.46.0")


def test_parser_stack_overflow_selects_python_fallback(monkeypatch):
    """An EXPLAIN probe overflow routes the update to Python on old SQLite."""

    class FakeDB:
        def execute(self, sql, params=()):
            if sql.startswith("EXPLAIN"):
                raise sqlite3.OperationalError("parser stack overflow")
            raise AssertionError("unexpected execute: " + sql)

    with neosqlite.Connection(":memory:") as conn:
        helpers = conn.ops.query_engine.helpers
        monkeypatch.setattr(
            _sql_updates, "_supports_heap_parser_stack", lambda: False
        )
        monkeypatch.setattr(helpers, "collection", SimpleNamespace(db=FakeDB()))
        assert helpers._build_set_data_expression(_COMPLEX_UPDATE) is None


def test_legacy_sql_path_declines_rename():
    """The lossy legacy $rename SQL must never be selected.

    It creates JSON null for missing sources and deletes self-renamed fields,
    so updates containing $rename must use the unified builder or Python.
    """
    with neosqlite.Connection(":memory:") as conn:
        helpers = conn.ops.query_engine.helpers
        assert not helpers._can_use_sql_updates(
            {"$rename": {"a": "b"}}, doc_id=1
        )
        assert not helpers._can_use_sql_updates(_COMPLEX_UPDATE, doc_id=1)
        # Without $rename the legacy path is still available.
        assert helpers._can_use_sql_updates({"$set": {"a": 1}}, doc_id=1)


def test_probe_error_other_than_overflow_does_not_change_selection(monkeypatch):
    """Only 'parser stack overflow' forces the fallback; other errors don't."""

    class FakeDB:
        def execute(self, sql, params=()):
            if sql.startswith("EXPLAIN"):
                raise sqlite3.OperationalError("some other parse issue")
            raise AssertionError("unexpected execute: " + sql)

    with neosqlite.Connection(":memory:") as conn:
        helpers = conn.ops.query_engine.helpers
        monkeypatch.setattr(
            _sql_updates, "_supports_heap_parser_stack", lambda: False
        )
        monkeypatch.setattr(helpers, "collection", SimpleNamespace(db=FakeDB()))
        result = helpers._build_set_data_expression(_COMPLEX_UPDATE)
        assert result is not None
