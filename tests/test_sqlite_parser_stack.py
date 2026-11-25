"""Tests for the SQLite parser-stack fallback in complex updates."""

import pytest
from types import SimpleNamespace

from neosqlite import Connection
from neosqlite._sqlite import sqlite3
from neosqlite.collection.jsonb_support import supports_jsonb

import neosqlite
from neosqlite.collection.query_helper import _sql_updates

_JSONB_PROBE_CONN = sqlite3.connect(":memory:")
_JSONB_AVAILABLE = supports_jsonb(_JSONB_PROBE_CONN)
_JSONB_PROBE_CONN.close()
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


def test_rename_fallback_uses_python_without_corruption(monkeypatch):
    """When unified SQL declines $rename (e.g. parser overflow), Python fallback stays lossless.

    Missing source fields must not become JSON null, and self-renames must not
    delete the field.
    """
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.items
        coll.insert_one({"_id": 1, "bits": 7})
        helpers = coll.query_engine.helpers
        monkeypatch.setattr(
            helpers, "_build_set_data_expression", lambda *args, **kwargs: None
        )
        coll.update_one(
            {"_id": 1}, {"$rename": {"missing": "b", "bits": "bits"}}
        )
        doc = coll.find_one({"_id": 1})
        assert doc == {"_id": 1, "bits": 7}


@pytest.mark.skipif(
    not _JSONB_AVAILABLE, reason="Requires SQLite with JSONB support"
)
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
