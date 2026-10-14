"""Tests to improve coverage of neosqlite.requests bulk request parsing."""

from __future__ import annotations

import pytest

from neosqlite.requests import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
    _parse_bulk_request,
)


class CustomUpdateReq:
    """Mock custom object with filter and update attributes."""

    def __init__(
        self,
        filter_spec,
        update_spec,
        upsert=False,
        array_filters=None,
        multi=False,
    ):
        self.filter = filter_spec
        self.update = update_spec
        self.upsert = upsert
        self.array_filters = array_filters
        self.multi = multi


class CustomDeleteReq:
    """Mock custom object with filter attribute."""

    def __init__(self, filter_spec, multi=False):
        self.filter = filter_spec
        self.multi = multi


class CustomPrivateFilterDeleteReq:
    """Mock custom object with _filter attribute."""

    def __init__(self, filter_spec, multi=False):
        self._filter = filter_spec
        self.multi = multi


class InvalidReq:
    """Mock object with no recognized bulk request attributes."""

    pass


def test_parse_bulk_request_standard_classes():
    """Verify parsing of standard neosqlite.requests classes."""
    ins = InsertOne({"a": 1})
    op, kwargs = _parse_bulk_request(ins)
    assert op == "insert_one"
    assert kwargs == {"document": {"a": 1}}

    rep = ReplaceOne({"_id": 1}, {"a": 2}, upsert=True)
    op, kwargs = _parse_bulk_request(rep)
    assert op == "replace_one"
    assert kwargs == {
        "filter": {"_id": 1},
        "replacement": {"a": 2},
        "upsert": True,
    }

    u1 = UpdateOne({"_id": 1}, {"$set": {"a": 3}}, upsert=True)
    op, kwargs = _parse_bulk_request(u1)
    assert op == "update_one"
    assert kwargs["filter"] == {"_id": 1}
    assert kwargs["upsert"] is True

    um = UpdateMany(
        {"x": 1}, {"$inc": {"count": 1}}, upsert=False, array_filters=[{"i": 1}]
    )
    op, kwargs = _parse_bulk_request(um)
    assert op == "update_many"
    assert kwargs["filter"] == {"x": 1}
    assert kwargs["array_filters"] == [{"i": 1}]

    d1 = DeleteOne({"_id": 1})
    op, kwargs = _parse_bulk_request(d1)
    assert op == "delete_one"
    assert kwargs == {"filter": {"_id": 1}}

    dm = DeleteMany({"status": "old"})
    op, kwargs = _parse_bulk_request(dm)
    assert op == "delete_many"
    assert kwargs == {"filter": {"status": "old"}}


def test_parse_bulk_request_duck_typing():
    """Verify duck-typing fallback in _parse_bulk_request."""
    # Custom update single
    c_up_single = CustomUpdateReq(
        {"a": 1}, {"$set": {"b": 2}}, upsert=True, multi=False
    )
    op, kwargs = _parse_bulk_request(c_up_single)
    assert op == "update_one"
    assert kwargs["filter"] == {"a": 1}
    assert kwargs["upsert"] is True

    # Custom update multi
    c_up_multi = CustomUpdateReq({"a": 1}, {"$set": {"b": 2}}, multi=True)
    op, kwargs = _parse_bulk_request(c_up_multi)
    assert op == "update_many"
    assert kwargs["filter"] == {"a": 1}

    # Custom delete single
    c_del_single = CustomDeleteReq({"x": 10}, multi=False)
    op, kwargs = _parse_bulk_request(c_del_single)
    assert op == "delete_one"
    assert kwargs["filter"] == {"x": 10}

    # Custom delete multi
    c_del_multi = CustomDeleteReq({"x": 10}, multi=True)
    op, kwargs = _parse_bulk_request(c_del_multi)
    assert op == "delete_many"
    assert kwargs["filter"] == {"x": 10}

    # Custom delete with _filter attribute
    c_priv_del = CustomPrivateFilterDeleteReq({"y": 20}, multi=True)
    op, kwargs = _parse_bulk_request(c_priv_del)
    assert op == "delete_many"
    assert kwargs["filter"] == {"y": 20}


def test_parse_bulk_request_invalid():
    """Verify that unsupported request objects raise ValueError."""
    with pytest.raises(ValueError, match="Unknown bulk write operation"):
        _parse_bulk_request(InvalidReq())
